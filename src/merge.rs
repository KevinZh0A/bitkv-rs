#![allow(clippy::field_reassign_with_default)]
use std::{
  fs,
  path::{Path, PathBuf},
  sync::atomic::Ordering,
};

use log::error;

use crate::{
  batch::{log_record_key_with_seq, parse_log_record_key, NON_TXN_SEQ_NO},
  data::{
    data_file::{
      get_data_file_name, DataFile, DATA_FILE_NAME_SUFFIX, HINT_FILE_NAME,
      MERGE_FINISHED_FILE_NAME, SEQ_NO_FILE_NAME,
    },
    log_record::{decode_log_record_pos, LogRecord, LogRecordType},
  },
  db::{Engine, FILE_LOCK_NAME},
  errors::{Errors, Result},
  option::{IOManagerType, Options},
  util,
};

const MERGE_DIR_NAME: &str = "merge";
const MERGE_FIN_KEY: &[u8] = "merge.finished".as_bytes();

impl Engine {
  /// merge data directories, produce valid data and create hint file
  pub fn merge(&self) -> Result<()> {
    // if engine is empty, just return
    if self.is_engine_empty() {
      return Ok(());
    }

    // if merge is running, just return
    let lock = self.merging_lock.try_lock();
    if lock.is_none() {
      return Err(Errors::MergeInProgress);
    }

    // determine if the merge is necessary
    let reclaim_size = self.reclaim_size.load(Ordering::SeqCst);
    let total_size = util::file::dir_disk_size(&self.options.dir_path);
    let ratio = reclaim_size as f32 / total_size as f32;
    if ratio < self.options.file_merge_threshold {
      return Err(Errors::MergeThresholdUnreached);
    }

    let available_space = util::file::available_disk_space();
    if total_size - reclaim_size as u64 >= available_space {
      return Err(Errors::MergeNoEnoughSpace);
    }

    let merge_path = get_merge_path(&self.options.dir_path);

    // if dir exists, remove it
    if merge_path.is_dir() {
      fs::remove_dir_all(merge_path.clone()).unwrap();
    }

    // create merge dir
    if let Err(e) = fs::create_dir(merge_path.clone()) {
      error!("fail to create merge path {}", e);
      return Err(Errors::FailedToCreateDatabaseDir);
    }

    // Retrieve all data files for merging
    let merge_files = self.rotate_merge_files()?;

    // open a new temporary database instance for merging
    let mut merge_db_opts = Options::default();
    merge_db_opts.dir_path = merge_path.clone();
    merge_db_opts.data_file_size = self.options.data_file_size;
    let merge_db = Engine::open(merge_db_opts)?;

    // open hint file
    let hint_file = DataFile::new_hint_file(&merge_path)?;

    // iterate over all data files and rewrite valid files
    for data_file in merge_files.iter() {
      let mut offset = 0;
      loop {
        let (mut log_record, size) = match data_file.read_log_record(offset) {
          Ok(result) => (result.record, result.size),
          Err(e) => {
            if e == Errors::ReadDataFileEOF {
              break;
            }
            return Err(e);
          }
        };

        // deserialize log record and get real key
        let (real_key, _) = parse_log_record_key(log_record.key.clone());
        if let Some(index_pos) = self.index.get(real_key.clone()) {
          // if file id and offset are the same, which means the record is valid
          if index_pos.file_id == data_file.get_file_id() && index_pos.offset == offset {
            // remove transaction sequence number
            log_record.key = log_record_key_with_seq(real_key.clone(), NON_TXN_SEQ_NO);
            let log_record_pos = merge_db.append_log_record(&mut log_record)?;
            // update hint file
            hint_file.write_hint_record(real_key.clone(), log_record_pos)?;
          }
        }
        offset += size as u64;
      }
    }

    // sync all files
    merge_db.sync()?;
    hint_file.sync()?;

    // get latest unmerged file id
    let non_merge_file_id = merge_files.last().unwrap().get_file_id() + 1;
    let merge_fin_file = DataFile::new_merge_fin_file(&merge_path)?;
    let merge_fin_record = LogRecord {
      key: MERGE_FIN_KEY.to_vec(),
      value: non_merge_file_id.to_string().into_bytes(),
      rec_type: LogRecordType::Normal,
    };
    let enc_record = merge_fin_record.encode();
    merge_fin_file.write(&enc_record)?;
    merge_fin_file.sync()?;

    Ok(())
  }

  fn is_engine_empty(&self) -> bool {
    let active_file = self.active_data_file.read();
    let old_files = self.old_data_files.read();
    active_file.get_write_off() == 0 && old_files.len() == 0
  }

  fn rotate_merge_files(&self) -> Result<Vec<DataFile>> {
    // retrieve old data files id
    let mut merge_file_ids = Vec::new();
    let mut old_files = self.old_data_files.write();
    for fid in old_files.keys() {
      merge_file_ids.push(*fid);
    }

    // create a new active file for writing
    let mut active_file = self.active_data_file.write();

    // sync active file
    active_file.sync()?;
    let active_file_id = active_file.get_file_id();
    let new_active_file = DataFile::new(
      &self.options.dir_path,
      active_file_id + 1,
      IOManagerType::StandardFileIO,
    )?;
    *active_file = new_active_file;

    // load current active data file to old data files
    let old_file = DataFile::new(
      &self.options.dir_path,
      active_file_id,
      IOManagerType::StandardFileIO,
    )?;
    old_files.insert(active_file_id, old_file);

    // load id to merge file ids list
    merge_file_ids.push(active_file_id);

    // sort for an ascending merge order
    merge_file_ids.sort();

    // retrieve data files
    let mut merge_files = Vec::new();
    for file_id in merge_file_ids {
      let data_file = DataFile::new(
        &self.options.dir_path,
        file_id,
        IOManagerType::StandardFileIO,
      )?;
      merge_files.push(data_file);
    }

    Ok(merge_files)
  }

  /// load index from hint file
  pub(crate) fn load_index_from_hint_file(&self) -> Result<()> {
    let hint_file_name = self.options.dir_path.join(HINT_FILE_NAME);

    // if hint file doesn't exist, just return
    if !hint_file_name.is_file() {
      return Ok(());
    }

    let hint_file = DataFile::new_hint_file(&self.options.dir_path)?;
    let mut offset = 0;
    loop {
      let (log_record, size) = match hint_file.read_log_record(offset) {
        Ok(result) => (result.record, result.size),
        Err(e) => {
          if e == Errors::ReadDataFileEOF {
            break;
          }
          return Err(e);
        }
      };

      // deserialize log record and get real key
      let log_record_pos = decode_log_record_pos(log_record.value);
      self.index.put(log_record.key, log_record_pos);

      offset += size as u64;
    }

    Ok(())
  }
}

fn get_merge_path<P>(dir_path: P) -> PathBuf
where
  P: AsRef<Path>,
{
  let file_name = dir_path.as_ref().file_name().unwrap();
  let merge_name = format!("{}-{}", file_name.to_str().unwrap(), MERGE_DIR_NAME);
  let parent = dir_path.as_ref().parent().unwrap();
  parent.to_path_buf().join(merge_name)
}

// load merge files
pub(crate) fn load_merge_files<P>(dir_path: P) -> Result<()>
where
  P: AsRef<Path>,
{
  let merge_path = get_merge_path(&dir_path);
  // merge never happened, just return
  if !merge_path.is_dir() {
    return Ok(());
  }

  let dir = match fs::read_dir(&merge_path) {
    Ok(dir) => dir,
    Err(e) => {
      error!("fail to read merge dir: {}", e);
      return Err(Errors::FailedToReadDatabaseDir);
    }
  };

  // check if merge finished file exists
  let mut merge_file_names = Vec::new();
  let mut merge_finished = false;
  for file in dir.flatten() {
    let file_os_str = file.file_name();
    let file_name = file_os_str.to_str().unwrap();

    if file_name.ends_with(MERGE_FINISHED_FILE_NAME) {
      merge_finished = true;
    }

    if file_name.ends_with(SEQ_NO_FILE_NAME) {
      continue;
    }

    if file_name.ends_with(FILE_LOCK_NAME) {
      continue;
    }

    // data file volume is 0 and ends with .data, just skip
    let meta = file.metadata().unwrap();
    if file_name.ends_with(DATA_FILE_NAME_SUFFIX) && meta.len() == 0 {
      continue;
    }

    merge_file_names.push(file.file_name());
  }

  // if merge doesn't finish, remove merge dir and return
  if !merge_finished {
    fs::remove_dir_all(merge_path.clone()).unwrap();
    return Ok(());
  }

  // open merge finished files, get the latest unmerged file id
  let merge_fin_file = DataFile::new_merge_fin_file(&merge_path)?;
  let merge_fin_record = merge_fin_file.read_log_record(0)?;
  let v = String::from_utf8(merge_fin_record.record.value).unwrap();
  let non_merge_file_id = v.parse::<u32>().unwrap();

  // remove old data files
  for fid in 0..non_merge_file_id {
    let file = get_data_file_name(&dir_path, fid);
    if file.is_file() {
      fs::remove_file(file).unwrap();
    }
  }

  // move temporary merge files to database dir
  for file_name in merge_file_names {
    let src_path = merge_path.join(&file_name);
    let dst_path = dir_path.as_ref().join(&file_name);
    fs::rename(src_path, dst_path).unwrap();
  }

  // remove merge dir
  fs::remove_dir_all(merge_path.clone()).unwrap();

  Ok(())
}

#[cfg(test)]
mod tests {
  use std::{sync::Arc, thread};

  use super::*;
  use crate::util::rand_kv::{get_test_key, get_test_value};
  use bytes::Bytes;

  #[test]
  fn test_merge_1() {
    // case for no data files
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitkv-rs-merge-1");
    opts.data_file_size = 32 * 1024 * 1024;

    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    let res1 = engine.merge();
    assert!(res1.is_ok());

    // delete tested files
    std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
  }

  #[test]
  fn test_merge_2() {
    // case for all valid data files
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitkv-rs-merge-2");
    opts.data_file_size = 32 * 1024 * 1024;
    opts.file_merge_threshold = 0 as f32;
    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    for i in 0..50000 {
      let put_res = engine.put(get_test_key(i), get_test_value(i));
      assert!(put_res.is_ok());
    }
    let res1 = engine.merge();
    assert!(res1.is_ok());

    // restart engine
    std::mem::drop(engine);

    let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
    let keys = engine2.list_keys().unwrap();
    assert_eq!(keys.len(), 50000);
    for i in 0..50000 {
      let get_res = engine2.get(get_test_key(i));
      assert!(get_res.ok().unwrap().len() > 0);
    }

    // delete tested files
    std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
  }

  #[test]
  fn test_merge_3() {
    // partial valid data files, and delete some data cases
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitkv-rs-merge-3");
    opts.data_file_size = 32 * 1024 * 1024;
    opts.file_merge_threshold = 0 as f32;
    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    for i in 0..50000 {
      let put_res = engine.put(get_test_key(i), get_test_value(i));
      assert!(put_res.is_ok());
    }

    for i in 0..10000 {
      let put_res = engine.put(get_test_key(i), Bytes::from("new value in merge"));
      assert!(put_res.is_ok());
    }

    for i in 40000..50000 {
      let del_res = engine.delete(get_test_key(i));
      assert!(del_res.is_ok());
    }

    let res1 = engine.merge();
    assert!(res1.is_ok());

    // restart engine
    std::mem::drop(engine);

    let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
    let keys = engine2.list_keys().unwrap();
    assert_eq!(keys.len(), 40000);

    for i in 0..10000 {
      let get_res = engine2.get(get_test_key(i));
      assert_eq!(Bytes::from("new value in merge"), get_res.ok().unwrap());
    }

    //delete tested files
    std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
  }

  #[test]
  fn test_merge_4() {
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitkv-rs-merge-4");
    opts.data_file_size = 32 * 1024 * 1024;
    opts.file_merge_threshold = 0 as f32;
    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    for i in 0..50000 {
      let put_res = engine.put(get_test_key(i), get_test_value(i));
      assert!(put_res.is_ok());
      let del_res = engine.delete(get_test_key(i));
      assert!(del_res.is_ok());
    }

    let res1 = engine.merge();
    assert!(res1.is_ok());

    // restart engine
    std::mem::drop(engine);

    let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
    let keys = engine2.list_keys().unwrap();
    assert_eq!(keys.len(), 0);

    for i in 0..50000 {
      let get_res = engine2.get(get_test_key(i));
      assert_eq!(Errors::KeyNotFound, get_res.err().unwrap());
    }

    // delete tested files
    std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
  }

  #[test]
  fn test_merge_5() {
    // write and delete process occurs when merging
    let mut opts = Options::default();
    opts.dir_path = PathBuf::from("/tmp/bitkv-rs-merge-5");
    opts.data_file_size = 32 * 1024 * 1024;
    opts.file_merge_threshold = 0 as f32;
    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    for i in 0..50000 {
      let put_res = engine.put(get_test_key(i), get_test_value(i));
      assert!(put_res.is_ok());
    }

    for i in 0..10000 {
      let put_res = engine.put(get_test_key(i), Bytes::from("new value in merge"));
      assert!(put_res.is_ok());
    }
    for i in 40000..50000 {
      let del_res = engine.delete(get_test_key(i));
      assert!(del_res.is_ok());
    }

    let eng = Arc::new(engine);

    let mut handles = vec![];
    let eng1 = eng.clone();
    let handle1 = thread::spawn(move || {
      for i in 60000..100000 {
        let put_res = eng1.put(get_test_key(i), get_test_value(i));
        assert!(put_res.is_ok());
      }
    });
    handles.push(handle1);

    let eng2 = eng.clone();
    let handle2 = thread::spawn(move || {
      let merge_res = eng2.merge();
      assert!(merge_res.is_ok());
    });
    handles.push(handle2);

    for handle in handles {
      handle.join().unwrap();
    }

    // restart engine
    std::mem::drop(eng);

    let engine2 = Engine::open(opts.clone()).expect("failed to open engine");
    let keys = engine2.list_keys().unwrap();

    assert_eq!(keys.len(), 80000);

    // delete tested files
    std::fs::remove_dir_all(opts.clone().dir_path).expect("failed to remove path");
  }
}
