#![allow(clippy::redundant_closure)]
use crate::{
    batch::{log_record_key_with_seq, parse_log_record_key, NON_TXN_SEQ_NO},
    data::{
        data_file::{DataFile, DATA_FILE_NAME_SUFFIX, MERGE_FINISHED_FILE_NAME, SEQ_NO_FILE_NAME},
        log_record::{LogRecord, LogRecordPos, LogRecordType, TransactionRecord},
    },
    errors::{Errors, Result},
    index,
    merge::load_merge_files,
    option::{IndexType, Options},
};
use bytes::Bytes;
use log::{error, warn};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

const INITIAL_FILE_ID: u32 = 0;
const SEQ_NO_KEY: &str = "seq.no";

pub enum SeqNoExist {
    Yes(usize),
    None,
}

// Storage Engine
pub struct Engine {
    pub(crate) options: Arc<Options>,
    pub(crate) active_data_file: Arc<RwLock<DataFile>>, // current active data file
    pub(crate) old_data_files: Arc<RwLock<HashMap<u32, DataFile>>>, // old data files
    pub(crate) index: Box<dyn index::Indexer>,          // data cache index
    file_ids: Vec<u32>, // database setup file id list, only used for setup, not allowed to be modified or updated somewhere else
    pub(crate) batch_commit_lock: Mutex<()>, // txn commit lock ensure serializable
    pub(crate) seq_no: Arc<AtomicUsize>, // transaction sequence number
    pub(crate) merging_lock: Mutex<()>, // prevent multiple threads from merging data files at the same time
    pub(crate) seq_file_exists: bool,   // whether the seq_no file exists
    pub(crate) is_initial: bool,        // whether the engine is initialized
}

impl Engine {
    /// open bitkv storage engine instance
    pub fn open(opts: Options) -> Result<Self> {
        // check user options
        if let Some(e) = check_options(&opts) {
            return Err(e);
        };
        let mut is_initial = false;
        let options = Arc::new(opts);

        // determine if dir is valid, dir does not exist, create a new one
        let dir_path = &options.dir_path;
        if !dir_path.is_dir() {
            is_initial = true;
            if let Err(e) = fs::create_dir(dir_path.as_path()) {
                warn!("failed to create database directory error: {}", e);
                return Err(Errors::FailedToCreateDatabaseDir);
            };
        }
        let entry = fs::read_dir(dir_path).unwrap();
        if entry.count() == 0 {
            is_initial = true;
        }
        // load merge files
        load_merge_files(dir_path)?;

        // load data files
        let mut data_files = load_data_files(dir_path)?;

        // set file id info
        let mut file_ids = Vec::new();
        for v in data_files.iter() {
            file_ids.push(v.get_file_id());
        }
        // adjust file_ids order, let current file id in the first place
        data_files.reverse();

        // save old file into older_files
        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..=data_files.len() - 2 {
                let file = data_files.pop().unwrap();
                older_files.insert(file.get_file_id(), file);
            }
        }

        // Retrieve the active data file, which is the last one in the data_files
        let active_file = match data_files.pop() {
            Some(v) => v,
            None => DataFile::new(dir_path, INITIAL_FILE_ID)?,
        };

        // create a new engine instance
        let mut engine = Self {
            options: options.clone(),
            active_data_file: Arc::new(RwLock::new(active_file)),
            old_data_files: Arc::new(RwLock::new(older_files)),
            index: index::new_indexer(&options.index_type, &options.dir_path),
            file_ids,
            batch_commit_lock: Mutex::new(()),
            seq_no: Arc::new(AtomicUsize::new(1)),
            merging_lock: Mutex::new(()),
            seq_file_exists: false,
            is_initial,
        };

        // if not B+Tree index type, load index from hint file and data files
        println!("if not B+Tree index type, load index from hint file and data files");
        match engine.options.index_type {
            IndexType::BPlusTree => {
                // load seq_no from current transaction
                let (is_exists, seq_no) = engine.load_seq_no();
                if is_exists {
                    engine.seq_no.store(seq_no, Ordering::SeqCst);
                    engine.seq_file_exists = is_exists;
                }

                // update offset of active data file
                let active_file = engine.active_data_file.write();
                active_file.set_write_off(active_file.file_size());
            }
            _ => {
                // load index from hint file
                engine.load_index_from_hint_file()?;

                // load index from data files
                let curr_seq_no = engine.load_index_from_data_files()?;

                // update seq_no
                if curr_seq_no > 0 {
                    engine
                        .seq_no
                        .store(curr_seq_no + 1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        }

        Ok(engine)
    }

    /// close engine, release resources
    pub fn close(&self) -> Result<()> {
        // load seq_no from current transaction
        let seq_no_file = DataFile::new_seq_no_file(&self.options.dir_path)?;
        let seq_no = self.seq_no.load(Ordering::SeqCst);
        let record = LogRecord {
            key: SEQ_NO_KEY.as_bytes().to_vec(),
            value: seq_no.to_string().into(),
            rec_type: LogRecordType::Normal,
        };
        seq_no_file.write(&record.encode())?;
        seq_no_file.sync()?;

        let read_guard = self.active_data_file.read();
        read_guard.sync()
    }

    /// sync current active data file to disk
    pub fn sync(&self) -> Result<()> {
        let read_guard = self.active_data_file.read();
        read_guard.sync()
    }

    /// store a key/value pair, ensuring key isn't null.
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // if the key is valid
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // construct LogRecord
        let mut record = LogRecord {
            key: log_record_key_with_seq(key.to_vec(), NON_TXN_SEQ_NO),
            value: value.to_vec(),
            rec_type: LogRecordType::Normal,
        };

        // appending write to active file
        let log_record_pos = self.append_log_record(&mut record)?;

        // update index
        let ok = self.index.put(key.to_vec(), log_record_pos);
        if !ok {
            return Err(Errors::IndexUpdateFailed);
        }
        Ok(())
    }

    // delete the data associated with the specified key.
    pub fn delete(&self, key: Bytes) -> Result<()> {
        // if the key is valid
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // retrieve specified data from index if it not exists then return
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Ok(());
        }

        // construct LogRecord
        let mut record = LogRecord {
            key: log_record_key_with_seq(key.to_vec(), NON_TXN_SEQ_NO),
            value: Default::default(),
            rec_type: LogRecordType::Deleted,
        };

        // appending write to active file
        self.append_log_record(&mut record)?;

        // delete key in index
        let ok = self.index.delete(key.to_vec());
        if !ok {
            return Err(Errors::IndexUpdateFailed);
        }
        Ok(())
    }

    /// Retrieves the data associated with the specified key.
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        // if the key is empty then return
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // Retrieves data for the specified key from the in-memory index.
        let pos = self.index.get(key.to_vec());

        // if key not found then return
        if pos.is_none() {
            return Err(Errors::KeyNotFound);
        }

        // Retrieves LogRecord from the specified file data.
        self.get_value_by_position(&pos.unwrap())
    }

    /// Retrieves the data by position.
    pub(crate) fn get_value_by_position(&self, log_record_pos: &LogRecordPos) -> Result<Bytes> {
        // Retrieves LogRecord from the specified file data.
        let active_file = self.active_data_file.read();
        let oldre_files = self.old_data_files.read();
        let log_record = match active_file.get_file_id() == log_record_pos.file_id {
            true => active_file.read_log_record(log_record_pos.offset)?.record,
            false => {
                let data_file = oldre_files.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    // Returns the error if the corresponding data file is not found.
                    return Err(Errors::DataFileNotFound);
                }
                data_file
                    .unwrap()
                    .read_log_record(log_record_pos.offset)?
                    .record
            }
        };

        // Determines the type of the log record.
        if let LogRecordType::Deleted = log_record.rec_type {
            return Err(Errors::KeyNotFound);
        };

        // return corresponding value
        Ok(log_record.value.into())
    }

    /// append write data to current active data file
    pub(crate) fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
        let dir_path = &self.options.dir_path;

        // encode input data
        let enc_record = log_record.encode();
        let record_len = enc_record.len() as u64;

        // obtain current active file
        let mut active_file = self.active_data_file.write();
        if active_file.get_write_off() + record_len > self.options.data_file_size {
            // active file persistence
            active_file.sync()?;

            let current_fid = active_file.get_file_id();

            // insert old data file to hash map
            let mut old_files = self.old_data_files.write();
            let old_file = DataFile::new(dir_path, current_fid)?;
            old_files.insert(current_fid, old_file);

            // open a new active data file
            let new_file = DataFile::new(dir_path, current_fid + 1)?;
            *active_file = new_file;
        }

        // append write to active file
        let write_off = active_file.get_write_off();
        active_file.write(&enc_record)?;

        // options to sync or not
        if self.options.sync_writes {
            active_file.sync()?;
        }

        // construct log record return info
        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
        })
    }

    /// load memory index from data files
    /// tranverse all data files, and process each log record

    fn load_index_from_data_files(&self) -> Result<usize> {
        let mut current_seq_no = NON_TXN_SEQ_NO;
        // if data_files is empty then return
        if self.file_ids.is_empty() {
            return Ok(current_seq_no);
        }

        // get lastest unmerged file id
        let mut has_merged = false;
        let mut non_merge_fid = 0;
        let merge_fin_file = self.options.dir_path.join(MERGE_FINISHED_FILE_NAME);
        if merge_fin_file.is_file() {
            let merge_file = DataFile::new_merge_fin_file(&self.options.dir_path)?;
            let merge_fin_record = merge_file.read_log_record(0)?;
            let v = String::from_utf8(merge_fin_record.record.value).unwrap();

            non_merge_fid = v.parse::<u32>().unwrap();
            has_merged = true;
        }

        // temporary store data related to txn
        let mut transaction_records = HashMap::new();

        let active_file = self.active_data_file.read();
        let old_files = self.old_data_files.read();

        // tranverse each file_id, retrieve data file and load its data
        for (i, file_id) in self.file_ids.iter().enumerate() {
            // if file_id is less than non_merge_fid, then skip
            if has_merged && *file_id < non_merge_fid {
                continue;
            }

            let mut offset = 0;
            loop {
                // read data in loop
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read_log_record(offset),
                    _ => {
                        let data_file = old_files.get(file_id).unwrap();
                        data_file.read_log_record(offset)
                    }
                };

                let (mut log_record, size) = match log_record_res {
                    Ok(result) => (result.record, result.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEOF {
                            break;
                        }
                        return Err(e);
                    }
                };

                // construct mem index
                let log_record_pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                };

                // parse key, obtain actual key and seq_no
                let (real_key, seq_no) = parse_log_record_key(log_record.key.clone());
                // non txn log record, update index as usual
                if seq_no == NON_TXN_SEQ_NO {
                    self.update_index(real_key, log_record.rec_type, log_record_pos)?;
                } else {
                    // txn log record commit, update index
                    if log_record.rec_type == LogRecordType::TxnFinished {
                        let records: &Vec<TransactionRecord> =
                            transaction_records.get(&seq_no).unwrap();
                        for txn_record in records.iter() {
                            self.update_index(
                                txn_record.record.key.clone(),
                                txn_record.record.rec_type,
                                txn_record.pos,
                            )?;
                        }
                        transaction_records.remove(&seq_no);
                    } else {
                        log_record.key = real_key;
                        transaction_records
                            .entry(seq_no)
                            .or_insert_with(|| Vec::new())
                            .push(TransactionRecord {
                                record: log_record,
                                pos: log_record_pos,
                            });
                    }
                }

                // seq_no update
                if seq_no > current_seq_no {
                    current_seq_no = seq_no;
                }

                // offset move, read next log record
                offset += size as u64;
            }

            // set active file offset
            if i == self.file_ids.len() - 1 {
                active_file.set_write_off(offset);
            }
        }
        Ok(current_seq_no)
    }

    /// load seq_no under B+Tree index type
    fn load_seq_no(&self) -> (bool, usize) {
        let file_name = self.options.dir_path.join(SEQ_NO_FILE_NAME);
        if !file_name.is_file() {
            return (false, 0);
        }
        let seq_no_file = DataFile::new_seq_no_file(&self.options.dir_path).unwrap();
        let record = match seq_no_file.read_log_record(0) {
            Ok(res) => res.record,
            Err(e) => panic!("failed to read seq_no: {}", e),
        };
        let v = String::from_utf8(record.value).unwrap();
        let seq_no = v.parse::<usize>().unwrap();

        // remove seq_no file, avoiding repeated writing
        fs::remove_file(file_name).unwrap();

        (true, seq_no)
    }

    fn update_index(&self, key: Vec<u8>, rec_type: LogRecordType, pos: LogRecordPos) -> Result<()> {
        if rec_type == LogRecordType::Normal {
            self.index.put(key.clone(), pos);
        }

        if rec_type == LogRecordType::Deleted {
            self.index.delete(key);
        }
        Ok(())
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            error!("error while closing engine {}", e);
        }
    }
}

// load data files from database directory
fn load_data_files<P>(dir_path: P) -> Result<Vec<DataFile>>
where
    P: AsRef<Path>,
{
    // read database directory
    let dir = fs::read_dir(&dir_path);
    if dir.is_err() {
        return Err(Errors::FailedToReadDatabaseDir);
    }

    let mut file_ids: Vec<u32> = Vec::new();
    let mut data_files: Vec<DataFile> = Vec::new();

    for file in dir.unwrap().flatten() {
        // Retrieve file name
        let file_os_str = file.file_name();
        let file_name = file_os_str.to_str().unwrap();

        // determine if file name ends up with .data
        if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
            let splited_names: Vec<&str> = file_name.split('.').collect();
            let file_id = match splited_names[0].parse::<u32>() {
                Ok(fid) => fid,
                Err(_) => {
                    return Err(Errors::DatabaseDirectoryCorrupted);
                }
            };

            file_ids.push(file_id);
        }
    }

    // if data file is empty then return
    if file_ids.is_empty() {
        return Ok(data_files);
    }

    // sort file_ids, loading from small to large
    file_ids.sort();

    // traverse file_ids, sequentially loading data files
    for file_id in file_ids.iter() {
        let data_file = DataFile::new(&dir_path, *file_id)?;
        data_files.push(data_file);
    }
    Ok(data_files)
}

fn check_options(opts: &Options) -> Option<Errors> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().is_empty() {
        return Some(Errors::DirPathIsEmpty);
    }

    if opts.data_file_size == 0 {
        return Some(Errors::DataFileSizeTooSmall);
    }

    None
}
