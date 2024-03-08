use crate::{
    data::{
        data_file::{DataFile, DATA_FILE_NAME_SUFFIX},
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    errors::{Errors, Result},
    index,
    option::Options,
};
use bytes::Bytes;
use log::warn;
use parking_lot::RwLock;
use std::{collections::HashMap, fs, path::PathBuf, sync::Arc};

const INITIAL_FILE_ID: u32 = 0;

// Storage Engine
pub struct Engine {
    options: Arc<Options>,
    active_data_file: Arc<RwLock<DataFile>>, // current active data file
    old_data_files: Arc<RwLock<HashMap<u32, DataFile>>>, // old data files
    index: Box<dyn index::Indexer>,          // data cache index
    file_ids: Vec<u32>, // database setup file id list, only used for setup, not allowed to be modified or updated somewhere else
}

impl Engine {
    /// open bitkv storage engine instance
    pub fn open(opts: Options) -> Result<Self> {
        // check user options
        if let Some(e) = check_options(&opts) {
            return Err(e);
        };

        let options = Arc::new(opts);
        // determine if dir is valid, dir does not exist, create a new one
        let dir_path = &options.dir_path;
        if !dir_path.is_dir() {
            if let Err(e) = fs::create_dir(dir_path.as_path()) {
                warn!("failed to create database directory error: {}", e);
                return Err(Errors::FailedToCreateDatabaseDir);
            };
        }

        // load data file
        let mut data_files = load_data_files(&dir_path)?;

        // set file id info
        let mut file_ids = Vec::new();
        for v in data_files.iter() {
            file_ids.push(v.get_file_id());
        }

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
        let engine = Self {
            options: options.clone(),
            active_data_file: Arc::new(RwLock::new(active_file)),
            old_data_files: Arc::new(RwLock::new(older_files)),
            index: Box::new(index::new_indexer(&options.index_type)),
            file_ids,
        };

        // load index from data files
        engine.load_index_from_data_files()?;

        Ok(engine)
    }

    /// store a key/value pair, ensuring key isn't null.
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<()> {
        // if the key is valid
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // construct LogRecord
        let mut record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NORMAL,
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
    pub fn delete(&self, key: Bytes, value: Bytes) -> Result<()> {
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
            key: key.to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::DELETED,
        };

        // appending write to active file
        let log_record_pos = self.append_log_record(&mut record)?;

        // delete key in index
        let ok = self.index.delete(key.to_vec());
        if !ok {
            return Err(Errors::IndexUpdateFailed);
        }
        Ok(())
    }

    /// Retrieves the data associated with the specified key.
    pub fn get(&self, key: Bytes) -> Result<Bytes> {
        // if the key is valid
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
        let log_record_pos = pos.unwrap();
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
        if let LogRecordType::DELETED = log_record.rec_type {
            return Err(Errors::KeyNotFound);
        };

        // return corresponding value
        Ok(log_record.value.into())
    }

    /// append write data to current active data file
    fn append_log_record(&self, log_record: &mut LogRecord) -> Result<LogRecordPos> {
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

        // construct log record

        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
        })
    }

    /// load memory index from data files
    /// tranverse all data files, and process each log record
    fn load_index_from_data_files(&self) -> Result<()> {
        // if data_files is empty then return
        if self.file_ids.is_empty() {
            return Ok(());
        }
        let active_file = self.active_data_file.read();
        let old_files = self.old_data_files.read();

        // tranverse each file_id, retrieve data file and load its data
        for (i, file_id) in self.file_ids.iter().enumerate() {
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

                let (log_record, size) = match log_record_res {
                    Ok(result) => (result.record, result.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEOF {
                            break;
                        }
                        return Err(e);
                    }
                };

                // construct mem index
                let lof_record_pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                };

                let ok = match log_record.rec_type {
                    LogRecordType::NORMAL => {
                        self.index.put(log_record.key.to_vec(), lof_record_pos)
                    }
                    LogRecordType::DELETED => self.index.delete(log_record.key),
                };

                if !ok {
                    return Err(Errors::IndexUpdateFailed);
                }

                // offset move, read next log record
                offset += size;
            }

            // set active file offset
            if i == self.file_ids.len() - 1 {
                active_file.set_write_off(offset);
            }
        }
        Ok(())
    }
}

// load data files from database directory
fn load_data_files(dir_path: &PathBuf) -> Result<Vec<DataFile>> {
    // read database directory
    let dir = fs::read_dir(dir_path);
    if dir.is_err() {
        return Err(Errors::FailedToReadDatabaseDir);
    }

    let mut file_ids: Vec<u32> = Vec::new();
    let mut data_files: Vec<DataFile> = Vec::new();

    for file in dir.unwrap() {
        if let Ok(entry) = file {
            // Retrieve file name
            let file_os_str = entry.file_name();
            let file_name = file_os_str.to_str().unwrap();

            // determine if file name ends up with .data
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                let splited_names: Vec<&str> = file_name.split(".").collect();
                let file_id = match splited_names[0].parse::<u32>() {
                    Ok(fid) => fid,
                    Err(_) => {
                        return Err(Errors::DatabaseDirectoryCorrupted);
                    }
                };

                file_ids.push(file_id);
            }
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
        let data_file = DataFile::new(dir_path, *file_id)?;
        data_files.push(data_file);
    }
    Ok(data_files)
}

fn check_options(opts: &Options) -> Option<Errors> {
    let dir_path = opts.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().is_empty() {
        return Some(Errors::DirPathIsEmpty);
    }

    if opts.data_file_size <= 0 {
        return Some(Errors::DataFileSizeTooSmall);
    }

    None
}
