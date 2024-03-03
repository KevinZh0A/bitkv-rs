use crate::{
    data::{
        data_file::DataFile,
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    errors::{Errors, Result},
    index::Indexer,
    option::Options,
};
use bytes::Bytes;
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};

// Storage Engine
pub struct Engine {
    options: Arc<Options>,
    active_data_file: Arc<RwLock<DataFile>>, // current active data file
    old_data_files: Arc<RwLock<HashMap<u32, DataFile>>>, // old data files
    index: Box<dyn Indexer>,                 // data cache index
}

impl Engine {
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
            true => active_file.read_log_record(log_record_pos.offset)?,
            false => {
                let data_file = oldre_files.get(&log_record_pos.file_id);
                if data_file.is_none() {
                    // Returns the error if the corresponding data file is not found.
                    return Err(Errors::DataFileNotFound);
                }
                data_file.unwrap().read_log_record(log_record_pos.offset)?
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
        let dir_path = self.options.dir_path.clone();

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
            let old_file = DataFile::new(dir_path.clone(), current_fid)?;
            old_files.insert(current_fid, old_file);

            // open a new active data file
            let new_file = DataFile::new(dir_path.clone(), current_fid + 1)?;
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
}
