use parking_lot::RwLock;
use std::{path::PathBuf, sync::Arc};

use super::log_record::ReadLogRecord;
use crate::errors::Result;
use crate::fio::IOManager;

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

pub struct DataFile {
    file_id: Arc<RwLock<u32>>,      // data file id
    write_off: Arc<RwLock<u64>>, // current write offset, used for recording appending write position
    io_manager: Box<dyn IOManager>, // IO manager interface
}

impl DataFile {
    pub fn new(dir_path: &PathBuf, file_id: u32) -> Result<DataFile> {
        todo!()
    }

    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }

    pub fn set_write_off(&self, offset: u64) {
        let mut write_guard = self.write_off.write();
        *write_guard = offset;
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    pub fn read_log_record(&self, offset: u64) -> Result<ReadLogRecord> {
        todo!()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}
