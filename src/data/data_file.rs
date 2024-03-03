use parking_lot::RwLock;
use std::{path::PathBuf, sync::Arc};

use super::log_record::LogRecord;
use crate::errors::Result;
use crate::fio::IOManager;

pub struct DataFile {
    file_id: Arc<RwLock<u32>>,      // data file id
    write_off: Arc<RwLock<u64>>, // current write offset, used for recording appending write position
    io_manager: Box<dyn IOManager>, // IO manager interface
}

impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<DataFile> {
        todo!()
    }

    pub fn get_write_off(&self) -> u64 {
        let read_guard = self.write_off.read();
        *read_guard
    }

    pub fn get_file_id(&self) -> u32 {
        let read_guard = self.file_id.read();
        *read_guard
    }

    pub fn read_log_record(&self, offset: u64) -> Result<LogRecord> {
        todo!()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize> {
        todo!()
    }

    pub fn sync(&self) -> Result<()> {
        todo!()
    }
}
