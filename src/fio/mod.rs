pub mod file_io;

use std::path::PathBuf;

use crate::errors::Result;

use self::file_io::FileIO;

/// Abstract IO Management Interface, support different IO type implemented, currently standard IO file supported
pub trait IOManager: Sync + Send {
    /// read data from predetermined position
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// write bytes array into file
    fn write(&self, buf: &[u8]) -> Result<usize>;

    /// data persistence
    fn sync(&self) -> Result<()>;

    /// get file size
    fn size(&self) -> u64;
}

/// Initialize IO manager by filename
pub fn new_io_manager(filename: &PathBuf) -> Result<impl IOManager> {
    FileIO::new(filename)
}
