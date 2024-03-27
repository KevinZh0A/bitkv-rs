pub mod file_io;
pub mod mmap;

use std::path::PathBuf;

use crate::{errors::Result, option::IOManagerType};

use self::{file_io::FileIO, mmap::MMapIO};

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
pub fn new_io_manager(filename: &PathBuf, io_type: &IOManagerType) -> Box<dyn IOManager> {
  match *io_type {
    IOManagerType::StandardFileIO => Box::new(FileIO::new(filename).unwrap()),
    IOManagerType::MemoryMap => Box::new(MMapIO::new(filename).unwrap()),
  }
}
