use super::IOManager;

use crate::errors::{Errors, Result};
use log::error;
use parking_lot::RwLock;
use std::{
  fs::{File, OpenOptions},
  io::Write,
  os::unix::fs::FileExt,
  path::Path,
  sync::Arc,
};

/// FileIO standard system file I/O
pub struct FileIO {
  fd: Arc<RwLock<File>>, //system file descriptor
}

impl FileIO {
  pub fn new<P>(file_name: P) -> Result<Self>
  where
    P: AsRef<Path>,
  {
    match OpenOptions::new()
      .create(true)
      .read(true)
      .append(true)
      .open(file_name)
    {
      Ok(file) => Ok(FileIO {
        fd: Arc::new(RwLock::new(file)),
      }),
      Err(e) => {
        error!("failed to open data file error: {}", e);
        Err(Errors::FailedToOpenDataFile)
      }
    }
  }
}

impl IOManager for FileIO {
  fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
    let read_guard = self.fd.read();
    match read_guard.read_at(buf, offset) {
      Ok(n) => Ok(n),
      Err(e) => {
        error!("read from date file error: {}", e);
        Err(Errors::FailedToReadFromDataFile)
      }
    }
  }

  fn write(&self, buf: &[u8]) -> Result<usize> {
    let mut write_guard = self.fd.write();
    match write_guard.write(buf) {
      Ok(n) => Ok(n),
      Err(e) => {
        error!("write to data file error: {}", e);
        Err(Errors::FailedToWriteToDataFile)
      }
    }
  }

  fn sync(&self) -> Result<()> {
    let read_guard = self.fd.read();
    if let Err(e) = read_guard.sync_all() {
      error!("failed to sync data file err: {}", e);
      return Err(Errors::FailedToSyncToDataFile);
    }
    Ok(())
  }

  fn size(&self) -> u64 {
    let read_guard = self.fd.read();
    read_guard.metadata().unwrap().len()
  }
}

#[cfg(test)]
mod tests {
  use std::{fs, path::PathBuf};

  use super::*;

  #[test]
  fn test_file_io_write() {
    let path = PathBuf::from("/tmp/a.data");
    let fio_res = FileIO::new(&path);
    assert!(fio_res.is_ok());

    let fio = fio_res.ok().unwrap();
    let res1 = fio.write("key-a".as_bytes());
    assert!(res1.is_ok());
    assert_eq!(5, res1.ok().unwrap());

    let res2 = fio.write("key-b".as_bytes());
    assert!(res2.is_ok());
    assert_eq!(5, res2.ok().unwrap());

    let res3 = fs::remove_file(path);
    assert!(res3.is_ok());
  }

  #[test]
  fn test_file_io_read() {
    let path = PathBuf::from("/tmp/b.data");
    let fio_res = FileIO::new(&path);
    assert!(fio_res.is_ok());

    let fio = fio_res.ok().unwrap();
    let res1 = fio.write("key-a".as_bytes());
    assert!(res1.is_ok());
    assert_eq!(5, res1.ok().unwrap());

    let res2 = fio.write("key-b".as_bytes());
    assert!(res2.is_ok());
    assert_eq!(5, res2.ok().unwrap());

    let mut buf = [0u8; 5];
    let read_res1 = fio.read(&mut buf, 0);
    assert!(read_res1.is_ok());
    assert_eq!(5, read_res1.ok().unwrap());

    let mut buf2 = [0u8; 5];
    let read_res2 = fio.read(&mut buf2, 5);
    assert!(read_res2.is_ok());
    assert_eq!(5, read_res2.ok().unwrap());

    let res3 = fs::remove_file(path.clone());
    assert!(res3.is_ok());
  }

  #[test]
  fn test_file_io_sync() {
    let path = PathBuf::from("/tmp/c.data");
    let fio_res = FileIO::new(&path);
    assert!(fio_res.is_ok());

    let fio = fio_res.ok().unwrap();
    let res1 = fio.write("key-a".as_bytes());
    assert!(res1.is_ok());
    assert_eq!(5, res1.ok().unwrap());

    let res2 = fio.write("key-b".as_bytes());
    assert!(res2.is_ok());
    assert_eq!(5, res2.ok().unwrap());

    let sync_res = fio.sync();
    assert!(sync_res.is_ok());

    let res3 = fs::remove_file(path);
    assert!(res3.is_ok());
  }

  #[test]
  fn test_file_io_size() {
    let path = PathBuf::from("/tmp/d.data");
    let fio_res = FileIO::new(&path);
    assert!(fio_res.is_ok());

    let fio = fio_res.ok().unwrap();
    let res1 = fio.write("key-a".as_bytes());
    assert!(res1.is_ok());
    assert_eq!(5, res1.ok().unwrap());

    let res2 = fio.write("key-b".as_bytes());
    assert!(res2.is_ok());
    assert_eq!(5, res2.ok().unwrap());

    let size = fio.size();
    assert_eq!(10, size);

    let res3 = fs::remove_file(path);
    assert!(res3.is_ok());
  }
}
