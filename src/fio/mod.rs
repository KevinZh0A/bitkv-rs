pub mod file_io;

use crate::errors::Result;

/// Abstract IO Management Interface, support different IO type implemented, currently standard IO file supported
pub trait IOManager: Sync + Send {
    /// read data from predetermined position
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize>;

    /// write bytes array into file
    fn write(&self, buf: &[u8]) -> Result<usize>;

    /// data persistence
    fn sync(&self) -> Result<()>;
}
