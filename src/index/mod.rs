pub mod btree;

use crate::data::log_record::LogRecordPos;
use std::io::Result;

// Abstract Indexer interface, to be implemented pluggable data structure
pub trait Indexer {
    /// Store key's position into indexer
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Result<()>;

    /// Retrieve key's position
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// Delete the position in indexer by key
    fn delete(&self, key: Vec<u8>) -> bool;
}
