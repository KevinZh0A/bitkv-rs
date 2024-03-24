pub mod btree;
pub mod skiplist;
use bytes::Bytes;

use crate::option::IteratorOptions;
use crate::{data::log_record::LogRecordPos, errors::Result, option::IndexType};

use crate::index::btree::BTree;

// Abstract interface specifies methods for interchangeable indexing data structures
pub trait Indexer: Sync + Send {
    /// Store key's position into indexer
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;

    /// Retrieve key's position
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// Delete the position in indexer by key
    fn delete(&self, key: Vec<u8>) -> bool;

    /// List all keys in the indexer
    fn list_keys(&self) -> Result<Vec<Bytes>>;

    /// Create an iterator for the indexer
    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator>;
}

pub fn new_indexer(index_type: &IndexType) -> impl Indexer {
    match *index_type {
        IndexType::BTree => BTree::new(),
        IndexType::SkipList => todo!(),
    }
}

// Abstract interface specifies methods for interchangeable index iterators
pub trait IndexIterator: Sync + Send {
    // `Rewind` go back to the beginning of the iterator
    fn rewind(&mut self);

    // `Seek` search for the first entry with a key greater than or equal to the given key
    fn seek(&mut self, key: Vec<u8>);

    // `Next` move to the next entry, when the iterator is exhausted, return None
    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)>;
}
