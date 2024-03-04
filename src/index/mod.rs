pub mod btree;


use crate::{data::log_record::LogRecordPos, option::IndexType};

use crate::index::btree::BTree;

// Abstract interface specifies methods for interchangeable indexing data structures
pub trait Indexer {
    /// Store key's position into indexer
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool;

    /// Retrieve key's position
    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos>;

    /// Delete the position in indexer by key
    fn delete(&self, key: Vec<u8>) -> bool;
}

pub fn new_indexer(index_type: IndexType) -> impl Indexer {
    match index_type {
        IndexType::BTree => BTree::new(),
        IndexType::SkipList => todo!(),
        _ => panic!("unsupported index type"),
    }
}
