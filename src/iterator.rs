use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::{
    db::Engine,
    index::{self, IndexIterator},
    option::IteratorOptions,
};

/// Iterator interface
pub struct Iterator<'a> {
    index_iter: Arc<RwLock<Box<dyn IndexIterator>>>, // index iterator
    engine: &'a Engine,
}

impl Engine {
    /// Create a new iterator
    fn iter(&self, options: IteratorOptions) -> Iterator {
        Iterator {
            index_iter: Arc::new(RwLock::new(self.index.iterator(options))),
            engine: self,
        }
    }
}

impl Iterator<'_> {
    // `Rewind` go back to the beginning of the iterator
    pub fn rewind(&self) {
        let mut index_iter = self.index_iter.write();
        index_iter.rewind();
    }

    // `Seek` search for the first entry with a key greater than or equal to the given key
    pub fn seek(&self, key: Vec<u8>) {
        let mut index_iter = self.index_iter.write();
        index_iter.seek(key);
    }

    // `Next` move to the next entry, when the iterator is exhausted, return None
    pub fn next(&self) -> Option<(Bytes, Bytes)> {
        let mut index_iter = self.index_iter.write();
        if let Some(item) = index_iter.next() {
            let val = self
                .engine
                .get_value_by_position(&item.1)
                .expect("failed to get value from data file");
            return Some((Bytes::from(item.0.to_vec()), val));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::{option::Options, util};

    use super::*;

    // #[test]
    // #[ignore]
    // fn test_iterator_seek() {
    //     let mut opt = Options::default();
    //     opt.dir_path = PathBuf::from("/tmp/bitkv-rs-iter-seek");
    //     opt.data_file_size = 64 * 1024 * 1024; // 64MB
    //     let engine = Engine::open(opt).expect("fail to open engine");

    //     // no items
    //     let iter1 = engine.iter(IteratorOptions::default());
    //     iter1.seek("aa".as_bytes().to_vec());
    //     assert!(iter1.next().is_none());

    //     // put one item
    //     let put_res1 = engine.put(
    //         Bytes::from("aaccc".as_bytes().to_vec()),
    //         util::rand_kv::get_test_value(10),
    //     );
    //     assert!(put_res1.is_ok());
    // }
}
