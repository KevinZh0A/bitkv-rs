use bytes::Bytes;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::{db::Engine, errors::Result, index::IndexIterator, option::IteratorOptions};

/// Iterator interface
pub struct Iterator<'a> {
    index_iter: Arc<RwLock<Box<dyn IndexIterator>>>, // index iterator
    engine: &'a Engine,
}

impl Engine {
    /// Create a new iterator
    pub fn iter(&self, options: IteratorOptions) -> Iterator {
        Iterator {
            index_iter: Arc::new(RwLock::new(self.index.iterator(options))),
            engine: self,
        }
    }

    /// list all keys in db
    pub fn list_keys(&self) -> Result<Vec<Bytes>> {
        self.index.list_keys()
    }

    /// operate on all key-value pairs in db, finish when `f` returns false
    pub fn fold<F>(&self, f: F) -> Result<()>
    where
        Self: Sized,
        F: Fn(Bytes, Bytes) -> bool,
    {
        let iter = self.iter(IteratorOptions::default());
        while let Some((key, value)) = iter.next() {
            if !f(key, value) {
                break;
            }
        }
        Ok(())
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

    #[test]
    fn test_iterator_fold() {
        let mut opt = Options::default();
        opt.dir_path = PathBuf::from("/tmp/bitkv-rs-iter-fold");
        opt.data_file_size = 64 * 1024 * 1024; // 64MB
        let engine = Engine::open(opt.clone()).expect("fail to open engine");

        let put_res1 = engine.put(
            Bytes::from("eecc".as_bytes().to_vec()),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let put_res2 = engine.put(
            Bytes::from("aade".as_bytes().to_vec()),
            util::rand_kv::get_test_value(11),
        );
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(
            Bytes::from("ddce".as_bytes().to_vec()),
            util::rand_kv::get_test_value(12),
        );
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(
            Bytes::from("bbcc".as_bytes().to_vec()),
            util::rand_kv::get_test_value(13),
        );
        assert!(put_res4.is_ok());

        engine
            .fold(|key, value| {
                assert!(key.len() > 0);
                assert!(value.len() > 0);
                true
            })
            .unwrap();

        // delete tested files
        std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_iterator_list_keys() {
        let mut opt = Options::default();
        opt.dir_path = PathBuf::from("/tmp/bitkv-rs-iter-list_keys");
        opt.data_file_size = 64 * 1024 * 1024; // 64MB
        let engine = Engine::open(opt.clone()).expect("fail to open engine");

        let keys1 = engine.list_keys();
        assert_eq!(keys1.ok().unwrap().len(), 0);

        let put_res1 = engine.put(
            Bytes::from("aaccc".as_bytes().to_vec()),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let put_res2 = engine.put(
            Bytes::from("eecc".as_bytes().to_vec()),
            util::rand_kv::get_test_value(11),
        );
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(
            Bytes::from("bbac".as_bytes().to_vec()),
            util::rand_kv::get_test_value(12),
        );
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(
            Bytes::from("ccde".as_bytes().to_vec()),
            util::rand_kv::get_test_value(13),
        );
        assert!(put_res4.is_ok());

        let keys2 = engine.list_keys();
        assert_eq!(keys2.ok().unwrap().len(), 4);

        // delete tested files
        std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_iterator_seek() {
        let mut opt = Options::default();
        opt.dir_path = PathBuf::from("/tmp/bitkv-rs-iter-seek");
        opt.data_file_size = 64 * 1024 * 1024; // 64MB
        let engine = Engine::open(opt.clone()).expect("fail to open engine");

        // no items
        let iter1 = engine.iter(IteratorOptions::default());
        iter1.seek("aa".as_bytes().to_vec());
        assert!(iter1.next().is_none());

        // put one item
        let put_res1 = engine.put(
            Bytes::from("aaccc".as_bytes().to_vec()),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let iter2 = engine.iter(IteratorOptions::default());
        iter2.seek("a".as_bytes().to_vec());
        assert!(iter2.next().is_some());

        // multiple items

        let put_res2 = engine.put(
            Bytes::from("eecc".as_bytes().to_vec()),
            util::rand_kv::get_test_value(11),
        );
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(
            Bytes::from("bbac".as_bytes().to_vec()),
            util::rand_kv::get_test_value(12),
        );
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(
            Bytes::from("ccde".as_bytes().to_vec()),
            util::rand_kv::get_test_value(13),
        );
        assert!(put_res4.is_ok());

        let iter3 = engine.iter(IteratorOptions::default());
        iter3.seek("a".as_bytes().to_vec());
        assert_eq!(Bytes::from("aaccc"), iter3.next().unwrap().0);

        // delete tested files
        std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_iterator_next() {
        let mut opt = Options::default();
        opt.dir_path = PathBuf::from("/tmp/bitkv-rs-iter-next");
        opt.data_file_size = 64 * 1024 * 1024; // 64MB
        let engine = Engine::open(opt.clone()).expect("fail to open engine");

        // put one item
        let put_res1 = engine.put(
            Bytes::from("eecc".as_bytes().to_vec()),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let iter1 = engine.iter(IteratorOptions::default());
        iter1.seek("a".as_bytes().to_vec());
        assert!(iter1.next().is_some());
        iter1.rewind();
        assert!(iter1.next().is_some());
        assert!(iter1.next().is_none());

        // multiple items
        let put_res2 = engine.put(
            Bytes::from("aade".as_bytes().to_vec()),
            util::rand_kv::get_test_value(11),
        );
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(
            Bytes::from("ddce".as_bytes().to_vec()),
            util::rand_kv::get_test_value(12),
        );
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(
            Bytes::from("bbcc".as_bytes().to_vec()),
            util::rand_kv::get_test_value(13),
        );
        assert!(put_res4.is_ok());

        let mut iter_opt = IteratorOptions::default();
        iter_opt.reverse = true;
        let iter2 = engine.iter(iter_opt);
        while let Some(item) = iter2.next() {
            assert!(item.0.len() > 0);
        }

        // delete tested files
        std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
    }

    #[test]
    fn test_iterator_prefix() {
        let mut opt = Options::default();
        opt.dir_path = PathBuf::from("/tmp/bitkv-rs-iter-prefix");
        opt.data_file_size = 64 * 1024 * 1024; // 64MB
        let engine = Engine::open(opt.clone()).expect("fail to open engine");

        let put_res1 = engine.put(
            Bytes::from("eecc".as_bytes().to_vec()),
            util::rand_kv::get_test_value(10),
        );
        assert!(put_res1.is_ok());
        let put_res2 = engine.put(
            Bytes::from("aade".as_bytes().to_vec()),
            util::rand_kv::get_test_value(11),
        );
        assert!(put_res2.is_ok());
        let put_res3 = engine.put(
            Bytes::from("ddce".as_bytes().to_vec()),
            util::rand_kv::get_test_value(12),
        );
        assert!(put_res3.is_ok());
        let put_res4 = engine.put(
            Bytes::from("bbcc".as_bytes().to_vec()),
            util::rand_kv::get_test_value(13),
        );
        assert!(put_res4.is_ok());

        let mut iter_opt = IteratorOptions::default();
        iter_opt.prefix = "dd".as_bytes().to_vec();
        let iter1 = engine.iter(iter_opt);
        while let Some(item) = iter1.next() {
            assert!(item.0.len() > 0);
        }

        // delete tested files
        std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
    }
}
