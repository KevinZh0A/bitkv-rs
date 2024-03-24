use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::Bytes;
use jammdb::{Error, DB};

use crate::{
    data::log_record::{decode_log_record_pos, LogRecordPos},
    errors::Result,
    option::IteratorOptions,
};

use super::{IndexIterator, Indexer};

const BPTREE_INDEX_FILE_NAME: &str = "bptree-index";
const BPTREE_BUCKET_NAME: &str = "bitcask-index";

// B+ tree indexer implementation
pub struct BPlusTree {
    tree: Arc<DB>,
}

impl BPlusTree {
    pub fn new<P>(dir_path: P) -> Self
    where
        P: AsRef<Path>,
    {
        let bptree =
            DB::open(dir_path.as_ref().join(BPTREE_INDEX_FILE_NAME)).expect("fail to open b+ tree");
        let tree = Arc::new(bptree);
        let tx = tree.tx(true).expect("failed to begin tx");
        tx.get_or_create_bucket(BPTREE_BUCKET_NAME).unwrap();
        tx.commit().unwrap();
        tree.into()
    }
}

impl From<Arc<DB>> for BPlusTree {
    fn from(tree: Arc<DB>) -> Self {
        Self { tree }
    }
}

impl Indexer for BPlusTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let tx = self.tree.tx(true).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        bucket
            .put(key, pos.encode())
            .expect("failed to put k/v pair");
        tx.commit().unwrap();
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        if let Some(kv) = bucket.get_kv(key) {
            return Some(decode_log_record_pos(kv.value().to_vec()));
        }
        None
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let tx = self.tree.tx(true).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        if let Err(e) = bucket.delete(key) {
            if e == Error::KeyValueMissing {
                return false;
            }
        };
        tx.commit().unwrap();
        true
    }

    fn list_keys(&self) -> Result<Vec<Bytes>> {
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();
        let mut keys = Vec::new();

        for data in bucket.cursor() {
            keys.push(Bytes::copy_from_slice(data.key()));
        }
        Ok(keys)
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let mut items = Vec::new();
        let tx = self.tree.tx(false).expect("failed to begin tx");
        let bucket = tx.get_bucket(BPTREE_BUCKET_NAME).unwrap();

        for data in bucket.cursor() {
            let key = data.key().to_vec();
            let pos = decode_log_record_pos(data.kv().value().to_vec());
            items.push((key, pos));
        }
        if options.reverse {
            items.reverse();
        }
        Box::new(BPTreeIterator {
            items,
            curr_index: 0,
            options,
        })
    }
}

/// B+ tree Index Iterator
pub struct BPTreeIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>, // store key and index
    curr_index: usize,                   //current index
    options: IteratorOptions,            // iterator options
}

impl IndexIterator for BPTreeIterator {
    fn rewind(&mut self) {
        self.curr_index = 0;
    }

    fn seek(&mut self, key: Vec<u8>) {
        self.curr_index = match self.items.binary_search_by(|(x, _)| {
            if self.options.reverse {
                x.cmp(&key).reverse()
            } else {
                x.cmp(&key)
            }
        }) {
            Ok(equal_val) => equal_val,
            Err(insert_val) => insert_val,
        };
    }

    fn next(&mut self) -> Option<(&Vec<u8>, &LogRecordPos)> {
        if self.curr_index >= self.items.len() {
            return None;
        }

        while let Some(item) = self.items.get(self.curr_index) {
            self.curr_index += 1;
            let prefix = &self.options.prefix;
            if prefix.is_empty() || item.0.starts_with(&prefix) {
                return Some((&item.0, &item.1));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {

    use std::fs;

    use super::*;

    #[test]
    fn test_bptree_put() {
        let path = PathBuf::from("/tmp/bptree-put");
        fs::create_dir_all(&path).unwrap();
        let bptree = BPlusTree::new(&path);
        let res1 = bptree.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);

        let res2 = bptree.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res2);

        let res3 = bptree.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res3);

        let res4 = bptree.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res4);

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn test_bptree_get() {
        let path = PathBuf::from("/tmp/bptree-get");
        fs::create_dir_all(&path).unwrap();
        let bptree = BPlusTree::new(&path);

        let res = bptree.get(b"not exists".to_vec());
        assert!(res.is_none());

        let res1 = bptree.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);

        let v1 = bptree.get(b"aacd".to_vec());
        assert!(v1.is_some());

        let res2 = bptree.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1233,
            },
        );
        assert!(res2);

        let v2 = bptree.get(b"acdd".to_vec());
        assert!(v2.is_some());

        let res3 = bptree.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1234,
            },
        );
        assert!(res3);

        let v3 = bptree.get(b"aacd".to_vec());
        assert!(v3.is_some());

        let res4 = bptree.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1235,
            },
        );
        assert!(res4);

        let v4 = bptree.get(b"aacd".to_vec());
        assert!(v4.is_some());

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn test_bptree_delete() {
        let path = PathBuf::from("/tmp/bptree-delete");
        fs::create_dir_all(&path).unwrap();
        let bptree = BPlusTree::new(&path);

        let res = bptree.delete(b"not exists".to_vec());
        assert!(!res);

        let res1 = bptree.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);

        let r1 = bptree.delete(b"aacd".to_vec());
        assert!(r1);

        let v1 = bptree.get(b"aacd".to_vec());
        assert!(v1.is_none());

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn test_bptree_list_keys() {
        let path = PathBuf::from("/tmp/bptree-list-keys");
        fs::create_dir_all(&path).unwrap();
        let bptree = BPlusTree::new(&path);

        let keys = bptree.list_keys().unwrap();
        assert!(keys.is_empty());

        let res1 = bptree.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);

        let res2 = bptree.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1233,
            },
        );
        assert!(res2);

        let res3 = bptree.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1234,
            },
        );
        assert!(res3);

        let keys = bptree.list_keys().unwrap();
        assert_eq!(keys.len(), 3);

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn test_bptree_iterator() {
        let path = PathBuf::from("/tmp/bptree-iterator");
        fs::create_dir_all(&path).unwrap();
        let bptree = BPlusTree::new(&path);

        let res1 = bptree.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);

        let res2 = bptree.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1233,
            },
        );
        assert!(res2);

        let res3 = bptree.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1234,
            },
        );
        assert!(res3);
        let mut opt = IteratorOptions::default();
        opt.reverse = true;
        let mut iter1 = bptree.iterator(opt);
        while let Some((key, _)) = iter1.next() {
            assert!(!key.is_empty());
        }

        fs::remove_dir_all(path).unwrap();
    }
}
