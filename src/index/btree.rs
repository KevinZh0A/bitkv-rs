use crate::data::log_record::LogRecordPos;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

use super::Indexer;

// BTree Indexer, primarily encapsulates the 'BTreeMap' from std, is used for efficiently storing and querying data in sorted manner,
// allowing for fast retrieval,insertion,and deletion of items based on their keys.
pub struct BTree {
    tree: Arc<RwLock<BTreeMap<Vec<u8>, LogRecordPos>>>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl Indexer for BTree {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        let mut write_guard = self.tree.write();
        write_guard.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        let read_guard = self.tree.read();
        read_guard.get(&key).copied()
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let mut write_guard = self.tree.write();
        let remove_res = write_guard.remove(&key);
        remove_res.is_some()
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_btree_put() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert!(res1);

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
            },
        );
        assert!(res2);
    }

    #[test]
    fn test_get() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert!(res1);

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
            },
        );
        assert!(res2);

        let pos1 = bt.get("".as_bytes().to_vec()).unwrap();
        println!("{:?}", pos1);
        assert_eq!(
            pos1,
            LogRecordPos {
                file_id: 1,
                offset: 10
            }
        );

        let pos2 = bt.get("aa".as_bytes().to_vec()).unwrap();
        assert_eq!(
            pos2,
            LogRecordPos {
                file_id: 11,
                offset: 22
            }
        );
    }

    #[test]
    fn test_delete() {
        let bt = BTree::new();
        let res1 = bt.put(
            "".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1,
                offset: 10,
            },
        );
        assert!(res1);

        let res2 = bt.put(
            "aa".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 11,
                offset: 22,
            },
        );
        assert!(res2);

        let res3 = bt.delete("".as_bytes().to_vec());
        assert!(res3);

        let res4 = bt.delete("aa".as_bytes().to_vec());
        assert!(res4);

        let res5 = bt.delete("".as_bytes().to_vec());
        assert!(!res5)
    }
}
