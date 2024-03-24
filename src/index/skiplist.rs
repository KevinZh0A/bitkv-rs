use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::{data::log_record::LogRecordPos, errors::Result, option::IteratorOptions};

use super::{IndexIterator, Indexer};

// skiplist index
pub struct SkipList {
    skl: Arc<SkipMap<Vec<u8>, LogRecordPos>>,
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            skl: Arc::new(SkipMap::new()),
        }
    }
}

impl Indexer for SkipList {
    fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> bool {
        self.skl.insert(key, pos);
        true
    }

    fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
        if let Some(entry) = self.skl.get(&key) {
            return Some(*entry.value());
        }
        None
    }

    fn delete(&self, key: Vec<u8>) -> bool {
        let remove_res = self.skl.remove(&key);
        remove_res.is_some()
    }

    fn list_keys(&self) -> Result<Vec<Bytes>> {
        let mut keys = Vec::with_capacity(self.skl.len());
        for e in self.skl.iter() {
            keys.push(Bytes::copy_from_slice(e.key()));
        }
        Ok(keys)
    }

    fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
        let mut items = Vec::with_capacity(self.skl.len());

        // copy all items from SkipList to Vec
        for entry in self.skl.iter() {
            items.push((entry.key().clone(), entry.value().clone()));
        }

        if options.reverse {
            items.reverse();
        }

        Box::new(SkipListIterator {
            items,
            curr_index: 0,
            options,
        })
    }
}

/// SkipList Index Iterator
pub struct SkipListIterator {
    items: Vec<(Vec<u8>, LogRecordPos)>, // store key and index
    curr_index: usize,                   //current index
    options: IteratorOptions,            // iterator options
}

impl IndexIterator for SkipListIterator {
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

    use super::*;

    #[test]
    fn test_skl_put() {
        let skl = SkipList::new();
        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);

        let res2 = skl.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res2);

        let res3 = skl.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res3);

        let res4 = skl.put(
            "ddee".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res4);
    }

    #[test]
    fn test_skl_get() {
        let skl = SkipList::new();

        let res = skl.get(b"not exists".to_vec());
        assert!(res.is_none());

        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);

        let v1 = skl.get(b"aacd".to_vec());
        assert!(v1.is_some());

        let res2 = skl.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1233,
            },
        );
        assert!(res2);

        let v2 = skl.get(b"acdd".to_vec());
        assert!(v2.is_some());

        let res3 = skl.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1234,
            },
        );
        assert!(res3);

        let v3 = skl.get(b"aacd".to_vec());
        assert!(v3.is_some());

        let res4 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1235,
            },
        );
        assert!(res4);

        let v4 = skl.get(b"aacd".to_vec());
        assert!(v4.is_some());
    }

    #[test]
    fn test_skl_delete() {
        let skl = SkipList::new();

        let res = skl.delete(b"not exists".to_vec());
        assert!(!res);

        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);

        let r1 = skl.delete(b"aacd".to_vec());
        assert!(r1);

        let v1 = skl.get(b"aacd".to_vec());
        assert!(v1.is_none());
    }

    #[test]
    fn test_skl_list_keys() {
        let skl = SkipList::new();

        let keys = skl.list_keys().unwrap();
        assert!(keys.is_empty());

        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);

        let res2 = skl.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1233,
            },
        );
        assert!(res2);

        let res3 = skl.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1234,
            },
        );
        assert!(res3);

        let keys = skl.list_keys().unwrap();
        assert_eq!(keys.len(), 3);
    }

    #[test]
    fn test_skl_iterator() {
        let skl = SkipList::new();

        let res1 = skl.put(
            "aacd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1232,
            },
        );
        assert!(res1);

        let res2 = skl.put(
            "acdd".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1233,
            },
        );
        assert!(res2);

        let res3 = skl.put(
            "bbae".as_bytes().to_vec(),
            LogRecordPos {
                file_id: 1123,
                offset: 1234,
            },
        );
        assert!(res3);
        let mut opt = IteratorOptions::default();
        opt.reverse = true;
        let mut iter1 = skl.iterator(opt);
        while let Some((key, _)) = iter1.next() {
            assert!(!key.is_empty());
        }
    }
}
