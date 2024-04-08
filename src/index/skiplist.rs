#![allow(clippy::clone_on_copy)]
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
  fn put(&self, key: Vec<u8>, pos: LogRecordPos) -> Option<LogRecordPos> {
    let mut result = None;
    if let Some(entry) = self.skl.get(&key) {
      result = Some(*entry.value());
    }

    self.skl.insert(key, pos);
    result
  }

  fn get(&self, key: Vec<u8>) -> Option<LogRecordPos> {
    if let Some(entry) = self.skl.get(&key) {
      return Some(*entry.value());
    }
    None
  }

  fn delete(&self, key: Vec<u8>) -> Option<LogRecordPos> {
    if let Some(entry) = self.skl.remove(&key) {
      return Some(*entry.value());
    }
    None
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
      if prefix.is_empty() || item.0.starts_with(prefix) {
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
        size: 12,
      },
    );
    assert!(res1.is_none());

    let res2 = skl.put(
      "acdd".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1232,
        size: 12,
      },
    );
    assert!(res2.is_none());

    let res3 = skl.put(
      "bbae".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1232,
        size: 12,
      },
    );
    assert!(res3.is_none());

    let res4 = skl.put(
      "ddee".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1232,
        size: 12,
      },
    );
    assert!(res4.is_none());

    let res5 = skl.put(
      "aacd".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1232,
        size: 12,
      },
    );
    assert!(res5.is_some());
    let v1 = res5.unwrap();
    assert_eq!(
      v1,
      LogRecordPos {
        file_id: 1123,
        offset: 1232,
        size: 12,
      }
    );
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
        size: 12,
      },
    );
    assert!(res1.is_none());

    let v1 = skl.get(b"aacd".to_vec());
    assert!(v1.is_some());

    let res2 = skl.put(
      "acdd".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1233,
        size: 12,
      },
    );
    assert!(res2.is_none());

    let v2 = skl.get(b"acdd".to_vec());
    assert!(v2.is_some());

    let res3 = skl.put(
      "bbae".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1234,
        size: 12,
      },
    );
    assert!(res3.is_none());

    let v3 = skl.get(b"aacd".to_vec());
    assert!(v3.is_some());

    let res4 = skl.put(
      "aacd".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1235,
        size: 12,
      },
    );
    assert!(res4.is_some());
    let v1 = res4.unwrap();
    assert_eq!(
      v1,
      LogRecordPos {
        file_id: 1123,
        offset: 1232,
        size: 12,
      }
    );

    let v4 = skl.get(b"aacd".to_vec());
    assert!(v4.is_some());
  }

  #[test]
  fn test_skl_delete() {
    let skl = SkipList::new();

    let res = skl.delete(b"not exists".to_vec());
    assert!(res.is_none());

    let res1 = skl.put(
      "aacd".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1232,
        size: 12,
      },
    );
    assert!(res1.is_none());

    let r1 = skl.delete(b"aacd".to_vec());
    assert!(r1.is_some());
    let v1 = r1.unwrap();
    assert_eq!(
      v1,
      LogRecordPos {
        file_id: 1123,
        offset: 1232,
        size: 12,
      }
    );

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
        size: 12,
      },
    );
    assert!(res1.is_none());

    let res2 = skl.put(
      "acdd".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1233,
        size: 12,
      },
    );
    assert!(res2.is_none());

    let res3 = skl.put(
      "bbae".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1234,
        size: 12,
      },
    );
    assert!(res3.is_none());

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
        size: 12,
      },
    );
    assert!(res1.is_none());

    let res2 = skl.put(
      "acdd".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1233,
        size: 12,
      },
    );
    assert!(res2.is_none());

    let res3 = skl.put(
      "bbae".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1234,
        size: 12,
      },
    );
    assert!(res3.is_none());
    let mut opt = IteratorOptions::default();
    opt.reverse = true;
    let mut iter1 = skl.iterator(opt);
    while let Some((key, _)) = iter1.next() {
      assert!(!key.is_empty());
    }
  }

  #[test]
  fn test_skl_iterator_rewind() {
    let skl = SkipList::new();

    let res1 = skl.put(
      "aacd".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1232,
        size: 12,
      },
    );
    assert!(res1.is_none());

    let res2 = skl.put(
      "acdd".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1233,
        size: 12,
      },
    );
    assert!(res2.is_none());

    let res3 = skl.put(
      "bbae".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1234,
        size: 12,
      },
    );
    assert!(res3.is_none());

    let mut iter1 = skl.iterator(IteratorOptions::default());
    let mut iter2 = skl.iterator(IteratorOptions::default());
    iter1.next();
    iter1.next();
    iter1.rewind();
    iter2.next();
    iter2.next();
    iter2.rewind();
    let mut count = 0;
    while let Some((key, _)) = iter1.next() {
      count += 1;
      assert!(!key.is_empty());
    }
    assert_eq!(count, 3);
  }

  #[test]
  fn test_skl_iterator_seek() {
    let skl = SkipList::new();

    let res1 = skl.put(
      "aacd".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1232,
        size: 12,
      },
    );
    assert!(res1.is_none());

    let res2 = skl.put(
      "acdd".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1233,
        size: 12,
      },
    );
    assert!(res2.is_none());

    let res3 = skl.put(
      "bbae".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1234,
        size: 12,
      },
    );
    assert!(res3.is_none());

    let mut iter1 = skl.iterator(IteratorOptions::default());
    iter1.seek(b"acdd".to_vec());
    let mut count = 0;
    while let Some((key, _)) = iter1.next() {
      count += 1;
      assert!(!key.is_empty());
    }
    assert_eq!(count, 2);
  }

  #[test]
  fn test_bptree_iterator_next() {
    let skl = SkipList::new();

    let res1 = skl.put(
      "aacd".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1232,
        size: 12,
      },
    );
    assert!(res1.is_none());

    let res2 = skl.put(
      "acdd".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1233,
        size: 12,
      },
    );
    assert!(res2.is_none());

    let res3 = skl.put(
      "bbae".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1123,
        offset: 1234,
        size: 12,
      },
    );
    assert!(res3.is_none());

    let mut iter1 = skl.iterator(IteratorOptions::default());
    let mut count = 0;
    while let Some((key, _)) = iter1.next() {
      count += 1;
      assert!(!key.is_empty());
    }
    assert_eq!(count, 3);
  }
}
