use crate::{data::log_record::LogRecordPos, errors::Result, option::IteratorOptions};
use bytes::Bytes;
use parking_lot::RwLock;
use std::{collections::BTreeMap, sync::Arc};

use super::{IndexIterator, Indexer};

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

#[allow(clippy::clone_on_copy)]
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

  fn list_keys(&self) -> Result<Vec<Bytes>> {
    let read_guard = self.tree.read();
    let mut keys = Vec::with_capacity(read_guard.len());

    for (k, _) in read_guard.iter() {
      keys.push(Bytes::copy_from_slice(k));
    }
    Ok(keys)
  }

  fn iterator(&self, options: IteratorOptions) -> Box<dyn IndexIterator> {
    let read_guard = self.tree.read();
    let mut items = Vec::with_capacity(read_guard.len());

    // copy all items from BTreeMap to Vec
    for (key, value) in read_guard.iter() {
      items.push((key.clone(), value.clone()));
    }

    if options.reverse {
      items.reverse();
    }

    Box::new(BTreeIterator {
      items,
      curr_index: 0,
      options,
    })
  }
}

/// BTree Index Iterator
pub struct BTreeIterator {
  items: Vec<(Vec<u8>, LogRecordPos)>, // store key and index
  curr_index: usize,                   //current index
  options: IteratorOptions,            // iterator options
}

impl IndexIterator for BTreeIterator {
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

  #[test]
  fn test_btree_iterator_seek() {
    let bt = BTree::new();

    // no items
    let mut iter1 = bt.iterator(IteratorOptions::default());
    iter1.seek("aa".as_bytes().to_vec());
    let res1 = iter1.next();
    assert!(res1.is_none());

    // one item
    bt.put(
      "acde".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1,
        offset: 10,
      },
    );
    let mut iter2 = bt.iterator(IteratorOptions::default());
    iter2.seek("aa".as_bytes().to_vec());
    let res2 = iter2.next();
    assert!(res2.is_some());

    let mut iter3 = bt.iterator(IteratorOptions::default());
    iter3.seek("zz".as_bytes().to_vec());
    let res3 = iter3.next();
    assert!(res3.is_none());

    // multiple items
    bt.put(
      "bcde".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1,
        offset: 10,
      },
    );
    bt.put(
      "ccde".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1,
        offset: 10,
      },
    );
    bt.put(
      "dcde".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1,
        offset: 10,
      },
    );
    let mut iter4 = bt.iterator(IteratorOptions::default());
    iter4.seek("c".as_bytes().to_vec());
    while let Some(item) = iter4.next() {
      assert!(item.0.len() > 0);
    }

    let mut iter5 = bt.iterator(IteratorOptions::default());
    iter5.seek("ccde".as_bytes().to_vec());
    while let Some(item) = iter5.next() {
      assert!(item.0.len() > 0);
    }

    let mut iter6 = bt.iterator(IteratorOptions::default());
    iter6.seek("zz".as_bytes().to_vec());
    let res6 = iter6.next();
    assert!(res6.is_none());

    // reverse order
    let reverse = true;
    let mut iter7 = bt.iterator(IteratorOptions {
      reverse,
      ..Default::default()
    });
    iter7.seek("b".as_bytes().to_vec());
    while let Some(item) = iter7.next() {
      assert!(item.0.len() > 0);
    }
  }

  #[test]
  fn test_btree_iterator_next() {
    let bt = BTree::new();

    // no items
    let mut iter1 = bt.iterator(IteratorOptions::default());
    let res1 = iter1.next();
    assert!(res1.is_none());

    // one item
    bt.put(
      "acde".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1,
        offset: 10,
      },
    );
    let mut iter_opt1 = IteratorOptions::default();
    iter_opt1.reverse = true;
    let mut iter2 = bt.iterator(iter_opt1);
    assert!(iter2.next().is_some());

    // multiple items
    bt.put(
      "bcde".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1,
        offset: 10,
      },
    );
    bt.put(
      "ccde".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1,
        offset: 10,
      },
    );
    bt.put(
      "dcde".as_bytes().to_vec(),
      LogRecordPos {
        file_id: 1,
        offset: 10,
      },
    );
    let mut iter_opt2 = IteratorOptions::default();
    iter_opt2.reverse = true;
    let mut iter3 = bt.iterator(iter_opt2);
    while let Some(item) = iter3.next() {
      assert!(item.0.len() > 0);
    }

    // prefix filter
    let mut iter_opt3 = IteratorOptions::default();
    iter_opt3.prefix = "c".as_bytes().to_vec();
    let mut iter4 = bt.iterator(iter_opt3);
    while let Some(item) = iter4.next() {
      // assert!(item.0.len() > 0);
      println!("{:?}", String::from_utf8(item.0.to_vec()));
    }
  }
}
