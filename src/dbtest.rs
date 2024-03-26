use std::{f64::consts::E, path::PathBuf};

use bytes::Bytes;

use crate::{
  db::Engine,
  errors::Errors,
  option::Options,
  util::rand_kv::{get_test_key, get_test_value},
};

#[test]
fn test_engine_put() {
  let mut opt = Options::default();
  opt.dir_path = PathBuf::from("/tmp/bitkv-rs-put");
  opt.data_file_size = 64 * 1024 * 1024; // 64MB
  let engine = Engine::open(opt.clone()).expect("fail to open engine");

  // put one item
  let res1 = engine.put(get_test_key(11), get_test_value(11));
  assert!(res1.is_ok());
  let res2 = engine.get(get_test_key(11));
  assert!(res2.is_ok());
  assert!(res2.unwrap().len() > 0);

  // put another item repeatedly
  let res3 = engine.put(get_test_key(22), get_test_value(11));
  assert!(res3.is_ok());
  let res4 = engine.put(get_test_key(22), Bytes::from("11"));
  assert!(res4.is_ok());
  let res5 = engine.get(get_test_key(22));
  assert!(res5.is_ok());
  assert_eq!(res5.unwrap(), Bytes::from("11"));

  // key is empty
  let res6 = engine.put(Bytes::new(), get_test_value(111));
  assert_eq!(Errors::KeyIsEmpty, res6.err().unwrap());

  // value is empty
  let res7 = engine.put(get_test_key(31), Bytes::new());
  assert!(res7.is_ok());
  let res8 = engine.get(get_test_key(31));
  assert_eq!(0, res8.ok().unwrap().len());

  // write to changed data file
  for i in 0..=10000 {
    let res = engine.put(get_test_key(i), get_test_value(i));
    assert!(res.is_ok());
  }

  // restart engine and write data
  std::mem::drop(engine);

  let engine2 = Engine::open(opt.clone()).expect("fail to open engine");
  let res9 = engine2.put(get_test_key(100), get_test_value(100));
  assert!(res9.is_ok());

  let res10 = engine2.get(get_test_key(100));
  assert_eq!(res10.unwrap(), get_test_value(100));

  // delete tested files
  std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
}

#[test]
fn test_engine_get() {
  let mut opt = Options::default();
  opt.dir_path = PathBuf::from("/tmp/bitkv-rs-get");
  opt.data_file_size = 64 * 1024 * 1024; // 64MB
  let engine = Engine::open(opt.clone()).expect("fail to open engine");

  // read one item
  let res1 = engine.put(get_test_key(11), get_test_value(11));
  assert!(res1.is_ok());
  let res2 = engine.get(get_test_key(11));
  assert!(res2.is_ok());
  assert!(res2.unwrap().len() > 0);

  // read after putting another items
  let res3 = engine.put(get_test_key(22), Bytes::from("22"));
  assert!(res3.is_ok());
  let res4 = engine.put(get_test_key(33), get_test_value(33));
  assert!(res4.is_ok());
  let res5 = engine.get(get_test_key(22));
  assert!(res5.is_ok());
  assert_eq!(res5.unwrap(), Bytes::from("22"));

  // read when key is invaild
  let res6 = engine.get(Bytes::from("not exist"));
  assert_eq!(Errors::KeyNotFound, res6.err().unwrap());

  // read after value is deleted
  let res7 = engine.put(get_test_key(31), Bytes::new());
  assert!(res7.is_ok());
  let res8 = engine.delete(get_test_key(31));
  assert!(res8.is_ok());
  let res9 = engine.get(get_test_key(31));
  assert_eq!(Errors::KeyNotFound, res9.err().unwrap());

  // read from old data file
  for i in 500..=100000 {
    let res = engine.put(get_test_key(i), get_test_value(i));
    assert!(res.is_ok());
  }
  let res10 = engine.get(get_test_key(5000));
  assert!(res10.is_ok());

  // restart engine and read data
  std::mem::drop(engine);

  let engine2 = Engine::open(opt.clone()).expect("fail to open engine");
  let res11 = engine2.get(get_test_key(33));
  assert_eq!(get_test_value(33), res11.unwrap());

  let res12 = engine2.get(get_test_key(22));
  assert_eq!(Bytes::from("22"), res12.unwrap());

  let res13 = engine2.get(get_test_key(333));
  assert_eq!(Errors::KeyNotFound, res13.err().unwrap());

  // delete tested files
  std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
}

#[test]
fn test_engine_delete() {
  let mut opt = Options::default();
  opt.dir_path = PathBuf::from("/tmp/bitkv-rs-delete");
  opt.data_file_size = 64 * 1024 * 1024; // 64MB
  let engine = Engine::open(opt.clone()).expect("fail to open engine");

  // delete one item
  let res1 = engine.put(get_test_key(11), Bytes::new());
  assert!(res1.is_ok());
  let res2 = engine.delete(get_test_key(11));
  assert!(res2.is_ok());
  let res3 = engine.get(get_test_key(11));
  assert_eq!(Errors::KeyNotFound, res3.err().unwrap());

  // delete a non-exist item
  let res4 = engine.delete(Bytes::from("not existed key"));
  assert!(res4.is_ok());

  // delete an empty key
  let res5 = engine.delete(Bytes::new());
  assert_eq!(Errors::KeyIsEmpty, res5.err().unwrap());

  // delete and put again
  let res6 = engine.put(get_test_key(11), get_test_value(11));
  assert!(res6.is_ok());
  let res7 = engine.delete(get_test_key(11));
  assert!(res7.is_ok());
  let res8 = engine.put(get_test_key(11), get_test_value(11));
  assert!(res8.is_ok());
  let res9 = engine.get(get_test_key(11));
  assert!(res9.is_ok());

  // restart engine and delete data
  std::mem::drop(engine);
  let engine2 = Engine::open(opt.clone()).expect("fail to open engine");
  let res10 = engine2.delete(get_test_key(11));
  assert!(res10.is_ok());
  let res11 = engine2.get(get_test_key(11));
  assert_eq!(Errors::KeyNotFound, res11.err().unwrap());

  // delete tested files
  std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
}

#[test]
fn test_engine_sync() {
  let mut opt = Options::default();
  opt.dir_path = PathBuf::from("/tmp/bitkv-rs-sync");
  opt.data_file_size = 64 * 1024 * 1024; // 64MB
  let engine = Engine::open(opt.clone()).expect("fail to open engine");

  let res = engine.put(get_test_key(11), get_test_value(11));
  assert!(res.is_ok());

  let sync_res = engine.sync();
  assert!(sync_res.is_ok());

  // delete tested files
  std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
}

#[test]
fn test_engine_close() {
  let mut opt = Options::default();
  opt.dir_path = PathBuf::from("/tmp/bitkv-rs-close");
  opt.data_file_size = 64 * 1024 * 1024; // 64MB
  let engine = Engine::open(opt.clone()).expect("fail to open engine");

  let res = engine.put(get_test_key(11), get_test_value(11));
  assert!(res.is_ok());

  let close_res = engine.close();
  assert!(close_res.is_ok());

  // delete tested files
  std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
}

#[test]
fn test_engine_filelock() {
  let mut opt = Options::default();
  opt.dir_path = PathBuf::from("/tmp/bitkv-rs-flock");
  let engine = Engine::open(opt.clone()).expect("fail to open engine");

  let res1 = Engine::open(opt.clone());
  assert_eq!(Errors::DatabaseIsUsing, res1.err().unwrap());

  let res2 = engine.close();
  assert!(res2.is_ok());

  let res3 = Engine::open(opt.clone());
  assert!(res3.is_ok());

  // delete tested files
  std::fs::remove_dir_all(opt.clone().dir_path).expect("failed to remove dir");
}
