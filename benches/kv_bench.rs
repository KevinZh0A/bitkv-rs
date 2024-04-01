use bitkv_rs::{
  db::Engine,
  option::Options,
  util::rand_kv::{get_test_key, get_test_value},
};
use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;
use std::path::PathBuf;

fn bench_put(c: &mut Criterion) {
  let mut option = Options::default();
  option.dir_path = PathBuf::from("/tmp/bitkv-rs-bench/put-bench");
  if !option.dir_path.is_dir() {
    std::fs::create_dir_all(&option.dir_path).unwrap();
  }
  let engine = Engine::open(option).unwrap();

  let mut rnd = rand::thread_rng();

  c.bench_function("bitkv-put-bench", |b| {
    b.iter(|| {
      let i = rnd.gen_range(0..std::u32::MAX) as usize;
      let res = engine.put(get_test_key(i), get_test_value(i));
      assert!(res.is_ok());
    })
  });

  std::fs::remove_dir_all("/tmp/bitkv-rs-bench/put-bench").unwrap();
}

fn bench_get(c: &mut Criterion) {
  let mut option = Options::default();
  option.dir_path = PathBuf::from("/tmp/bitkv-rs-bench/get-bench");
  if !option.dir_path.is_dir() {
    std::fs::create_dir_all(&option.dir_path).unwrap();
  }
  let engine = Engine::open(option).unwrap();

  for i in 0..100000 {
    let res = engine.put(get_test_key(i), get_test_value(i));
    assert!(res.is_ok());
  }

  let mut rnd = rand::thread_rng();

  c.bench_function("bitkv-get-bench", |b| {
    b.iter(|| {
      let i = rnd.gen_range(0..std::u32::MAX) as usize;

      if (0..100000).contains(&i) {
        let res = engine.get(get_test_key(i));
        assert!(res.is_ok());
      } else {
        let res = engine.get(get_test_key(i));
        assert!(res.is_err());
      }
    })
  });

  std::fs::remove_dir_all("/tmp/bitkv-rs-bench/get-bench").unwrap();
}

fn bench_delete(c: &mut Criterion) {
  let mut option = Options::default();
  option.dir_path = PathBuf::from("/tmp/bitkv-rs-bench/delete-bench");
  if !option.dir_path.is_dir() {
    std::fs::create_dir_all(&option.dir_path).unwrap();
  }
  let engine = Engine::open(option).unwrap();

  for i in 0..100000 {
    let res = engine.put(get_test_key(i), get_test_value(i));
    assert!(res.is_ok());
  }

  let mut rnd = rand::thread_rng();

  c.bench_function("bitkv-delete-bench", |b| {
    b.iter(|| {
      let i = rnd.gen_range(0..std::u32::MAX) as usize;
      engine.delete(get_test_key(i)).unwrap();
    })
  });

  std::fs::remove_dir_all("/tmp/bitkv-rs-bench/delete-bench").unwrap();
}

fn bench_listkeys(c: &mut Criterion) {
  let mut option = Options::default();
  option.dir_path = PathBuf::from("/tmp/bitkv-rs-bench/listkeys-bench");
  if !option.dir_path.is_dir() {
    std::fs::create_dir_all(&option.dir_path).unwrap();
  }
  let engine = Engine::open(option).unwrap();

  for i in 0..100000 {
    let res = engine.put(get_test_key(i), get_test_value(i));
    assert!(res.is_ok());
  }

  c.bench_function("bitkv-listkeys-bench", |b| {
    b.iter(|| {
      let res = engine.list_keys();
      assert!(res.is_ok());
    })
  });

  std::fs::remove_dir_all("/tmp/bitkv-rs-bench/listkeys-bench").unwrap();
}

fn bench_stat(c: &mut Criterion) {
  let mut option = Options::default();
  option.dir_path = PathBuf::from("/tmp/bitkv-rs-bench/stat-bench");
  if !option.dir_path.is_dir() {
    std::fs::create_dir_all(&option.dir_path).unwrap();
  }
  let engine = Engine::open(option).unwrap();

  for i in 0..100000 {
    let res = engine.put(get_test_key(i), get_test_value(i));
    assert!(res.is_ok());
  }

  c.bench_function("bitkv-stat-bench", |b| {
    b.iter(|| {
      let res = engine.get_engine_stat();
      assert!(res.is_ok());
    })
  });

  std::fs::remove_dir_all("/tmp/bitkv-rs-bench/stat-bench").unwrap();
}

criterion_group!(
  benches,
  bench_get,
  bench_put,
  bench_delete,
  bench_listkeys,
  bench_stat
);
criterion_main!(benches);
