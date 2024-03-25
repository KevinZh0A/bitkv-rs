use bitkv_rs::{db, option::Options};
use bytes::Bytes;

fn main() {
    let opts = Options::default();
    let engine = db::Engine::open(opts).expect("fail to open bitkv engine");

    let key = Bytes::from(b"hello".to_vec());
    let value = Bytes::from(b"world".to_vec());

    let res = engine.put(key.clone(), value.clone());
    assert!(res.is_ok());

    let res2 = engine.get(key.clone());
    assert!(res2.is_ok());

    println!("{:?}", res2);
    assert_eq!(res2.ok().unwrap(), value);

    engine.delete(key.clone()).expect("fail to delete");
    let res1 = engine.get(key.clone());
    assert!(res1.is_err());
}
