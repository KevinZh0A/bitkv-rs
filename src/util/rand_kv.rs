use bytes::Bytes;

#[allow(dead_code)]
pub fn get_test_key(i: usize) -> Bytes {
  Bytes::from(format!("key-{:09}", i))
}
#[allow(dead_code)]
pub fn get_test_value(i: usize) -> Bytes {
  Bytes::from(format!("value-{:09}", i))
}

#[test]
fn test_get_test_key() {
  for i in 0..=10 {
    let key = get_test_key(i);
    assert_eq!(key, Bytes::from(format!("key-{:09}", i)));
  }
}

#[test]
fn test_get_test_value() {
  for i in 0..=10 {
    let val = get_test_value(i);
    assert_eq!(val, Bytes::from(format!("value-{:09}", i)));
  }
}
