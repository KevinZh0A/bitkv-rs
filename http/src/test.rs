use super::*;
use actix_web::{http::StatusCode, test};

#[actix_web::test]
async fn test_put_handler() {
  let mut opts = Options::default();
  opts.dir_path = PathBuf::from("/tmp/bitkv-rs-http");
  let engine = Arc::new(Engine::open(opts).unwrap());

  let mut app = test::init_service(
    App::new()
      .app_data(web::Data::new(engine.clone()))
      .service(Scope::new("/bitkv").service(put_handler)),
  )
  .await;

  let req = test::TestRequest::with_uri("/bitkv/put")
    .method(actix_web::http::Method::POST)
    .set_json(&json!({"key": "test", "value": "test value"}))
    .to_request();

  let resp = test::call_service(&mut app, req).await;
  assert_eq!(resp.status(), StatusCode::OK);
}

#[actix_web::test]
async fn test_get_handler() {
  let mut opts = Options::default();
  opts.dir_path = PathBuf::from("/tmp/bitkv-rs-http");
  let engine = Arc::new(Engine::open(opts).unwrap());

  let mut app = test::init_service(
    App::new()
      .app_data(web::Data::new(engine.clone()))
      .service(Scope::new("/bitkv").service(get_handler)),
  )
  .await;

  // Insert a key-value pair
  let _ = test::TestRequest::with_uri("/bitkv/put")
    .method(actix_web::http::Method::POST)
    .set_json(&json!({"key": "test", "value": "test value"}))
    .to_request();

  let req = test::TestRequest::with_uri("/bitkv/get/test").to_request();
  let resp = test::call_service(&mut app, req).await;
  assert_eq!(resp.status(), StatusCode::OK);
}

#[actix_web::test]
async fn test_listkeys_handler() {
  let mut opts = Options::default();
  opts.dir_path = PathBuf::from("/tmp/bitkv-rs-http");
  let engine = Arc::new(Engine::open(opts).unwrap());

  let mut app = test::init_service(
    App::new()
      .app_data(web::Data::new(engine.clone()))
      .service(Scope::new("/bitkv").service(listkeys_handler)),
  )
  .await;

  let req = test::TestRequest::with_uri("/bitkv/listkeys").to_request();
  let resp = test::call_service(&mut app, req).await;
  assert_eq!(resp.status(), StatusCode::OK);
}

#[actix_web::test]
async fn test_stat_handler() {
  let mut opts = Options::default();
  opts.dir_path = PathBuf::from("/tmp/bitkv-rs-http");
  let engine = Arc::new(Engine::open(opts).unwrap());

  let mut app = test::init_service(
    App::new()
      .app_data(web::Data::new(engine.clone()))
      .service(Scope::new("/bitkv").service(stat_handler)),
  )
  .await;

  let req = test::TestRequest::with_uri("/bitkv/stat").to_request();
  let resp = test::call_service(&mut app, req).await;
  assert_eq!(resp.status(), StatusCode::OK);
}
