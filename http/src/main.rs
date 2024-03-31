#[cfg(test)]
mod test;

use actix_web::{delete, get, post, web, App, HttpResponse, HttpServer, Responder, Scope};
use bitkv_rs::{db::Engine, errors::Errors, option::Options};
use serde_json::json;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use surf::post as surf_post; // 为避免与 actix_web 的 post 宏冲突
use tokio::{
  io::{self, AsyncBufReadExt, BufReader},
  sync::Notify,
};

#[post("/put")]
pub async fn put_handler(
  eng: web::Data<Arc<Engine>>,
  data: web::Json<HashMap<String, String>>,
) -> impl Responder {
  for (key, val) in data.iter() {
    if eng
      .put(web::Bytes::from(key.clone()), web::Bytes::from(val.clone()))
      .is_err()
    {
      return HttpResponse::InternalServerError().body("failed to put value into engine");
    }
  }
  HttpResponse::Ok().body("成功")
}

#[get("/get/{key}")]
pub async fn get_handler(eng: web::Data<Arc<Engine>>, key: web::Path<String>) -> impl Responder {
  match eng.get(web::Bytes::from(key.to_string())) {
    Ok(val) => HttpResponse::Ok().body(val),
    Err(e) => match e {
      Errors::KeyNotFound => HttpResponse::Ok().body("key not found"),
      _ => HttpResponse::InternalServerError().body("failed to get value from engine"),
    },
  }
}

#[delete("/delete/{key}")]
pub async fn delete_handler(eng: web::Data<Arc<Engine>>, key: web::Path<String>) -> impl Responder {
  if let Err(e) = eng.delete(web::Bytes::from(key.to_string())) {
    match e {
      Errors::KeyNotFound => return HttpResponse::Ok().body("key not found"),
      _ => return HttpResponse::InternalServerError().body("failed to delete value in engine"),
    }
  }
  HttpResponse::Ok().body("OK")
}

#[get("/listkeys")]
pub async fn listkeys_handler(eng: web::Data<Arc<Engine>>) -> impl Responder {
  let keys = match eng.list_keys() {
    Ok(keys) => keys,
    Err(_) => return HttpResponse::InternalServerError().body("failed to list keys"),
  };

  let keys = keys
    .into_iter()
    .map(|key| String::from_utf8(key.to_vec()).unwrap())
    .collect::<Vec<String>>();
  let res = serde_json::to_string(&keys).unwrap();
  HttpResponse::Ok()
    .content_type("application/json")
    .body(res)
}

#[get("/stat")]
pub async fn stat_handler(eng: web::Data<Arc<Engine>>) -> impl Responder {
  let stat = match eng.get_engine_stat() {
    Ok(stat) => stat,
    Err(_) => return HttpResponse::InternalServerError().body("failed to get stat in engine"),
  };

  let mut res = HashMap::new();
  res.insert("key_num", stat.key_num);
  res.insert("data_file_num", stat.data_file_num);
  res.insert("reclaim_size", stat.reclaim_size);
  res.insert("disk_size", stat.disk_size as usize);

  HttpResponse::Ok()
    .content_type("application/json")
    .body(serde_json::to_string(&res).unwrap())
}

async fn send_request() -> surf::Result<()> {
  let uri = "http://127.0.0.1:8080/bitkv/put";
  let data = json!({ "key1": "value1", "key2": "value2" });
  let mut res = surf_post(uri).body_json(&data)?.await?;

  println!("Status: {}", res.status());
  let body = res.body_string().await?;
  println!("Response: {}", body);

  let uri = "http://127.0.0.1:8080/bitkv/listkeys";
  let mut res = surf::get(uri).await?;

  println!("Status: {}", res.status());
  let body = res.body_string().await?;
  println!("Response: {}", body);
  let keys: Vec<String> = serde_json::from_str(&body)?;

  for key in keys {
    let url = format!("http://127.0.0.1:8080/bitkv/get/{}", key);
    let mut res = surf::get(url).await?;
    println!("Status: {}", res.status());
    let body = res.body_string().await?;
    println!("Key: {}, Value: {}", key, body);
  }

  Ok(())
}

async fn run_server(engine: Arc<Engine>) -> std::io::Result<()> {
  let server = HttpServer::new(move || {
    App::new().app_data(web::Data::new(engine.clone())).service(
      Scope::new("/bitkv")
        .service(put_handler)
        .service(get_handler)
        .service(delete_handler)
        .service(listkeys_handler)
        .service(stat_handler),
    )
  })
  .bind("127.0.0.1:8080")?
  .run();

  server.await?;
  Ok(())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
  let engine = Arc::new(
    Engine::open(Options {
      dir_path: PathBuf::from("/tmp/bitkv-rs-http"),
      ..Default::default()
    })
    .unwrap(),
  );
  let shutdown_notify = Arc::new(Notify::new());

  let shutdown_notify_for_server = shutdown_notify.clone();
  let engine_for_server = engine.clone();

  let server_handle = tokio::spawn(async move {
    if let Err(e) = run_server(engine_for_server).await {
      eprintln!("Server error: {}", e);
    }
  });

  tokio::spawn(async move {
    if let Err(e) = send_request().await {
      eprintln!("failed to request: {}", e);
    }
  });

  let stdin = io::stdin();
  let reader = BufReader::new(stdin);
  let mut lines = reader.lines();

  if let Ok(Some(_)) = lines.next_line().await {
    shutdown_notify_for_server.notify_one();
  }

  let _ = server_handle.await;

  // Wait for user input to trigger the shutdown.
  shutdown_notify_for_server.notified().await;

  if let Err(e) = engine.close() {
    eprintln!("failed to close engine: {}", e);
  }

  println!("engine is closed");

  Ok(())
}
