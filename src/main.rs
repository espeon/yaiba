use std::fs::File;

use crate::db::get_pool;

use axum::{
    body::StreamBody,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Extension, Router,
};
use tokio_util::io::ReaderStream;
use cache::Cache;
use sqlx::SqlitePool;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod cache;
mod db;
mod structs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "yaiba=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();
    let db = get_pool().await.expect("can connect to database");
    sqlx::migrate!().run(&db).await?;
    let cache = cache::Cache::new(db.clone());

    let app = Router::new()
        .route(
            "/*key",
            get(serve_file),
        )
        .with_state(db)
        .layer(Extension(cache));

    // run it with axum on localhost:3000
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn conn(
    State(pool): State<SqlitePool>,
) -> Result<String, (StatusCode, String)> {
    sqlx::query_scalar("select 'hello world from pg'")
        .fetch_one(&pool)
        .await
        .map_err(internal_error)
}

/// serve a file based on a given key
#[axum::debug_handler]
async fn serve_file(
    Path(key): Path<String>,
    Extension(cache): Extension<Cache>,
) -> impl IntoResponse {
    let file = cache.get(key).await;

    // return json response
    match file {
        Ok(f) => {
            let stream = ReaderStream::new(f);
            let body = StreamBody::new(stream);
            Ok(body)
        },
        Err(e) => {
            dbg!(e);
            Err((StatusCode::NOT_FOUND, "file not found".to_owned()))
        }
    }
}


/// Utility function for mapping any error into a `500 Internal Server Error`
/// response.
fn internal_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
{
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
