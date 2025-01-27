use std::sync::Arc;

use crate::db::get_pool;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get},
    Extension, Json, Router,
};
use cache::Cache;
use hyper::header;
use sqlx::SqlitePool;
use storage::{fs::FilesystemStorage, StorageBackend};
use structs::CacheEntry;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod cache;
mod db;
mod storage;
mod structs;

fn get_storage_backend() -> Arc<dyn StorageBackend> {
    Arc::new(FilesystemStorage::new("./cache".to_string()))
}

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
    // default of 5gib
    let max_size_bytes = std::env::var("MAX_SIZE_BYTES")
        .unwrap_or_else(|_| "5368709120".into())
        .parse()
        .unwrap();
    let url_base = std::env::var("URL_BASE").unwrap_or_else(|_| "https://cdn.yaiba.org/".into());
    let cache = cache::Cache::new(db.clone(), get_storage_backend(), max_size_bytes, url_base);

    let app = Router::new()
        .route("/all_files", get(conn))
        .route("/api/v1/flush/prefix", delete(flush_cache_prefix))
        .route("/api/v1/flush/*key", delete(flush_cache))
        .route("/*key", delete(flush_cache))
        .route("/*key", get(serve_file))
        .with_state(db)
        .layer(Extension(cache));

    // run it with axum on localhost:3000
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

// TODO: put behind auth or something
// Get all files in the sqlite cache
async fn conn(
    State(pool): State<SqlitePool>,
) -> Result<Json<Vec<CacheEntry>>, (StatusCode, String)> {
    sqlx::query_as!(CacheEntry, "select * from cache order by importance desc")
        .fetch_all(&pool)
        .await
        .map_err(internal_error)
        .map(Json)
}

/// serve a file based on a given key
#[axum::debug_handler]
async fn serve_file(
    Path(key): Path<String>,
    Extension(cache): Extension<Cache>,
) -> impl IntoResponse {
    match cache.get(key).await {
        Ok(stream) => {
            let headers = [
                (header::CONTENT_TYPE, "application/octet-stream"),
                // Add other headers as needed
            ];

            Ok((headers, stream))
        }
        Err(e) => {
            dbg!(e);
            Err((StatusCode::NOT_FOUND, "file not found".to_owned()))
        }
    }
}

async fn flush_cache(
    Path(key): Path<String>,
    Extension(cache): Extension<Cache>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    cache
        .delete(&key)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
}

async fn flush_cache_prefix(
    Path(prefix): Path<String>,
    Extension(cache): Extension<Cache>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    cache
        .delete_with_prefix(&prefix)
        .await
        .map_err(|err| (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()))
}

/// Utility function for mapping any error into a `500 Internal Server Error`
/// response.
fn internal_error<E>(err: E) -> (StatusCode, String)
where
    E: std::error::Error,
{
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}
