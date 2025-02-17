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
use hyper::HeaderMap;
use metadata::{sqlite::SqliteCacheMetadata, CacheMetadataBackend, CacheScoringPolicy};
use sqlx::SqlitePool;
use storage::{fs::FilesystemStorage, StorageBackend};
use structs::{parse_range, CacheEntry};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod cache;
mod db;
mod metadata;
mod storage;
mod structs;

// TIL you can just pass in a string literal to get()
const LANDING: &str = r#"
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢠⣶⡄
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⡿⣿⡃
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣰⢿⣻⠇⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣰⣿⣿⠋⠀⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣤⣶⣳⡽⠇⠀⠀⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠏⢻⣿⡇⠀⠀⠀⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡰⠃⡴⠋⠉⠁⠀⠀⠀⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠞⢠⡞⠁⠀⠀⠀⠀⠀⠀⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡰⢃⣴⠏⠁⠀⠀⠀⠀⠀⠀⠀⠀           yaiba
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠎⣠⣾⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡔⢃⣴⠏⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀precision-engineered static
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡠⠊⣰⠟⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀       asset delivery
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⠞⣠⡾⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⠞⣡⡾⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀ https://github.com/espeon/yaiba
    ⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⠞⣡⡾⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
    ⠀⠀⠀⠀⠀⠀⠀⣠⠞⣡⡾⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
    ⠀⠀⠀⠀⠀⡠⢊⣠⠞⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
    ⠀⠀⢀⡴⣊⡴⠛⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
    ⠠⠶⠥⠾⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀"#;

// TODO: get from config
fn get_storage_backend() -> Arc<dyn StorageBackend> {
    Arc::new(FilesystemStorage::new("./cache".to_string()))
}
fn get_metadata_backend(pool: &SqlitePool) -> Arc<dyn CacheMetadataBackend> {
    Arc::new(SqliteCacheMetadata::new(pool))
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
    // default of 500mib
    let max_size_bytes = std::env::var("MAX_SIZE_BYTES")
        .unwrap_or_else(|_| "524288000".into())
        .parse()
        .unwrap();
    let url_base = std::env::var("URL_BASE").unwrap_or_else(|_| "https://cdn.yaiba.org/".into());
    let cache = cache::Cache::new(
        get_storage_backend(),
        get_metadata_backend(&db),
        max_size_bytes,
        url_base,
        CacheScoringPolicy::Lru,
    );

    let app = Router::new()
        .route("/", get(LANDING))
        .route("/all_files", get(conn))
        .route("/api/v1/flush/prefix/{*prefix}", delete(flush_cache_prefix))
        .route("/api/v1/flush/{*key}", delete(flush_cache))
        .route("/{*key}", delete(flush_cache))
        .route("/{*key}", get(serve_file))
        .layer(CorsLayer::permissive())
        .with_state(db)
        .layer(Extension(cache));

    // run it with axum on localhost:3000
    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();

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
    headers: HeaderMap,
) -> impl IntoResponse {
    // get headers
    let range = headers.get("Range");
    let range = if let Some(range) = range {
        info!("Detected range! {:?}", range);
        parse_range(range.to_str().unwrap())
    } else {
        None
    };
    match cache.get(key, range, None).await {
        Ok((stream, headers, status)) => Ok((status, headers, stream)),
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
