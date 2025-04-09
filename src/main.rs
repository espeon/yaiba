use crate::structs::CacheEntry;
use anyhow::{Context, Result}; // Added Context and Result from anyhow
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::db::get_pool;

use cache::Cache;
use hyper::service::service_fn;
use hyper::{HeaderMap, Method, Request, Response, StatusCode};
// hyper_util is not directly used after refactor, consider removing if not needed elsewhere
use hyper_util::server::conn::auto::Builder as AutoBuilder;
use metadata::{sqlite::SqliteCacheMetadata, CacheMetadataBackend, CacheScoringPolicy};
use serde::Serialize;
use serde_json;
use sqlx::SqlitePool;
use storage::{fs::FilesystemStorage, StorageBackend};
use structs::parse_range;
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full};
use std::error::Error as StdError;

type BoxedResponse = Response<BoxBody<Bytes, Box<dyn StdError + Send + Sync>>>;

#[derive(Clone)]
pub struct TokioExecutor;

impl<F> hyper::rt::Executor<F> for TokioExecutor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, fut: F) {
        tokio::task::spawn(fut);
    }
}

mod cache;
mod db;
mod metadata;
mod storage;
mod structs;

const LANDING: &str = r#"
<html><body><pre>
                                  ⢠⣶⡄
                                 ⣠⡿⣿⡃
                                ⢠⣶⡄
                               ⣠⡿⣿⡃
                             ⣤⣶⣳⡽⠇
                            ⢀⠏⢻⣿⡇
                           ⡰⠃⡴⠋⠉⠁
                         ⢀⠞⢠⡞⠁
                        ⡰⢃⣴⠏⠁               yaiba
                      ⢀⠎⣠⣾⠃
                    ⢀⡔⢃⣴⠏      precision-engineered static
                   ⡠⠊⣰⠟⠁             asset delivery
                 ⣠⠞⣠⡾⠃
               ⣠⠞⣡⡾⠋       https://github.com/espeon/yaiba
             ⣠⠞⣡⡾⠋
           ⣠⠞⣡⡾⠋
         ⡠⢊⣠⠞⠉
      ⢀⡴⣊⡴⠛⠁
    ⠠⠶⠥⠾⠋</pre></body></html>"#;

// TODO: get from config
fn get_storage_backend() -> Arc<dyn StorageBackend + Send + Sync> {
    Arc::new(FilesystemStorage::new("./cache".to_string()))
}
fn get_metadata_backend(pool: &SqlitePool) -> Arc<dyn CacheMetadataBackend> {
    Arc::new(SqliteCacheMetadata::new(pool))
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "yaiba=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();
    let db = get_pool().await.context("Failed to connect to database")?;
    sqlx::migrate!().run(&db).await?;
    let max_size_bytes = std::env::var("MAX_SIZE_BYTES")
        .unwrap_or_else(|_| "524288000".into())
        .parse()
        .context("Failed to parse MAX_SIZE_BYTES")?;
    let url_base = std::env::var("URL_BASE").unwrap_or_else(|_| "https://cdn.yaiba.org/".into());
    let pool = Arc::new(db);
    let cache = Arc::new(cache::Cache::new(
        get_storage_backend(),
        get_metadata_backend(&pool),
        max_size_bytes,
        url_base,
        CacheScoringPolicy::Lru,
        pool.clone(),
    ));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr).await?;

    info!("Listening on {}", addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = hyper_util::rt::tokio::TokioIo::new(stream);

        let cache_clone = cache.clone();
        let pool_clone = pool.clone();

        tokio::task::spawn(async move {
            let builder = AutoBuilder::new(TokioExecutor);
            let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                let cache_req_clone = cache_clone.clone();
                let pool_req_clone = pool_clone.clone();
                async move {
                    let result = handle_request(req, cache_req_clone, pool_req_clone).await;
                    let response = match result {
                        Ok(resp) => resp,
                        Err(e) => {
                            tracing::error!("Request handling error: {:?}", e);
                            internal_server_error("Internal Server Error".to_string())
                        }
                    };
                    // The service_fn must return Ok(Response)
                    Ok::<_, Infallible>(response)
                }
            });

            if let Err(err) = builder.serve_connection(io, service).await {
                // Log connection-level errors
                if !err.to_string().contains("connection reset by peer")
                    && !err.to_string().contains("connection closed")
                {
                    tracing::warn!("Error serving connection: {}", err);
                }
            }
        });
    }
}

// get all files in the sqlite cache
async fn list_all_files(pool: Arc<SqlitePool>) -> Result<BoxedResponse> {
    // Use anyhow::Result
    let entries = sqlx::query_as!(CacheEntry, "select * from cache order by importance desc")
        .fetch_all(&*pool)
        .await
        .context("Failed to fetch cache entries from database")?;
    json_response(&entries, StatusCode::OK)
}

/// serve a file based on a given key
async fn serve_file(key: String, cache: Arc<Cache>, headers: &HeaderMap) -> Result<BoxedResponse> {
    // Use anyhow::Result
    let range_header = headers.get("Range");
    let range = if let Some(range_val) = range_header {
        info!("Detected range! {:?}", range_val);
        let range_str = range_val.to_str().context("Invalid Range header format")?;
        parse_range(range_str)
    } else {
        None
    };

    match cache.get(key.clone(), range, None).await {
        // Clone key for potential error message
        Ok((body, resp_headers, status)) => {
            let mut response_builder = Response::builder().status(status);
            for (key, value) in resp_headers.iter() {
                if !key.as_str().starts_with("access-control-") {
                    response_builder = response_builder.header(key, value);
                }
            }
            // add CORS headers manually
            response_builder = response_builder.header("Access-Control-Allow-Origin", "*");

            response_builder
                .body(body)
                .context("Failed to build response body")
        }
        Err(e) => {
            tracing::warn!("Cache get failed for key '{}': {:?}", key, e);
            let body: BoxBody<Bytes, Box<dyn StdError + Send + Sync>> =
                Full::new(Bytes::from("Cache entry not found or error retrieving"))
                    .map_err(|never| -> Box<dyn StdError + Send + Sync> { Box::new(never) })
                    .boxed();

            Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .header("Access-Control-Allow-Origin", "*")
                .body(body)
                .context("Failed to build NOT_FOUND response body")?)
        }
    }
}

async fn flush_cache(key: String, cache: Arc<Cache>) -> Result<BoxedResponse> {
    // Use anyhow::Result
    cache
        .delete(&key)
        .await
        .with_context(|| format!("Failed to delete cache key: {}", key))?; // Use with_context

    let body = Full::new(Bytes::from("cache flushed"))
        .map_err(|e| -> Box<dyn StdError + Send + Sync> { Box::new(e) })
        .boxed();

    Ok(Response::new(body))
}

async fn flush_cache_prefix(prefix: String, cache: Arc<Cache>) -> Result<BoxedResponse> {
    cache
        .delete_with_prefix(&prefix)
        .await
        .with_context(|| format!("Failed to delete cache prefix: {}", prefix))?;

    let body = Full::new(Bytes::from("cache prefix flushed"))
        .map_err(|e| -> Box<dyn StdError + Send + Sync> { Box::new(e) })
        .boxed();

    Response::builder()
        .status(StatusCode::OK)
        .header("Access-Control-Allow-Origin", "*")
        .body(body)
        .context("Failed to build OK response body for flush_prefix")
}

async fn handle_request(
    req: Request<hyper::body::Incoming>,
    cache: Arc<Cache>,
    pool: Arc<SqlitePool>,
) -> Result<BoxedResponse> {
    // Use anyhow::Result
    let (parts, _body) = req.into_parts();
    let path = parts.uri.path().to_string();
    let method = parts.method;
    let headers = parts.headers;

    match (method, path.as_str()) {
        // Match on &str
        (Method::GET, "/") => Ok(Response::new(
            http_body_util::Full::from(LANDING)
                .map_err(|never| Box::new(never) as Box<dyn StdError + Send + Sync>)
                .boxed(),
        )),
        (Method::GET, "/all_files") => list_all_files(pool).await, // Propagates Result
        (Method::DELETE, p) if p.starts_with("/api/v1/flush/prefix/") => {
            let prefix = p.trim_start_matches("/api/v1/flush/prefix/").to_string();
            flush_cache_prefix(prefix, cache).await // Propagates Result
        }
        (Method::DELETE, p) if p.starts_with("/api/v1/flush/") => {
            let key = p.trim_start_matches("/api/v1/flush/").to_string();
            flush_cache(key, cache).await // Propagates Result
        }
        (Method::DELETE, p) => {
            let key = p.trim_start_matches('/').to_string();
            flush_cache(key, cache).await
        }
        (Method::GET, p) => {
            let key = p.trim_start_matches('/').to_string();
            serve_file(key, cache, &headers).await // Propagates Result
        }
        (Method::OPTIONS, _) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE")
            .header("Access-Control-Allow-Headers", "*")
            .body(
                http_body_util::Empty::new() // Use Empty for default
                    .map_err(|never| Box::new(never) as Box<dyn StdError + Send + Sync>)
                    .boxed(),
            )
            .context("Failed to build OPTIONS response body")?),
         _ => Ok(Response::builder()
             .status(StatusCode::NOT_FOUND)
             .body(
                 http_body_util::Full::from("Not Found")
                     .map_err(|never| Box::new(never) as Box<dyn StdError + Send + Sync>)
                     .boxed()
             )
             .context("Failed to build Not Found response body")?),

    }
}

fn internal_server_error(message: String) -> BoxedResponse {
    Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .header("Access-Control-Allow-Origin", "*")
        .body(
            http_body_util::Full::from(message)
                .map_err(|never| Box::new(never) as Box<dyn StdError + Send + Sync>)
                .boxed(),
        )
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(
                    http_body_util::Full::from("Internal Server Error")
                        .map_err(|never| Box::new(never) as Box<dyn StdError + Send + Sync>)
                        .boxed(),
                )
                .unwrap()
        })
}

fn json_response<T: Serialize>(data: &T, status: StatusCode) -> Result<BoxedResponse> {
    let json = serde_json::to_string(data).context("Failed to serialize data to JSON")?;
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Access-Control-Allow-Origin", "*")
        .body(
            http_body_util::Full::from(json)
                .map_err(|never| Box::new(never) as Box<dyn StdError + Send + Sync>)
                .boxed(),
        )
        .context("Failed to build JSON response body")
}
