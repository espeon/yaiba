use std::{io, pin::Pin};

use axum::body::StreamBody;
use bytes::Bytes;
use hyper::StatusCode;
use tokio::{fs::File, io::AsyncWriteExt};

use futures::{Stream, StreamExt, TryStreamExt};
use sqlx::SqlitePool;
use time::OffsetDateTime;
use tokio_util::codec::{BytesCodec, FramedRead};
use tracing::{info, warn};

use crate::structs::CacheEntry;

#[derive(Clone)]
pub struct Cache {
    max_size_bytes: i128,
    db: SqlitePool,
    // TODO: make this configurable via a bucket-type system
    // similar to s3 with its subdomain and bucket prefixes
    url_base: String,
}

async fn file_to_stream(path: &str) -> io::Result<impl futures::Stream<Item = io::Result<Bytes>>> {
    let file = File::open(path).await?;
    let stream =
        FramedRead::new(file, BytesCodec::new()).map(|res| res.map(|bytes_mut| bytes_mut.freeze())); // Convert BytesMut to Bytes
    Ok(stream)
}

impl Cache {
    pub fn new(db: SqlitePool, max_size_bytes: i128, url_base: String) -> Cache {
        Cache {
            max_size_bytes,
            url_base,
            db,
        }
    }
    /// gets a file from the cache, and pulls from source and puts into cache if it doesn't exist
    pub async fn get(
        self,
        k: String,
    ) -> anyhow::Result<
        StreamBody<Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static>>>,
    > {
        let k1 = k.clone();
        match sqlx::query_as!(CacheEntry, "select * from cache where key = $1", k1)
            .fetch_one(&self.db)
            .await
        {
            Ok(e) => {
                // File exists in cache, stream from disk
                if let Some(key) = e.key {
                    let path = format!("./cache/{}", &key);
                    let stream = file_to_stream(&path).await?;
                    tokio::spawn(async move {
                        if let Err(e) = self.record_access(&key).await {
                            warn!("error recording access to {}: {}", &key, e);
                        }
                    });
                    Ok(StreamBody::new(Box::pin(stream)))
                } else {
                    // Need to download
                    let stream = self
                        .stream_url_to_disk_and_client(format!("{}{}", self.url_base, k), &k)
                        .await?;
                    Ok(StreamBody::new(Box::pin(stream)))
                }
            }
            Err(_) => {
                // Need to download - use stream_url_to_disk_and_client
                let stream = self
                    .stream_url_to_disk_and_client(format!("{}{}", self.url_base, k), &k)
                    .await?;
                Ok(StreamBody::new(Box::pin(stream)))
            }
        }
    }
    pub async fn put(&self, k: &String, size_bytes: i64) -> anyhow::Result<CacheEntry> {
        // save file to db
        let current_time = OffsetDateTime::now_utc();

        let file = match sqlx::query_as!(
            CacheEntry,
            "insert into cache (key, size, date, importance) values ($1, $2, $3, $4) returning *",
            k,
            size_bytes,
            current_time,
            0
        )
        .fetch_one(&self.db)
        .await
        .map_err(crate::internal_error)
        {
            Ok(e) => e,
            Err(e) => {
                dbg!(e);
                return anyhow::Result::Err(anyhow::anyhow!("error saving to db"));
            }
        };
        Ok(file)
    }

    pub async fn record_access(&self, k: &String) -> anyhow::Result<()> {
        let current_time = OffsetDateTime::now_utc();
        sqlx::query!(
            "update cache set last_access = $1, times_accessed = times_accessed + 1 where key = $2",
            current_time,
            k
        )
        .execute(&self.db)
        .await?;
        Ok(())
    }

    pub async fn ensure_space(&self, needed: i64) -> anyhow::Result<()> {
        let current_size = sqlx::query_scalar!("SELECT COALESCE(SUM(size), 0) FROM cache")
            .fetch_one(&self.db)
            .await?;
        if (current_size as i128 + needed as i128) > self.max_size_bytes {
            info!("cache full, purging enough to make space");
            let mut purged = 0;
            let mut purged_keys = Vec::new();
            let mut current_size: i64 = current_size as i64;
            while (current_size as i128 + needed as i128) > self.max_size_bytes {
                let entry = sqlx::query_as!(
                    CacheEntry, r#"WITH scoring AS (
                        SELECT
                            *,
                            (julianday('now') - julianday(date)) as age_days,
                            CAST(size AS FLOAT) / (SELECT MAX(CAST(size AS FLOAT)) FROM cache) as size_ratio
                        FROM cache
                    )
                    SELECT
                        key,
                        size,
                        date,
                        last_access,
                        times_accessed,
                        expiration,
                        importance
                    FROM scoring
                    ORDER BY
                        (importance * 0.5) +
                        ((1.0 - size_ratio) * 0.3) +
                        (1.0 / (age_days + 1.0) * 0.2)
                    ASC
                    LIMIT 1"#
                )
                .fetch_one(&self.db)
                .await?;
                if let Some(size) = entry.size {
                    current_size = current_size as i64 - size;
                    let key = entry.key.unwrap();
                    purged_keys.push(key.clone());
                    sqlx::query!("delete from cache where key = $1", key)
                        .execute(&self.db)
                        .await?;
                    purged += 1;
                } else {
                    return Err(anyhow::anyhow!(
                        "no entries to purge, space must be too small!"
                    ));
                }
            }
            info!("purged {} entries", purged);
        }
        Ok(())
    }
    #[allow(dead_code)]
    pub async fn stream_url_to_disk(&self, url: String, name: &String) -> anyhow::Result<File> {
        let req = reqwest::get(url).await?;
        let headers = req.headers();
        // do we have space?
        self.ensure_space(headers.len() as i64).await?;
        // for now - save to file
        // TODO: stream to file and client simultaneously
        let mut file = File::create(format!("./cache/{}", name)).await?;
        let mut stream = req.bytes_stream();
        while let Some(item) = stream.next().await {
            let item = item?;
            file.write_all(&item).await.unwrap();
        }
        Ok(file)
    }
    pub async fn stream_url_to_disk_and_client(
        &self,
        url: String,
        name: &String,
    ) -> anyhow::Result<Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>> {
        let req = reqwest::get(url).await?;
        let status = req.status();
        let headers = req.headers().clone();

        // Handle non-success status codes
        if !status.is_success()
            && status != StatusCode::PERMANENT_REDIRECT
            && status != StatusCode::MOVED_PERMANENTLY
            && status != StatusCode::NOT_MODIFIED
        {
            // For error status codes, just proxy the stream
            let stream = req
                .bytes_stream()
                .map_ok(|bytes| bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
            return Ok(Box::pin(stream));
        }

        // Get content length, default to 0 if not present
        let size_bytes = headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);

        // Ensure we have space in cache
        self.ensure_space(size_bytes).await?;

        // Create cache entry
        self.put(name, size_bytes).await?;

        // Create cache file
        let file_path = format!("./cache/{}", name);
        let file = File::create(&file_path).await?;

        // Setup broadcast channel for sharing bytes
        let (tx, rx) = tokio::sync::broadcast::channel(32);

        // Clone necessary values for spawn
        let file_path = file_path.clone();

        // Spawn file writing task
        let file_write_handle = tokio::spawn(async move {
            let mut file = file;
            let mut stream = req.bytes_stream();

            let mut result = Ok(());
            while let Some(item) = stream.next().await {
                match item {
                    Ok(bytes) => {
                        if let Err(e) = file.write_all(&bytes).await {
                            result = Err(e);
                            break;
                        }
                        if tx.send(bytes).is_err() {
                            break; // Channel closed
                        }
                    }
                    Err(e) => {
                        result = Err(std::io::Error::new(std::io::ErrorKind::Other, e));
                        break;
                    }
                }
            }

            if result.is_err() {
                // Cleanup on error
                if let Err(e) = tokio::fs::remove_file(&file_path).await {
                    warn!("Failed to cleanup cache file: {}", e);
                }
            }

            result
        });

        // Create client stream from broadcast receiver
        let client_stream = tokio_stream::wrappers::BroadcastStream::new(rx)
            .map_ok(|bytes| bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

        // Spawn task to await file writing completion
        tokio::spawn(async move {
            if let Err(e) = file_write_handle.await {
                warn!("File writing task failed: {}", e);
            }
        });

        Ok(Box::pin(client_stream))
    }
}
