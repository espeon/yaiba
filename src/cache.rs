use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use axum::body::StreamBody;
use bytes::Bytes;
use hyper::StatusCode;

use futures::{Stream, TryStreamExt};
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

use crate::{
    metadata::{CacheMetadataBackend, CacheScoringPolicy},
    storage::StorageBackend,
    structs::{CacheEntry, CacheTier, CacheTierRank},
};

const DEFAULT_TIER: CacheTier = CacheTier {
    rank: CacheTierRank::L0,
    name: String::new(),
};

#[derive(Clone)]
pub struct Cache {
    max_size_bytes: i128,
    // TODO: make this configurable via a bucket-type system
    // similar to s3 with its subdomain and bucket prefixes
    url_base: String,
    storage: Arc<dyn StorageBackend>,
    metadata: Arc<dyn CacheMetadataBackend>,
    policy: CacheScoringPolicy,
}

impl Cache {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        metadata: Arc<dyn CacheMetadataBackend>,
        max_size_bytes: i128,
        url_base: String,
        policy: CacheScoringPolicy,
    ) -> Cache {
        Cache {
            max_size_bytes,
            storage,
            metadata,
            url_base,
            policy,
        }
    }
    /// gets a file from the cache, and pulls from source and puts into cache if it doesn't exist
    pub async fn get(
        self,
        k: String,
        range: Option<(u64, Option<u64>)>,
    ) -> anyhow::Result<(
        StreamBody<Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static>>>,
        hyper::HeaderMap,
        hyper::StatusCode,
    )> {
        let k1 = k.clone();
        if let Some(e) = self.metadata.get_metadata(&k1).await? {
            // File exists in cache, stream from disk
            if let Some(key) = e.key {
                // do we need to return a byterange?
                if let Some((s, e)) = range {
                    // TODO: check if we want to do range bites
                    return match self.storage.retrieve_range(&key, s, e).await {
                        Ok((stream, end)) => {
                            let stream = StreamBody::new(stream);
                            let mut headers = hyper::HeaderMap::new();
                            let fmt = &format!("bytes={:?}-{:?}", s, end);
                            headers.append("Range", fmt.parse().unwrap());
                            Ok((stream, headers, StatusCode::PARTIAL_CONTENT))
                        }
                        Err(_) => {
                            warn!(
                                "File could not be found on disk, attempting delete and redownload"
                            );
                            // purge cache entry
                            self.metadata.delete_metadata(&k1).await?;
                            let (stream, headers, statuscode) = self
                                .stream_url_to_disk_and_client(
                                    format!("{}{}", self.url_base, k),
                                    &k,
                                    range,
                                )
                                .await?;
                            Ok((StreamBody::new(stream), headers, statuscode))
                        }
                    };
                }
                match self.storage.retrieve(&key).await {
                    Err(_) => {
                        warn!("File could not be found on disk, attempting delete and redownload");
                        // purge cache entry
                        self.metadata.delete_metadata(&k1).await?;
                        let (stream, headers, statuscode) = self
                            .stream_url_to_disk_and_client(
                                format!("{}{}", self.url_base, k),
                                &k,
                                range,
                            )
                            .await?;
                        Ok((StreamBody::new(stream), headers, statuscode))
                    }
                    Ok(stream) => {
                        tokio::spawn(async move {
                            if let Err(e) = self.record_access(&key).await {
                                warn!("error recording access to {}: {}", &key, e);
                            }
                        });
                        // TODO: add headers for file types, etc.
                        Ok((
                            StreamBody::new(stream),
                            hyper::HeaderMap::new(),
                            StatusCode::OK,
                        ))
                    }
                }
            } else {
                // Need to download
                let (stream, headers, statuscode) = self
                    .stream_url_to_disk_and_client(format!("{}{}", self.url_base, k), &k, range)
                    .await?;
                Ok((StreamBody::new(stream), headers, statuscode))
            }
        } else {
            // Need to download - use stream_url_to_disk_and_client
            let (stream, headers, statuscode) = self
                .stream_url_to_disk_and_client(format!("{}{}", self.url_base, k), &k, range)
                .await?;
            Ok((StreamBody::new(stream), headers, statuscode))
        }
    }
    pub async fn put(&self, k: &str, size_bytes: i64) -> anyhow::Result<CacheEntry> {
        // save file to db
        self.metadata
            .put_metadata(k, size_bytes, DEFAULT_TIER)
            .await
    }

    pub async fn record_access(&self, k: &str) -> anyhow::Result<()> {
        self.metadata.record_access(k).await
    }

    /// Delete a file (flush it from cache and storage)
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        if let Err(e) = self.storage.delete(key).await {
            warn!("Failed to delete file: {}", e);
        }
        self.metadata.delete_metadata(key).await
    }

    pub async fn delete_with_prefix(&self, prefix: &String) -> anyhow::Result<()> {
        let fmt_prefix = format!("{}%", prefix);
        let entries = self.metadata.get_metadata_with_prefix(&fmt_prefix).await?;
        // TODO: parallelize
        for entry in entries {
            if let Err(e) = self.delete(&entry.key.unwrap()).await {
                warn!("Failed to delete file: {}", e);
            }
        }
        Ok(())
    }

    pub async fn ensure_space(&self, needed: i64) -> anyhow::Result<()> {
        let mut current_size = self.metadata.get_total_size().await?;
        if (current_size as i128 + needed as i128) > self.max_size_bytes {
            info!("cache full, purging enough to make space");
            let mut purged = 0;
            let mut purged_keys = self
                .metadata
                .get_entries_to_purge(needed, &self.policy)
                .await?;
            while (current_size as i128 + needed as i128) > self.max_size_bytes {
                // if we don't have any entries to purge, we can just break
                let Some(entry) = purged_keys.pop() else {
                    break;
                };
                if let Some(size) = entry.size {
                    current_size = current_size as i64 - size;
                    let key = entry.key.unwrap();
                    // delete actual file
                    if let Err(e) = self.storage.delete(&key).await {
                        warn!("Failed to delete file: {}", e);
                        // if we can't delete the file, we shan't purge it from the db
                        break;
                    }
                    self.metadata.delete_metadata(&key).await?;
                    purged += 1;
                }
            }
            info!("purged {} entries", purged);
        }
        Ok(())
    }
    pub async fn stream_url_to_disk_and_client(
        &self,
        url: String,
        name: &str,
        range: Option<(u64, Option<u64>)>, // Add range parameter (start, end)
    ) -> anyhow::Result<(
        Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
        hyper::HeaderMap,
        hyper::StatusCode,
    )> {
        let req = reqwest::get(&url).await?;
        let status = req.status();
        let headers = req.headers().clone();

        let mut hm = hyper::HeaderMap::new();

        // Handle non-success status codes (same as before)
        if !status.is_success()
            && status != StatusCode::PERMANENT_REDIRECT
            && status != StatusCode::MOVED_PERMANENTLY
            && status != StatusCode::NOT_MODIFIED
        {
            let stream = req
                .bytes_stream()
                .map_ok(|bytes| bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e));
            return Ok((Box::pin(stream), hm, status));
        }

        // Get content length for range validation
        let content_length = headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| anyhow::anyhow!("Missing Content-Length"))?;

        let range_response = headers.get("Range").and_then(|v| v.to_str().ok());
        info!("range_response: {:?}", range_response);

        // Validate the range
        let (start, end) = match range {
            Some((s, e)) => {
                let mut end = e.unwrap_or(content_length - 1);
                if s > end || end >= content_length {
                    info!("Invalid range, falling back to full content");
                    end = content_length - 1;
                }
                (s, end)
            }
            None => {
                // If no range requested, default to full content
                (0, content_length - 1)
            }
        };

        // set broadcast to 5kib
        let (tx, rx) = broadcast::channel::<Bytes>(65536);
        let storage = self.storage.clone();
        let name = name.to_string();

        // Clone for background tasks
        let self_clone = self.clone();
        let name_clone = name.clone();

        // Spawn ensure_space
        let ensure_space_handle = tokio::spawn(async move {
            if let Err(e) = self_clone.ensure_space(content_length as i64).await {
                error!("Failed to ensure cache space: {}", e);
                return Err(e);
            }
            if let Err(e) = self_clone.put(&name_clone, content_length as i64).await {
                error!("Failed to put cache entry: {}", e);
                return Err(e);
            }
            Ok(())
        });

        // Split the original stream into two:
        // 1. Save the FULL stream to disk
        // 2. Slice the RANGE for the client
        let original_stream = req
            .bytes_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

        // Spawn disk writer (full content)
        let disk_stream = original_stream
            .map_ok(move |bytes| {
                if tx.send(bytes.clone()).is_err() {
                    // do nothing for now
                    //warn!("Failed to send bytes to storage: {}", e);
                }
                bytes
            })
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
        let storage_stream = Box::pin(disk_stream);

        tokio::spawn(async move {
            if let Err(e) = ensure_space_handle.await.unwrap() {
                warn!("Cache space management failed: {}", e);
                return;
            }

            if let Err(e) = storage.store_streaming(&name, storage_stream).await {
                warn!("Failed to store stream: {}", e);
            }
        });

        // headers for client
        for (key, value) in headers.iter() {
            hm.insert(key, value.clone());
        }

        // Create client stream (sliced range)
        let client_stream = RangeStream {
            inner: tokio_stream::wrappers::BroadcastStream::new(rx)
                .filter_map(|x| x.ok()) // Ignore broadcast errors
                .map(Ok::<Bytes, std::io::Error>), // Convert Bytes to Result<Bytes, Error>
            current: 0,
            start,
            end,
        };

        Ok((Box::pin(client_stream), hm, status))
    }
}

struct RangeStream<S> {
    inner: S,
    current: u64,
    start: u64,
    end: u64,
}
impl<S> Stream for RangeStream<S>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while self.current <= self.end {
            match futures::ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                Some(Ok(chunk)) => {
                    let chunk_len = chunk.len() as u64;
                    let chunk_start = self.current;
                    let chunk_end = chunk_start + chunk_len;

                    // Check if this chunk contains the start of the requested range
                    if chunk_end <= self.start {
                        self.current = chunk_end;
                        continue;
                    }

                    // Check if we've already passed the end of the requested range
                    if chunk_start > self.end {
                        return Poll::Ready(None);
                    }

                    // Calculate slice offsets within this chunk
                    let start = self.start.max(chunk_start);
                    let end = self.end.min(chunk_end - 1);

                    let start_in_chunk = (start - chunk_start) as usize;
                    let end_in_chunk = (end - chunk_start + 1) as usize;

                    // Slice the chunk and update current offset
                    let sliced = chunk.slice(start_in_chunk..end_in_chunk);
                    self.current = chunk_end;

                    return Poll::Ready(Some(Ok(sliced)));
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => return Poll::Ready(None),
            }
        }
        Poll::Ready(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{metadata::MockCacheMetadataBackend, storage::MockStorageBackend};
    use futures::stream;

    #[tokio::test]
    async fn test_put_and_get() -> anyhow::Result<()> {
        let mut storage = crate::storage::MockStorageBackend::new();
        let mut metadata = crate::metadata::MockCacheMetadataBackend::new();

        // Setup expectations
        metadata.expect_put_metadata().returning(|key, size, _| {
            Box::pin(futures::future::ready(Ok(CacheEntry {
                key: Some(key.to_string()),
                size: Some(size),
                tier: DEFAULT_TIER.name.to_string(),
                date: Some(time::OffsetDateTime::now_utc()),
                last_access: Some(time::OffsetDateTime::now_utc()),
                times_accessed: Some(0),
                expiration: None,
                importance: Some(0),
            })))
        });

        metadata.expect_get_metadata().returning(|key| {
            Box::pin(futures::future::ready(Ok(Some(CacheEntry {
                key: Some(key.to_string()),
                size: Some(0),
                tier: DEFAULT_TIER.name.to_string(),
                date: Some(time::OffsetDateTime::now_utc()),
                last_access: Some(time::OffsetDateTime::now_utc()),
                times_accessed: Some(0),
                expiration: None,
                importance: Some(0),
            }))))
        });

        storage.expect_retrieve().returning(|_| {
            let stream = stream::once(futures::future::ok(Bytes::from("test data")));
            Box::pin(futures::future::ok(
                Box::pin(stream) as Pin<Box<dyn Stream<Item = Result<Bytes, _>> + Send>>
            ))
        });

        let cache = Cache::new(
            Arc::new(storage) as Arc<dyn StorageBackend>,
            Arc::new(metadata),
            1000,
            "http://example.com/".to_string(),
            CacheScoringPolicy::Lru,
        );

        // Test put
        let result = cache.put("test-key", 100).await?;
        assert_eq!(result.key, Some("test-key".to_string()));
        assert_eq!(result.size, Some(100));

        // Test get
        let (_, _, status) = cache.clone().get("test-key".to_string(), None).await?;
        assert_eq!(status, StatusCode::OK);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> anyhow::Result<()> {
        let mut storage = MockStorageBackend::new();
        let mut metadata = MockCacheMetadataBackend::new();

        storage
            .expect_delete()
            .returning(|_| Box::pin(futures::future::ready(Ok(()))));

        metadata
            .expect_delete_metadata()
            .returning(|_| Box::pin(futures::future::ready(Ok(()))));

        let cache = Cache::new(
            Arc::new(storage),
            Arc::new(metadata),
            1000,
            "http://example.com/".to_string(),
            CacheScoringPolicy::Lru,
        );

        let result = cache.delete("test-key").await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_range_request() -> anyhow::Result<()> {
        let mut storage = MockStorageBackend::new();
        let mut metadata = MockCacheMetadataBackend::new();

        metadata.expect_get_metadata().returning(|key| {
            Box::pin(futures::future::ready(Ok(Some(CacheEntry {
                key: Some(key.to_string()),
                size: Some(0),
                tier: DEFAULT_TIER.name.to_string(),
                date: Some(time::OffsetDateTime::now_utc()),
                last_access: Some(time::OffsetDateTime::now_utc()),
                times_accessed: Some(0),
                expiration: None,
                importance: Some(0),
            }))))
        });

        storage.expect_retrieve_range().returning(|_, _, _| {
            let stream = stream::once(futures::future::ok(Bytes::from("partial data")));
            Box::pin(futures::future::ok((
                Box::pin(stream)
                    as Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
                100,
            )))
        });

        let cache = Cache::new(
            Arc::new(storage),
            Arc::new(metadata),
            1000,
            "http://example.com/".to_string(),
            CacheScoringPolicy::Lru,
        );

        let (_, headers, status) = cache
            .clone()
            .get("test-key".to_string(), Some((0, Some(50))))
            .await?;

        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert!(headers.contains_key("Range"));

        Ok(())
    }

    #[tokio::test]
    async fn test_ensure_space() -> anyhow::Result<()> {
        let mut storage = MockStorageBackend::new();
        let mut metadata = MockCacheMetadataBackend::new();

        metadata
            .expect_get_total_size()
            .returning(|| Box::pin(futures::future::ready(Ok(800))));

        metadata.expect_get_entries_to_purge().returning(|_, _| {
            Box::pin(futures::future::ready(Ok(vec![CacheEntry {
                key: Some("old-entry".to_string()),
                size: Some(0),
                tier: DEFAULT_TIER.name.to_string(),
                date: Some(time::OffsetDateTime::now_utc()),
                last_access: Some(time::OffsetDateTime::now_utc()),
                times_accessed: Some(0),
                expiration: None,
                importance: Some(0),
            }])))
        });

        storage
            .expect_delete()
            .returning(|_| Box::pin(futures::future::ready(Ok(()))));

        metadata
            .expect_delete_metadata()
            .returning(|_| Box::pin(futures::future::ready(Err(anyhow::anyhow!("test")))));

        let cache = Cache::new(
            Arc::new(storage),
            Arc::new(metadata),
            1000,
            "http://example.com/".to_string(),
            CacheScoringPolicy::Lru,
        );

        let result = cache.ensure_space(300).await;
        let Err(e) = result else {
            panic!("Expected error");
        };
        println!("{}", &e);

        Ok(())
    }
}
