use bytes::Bytes;
use http_body::Frame;
use http_body_util::{combinators::BoxBody, BodyExt, StreamBody};
use hyper::{HeaderMap, StatusCode};
use reqwest::Response;
use std::{
    error::Error as StdError,
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
}; // Use hyper types directly

use futures::{Stream, TryStreamExt};
use sqlx::SqlitePool;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;

use crate::{
    metadata::{CacheMetadataBackend, CacheScoringPolicy},
    storage::StorageBackend,
    structs::{CacheEntry, CacheTier, CacheTierRank},
};

type CacheBoxBody = BoxBody<Bytes, Box<dyn StdError + Send + Sync + 'static>>;

use tracing::{debug, error, info, warn};
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
    //db_pool: Arc<SqlitePool>, // Keep pool for access recording if needed elsewhere
}

impl Cache {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        metadata: Arc<dyn CacheMetadataBackend>,
        max_size_bytes: i128,
        url_base: String,
        policy: CacheScoringPolicy,
        _db_pool: Arc<SqlitePool>,
    ) -> Cache {
        Cache {
            max_size_bytes,
            storage,
            metadata,
            url_base,
            policy,
            //db_pool,
        }
    }
    /// gets a file from the cache, and pulls from source and puts into cache if it doesn't exist
    // Inside yaiba/src/cache.rs

    pub async fn get(
        &self,
        k: String,
        range: Option<(u64, Option<u64>)>,
        _last_modified_query: Option<String>,
        // ) -> anyhow::Result<(StreamBody, HeaderMap, StatusCode)> { // OLD (StreamBody isn't quite right, and inconsistent with download_file branch)
    ) -> anyhow::Result<(CacheBoxBody, HeaderMap, StatusCode)> {
        // NEW - Use CacheBoxBody consistently
        let metadata = self.metadata.get_metadata(&k).await?;

        let key = match metadata.and_then(|e| e.key) {
            None => {
                // download_file now returns the correct tuple type
                return self.download_file(&k, range).await;
            }
            Some(key) => key,
        };

        if let Some((start, end)) = range {
            // handle_range_request now returns the correct tuple type
            return self.handle_range_request(&k, &key, start, end).await;
        }

        match self.storage.retrieve(&key).await {
            Ok(stream) => {
                self.spawn_access_recorder(&k);
                // Convert stream to BoxBody
                let body = StreamBody::new(
                    stream
                        .map_err(|e| Box::new(e) as Box<dyn StdError + Send + Sync + 'static>)
                        .map_ok(Frame::data),
                )
                .boxed();
                Ok((
                    body,
                    HeaderMap::new(), // You might want more headers here
                    StatusCode::OK,
                ))
            }
            Err(_) => {
                warn!("File could not be found on disk, attempting delete and redownload");
                self.metadata.delete_metadata(&k).await?;
                // download_file now returns the correct tuple type
                self.download_file(&k, range).await
            }
        }
    }

    async fn download_file(
        &self,
        k: &str,
        range: Option<(u64, Option<u64>)>,
        // ) -> anyhow::Result<(BoxedResponse, HeaderMap, StatusCode)> { // OLD
    ) -> anyhow::Result<(CacheBoxBody, HeaderMap, StatusCode)> {
        // NEW - Return tuple with CacheBoxBody
        let (stream, headers, statuscode) = self
            .stream_url_to_disk_and_client(format!("{}{}", self.url_base, k), k, range)
            .await?;

        // Convert the stream into a BoxBody
        // Map the std::io::Error from the stream to a Box<dyn StdError...>
        let body = StreamBody::new(
            stream
                .map_err(|e| Box::new(e) as Box<dyn StdError + Send + Sync + 'static>)
                .map_ok(Frame::data),
        )
        .boxed(); // Box it into CacheBoxBody

        // Return the tuple, not a full response
        Ok((body, headers, statuscode))
    }

    async fn handle_range_request(
        &self,
        k: &str,   // Add original key 'k' for recording access
        key: &str, // key from metadata (actual storage key)
        start: u64,
        end: Option<u64>,
    ) -> anyhow::Result<(CacheBoxBody, HeaderMap, StatusCode)> {
        match self.storage.retrieve_range(key, start, end).await {
            Ok((stream, end, size)) => {
                let mut headers = HeaderMap::new();
                // For further reference:
                // If there's an equals sign here, it breaks ffmpeg :)
                let fmt = &format!("bytes {:?}-{:?}/{:?}", start, end, size);
                headers.append(
                    "content-length",
                    format!("{}", end - start + 1).parse().unwrap(),
                );
                headers.append("content-range", fmt.parse().unwrap());
                self.spawn_access_recorder(k); // Use original key `k` for metadata
                let body = StreamBody::new(
                    // Assuming retrieve_range stream is already Result<Bytes, io::Error>
                    stream
                        .map_err(|e| Box::new(e) as Box<dyn StdError + Send + Sync + 'static>)
                        .map_ok(Frame::data),
                )
                .boxed(); // Box it
                Ok((body, headers, StatusCode::PARTIAL_CONTENT))
            }
            Err(_) => {
                warn!("File could not be found on disk, attempting delete and redownload");
                self.metadata.delete_metadata(k).await?;
                self.download_file(k, Some((start, end))).await
            }
        }
    }

    fn spawn_access_recorder(&self, key: &str) {
        let key = key.to_string();
        let metadata_clone = self.metadata.clone(); // Clone only metadata Arc
        tokio::spawn(async move {
            if let Err(e) = metadata_clone.record_access(&key).await {
                // Call directly on metadata
                warn!("error recording access to {}: {}", &key, e);
            }
        });
    }

    pub async fn put(
        &self,
        k: &str,
        size_bytes: i64,
        content_type: Option<String>,
    ) -> anyhow::Result<CacheEntry> {
        // save file to db
        self.metadata
            .put_metadata(k, size_bytes, DEFAULT_TIER, content_type)
            .await
    }

    // Removed record_access as it's now called directly via metadata in spawn_access_recorder

    /// Delete a file (flush it from cache and storage)
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        if let Err(e) = self.storage.delete(key).await {
            warn!("Failed to delete file: {}", e);
        }
        self.metadata.delete_metadata(key).await
    }

    /// Delete all files with a given prefix
    pub async fn delete_with_prefix(&self, prefix: &String) -> anyhow::Result<()> {
        let fmt_prefix = format!("{}%", prefix);
        let entries = self.metadata.get_metadata_with_prefix(&fmt_prefix).await?;
        // TODO: parallelize
        for entry in entries {
            if let Some(key) = entry.key {
                // Ensure key exists
                if let Err(e) = self.delete(&key).await {
                    warn!("Failed to delete file {}: {}", key, e);
                    // Decide if you want to continue or return error
                }
            }
        }
        Ok(())
    }

    /// Ensure that the cache has at least the given amount of space available
    pub async fn ensure_space(&self, needed: i64) -> anyhow::Result<()> {
        // Add check for non-positive needed space, although unlikely with current logic
        if needed <= 0 {
            return Ok(());
        }

        let current_size = self.metadata.get_total_size().await?;
        let max_size = self.max_size_bytes as i64; // Use i64 for comparison

        // if we don't have enough space, purge enough to make space
        // Calculate space_to_free: ensure it's non-negative
        let space_to_free = (current_size + needed - max_size).max(0);

        if space_to_free > 0 {
            info!(
                current_size = current_size,
                needed = needed,
                max_size = max_size,
                freeing = space_to_free,
                "Cache full or nearing capacity, purging entries."
            );
            let mut purged_count = 0;
            let mut freed_space: i64 = 0;
            // Fetch slightly more than needed just in case? Or iterate until enough freed.
            // get_entries_to_purge needs amount to free, not the size of the incoming file
            let mut entries_to_purge = self
                .metadata
                .get_entries_to_purge(space_to_free, &self.policy)
                .await?;

            // Purge until enough space is freed
            while freed_space < space_to_free {
                let Some(entry) = entries_to_purge.pop() else {
                    warn!(
                        needed = space_to_free,
                        freed = freed_space,
                        "Could not free enough space, not enough purgeable entries found."
                    );
                    // We couldn't free enough space. Return an error? Or proceed anyway?
                    // Proceeding risks exceeding max size, returning error prevents caching the new item.
                    // Let's return an error for now to be strict about max_size.
                    return Err(anyhow::anyhow!(
                        "Failed to free up required cache space (needed: {}, freed: {})",
                        space_to_free,
                        freed_space
                    ));
                };

                if let Some(size) = entry.size {
                    if size <= 0 {
                        continue;
                    } // Skip weird entries
                    let Some(key) = entry.key else { continue }; // Skip if key is missing

                    // delete actual file
                    debug!(key = %key, size = size, "Purging entry");
                    if let Err(e) = self.storage.delete(&key).await {
                        warn!(key = %key, "Failed to delete file during purge: {}", e);
                        // If storage deletion fails, we *cannot* count this space as freed
                        // and shouldn't delete the metadata.
                        continue; // Try the next file
                    }

                    // Only delete metadata if storage deletion was successful
                    if let Err(e) = self.metadata.delete_metadata(&key).await {
                        warn!(key = %key, "Failed to delete metadata for purged file: {}", e);
                    }

                    freed_space += size;
                    purged_count += 1;
                }
            }
            info!(
                purged_count = purged_count,
                freed_space = freed_space,
                "Purged entries successfully."
            );
        }
        Ok(())
    }

    /// Stream a file, given that the response passed in contains the whole file content
    pub async fn stream_url_one_shot(
        &self,
        res: Response,
        name: &str,
    ) -> anyhow::Result<(
        Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync>>,
        HeaderMap,
        StatusCode,
    )> {
        debug!("Streaming file {}", name);
        let headers = res.headers().clone();
        let status = res.status();
        let bytes = res.bytes().await?;
        let content_type = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string());
        let mut hm = hyper::HeaderMap::new();

        let storage = self.storage.clone();
        let name = name.to_string();

        // Clone for background tasks
        let self_clone = self.clone();
        let name_clone = name.clone();
        let content_type_clone = content_type.clone(); // Clone content_type

        let content_length = bytes.len();

        // Spawn ensure_space and put_metadata
        let ensure_space_handle = tokio::spawn(async move {
            if let Err(e) = self_clone.ensure_space(content_length as i64).await {
                error!("Failed to ensure cache space: {}", e);
                return Err(e); // Propagate error
            }
            if let Err(e) = self_clone
                .put(&name_clone, content_length as i64, content_type_clone) // Use cloned content_type
                .await
            {
                error!("Failed to put cache entry: {}", e);
                return Err(e); // Propagate error
            }
            Ok(())
        });

        let self_clone = self.clone();
        // todo: find a better option for this (maybe make bytes into a stream?)
        let bytes_clone = bytes.clone();

        tokio::spawn(async move {
            // Wait for ensure_space and put to complete successfully
            if let Err(e) = ensure_space_handle.await? {
                // Handle JoinError and inner Result
                warn!("Cache space management or metadata put failed: {}", e);
                return Ok::<(), io::Error>(());
            }

            if let Err(e) = storage.store(&name, bytes_clone).await {
                warn!("Failed to store stream: {}", e);
                // delete metadata if storage fails
                if let Err(del_err) = self_clone.metadata.delete_metadata(&name).await {
                    warn!(
                        "Failed to delete metadata after storage failure for {}: {}",
                        name, del_err
                    );
                }
            }
            Ok(())
        });

        // headers for client
        for (key, value) in headers.iter() {
            hm.insert(key, value.clone());
        }

        Ok((
            Box::pin(futures::stream::once(async move { Ok(bytes) })),
            headers,
            status,
        ))
    }

    /// Stream a file from a URL, split into two streams: one for the storage backend, one for the client
    pub async fn stream_url_to_disk_and_client(
        &self,
        url: String,
        name: &str,
        range: Option<(u64, Option<u64>)>, // Add range parameter (start, end)
    ) -> anyhow::Result<(
        Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync>>,
        HeaderMap,
        StatusCode,
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
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e)); // Wrap error
            return Ok((Box::pin(stream), hm, status));
        }

        debug!("status code {}", status);

        if status != StatusCode::PARTIAL_CONTENT {
            return self.stream_url_one_shot(req, &name).await;
        }

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

        // Get content-type for later storage
        // TODO: there's probably a much better way to do HeaderValue -> String
        let content_type = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string());

        // set broadcast to 5kib
        let (tx, rx) = broadcast::channel::<Bytes>(65536);
        let storage = self.storage.clone();
        let name = name.to_string();

        // Clone for background tasks
        let self_clone = self.clone();
        let name_clone = name.clone();
        let content_type_clone = content_type.clone(); // Clone content_type

        // Spawn ensure_space and put_metadata
        let ensure_space_handle = tokio::spawn(async move {
            if let Err(e) = self_clone.ensure_space(content_length as i64).await {
                error!("Failed to ensure cache space: {}", e);
                return Err(e); // Propagate error
            }
            if let Err(e) = self_clone
                .put(&name_clone, content_length as i64, content_type_clone) // Use cloned content_type
                .await
            {
                error!("Failed to put cache entry: {}", e);
                return Err(e); // Propagate error
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

        let self_clone = self.clone();

        tokio::spawn(async move {
            // Wait for ensure_space and put to complete successfully
            if let Err(e) = ensure_space_handle.await? {
                // Handle JoinError and inner Result
                warn!("Cache space management or metadata put failed: {}", e);
                return Ok::<(), io::Error>(());
            }

            if let Err(e) = storage.store_streaming(&name, storage_stream).await {
                warn!("Failed to store stream: {}", e);
                // delete metadata if storage fails
                if let Err(del_err) = self_clone.metadata.delete_metadata(&name).await {
                    warn!(
                        "Failed to delete metadata after storage failure for {}: {}",
                        name, del_err
                    );
                }
            }
            Ok(())
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
    use sqlx::sqlite::SqlitePoolOptions; // Add for mock pool

    // Helper to create a mock pool (in-memory)
    async fn mock_pool() -> Arc<SqlitePool> {
        Arc::new(
            SqlitePoolOptions::new()
                .connect("sqlite::memory:")
                .await
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_put_and_get() -> anyhow::Result<()> {
        let mut storage = crate::storage::MockStorageBackend::new();
        let mut metadata = crate::metadata::MockCacheMetadataBackend::new();
        let pool = mock_pool().await;

        // Setup expectations
        metadata
            .expect_put_metadata()
            .returning(|key, size, _, ctype| {
                Box::pin(futures::future::ready(Ok(CacheEntry {
                    key: Some(key.to_string()),
                    size: Some(size),
                    tier: DEFAULT_TIER.name.to_string(),
                    date: Some(time::OffsetDateTime::now_utc()),
                    last_access: Some(time::OffsetDateTime::now_utc()),
                    times_accessed: Some(0),
                    expiration: None,
                    content_type: ctype,
                    importance: Some(0),
                })))
            });

        metadata
            .expect_get_metadata()
            .returning(|key| {
                Box::pin(futures::future::ready(Ok(Some(CacheEntry {
                    key: Some(key.to_string()),
                    size: Some(0), // Size doesn't matter for this part of the test
                    tier: DEFAULT_TIER.name.to_string(),
                    date: Some(time::OffsetDateTime::now_utc()),
                    last_access: Some(time::OffsetDateTime::now_utc()),
                    times_accessed: Some(0),
                    expiration: None,
                    content_type: None,
                    importance: Some(0),
                }))))
            })
            .times(1); // Expect get_metadata once

        storage
            .expect_retrieve()
            .returning(|_| {
                let stream = stream::once(futures::future::ok(Bytes::from("test data")));
                Box::pin(futures::future::ok(
                    Box::pin(stream) as Pin<Box<dyn Stream<Item = Result<Bytes, _>> + Send + Sync>>
                ))
            })
            .times(1); // Expect retrieve once

        // Expect record_access to be called after successful retrieve
        metadata
            .expect_record_access()
            .returning(|_| Box::pin(async { Ok(()) }))
            .times(1); // Expect record_access once

        let cache = Cache::new(
            Arc::new(storage) as Arc<dyn StorageBackend>,
            Arc::new(metadata),
            1000,
            "http://example.com/".to_string(),
            CacheScoringPolicy::Lru,
            pool, // Pass mock pool
        );

        // Test put
        let result = cache.put("test-key", 100, None).await?;
        assert_eq!(result.key, Some("test-key".to_string()));
        assert_eq!(result.size, Some(100));

        // Test get
        let cache_clone = Arc::new(cache); // Clone Arc for get call
        let (_, _, status) = cache_clone.get("test-key".to_string(), None, None).await?;
        assert_eq!(status, StatusCode::OK);

        Ok(())
    }

    #[tokio::test]
    async fn test_delete() -> anyhow::Result<()> {
        let mut storage = MockStorageBackend::new();
        let mut metadata = MockCacheMetadataBackend::new();
        let pool = mock_pool().await;

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
            pool,
        );

        let result = cache.delete("test-key").await;
        assert!(result.is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_range_request() -> anyhow::Result<()> {
        let mut storage = MockStorageBackend::new();
        let mut metadata = MockCacheMetadataBackend::new();
        let pool = mock_pool().await;

        metadata
            .expect_get_metadata()
            .returning(|key| {
                Box::pin(futures::future::ready(Ok(Some(CacheEntry {
                    key: Some(key.to_string()), // Return the storage key
                    size: Some(300),            // Example size
                    tier: DEFAULT_TIER.name.to_string(),
                    date: Some(time::OffsetDateTime::now_utc()),
                    last_access: Some(time::OffsetDateTime::now_utc()),
                    times_accessed: Some(0),
                    expiration: None,
                    content_type: None,
                    importance: Some(0),
                }))))
            })
            .times(1);

        storage
            .expect_retrieve_range()
            .returning(|_key, start, _end| {
                // Use _key as it's checked via metadata
                let data = Bytes::from("partial data for range request");
                let actual_end = start + data.len() as u64 - 1; // Calculate end based on data len
                let total_size = 300; // Match metadata size
                let stream = stream::once(futures::future::ok(data));
                Box::pin(futures::future::ok((
                    Box::pin(stream)
                        as Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync>>,
                    actual_end,
                    total_size,
                )))
            })
            .times(1);

        // Add expectation for record_access
        metadata
            .expect_record_access()
            .withf(|k| k == "test-key") // Ensure correct key
            .returning(|_| Box::pin(async { Ok(()) }))
            .times(1);

        let cache = Cache::new(
            Arc::new(storage),
            Arc::new(metadata),
            1000,
            "http://example.com/".to_string(),
            CacheScoringPolicy::Lru,
            pool,
        );

        let cache_clone = Arc::new(cache);
        let (_, headers, status) = cache_clone
            .get("test-key".to_string(), Some((0, Some(50))), None) // Request range 0-50
            .await?;

        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert!(headers.contains_key("content-range"));
        assert!(headers.contains_key("content-length")); // Should be present for ranges
                                                         // Optionally check the specific range/length values if needed

        Ok(())
    }

    #[tokio::test]
    async fn test_ensure_space() -> anyhow::Result<()> {
        let mut storage = MockStorageBackend::new();
        let mut metadata = MockCacheMetadataBackend::new();
        let pool = mock_pool().await;

        metadata
            .expect_get_total_size()
            .returning(|| Box::pin(futures::future::ready(Ok(800))));

        metadata
            .expect_get_entries_to_purge()
            .returning(|needed, _| {
                assert_eq!(needed, 300); // Verify the needed space calculation
                Box::pin(futures::future::ready(Ok(vec![
                    CacheEntry {
                        key: Some("old-entry-1".to_string()),
                        size: Some(200), // Size to be purged
                        tier: DEFAULT_TIER.name.to_string(),
                        date: Some(time::OffsetDateTime::now_utc()),
                        last_access: Some(time::OffsetDateTime::now_utc()),
                        times_accessed: Some(0),
                        content_type: None,
                        expiration: None,
                        importance: Some(0),
                    },
                    CacheEntry {
                        // Need to purge another one
                        key: Some("old-entry-2".to_string()),
                        size: Some(150), // This should be enough
                        tier: DEFAULT_TIER.name.to_string(),
                        date: Some(time::OffsetDateTime::now_utc()),
                        last_access: Some(time::OffsetDateTime::now_utc()),
                        times_accessed: Some(0),
                        content_type: None,
                        expiration: None,
                        importance: Some(0),
                    },
                ])))
            })
            .times(1);

        // Expect delete for both entries that need purging
        storage
            .expect_delete()
            .withf(|k| k == "old-entry-1" || k == "old-entry-2")
            .times(2) // Expect 2 deletions
            .returning(|_| Box::pin(futures::future::ready(Ok(()))));

        metadata
            .expect_delete_metadata()
            .withf(|k| k == "old-entry-1" || k == "old-entry-2")
            .times(2) // Expect 2 metadata deletions
            .returning(|_| Box::pin(futures::future::ready(Ok(())))); // Simulate success

        let cache = Cache::new(
            Arc::new(storage),
            Arc::new(metadata),
            1000, // Max size
            "http://example.com/".to_string(),
            CacheScoringPolicy::Lru,
            pool,
        );

        // Need 300, current 800, max 1000. Need to free at least 100.
        // Purge policy returns 200 + 150. Should purge both.
        let result = cache.ensure_space(300).await;
        assert!(result.is_ok()); // Expect success now

        Ok(())
    }
}
