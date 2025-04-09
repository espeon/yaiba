use std::{io, pin::Pin};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{fs::File, io::AsyncSeekExt};
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};
use tracing::{info, warn};

use super::StorageBackend;

pub struct FilesystemStorage {
    base_path: String,
}

pub async fn create_dir(path: &str) -> io::Result<()> {
    let path = std::path::Path::new(path);
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            tokio::fs::create_dir_all(parent).await?;
        }
    }
    Ok(())
}

pub async fn file_to_stream(
    path: &str,
) -> io::Result<impl futures::Stream<Item = io::Result<Bytes>>> {
    let file = File::open(path).await?;
    let stream =
        FramedRead::new(file, BytesCodec::new()).map(|res| res.map(|bytes_mut| bytes_mut.freeze())); // Convert BytesMut to Bytes
    Ok(stream)
}

impl FilesystemStorage {
    pub fn new(base_path: String) -> Self {
        Self { base_path }
    }
    async fn file_to_stream_range(
        path: &str,
        start: u64,
        end: Option<u64>,
    ) -> io::Result<(impl Stream<Item = io::Result<Bytes>>, u64, u64)> {
        let mut file = File::open(path).await?;
        let file_size = file.metadata().await?.len();

        let end = end.unwrap_or(file_size - 1).min(file_size - 1);
        if start >= file_size || start > end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid range specified",
            ));
        }

        file.seek(io::SeekFrom::Start(start)).await?;
        let limited_reader = file.take(end - start + 1);

        Ok((
            FramedRead::new(limited_reader, BytesCodec::new())
                .map(|res| res.map(|bytes_mut| bytes_mut.freeze())),
            end,
            file_size,
        ))
    }
}

#[async_trait]
impl StorageBackend for FilesystemStorage {
    async fn store(&self, key: &str, data: Bytes) -> anyhow::Result<()> {
        let path = format!("{}/{}", self.base_path, key);
        let mut file = File::create(&path).await?;
        file.write_all(&data).await?;
        Ok(())
    }

    async fn store_streaming(
        &self,
        key: &str,
        mut stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync>>,
    ) -> anyhow::Result<i64> {
        let path = format!("{}/{}", self.base_path, key);
        info!("Storing cache file: {}", path);
        create_dir(&path).await?; // Propagate error using ?
        let mut file = File::create(&path).await?; // Propagate error using ?

        let mut loop_error: Option<anyhow::Error> = None;
        let mut total_bytes: i64 = 0; // Use i64 to match return type

        while let Some(item) = stream.next().await {
            match item {
                Ok(bytes) => {
                    let bytes_len = bytes.len() as i64; // Cast usize to i64
                    if let Err(e) = file.write_all(&bytes).await {
                        // Store the error as anyhow::Error and break
                        loop_error = Some(e.into());
                        break;
                    }
                    total_bytes += bytes_len;
                }
                Err(e) => {
                    // Store the stream error as anyhow::Error and break
                    loop_error = Some(e.into());
                    break;
                }
            }
        }

        // Ensure data is flushed before checking error or returning success
        // Only flush if no error occurred during the loop
        if loop_error.is_none() {
            if let Err(e) = file.flush().await {
                // If flush fails, capture that as the error
                loop_error = Some(e.into());
            }
        }

        // Drop the file handle explicitly to close it before potential removal
        drop(file);

        if let Some(err) = loop_error {
            // An error occurred, attempt cleanup
            warn!(
                "Error storing stream to cache file '{}', attempting cleanup.",
                path
            );
            if let Err(e) = tokio::fs::remove_file(&path).await {
                warn!("Failed to cleanup cache file '{}' after error: {}", path, e);
            } else {
                info!("Cleaned up cache file '{}' after error.", path);
            }
            // Return the captured error
            Err(err)
        } else {
            // No error occurred, return success
            info!(
                "Cache file stored successfully: {} ({} bytes)",
                path, total_bytes
            );
            Ok(total_bytes)
        }
    }

    async fn retrieve(
        &self,
        key: &str,
    ) -> anyhow::Result<Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync>>>
    {
        let path = format!("{}/{}", self.base_path, key);
        info!("Retrieving cache file: {}", path);
        let stream = file_to_stream(&path).await?;
        Ok(Box::pin(stream))
    }

    async fn retrieve_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
    ) -> anyhow::Result<(
        Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync>>,
        u64,
        u64,
    )> {
        let path = format!("{}/{}", self.base_path, key);
        info!(
            "Retrieving cache file range: {} ({}-{:?})",
            path, start, end
        );
        // Returns a stream and the end of the range
        let (stream, r_end, size) = Self::file_to_stream_range(&path, start, end).await?;
        info!(
            "Stream range: start={}, end={}, size={}",
            start, r_end, size
        );
        Ok((Box::pin(stream), r_end, size))
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let path = format!("{}/{}", self.base_path, key);
        info!("Deleting cache file: {}", path);
        tokio::fs::remove_file(path).await?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        let path = format!("{}/{}", self.base_path, key);
        Ok(tokio::fs::metadata(path).await.is_ok())
    }
}
