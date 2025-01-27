use std::{io, pin::Pin};

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};
use tracing::warn;

use super::StorageBackend;

pub struct FilesystemStorage {
    base_path: String,
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
        mut stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
    ) -> anyhow::Result<()> {
        let path = format!("{}/{}", self.base_path, key);
        tokio::spawn(async move {
            let mut file = File::create(&path).await?;
            let mut result = Ok(());
            while let Some(item) = stream.next().await {
                match item {
                    Ok(bytes) => {
                        if let Err(e) = file.write_all(&bytes).await {
                            result = Err(e);
                            break;
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
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    warn!("Failed to cleanup cache file: {}", e);
                }
            }

            result
        });
        Ok(())
    }

    async fn retrieve(
        &self,
        key: &str,
    ) -> anyhow::Result<Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>> {
        let path = format!("{}/{}", self.base_path, key);
        let stream = file_to_stream(&path).await?;
        Ok(Box::pin(stream))
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let path = format!("{}/{}", self.base_path, key);
        tokio::fs::remove_file(path).await?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        let path = format!("{}/{}", self.base_path, key);
        Ok(tokio::fs::metadata(path).await.is_ok())
    }
}
