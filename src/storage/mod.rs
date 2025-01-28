use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use mockall::automock;
use std::pin::Pin;

pub mod fs;
pub mod memory;

#[async_trait]
#[allow(unused)]
#[automock]
pub trait StorageBackend: Send + Sync + 'static {
    /// Stores data in the storage backend with the given key.
    async fn store(&self, key: &str, data: Bytes) -> anyhow::Result<()>;
    async fn store_streaming(
        &self,
        key: &str,
        mut stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
    ) -> anyhow::Result<()>;
    /// Retrieves data from the storage backend with the given key.
    /// Returns a bytestream meant to be piped to the client.
    async fn retrieve(
        &self,
        key: &str,
    ) -> anyhow::Result<Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>>;
    async fn retrieve_range(
        &self,
        key: &str,
        start: u64,
        end: Option<u64>,
    ) -> anyhow::Result<(
        Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
        i64,
    )>;
    /// Deletes data from the storage backend with the given key.
    async fn delete(&self, key: &str) -> anyhow::Result<()>;
    /// Checks if data with the given key exists in the storage backend.
    async fn exists(&self, key: &str) -> anyhow::Result<bool>;
}
