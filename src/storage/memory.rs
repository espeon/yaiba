use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{stream, Stream};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;

use super::StorageBackend;

pub struct MemoryStorage {
    data: Arc<RwLock<DashMap<String, Bytes>>>,
}

#[async_trait]
impl StorageBackend for MemoryStorage {
    async fn store(&self, key: &str, data: Bytes) -> anyhow::Result<()> {
        let storage = self.data.write().await;
        storage.insert(key.to_string(), data);
        Ok(())
    }

    async fn store_streaming(
        &self,
        key: &str,
        mut stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
    ) -> anyhow::Result<()> {
        let storage = self.data.write().await;
        let mut data = Vec::new();
        while let Some(item) = stream.next().await {
            match item {
                Ok(bytes) => {
                    data.push(bytes);
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(e));
                }
            }
        }
        storage.insert(
            key.to_string(),
            Bytes::copy_from_slice(&data.into_iter().flatten().collect::<Vec<u8>>()),
        );
        Ok(())
    }

    async fn retrieve(
        &self,
        key: &str,
    ) -> anyhow::Result<Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>> {
        let storage = self.data.read().await;
        let data_val = storage.get(key);
        if let Some(data) = data_val {
            let data = data.value().clone();
            // Create a stream that yields a single Ok(Bytes) result
            let stream = stream::once(async move { Ok(data) });
            Ok(Box::pin(stream))
        } else {
            Err(anyhow::anyhow!("Key not found"))
        }
    }

    // TODO: implement range retrieval for MemoryStorage!
    async fn retrieve_range(
        &self,
        _: &str,
        _: u64,
        _: Option<u64>,
    ) -> anyhow::Result<(
        Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
        i64,
    )> {
        todo!("not implemented")
    }

    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let storage = self.data.write().await;
        storage.remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        let storage = self.data.read().await;
        Ok(storage.contains_key(key))
    }
}
