use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use futures::{stream, Stream};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;

use crate::structs::CacheEntry;

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
        mut stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync>>,
    ) -> anyhow::Result<i64> {
        let storage = self.data.write().await;
        let mut data = Vec::new();
        let mut bytes_count = 0;
        while let Some(item) = stream.next().await {
            match item {
                Ok(bytes) => {
                    bytes_count += bytes.len() as i64;
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
        Ok(bytes_count)
    }

    async fn retrieve(
        &self,
        entry: &CacheEntry,
    ) -> anyhow::Result<Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync>>>
    {
        let storage = self.data.read().await;
        let key = entry.key.as_ref().expect("Cache entry should have a key");
        let data_val = storage.get(key);
        if let Some(data) = data_val {
            let data = data.value().clone();
            let stream = stream::once(async move { Ok(data) });
            Ok(Box::pin(stream))
        } else {
            Err(anyhow::anyhow!("Key not found"))
        }
    }

    async fn retrieve_range(
        &self,
        _entry: &CacheEntry,
        _start: u64,
        _end: Option<u64>,
    ) -> anyhow::Result<(
        Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync>>,
        u64,
        u64,
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
