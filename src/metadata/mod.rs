use async_trait::async_trait;

use crate::structs::{CacheEntry, CacheTier};

pub mod memory;
pub mod sqlite;

/// Represents different ways to score cache entries for eviction
#[derive(Clone, Debug)]
#[allow(unused)]
pub enum CacheScoringPolicy {
    /// Basic LRU (Least Recently Used)
    Lru,
    /// Based on importance, size ratio, and age with configurable weights
    WeightedScore {
        importance_weight: f32,
        size_weight: f32,
        age_weight: f32,
    },
    /// Custom SQL scoring expression
    Custom(String),
}

#[async_trait]
#[mockall::automock]
pub trait CacheMetadataBackend: Send + Sync {
    /// Record a new cache entry in the metadata store
    async fn put_metadata(
        &self,
        key: &str,
        size_bytes: i64,
        tier: CacheTier,
    ) -> anyhow::Result<CacheEntry>;

    /// Record an access to a cached item
    async fn record_access(&self, key: &str) -> anyhow::Result<()>;

    /// Get metadata for a cached item
    async fn get_metadata(&self, key: &str) -> anyhow::Result<Option<CacheEntry>>;

    /// Delete metadata for a cached item
    async fn delete_metadata(&self, key: &str) -> anyhow::Result<()>;

    /// Delete all metadata entries matching a prefix
    async fn get_metadata_with_prefix(&self, prefix: &str) -> anyhow::Result<Vec<CacheEntry>>;

    /// Get the total size of all cached items
    async fn get_total_size(&self) -> anyhow::Result<i64>;

    /// Get entries to purge based on provided cache policy
    async fn get_entries_to_purge(
        &self,
        needed_space: i64,
        policy: &CacheScoringPolicy,
    ) -> anyhow::Result<Vec<CacheEntry>>;
}
