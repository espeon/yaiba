use axum::async_trait;
use dashmap::DashMap;
use time::OffsetDateTime;

use crate::structs::{CacheEntry, CacheTier};

use super::{CacheMetadataBackend, CacheScoringPolicy};

/// A DashMap-backed cache metadata backend. (hi acri!)
/// This is useful for testing, high-performance use-cases, or for when you don't need persistence.
pub struct InMemoryCacheMetadata {
    entries: DashMap<String, CacheEntry>,
}

#[allow(unused)]
impl InMemoryCacheMetadata {
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
        }
    }
}

#[async_trait]
impl CacheMetadataBackend for InMemoryCacheMetadata {
    async fn put_metadata(
        &self,
        key: &str,
        size_bytes: i64,
        tier: CacheTier,
    ) -> anyhow::Result<CacheEntry> {
        let entry = CacheEntry {
            key: Some(key.to_string()),
            size: Some(size_bytes),
            date: Some(OffsetDateTime::now_utc()),
            last_access: Some(OffsetDateTime::now_utc()),
            times_accessed: Some(0),
            expiration: None,
            importance: Some(0),
            tier: tier.to_string(),
        };

        self.entries.insert(key.to_string(), entry.clone());
        Ok(entry)
    }

    async fn record_access(&self, key: &str) -> anyhow::Result<()> {
        if let Some(mut entry) = self.entries.get_mut(key) {
            entry.last_access = Some(OffsetDateTime::now_utc());
            entry.times_accessed = Some(entry.times_accessed.unwrap_or(0) + 1);
        }
        Ok(())
    }

    async fn get_metadata(&self, key: &str) -> anyhow::Result<Option<CacheEntry>> {
        Ok(self.entries.get(key).map(|entry| entry.clone()))
    }

    async fn delete_metadata(&self, key: &str) -> anyhow::Result<()> {
        self.entries.remove(key);
        Ok(())
    }

    async fn get_metadata_with_prefix(&self, prefix: &str) -> anyhow::Result<Vec<CacheEntry>> {
        let mut entries = Vec::new();
        for item in self.entries.iter() {
            if item.key.as_deref().unwrap().starts_with(prefix) {
                entries.push(item.clone());
            }
        }
        Ok(entries)
    }

    async fn get_total_size(&self) -> anyhow::Result<i64> {
        Ok(self.entries.iter().filter_map(|entry| entry.size).sum())
    }

    async fn get_entries_to_purge(
        &self,
        needed_space: i64,
        policy: &CacheScoringPolicy,
    ) -> anyhow::Result<Vec<CacheEntry>> {
        let now = OffsetDateTime::now_utc();

        // Convert DashMap entries to a vector for sorting
        let mut entries: Vec<_> = self.entries.iter().map(|entry| entry.clone()).collect();

        // Calculate max size once before sorting
        let max_size = entries.iter().filter_map(|e| e.size).max().unwrap_or(1) as f32;

        match policy {
            CacheScoringPolicy::Lru => {
                entries.sort_by_key(|entry| entry.last_access);
            }
            CacheScoringPolicy::WeightedScore {
                importance_weight,
                size_weight,
                age_weight,
            } => {
                entries.sort_by(|a, b| {
                    let score_entry = |e: &CacheEntry| {
                        let importance = e.importance.unwrap_or(0) as f32;
                        let size_ratio = e.size.unwrap_or(0) as f32 / max_size;
                        let age_days = e.date.map(|d| (now - d).whole_days() as f32).unwrap_or(0.0);

                        (importance * importance_weight)
                            + ((1.0 - size_ratio) * size_weight)
                            + (1.0 / (age_days + 1.0) * age_weight)
                    };

                    score_entry(a).partial_cmp(&score_entry(b)).unwrap()
                });
            }
            CacheScoringPolicy::Custom(_) => {
                return Err(anyhow::anyhow!(
                    "Custom scoring not supported for in-memory cache"
                ));
            }
        }

        // Return entries until we have enough space
        let mut space_freed = 0;
        let mut result = Vec::new();

        for entry in entries {
            if let Some(size) = entry.size {
                result.push(entry);
                space_freed += size;
                if space_freed >= needed_space {
                    break;
                }
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use crate::structs::CacheTierRank;

    use super::*;

    const DEFAULT_TIER: CacheTier = CacheTier {
        rank: CacheTierRank::L0,
        name: String::new(),
    };

    #[tokio::test]
    async fn test_in_memory_cache_metadata() {
        let metadata = InMemoryCacheMetadata::new();

        // Test put and get
        let entry = metadata
            .put_metadata("test_key", 100, DEFAULT_TIER)
            .await
            .unwrap();
        assert_eq!(entry.size, Some(100));

        let retrieved = metadata.get_metadata("test_key").await.unwrap().unwrap();
        assert_eq!(retrieved.size, Some(100));

        // Test record access
        metadata.record_access("test_key").await.unwrap();
        let accessed = metadata.get_metadata("test_key").await.unwrap().unwrap();
        assert_eq!(accessed.times_accessed, Some(1));

        // Test delete
        metadata.delete_metadata("test_key").await.unwrap();
        assert!(metadata.get_metadata("test_key").await.unwrap().is_none());

        // Test prefix delete
        metadata
            .put_metadata("prefix_1", 100, DEFAULT_TIER)
            .await
            .unwrap();
        metadata
            .put_metadata("prefix_2", 100, DEFAULT_TIER)
            .await
            .unwrap();
        metadata
            .put_metadata("other", 100, DEFAULT_TIER)
            .await
            .unwrap();

        let entries = metadata.get_metadata_with_prefix("prefix_").await.unwrap();
        assert_eq!(entries.len(), 2);

        assert!(metadata.get_metadata("prefix_1").await.unwrap().is_some());
        assert!(metadata.get_metadata("prefix_2").await.unwrap().is_some());
    }
}
