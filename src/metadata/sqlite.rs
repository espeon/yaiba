use async_trait::async_trait;
use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::structs::{self, CacheEntry};

use super::{CacheMetadataBackend, CacheScoringPolicy};

/// A disk-backed cache metadata backend using SQLite.
/// Will persist to disk at the configured path, so only use this for persistent storage.
pub struct SqliteCacheMetadata {
    db: SqlitePool,
}

impl SqliteCacheMetadata {
    pub fn new(db: &SqlitePool) -> Self {
        // cloning this is fine b/c it's an arc internally
        Self { db: db.clone() }
    }
}

#[async_trait]
impl CacheMetadataBackend for SqliteCacheMetadata {
    async fn put_metadata(
        &self,
        key: &str,
        size_bytes: i64,
        tier: structs::CacheTier,
        content_type: Option<String>,
    ) -> anyhow::Result<CacheEntry> {
        let current_time = OffsetDateTime::now_utc();
        let tier = tier.to_string();
        sqlx::query_as!(
        CacheEntry,
        "insert into cache (key, size, date, importance, tier, content_type) values ($1, $2, $3, $4, $5, $6) returning *",
        key,
        size_bytes,
        current_time,
        0,
        tier,
        content_type
    )
    .fetch_one(&self.db)
    .await
    .map_err(|e| anyhow::anyhow!("Failed to insert metadata: {}", e))
    }

    async fn get_metadata(&self, key: &str) -> anyhow::Result<Option<CacheEntry>> {
        sqlx::query_as!(CacheEntry, "select * from cache where key = $1", key)
            .fetch_optional(&self.db)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get metadata: {}", e))
    }

    async fn record_access(&self, key: &str) -> anyhow::Result<()> {
        let current_time = OffsetDateTime::now_utc();
        sqlx::query!(
            "update cache set last_access = $1, times_accessed = times_accessed + 1 where key = $2",
            current_time,
            key
        )
        .execute(&self.db)
        .await?;
        Ok(())
    }

    async fn delete_metadata(&self, key: &str) -> anyhow::Result<()> {
        sqlx::query!("delete from cache where key = $1", key)
            .execute(&self.db)
            .await?;
        Ok(())
    }

    async fn get_metadata_with_prefix(&self, prefix: &str) -> anyhow::Result<Vec<CacheEntry>> {
        let fmt_prefix = format!("{}%", prefix);
        sqlx::query_as!(
            CacheEntry,
            "select * from cache where key like $1",
            fmt_prefix
        )
        .fetch_all(&self.db)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to get metadata with prefix: {}", e))
    }

    async fn get_total_size(&self) -> anyhow::Result<i64> {
        let size = sqlx::query_scalar!("SELECT COALESCE(SUM(size), 0) FROM cache")
            .fetch_one(&self.db)
            .await?;
        Ok(size as i64)
    }

    async fn get_entries_to_purge(
        &self,
        _needed_space: i64,
        policy: &CacheScoringPolicy,
    ) -> anyhow::Result<Vec<CacheEntry>> {
        let order_by = match policy {
            CacheScoringPolicy::Lru => "ORDER BY last_access ASC NULLS FIRST".to_string(),
            CacheScoringPolicy::WeightedScore {
                importance_weight,
                size_weight,
                age_weight,
            } => {
                format!(
                    "ORDER BY
                    (importance * {}) +
                    ((1.0 - size_ratio) * {}) +
                    (1.0 / (age_days + 1.0) * {})
                ASC",
                    importance_weight, size_weight, age_weight
                )
            }
            CacheScoringPolicy::Custom(expr) => format!("ORDER BY {}", expr),
        };

        let query = format!(
            r#"WITH scoring AS (
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
            importance,
            tier
        FROM scoring
        {}"#,
            order_by
        );

        sqlx::query_as(&query)
            .fetch_all(&self.db)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get entries to purge: {}", e))
    }
}
