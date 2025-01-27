use time::OffsetDateTime;

#[derive(sqlx::FromRow, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheEntry {
    pub key: Option<String>,
    pub size: Option<i64>,
    pub date: Option<OffsetDateTime>,
    pub last_access: Option<OffsetDateTime>,
    pub times_accessed: Option<i64>,
    pub expiration: Option<OffsetDateTime>,
    pub importance: Option<i64>,
}
