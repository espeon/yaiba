use std::{fmt::Display, str::FromStr};

use time::OffsetDateTime;

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct CacheTier {
    pub rank: CacheTierRank,
    pub name: String,
}
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub enum CacheTierRank {
    /// Fastest, in-memory cache
    /// Expected latency: <1ms
    /// Typical use: Hot data, frequent access
    L0,

    /// Second fastest, (for example) NVME SSD-based cache
    /// Expected latency: 1-10ms
    /// Typical use: Warm data, regular access
    L1,

    /// Third fastest, HDD-based cache
    /// Expected latency: 10-100ms
    /// Typical use: Cool data, occasional access
    L2,

    /// Fourth fastest, Network-based cache
    /// Expected latency: 100-500ms
    /// Typical use: Cold data, infrequent access
    L3,

    /// Fifth fastest, Backup/archival storage
    /// Expected latency: 500ms-5s
    /// Typical use: Archive data, rare access
    L4,

    /// Sixth fastest, Deep storage
    /// Expected latency: >5s
    /// Typical use: Historical data, very rare access
    L5,
}

impl Display for CacheTierRank {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        let string = match self {
            Self::L0 => "L0".to_string(),
            Self::L1 => "L2".to_string(),
            Self::L2 => "L3".to_string(),
            Self::L3 => "L4".to_string(),
            Self::L4 => "L5".to_string(),
            Self::L5 => "L6".to_string(),
        };
        write!(f, "{}", string)
    }
}

impl FromStr for CacheTierRank {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "L0" => Ok(Self::L0),
            "L1" => Ok(Self::L1),
            "L2" => Ok(Self::L2),
            "L3" => Ok(Self::L3),
            "L4" => Ok(Self::L4),
            "L5" => Ok(Self::L5),
            _ => Err(format!("Invalid tier rank: {}", s)),
        }
    }
}

impl Display for CacheTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{}-{}", self.rank, &self.name.to_string())
    }
}

impl FromStr for CacheTier {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // just split off the first part
        let parts: Vec<String> = s.split('-').map(|s| s.to_string()).collect();
        let rank = parts[0].parse::<CacheTierRank>()?;
        // get the rest of the parts
        let name = parts[1..].join("-");
        Ok(Self { rank, name })
    }
}

#[derive(sqlx::FromRow, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheEntry {
    pub key: Option<String>,
    pub size: Option<i64>,
    pub date: Option<OffsetDateTime>,
    pub last_access: Option<OffsetDateTime>,
    pub times_accessed: Option<i64>,
    pub expiration: Option<OffsetDateTime>,
    pub importance: Option<i64>,
    pub tier: String,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::structs::{CacheTier, CacheTierRank};

    #[test]
    fn test_cachetier_to_string() {
        let cache_tier_string = CacheTier {
            rank: CacheTierRank::L0,
            name: "rapidcache-1".to_string(),
        }
        .to_string();
        assert_eq!("L0-rapidcache-1", cache_tier_string);
    }

    #[test]
    fn test_string_to_cachetier() {
        if let Ok(cachetier) = CacheTier::from_str("L0-rapidcache-1") {
            assert_eq!(
                cachetier,
                CacheTier {
                    rank: CacheTierRank::L0,
                    name: "rapidcache-1".to_string(),
                }
            )
        } else {
            panic!("Failed to parse cachetier string");
        }
    }

    #[test]
    fn test_invalid_cachetier_string() {
        if let Ok(cachetier) = CacheTier::from_str("L9-rapidcache-1-invalid") {
            panic!("Parsed invalid cachetier string: {:?}", cachetier);
        } else {
            // Expected error!
        }
    }
}
