use tokio::{fs::File, io::AsyncWriteExt};

use futures::StreamExt;
use sqlx::SqlitePool;
use time::OffsetDateTime;

use crate::structs::CacheEntry;

#[derive(Clone)]
pub struct Cache {
    item_count: i128,
    item_size_bytes: i128,
    db: SqlitePool
}

const URL_BASE: &str = "https://s3.natalie.sh/nano/";

impl Cache {
    pub fn new(db:SqlitePool) -> Cache {
        Cache {
            item_count: 0,
            item_size_bytes:0,
            db
        }
    }
    /// gets a file from the cache, and pulls from source and puts into cache if it doesn't exist
    pub async fn get(self, k:String) -> anyhow::Result<File> {
        let k1 = k.clone();
        match sqlx::query_as!(CacheEntry, "select * from cache where key = $1", k1)
        .fetch_one(&self.db)
        .await {
            Ok(e) => {
                // get the file from disk
                let path = format!("./cache/{}", e.key.unwrap());
                Ok(File::open(path).await?)
            },
            Err(_) => {
                let file = Self::stream_url_to_disk(format!("{}{}", URL_BASE, k), &k).await?;
                Self::put(self, k, &file).await?;
                Ok(file)
            },
        }
    }
    pub async fn put(self, k: String, v: &File) -> anyhow::Result<CacheEntry> {
        // save file to db

        let size_bytes = v.metadata().await.unwrap().len() as i64;
        let current_time = OffsetDateTime::now_utc();

        let file = match sqlx::query_as!(CacheEntry, "insert into cache (key, size, date, importance) values ($1, $2, $3, $4) returning *", k, size_bytes, current_time, 0)
        .fetch_one(&self.db)
        .await
        .map_err(crate::internal_error) {
            Ok(e) => e,
            Err(e) => {
                dbg!(e);
                return anyhow::Result::Err(anyhow::anyhow!("error saving to db"));
            },
        };
        Ok(file)
    }
    pub async fn stream_url_to_disk(url: String, name: &String) -> anyhow::Result<File> {
        let req = reqwest::get(url).await?;
        let headers = req.headers();
        dbg!(headers);
        // for now - save to file
        // TODO: stream to file and client simultaneously
        let mut file = File::create(format!("./cache/{}", name)).await?;
        let mut stream = req.bytes_stream();
        while let Some(item) = stream.next().await {
            let item = item?;
            file.write_all(&item).await.unwrap();
        }
        Ok(file)
    }
}