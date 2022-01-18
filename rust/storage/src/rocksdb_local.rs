use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use rocksdb::{DBIterator, ReadOptions, SeekKey, Writable, WriteBatch, DB};
use tokio::sync::OnceCell;
use tokio::task;

use super::{StateStore, StateStoreIter};
use crate::monitor::{StateStoreStats, DEFAULT_STATE_STORE_STATS};

#[derive(Clone)]
pub struct RocksDBStateStore {
    storage: Arc<OnceCell<RocksDBStorage>>,
    db_path: String,
    stats: Arc<StateStoreStats>,
}

impl RocksDBStateStore {
    pub fn new(db_path: &str) -> Self {
        Self {
            storage: Arc::new(OnceCell::new()),
            db_path: db_path.to_string(),
            stats: DEFAULT_STATE_STORE_STATS.clone(),
        }
    }

    pub async fn storage(&self) -> &RocksDBStorage {
        self.storage
            .get_or_init(|| async { RocksDBStorage::new(&self.db_path).await })
            .await
    }

    pub fn get_stats_ref(&self) -> &StateStoreStats {
        self.stats.as_ref()
    }
}

#[async_trait]
impl StateStore for RocksDBStateStore {
    type Iter = RocksDBStateStoreIter;

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.get_stats_ref().get_counts.inc();
        self.storage().await.get(key).await
    }

    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>, _epoch: u64) -> Result<()> {
        self.storage().await.write_batch(kv_pairs).await
    }

    async fn iter(&self, prefix: &[u8]) -> Result<Self::Iter> {
        RocksDBStateStoreIter::new(self.clone(), prefix.to_owned()).await
    }
}

pub fn next_prefix(prefix: &[u8]) -> Vec<u8> {
    let pos = prefix.iter().rposition(|b| *b != 0xff).unwrap();
    let (s, e) = (&prefix[..pos], prefix[pos] + 1);
    let mut res = Vec::with_capacity(s.len() + 1);
    res.extend_from_slice(s);
    res.push(e);
    res
}

pub struct RocksDBStateStoreIter {
    store: RocksDBStateStore,
    iter: Option<Box<DBIterator<Arc<DB>>>>,
    end_key: Bytes,
}

impl RocksDBStateStoreIter {
    async fn new(store: RocksDBStateStore, prefix: Vec<u8>) -> Result<Self> {
        let mut iter = store.storage().await.iter().await;
        let end_key = Bytes::from(next_prefix(prefix.as_slice()));
        task::spawn_blocking(move || {
            return if iter.seek(SeekKey::from(prefix.as_slice())).unwrap() {
                Ok(Self {
                    store,
                    iter: Some(Box::new(iter)),
                    end_key,
                })
            } else {
                Err(InternalError("prefix not found".to_string()).into())
            };
        })
        .await?
    }
}

#[async_trait]
impl StateStoreIter for RocksDBStateStoreIter {
    type Item = (Bytes, Bytes);

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        let mut iter = self.iter.take().unwrap();
        let end_key = self.end_key.clone();
        let (kv, iter) = tokio::task::spawn_blocking(move || {
            let result = iter.valid().map_err(|e| RwError::from(InternalError(e)));
            if let Err(e) = result {
                return (Err(e), iter);
            }
            if !result.unwrap() {
                return (Ok(None), iter);
            }
            let k = Bytes::from(iter.key().to_vec());
            let v = Bytes::from(iter.value().to_vec());
            if k >= end_key {
                return (Ok(None), iter);
            }
            if let Err(e) = iter.next().map_err(|e| RwError::from(InternalError(e))) {
                return (Err(e), iter);
            }
            (Ok(Some((k, v))), iter)
        })
        .await
        .unwrap();

        self.iter = Some(iter);
        let kv = kv?;
        Ok(kv)
    }
}

#[derive(Clone)]
pub struct RocksDBStorage {
    db: Arc<DB>,
}

impl RocksDBStorage {
    pub async fn new(path: &str) -> Self {
        let path = path.to_string();
        let db = task::spawn_blocking(move || DB::open_default(path.as_str()).unwrap())
            .await
            .unwrap();
        let storage = RocksDBStorage { db: Arc::new(db) };
        storage.clear_all().await.unwrap();
        storage
    }

    async fn clear_all(&self) -> Result<()> {
        let db = self.db.clone();
        task::spawn_blocking(move || {
            let mut it = db.iter();
            if !it.seek(SeekKey::Start).unwrap() {
                return Ok(());
            }
            let start_key = it.key().to_vec();
            it.seek(SeekKey::End).unwrap();
            let end_key = next_prefix(it.key());
            db.delete_range(start_key.as_slice(), end_key.as_slice())
                .map_err(|e| RwError::from(InternalError(e)))
        })
        .await?
    }

    async fn write_batch(&self, mut kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        let wb = WriteBatch::new();
        for (key, value) in kv_pairs {
            if let Some(value) = value {
                if let Err(e) = wb.put(key.as_ref(), value.as_ref()) {
                    return Err(InternalError(e).into());
                }
            } else if let Err(e) = wb.delete(key.as_ref()) {
                return Err(InternalError(e).into());
            }
        }

        let db = self.db.clone();
        task::spawn_blocking(move || db.write(&wb).map_err(|e| InternalError(e).into())).await?
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let db = self.db.clone();
        let seek_key = key.to_vec();
        task::spawn_blocking(move || {
            db.get(&seek_key).map_or_else(
                |e| Err(InternalError(e).into()),
                |option_v| Ok(option_v.map(|v| Bytes::from(v.to_vec()))),
            )
        })
        .await?
    }

    async fn iter(&self) -> DBIterator<Arc<DB>> {
        let db = self.db.clone();
        task::spawn_blocking(move || DBIterator::new(db, ReadOptions::default()))
            .await
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[tokio::test]
    async fn test_rocksdb() {
        let rocksdb_state_store = RocksDBStateStore::new("/tmp/default");
        let result = rocksdb_state_store.get("key1".as_bytes()).await;
        assert_eq!(result.unwrap(), None);
        let result = rocksdb_state_store.get("key2".as_bytes()).await;
        assert_eq!(result.unwrap(), None);
        let result = rocksdb_state_store.get("key3".as_bytes()).await;
        assert_eq!(result.unwrap(), None);

        let kv_pairs: Vec<(Bytes, Option<Bytes>)> = vec![
            ("key1".into(), Some("val1".into())),
            ("key2".into(), Some("val2".into())),
            ("key3".into(), Some("val3".into())),
        ];
        rocksdb_state_store.ingest_batch(kv_pairs, 0).await.unwrap();
        let result = rocksdb_state_store
            .get("key1".as_bytes())
            .await
            .unwrap()
            .unwrap();
        assert!(result.eq(&Bytes::from("val1")));
        let result = rocksdb_state_store
            .get("key2".as_bytes())
            .await
            .unwrap()
            .unwrap();
        assert!(result.eq(&Bytes::from("val2")));
        let result = rocksdb_state_store
            .get("key3".as_bytes())
            .await
            .unwrap()
            .unwrap();
        assert!(result.eq(&Bytes::from("val3")));
        let result = rocksdb_state_store.get("key4".as_bytes()).await;
        assert_eq!(result.unwrap(), None);

        let result = rocksdb_state_store
            .scan("key".as_bytes(), Some(2))
            .await
            .unwrap();
        assert!(result.get(0).unwrap().0.eq(&Bytes::from("key1")));
        assert!(result.get(1).unwrap().0.eq(&Bytes::from("key2")));
    }
}
