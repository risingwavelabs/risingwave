use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::error::Result;
use tokio::sync::Mutex;

use super::StateStore;

/// An in-memory state store
#[derive(Clone, Default)]
pub struct MemoryStateStore {
    inner: Arc<Mutex<BTreeMap<Bytes, Bytes>>>,
}

impl MemoryStateStore {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// Verify if the ingested batch does not have duplicated key.
    fn verify_ingest_batch(&self, kv_pairs: &mut Vec<(Bytes, Option<Bytes>)>) -> bool {
        let original_length = kv_pairs.len();
        kv_pairs.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        // There should not be duplicated key in one batch
        kv_pairs.dedup_by(|(k1, _), (k2, _)| k1 == k2);
        original_length == kv_pairs.len()
    }

    async fn ingest_batch_inner(&self, mut kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        let mut inner = self.inner.lock().await;
        let result = self.verify_ingest_batch(&mut kv_pairs);
        debug_assert!(result);
        for (key, value) in kv_pairs {
            if let Some(value) = value {
                inner.insert(key, value);
            } else {
                inner.remove(&key);
            }
        }
        Ok(())
    }

    async fn scan_inner(&self, prefix: &[u8], limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        let mut data = vec![];
        if limit == Some(0) {
            return Ok(vec![]);
        }
        let inner = self.inner.lock().await;
        for (key, value) in inner.iter() {
            if key.starts_with(prefix) {
                data.push((key.clone(), value.clone()));
                if let Some(limit) = limit {
                    if data.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(data)
    }
}

#[async_trait]
impl StateStore for MemoryStateStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let inner = self.inner.lock().await;
        Ok(inner.get(key).cloned())
    }

    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        self.ingest_batch_inner(kv_pairs).await
    }

    async fn scan(&self, prefix: &[u8], limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        self.scan_inner(prefix, limit).await
    }
}
