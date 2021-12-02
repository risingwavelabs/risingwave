use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::error::Result;
use tokio::sync::Mutex;

use super::StateStore;

/// An in-memory state store
#[derive(Clone, Default)]
pub struct MemoryStateStore {
    inner: Arc<Mutex<HashMap<Bytes, Bytes>>>,
}

#[async_trait]
impl StateStore for MemoryStateStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let inner = self.inner.lock().await;
        Ok(inner.get(key).cloned())
    }

    async fn ingest_batch(&self, kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        let mut inner = self.inner.lock().await;
        for (key, value) in kv_pairs {
            if let Some(value) = value {
                inner.insert(key, value);
            } else {
                inner.remove(&key);
            }
        }
        Ok(())
    }
}
