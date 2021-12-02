use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::{Result, ToRwResult};

use super::StateStore;

use crate::storage::hummock::HummockStorage;

/// A wrapper over [`HummockStorage`] as a state store.
///
/// TODO: this wrapper introduces extra overhead of async trait, may be turned into an enum if
/// possible.
#[derive(Clone)]
pub struct HummockStateStore {
    storage: HummockStorage,
}

#[async_trait]
impl StateStore for HummockStateStore {
    async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.storage
            .get(key)
            .await
            .map(|x| x.map(Bytes::from))
            // TODO: make the HummockError into an I/O Error.
            .map_err(anyhow::Error::new)
            .to_rw_result()
    }

    async fn scan(&self, _prefix: &[u8], _limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        todo!()
    }

    async fn ingest_batch(&self, mut kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        // TODO: reduce the redundant vec clone
        kv_pairs.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        self.storage
            .write_batch(
                kv_pairs
                    .into_iter()
                    .map(|(k, v)| (k.to_vec(), v.map(|x| x.to_vec()).into())),
            )
            .await
            .map_err(anyhow::Error::new)
            .to_rw_result()
    }
}
