use async_trait::async_trait;
use bytes::Bytes;

use risingwave_common::error::Result;

use super::StateStore;

/// A panic state store. If a workload is fully in-memory, we can use this state store, so as to
/// ensure that no data is stored in the state store and no serialization will happen.
#[derive(Clone, Default)]
pub struct PanicStateStore;

#[async_trait]
impl StateStore for PanicStateStore {
    async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        panic!("should not read from the state store!");
    }

    async fn ingest_batch(&self, _kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        panic!("should not write the panic state store!");
    }

    async fn scan(&self, _prefix: &[u8], _limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        panic!("should not read from the state store!");
    }
}
