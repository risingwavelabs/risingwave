use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use super::{StateStore, StateStoreIter};

/// A panic state store. If a workload is fully in-memory, we can use this state store, so as to
/// ensure that no data is stored in the state store and no serialization will happen.
#[derive(Clone, Default)]
pub struct PanicStateStore;

#[async_trait]
impl StateStore for PanicStateStore {
    type Iter = PanicStateStoreIter;

    async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        panic!("should not read from the state store!");
    }

    async fn ingest_batch(&self, _kv_pairs: Vec<(Bytes, Option<Bytes>)>) -> Result<()> {
        panic!("should not write the panic state store!");
    }

    async fn scan(&self, _prefix: &[u8], _limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
        panic!("should not read from the panic state store!");
    }

    fn iter(&self, _prefix: &[u8]) -> Self::Iter {
        panic!("should not create iter from the panic state store!");
    }
}

pub struct PanicStateStoreIter {}

#[async_trait]
impl StateStoreIter for PanicStateStoreIter {
    type Item = (Bytes, Bytes);

    async fn open(&mut self) -> Result<()> {
        unreachable!()
    }

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        unreachable!()
    }
}
