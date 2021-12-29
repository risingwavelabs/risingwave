use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use crate::{StateStore, StateStoreIter};

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

    async fn ingest_batch(
        &self,
        _kv_pairs: Vec<(Bytes, Option<Bytes>)>,
        _epoch: u64,
    ) -> Result<()> {
        panic!("should not write the panic state store!");
    }

    async fn iter(&self, _prefix: &[u8]) -> Result<Self::Iter> {
        panic!("should not create iter from the panic state store!");
    }
}

pub struct PanicStateStoreIter {}

#[async_trait]
impl StateStoreIter for PanicStateStoreIter {
    type Item = (Bytes, Bytes);

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        unreachable!()
    }
}
