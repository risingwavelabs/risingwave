use std::ops::RangeBounds;

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
    type Iter<'a> = PanicStateStoreIter;

    async fn get(&self, _key: &[u8], _epoch: u64) -> Result<Option<Bytes>> {
        panic!("should not read from the state store!");
    }

    async fn ingest_batch(
        &self,
        _kv_pairs: Vec<(Bytes, Option<Bytes>)>,
        _epoch: u64,
    ) -> Result<()> {
        panic!("should not write the panic state store!");
    }

    async fn iter<R, B>(&self, _key_range: R, _epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        panic!("should not create iter from the panic state store!");
    }

    async fn replicate_batch(
        &self,
        _kv_pairs: Vec<(Bytes, Option<Bytes>)>,
        _epoch: u64,
    ) -> Result<()> {
        panic!("should not replicate batch from the panic state store!");
    }

    async fn reverse_iter<R, B>(&self, _key_range: R, _epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        panic!("should not create iter from the panic state store!");
    }

    async fn wait_epoch(&self, _epoch: u64) -> Result<()>{
        panic!("should not wait epoch from the panic state store!");
    }

    async fn sync(&self, _epoch: Option<u64>) -> Result<()> {
        panic!("should not sync from the panic state store!");
    }
}

pub struct PanicStateStoreIter {}

#[async_trait]
impl StateStoreIter for PanicStateStoreIter {
    type Item = (Bytes, Bytes);

    async fn next(&'_ mut self) -> Result<Option<Self::Item>> {
        unreachable!()
    }
}
