use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use super::{StateStore, StateStoreIter};

#[derive(Clone)]
pub struct RocksDBStateStore {}

impl RocksDBStateStore {
    pub fn new(_db_path: &str) -> Self {
        unimplemented!()
    }
}

#[async_trait]
impl StateStore for RocksDBStateStore {
    type Iter = RocksDBStateStoreIter;

    async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        unimplemented!()
    }

    async fn ingest_batch(
        &self,
        _kv_pairs: Vec<(Bytes, Option<Bytes>)>,
        _epoch: u64,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn iter(&self, _prefix: &[u8]) -> Result<Self::Iter> {
        unimplemented!()
    }
}

pub struct RocksDBStateStoreIter {}

impl RocksDBStateStoreIter {
    async fn new(_store: RocksDBStateStore, _prefix: Vec<u8>) -> Result<Self> {
        unimplemented!()
    }
}

#[async_trait]
impl StateStoreIter for RocksDBStateStoreIter {
    type Item = (Bytes, Bytes);

    async fn next(&mut self) -> Result<Option<Self::Item>> {
        unimplemented!()
    }
}
