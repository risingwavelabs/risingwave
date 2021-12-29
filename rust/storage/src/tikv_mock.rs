use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use super::StateStore;
use crate::StateStoreIter;

#[derive(Clone)]
pub struct TikvStateStore {}

impl TikvStateStore {
    pub fn new(_pd_endpoints: Vec<String>) -> Self {
        unimplemented!()
    }
}
#[async_trait]
impl StateStore for TikvStateStore {
    type Iter = TikvStateStoreIter;

    async fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        unimplemented!()
    }

    async fn scan(&self, _prefix: &[u8], _limit: Option<usize>) -> Result<Vec<(Bytes, Bytes)>> {
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

pub struct TikvStateStoreIter;

impl TikvStateStoreIter {
    pub async fn new(_store: TikvStateStore, _prefix: Vec<u8>) -> Self {
        unimplemented!()
    }
}

#[async_trait]
impl StateStoreIter for TikvStateStoreIter {
    async fn next(&mut self) -> Result<Option<Self::Item>> {
        unimplemented!()
    }

    type Item = (Bytes, Bytes);
}
