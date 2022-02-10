#![allow(dead_code)]

use std::ops::RangeBounds;

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
    type Iter<'a> = RocksDBStateStoreIter;

    async fn get(&self, _key: &[u8], _epoch: u64) -> Result<Option<Bytes>> {
        unimplemented!()
    }

    async fn ingest_batch(
        &self,
        _kv_pairs: Vec<(Bytes, Option<Bytes>)>,
        _epoch: u64,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn iter<R, B>(&self, _key_range: R, _epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
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

    async fn next(&'_ mut self) -> Result<Option<Self::Item>> {
        unimplemented!()
    }
}
