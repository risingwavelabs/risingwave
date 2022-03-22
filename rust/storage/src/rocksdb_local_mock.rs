// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#![allow(dead_code)]

use std::ops::RangeBounds;

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use super::{StateStore, StateStoreIter};
use crate::storage_value::StorageValue;

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

    async fn get(&self, _key: &[u8], _epoch: u64) -> Result<Option<StorageValue>> {
        unimplemented!()
    }

    async fn ingest_batch(
        &self,
        _kv_pairs: Vec<(Bytes, Option<StorageValue>)>,
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

    async fn replicate_batch(
        &self,
        _kv_pairs: Vec<(Bytes, Option<StorageValue>)>,
        _epoch: u64,
    ) -> Result<()> {
        unimplemented!()
    }

    async fn reverse_iter<R, B>(&self, _key_range: R, _epoch: u64) -> Result<Self::Iter<'_>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
        unimplemented!()
    }

    async fn wait_epoch(&self, _epoch: u64) -> Result<()> {
        unimplemented!()
    }

    async fn sync(&self, _epoch: Option<u64>) -> Result<()> {
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
    type Item = (Bytes, StorageValue);

    async fn next(&'_ mut self) -> Result<Option<Self::Item>> {
        unimplemented!()
    }
}
