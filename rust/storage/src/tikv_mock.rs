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
use std::ops::RangeBounds;

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use super::StateStore;
use crate::storage_value::StorageValue;
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
    type Iter<'a> = TikvStateStoreIter;

    async fn get(&self, _key: &[u8], _epoch: u64) -> Result<Option<StorageValue>> {
        unimplemented!()
    }

    async fn scan<R, B>(
        &self,
        _key_range: R,
        _limit: Option<usize>,
        _epoch: u64,
    ) -> Result<Vec<(Bytes, StorageValue)>>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]>,
    {
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

    type Item = (Bytes, StorageValue);
}
