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

#![allow(dead_code)]

use std::future::Future;
use std::ops::RangeBounds;

use bytes::Bytes;
use risingwave_common::error::Result;

use super::{StateStore, StateStoreIter};
use crate::define_state_store_associated_type;
use crate::storage_value::StorageValue;
use crate::store::*;

#[derive(Clone)]
pub struct RocksDBStateStore {}

impl RocksDBStateStore {
    pub fn new(_db_path: &str) -> Self {
        unimplemented!()
    }
}

impl StateStore for RocksDBStateStore {
    type Iter<'a> = RocksDBStateStoreIter;
    define_state_store_associated_type!();

    fn get<'a>(&'a self, _key: &'a [u8], _epoch: u64) -> Self::GetFuture<'_> {
        async move { unimplemented!() }
    }

    fn scan<R, B>(
        &self,
        _key_range: R,
        _limit: Option<usize>,
        _epoch: u64,
    ) -> Self::ScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn reverse_scan<R, B>(
        &self,
        _key_range: R,
        _limit: Option<usize>,
        _epoch: u64,
    ) -> Self::ReverseScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn ingest_batch(
        &self,
        _kv_pairs: Vec<(Bytes, StorageValue)>,
        _epoch: u64,
    ) -> Self::IngestBatchFuture<'_> {
        async move { unimplemented!() }
    }

    fn replicate_batch(
        &self,
        _kv_pairs: Vec<(Bytes, StorageValue)>,
        _epoch: u64,
    ) -> Self::ReplicateBatchFuture<'_> {
        async move { unimplemented!() }
    }

    fn iter<R, B>(&self, _key_range: R, _epoch: u64) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn reverse_iter<R, B>(&self, _key_range: R, _epoch: u64) -> Self::ReverseIterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn wait_epoch(&self, _epoch: u64) -> Self::WaitEpochFuture<'_> {
        async move { unimplemented!() }
    }

    fn sync(&self, _epoch: Option<u64>) -> Self::SyncFuture<'_> {
        async move { unimplemented!() }
    }
}

pub struct RocksDBStateStoreIter {}

impl RocksDBStateStoreIter {
    async fn new(_store: RocksDBStateStore, _prefix: Vec<u8>) -> Result<Self> {
        unimplemented!()
    }
}

impl StateStoreIter for RocksDBStateStoreIter {
    type Item = (Bytes, Bytes);
    type NextFuture<'a> = impl Future<Output = crate::error::StorageResult<Option<Self::Item>>>;
    fn next(&mut self) -> Self::NextFuture<'_> {
        async move { unimplemented!() }
    }
}
