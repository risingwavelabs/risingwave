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

use std::future::Future;
use std::ops::RangeBounds;

use bytes::Bytes;
use risingwave_hummock_sdk::HummockEpoch;

use super::StateStore;
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStoreIter};

#[derive(Clone)]
pub struct TikvStateStore {}

impl TikvStateStore {
    pub fn new(_pd_endpoints: Vec<String>) -> Self {
        unimplemented!()
    }
}

impl StateStore for TikvStateStore {
    type Iter<'a> = TikvStateStoreIter;

    define_state_store_associated_type!();

    fn get<'a>(&'a self, _key: &'a [u8], _epoch: HummockEpoch) -> Self::GetFuture<'_> {
        async move { unimplemented!() }
    }

    fn ingest_batch(
        &self,
        _kv_pairs: Vec<(Bytes, StorageValue)>,
        _epoch: HummockEpoch,
        _table_id: StorageTableId,
    ) -> Self::IngestBatchFuture<'_> {
        async move { unimplemented!() }
    }

    fn iter<R, B>(&self, _key_range: R, _epoch: HummockEpoch) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn replicate_batch(
        &self,
        _kv_pairs: Vec<(Bytes, StorageValue)>,
        _epoch: HummockEpoch,
    ) -> Self::ReplicateBatchFuture<'_> {
        async move { unimplemented!() }
    }

    fn reverse_iter<R, B>(
        &self,
        _key_range: R,
        _epoch: HummockEpoch,
    ) -> Self::ReverseIterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }

    fn wait_epoch(
        &self,
        _epoch: HummockEpoch,
        _table_id: StorageTableId,
    ) -> Self::WaitEpochFuture<'_> {
        async move { unimplemented!() }
    }

    fn sync(
        &self,
        _epoch: Option<HummockEpoch>,
        _table_id: Option<Vec<StorageTableId>>,
    ) -> Self::SyncFuture<'_> {
        async move { unimplemented!() }
    }

    fn scan<R, B>(
        &self,
        _key_range: R,
        _limit: Option<usize>,
        _epoch: HummockEpoch,
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
        _epoch: HummockEpoch,
    ) -> Self::ReverseScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move { unimplemented!() }
    }
}

pub struct TikvStateStoreIter;

impl TikvStateStoreIter {
    pub async fn new(_store: TikvStateStore, _prefix: Vec<u8>) -> Self {
        unimplemented!()
    }
}

impl StateStoreIter for TikvStateStoreIter {
    type Item = (Bytes, Bytes);

    type NextFuture<'a> = impl Future<Output = crate::error::StorageResult<Option<Self::Item>>>;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move { unimplemented!() }
    }
}
