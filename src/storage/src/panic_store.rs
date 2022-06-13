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
use std::sync::Arc;

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::consistent_hash::VNodeBitmap;

use crate::monitor::StateStoreMetrics;
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStore, StateStoreIter};

/// A panic state store. If a workload is fully in-memory, we can use this state store to
/// ensure that no data is stored in the state store and no serialization will happen.
#[derive(Clone, Default)]
pub struct PanicStateStore;

impl StateStore for PanicStateStore {
    type Iter = PanicStateStoreIter;

    define_state_store_associated_type!();

    fn get<'a>(
        &'a self,
        _key: &'a [u8],
        _epoch: u64,
        _vnode: Option<&'a VNodeBitmap>,
    ) -> Self::GetFuture<'_> {
        async move {
            panic!("should not read from the state store!");
        }
    }

    fn scan<R, B>(
        &self,
        _key_range: R,
        _limit: Option<usize>,
        _epoch: u64,
        _vnodes: Option<VNodeBitmap>,
    ) -> Self::ScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            panic!("should not scan from the state store!");
        }
    }

    fn backward_scan<R, B>(
        &self,
        _key_range: R,
        _limit: Option<usize>,
        _epoch: u64,
        _vnodes: Option<VNodeBitmap>,
    ) -> Self::BackwardScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            panic!("should not backward scan from the state store!");
        }
    }

    fn ingest_batch(
        &self,
        _kv_pairs: Vec<(Bytes, StorageValue)>,
        _epoch: u64,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            panic!("should not write the state store!");
        }
    }

    fn replicate_batch(
        &self,
        _kv_pairs: Vec<(Bytes, StorageValue)>,
        _epoch: u64,
    ) -> Self::ReplicateBatchFuture<'_> {
        async move {
            panic!("should not replicate batch from the state store!");
        }
    }

    fn iter<R, B>(
        &self,
        _key_range: R,
        _epoch: u64,
        _vnodes: Option<VNodeBitmap>,
    ) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            panic!("should not create iter from the state store!");
        }
    }

    fn backward_iter<R, B>(
        &self,
        _key_range: R,
        _epoch: u64,
        _vnodes: Option<VNodeBitmap>,
    ) -> Self::BackwardIterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            panic!("should not create backward iter from the panic state store!");
        }
    }

    fn wait_epoch(&self, _epoch: u64) -> Self::WaitEpochFuture<'_> {
        async move {
            panic!("should not wait epoch from the panic state store!");
        }
    }
}

impl StateStoreProxy for PanicStateStore {
    type Output = PanicStateStore;

    type SyncFuture<'a> = impl EmptyFutureTrait<'a>;

    fn sync(&self, _epoch: Option<u64>) -> Self::SyncFuture<'_> {
        async move {
            panic!("should not sync from the panic state store!");
        }
    }

    fn state(&self, _table_id: TableId) -> Self::Output {
        panic!("should not build the panic state store!");
    }

    fn stats(&self) -> Arc<StateStoreMetrics> {
        panic!("should not get stats from the panic state store!");
    }
}

pub struct PanicStateStoreIter {}

impl StateStoreIter for PanicStateStoreIter {
    type Item = (Bytes, Bytes);

    type NextFuture<'a> =
        impl Future<Output = crate::error::StorageResult<Option<Self::Item>>> + Send;

    fn next(&'_ mut self) -> Self::NextFuture<'_> {
        async move { unreachable!() }
    }
}
