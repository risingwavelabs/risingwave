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

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStore, StateStoreIter};

/// A panic state store. If a workload is fully in-memory, we can use this state store, so as to
/// ensure that no data is stored in the state store and no serialization will happen.
#[derive(Clone, Default)]
pub struct PanicStateStore;

impl StateStore for PanicStateStore {
    type Iter<'a> = PanicStateStoreIter;
    define_state_store_associated_type!();

    fn get<'a>(&'a self, key: &'a [u8], epoch: u64) -> Self::GetFuture<'a> {
        async move {
            panic!("should not read from the state store!");
        }
    }

    fn scan<'a, R: 'a, B: 'a>(
        &'a self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ScanFuture<'a, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            panic!("should not scan from the state store!");
        }
    }

    fn reverse_scan<'a, R: 'a, B: 'a>(
        &'a self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ReverseScanFuture<'a, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            panic!("should not reverse scan from the state store!");
        }
    }

    fn ingest_batch(
        &self,
        _kv_pairs: Vec<(Bytes, Option<StorageValue>)>,
        _epoch: u64,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            panic!("should not write the state store!");
        }
    }

    fn replicate_batch(
        &self,
        _kv_pairs: Vec<(Bytes, Option<StorageValue>)>,
        _epoch: u64,
    ) -> Self::ReplicateBatchFuture<'_> {
        async move {
            panic!("should not replicate batch from the state store!");
        }
    }

    fn iter<'a, R: 'a, B: 'a>(&'a self, key_range: R, epoch: u64) -> Self::IterFuture<'a, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            panic!("should not create iter from the state store!");
        }
    }

    fn reverse_iter<'a, R: 'a, B: 'a>(
        &'a self,
        key_range: R,
        epoch: u64,
    ) -> Self::ReverseIterFuture<'a, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            panic!("should not create reverse iter from the panic state store!");
        }
    }

    fn wait_epoch(&self, _epoch: u64) -> Self::WaitEpochFuture<'_> {
        async move {
            panic!("should not wait epoch from the panic state store!");
        }
    }

    fn sync(&self, _epoch: Option<u64>) -> Self::SyncFuture<'_> {
        async move {
            panic!("should not sync from the panic state store!");
        }
    }
}

pub struct PanicStateStoreIter {}

#[async_trait]
impl StateStoreIter for PanicStateStoreIter {
    type Item = (Bytes, StorageValue);

    async fn next(&'_ mut self) -> Result<Option<Self::Item>> {
        unreachable!()
    }
}
