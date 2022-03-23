use std::future::Future;
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

use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use risingwave_common::error::Result;

use crate::monitor::{MonitoredStateStore, StateStoreMetrics};
use crate::storage_value::StorageValue;
use crate::write_batch::WriteBatch;

pub trait GetFutureTrait<'a> = Future<Output = Result<Option<StorageValue>>> + Send + 'a;
pub trait ScanFutureTrait<'a, R, B> =
    Future<Output = Result<Vec<(Bytes, StorageValue)>>> + Send + 'a;
pub trait EmptyFutureTrait<'a> = Future<Output = Result<()>> + Send + 'a;

#[macro_export]
macro_rules! define_state_store_associated_type {
    () => {
        type GetFuture<'a> = impl GetFutureTrait<'a> + 'a;
        type ScanFuture<'a, R: 'a, B: 'a> = impl ScanFutureTrait<'a, R, B> + 'a where Self: 'a, R: 'a + Send, B: 'a + Send;
        type ReverseScanFuture<'a, R: 'a, B: 'a> = impl ScanFutureTrait<'a, R, B> + 'a where Self: 'a, R: 'a + Send, B: 'a + Send;
        type IngestBatchFuture<'a> = impl EmptyFutureTrait<'a> + 'a where Self: 'a;
        type ReplicateBatchFuture<'a> = impl EmptyFutureTrait<'a> + 'a where Self: 'a;
        type WaitEpochFuture<'a> = impl EmptyFutureTrait<'a> + 'a where Self: 'a;
        type SyncFuture<'a> = impl EmptyFutureTrait<'a> + 'a where Self: 'a;
        type IterFuture<'a, R: 'a, B: 'a> = impl Future<Output = Result<Self::Iter<'a>>> + Send + 'a where Self: 'a, R: 'a + Send, B: 'a + Send;
        type ReverseIterFuture<'a, R: 'a, B: 'a> = impl Future<Output = Result<Self::Iter<'a>>> + Send + 'a where Self: 'a, R: 'a + Send, B: 'a + Send;
    }
}

pub trait StateStore: Send + Sync + 'static + Clone {
    type Iter<'a>: StateStoreIter<Item = (Bytes, StorageValue)> + 'a;

    type GetFuture<'a>: GetFutureTrait<'a>;

    type ScanFuture<'a, R, B>: ScanFutureTrait<'a, R, B>
    where
        Self: 'a,
        R: 'a + Send,
        B: 'a + Send;

    type ReverseScanFuture<'a, R, B>: ScanFutureTrait<'a, R, B>
    where
        Self: 'a,
        R: 'a + Send,
        B: 'a + Send;

    type IngestBatchFuture<'a>: EmptyFutureTrait<'a>;

    type ReplicateBatchFuture<'a>: EmptyFutureTrait<'a>;

    type WaitEpochFuture<'a>: EmptyFutureTrait<'a>;

    type SyncFuture<'a>: EmptyFutureTrait<'a>;

    type IterFuture<'a, R, B>: Future<Output = Result<Self::Iter<'a>>> + Send + 'a
    where
        Self: 'a,
        R: 'a + Send,
        B: 'a + Send;

    type ReverseIterFuture<'a, R, B>: Future<Output = Result<Self::Iter<'a>>> + Send + 'a
    where
        Self: 'a,
        R: 'a + Send,
        B: 'a + Send;

    /// Point get a value from the state store.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn get<'a>(&'a self, key: &'a [u8], epoch: u64) -> Self::GetFuture<'a>;

    /// Scan `limit` number of keys from a key range. If `limit` is `None`, scan all elements.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    ///
    ///
    /// By default, this simply calls `StateStore::iter` to fetch elements.
    fn scan<'a, R: 'a, B: 'a>(
        &'a self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ScanFuture<'a, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;

    fn reverse_scan<'a, R: 'a, B: 'a>(
        &'a self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ReverseScanFuture<'a, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;

    /// Ingest a batch of data into the state store. One write batch should never contain operation
    /// on the same key. e.g. Put(233, x) then Delete(233).
    /// A epoch should be provided to ingest a write batch. It is served as:
    /// - A handle to represent an atomic write session. All ingested write batches associated with
    ///   the same `Epoch` have the all-or-nothing semantics, meaning that partial changes are not
    ///   queryable and will be rollbacked if instructed.
    /// - A version of a kv pair. kv pair associated with larger `Epoch` is guaranteed to be newer
    ///   then kv pair with smaller `Epoch`. Currently this version is only used to derive the
    ///   per-key modification history (e.g. in compaction), not across different keys.
    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, Option<StorageValue>)>,
        epoch: u64,
    ) -> Self::IngestBatchFuture<'_>;

    /// Functions the same as `ingest_batch`, except that data won't be persisted.
    fn replicate_batch(
        &self,
        kv_pairs: Vec<(Bytes, Option<StorageValue>)>,
        epoch: u64,
    ) -> Self::ReplicateBatchFuture<'_>;

    /// Open and return an iterator for given `key_range`.
    /// The returned iterator will iterate data based on a snapshot corresponding to the given
    /// `epoch`.
    fn iter<'a, R: 'a, B: 'a>(&'a self, key_range: R, epoch: u64) -> Self::IterFuture<'a, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;

    /// Open and return a reversed iterator for given `key_range`.
    /// The returned iterator will iterate data based on a snapshot corresponding to the given
    /// `epoch`
    fn reverse_iter<'a, R: 'a, B: 'a>(
        &'a self,
        key_range: R,
        epoch: u64,
    ) -> Self::ReverseIterFuture<'a, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;

    /// Create a `WriteBatch` associated with this state store.
    fn start_write_batch(&self) -> WriteBatch<Self> {
        WriteBatch::new(self.clone())
    }

    /// Wait until the epoch is committed and its data is ready to read.
    fn wait_epoch(&self, epoch: u64) -> Self::WaitEpochFuture<'_>;

    /// Sync buffered data to S3.
    /// If epoch is None, all buffered data will be synced.
    /// Otherwise, only data of the provided epoch will be synced.
    fn sync(&self, epoch: Option<u64>) -> Self::SyncFuture<'_>;

    /// Create a [`MonitoredStateStore`] from this state store, with given `stats`.
    fn monitored(self, stats: Arc<StateStoreMetrics>) -> MonitoredStateStore<Self> {
        MonitoredStateStore::new(self, stats)
    }
}

#[async_trait]
pub trait StateStoreIter: Send {
    type Item;

    async fn next(&mut self) -> Result<Option<Self::Item>>;
}

pub async fn collect_from_iter<'a, I>(mut iter: I, limit: Option<usize>) -> Result<Vec<I::Item>>
where
    I: StateStoreIter,
    I::Item: Send,
{
    let mut kvs = Vec::with_capacity(limit.unwrap_or_default());

    for _ in 0..limit.unwrap_or(usize::MAX) {
        match iter.next().await? {
            Some(kv) => kvs.push(kv),
            None => break,
        }
    }

    Ok(kvs)
}
