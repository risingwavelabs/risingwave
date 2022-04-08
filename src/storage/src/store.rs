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

use crate::error::StorageResult;
use crate::monitor::{MonitoredStateStore, StateStoreMetrics};
use crate::storage_value::StorageValue;
use crate::write_batch::WriteBatch;

pub trait GetFutureTrait<'a> = Future<Output = StorageResult<Option<Bytes>>> + Send;
pub trait ScanFutureTrait<'a, R, B> = Future<Output = StorageResult<Vec<(Bytes, Bytes)>>> + Send;
pub trait EmptyFutureTrait<'a> = Future<Output = StorageResult<()>> + Send;

#[macro_export]
macro_rules! define_state_store_associated_type {
    () => {
        type GetFuture<'a> = impl GetFutureTrait<'a>;
        type ScanFuture<'a, R, B> = impl ScanFutureTrait<'a, R, B> where R: 'static + Send, B: 'static + Send;
        type ReverseScanFuture<'a, R, B> = impl ScanFutureTrait<'a, R, B> where R: 'static + Send, B: 'static + Send;
        type IngestBatchFuture<'a> = impl EmptyFutureTrait<'a>;
        type ReplicateBatchFuture<'a> = impl EmptyFutureTrait<'a>;
        type WaitEpochFuture<'a> = impl EmptyFutureTrait<'a>;
        type SyncFuture<'a> = impl EmptyFutureTrait<'a>;
        type IterFuture<'a, R, B> = impl Future<Output = $crate::error::StorageResult<Self::Iter<'a>>> + Send where R: 'static + Send, B: 'static + Send;
        type ReverseIterFuture<'a, R, B> = impl Future<Output = $crate::error::StorageResult<Self::Iter<'a>>> + Send where R: 'static + Send, B: 'static + Send;
    }
}

pub trait StateStore: Send + Sync + 'static + Clone {
    type Iter<'a>: StateStoreIter<Item = (Bytes, Bytes)>
    where
        Self: 'a;

    type GetFuture<'a>: GetFutureTrait<'a>;

    type ScanFuture<'a, R, B>: ScanFutureTrait<'a, R, B>
    where
        R: 'static + Send,
        B: 'static + Send;

    type ReverseScanFuture<'a, R, B>: ScanFutureTrait<'a, R, B>
    where
        R: 'static + Send,
        B: 'static + Send;

    type IngestBatchFuture<'a>: EmptyFutureTrait<'a>;

    type ReplicateBatchFuture<'a>: EmptyFutureTrait<'a>;

    type WaitEpochFuture<'a>: EmptyFutureTrait<'a>;

    type SyncFuture<'a>: EmptyFutureTrait<'a>;

    type IterFuture<'a, R, B>: Future<Output = StorageResult<Self::Iter<'a>>> + Send
    where
        R: 'static + Send,
        B: 'static + Send;

    type ReverseIterFuture<'a, R, B>: Future<Output = StorageResult<Self::Iter<'a>>> + Send
    where
        R: 'static + Send,
        B: 'static + Send;

    /// Point gets a value from the state store.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn get<'a>(&'a self, key: &'a [u8], epoch: u64) -> Self::GetFuture<'_>;

    /// Scans `limit` number of keys from a key range. If `limit` is `None`, scans all elements.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    ///
    ///
    /// By default, this simply calls `StateStore::iter` to fetch elements.
    fn scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;

    fn reverse_scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ReverseScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;

    /// Ingests a batch of data into the state store. One write batch should never contain operation
    /// on the same key. e.g. Put(233, x) then Delete(233).
    /// An epoch should be provided to ingest a write batch. It is served as:
    /// - A handle to represent an atomic write session. All ingested write batches associated with
    ///   the same `Epoch` have the all-or-nothing semantics, meaning that partial changes are not
    ///   queryable and will be rolled back if instructed.
    /// - A version of a kv pair. kv pair associated with larger `Epoch` is guaranteed to be newer
    ///   then kv pair with smaller `Epoch`. Currently this version is only used to derive the
    ///   per-key modification history (e.g. in compaction), not across different keys.
    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        epoch: u64,
    ) -> Self::IngestBatchFuture<'_>;

    /// Functions the same as `ingest_batch`, except that data won't be persisted.
    fn replicate_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        epoch: u64,
    ) -> Self::ReplicateBatchFuture<'_>;

    /// Opens and returns an iterator for given `key_range`.
    /// The returned iterator will iterate data based on a snapshot corresponding to the given
    /// `epoch`.
    fn iter<R, B>(&self, key_range: R, epoch: u64) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;

    /// Opens and returns a reversed iterator for given `key_range`.
    /// The returned iterator will iterate data based on a snapshot corresponding to the given
    /// `epoch`
    fn reverse_iter<R, B>(&self, key_range: R, epoch: u64) -> Self::ReverseIterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send;

    /// Creates a `WriteBatch` associated with this state store.
    fn start_write_batch(&self) -> WriteBatch<Self> {
        WriteBatch::new(self.clone())
    }

    /// Waits until the epoch is committed and its data is ready to read.
    fn wait_epoch(&self, epoch: u64) -> Self::WaitEpochFuture<'_>;

    /// Syncs buffered data to S3.
    /// If the epoch is None, all buffered data will be synced.
    /// Otherwise, only data of the provided epoch will be synced.
    fn sync(&self, epoch: Option<u64>) -> Self::SyncFuture<'_>;

    /// Creates a [`MonitoredStateStore`] from this state store, with given `stats`.
    fn monitored(self, stats: Arc<StateStoreMetrics>) -> MonitoredStateStore<Self> {
        MonitoredStateStore::new(self, stats)
    }
}

pub trait StateStoreIter: Send {
    type Item;
    type NextFuture<'a>: Future<Output = StorageResult<Option<Self::Item>>>
    where
        Self: 'a;

    fn next(&mut self) -> Self::NextFuture<'_>;
}
