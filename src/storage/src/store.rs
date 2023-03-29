// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::key::{FullKey, KeyPayloadType};
use risingwave_hummock_sdk::{HummockReadEpoch, LocalSstableInfo};

use crate::error::{StorageError, StorageResult};
use crate::hummock::CachePolicy;
use crate::monitor::{MonitoredStateStore, MonitoredStorageMetrics};
use crate::storage_value::StorageValue;
use crate::write_batch::WriteBatch;

pub trait StaticSendSync = Send + Sync + 'static;

pub trait NextFutureTrait<'a, Item> = Future<Output = StorageResult<Option<Item>>> + Send + 'a;
pub trait StateStoreIter: StaticSendSync {
    type Item: Send;
    type NextFuture<'a>: NextFutureTrait<'a, Self::Item>;

    fn next(&mut self) -> Self::NextFuture<'_>;
}

pub trait StateStoreIterStreamTrait<Item> = Stream<Item = StorageResult<Item>> + Send + 'static;
pub trait StateStoreIterExt: StateStoreIter {
    type ItemStream: StateStoreIterStreamTrait<<Self as StateStoreIter>::Item>;

    fn into_stream(self) -> Self::ItemStream;
}

#[try_stream(ok = I::Item, error = StorageError)]
async fn into_stream_inner<I: StateStoreIter>(mut iter: I) {
    while let Some(item) = iter.next().await? {
        yield item;
    }
}

pub type StreamTypeOfIter<I> = <I as StateStoreIterExt>::ItemStream;
impl<I: StateStoreIter> StateStoreIterExt for I {
    type ItemStream = impl Stream<Item = StorageResult<<Self as StateStoreIter>::Item>>;

    fn into_stream(self) -> Self::ItemStream {
        into_stream_inner(self)
    }
}

#[macro_export]
macro_rules! define_state_store_read_associated_type {
    () => {
        type GetFuture<'a> = impl GetFutureTrait<'a>;
        type IterFuture<'a> = impl IterFutureTrait<'a, Self::IterStream>;
    };
}

pub trait GetFutureTrait<'a> = Future<Output = StorageResult<Option<Bytes>>> + Send + 'a;
pub type StateStoreIterItem = (FullKey<Bytes>, Bytes);
pub trait StateStoreIterNextFutureTrait<'a> = NextFutureTrait<'a, StateStoreIterItem>;
pub trait StateStoreIterItemStream = Stream<Item = StorageResult<StateStoreIterItem>> + Send;
pub trait StateStoreReadIterStream = StateStoreIterItemStream + 'static;

pub type IterKeyRange = (Bound<KeyPayloadType>, Bound<KeyPayloadType>);

pub trait IterFutureTrait<'a, I: StateStoreReadIterStream> =
    Future<Output = StorageResult<I>> + Send + 'a;
pub trait StateStoreRead: StaticSendSync {
    type IterStream: StateStoreReadIterStream;

    type GetFuture<'a>: GetFutureTrait<'a>;
    type IterFuture<'a>: IterFutureTrait<'a, Self::IterStream>;

    /// Point gets a value from the state store.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn get(&self, key: Bytes, epoch: u64, read_options: ReadOptions) -> Self::GetFuture<'_>;

    /// Opens and returns an iterator for given `prefix_hint` and `full_key_range`
    /// Internally, `prefix_hint` will be used to for checking `bloom_filter` and
    /// `full_key_range` used for iter. (if the `prefix_hint` not None, it should be be included
    /// in `key_range`) The returned iterator will iterate data based on a snapshot
    /// corresponding to the given `epoch`.
    fn iter(
        &self,
        key_range: IterKeyRange,
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_>;
}

pub trait ScanFutureTrait<'a> = Future<Output = StorageResult<Vec<StateStoreIterItem>>> + Send + 'a;

pub trait StateStoreReadExt: StaticSendSync {
    type ScanFuture<'a>: ScanFutureTrait<'a>;

    /// Scans `limit` number of keys from a key range. If `limit` is `None`, scans all elements.
    /// Internally, `prefix_hint` will be used to for checking `bloom_filter` and
    /// `full_key_range` used for iter.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    ///
    ///
    /// By default, this simply calls `StateStore::iter` to fetch elements.
    fn scan(
        &self,
        key_range: IterKeyRange,
        epoch: u64,
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::ScanFuture<'_>;
}

impl<S: StateStoreRead> StateStoreReadExt for S {
    type ScanFuture<'a> = impl ScanFutureTrait<'a>;

    fn scan(
        &self,
        key_range: IterKeyRange,
        epoch: u64,
        limit: Option<usize>,
        mut read_options: ReadOptions,
    ) -> Self::ScanFuture<'_> {
        if limit.is_some() {
            read_options.prefetch_options.exhaust_iter = false;
        }
        let limit = limit.unwrap_or(usize::MAX);
        async move {
            self.iter(key_range, epoch, read_options)
                .await?
                .take(limit)
                .try_collect()
                .await
        }
    }
}

#[macro_export]
macro_rules! define_state_store_write_associated_type {
    () => {
        type IngestBatchFuture<'a> = impl IngestBatchFutureTrait<'a>;
    };
}

pub trait IngestBatchFutureTrait<'a> = Future<Output = StorageResult<usize>> + Send + 'a;
pub trait StateStoreWrite: StaticSendSync {
    type IngestBatchFuture<'a>: IngestBatchFutureTrait<'a>;

    /// Writes a batch to storage. The batch should be:
    /// * Ordered. KV pairs will be directly written to the table, so it must be ordered.
    /// * Locally unique. There should not be two or more operations on the same key in one write
    ///   batch.
    ///
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
        delete_ranges: Vec<(Bytes, Bytes)>,
        write_options: WriteOptions,
    ) -> Self::IngestBatchFuture<'_>;

    /// Creates a `WriteBatch` associated with this state store.
    fn start_write_batch(&self, write_options: WriteOptions) -> WriteBatch<'_, Self>
    where
        Self: Sized,
    {
        WriteBatch::new(self, write_options)
    }
}

#[derive(Default, Debug)]
pub struct SyncResult {
    /// The size of all synced shared buffers.
    pub sync_size: usize,
    /// The sst_info of sync.
    pub uncommitted_ssts: Vec<LocalSstableInfo>,
}
pub trait EmptyFutureTrait<'a> = Future<Output = StorageResult<()>> + Send + 'a;
pub trait SyncFutureTrait<'a> = Future<Output = StorageResult<SyncResult>> + Send + 'a;

#[macro_export]
macro_rules! define_state_store_associated_type {
    () => {
        type WaitEpochFuture<'a> = impl EmptyFutureTrait<'a>;
        type SyncFuture<'a> = impl SyncFutureTrait<'a>;
        type ClearSharedBufferFuture<'a> = impl EmptyFutureTrait<'a>;
    };
}

pub trait StateStore: StateStoreRead + StaticSendSync + Clone {
    type Local: LocalStateStore;

    type WaitEpochFuture<'a>: EmptyFutureTrait<'a>;

    type SyncFuture<'a>: SyncFutureTrait<'a>;

    type ClearSharedBufferFuture<'a>: EmptyFutureTrait<'a>;

    type NewLocalFuture<'a>: Future<Output = Self::Local> + Send + 'a;

    /// If epoch is `Committed`, we will wait until the epoch is committed and its data is ready to
    /// read. If epoch is `Current`, we will only check if the data can be read with this epoch.
    fn try_wait_epoch(&self, epoch: HummockReadEpoch) -> Self::WaitEpochFuture<'_>;

    fn sync(&self, epoch: u64) -> Self::SyncFuture<'_>;

    /// update max current epoch in storage.
    fn seal_epoch(&self, epoch: u64, is_checkpoint: bool);

    /// Creates a [`MonitoredStateStore`] from this state store, with given `stats`.
    fn monitored(self, storage_metrics: Arc<MonitoredStorageMetrics>) -> MonitoredStateStore<Self> {
        MonitoredStateStore::new(self, storage_metrics)
    }

    /// Clears contents in shared buffer.
    /// This method should only be called when dropping all actors in the local compute node.
    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        todo!()
    }

    fn new_local(&self, option: NewLocalOptions) -> Self::NewLocalFuture<'_>;

    /// Validates whether store can serve `epoch` at the moment.
    fn validate_read_epoch(&self, epoch: HummockReadEpoch) -> StorageResult<()>;
}

pub trait MayExistTrait<'a> = Future<Output = StorageResult<bool>> + Send + 'a;

#[macro_export]
macro_rules! define_local_state_store_associated_type {
    () => {
        type MayExistFuture<'a> = impl MayExistTrait<'a>;
    };
}

/// A state store that is dedicated for streaming operator, which only reads the uncommitted data
/// written by itself. Each local state store is not `Clone`, and is owned by a streaming state
/// table.
pub trait LocalStateStore: StaticSendSync {
    type IterStream<'a>: StateStoreIterItemStream + 'a;

    type MayExistFuture<'a>: MayExistTrait<'a>;
    type GetFuture<'a>: GetFutureTrait<'a>;
    type IterFuture<'a>: Future<Output = StorageResult<Self::IterStream<'a>>> + Send + 'a;
    type FlushFuture<'a>: Future<Output = StorageResult<usize>> + Send + 'a;

    /// Point gets a value from the state store.
    /// The result is based on the latest written snapshot.
    fn get(&self, key: Bytes, read_options: ReadOptions) -> Self::GetFuture<'_>;

    /// Opens and returns an iterator for given `prefix_hint` and `full_key_range`
    /// Internally, `prefix_hint` will be used to for checking `bloom_filter` and
    /// `full_key_range` used for iter. (if the `prefix_hint` not None, it should be be included
    /// in `key_range`) The returned iterator will iterate data based on the latest written
    /// snapshot.
    fn iter(&self, key_range: IterKeyRange, read_options: ReadOptions) -> Self::IterFuture<'_>;

    /// Inserts a key-value entry associated with a given `epoch` into the state store.
    fn insert(&mut self, key: Bytes, new_val: Bytes, old_val: Option<Bytes>) -> StorageResult<()>;

    /// Deletes a key-value entry from the state store. Only the key-value entry with epoch smaller
    /// than the given `epoch` will be deleted.
    fn delete(&mut self, key: Bytes, old_val: Bytes) -> StorageResult<()>;

    fn flush(&mut self, delete_ranges: Vec<(Bytes, Bytes)>) -> Self::FlushFuture<'_>;

    fn epoch(&self) -> u64;

    fn is_dirty(&self) -> bool;

    fn init(&mut self, epoch: u64);

    /// Updates the monotonically increasing write epoch to `new_epoch`.
    /// All writes after this function is called will be tagged with `new_epoch`. In other words,
    /// the previous write epoch is sealed.
    fn seal_current_epoch(&mut self, next_epoch: u64);

    /// Check existence of a given `key_range`.
    /// It is better to provide `prefix_hint` in `read_options`, which will be used
    /// for checking bloom filter if hummock is used. If `prefix_hint` is not provided,
    /// the false positive rate can be significantly higher because bloom filter cannot
    /// be used.
    ///
    /// Returns:
    /// - false: `key_range` is guaranteed to be absent in storage.
    /// - true: `key_range` may or may not exist in storage.
    fn may_exist(
        &self,
        key_range: IterKeyRange,
        read_options: ReadOptions,
    ) -> Self::MayExistFuture<'_>;
}

/// If `exhaust_iter` is true, prefetch will be enabled. Prefetching may increase the memory
/// footprint of the CN process because the prefetched blocks cannot be evicted.
#[derive(Default, Clone, Copy)]
pub struct PrefetchOptions {
    /// `exhaust_iter` is set `true` only if the return value of `iter()` will definitely be
    /// exhausted, i.e., will iterate until end.
    pub exhaust_iter: bool,
}

impl PrefetchOptions {
    pub fn new_for_exhaust_iter() -> Self {
        Self { exhaust_iter: true }
    }
}

#[derive(Default, Clone)]
pub struct ReadOptions {
    /// A hint for prefix key to check bloom filter.
    /// If the `prefix_hint` is not None, it should be included in
    /// `key` or `key_range` in the read API.
    pub prefix_hint: Option<Bytes>,
    pub ignore_range_tombstone: bool,
    pub prefetch_options: PrefetchOptions,
    pub cache_policy: CachePolicy,

    pub retention_seconds: Option<u32>,
    pub table_id: TableId,
    /// Read from historical hummock version of meta snapshot backup.
    /// It should only be used by `StorageTable` for batch query.
    pub read_version_from_backup: bool,
}

pub fn gen_min_epoch(base_epoch: u64, retention_seconds: Option<&u32>) -> u64 {
    let base_epoch = Epoch(base_epoch);
    match retention_seconds {
        Some(retention_seconds_u32) => {
            base_epoch
                .subtract_ms((retention_seconds_u32 * 1000) as u64)
                .0
        }
        None => 0,
    }
}

#[derive(Default, Clone)]
pub struct WriteOptions {
    pub epoch: u64,
    pub table_id: TableId,
}

#[derive(Clone, Default)]
pub struct NewLocalOptions {
    pub table_id: TableId,
    /// Whether the operation is consistent. The term `consistent` requires the following:
    ///
    /// 1. A key cannot be inserted or deleted for more than once, i.e. inserting to an existing
    /// key or deleting an non-existing key is not allowed.
    ///
    /// 2. The old value passed from
    /// `update` and `delete` should match the original stored value.
    pub is_consistent_op: bool,
    pub table_option: TableOption,
}

impl NewLocalOptions {
    pub fn for_test(table_id: TableId) -> Self {
        Self {
            table_id,
            is_consistent_op: false,
            table_option: TableOption {
                retention_seconds: None,
            },
        }
    }
}
