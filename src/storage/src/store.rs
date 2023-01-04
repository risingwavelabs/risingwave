// Copyright 2023 Singularity Data
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

use std::cmp::Ordering;
use std::future::Future;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;
use risingwave_hummock_sdk::key::{FullKey, TableKey};
use risingwave_hummock_sdk::{HummockReadEpoch, LocalSstableInfo};

use crate::error::{StorageError, StorageResult};
use crate::mem_table::{KeyOp, MemTable};
use crate::monitor::{MonitoredStateStore, StateStoreMetrics};
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
// TODO: directly return `&[u8]` or `Bytes` to user instead of `Vec<u8>`.
pub type StateStoreIterItem = (FullKey<Bytes>, Bytes);
pub trait StateStoreIterNextFutureTrait<'a> = NextFutureTrait<'a, StateStoreIterItem>;
pub trait StateStoreIterItemStream = Stream<Item = StorageResult<StateStoreIterItem>> + Send;
pub trait StateStoreReadIterStream = StateStoreIterItemStream + 'static;

pub trait IterFutureTrait<'a, I: StateStoreReadIterStream> =
    Future<Output = StorageResult<I>> + Send + 'a;
pub trait StateStoreRead: StaticSendSync {
    type IterStream: StateStoreReadIterStream;

    type GetFuture<'a>: GetFutureTrait<'a>;
    type IterFuture<'a>: IterFutureTrait<'a, Self::IterStream>;

    /// Point gets a value from the state store.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_>;

    /// Opens and returns an iterator for given `prefix_hint` and `full_key_range`
    /// Internally, `prefix_hint` will be used to for checking `bloom_filter` and
    /// `full_key_range` used for iter. (if the `prefix_hint` not None, it should be be included
    /// in `key_range`) The returned iterator will iterate data based on a snapshot
    /// corresponding to the given `epoch`.
    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
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
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::ScanFuture<'_>;
}

impl<S: StateStoreRead> StateStoreReadExt for S {
    type ScanFuture<'a> = impl ScanFutureTrait<'a>;

    fn scan(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        limit: Option<usize>,
        read_options: ReadOptions,
    ) -> Self::ScanFuture<'_> {
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
    fn monitored(self, stats: Arc<StateStoreMetrics>) -> MonitoredStateStore<Self> {
        MonitoredStateStore::new(self, stats)
    }

    /// Clears contents in shared buffer.
    /// This method should only be called when dropping all actors in the local compute node.
    fn clear_shared_buffer(&self) -> Self::ClearSharedBufferFuture<'_> {
        todo!()
    }

    fn new_local(&self, table_id: TableId) -> Self::NewLocalFuture<'_>;
}

/// A state store that is dedicated for streaming operator, which only reads the uncommitted data
/// written by itself. Each local state store is not `Clone`, and is owned by a streaming state
/// table.
pub trait LocalStateStore: StaticSendSync {
    type IterStream<'a>: StateStoreIterItemStream + 'a;

    type GetFuture<'a>: GetFutureTrait<'a>;
    type IterFuture<'a>: Future<Output = StorageResult<Self::IterStream<'a>>> + Send + 'a;
    type SealEpochFuture<'a>: Future<Output = StorageResult<()>> + Send + 'a;

    /// Point gets a value from the state store.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn get<'a>(&'a self, key: &'a [u8], read_options: ReadOptions) -> Self::GetFuture<'_>;

    /// Opens and returns an iterator for given `prefix_hint` and `full_key_range`
    /// Internally, `prefix_hint` will be used to for checking `bloom_filter` and
    /// `full_key_range` used for iter. (if the `prefix_hint` not None, it should be be included
    /// in `key_range`) The returned iterator will iterate data based on a snapshot
    /// corresponding to the given `epoch`.
    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_>;

    /// Inserts a key-value entry associated with a given `epoch` into the state store.
    fn insert(&mut self, key: Bytes, new_val: Bytes, old_val: Option<Bytes>) -> StorageResult<()>;

    /// Deletes a key-value entry from the state store. Only the key-value entry with epoch smaller
    /// than the given `epoch` will be deleted.
    fn delete(&mut self, key: Bytes, old_val: Bytes) -> StorageResult<()>;

    fn epoch(&self) -> u64;

    fn is_dirty(&self) -> bool;

    fn init(&mut self, epoch: u64);

    /// Updates the monotonically increasing write epoch to `new_epoch`.
    /// All writes after this function is called will be tagged with `new_epoch`. In other words,
    /// the previous write epoch is sealed.
    fn seal_current_epoch(
        &mut self,
        next_epoch: u64,
        delete_ranges: Vec<(Bytes, Bytes)>,
    ) -> Self::SealEpochFuture<'_>;
}

pub struct BatchWriteLocalStateStore<S: StateStoreWrite + StateStoreRead> {
    mem_table: MemTable,
    inner: S,
    epoch: Option<u64>,
    table_id: TableId,
}

#[try_stream(ok = StateStoreIterItem, error = StorageError)]
async fn merge_stream<'a>(
    mem_table_iter: impl Iterator<Item = (&'a Bytes, &'a KeyOp)> + 'a,
    inner_stream: impl StateStoreReadIterStream,
    table_id: TableId,
    epoch: u64,
) {
    let inner_stream = inner_stream.peekable();
    pin_mut!(inner_stream);

    let mut mem_table_iter = mem_table_iter.fuse().peekable();

    loop {
        match (inner_stream.as_mut().peek().await, mem_table_iter.peek()) {
            (None, None) => break,
            // The mem table side has come to an end, return data from the shared storage.
            (Some(_), None) => {
                let (key, value) = inner_stream.next().await.unwrap()?;
                yield (key, value)
            }
            // The stream side has come to an end, return data from the mem table.
            (None, Some(_)) => {
                let (key, key_op) = mem_table_iter.next().unwrap();
                match key_op {
                    KeyOp::Insert(value) | KeyOp::Update((_, value)) => {
                        yield (
                            FullKey::new(table_id, TableKey(key.clone()), epoch),
                            value.clone(),
                        )
                    }
                    _ => {}
                }
            }
            (Some(Ok((inner_key, _))), Some((mem_table_key, _))) => {
                debug_assert_eq!(inner_key.user_key.table_id, table_id);
                match inner_key.user_key.table_key.0.cmp(mem_table_key) {
                    Ordering::Less => {
                        // yield data from storage
                        let (key, value) = inner_stream.next().await.unwrap()?;
                        yield (key, value);
                    }
                    Ordering::Equal => {
                        // both memtable and storage contain the key, so we advance both
                        // iterators and return the data in memory.

                        let (_, key_op) = mem_table_iter.next().unwrap();
                        let (key, old_value_in_inner) = inner_stream.next().await.unwrap()?;
                        match key_op {
                            KeyOp::Insert(value) => {
                                yield (key.clone(), value.clone());
                            }
                            KeyOp::Delete(_) => {}
                            KeyOp::Update((old_value, new_value)) => {
                                debug_assert!(old_value == &old_value_in_inner);

                                yield (key, new_value.clone());
                            }
                        }
                    }
                    Ordering::Greater => {
                        // yield data from mem table
                        let (key, key_op) = mem_table_iter.next().unwrap();

                        match key_op {
                            KeyOp::Insert(value) => {
                                yield (
                                    FullKey::new(table_id, TableKey(key.clone()), epoch),
                                    value.clone(),
                                );
                            }
                            KeyOp::Delete(_) => {}
                            KeyOp::Update(_) => unreachable!(
                                "memtable update should always be paired with a storage key"
                            ),
                        }
                    }
                }
            }
            (Some(Err(_)), Some(_)) => {
                // Throw the error.
                return Err(inner_stream.next().await.unwrap().unwrap_err());
            }
        }
    }
}

impl<S: StateStoreWrite + StateStoreRead> BatchWriteLocalStateStore<S> {
    pub fn new(inner: S, table_id: TableId) -> Self {
        Self {
            inner,
            mem_table: MemTable::new(),
            epoch: None,
            table_id,
        }
    }
}

impl<S: StateStoreWrite + StateStoreRead> LocalStateStore for BatchWriteLocalStateStore<S> {
    type GetFuture<'a> = impl GetFutureTrait<'a>;
    type IterFuture<'a> = impl Future<Output = StorageResult<Self::IterStream<'a>>> + Send + 'a;
    type IterStream<'a> = impl StateStoreIterItemStream + 'a;
    type SealEpochFuture<'a> = impl Future<Output = StorageResult<()>> + 'a;

    fn get<'a>(&'a self, key: &'a [u8], read_options: ReadOptions) -> Self::GetFuture<'_> {
        async move { self.inner.get(key, self.epoch(), read_options).await }
    }

    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_> {
        async move {
            let stream = self
                .inner
                .iter(key_range.clone(), self.epoch(), read_options)
                .await?;
            let (l, r) = key_range;
            let key_range = (l.map(Bytes::from), r.map(Bytes::from));
            Ok(merge_stream(
                self.mem_table.iter(key_range),
                stream,
                self.table_id,
                self.epoch(),
            ))
            // Ok(self.merge_stream(stream))
        }
    }

    fn insert(&mut self, key: Bytes, new_val: Bytes, old_val: Option<Bytes>) -> StorageResult<()> {
        Ok(match old_val {
            None => self.mem_table.insert(key, new_val)?,
            Some(old_val) => self.mem_table.update(key, new_val, old_val)?,
        })
    }

    fn delete(&mut self, key: Bytes, old_val: Bytes) -> StorageResult<()> {
        Ok(self.mem_table.delete(key, old_val)?)
    }

    fn epoch(&self) -> u64 {
        self.epoch.expect("should have set the epoch")
    }

    fn is_dirty(&self) -> bool {
        self.mem_table.is_dirty()
    }

    fn init(&mut self, epoch: u64) {
        assert!(
            self.epoch.replace(epoch).is_none(),
            "local state store of table id {:?} is init for more than once",
            self.table_id
        );
    }

    fn seal_current_epoch(
        &mut self,
        next_epoch: u64,
        delete_ranges: Vec<(Bytes, Bytes)>,
    ) -> Self::SealEpochFuture<'_> {
        async move {
            debug_assert!(delete_ranges.iter().map(|(key, _)| key).is_sorted());
            let buffer = self.mem_table.drain().into_parts();
            let mut kv_pairs = Vec::with_capacity(buffer.len());
            for (key, key_op) in buffer {
                // TODO: filter by delete range
                //         if let Some(ref range_end) = range_end_suffix && &pk[VirtualNode::SIZE..]
                // < range_end.as_slice() {             continue;
                //         }
                match key_op {
                    // Currently, some executors do not strictly comply with these semantics. As
                    // a workaround you may call disable the check by initializing the
                    // state store with `disable_sanity_check=true`.
                    KeyOp::Insert(value) => {
                        // if ENABLE_SANITY_CHECK && !self.disable_sanity_check {
                        //     self.do_insert_sanity_check(&pk, &row, epoch).await?;
                        // }
                        kv_pairs.push((key, StorageValue::new_put(value)));
                    }
                    KeyOp::Delete(_) => {
                        // if ENABLE_SANITY_CHECK && !self.disable_sanity_check {
                        //     self.do_delete_sanity_check(&pk, &row, epoch).await?;
                        // }
                        kv_pairs.push((key, StorageValue::new_delete()));
                    }
                    KeyOp::Update((_, new_value)) => {
                        // if ENABLE_SANITY_CHECK && !self.disable_sanity_check {
                        //     self.do_update_sanity_check(&pk, &old_row, &new_row, epoch)
                        //         .await?;
                        // }
                        kv_pairs.push((key, StorageValue::new_put(new_value)));
                    }
                }
            }
            self.inner
                .ingest_batch(
                    kv_pairs,
                    delete_ranges,
                    WriteOptions {
                        epoch: self.epoch(),
                        table_id: self.table_id,
                    },
                )
                .await?;

            let prev_epoch = self
                .epoch
                .replace(next_epoch)
                .expect("should have init epoch before seal the first epoch");
            assert!(
                next_epoch > prev_epoch,
                "new epoch {} should be greater than current epoch: {}",
                next_epoch,
                prev_epoch
            );
            Ok(())
        }
    }
}

#[derive(Default, Clone)]
pub struct ReadOptions {
    /// A hint for prefix key to check bloom filter.
    /// If the `prefix_hint` is not None, it should be included in
    /// `key` or `key_range` in the read API.
    pub prefix_hint: Option<Vec<u8>>,
    pub ignore_range_tombstone: bool,
    pub check_bloom_filter: bool,

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
