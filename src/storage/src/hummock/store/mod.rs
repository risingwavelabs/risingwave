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

pub mod event_handler;
pub mod memtable;
pub mod state_store;
pub mod version;

use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use futures::Future;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;

use crate::error::StorageResult;
use crate::hummock::iterator::{
    ConcatIteratorInner, Forward, HummockIteratorUnion, OrderedMergeIteratorInner,
    UnorderedMergeIteratorInner, UserIterator,
};
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatchIterator;
use crate::hummock::SstableIterator;
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};
use crate::storage_value::StorageValue;
use crate::store::WriteOptions;
use crate::StateStoreIter;

pub trait GetFutureTrait<'a> = Future<Output = StorageResult<Option<Bytes>>> + Send;
pub trait IterFutureTrait<'a, I: StateStoreIter<Item = (Bytes, Bytes)>> =
    Future<Output = StorageResult<I>> + Send;

pub trait IngestKVBatchFutureTrait<'a> = Future<Output = StorageResult<usize>> + Send;

#[macro_export]
macro_rules! define_local_state_store_associated_type {
    () => {
        type GetFuture<'a> = impl GetFutureTrait<'a>;
        type IterFuture<'a> = impl IterFutureTrait<'a, Self::Iter>;
        type IngestKVBatchFuture<'a> = impl IngestKVBatchFutureTrait<'a>;
    };
}

/// State store v2.
/// It provides the basic functionalities streaming/batch executor needs to access the underlying
/// state store.
pub trait StateStore: Send + Sync + 'static + Clone {
    type Iter: StateStoreIter<Item = (Bytes, Bytes)>;

    type GetFuture<'a>: GetFutureTrait<'a>;

    type IterFuture<'a>: IterFutureTrait<'a, Self::Iter>;

    type IngestKVBatchFuture<'a>: IngestKVBatchFutureTrait<'a>;

    /// Point gets a value from the state store.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn get<'a>(
        &'a self,
        key: &'a [u8],
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::GetFuture<'_>;

    /// Opens and returns an iterator for a given `key_range`.
    /// The returned iterator will iterate data based on a snapshot corresponding to
    /// the given `epoch`.
    fn iter(
        &self,
        key_range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        epoch: u64,
        read_options: ReadOptions,
    ) -> Self::IterFuture<'_>;

    /// Inserts a key-value entry associated with a given `epoch` into the state store.
    fn insert(&self, key: Bytes, val: Bytes) -> StorageResult<()>;

    /// Deletes a key-value entry from the state store. Only the key-value entry with epoch smaller
    /// than the given `epoch` will be deleted.
    fn delete(&self, key: Bytes) -> StorageResult<()>;

    /// Triggers a flush to persistent storage for the in-memory states.
    fn flush(&self) -> StorageResult<usize>;

    /// Updates the monotonically increasing write epoch to `new_epoch`.
    /// All writes after this function is called will be tagged with `new_epoch`. In other words,
    /// the previous write epoch is sealed.
    fn advance_write_epoch(&mut self, new_epoch: u64) -> StorageResult<()>;

    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        write_options: WriteOptions,
    ) -> Self::IngestKVBatchFuture<'_>;
}

#[derive(Default, Clone)]
pub struct ReadOptions {
    /// A hint for prefix key to check bloom filter.
    /// If the `prefix_hint` is not None, it should be included in
    /// `key` or `key_range` in the read API.
    pub prefix_hint: Option<Vec<u8>>,
    pub check_bloom_filter: bool,

    // TODO: support min_epoch
    pub retention_seconds: Option<u32>,
    pub table_id: TableId,
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

type StagingDataIterator = OrderedMergeIteratorInner<
    HummockIteratorUnion<Forward, SharedBufferBatchIterator<Forward>, SstableIterator>,
>;
type HummockStorageIteratorPayload = UnorderedMergeIteratorInner<
    HummockIteratorUnion<
        Forward,
        StagingDataIterator,
        OrderedMergeIteratorInner<SstableIterator>,
        ConcatIteratorInner<SstableIterator>,
    >,
>;

pub struct HummockStorageIterator {
    inner: UserIterator<HummockStorageIteratorPayload>,
    metrics: Arc<StateStoreMetrics>,
}

impl StateStoreIter for HummockStorageIterator {
    type Item = (Bytes, Bytes);

    type NextFuture<'a> = impl Future<Output = StorageResult<Option<Self::Item>>> + Send;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async {
            let iter = &mut self.inner;

            if iter.is_valid() {
                let kv = (
                    Bytes::copy_from_slice(iter.key()),
                    Bytes::copy_from_slice(iter.value()),
                );
                iter.next().await?;
                Ok(Some(kv))
            } else {
                Ok(None)
            }
        }
    }
}

impl HummockStorageIterator {
    pub async fn collect(mut self, limit: Option<usize>) -> StorageResult<Vec<(Bytes, Bytes)>> {
        let mut kvs = Vec::with_capacity(limit.unwrap_or_default());

        for _ in 0..limit.unwrap_or(usize::MAX) {
            match self.next().await? {
                Some(kv) => kvs.push(kv),
                None => break,
            }
        }

        Ok(kvs)
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        self.inner.collect_local_statistic(stats);
    }
}

impl Drop for HummockStorageIterator {
    fn drop(&mut self) {
        let mut stats = StoreLocalStatistic::default();
        self.collect_local_statistic(&mut stats);
        stats.report(&self.metrics);
    }
}
