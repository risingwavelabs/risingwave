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

//! Hummock is the state store of the streaming system.

use std::cmp::Ordering;
use std::fmt;
use std::future::Future;
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::{VersionedComparator, *};
use risingwave_pb::hummock::LevelType;
use risingwave_rpc_client::HummockMetaClient;

mod block_cache;
pub use block_cache::*;
mod sstable;
pub use sstable::*;
pub mod compactor;
#[cfg(test)]
mod compactor_tests;
mod conflict_detector;
mod error;
pub mod hummock_meta_client;
mod iterator;
pub mod local_version_manager;
mod shared_buffer;
#[cfg(test)]
mod snapshot_tests;
mod sstable_store;
#[cfg(test)]
mod state_store_tests;
#[cfg(test)]
mod test_utils;
mod utils;
mod vacuum;
pub mod value;

pub use error::*;
use value::*;

use self::iterator::{
    BoxedHummockIterator, DirectedUserIterator, HummockIterator, MergeIterator,
    ReverseMergeIterator, UserIterator,
};
use self::key::{key_with_epoch, user_key, FullKey};
pub use self::sstable_store::*;
use self::utils::range_overlap;
use super::monitor::StateStoreMetrics;
use crate::hummock::iterator::{ConcatIterator, ReverseConcatIterator, ReverseUserIterator};
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::shared_buffer::shared_buffer_manager::SharedBufferManager;
use crate::hummock::utils::{validate_epoch, validate_table_key_range};
use crate::storage_value::StorageValue;
use crate::store::*;
use crate::{define_state_store_associated_type, StateStore, StateStoreIter};

/// Hummock is the state store backend.
#[derive(Clone)]
pub struct HummockStorage {
    options: Arc<StorageConfig>,

    local_version_manager: Arc<LocalVersionManager>,

    hummock_meta_client: Arc<dyn HummockMetaClient>,

    sstable_store: SstableStoreRef,

    /// Manager for immutable shared buffers
    shared_buffer_manager: Arc<SharedBufferManager>,

    /// Statistics
    stats: Arc<StateStoreMetrics>,
}

impl HummockStorage {
    /// Creates a [`HummockStorage`] with default stats. Should only be used by tests.
    pub async fn with_default_stats(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        local_version_manager: Arc<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        hummock_metrics: Arc<StateStoreMetrics>,
    ) -> HummockResult<Self> {
        Self::new(
            options,
            sstable_store,
            local_version_manager,
            hummock_meta_client,
            hummock_metrics,
        )
        .await
    }

    /// Creates a [`HummockStorage`].
    pub async fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        local_version_manager: Arc<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
    ) -> HummockResult<Self> {
        let shared_buffer_manager = Arc::new(SharedBufferManager::new(
            options.clone(),
            local_version_manager.clone(),
            sstable_store.clone(),
            stats.clone(),
            hummock_meta_client.clone(),
        ));

        LocalVersionManager::start_workers(
            local_version_manager.clone(),
            hummock_meta_client.clone(),
            shared_buffer_manager.clone(),
        );
        // Ensure at least one available version in cache.
        local_version_manager.wait_epoch(HummockEpoch::MIN).await?;

        let instance = Self {
            options: options.clone(),
            local_version_manager,
            hummock_meta_client,
            sstable_store,
            shared_buffer_manager,
            stats,
        };
        Ok(instance)
    }

    fn get_builder(options: &StorageConfig) -> SSTableBuilder {
        SSTableBuilder::new(SSTableBuilderOptions {
            capacity: options.sstable_size as usize,
            block_capacity: options.block_size as usize,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            bloom_false_positive: options.bloom_false_positive,
            // TODO: Make this configurable.
            compression_algorithm: CompressionAlgorithm::None,
        })
    }

    async fn get_from_table(
        &self,
        table: Arc<Sstable>,
        internal_key: &[u8],
        key: &[u8],
    ) -> HummockResult<Option<Bytes>> {
        if table.surely_not_have_user_key(key) {
            self.stats.bloom_filter_true_negative_counts.inc();
            return Ok(None);
        }
        // Might have the key, take it as might positive.
        self.stats.bloom_filter_might_positive_counts.inc();
        let mut iter = SSTableIterator::new(table, self.sstable_store.clone());
        iter.seek(internal_key).await?;
        // Iterator has seeked passed the borders.
        if !iter.is_valid() {
            return Ok(None);
        }

        // Iterator gets us the key, we tell if it's the key we want
        // or key next to it.
        let value = match user_key(iter.key()) == key {
            true => iter.value().into_user_value().map(Bytes::copy_from_slice),
            false => None,
        };
        Ok(value)
    }

    pub fn hummock_meta_client(&self) -> &Arc<dyn HummockMetaClient> {
        &self.hummock_meta_client
    }

    pub fn options(&self) -> &Arc<StorageConfig> {
        &self.options
    }

    pub fn sstable_store(&self) -> SstableStoreRef {
        self.sstable_store.clone()
    }

    pub fn local_version_manager(&self) -> &Arc<LocalVersionManager> {
        &self.local_version_manager
    }

    pub fn shared_buffer_manager(&self) -> &SharedBufferManager {
        &self.shared_buffer_manager
    }
}

impl fmt::Debug for HummockStorage {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl StateStore for HummockStorage {
    type Iter<'a> = HummockStateStoreIter<'a>;
    define_state_store_associated_type!();

    /// Gets the value of a specified `key`.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    fn get<'a>(&'a self, key: &'a [u8], epoch: u64) -> Self::GetFuture<'_> {
        async move {
            let version = self.local_version_manager.get_version()?;
            // check epoch validity
            validate_epoch(version.safe_epoch(), epoch)?;

            // Query shared buffer. Return the value without iterating SSTs if found
            if version.max_committed_epoch() < epoch {
                if let Some(v) = self
                    .shared_buffer_manager
                    .get(key, (version.max_committed_epoch() + 1)..=epoch)
                {
                    self.stats.get_shared_buffer_hit_counts.inc();
                    return Ok(v.into_user_value().map(|v| v.into()));
                }
            }
            let internal_key = key_with_epoch(key.to_vec(), epoch);

            let mut table_counts = 0;
            for level in &version.levels() {
                if level.table_infos.is_empty() {
                    continue;
                }
                match level.level_type() {
                    LevelType::Overlapping => {
                        let table_infos = level
                            .table_infos
                            .iter()
                            .filter(|info| {
                                let table_range = info.key_range.as_ref().unwrap();
                                let table_start = user_key(table_range.left.as_slice());
                                let table_end = user_key(table_range.right.as_slice());
                                table_start.le(key) && table_end.ge(key)
                            })
                            .map(|info| info.id)
                            .collect_vec();
                        let tables = self
                            .local_version_manager
                            .pick_few_tables(&table_infos)
                            .await?;
                        for table in tables.into_iter().rev() {
                            table_counts += 1;
                            if let Some(v) = self.get_from_table(table, &internal_key, key).await? {
                                return Ok(Some(v));
                            }
                        }
                    }
                    LevelType::Nonoverlapping => {
                        let table_idx = level
                            .table_infos
                            .partition_point(|table| {
                                let ord = VersionedComparator::compare_key(
                                    user_key(&table.key_range.as_ref().unwrap().left),
                                    key,
                                );
                                ord == Ordering::Less || ord == Ordering::Equal
                            })
                            .saturating_sub(1); // considering the boundary of 0
                        assert!(table_idx < level.table_infos.len());
                        table_counts += 1;
                        // Because we will keep multiple version of one in the same sst file, we
                        // do not find it in the next adjacent file.
                        let tables = self
                            .local_version_manager
                            .pick_few_tables(&[level.table_infos[table_idx].id])
                            .await?;
                        if let Some(v) = self
                            .get_from_table(tables.first().unwrap().clone(), &internal_key, key)
                            .await?
                        {
                            return Ok(Some(v));
                        }
                    }
                }
            }

            self.stats
                .iter_merge_sstable_counts
                .observe(table_counts as f64);
            Ok(None)
        }
    }

    fn scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            let mut iter = self.iter(key_range, epoch).await?;
            let mut kvs = Vec::with_capacity(limit.unwrap_or_default());

            for _ in 0..limit.unwrap_or(usize::MAX) {
                match iter.next().await? {
                    Some(kv) => kvs.push(kv),
                    None => break,
                }
            }

            Ok(kvs)
        }
    }

    fn reverse_scan<R, B>(
        &self,
        key_range: R,
        limit: Option<usize>,
        epoch: u64,
    ) -> Self::ReverseScanFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            let mut iter = self.reverse_iter(key_range, epoch).await?;
            let mut kvs = Vec::with_capacity(limit.unwrap_or_default());

            for _ in 0..limit.unwrap_or(usize::MAX) {
                match iter.next().await? {
                    Some(kv) => kvs.push(kv),
                    None => break,
                }
            }

            Ok(kvs)
        }
    }

    /// Writes a batch to storage. The batch should be:
    /// * Ordered. KV pairs will be directly written to the table, so it must be ordered.
    /// * Locally unique. There should not be two or more operations on the same key in one write
    ///   batch.
    /// * Globally unique. The streaming operators should ensure that different operators won't
    ///   operate on the same key. The operator operating on one keyspace should always wait for all
    ///   changes to be committed before reading and writing new keys to the engine. That is because
    ///   that the table with lower epoch might be committed after a table with higher epoch has
    ///   been committed. If such case happens, the outcome is non-predictable.
    fn ingest_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        epoch: u64,
    ) -> Self::IngestBatchFuture<'_> {
        async move {
            let batch = kv_pairs
                .into_iter()
                .map(|(key, value)| {
                    (
                        Bytes::from(FullKey::from_user_key(key.to_vec(), epoch).into_inner()),
                        value.into(),
                    )
                })
                .collect_vec();
            self.shared_buffer_manager.write_batch(batch, epoch)?;

            if !self.options.async_checkpoint_enabled {
                self.shared_buffer_manager.sync(Some(epoch)).await?;
            }
            Ok(())
        }
    }

    /// Replicates a batch to shared buffer, without uploading to the storage backend.
    fn replicate_batch(
        &self,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        epoch: u64,
    ) -> Self::ReplicateBatchFuture<'_> {
        async move {
            let batch = kv_pairs
                .into_iter()
                .map(|(key, value)| {
                    (
                        Bytes::from(FullKey::from_user_key(key.to_vec(), epoch).into_inner()),
                        value.into(),
                    )
                })
                .collect_vec();
            self.shared_buffer_manager
                .replicate_remote_batch(batch, epoch)?;

            Ok(())
        }
    }

    /// Returns an iterator that scan from the begin key to the end key
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn iter<R, B>(&self, key_range: R, epoch: u64) -> Self::IterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            let version = self.local_version_manager.get_version()?;
            // Check epoch validity
            validate_epoch(version.safe_epoch(), epoch)?;
            let levels = version.levels();
            validate_table_key_range(&levels)?;

            // Filter out tables that overlap with given `key_range`
            let mut overlapped_sstable_iters = vec![];
            for level in &version.levels() {
                let table_ids = level
                    .table_infos
                    .iter()
                    .filter(|info| {
                        let table_range = info.key_range.as_ref().unwrap();
                        let table_start = user_key(table_range.left.as_slice());
                        let table_end = user_key(table_range.right.as_slice());
                        range_overlap(&key_range, table_start, table_end, false)
                    })
                    .map(|info| info.id)
                    .collect_vec();
                if table_ids.is_empty() {
                    continue;
                }
                let tables = self
                    .local_version_manager
                    .pick_few_tables(&table_ids)
                    .await?;
                match level.level_type() {
                    LevelType::Overlapping => {
                        for table in tables.into_iter().rev() {
                            overlapped_sstable_iters.push(Box::new(SSTableIterator::new(
                                table,
                                self.sstable_store.clone(),
                            ))
                                as BoxedHummockIterator);
                        }
                    }
                    LevelType::Nonoverlapping => overlapped_sstable_iters.push(Box::new(
                        ConcatIterator::new(tables, self.sstable_store.clone()),
                    )),
                }
            }
            self.stats
                .iter_merge_sstable_counts
                .observe(overlapped_sstable_iters.len() as f64);
            let mi = if version.max_committed_epoch() < epoch {
                // Take shared buffers into consideration if the read epoch is above the max
                // committed epoch
                let overlapped_shared_buffer_iters = self
                    .shared_buffer_manager
                    .iters(&key_range, (version.max_committed_epoch() + 1)..=epoch)
                    .into_iter()
                    .map(|i| Box::new(i) as BoxedHummockIterator);
                MergeIterator::new(
                    overlapped_shared_buffer_iters.chain(overlapped_sstable_iters),
                    self.stats.clone(),
                )
            } else {
                MergeIterator::new(overlapped_sstable_iters, self.stats.clone())
            };

            // TODO: avoid this clone
            let mut user_iter = DirectedUserIterator::Forward(UserIterator::new(
                mi,
                (
                    key_range.start_bound().map(|b| b.as_ref().to_owned()),
                    key_range.end_bound().map(|b| b.as_ref().to_owned()),
                ),
                epoch,
                Some(version),
            ));

            user_iter.rewind().await?;
            Ok(HummockStateStoreIter::new(user_iter))
        }
    }

    /// Returns a reversed iterator that scans from the end key to the begin key
    /// The result is based on a snapshot corresponding to the given `epoch`.
    fn reverse_iter<R, B>(&self, key_range: R, epoch: u64) -> Self::ReverseIterFuture<'_, R, B>
    where
        R: RangeBounds<B> + Send,
        B: AsRef<[u8]> + Send,
    {
        async move {
            let version = self.local_version_manager.get_version()?;
            // Check epoch validity
            validate_epoch(version.safe_epoch(), epoch)?;
            // Filter out tables that overlap with given `key_range`
            let mut overlapped_sstable_iters = vec![];
            for level in &version.levels() {
                let table_ids = level
                    .table_infos
                    .iter()
                    .filter(|info| {
                        let table_range = info.key_range.as_ref().unwrap();
                        let table_start = user_key(table_range.left.as_slice());
                        let table_end = user_key(table_range.right.as_slice());
                        range_overlap(&key_range, table_start, table_end, true)
                    })
                    .map(|info| info.id)
                    .collect_vec();
                if table_ids.is_empty() {
                    continue;
                }
                let mut tables = self
                    .local_version_manager
                    .pick_few_tables(&table_ids)
                    .await?;
                match level.level_type() {
                    LevelType::Overlapping => {
                        for table in tables.into_iter().rev() {
                            overlapped_sstable_iters.push(Box::new(ReverseSSTableIterator::new(
                                table,
                                self.sstable_store.clone(),
                            ))
                                as BoxedHummockIterator);
                        }
                    }
                    LevelType::Nonoverlapping => {
                        if tables.len() > 1 {
                            tables.reverse();
                            overlapped_sstable_iters.push(Box::new(ReverseConcatIterator::new(
                                tables,
                                self.sstable_store.clone(),
                            )))
                        } else {
                            overlapped_sstable_iters.push(Box::new(ReverseSSTableIterator::new(
                                tables.pop().unwrap(),
                                self.sstable_store.clone(),
                            ))
                                as BoxedHummockIterator);
                        }
                    }
                }
            }
            self.stats
                .iter_merge_sstable_counts
                .observe(overlapped_sstable_iters.len() as f64);
            let reverse_merge_iterator = if version.max_committed_epoch() < epoch {
                // Take shared buffers into consideration if the read epoch is above the max
                // committed epoch
                let overlapped_shared_buffer_iters = self
                    .shared_buffer_manager
                    .reverse_iters(&key_range, (version.max_committed_epoch() + 1)..=epoch)
                    .into_iter()
                    .map(|i| Box::new(i) as BoxedHummockIterator);
                ReverseMergeIterator::new(
                    overlapped_shared_buffer_iters.chain(overlapped_sstable_iters),
                    self.stats.clone(),
                )
            } else {
                ReverseMergeIterator::new(overlapped_sstable_iters, self.stats.clone())
            };

            // TODO: avoid this clone
            let mut reverse_user_iter =
                DirectedUserIterator::Backward(ReverseUserIterator::new_with_epoch(
                    reverse_merge_iterator,
                    (
                        key_range.end_bound().map(|b| b.as_ref().to_owned()),
                        key_range.start_bound().map(|b| b.as_ref().to_owned()),
                    ),
                    epoch,
                    Some(version),
                ));

            reverse_user_iter.rewind().await?;
            Ok(HummockStateStoreIter::new(reverse_user_iter))
        }
    }

    fn wait_epoch(&self, epoch: u64) -> Self::WaitEpochFuture<'_> {
        async move { Ok(self.local_version_manager.wait_epoch(epoch).await?) }
    }

    fn sync(&self, epoch: Option<u64>) -> Self::SyncFuture<'_> {
        async move {
            self.shared_buffer_manager.sync(epoch).await?;
            Ok(())
        }
    }
}

pub struct HummockStateStoreIter<'a> {
    inner: DirectedUserIterator<'a>,
}

impl<'a> HummockStateStoreIter<'a> {
    fn new(inner: DirectedUserIterator<'a>) -> Self {
        Self { inner }
    }
}

impl<'a> StateStoreIter for HummockStateStoreIter<'a> {
    // TODO: directly return `&[u8]` to user instead of `Bytes`.
    type Item = (Bytes, Bytes);
    type NextFuture<'b> = impl Future<Output = crate::error::StorageResult<Option<Self::Item>>> where Self:'b;
    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
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
