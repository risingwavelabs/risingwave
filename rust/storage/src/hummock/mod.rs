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
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;

mod block_cache;
pub use block_cache::*;
mod sstable;
use risingwave_common::error::Result;
pub use sstable::*;
pub mod compactor;
mod conflict_detector;
mod error;
pub mod hummock_meta_client;
mod iterator;
pub mod key;
pub mod key_range;
pub mod local_version_manager;
#[cfg(test)]
pub(crate) mod mock;
mod shared_buffer;
#[cfg(test)]
mod snapshot_tests;
mod sstable_store;
mod state_store;
#[cfg(test)]
mod state_store_tests;
#[cfg(test)]
mod test_utils;
mod utils;
mod vacuum;
pub mod value;
mod version_cmp;

pub use error::*;
use risingwave_common::config::StorageConfig;
use risingwave_pb::hummock::LevelType;
use value::*;

use self::iterator::{
    BoxedHummockIterator, HummockIterator, MergeIterator, ReverseMergeIterator, UserIterator,
};
use self::key::{key_with_epoch, user_key, FullKey};
pub use self::sstable_store::*;
pub use self::state_store::*;
use self::utils::range_overlap;
use super::monitor::StateStoreMetrics;
use crate::hummock::hummock_meta_client::HummockMetaClient;
use crate::hummock::iterator::ReverseUserIterator;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::shared_buffer::shared_buffer_manager::SharedBufferManager;
use crate::hummock::utils::validate_epoch;
use crate::hummock::version_cmp::VersionedComparator;

pub type HummockTTL = u64;
pub type HummockSSTableId = u64;
pub type HummockRefCount = u64;
pub type HummockVersionId = u64;
pub type HummockContextId = u32;
pub type HummockEpoch = u64;
pub const INVALID_EPOCH: HummockEpoch = 0;
pub const INVALID_VERSION_ID: HummockVersionId = 0;
pub const FIRST_VERSION_ID: HummockVersionId = 1;

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

    /// Gets the value of a specified `key`.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    pub async fn get(&self, key: &[u8], epoch: u64) -> HummockResult<Option<Vec<u8>>> {
        let version = self.local_version_manager.get_version()?;
        // check epoch validity
        validate_epoch(version.safe_epoch(), epoch)?;

        // Query shared buffer. Return the value without iterating SSTs if found
        if let Some(v) = self
            .shared_buffer_manager
            .get(key, (version.max_committed_epoch() + 1)..=epoch)
        {
            self.stats.get_shared_buffer_hit_counts.inc();
            return Ok(v.into_put_value().map(|x| x.to_vec()));
        }
        let internal_key = key_with_epoch(key.to_vec(), epoch);

        let mut table_counts = 0;
        for level in &version.levels() {
            let tables = self
                .local_version_manager
                .pick_few_tables(&level.table_ids)
                .await?;
            match level.level_type() {
                LevelType::Overlapping => {
                    for table in tables {
                        table_counts += 1;
                        if let Some(v) = self.get_from_table(table, &internal_key, key).await? {
                            return Ok(Some(v));
                        }
                    }
                }
                LevelType::Nonoverlapping => {
                    let table_idx = tables
                        .partition_point(|table| {
                            let ord = VersionedComparator::compare_key(
                                user_key(&table.meta.smallest_key),
                                key,
                            );
                            ord == Ordering::Less || ord == Ordering::Equal
                        })
                        .saturating_sub(1); // considering the boundary of 0
                    if table_idx < tables.len() {
                        table_counts += 1;
                        // Because we will keep multiple version of one in the same sst file, we do
                        // not find it in the next adjacent file.
                        if let Some(v) = self
                            .get_from_table(tables[table_idx].clone(), &internal_key, key)
                            .await?
                        {
                            return Ok(Some(v));
                        }
                    }
                }
            }
        }

        self.stats
            .iter_merge_sstable_counts
            .observe(table_counts as f64);
        Ok(None)
    }

    /// Returns an iterator that scan from the begin key to the end key
    /// The result is based on a snapshot corresponding to the given `epoch`.
    pub async fn range_scan<R, B>(
        &self,
        key_range: R,
        epoch: u64,
    ) -> HummockResult<UserIterator<'_>>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        let version = self.local_version_manager.get_version()?;
        // Check epoch validity
        validate_epoch(version.safe_epoch(), epoch)?;

        // Filter out tables that overlap with given `key_range`
        let tables = self.local_version_manager.tables(&version.levels()).await?;
        let tables_count = tables.len();
        let overlapped_sstable_iters = tables
            .into_iter()
            .filter(|t| {
                let table_start = user_key(t.meta.smallest_key.as_slice());
                let table_end = user_key(t.meta.largest_key.as_slice());
                range_overlap(&key_range, table_start, table_end, false)
            })
            .map(|t| {
                Box::new(SSTableIterator::new(t, self.sstable_store.clone()))
                    as BoxedHummockIterator
            });

        let mi = if version.max_committed_epoch() < epoch {
            // Take shared buffers into consideration if the read epoch is above the max committed
            // epoch
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
            self.stats
                .iter_merge_sstable_counts
                .observe(tables_count as f64);
            MergeIterator::new(overlapped_sstable_iters, self.stats.clone())
        };

        // TODO: avoid this clone
        Ok(UserIterator::new(
            mi,
            (
                key_range.start_bound().map(|b| b.as_ref().to_owned()),
                key_range.end_bound().map(|b| b.as_ref().to_owned()),
            ),
            epoch,
            Some(version),
        ))
    }

    /// Returns a reversed iterator that scans from the end key to the begin key
    /// The result is based on a snapshot corresponding to the given `epoch`.
    pub async fn reverse_range_scan<R, B>(
        &self,
        key_range: R,
        epoch: u64,
    ) -> HummockResult<ReverseUserIterator<'_>>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        let version = self.local_version_manager.get_version()?;
        // Check epoch validity
        validate_epoch(version.safe_epoch(), epoch)?;

        // Filter out tables that overlap with given `key_range`
        let overlapped_sstable_iters = self
            .local_version_manager
            .tables(&version.levels())
            .await?
            .into_iter()
            .filter(|t| {
                let table_start = user_key(t.meta.smallest_key.as_slice());
                let table_end = user_key(t.meta.largest_key.as_slice());
                range_overlap(&key_range, table_start, table_end, true)
            })
            .map(|t| {
                Box::new(ReverseSSTableIterator::new(t, self.sstable_store.clone()))
                    as BoxedHummockIterator
            });

        let reverse_merge_iterator = if version.max_committed_epoch() < epoch {
            // Take shared buffers into consideration if the read epoch is above the max committed
            // epoch
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
        Ok(ReverseUserIterator::new_with_epoch(
            reverse_merge_iterator,
            (
                key_range.end_bound().map(|b| b.as_ref().to_owned()),
                key_range.start_bound().map(|b| b.as_ref().to_owned()),
            ),
            epoch,
            Some(version),
        ))
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
    pub async fn write_batch(
        &self,
        kv_pairs: impl Iterator<Item = (Bytes, HummockValue<Bytes>)>,
        epoch: u64,
    ) -> HummockResult<()> {
        let batch = kv_pairs
            .map(|(key, value)| {
                (
                    Bytes::from(FullKey::from_user_key(key.to_vec(), epoch).into_inner()),
                    value,
                )
            })
            .collect_vec();
        self.shared_buffer_manager.write_batch(batch, epoch)?;

        if !self.options.async_checkpoint_enabled {
            return self.shared_buffer_manager.sync(Some(epoch)).await;
        }
        Ok(())
    }

    /// Replicates a batch to shared buffer, without uploading to the storage backend.
    pub async fn replicate_batch(
        &self,
        kv_pairs: impl Iterator<Item = (Bytes, HummockValue<Bytes>)>,
        epoch: u64,
    ) -> HummockResult<()> {
        let batch = kv_pairs
            .map(|i| {
                (
                    Bytes::from(FullKey::from_user_key(i.0.to_vec(), epoch).into_inner()),
                    i.1,
                )
            })
            .collect_vec();
        self.shared_buffer_manager
            .replicate_remote_batch(batch, epoch)?;

        // self.sync(epoch).await?;
        Ok(())
    }

    pub async fn sync(&self, epoch: Option<u64>) -> HummockResult<()> {
        self.shared_buffer_manager.sync(epoch).await
    }

    #[cfg(not(feature = "blockv2"))]
    fn get_builder(options: &StorageConfig) -> SSTableBuilder {
        SSTableBuilder::new(SSTableBuilderOptions {
            table_capacity: options.sstable_size,
            block_size: options.block_size,
            bloom_false_positive: options.bloom_false_positive,
            checksum_algo: options.checksum_algo,
        })
    }

    #[cfg(feature = "blockv2")]
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
    ) -> HummockResult<Option<Vec<u8>>> {
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
            true => iter.value().into_put_value().map(|x| x.to_vec()),
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

    pub async fn wait_epoch(&self, epoch: HummockEpoch) -> Result<()> {
        Ok(self.local_version_manager.wait_epoch(epoch).await?)
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
