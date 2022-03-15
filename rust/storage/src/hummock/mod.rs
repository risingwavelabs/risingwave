//! Hummock is the state store of the streaming system.

use std::fmt;
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;

mod block_cache;
pub use block_cache::*;
mod sstable;
pub use sstable::*;
pub mod compactor;
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
    BoxedHummockIterator, ConcatIterator, HummockIterator, MergeIterator, ReverseMergeIterator,
    UserIterator,
};
use self::key::{key_with_epoch, user_key, FullKey};
use self::shared_buffer::SharedBufferManager;
pub use self::sstable_store::*;
pub use self::state_store::*;
use self::utils::{bloom_filter_sstables, range_overlap};
use super::monitor::StateStoreMetrics;
use crate::hummock::hummock_meta_client::HummockMetaClient;
use crate::hummock::iterator::ReverseUserIterator;
use crate::hummock::local_version_manager::LocalVersionManager;

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
}

impl HummockStorage {
    /// Create a [`HummockStorage`] with default stats. Should only used by tests.
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

    /// Create a [`HummockStorage`].
    pub async fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        local_version_manager: Arc<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        // TODO: should be separated `HummockStats` instead of `StateStoreMetrics`.
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
        local_version_manager.wait_epoch(HummockEpoch::MIN).await;

        let instance = Self {
            options,
            local_version_manager,
            hummock_meta_client,
            sstable_store,
            shared_buffer_manager,
        };
        Ok(instance)
    }

    /// Get the value of a specified `key`.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    pub async fn get(&self, key: &[u8], epoch: u64) -> HummockResult<Option<Vec<u8>>> {
        let mut table_iters: Vec<BoxedHummockIterator> = Vec::new();

        let version = self.local_version_manager.get_version()?;

        // Query shared buffer. Return the value without iterating SSTs if found
        if let Some(v) = self
            .shared_buffer_manager
            .get(key, (version.max_committed_epoch() + 1)..=epoch)
        {
            return Ok(v.into_put_value().map(|x| x.to_vec()));
        }

        for level in &version.levels() {
            match level.level_type() {
                LevelType::Overlapping => {
                    let tables = bloom_filter_sstables(
                        self.local_version_manager
                            .pick_few_tables(&level.table_ids)
                            .await?,
                        key,
                    )?;
                    table_iters.extend(tables.into_iter().map(|table| {
                        Box::new(SSTableIterator::new(table, self.sstable_store.clone()))
                            as BoxedHummockIterator
                    }))
                }
                LevelType::Nonoverlapping => {
                    let tables = bloom_filter_sstables(
                        self.local_version_manager
                            .pick_few_tables(&level.table_ids)
                            .await?,
                        key,
                    )?;
                    table_iters.push(Box::new(ConcatIterator::new(
                        tables,
                        self.sstable_store.clone(),
                    )))
                }
            }
        }

        let mut it = MergeIterator::new(table_iters);

        // Use `MergeIterator` to seek for the key with latest version to
        // get the latest key.
        it.seek(&key_with_epoch(key.to_vec(), epoch)).await?;

        // Iterator has seeked passed the borders.
        if !it.is_valid() {
            return Ok(None);
        }

        // Iterator gets us the key, we tell if it's the key we want
        // or key next to it.
        let value = match user_key(it.key()) == key {
            true => it.value().into_put_value().map(|x| x.to_vec()),
            false => None,
        };

        Ok(value)
    }

    /// Return an iterator that scan from the begin key to the end key
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

        // Filter out tables that overlap with given `key_range`
        let overlapped_sstable_iters = self
            .local_version_manager
            .tables(&version.levels())
            .await?
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
            MergeIterator::new(overlapped_shared_buffer_iters.chain(overlapped_sstable_iters))
        } else {
            MergeIterator::new(overlapped_sstable_iters)
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

    /// Return a reversed iterator that scans from the end key to the begin key
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
            )
        } else {
            ReverseMergeIterator::new(overlapped_sstable_iters)
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

    /// Write batch to storage. The batch should be:
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
            .map(|i| {
                (
                    Bytes::from(FullKey::from_user_key(i.0.to_vec(), epoch).into_inner()),
                    i.1,
                )
            })
            .collect_vec();
        self.shared_buffer_manager.write_batch(batch, epoch)?;

        if !self.options.async_checkpoint_enabled {
            return self.shared_buffer_manager.sync(Some(epoch)).await;
        }
        Ok(())
    }

    /// Replicate batch to shared buffer, without uploading to the storage backend.
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

    fn get_builder(options: &StorageConfig) -> SSTableBuilder {
        // TODO: use different option values (especially table_size) for compaction
        SSTableBuilder::new(SSTableBuilderOptions {
            table_capacity: options.sstable_size,
            block_size: options.block_size,
            bloom_false_positive: options.bloom_false_positive,
            checksum_algo: options.checksum_algo,
        })
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

    pub async fn wait_epoch(&self, epoch: HummockEpoch) {
        self.local_version_manager.wait_epoch(epoch).await;
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
