//! Hummock is the state store of the streaming system.

use std::ops::Bound::{Excluded, Included, Unbounded};
use std::ops::RangeBounds;
use std::sync::Arc;

use itertools::Itertools;

mod sstable;
pub use sstable::*;
mod cloud;
pub mod compactor;
mod error;
pub mod hummock_meta_client;
mod iterator;
pub mod key;
pub mod key_range;
pub mod local_version_manager;
#[cfg(test)]
pub(crate) mod mock;
#[cfg(test)]
mod snapshot_tests;
mod state_store;
#[cfg(test)]
mod state_store_tests;
mod utils;
pub mod value;
mod version_cmp;

pub use error::*;
use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use risingwave_pb::hummock::{KeyRange, LevelType, SstableInfo};
use value::*;

use self::iterator::{
    BoxedHummockIterator, ConcatIterator, HummockIterator, MergeIterator, ReverseMergeIterator,
    UserIterator,
};
use self::key::{key_with_epoch, user_key};
use self::multi_builder::CapacitySplitTableBuilder;
pub use self::state_store::*;
use self::utils::bloom_filter_sstables;
use super::monitor::{StateStoreStats, DEFAULT_STATE_STORE_STATS};
use crate::hummock::hummock_meta_client::HummockMetaClient;
use crate::hummock::iterator::ReverseUserIterator;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::object::ObjectStore;

pub type HummockTTL = u64;
pub type HummockSSTableId = u64;
pub type HummockRefCount = u64;
pub type HummockVersionId = u64;
pub type HummockContextId = u32;
pub type HummockEpoch = u64;
pub const INVALID_EPOCH: HummockEpoch = 0;
pub const INVALID_VERSION_ID: HummockVersionId = 0;
pub const FIRST_VERSION_ID: HummockVersionId = 1;

#[derive(Default, Debug, Clone)]
pub struct HummockOptions {
    /// target size of the SSTable
    pub sstable_size: u32,
    /// size of each block in bytes in SST
    pub block_size: u32,
    /// false positive probability of Bloom filter
    pub bloom_false_positive: f64,
    /// remote directory for storing data and metadata objects
    pub remote_dir: String,
    /// checksum algorithm
    pub checksum_algo: ChecksumAlg,
}

impl HummockOptions {
    #[cfg(test)]
    pub fn default_for_test() -> Self {
        Self {
            sstable_size: 256 * (1 << 20),
            block_size: 64 * (1 << 10),
            bloom_false_positive: 0.1,
            remote_dir: "hummock_001".to_string(),
            checksum_algo: ChecksumAlg::XxHash64,
        }
    }

    #[cfg(test)]
    pub fn small_for_test() -> Self {
        Self {
            sstable_size: 4 * (1 << 10),
            block_size: 1 << 10,
            bloom_false_positive: 0.1,
            remote_dir: "hummock_001_small".to_string(),
            checksum_algo: ChecksumAlg::XxHash64,
        }
    }
}

/// Hummock is the state store backend.
#[derive(Clone)]
pub struct HummockStorage {
    options: Arc<HummockOptions>,

    local_version_manager: Arc<LocalVersionManager>,

    obj_client: Arc<dyn ObjectStore>,

    /// Statistics.
    stats: Arc<StateStoreStats>,

    hummock_meta_client: Arc<dyn HummockMetaClient>,
}

impl HummockStorage {
    pub async fn new(
        obj_client: Arc<dyn ObjectStore>,
        options: HummockOptions,
        local_version_manager: Arc<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> HummockResult<HummockStorage> {
        let stats = DEFAULT_STATE_STORE_STATS.clone();

        LocalVersionManager::start_workers(
            local_version_manager.clone(),
            hummock_meta_client.clone(),
        )
        .await;
        // Ensure at least one available version in cache.
        local_version_manager.wait_epoch(HummockEpoch::MIN).await;

        let instance = Self {
            options: Arc::new(options),
            local_version_manager,
            obj_client,
            stats,
            hummock_meta_client,
        };
        Ok(instance)
    }

    pub fn get_options(&self) -> Arc<HummockOptions> {
        self.options.clone()
    }

    /// Get the value of a specified `key`.
    /// The result is based on a snapshot corresponding to the given `epoch`.
    ///
    /// If `Ok(Some())` is returned, the key is found. If `Ok(None)` is returned,
    /// the key is not found. If `Err()` is returned, the searching for the key
    /// failed due to other non-EOF errors.
    pub async fn get(&self, key: &[u8], epoch: u64) -> HummockResult<Option<Vec<u8>>> {
        self.stats.get_counts.inc();
        self.stats.get_key_size.observe(key.len() as f64);
        let timer = self.stats.get_latency.start_timer();

        let mut table_iters: Vec<BoxedHummockIterator> = Vec::new();

        let version = self.local_version_manager.get_version()?;

        for level in &version.levels() {
            match level.level_type() {
                LevelType::Overlapping => {
                    let tables = bloom_filter_sstables(
                        self.local_version_manager
                            .pick_few_tables(&level.table_ids)
                            .await?,
                        key,
                    )?;
                    table_iters.extend(
                        tables.into_iter().map(|table| {
                            Box::new(SSTableIterator::new(table)) as BoxedHummockIterator
                        }),
                    )
                }
                LevelType::Nonoverlapping => {
                    let tables = bloom_filter_sstables(
                        self.local_version_manager
                            .pick_few_tables(&level.table_ids)
                            .await?,
                        key,
                    )?;
                    table_iters.push(Box::new(ConcatIterator::new(tables)))
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
        timer.observe_duration();
        self.stats
            .get_value_size
            .observe((value.as_ref().map(|x| x.len()).unwrap_or(0) + 1) as f64);

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
        self.stats.range_scan_counts.inc();

        let version = self.local_version_manager.get_version()?;

        // Filter out tables that overlap with given `key_range`
        let overlapped_tables = self
            .local_version_manager
            .tables(&version.levels())
            .await?
            .into_iter()
            .filter(|t| {
                let table_start = user_key(t.meta.smallest_key.as_slice());
                let table_end = user_key(t.meta.largest_key.as_slice());

                //        RANGE
                // TABLE
                let too_left = match key_range.start_bound() {
                    Included(range_start) => range_start.as_ref() > table_end,
                    Excluded(_) => unimplemented!("excluded begin key is not supported"),
                    Unbounded => false,
                };
                // RANGE
                //        TABLE
                let too_right = match key_range.end_bound() {
                    Included(range_end) => range_end.as_ref() < table_start,
                    Excluded(range_end) => range_end.as_ref() <= table_start,
                    Unbounded => false,
                };

                !too_left && !too_right
            });

        let table_iters =
            overlapped_tables.map(|t| Box::new(SSTableIterator::new(t)) as BoxedHummockIterator);
        let mi = MergeIterator::new(table_iters);

        // TODO: avoid this clone
        Ok(UserIterator::new_with_epoch(
            mi,
            (
                key_range.start_bound().map(|b| b.as_ref().to_owned()),
                key_range.end_bound().map(|b| b.as_ref().to_owned()),
            ),
            epoch,
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
        self.stats.range_scan_counts.inc();

        let version = self.local_version_manager.get_version()?;

        // Filter out tables that overlap with given `key_range`
        let overlapped_tables = self
            .local_version_manager
            .tables(&version.levels())
            .await?
            .into_iter()
            .filter(|t| {
                let table_start = user_key(t.meta.smallest_key.as_slice());
                let table_end = user_key(t.meta.largest_key.as_slice());

                //        RANGE
                // TABLE
                let too_left = match key_range.end_bound() {
                    Included(range_start) => range_start.as_ref() > table_end,
                    Excluded(range_start) => range_start.as_ref() >= table_end,
                    Unbounded => false,
                };
                // RANGE
                //        TABLE
                let too_right = match key_range.start_bound() {
                    Included(range_end) => range_end.as_ref() < table_start,
                    Excluded(_) => unimplemented!("excluded end key is not supported"),
                    Unbounded => false,
                };

                !too_left && !too_right
            });

        let reverse_table_iters = overlapped_tables
            .map(|t| Box::new(ReverseSSTableIterator::new(t)) as BoxedHummockIterator);
        let reverse_merge_iterator = ReverseMergeIterator::new(reverse_table_iters);

        // TODO: avoid this clone
        Ok(ReverseUserIterator::new_with_epoch(
            reverse_merge_iterator,
            (
                key_range.end_bound().map(|b| b.as_ref().to_owned()),
                key_range.start_bound().map(|b| b.as_ref().to_owned()),
            ),
            epoch,
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
        kv_pairs: impl Iterator<Item = (Vec<u8>, HummockValue<Vec<u8>>)>,
        epoch: u64,
    ) -> HummockResult<()> {
        let get_id_and_builder = || async {
            let id = self.hummock_meta_client().get_new_table_id().await?;
            let timer = self.stats.batch_write_build_table_latency.start_timer();
            let builder = Self::get_builder(&self.options);
            timer.observe_duration();
            Ok((id, builder))
        };
        let mut builder = CapacitySplitTableBuilder::new(get_id_and_builder);

        // TODO: do not generate epoch if `kv_pairs` is empty
        for (k, v) in kv_pairs {
            builder.add_user_key(k, v.as_slice(), epoch).await?;
        }

        let tables = {
            let mut tables = Vec::with_capacity(builder.len());

            // TODO: decide upload concurrency
            for (table_id, blocks, meta) in builder.finish() {
                cloud::upload(
                    &self.obj_client,
                    table_id,
                    &meta,
                    blocks,
                    self.options.remote_dir.as_str(),
                )
                .await?;

                tables.push(SSTable {
                    id: table_id,
                    meta,
                    obj_client: self.obj_client.clone(),
                    data_path: cloud::get_sst_data_path(self.options.remote_dir.as_str(), table_id),
                    block_cache: self.local_version_manager.block_cache.clone(),
                });
            }

            tables
        };

        if tables.is_empty() {
            return Ok(());
        }

        // Add all tables at once.
        let timer = self.stats.batch_write_add_l0_latency.start_timer();
        let version = self
            .hummock_meta_client()
            .add_tables(
                epoch,
                tables
                    .iter()
                    .map(|table| SstableInfo {
                        id: table.id,
                        key_range: Some(KeyRange {
                            left: table.meta.smallest_key.clone(),
                            right: table.meta.largest_key.clone(),
                            inf: false,
                        }),
                    })
                    .collect_vec(),
            )
            .await?;
        timer.observe_duration();

        // Ensure the added data is available locally
        self.local_version_manager.try_set_version(version);

        Ok(())
    }

    fn get_builder(options: &HummockOptions) -> SSTableBuilder {
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

    pub fn options(&self) -> &Arc<HummockOptions> {
        &self.options
    }
    pub fn local_version_manager(&self) -> &Arc<LocalVersionManager> {
        &self.local_version_manager
    }
    pub fn obj_client(&self) -> &Arc<dyn ObjectStore> {
        &self.obj_client
    }

    pub async fn wait_epoch(&self, epoch: HummockEpoch) {
        self.local_version_manager.wait_epoch(epoch).await;
    }
}
