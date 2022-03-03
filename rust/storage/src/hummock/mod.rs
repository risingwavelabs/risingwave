//! Hummock is the state store of the streaming system.

use std::fmt;
use std::ops::RangeBounds;
use std::sync::Arc;

use itertools::Itertools;

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
mod sstable_manager;
mod state_store;
#[cfg(test)]
mod state_store_tests;
mod utils;
pub mod value;
mod version_cmp;

use compactor::{Compactor, SubCompactContext};
pub use error::*;
use parking_lot::Mutex as PLMutex;
use risingwave_pb::hummock::checksum::Algorithm as ChecksumAlg;
use risingwave_pb::hummock::LevelType;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use value::*;

use self::iterator::{
    BoxedHummockIterator, ConcatIterator, HummockIterator, MergeIterator, ReverseMergeIterator,
    UserIterator,
};
use self::key::{key_with_epoch, user_key, FullKey};
use self::shared_buffer::SharedBufferManager;
pub use self::sstable_manager::*;
pub use self::state_store::*;
use self::utils::{bloom_filter_sstables, range_overlap};
use super::monitor::{StateStoreStats, DEFAULT_STATE_STORE_STATS};
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

    /// Receiver of the compactor.
    #[allow(dead_code)]
    rx: Arc<PLMutex<Option<mpsc::UnboundedReceiver<()>>>>,

    stop_compact_tx: mpsc::UnboundedSender<()>,

    compactor_joinhandle: Arc<PLMutex<Option<JoinHandle<HummockResult<()>>>>>,

    /// Statistics.
    stats: Arc<StateStoreStats>,

    hummock_meta_client: Arc<dyn HummockMetaClient>,

    sstable_manager: SstableManagerRef,
    /// Manager for immutable shared buffers
    shared_buffer_manager: Arc<SharedBufferManager>,
}

impl HummockStorage {
    pub async fn new(
        options: HummockOptions,
        sstable_manager: SstableManagerRef,
        local_version_manager: Arc<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> HummockResult<HummockStorage> {
        let (trigger_compact_tx, trigger_compact_rx) = mpsc::unbounded_channel();
        let (stop_compact_tx, stop_compact_rx) = mpsc::unbounded_channel();

        let stats = DEFAULT_STATE_STORE_STATS.clone();

        let arc_options = Arc::new(options);
        let options_for_compact = arc_options.clone();
        let local_version_manager_for_compact = local_version_manager.clone();
        let hummock_meta_client_for_compact = hummock_meta_client.clone();
        let rx = Arc::new(PLMutex::new(Some(trigger_compact_rx)));
        let rx_for_compact = rx.clone();

        let shared_buffer_manager = Arc::new(SharedBufferManager::new(
            arc_options.clone(),
            local_version_manager.clone(),
            sstable_manager.clone(),
            trigger_compact_tx.clone(),
            stats.clone(),
            hummock_meta_client.clone(),
        ));

        LocalVersionManager::start_workers(
            local_version_manager.clone(),
            hummock_meta_client.clone(),
            shared_buffer_manager.clone(),
        )
        .await;
        // Ensure at least one available version in cache.
        local_version_manager.wait_epoch(HummockEpoch::MIN).await;

        let sub_compact_context = SubCompactContext {
            options: options_for_compact,
            local_version_manager: local_version_manager_for_compact,
            hummock_meta_client: hummock_meta_client_for_compact,
            sstable_manager: sstable_manager.clone(),
        };

        let instance = Self {
            options: arc_options,
            local_version_manager,
            rx,
            stop_compact_tx,
            compactor_joinhandle: Arc::new(PLMutex::new(Some(tokio::spawn(async move {
                Self::start_compactor(sub_compact_context, rx_for_compact, stop_compact_rx).await
            })))),
            stats,
            hummock_meta_client,
            shared_buffer_manager,
            sstable_manager,
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

        // Query shared buffer. Return the value without iterating SSTs if found
        if let Some(v) = self
            .shared_buffer_manager
            .get(key, (version.max_committed_epoch() + 1)..=epoch)
        {
            return Ok(v.into_put_value());
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
                        Box::new(SSTableIterator::new(table, self.sstable_manager.clone()))
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
                        self.sstable_manager.clone(),
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
                Box::new(SSTableIterator::new(t, self.sstable_manager.clone()))
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
                Box::new(ReverseSSTableIterator::new(t, self.sstable_manager.clone()))
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
        let batch = kv_pairs
            .map(|i| (FullKey::from_user_key(i.0, epoch).into_inner(), i.1))
            .collect_vec();
        self.shared_buffer_manager.write_batch(batch, epoch)?;

        // self.sync(epoch).await?;
        Ok(())
    }

    pub async fn sync(&self, epoch: Option<u64>) -> HummockResult<()> {
        self.shared_buffer_manager.sync(epoch).await
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

    pub async fn start_compactor(
        context: SubCompactContext,
        compact_signal: Arc<PLMutex<Option<mpsc::UnboundedReceiver<()>>>>,
        mut stop: mpsc::UnboundedReceiver<()>,
    ) -> HummockResult<()> {
        let mut compact_notifier = compact_signal.lock().take().unwrap();
        loop {
            select! {
                Some(_) = compact_notifier.recv() => Compactor::compact(&context).await?,
                Some(_) = stop.recv() => break
            }
        }
        Ok(())
    }

    pub async fn shutdown_compactor(&mut self) -> HummockResult<()> {
        self.stop_compact_tx.send(()).ok();
        self.compactor_joinhandle
            .lock()
            .take()
            .unwrap()
            .await
            .unwrap()
    }

    pub fn sstable_manager(&self) -> SstableManagerRef {
        self.sstable_manager.clone()
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
