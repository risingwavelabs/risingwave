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

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::poll_fn;
use std::ops::{Bound, Range};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, LazyLock};
use std::task::Poll;
use std::time::{Duration, Instant};

use foyer::{HybridCacheEntry, RangeBoundsExt};
use futures::future::{join_all, try_join_all};
use futures::{Future, FutureExt};
use itertools::Itertools;
use parking_lot::RwLock;
use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{
    Histogram, HistogramVec, IntGauge, Registry, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_counter_with_registry,
    register_int_gauge_with_registry,
};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::license::Feature;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::key::{FullKey, vnode_range};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::{HummockSstableObjectId, KeyComparator};
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use super::ReadVersionMappingType;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::{
    Block, HummockError, HummockResult, RecentFilterTrait, Sstable, SstableBlockIndex,
    SstableStoreRef, TableHolder,
};
use crate::monitor::StoreLocalStatistic;
use crate::opts::StorageOpts;

pub static GLOBAL_CACHE_REFILL_METRICS: LazyLock<CacheRefillMetrics> =
    LazyLock::new(|| CacheRefillMetrics::new(&GLOBAL_METRICS_REGISTRY));

pub struct CacheRefillMetrics {
    pub refill_duration: HistogramVec,
    pub refill_total: GenericCounterVec<AtomicU64>,
    pub refill_bytes: GenericCounterVec<AtomicU64>,

    pub data_refill_success_duration: Histogram,
    pub meta_refill_success_duration: Histogram,

    pub data_refill_filtered_total: GenericCounter<AtomicU64>,
    pub data_refill_attempts_total: GenericCounter<AtomicU64>,
    pub data_refill_started_total: GenericCounter<AtomicU64>,
    pub meta_refill_attempts_total: GenericCounter<AtomicU64>,

    pub data_refill_parent_meta_lookup_hit_total: GenericCounter<AtomicU64>,
    pub data_refill_parent_meta_lookup_miss_total: GenericCounter<AtomicU64>,
    pub data_refill_unit_inheritance_hit_total: GenericCounter<AtomicU64>,
    pub data_refill_unit_inheritance_miss_total: GenericCounter<AtomicU64>,

    pub data_refill_block_unfiltered_total: GenericCounter<AtomicU64>,
    pub data_refill_block_success_total: GenericCounter<AtomicU64>,

    pub data_refill_ideal_bytes: GenericCounter<AtomicU64>,
    pub data_refill_success_bytes: GenericCounter<AtomicU64>,

    pub data_refill_locality_filtered_total: GenericCounter<AtomicU64>,
    pub meta_refill_locality_skipped_total: GenericCounter<AtomicU64>,
    pub meta_refill_locality_saved_remote_bytes_total: GenericCounter<AtomicU64>,

    pub refill_queue_total: IntGauge,
}

impl CacheRefillMetrics {
    pub fn new(registry: &Registry) -> Self {
        let refill_duration = register_histogram_vec_with_registry!(
            "refill_duration",
            "refill duration",
            &["type", "op"],
            registry,
        )
        .unwrap();
        let refill_total = register_int_counter_vec_with_registry!(
            "refill_total",
            "refill total",
            &["type", "op"],
            registry,
        )
        .unwrap();
        let refill_bytes = register_int_counter_vec_with_registry!(
            "refill_bytes",
            "refill bytes",
            &["type", "op"],
            registry,
        )
        .unwrap();

        let data_refill_success_duration = refill_duration
            .get_metric_with_label_values(&["data", "success"])
            .unwrap();
        let meta_refill_success_duration = refill_duration
            .get_metric_with_label_values(&["meta", "success"])
            .unwrap();

        let data_refill_filtered_total = refill_total
            .get_metric_with_label_values(&["data", "filtered"])
            .unwrap();
        let data_refill_attempts_total = refill_total
            .get_metric_with_label_values(&["data", "attempts"])
            .unwrap();
        let data_refill_started_total = refill_total
            .get_metric_with_label_values(&["data", "started"])
            .unwrap();
        let meta_refill_attempts_total = refill_total
            .get_metric_with_label_values(&["meta", "attempts"])
            .unwrap();

        let data_refill_parent_meta_lookup_hit_total = refill_total
            .get_metric_with_label_values(&["parent_meta", "hit"])
            .unwrap();
        let data_refill_parent_meta_lookup_miss_total = refill_total
            .get_metric_with_label_values(&["parent_meta", "miss"])
            .unwrap();
        let data_refill_unit_inheritance_hit_total = refill_total
            .get_metric_with_label_values(&["unit_inheritance", "hit"])
            .unwrap();
        let data_refill_unit_inheritance_miss_total = refill_total
            .get_metric_with_label_values(&["unit_inheritance", "miss"])
            .unwrap();

        let data_refill_block_unfiltered_total = refill_total
            .get_metric_with_label_values(&["block", "unfiltered"])
            .unwrap();
        let data_refill_block_success_total = refill_total
            .get_metric_with_label_values(&["block", "success"])
            .unwrap();

        let data_refill_ideal_bytes = refill_bytes
            .get_metric_with_label_values(&["data", "ideal"])
            .unwrap();
        let data_refill_success_bytes = refill_bytes
            .get_metric_with_label_values(&["data", "success"])
            .unwrap();
        let data_refill_locality_filtered_total = register_int_counter_with_registry!(
            "data_refill_locality_filtered_total",
            "Total number of blocks filtered by streaming-only data refill locality gating.",
            registry,
        )
        .unwrap();
        let meta_refill_locality_skipped_total = register_int_counter_with_registry!(
            "meta_refill_locality_skipped_total",
            "Total number of SSTs skipped by streaming-only meta refill locality gating.",
            registry,
        )
        .unwrap();
        let meta_refill_locality_saved_remote_bytes_total = register_int_counter_with_registry!(
            "meta_refill_locality_saved_remote_bytes_total",
            "Total estimated remote bytes saved by streaming-only meta refill locality gating.",
            registry,
        )
        .unwrap();

        let refill_queue_total = register_int_gauge_with_registry!(
            "refill_queue_total",
            "refill queue total",
            registry,
        )
        .unwrap();

        Self {
            refill_duration,
            refill_total,
            refill_bytes,

            data_refill_success_duration,
            meta_refill_success_duration,
            data_refill_filtered_total,
            data_refill_attempts_total,
            data_refill_started_total,
            meta_refill_attempts_total,

            data_refill_parent_meta_lookup_hit_total,
            data_refill_parent_meta_lookup_miss_total,
            data_refill_unit_inheritance_hit_total,
            data_refill_unit_inheritance_miss_total,

            data_refill_block_unfiltered_total,
            data_refill_block_success_total,

            data_refill_ideal_bytes,
            data_refill_success_bytes,
            data_refill_locality_filtered_total,
            meta_refill_locality_skipped_total,
            meta_refill_locality_saved_remote_bytes_total,

            refill_queue_total,
        }
    }
}

#[derive(Debug)]
pub struct CacheRefillConfig {
    /// Cache refill timeout.
    pub timeout: Duration,

    /// Data file cache refill levels.
    pub data_refill_levels: HashSet<u32>,

    /// Meta file cache refill concurrency.
    pub meta_refill_concurrency: usize,

    /// Data file cache refill concurrency.
    pub concurrency: usize,

    /// Data file cache refill unit (blocks).
    pub unit: usize,

    /// Data file cache reill unit threshold.
    ///
    /// Only units whose admit rate > threshold will be refilled.
    pub threshold: f64,

    /// Skip recent filter.
    pub skip_recent_filter: bool,
}

impl CacheRefillConfig {
    pub fn from_storage_opts(options: &StorageOpts) -> Self {
        let data_refill_levels = match Feature::ElasticDiskCache.check_available() {
            Ok(_) => options
                .cache_refill_data_refill_levels
                .iter()
                .copied()
                .collect(),
            Err(e) => {
                tracing::warn!(error = %e.as_report(), "ElasticDiskCache is not available.");
                HashSet::new()
            }
        };

        Self {
            timeout: Duration::from_millis(options.cache_refill_timeout_ms),
            data_refill_levels,
            concurrency: options.cache_refill_concurrency,
            meta_refill_concurrency: options.cache_refill_meta_refill_concurrency,
            unit: options.cache_refill_unit,
            threshold: options.cache_refill_threshold,
            skip_recent_filter: options.cache_refill_skip_recent_filter,
        }
    }
}

struct Item {
    handle: JoinHandle<()>,
    event: CacheRefillerEvent,
}

pub(crate) type SpawnRefillTask = Arc<
    // first current version, second new version
    dyn Fn(Vec<SstDeltaInfo>, CacheRefillContext, PinnedVersion, PinnedVersion) -> JoinHandle<()>
        + Send
        + Sync
        + 'static,
>;

pub struct TableCacheRefillContext {
    pub read_version_mapping: Arc<RwLock<ReadVersionMappingType>>,
}

#[derive(Default)]
struct StreamingTableCacheRefillView {
    streaming: HashMap<TableId, Bitmap>,
}

impl TableCacheRefillContext {
    fn build_streaming_view(&self) -> StreamingTableCacheRefillView {
        let read_version_mapping = self.read_version_mapping.read();
        let mut streaming = HashMap::new();
        for (table_id, mapping) in &*read_version_mapping {
            for read_version in mapping.values() {
                let vnodes = read_version.read().vnodes();
                streaming
                    .entry(*table_id)
                    .and_modify(|bitmap: &mut Bitmap| *bitmap |= vnodes.as_ref())
                    .or_insert_with(|| vnodes.as_ref().clone());
            }
        }
        StreamingTableCacheRefillView { streaming }
    }
}

impl StreamingTableCacheRefillView {
    fn contains_table(&self, table_id: &TableId) -> bool {
        self.streaming.contains_key(table_id)
    }

    fn check_table_refill_vnodes(&self, sstable: &Sstable, block_index: usize) -> bool {
        let block_smallest_key =
            FullKey::decode(&sstable.meta.block_metas[block_index].smallest_key)
                .to_vec()
                .into_bytes();
        let table_id = block_smallest_key.user_key.table_id;
        let Some(streaming_bitmap) = self.streaming.get(&table_id) else {
            return false;
        };

        let block_largest_key =
            if let Some(next_block_meta) = sstable.meta.block_metas.get(block_index + 1) {
                if next_block_meta.table_id() != table_id {
                    // The next block belongs to another table, so we no longer have a safe upper
                    // bound for vnode_range. Admit to avoid false negative filtering.
                    return true;
                }
                FullKey::decode(&next_block_meta.smallest_key)
                    .to_vec()
                    .into_bytes()
            } else {
                let largest_key = FullKey::decode(&sstable.meta.largest_key)
                    .to_vec()
                    .into_bytes();
                if largest_key.user_key.table_id != table_id {
                    // Multi-table SSTs can end with another table's largest key. Admit when the local
                    // table is present to avoid false negative filtering on the boundary block.
                    return true;
                }
                largest_key
            };

        let table_key_range = (
            Bound::Included(block_smallest_key.user_key.table_key),
            Bound::Excluded(block_largest_key.user_key.table_key),
        );
        let vnode_range = vnode_range(&table_key_range);
        let bitmap = Bitmap::from_range(streaming_bitmap.len(), vnode_range.0..vnode_range.1);
        (&bitmap & streaming_bitmap).any()
    }
}

/// A cache refiller for hummock data.
pub(crate) struct CacheRefiller {
    /// order: old => new
    queue: VecDeque<Item>,

    context: CacheRefillContext,

    spawn_refill_task: SpawnRefillTask,
}

impl CacheRefiller {
    pub(crate) fn new(
        config: CacheRefillConfig,
        sstable_store: SstableStoreRef,
        spawn_refill_task: SpawnRefillTask,
        read_version_mapping: Arc<RwLock<ReadVersionMappingType>>,
    ) -> Self {
        let config = Arc::new(config);
        let concurrency = Arc::new(Semaphore::new(config.concurrency));
        let meta_refill_concurrency = if config.meta_refill_concurrency == 0 {
            None
        } else {
            Some(Arc::new(Semaphore::new(config.meta_refill_concurrency)))
        };
        Self {
            queue: VecDeque::new(),
            context: CacheRefillContext {
                config,
                meta_refill_concurrency,
                concurrency,
                sstable_store,
                table_cache_refill_context: Arc::new(RwLock::new(TableCacheRefillContext {
                    read_version_mapping,
                })),
            },
            spawn_refill_task,
        }
    }

    pub(crate) fn default_spawn_refill_task() -> SpawnRefillTask {
        Arc::new(|deltas, context, _, _| {
            let task = CacheRefillTask { deltas, context };
            tokio::spawn(task.run())
        })
    }

    pub(crate) fn start_cache_refill(
        &mut self,
        deltas: Vec<SstDeltaInfo>,
        pinned_version: PinnedVersion,
        new_pinned_version: PinnedVersion,
    ) {
        let handle = (self.spawn_refill_task)(
            deltas,
            self.context.clone(),
            pinned_version.clone(),
            new_pinned_version.clone(),
        );
        let event = CacheRefillerEvent {
            pinned_version,
            new_pinned_version,
        };
        let item = Item { handle, event };
        self.queue.push_back(item);
        GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.add(1);
    }

    pub(crate) fn last_new_pinned_version(&self) -> Option<&PinnedVersion> {
        self.queue.back().map(|item| &item.event.new_pinned_version)
    }
}

impl CacheRefiller {
    pub(crate) fn next_events(&mut self) -> impl Future<Output = Vec<CacheRefillerEvent>> + '_ {
        poll_fn(|cx| {
            const MAX_BATCH_SIZE: usize = 16;
            let mut events = None;
            while let Some(item) = self.queue.front_mut()
                && let Poll::Ready(result) = item.handle.poll_unpin(cx)
            {
                result.unwrap();
                let item = self.queue.pop_front().unwrap();
                GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.sub(1);
                let events = events.get_or_insert_with(|| Vec::with_capacity(MAX_BATCH_SIZE));
                events.push(item.event);
                if events.len() >= MAX_BATCH_SIZE {
                    break;
                }
            }
            if let Some(events) = events {
                Poll::Ready(events)
            } else {
                Poll::Pending
            }
        })
    }
}

pub struct CacheRefillerEvent {
    pub pinned_version: PinnedVersion,
    pub new_pinned_version: PinnedVersion,
}

#[derive(Clone)]
pub(crate) struct CacheRefillContext {
    config: Arc<CacheRefillConfig>,
    meta_refill_concurrency: Option<Arc<Semaphore>>,
    concurrency: Arc<Semaphore>,
    sstable_store: SstableStoreRef,
    table_cache_refill_context: Arc<RwLock<TableCacheRefillContext>>,
}

struct CacheRefillTask {
    deltas: Vec<SstDeltaInfo>,
    context: CacheRefillContext,
}

impl CacheRefillTask {
    fn with_fail_open_locality_gate(gate_name: &'static str, check: impl FnOnce() -> bool) -> bool {
        match catch_unwind(AssertUnwindSafe(check)) {
            Ok(admit) => admit,
            Err(_) => {
                tracing::warn!(
                    gate = gate_name,
                    "cache refill locality gate panicked, admit"
                );
                true
            }
        }
    }

    fn should_admit_meta_refill(
        context: &StreamingTableCacheRefillView,
        info: &SstableInfo,
    ) -> bool {
        info.table_ids
            .iter()
            .any(|table_id| context.contains_table(table_id))
    }

    fn filter_by_streaming_vnodes(
        streaming_view: &StreamingTableCacheRefillView,
        tasks: Vec<DataCacheRefillTask>,
    ) -> Vec<DataCacheRefillTask> {
        let mut filtered_blocks = 0;
        let mut retained = Vec::with_capacity(tasks.len());

        for task in tasks {
            let admit = Self::with_fail_open_locality_gate("data_refill", || {
                (task.blks.start..task.blks.end)
                    .any(|blk| streaming_view.check_table_refill_vnodes(&task.sst, blk))
            });

            if admit {
                retained.push(task);
            } else {
                filtered_blocks += task.blks.end - task.blks.start;
            }
        }

        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_locality_filtered_total
            .inc_by(filtered_blocks as u64);
        retained
    }

    async fn run(self) {
        let tasks = self
            .deltas
            .iter()
            .map(|delta| {
                let context = self.context.clone();
                async move {
                    let holders = match Self::meta_cache_refill(&context, delta).await {
                        Ok(holders) => holders,
                        Err(e) => {
                            tracing::warn!(error = %e.as_report(), "meta cache refill error");
                            return;
                        }
                    };
                    Self::data_cache_refill(&context, delta, holders).await;
                }
            })
            .collect_vec();
        let future = join_all(tasks);

        let _ = tokio::time::timeout(self.context.config.timeout, future).await;
    }

    async fn meta_cache_refill(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
    ) -> HummockResult<Vec<TableHolder>> {
        let streaming_view = context
            .table_cache_refill_context
            .read()
            .build_streaming_view();
        let tasks = delta
            .insert_sst_infos
            .iter()
            .filter(|info| {
                let admit = Self::with_fail_open_locality_gate("meta_refill", || {
                    Self::should_admit_meta_refill(&streaming_view, info)
                });
                if !admit {
                    GLOBAL_CACHE_REFILL_METRICS
                        .meta_refill_locality_skipped_total
                        .inc();
                    GLOBAL_CACHE_REFILL_METRICS
                        .meta_refill_locality_saved_remote_bytes_total
                        .inc_by(info.file_size.saturating_sub(info.meta_offset));
                }
                admit
            })
            .map(|info| async {
                let mut stats = StoreLocalStatistic::default();
                GLOBAL_CACHE_REFILL_METRICS.meta_refill_attempts_total.inc();

                let permit = if let Some(c) = &context.meta_refill_concurrency {
                    Some(c.acquire().await.unwrap())
                } else {
                    None
                };

                let now = Instant::now();
                let res = context.sstable_store.sstable(info, &mut stats).await;
                stats.discard();
                GLOBAL_CACHE_REFILL_METRICS
                    .meta_refill_success_duration
                    .observe(now.elapsed().as_secs_f64());
                drop(permit);

                res
            })
            .collect_vec();
        let holders = try_join_all(tasks).await?;
        Ok(holders)
    }

    /// Get sstable inheritance info in unit level.
    fn get_units_to_refill_by_inheritance(
        context: &CacheRefillContext,
        ssts: &[TableHolder],
        parent_ssts: impl IntoIterator<Item = HybridCacheEntry<HummockSstableObjectId, Box<Sstable>>>,
    ) -> HashSet<SstableUnit> {
        let mut res = HashSet::default();

        let recent_filter = context.sstable_store.recent_filter();

        let units = {
            let unit = context.config.unit;
            ssts.iter()
                .flat_map(|sst| {
                    let units = Unit::units(sst, unit);
                    (0..units).map(|uidx| Unit::new(sst, unit, uidx))
                })
                .collect_vec()
        };

        if cfg!(debug_assertions) {
            // assert units in asc order
            units.iter().tuple_windows().for_each(|(a, b)| {
                debug_assert_ne!(
                    KeyComparator::compare_encoded_full_key(a.largest_key(), b.smallest_key()),
                    std::cmp::Ordering::Greater
                )
            });
        }

        for psst in parent_ssts {
            for pblk in 0..psst.block_count() {
                let pleft = &psst.meta.block_metas[pblk].smallest_key;
                let pright = if pblk + 1 == psst.block_count() {
                    // `largest_key` can be included or excluded, both are treated as included here
                    &psst.meta.largest_key
                } else {
                    &psst.meta.block_metas[pblk + 1].smallest_key
                };

                // partition point: unit.right < pblk.left
                let uleft = units.partition_point(|unit| {
                    KeyComparator::compare_encoded_full_key(unit.largest_key(), pleft)
                        == std::cmp::Ordering::Less
                });
                // partition point: unit.left <= pblk.right
                let uright = units.partition_point(|unit| {
                    KeyComparator::compare_encoded_full_key(unit.smallest_key(), pright)
                        != std::cmp::Ordering::Greater
                });

                // overlapping: uleft..uright
                for u in units.iter().take(uright).skip(uleft) {
                    let unit = SstableUnit {
                        sst_obj_id: u.sst.id,
                        blks: u.blks.clone(),
                    };
                    if res.contains(&unit) {
                        continue;
                    }
                    if context.config.skip_recent_filter || recent_filter.contains(&(psst.id, pblk))
                    {
                        res.insert(unit);
                    }
                }
            }
        }

        let hit = res.len();
        let miss = units.len() - res.len();
        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_unit_inheritance_hit_total
            .inc_by(hit as u64);
        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_unit_inheritance_miss_total
            .inc_by(miss as u64);

        res
    }

    /// Data cache refill entry point.
    async fn data_cache_refill(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
        holders: Vec<TableHolder>,
    ) {
        // Skip data cache refill if data disk cache is not enabled.
        if !context.sstable_store.block_cache().is_hybrid() {
            return;
        }

        // Return if no data to refill.
        if delta.insert_sst_infos.is_empty() || delta.delete_sst_object_ids.is_empty() {
            return;
        }

        // Return if the target level is not in the refill levels
        if !context
            .config
            .data_refill_levels
            .contains(&delta.insert_sst_level)
        {
            return;
        }

        let recent_filter = context.sstable_store.recent_filter();

        // Return if recent filter is required and no deleted sst ids are in the recent filter.
        let targets = delta
            .delete_sst_object_ids
            .iter()
            .map(|id| (*id, usize::MAX))
            .collect_vec();
        if !context.config.skip_recent_filter && !recent_filter.contains_any(targets.iter()) {
            GLOBAL_CACHE_REFILL_METRICS
                .data_refill_filtered_total
                .inc_by(delta.delete_sst_object_ids.len() as _);
            return;
        }

        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_block_unfiltered_total
            .inc_by(
                holders
                    .iter()
                    .map(|sst| sst.block_count() as u64)
                    .sum::<u64>(),
            );

        if delta.insert_sst_level == 0 {
            Self::data_file_cache_refill_full_impl(context, delta, holders).await;
        } else {
            Self::data_file_cache_impl(context, delta, holders).await;
        }
    }

    async fn data_file_cache_refill_full_impl(
        context: &CacheRefillContext,
        _delta: &SstDeltaInfo,
        holders: Vec<TableHolder>,
    ) {
        let unit = context.config.unit;
        let futures = holders.into_iter().flat_map(|sst| {
            (0..sst.block_count()).step_by(unit).map(move |blk_start| {
                let task_sst = sst.clone();
                async move {
                    let unit = SstableUnit {
                        sst_obj_id: task_sst.id,
                        blks: blk_start..std::cmp::min(task_sst.block_count(), blk_start + unit),
                    };
                    Self::data_file_cache_refill_unit(context, &task_sst, unit).await
                }
            })
        });
        join_all(futures).await;
    }

    async fn data_file_cache_impl(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
        holders: Vec<TableHolder>,
    ) {
        let sstable_store = context.sstable_store.clone();
        let futures = delta.delete_sst_object_ids.iter().map(|sst_obj_id| {
            let store = &sstable_store;
            async move {
                let res = store.sstable_cached(*sst_obj_id).await;
                match res {
                    Ok(Some(_)) => GLOBAL_CACHE_REFILL_METRICS
                        .data_refill_parent_meta_lookup_hit_total
                        .inc(),
                    Ok(None) => GLOBAL_CACHE_REFILL_METRICS
                        .data_refill_parent_meta_lookup_miss_total
                        .inc(),
                    _ => {}
                }
                res
            }
        });
        let parent_ssts = match try_join_all(futures).await {
            Ok(parent_ssts) => parent_ssts.into_iter().flatten(),
            Err(e) => {
                return tracing::error!(error = %e.as_report(), "get old meta from cache error");
            }
        };
        let units = Self::get_units_to_refill_by_inheritance(context, &holders, parent_ssts);

        let ssts: HashMap<HummockSstableObjectId, TableHolder> =
            holders.into_iter().map(|meta| (meta.id, meta)).collect();
        let tasks = units
            .into_iter()
            .map(|unit| DataCacheRefillTask {
                sst: ssts.get(&unit.sst_obj_id).unwrap().clone(),
                blks: unit.blks,
            })
            .collect_vec();
        let streaming_view = context
            .table_cache_refill_context
            .read()
            .build_streaming_view();
        let tasks = Self::filter_by_streaming_vnodes(&streaming_view, tasks);
        let futures = tasks.into_iter().map(|task| async move {
            let unit = SstableUnit {
                sst_obj_id: task.sst.id,
                blks: task.blks.clone(),
            };
            if let Err(e) = Self::data_file_cache_refill_unit(context, &task.sst, unit).await {
                tracing::error!(error = %e.as_report(), "data file cache unit refill error");
            }
        });
        join_all(futures).await;
    }

    async fn data_file_cache_refill_unit(
        context: &CacheRefillContext,
        sst: &Sstable,
        unit: SstableUnit,
    ) -> HummockResult<()> {
        let sstable_store = &context.sstable_store;
        let threshold = context.config.threshold;
        let recent_filter = sstable_store.recent_filter();

        // update filter for sst id only
        recent_filter.insert((sst.id, usize::MAX));

        let blocks = unit.blks.size().unwrap();

        let mut tasks = vec![];
        let mut contexts = Vec::with_capacity(blocks);
        let mut admits = 0;

        let (range_first, _) = sst.calculate_block_info(unit.blks.start);
        let (range_last, _) = sst.calculate_block_info(unit.blks.end - 1);
        let range = range_first.start..range_last.end;

        let size = range.size().unwrap();

        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_ideal_bytes
            .inc_by(size as _);

        for blk in unit.blks {
            let (range, uncompressed_capacity) = sst.calculate_block_info(blk);
            let key = SstableBlockIndex {
                sst_id: sst.id,
                block_idx: blk as u64,
            };

            let mut writer = sstable_store.block_cache().storage_writer(key);

            if writer.filter(size).is_admitted() {
                admits += 1;
            }

            contexts.push((writer, range, uncompressed_capacity))
        }

        if admits as f64 / contexts.len() as f64 >= threshold {
            let task = async move {
                GLOBAL_CACHE_REFILL_METRICS.data_refill_attempts_total.inc();

                let permit = context.concurrency.acquire().await.unwrap();

                GLOBAL_CACHE_REFILL_METRICS.data_refill_started_total.inc();

                let timer = GLOBAL_CACHE_REFILL_METRICS
                    .data_refill_success_duration
                    .start_timer();

                let data = sstable_store
                    .store()
                    .read(&sstable_store.get_sst_data_path(sst.id), range.clone())
                    .await?;
                let mut futures = vec![];
                for (w, r, uc) in contexts {
                    let offset = r.start - range.start;
                    let len = r.end - r.start;
                    let bytes = data.slice(offset..offset + len);
                    let future = async move {
                        let value = Box::new(Block::decode(bytes, uc)?);
                        // The entry should always be `Some(..)`, use if here for compatible.
                        if let Some(_entry) = w.force().insert(value) {
                            GLOBAL_CACHE_REFILL_METRICS
                                .data_refill_success_bytes
                                .inc_by(len as u64);
                            GLOBAL_CACHE_REFILL_METRICS
                                .data_refill_block_success_total
                                .inc();
                        }
                        Ok::<_, HummockError>(())
                    };
                    futures.push(future);
                }
                try_join_all(futures)
                    .await
                    .map_err(HummockError::file_cache)?;

                drop(permit);
                drop(timer);

                Ok::<_, HummockError>(())
            };
            tasks.push(task);
        }

        try_join_all(tasks).await?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct DataCacheRefillTask {
    sst: TableHolder,
    blks: Range<usize>,
}

#[derive(Debug)]
pub struct SstableBlock {
    pub sst_obj_id: HummockSstableObjectId,
    pub blk_idx: usize,
}

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct SstableUnit {
    pub sst_obj_id: HummockSstableObjectId,
    pub blks: Range<usize>,
}

impl Ord for SstableUnit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.sst_obj_id.cmp(&other.sst_obj_id) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        match self.blks.start.cmp(&other.blks.start) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        self.blks.end.cmp(&other.blks.end)
    }
}

impl PartialOrd for SstableUnit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct Unit<'a> {
    sst: &'a Sstable,
    blks: Range<usize>,
}

impl<'a> Unit<'a> {
    fn new(sst: &'a Sstable, unit: usize, uidx: usize) -> Self {
        let blks = unit * uidx..std::cmp::min(unit * (uidx + 1), sst.block_count());
        Self { sst, blks }
    }

    fn smallest_key(&self) -> &Vec<u8> {
        &self.sst.meta.block_metas[self.blks.start].smallest_key
    }

    // `largest_key` can be included or excluded, both are treated as included here
    fn largest_key(&self) -> &Vec<u8> {
        if self.blks.end == self.sst.block_count() {
            &self.sst.meta.largest_key
        } else {
            &self.sst.meta.block_metas[self.blks.end].smallest_key
        }
    }

    fn units(sst: &Sstable, unit: usize) -> usize {
        sst.block_count() / unit
            + if sst.block_count().is_multiple_of(unit) {
                0
            } else {
                1
            }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;

    use parking_lot::RwLock;
    use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::key::{FullKey, gen_key_from_str};
    use risingwave_hummock_sdk::sstable_info::SstableInfoInner;
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_pb::hummock::{PbHummockVersion, StateTableInfo};
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::hummock::HummockValue;
    use crate::hummock::iterator::test_utils::{iterator_test_key_of, mock_sstable_store};
    use crate::hummock::local_version::pinned_version::PinnedVersion;
    use crate::hummock::sstable::{BlockMeta, SstableMeta};
    use crate::hummock::store::version::HummockReadVersion;
    use crate::hummock::test_utils::{default_builder_opt_for_test, gen_test_sstable_info};
    use crate::monitor::StoreLocalStatistic;

    fn test_bitmap(bits: &[usize]) -> Bitmap {
        let mut builder = BitmapBuilder::zeroed(VirtualNode::COUNT_FOR_TEST);
        for bit in bits {
            builder.set(*bit, true);
        }
        builder.finish()
    }

    fn test_full_key(table_id: TableId, vnode: usize, suffix: &str) -> Vec<u8> {
        FullKey::new(
            table_id,
            gen_key_from_str(VirtualNode::from_index(vnode), suffix),
            0,
        )
        .encode()
    }

    fn test_sstable_meta(blocks: &[(TableId, usize, &str)]) -> SstableMeta {
        let block_metas = blocks
            .iter()
            .enumerate()
            .map(|(idx, (table_id, vnode, suffix))| BlockMeta {
                smallest_key: test_full_key(*table_id, *vnode, suffix),
                offset: idx as u32,
                len: 1,
                uncompressed_size: 1,
                total_key_count: 1,
                stale_key_count: 0,
            })
            .collect_vec();
        let (last_table_id, last_vnode, last_suffix) = blocks.last().copied().unwrap();
        SstableMeta {
            block_metas,
            smallest_key: test_full_key(blocks[0].0, blocks[0].1, blocks[0].2),
            largest_key: test_full_key(last_table_id, last_vnode, &format!("{last_suffix}_end")),
            estimated_size: 1024,
            key_count: blocks.len() as u32,
            meta_offset: 512,
            ..Default::default()
        }
    }

    fn test_pinned_version(table_ids: impl IntoIterator<Item = TableId>) -> PinnedVersion {
        PinnedVersion::new(
            HummockVersion::from_rpc_protobuf(&PbHummockVersion {
                id: 1.into(),
                state_table_info: HashMap::from_iter(table_ids.into_iter().map(|table_id| {
                    (
                        table_id,
                        StateTableInfo {
                            committed_epoch: test_epoch(233),
                            compaction_group_id: StaticCompactionGroupId::StateDefault,
                        },
                    )
                })),
                ..Default::default()
            }),
            unbounded_channel().0,
        )
    }

    fn test_read_version(
        table_id: TableId,
        instance_id: u64,
        vnodes: Bitmap,
    ) -> Arc<RwLock<HummockReadVersion>> {
        Arc::new(RwLock::new(
            HummockReadVersion::new_with_replication_option(
                table_id,
                instance_id,
                test_pinned_version([table_id]),
                false,
                Arc::new(vnodes),
            ),
        ))
    }

    fn test_refill_context(
        sstable_store: SstableStoreRef,
        read_version_mapping: ReadVersionMappingType,
    ) -> CacheRefillContext {
        test_refill_context_with_options(sstable_store, read_version_mapping, HashSet::new(), true)
    }

    fn test_refill_context_with_options(
        sstable_store: SstableStoreRef,
        read_version_mapping: ReadVersionMappingType,
        data_refill_levels: HashSet<u32>,
        skip_recent_filter: bool,
    ) -> CacheRefillContext {
        CacheRefillContext {
            config: Arc::new(CacheRefillConfig {
                timeout: Duration::from_secs(1),
                data_refill_levels,
                meta_refill_concurrency: 0,
                concurrency: 1,
                unit: 2,
                threshold: 0.0,
                skip_recent_filter,
            }),
            meta_refill_concurrency: None,
            concurrency: Arc::new(Semaphore::new(1)),
            sstable_store,
            table_cache_refill_context: Arc::new(RwLock::new(TableCacheRefillContext {
                read_version_mapping: Arc::new(RwLock::new(read_version_mapping)),
            })),
        }
    }

    #[test]
    fn test_live_streaming_view_tracks_read_version_updates() {
        let local_table = TableId::new(1);
        let context = TableCacheRefillContext {
            read_version_mapping: Arc::new(RwLock::new(HashMap::from_iter([(
                local_table,
                HashMap::from_iter([
                    (1, test_read_version(local_table, 1, test_bitmap(&[0]))),
                    (2, test_read_version(local_table, 2, test_bitmap(&[1]))),
                ]),
            )]))),
        };
        let read_version_1 = context
            .read_version_mapping
            .read()
            .get(&local_table)
            .unwrap()
            .get(&1)
            .unwrap()
            .clone();

        let view = context.build_streaming_view();
        assert_eq!(
            view.streaming.get(&local_table),
            Some(&test_bitmap(&[0, 1]))
        );

        read_version_1
            .write()
            .update_vnode_bitmap(Arc::new(test_bitmap(&[2])));
        let view = context.build_streaming_view();
        assert_eq!(
            view.streaming.get(&local_table),
            Some(&test_bitmap(&[1, 2]))
        );
    }

    #[test]
    fn test_meta_gate_filters_by_table_overlap() {
        let local_table = TableId::new(1);
        let remote_table = TableId::new(2);
        let context = TableCacheRefillContext {
            read_version_mapping: Arc::new(RwLock::new(HashMap::from_iter([(
                local_table,
                HashMap::from_iter([(1, test_read_version(local_table, 1, test_bitmap(&[0])))]),
            )]))),
        };
        let streaming_view = context.build_streaming_view();
        let local_info: SstableInfo = SstableInfoInner {
            object_id: 1.into(),
            sst_id: 1.into(),
            table_ids: vec![local_table],
            file_size: 1024,
            meta_offset: 512,
            ..Default::default()
        }
        .into();
        let remote_info: SstableInfo = SstableInfoInner {
            object_id: 2.into(),
            sst_id: 2.into(),
            table_ids: vec![remote_table],
            file_size: 1024,
            meta_offset: 512,
            ..Default::default()
        }
        .into();
        let mixed_info: SstableInfo = SstableInfoInner {
            object_id: 3.into(),
            sst_id: 3.into(),
            table_ids: vec![remote_table, local_table],
            file_size: 1024,
            meta_offset: 512,
            ..Default::default()
        }
        .into();

        assert!(CacheRefillTask::should_admit_meta_refill(
            &streaming_view,
            &local_info
        ));
        assert!(!CacheRefillTask::should_admit_meta_refill(
            &streaming_view,
            &remote_info
        ));
        assert!(CacheRefillTask::should_admit_meta_refill(
            &streaming_view,
            &mixed_info
        ));
    }

    #[tokio::test]
    async fn test_data_gate_uses_unit_or_semantics() {
        let remote_table_id = TableId::new(1);
        let local_table_id = TableId::new(2);
        let sstable_store = mock_sstable_store().await;
        sstable_store.insert_meta_cache(
            1.into(),
            test_sstable_meta(&[(remote_table_id, 0, "a"), (local_table_id, 1, "b")]),
        );
        let sst = sstable_store
            .sstable_cached(1.into())
            .await
            .unwrap()
            .unwrap();
        let context = test_refill_context(
            sstable_store,
            HashMap::from_iter([(
                local_table_id,
                HashMap::from_iter([(1, test_read_version(local_table_id, 1, test_bitmap(&[1])))]),
            )]),
        );
        let streaming_view = context
            .table_cache_refill_context
            .read()
            .build_streaming_view();

        let retained = CacheRefillTask::filter_by_streaming_vnodes(
            &streaming_view,
            vec![
                DataCacheRefillTask {
                    sst: sst.clone(),
                    blks: 0..2,
                },
                DataCacheRefillTask { sst, blks: 0..1 },
            ],
        );

        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].blks, 0..2);
    }

    #[tokio::test]
    async fn test_data_gate_handles_high_vnode_boundary() {
        let table_id = TableId::new(1);
        let high_vnode = VirtualNode::COUNT_FOR_TEST - 1;
        let sstable_store = mock_sstable_store().await;
        sstable_store
            .insert_meta_cache(1.into(), test_sstable_meta(&[(table_id, high_vnode, "z")]));
        let sst = sstable_store
            .sstable_cached(1.into())
            .await
            .unwrap()
            .unwrap();
        let streaming_view = StreamingTableCacheRefillView {
            streaming: HashMap::from_iter([(table_id, test_bitmap(&[high_vnode]))]),
        };

        assert!(streaming_view.check_table_refill_vnodes(&sst, 0));
    }

    #[tokio::test]
    async fn test_data_gate_fail_open_on_cross_table_next_block_boundary() {
        let local_table_id = TableId::new(1);
        let remote_table_id = TableId::new(2);
        let sstable_store = mock_sstable_store().await;
        sstable_store.insert_meta_cache(
            1.into(),
            test_sstable_meta(&[(local_table_id, 0, "a"), (remote_table_id, 1, "b")]),
        );
        let sst = sstable_store
            .sstable_cached(1.into())
            .await
            .unwrap()
            .unwrap();
        let streaming_view = StreamingTableCacheRefillView {
            streaming: HashMap::from_iter([(local_table_id, test_bitmap(&[0]))]),
        };

        assert!(streaming_view.check_table_refill_vnodes(&sst, 0));
    }

    #[tokio::test]
    async fn test_data_gate_fail_open_on_cross_table_largest_key_boundary() {
        let local_table_id = TableId::new(1);
        let remote_table_id = TableId::new(2);
        let sstable_store = mock_sstable_store().await;
        let mut meta = test_sstable_meta(&[(local_table_id, 0, "a")]);
        meta.largest_key = test_full_key(remote_table_id, 1, "remote_end");
        sstable_store.insert_meta_cache(1.into(), meta);
        let sst = sstable_store
            .sstable_cached(1.into())
            .await
            .unwrap()
            .unwrap();
        let streaming_view = StreamingTableCacheRefillView {
            streaming: HashMap::from_iter([(local_table_id, test_bitmap(&[0]))]),
        };

        assert!(streaming_view.check_table_refill_vnodes(&sst, 0));
    }

    #[tokio::test]
    async fn test_non_l0_skip_recent_filter_still_applies_streaming_gate() {
        let local_table_id = TableId::new(1);
        let remote_table_id = TableId::new(2);
        let sstable_store = mock_sstable_store().await;
        let inserted_sst_id = 11;
        let parent_sst_id = 22;
        let remote_only_meta =
            test_sstable_meta(&[(remote_table_id, 0, "a"), (remote_table_id, 1, "b")]);
        sstable_store.insert_meta_cache(inserted_sst_id.into(), remote_only_meta.clone());
        sstable_store.insert_meta_cache(parent_sst_id.into(), remote_only_meta);
        let holder = sstable_store
            .sstable_cached(inserted_sst_id.into())
            .await
            .unwrap()
            .unwrap();
        let context = test_refill_context_with_options(
            sstable_store,
            HashMap::from_iter([(
                local_table_id,
                HashMap::from_iter([(1, test_read_version(local_table_id, 1, test_bitmap(&[7])))]),
            )]),
            HashSet::from_iter([1]),
            true,
        );
        let delta = SstDeltaInfo {
            insert_sst_infos: vec![
                SstableInfoInner {
                    object_id: inserted_sst_id.into(),
                    sst_id: inserted_sst_id.into(),
                    table_ids: vec![remote_table_id],
                    ..Default::default()
                }
                .into(),
            ],
            delete_sst_object_ids: vec![parent_sst_id.into()],
            insert_sst_level: 1,
            ..Default::default()
        };
        let recent_filter = context.sstable_store.recent_filter();
        assert!(!recent_filter.contains(&(inserted_sst_id.into(), usize::MAX)));

        CacheRefillTask::data_cache_refill(&context, &delta, vec![holder]).await;

        assert!(!recent_filter.contains(&(inserted_sst_id.into(), usize::MAX)));
    }

    #[tokio::test]
    async fn test_meta_gate_skip_still_allows_miss_load() {
        let local_table = TableId::new(1);
        let sstable_store = mock_sstable_store().await;
        let sst_info = gen_test_sstable_info(
            default_builder_opt_for_test(),
            1,
            (0..2).map(|idx| {
                (
                    iterator_test_key_of(idx),
                    HummockValue::put(vec![idx as u8]),
                )
            }),
            sstable_store.clone(),
        )
        .await;
        sstable_store.clear_meta_cache().await.unwrap();
        let delta = SstDeltaInfo {
            insert_sst_infos: vec![sst_info.clone()],
            ..Default::default()
        };
        let context = test_refill_context(
            sstable_store.clone(),
            HashMap::from_iter([(
                local_table,
                HashMap::from_iter([(1, test_read_version(local_table, 1, test_bitmap(&[0])))]),
            )]),
        );

        let holders = CacheRefillTask::meta_cache_refill(&context, &delta)
            .await
            .unwrap();
        assert!(holders.is_empty());
        assert!(
            sstable_store
                .sstable_cached(sst_info.object_id)
                .await
                .unwrap()
                .is_none()
        );

        let holder = sstable_store
            .sstable(&sst_info, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        assert_eq!(holder.id, sst_info.object_id);
        assert!(
            sstable_store
                .sstable_cached(sst_info.object_id)
                .await
                .unwrap()
                .is_some()
        );
    }
}
