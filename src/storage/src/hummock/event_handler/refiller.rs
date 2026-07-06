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

use std::collections::hash_map::HashMap;
use std::collections::{HashSet, VecDeque};
use std::future::poll_fn;
use std::hash::Hash;
use std::ops::{Bound, Range};
use std::sync::{Arc, LazyLock};
use std::task::Poll;
use std::time::{Duration, Instant};

use foyer::RangeBoundsExt;
use futures::future::{join_all, try_join_all};
use futures::{Future, FutureExt};
use itertools::Itertools;
use parking_lot::RwLock;
use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{
    Histogram, HistogramVec, IntGauge, Registry, register_histogram_vec_with_registry,
    register_int_counter_vec_with_registry, register_int_gauge_with_registry,
};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::Role;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::license::Feature;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::key::{FullKey, vnode_range};
use risingwave_hummock_sdk::{HummockSstableObjectId, KeyComparator};
use risingwave_pb::id::TableId;
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

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

    /// Skip inheritance filter.s
    pub skip_inheritance_filter: bool,

    /// Default table cache refill policy.
    pub table_cache_refill_default_policy: CacheRefillPolicy,
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
            skip_inheritance_filter: options.cache_refill_skip_inheritance_filter,
            table_cache_refill_default_policy: options
                .cache_refill_table_cache_refill_default_policy,
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

pub type TableCacheRefillContextMap = HashMap<TableId, TableCacheRefillContext>;

/// Per-table metadata captured for a refill task to decide whether an sstable block should be
/// refilled. Mutable runtime state used to build this snapshot is owned by `CacheRefiller`.
#[derive(Clone)]
pub struct TableCacheRefillContext {
    /// Vnodes covered by local streaming read versions on this compute node.
    pub streaming_vnode_bitmap: Option<Bitmap>,
    /// Vnodes served by this compute node according to the serving vnode mapping.
    pub serving_vnode_bitmap: Option<Bitmap>,
    /// Effective refill policy after applying the default policy and per-table overrides.
    pub policy: CacheRefillPolicy,
}

/// Read-only data cloned from `CacheRefiller` for monitor/debugging APIs.
///
/// Streaming vnode mapping is the table-level union maintained by the refiller.
#[derive(Clone)]
pub struct TableCacheRefillMonitorSnapshot {
    pub contexts: TableCacheRefillContextMap,
    pub policies: HashMap<TableId, CacheRefillPolicy>,
    pub default_policy: CacheRefillPolicy,
    pub streaming_table_vnode_mapping: HashMap<TableId, Bitmap>,
    pub serving_table_vnode_mapping: HashMap<TableId, Bitmap>,
}

fn vnode_range_overlaps_bitmap(vnode_range: (usize, usize), bitmap: &Bitmap) -> bool {
    assert!(vnode_range.0 <= vnode_range.1);
    let start = vnode_range.0.min(bitmap.len());
    let end = vnode_range.1.min(bitmap.len());
    if start == end || !bitmap.any() {
        return false;
    }
    if bitmap.all() {
        return true;
    }
    (start..end).any(|vnode| bitmap.is_set(vnode))
}

impl TableCacheRefillContext {
    fn allows_normal_data_refill_block(&self, sstable: &Sstable, block_index: usize) -> bool {
        if self.policy.is_unscoped_enabled() {
            return true;
        }

        (self.policy.is_streaming_scoped()
            && self.check_table_refill_streaming_vnodes(sstable, block_index))
            || (self.policy.is_serving_scoped()
                && self.check_table_refill_serving_vnodes(sstable, block_index))
    }

    fn allows_insert_only_data_refill_block(&self, sstable: &Sstable, block_index: usize) -> bool {
        // Insert-only deltas have no delete-side evidence for recent/inheritance filters.
        // Only serving-owned blocks need refill, because streaming writers already populated
        // their local cache.
        (self.policy.is_unscoped_enabled() || self.policy.is_serving_scoped())
            && self.check_table_refill_serving_vnodes(sstable, block_index)
    }

    fn check_table_refill_streaming_vnodes(&self, sstable: &Sstable, block_index: usize) -> bool {
        self.streaming_vnode_bitmap.as_ref().is_some_and(|bitmap| {
            let vnode_range = block_vnode_range(sstable, block_index);
            vnode_range_overlaps_bitmap(vnode_range, bitmap)
        })
    }

    fn check_table_refill_serving_vnodes(&self, sstable: &Sstable, block_index: usize) -> bool {
        self.serving_vnode_bitmap.as_ref().is_some_and(|bitmap| {
            let vnode_range = block_vnode_range(sstable, block_index);
            vnode_range_overlaps_bitmap(vnode_range, bitmap)
        })
    }
}

fn block_vnode_range(sstable: &Sstable, block_index: usize) -> (usize, usize) {
    let block_smallest_key = FullKey::decode(&sstable.meta.block_metas[block_index].smallest_key);
    let block_largest_key = FullKey::decode(
        sstable
            .meta
            .block_metas
            .get(block_index + 1)
            .map(|b| &b.smallest_key)
            .unwrap_or_else(|| &sstable.meta.largest_key),
    );

    let table_key_range = (
        Bound::Included(block_smallest_key.user_key.table_key),
        Bound::Excluded(block_largest_key.user_key.table_key),
    );
    vnode_range(&table_key_range)
}

/// A cache refiller for hummock data.
pub(crate) struct CacheRefiller {
    /// order: old => new
    queue: VecDeque<Item>,

    spawn_refill_task: SpawnRefillTask,

    config: Arc<CacheRefillConfig>,
    meta_refill_concurrency: Option<Arc<Semaphore>>,
    concurrency: Arc<Semaphore>,
    sstable_store: SstableStoreRef,

    role: Role,
    default_policy: CacheRefillPolicy,
    table_cache_refill_policies: HashMap<TableId, CacheRefillPolicy>,
    streaming_table_vnode_mapping: HashMap<TableId, Bitmap>,
    serving_table_vnode_mapping: HashMap<TableId, Bitmap>,
    table_cache_refill_context_map: Arc<RwLock<TableCacheRefillContextMap>>,
}

impl CacheRefiller {
    pub(crate) fn new(
        role: Role,
        config: CacheRefillConfig,
        sstable_store: SstableStoreRef,
        spawn_refill_task: SpawnRefillTask,
    ) -> Self {
        let config = Arc::new(config);
        let concurrency = Arc::new(Semaphore::new(config.concurrency));
        let table_cache_refill_context_map = Arc::new(RwLock::new(HashMap::new()));
        let default_policy = config.table_cache_refill_default_policy;
        let meta_refill_concurrency = if config.meta_refill_concurrency == 0 {
            None
        } else {
            Some(Arc::new(Semaphore::new(config.meta_refill_concurrency)))
        };
        Self {
            queue: VecDeque::new(),
            spawn_refill_task,
            config,
            meta_refill_concurrency,
            concurrency,
            sstable_store,
            role,
            default_policy,
            table_cache_refill_policies: HashMap::new(),
            streaming_table_vnode_mapping: HashMap::new(),
            serving_table_vnode_mapping: HashMap::new(),
            table_cache_refill_context_map,
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
        let context = self.new_cache_refill_context(&deltas);
        let handle = (self.spawn_refill_task)(
            deltas,
            context,
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

    fn new_cache_refill_context(&self, deltas: &[SstDeltaInfo]) -> CacheRefillContext {
        let table_ids = Self::table_ids_in_deltas(deltas);
        CacheRefillContext {
            config: self.config.clone(),
            meta_refill_concurrency: self.meta_refill_concurrency.clone(),
            concurrency: self.concurrency.clone(),
            sstable_store: self.sstable_store.clone(),
            table_cache_refill_context_map: Arc::new(
                self.get_table_cache_refill_context_map(&table_ids),
            ),
        }
    }

    pub(crate) fn last_new_pinned_version(&self) -> Option<&PinnedVersion> {
        self.queue.back().map(|item| &item.event.new_pinned_version)
    }

    /// Replaces the complete explicit table refill policy map and rebuilds affected tables.
    pub(crate) fn replace_table_cache_refill_policies(
        &mut self,
        policies: HashMap<TableId, CacheRefillPolicy>,
    ) {
        let table_ids = self
            .table_cache_refill_policies
            .keys()
            .chain(policies.keys())
            .copied()
            .collect::<HashSet<_>>();
        self.table_cache_refill_policies = policies;
        self.rebuild_table_cache_refill_contexts(table_ids);
    }

    /// Replaces the complete serving vnode mapping snapshot and rebuilds affected tables.
    pub(crate) fn replace_serving_table_vnode_mapping(
        &mut self,
        mapping: HashMap<TableId, Bitmap>,
    ) {
        let table_ids = self
            .serving_table_vnode_mapping
            .keys()
            .chain(mapping.keys())
            .copied()
            .collect::<HashSet<_>>();
        self.serving_table_vnode_mapping = mapping;
        self.rebuild_table_cache_refill_contexts(table_ids);
    }

    pub(crate) fn update_streaming_table_vnodes(
        &mut self,
        table_id: TableId,
        streaming_vnodes: Option<Bitmap>,
    ) {
        if let Some(streaming_vnodes) = streaming_vnodes {
            self.streaming_table_vnode_mapping
                .insert(table_id, streaming_vnodes);
        } else {
            self.streaming_table_vnode_mapping.remove(&table_id);
        }
        self.rebuild_table_cache_refill_contexts([table_id]);
    }

    fn rebuild_table_cache_refill_contexts(&self, table_ids: impl IntoIterator<Item = TableId>) {
        let mut table_cache_refill_context_map = self.table_cache_refill_context_map.write();
        for table_id in table_ids {
            self.rebuild_table_cache_refill_context(&mut table_cache_refill_context_map, table_id);
        }
    }

    fn rebuild_table_cache_refill_context(
        &self,
        table_cache_refill_context_map: &mut TableCacheRefillContextMap,
        table_id: TableId,
    ) {
        tracing::debug!(?table_id, "rebuild table cache refill context for table");

        let policy = self
            .table_cache_refill_policies
            .get(&table_id)
            .copied()
            .unwrap_or(self.default_policy);

        if policy.is_streaming_scoped() && !self.role.for_streaming() {
            tracing::warn!(
                ?table_id,
                ?policy,
                role = ?self.role,
                "skip materializing streaming vnode bitmap because worker role cannot provide streaming ownership evidence",
            );
        }
        let streaming_vnode_bitmap = (self.role.for_streaming() && policy.is_streaming_scoped())
            .then(|| self.streaming_table_vnode_mapping.get(&table_id).cloned())
            .flatten();
        if policy.is_serving_scoped() && !self.role.for_serving() {
            tracing::warn!(
                ?table_id,
                ?policy,
                role = ?self.role,
                "skip materializing serving vnode bitmap because worker role cannot provide serving ownership evidence",
            );
        }
        // `Enabled` normally does not use bitmap filtering. The only exception is L0
        // insert-only refill, where serving workers still need serving-locality evidence.
        let serving_vnode_bitmap = (self.role.for_serving()
            && (policy.is_serving_scoped() || policy.is_unscoped_enabled()))
        .then(|| self.serving_table_vnode_mapping.get(&table_id).cloned())
        .flatten();

        if self.table_cache_refill_policies.contains_key(&table_id)
            || streaming_vnode_bitmap.is_some()
            || serving_vnode_bitmap.is_some()
        {
            table_cache_refill_context_map.insert(
                table_id,
                TableCacheRefillContext {
                    streaming_vnode_bitmap,
                    serving_vnode_bitmap,
                    policy,
                },
            );
        } else {
            table_cache_refill_context_map.remove(&table_id);
        }
    }

    fn table_ids_in_deltas(deltas: &[SstDeltaInfo]) -> HashSet<TableId> {
        deltas
            .iter()
            .flat_map(|delta| {
                delta
                    .insert_sst_infos
                    .iter()
                    .flat_map(|sst| sst.table_ids.iter().copied())
            })
            .collect()
    }

    fn get_table_cache_refill_context_map(
        &self,
        table_ids: &HashSet<TableId>,
    ) -> TableCacheRefillContextMap {
        let table_cache_refill_context_map = self.table_cache_refill_context_map.read();
        table_ids
            .iter()
            .map(|table_id| {
                let context = table_cache_refill_context_map
                    .get(table_id)
                    .cloned()
                    .unwrap_or(TableCacheRefillContext {
                        streaming_vnode_bitmap: None,
                        serving_vnode_bitmap: None,
                        policy: self.default_policy,
                    });
                (*table_id, context)
            })
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn table_cache_refill_context_map(
        &self,
    ) -> &Arc<RwLock<TableCacheRefillContextMap>> {
        &self.table_cache_refill_context_map
    }

    pub(crate) fn table_cache_refill_monitor_snapshot(&self) -> TableCacheRefillMonitorSnapshot {
        TableCacheRefillMonitorSnapshot {
            contexts: self.table_cache_refill_context_map.read().clone(),
            policies: self.table_cache_refill_policies.clone(),
            default_policy: self.default_policy,
            streaming_table_vnode_mapping: self.streaming_table_vnode_mapping.clone(),
            serving_table_vnode_mapping: self.serving_table_vnode_mapping.clone(),
        }
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
    table_cache_refill_context_map: Arc<TableCacheRefillContextMap>,
}

struct DataCacheRefillTaskGenerator<'a> {
    context: &'a CacheRefillContext,
    delta: &'a SstDeltaInfo,
    ssts: &'a [TableHolder],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DataCacheRefillPath {
    Normal,
    L0InsertOnly,
}

fn classify_data_cache_refill_path(delta: &SstDeltaInfo) -> Option<DataCacheRefillPath> {
    if delta.insert_sst_infos.is_empty() {
        return None;
    }

    if delta.delete_sst_object_ids.is_empty() {
        // CN-written L0 SSTs have no deleted SSTs. Streaming writers already warm their local
        // cache through upload, but serving workers still need a chance to refill serving-owned
        // blocks from the inserted SSTs.
        if delta.insert_sst_level == 0 {
            Some(DataCacheRefillPath::L0InsertOnly)
        } else {
            None
        }
    } else {
        Some(DataCacheRefillPath::Normal)
    }
}

impl DataCacheRefillTaskGenerator<'_> {
    async fn generate(&self) -> Vec<DataCacheRefillTask> {
        let mut tasks = Vec::new();

        // Skip data cache refill if data disk cache is not enabled.
        if !self.context.sstable_store.block_cache().is_hybrid() {
            return tasks;
        }

        let Some(refill_kind) = classify_data_cache_refill_path(self.delta) else {
            return tasks;
        };

        // Return if the target level is not in the refill levels
        if !self
            .context
            .config
            .data_refill_levels
            .contains(&self.delta.insert_sst_level)
        {
            return tasks;
        }

        // Filter by recent filter.
        let total_block_count: u64 = self.ssts.iter().map(|sst| sst.block_count() as u64).sum();
        if refill_kind == DataCacheRefillPath::Normal && !self.context.config.skip_recent_filter {
            let refill = self.filter_by_recent_filter();
            if refill {
                GLOBAL_CACHE_REFILL_METRICS
                    .data_refill_block_unfiltered_total
                    .inc_by(total_block_count);
            } else {
                GLOBAL_CACHE_REFILL_METRICS
                    .data_refill_filtered_total
                    .inc_by(total_block_count);
                return tasks;
            }
        }

        // Split tasks by unit.
        let unit = self.context.config.unit;
        for sst in self.ssts {
            for blk_start in (0..sst.block_count()).step_by(unit) {
                let blk_end = std::cmp::min(sst.block_count(), blk_start + unit);
                let task = DataCacheRefillTask {
                    sst: sst.clone(),
                    blks: blk_start..blk_end,
                };
                tasks.push(task);
            }
        }

        // Filter by inheritance filter, also requires recent filter enabled.
        if refill_kind == DataCacheRefillPath::Normal
            && self.delta.insert_sst_level != 0
            && !self.context.config.skip_recent_filter
            && !self.context.config.skip_inheritance_filter
        {
            tasks = self.filter_by_inheritance_filter(tasks).await;
        }

        // Filter by table cache refill policy and vnodes.
        tasks = match refill_kind {
            DataCacheRefillPath::Normal => self.filter_by_table_cache_refill_policy(tasks),
            DataCacheRefillPath::L0InsertOnly => {
                self.filter_insert_only_by_table_cache_refill_policy(tasks)
            }
        };

        tasks
    }

    // Return if recent filter is required and no deleted sst ids are in the recent filter.
    fn filter_by_recent_filter(&self) -> bool {
        let recent_filter = self.context.sstable_store.recent_filter();
        let targets = self
            .delta
            .delete_sst_object_ids
            .iter()
            .map(|id| (*id, usize::MAX))
            .collect_vec();
        recent_filter.contains_any(targets.iter())
    }

    async fn filter_by_inheritance_filter(
        &self,
        originals: Vec<DataCacheRefillTask>,
    ) -> Vec<DataCacheRefillTask> {
        // Get parent sst metas from cache.
        let sstable_store = self.context.sstable_store.clone();
        let futures = self.delta.delete_sst_object_ids.iter().map(|sst_obj_id| {
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
                tracing::error!(error = %e.as_report(), "get old meta from cache error");
                return vec![];
            }
        };

        // assert units in asc order
        if cfg!(debug_assertions) {
            originals.iter().tuple_windows().for_each(|(a, b)| {
                debug_assert_ne!(
                    KeyComparator::compare_encoded_full_key(a.largest_key(), b.smallest_key()),
                    std::cmp::Ordering::Greater
                )
            });
        }

        let mut filtered: HashSet<SstableUnit> = HashSet::default();
        let recent_filter = self.context.sstable_store.recent_filter();
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
                let uleft = originals.partition_point(|task| {
                    KeyComparator::compare_encoded_full_key(task.largest_key(), pleft)
                        == std::cmp::Ordering::Less
                });
                // partition point: unit.left <= pblk.right
                let uright = originals.partition_point(|task| {
                    KeyComparator::compare_encoded_full_key(task.smallest_key(), pright)
                        != std::cmp::Ordering::Greater
                });

                // overlapping: uleft..uright
                for task in originals.iter().take(uright).skip(uleft) {
                    let unit = task.unit();
                    if filtered.contains(&unit) {
                        continue;
                    }
                    if recent_filter.contains(&(psst.id, pblk)) {
                        filtered.insert(unit);
                    }
                }
            }
        }

        let hit = filtered.len();
        let miss = originals.len() - hit;
        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_unit_inheritance_hit_total
            .inc_by(hit as u64);
        GLOBAL_CACHE_REFILL_METRICS
            .data_refill_unit_inheritance_miss_total
            .inc_by(miss as u64);

        originals
            .into_iter()
            .filter(|task| filtered.contains(&task.unit()))
            .collect()
    }

    /// Filter tasks by table cache refill policy and related table id and vnode mapping.
    fn filter_by_table_cache_refill_policy(
        &self,
        mut tasks: Vec<DataCacheRefillTask>,
    ) -> Vec<DataCacheRefillTask> {
        let table_cache_refill_context_map = &self.context.table_cache_refill_context_map;
        let check = |task: &DataCacheRefillTask| {
            for blk in task.blks.start..task.blks.end {
                let table_id = task.sst.meta.block_metas[blk].table_id();
                let Some(context) = table_cache_refill_context_map.get(&table_id) else {
                    continue;
                };
                if context.allows_normal_data_refill_block(&task.sst, blk) {
                    return true;
                }
            }
            false
        };
        tasks.retain(check);
        tasks
    }

    fn filter_insert_only_by_table_cache_refill_policy(
        &self,
        mut tasks: Vec<DataCacheRefillTask>,
    ) -> Vec<DataCacheRefillTask> {
        let table_cache_refill_context_map = &self.context.table_cache_refill_context_map;
        let check = |task: &DataCacheRefillTask| {
            for blk in task.blks.start..task.blks.end {
                let table_id = task.sst.meta.block_metas[blk].table_id();
                if let Some(context) = table_cache_refill_context_map.get(&table_id)
                    && context.allows_insert_only_data_refill_block(&task.sst, blk)
                {
                    return true;
                }
            }
            false
        };
        tasks.retain(check);
        tasks
    }
}

#[derive(Debug)]
struct DataCacheRefillTask {
    sst: TableHolder,
    blks: Range<usize>,
}

impl DataCacheRefillTask {
    fn unit(&self) -> SstableUnit {
        SstableUnit {
            sst_obj_id: self.sst.id,
            blks: self.blks.clone(),
        }
    }

    fn smallest_key(&self) -> &[u8] {
        &self.sst.meta.block_metas[self.blks.start].smallest_key
    }

    fn largest_key(&self) -> &[u8] {
        if self.blks.end == self.sst.block_count() {
            &self.sst.meta.largest_key
        } else {
            &self.sst.meta.block_metas[self.blks.end].smallest_key
        }
    }
}

struct CacheRefillTask {
    deltas: Vec<SstDeltaInfo>,
    context: CacheRefillContext,
}

impl CacheRefillTask {
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
        let tasks = delta
            .insert_sst_infos
            .iter()
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

    async fn data_cache_refill(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
        holders: Vec<TableHolder>,
    ) {
        let generator = DataCacheRefillTaskGenerator {
            context,
            delta,
            ssts: &holders,
        };
        let tasks = generator.generate().await;

        let mut futures = Vec::with_capacity(tasks.len());
        for task in tasks {
            // update filter for sst id only
            context
                .sstable_store
                .recent_filter()
                .insert((task.sst.id, usize::MAX));

            let blocks = task.blks.end - task.blks.start + 1;
            let mut contexts = Vec::with_capacity(blocks);
            let mut admits = 0;

            let (range_first, _) = task.sst.calculate_block_info(task.blks.start);
            let (range_last, _) = task.sst.calculate_block_info(task.blks.end - 1);
            let range = range_first.start..range_last.end;

            let size = range.size().unwrap();

            GLOBAL_CACHE_REFILL_METRICS
                .data_refill_ideal_bytes
                .inc_by(size as _);

            for blk in task.blks {
                let (range, uncompressed_capacity) = task.sst.calculate_block_info(blk);
                let key = SstableBlockIndex {
                    sst_id: task.sst.id,
                    block_idx: blk as u64,
                };

                let mut writer = context.sstable_store.block_cache().storage_writer(key);

                if writer.filter(size).is_admitted() {
                    admits += 1;
                }

                contexts.push((writer, range, uncompressed_capacity))
            }

            if admits as f64 / contexts.len() as f64 >= context.config.threshold {
                let sstable_store = context.sstable_store.clone();
                let context = context.clone();
                let future = async move {
                    GLOBAL_CACHE_REFILL_METRICS.data_refill_attempts_total.inc();

                    let permit = context.concurrency.acquire().await.unwrap();

                    GLOBAL_CACHE_REFILL_METRICS.data_refill_started_total.inc();

                    let timer = GLOBAL_CACHE_REFILL_METRICS
                        .data_refill_success_duration
                        .start_timer();

                    let data = sstable_store
                        .store()
                        .read(&sstable_store.get_sst_data_path(task.sst.id), range.clone())
                        .await?;
                    let mut apply_disk_cache_futures = vec![];
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
                        apply_disk_cache_futures.push(future);
                    }
                    try_join_all(apply_disk_cache_futures)
                        .await
                        .map_err(HummockError::file_cache)?;

                    drop(permit);
                    drop(timer);

                    Ok::<_, HummockError>(())
                };
                futures.push(future);
            }
        }

        let futures = futures.into_iter().map(|future| async move {
            if let Err(e) = future.await {
                tracing::error!(error = %e.as_report(), "data cache refill task error");
            }
        });

        join_all(futures).await;
    }
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

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;

    use parking_lot::Mutex;
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::config::Role;
    use risingwave_common::config::streaming::CacheRefillPolicy;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::EpochWithGap;
    use risingwave_hummock_sdk::key::FullKey;
    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_pb::hummock::PbHummockVersion;
    use risingwave_pb::id::TableId;
    use tokio::sync::mpsc::unbounded_channel;

    use super::{
        CacheRefillConfig, CacheRefillContext, CacheRefiller, DataCacheRefillPath,
        DataCacheRefillTaskGenerator, SpawnRefillTask, SstDeltaInfo,
        classify_data_cache_refill_path, vnode_range_overlaps_bitmap,
    };
    use crate::hummock::iterator::test_utils::{iterator_test_table_key_of, mock_sstable_store};
    use crate::hummock::local_version::pinned_version::PinnedVersion;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_with_table_ids,
    };
    use crate::hummock::value::HummockValue;

    fn test_refill_config(default_policy: CacheRefillPolicy) -> CacheRefillConfig {
        CacheRefillConfig {
            timeout: Duration::from_secs(1),
            data_refill_levels: HashSet::new(),
            meta_refill_concurrency: 1,
            concurrency: 1,
            unit: 1,
            threshold: 0.0,
            skip_recent_filter: true,
            skip_inheritance_filter: true,
            table_cache_refill_default_policy: default_policy,
        }
    }

    fn pinned_version_for_test() -> PinnedVersion {
        PinnedVersion::new(
            HummockVersion::from(PbHummockVersion::default()),
            unbounded_channel().0,
        )
    }

    #[tokio::test]
    async fn test_serving_snapshot_updates_table_context_lifecycle() {
        let table_id = TableId::from(233);
        let removed_table_id = TableId::from(234);
        let serving_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
        let mut refiller = CacheRefiller::new(
            Role::Serving,
            test_refill_config(CacheRefillPolicy::Disabled),
            mock_sstable_store().await,
            CacheRefiller::default_spawn_refill_task(),
        );

        refiller.replace_table_cache_refill_policies(HashMap::from([
            (table_id, CacheRefillPolicy::Serving),
            (removed_table_id, CacheRefillPolicy::Serving),
        ]));
        let context_map = refiller.table_cache_refill_context_map().read();
        assert!(
            context_map
                .get(&table_id)
                .is_none_or(|context| context.serving_vnode_bitmap.is_none())
        );
        drop(context_map);

        refiller.replace_serving_table_vnode_mapping(HashMap::from([
            (table_id, serving_vnodes.clone()),
            (removed_table_id, serving_vnodes.clone()),
        ]));

        let context_map = refiller.table_cache_refill_context_map().read();
        let context = context_map.get(&table_id).unwrap();
        assert!(context.streaming_vnode_bitmap.is_none());
        assert_eq!(context.serving_vnode_bitmap.as_ref(), Some(&serving_vnodes));
        assert!(
            context_map
                .get(&removed_table_id)
                .is_some_and(|context| context.serving_vnode_bitmap.is_some())
        );
        drop(context_map);

        refiller.replace_serving_table_vnode_mapping(HashMap::from([(
            table_id,
            serving_vnodes.clone(),
        )]));

        let context_map = refiller.table_cache_refill_context_map().read();
        assert_eq!(
            context_map
                .get(&table_id)
                .and_then(|context| context.serving_vnode_bitmap.as_ref()),
            Some(&serving_vnodes)
        );
        assert!(
            context_map
                .get(&removed_table_id)
                .is_some_and(|context| context.serving_vnode_bitmap.is_none())
        );
    }

    #[tokio::test]
    async fn test_policy_runtime_config_replacement_removes_context_without_active_vnodes() {
        let table_id = TableId::from(233);
        let serving_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
        let mut refiller = CacheRefiller::new(
            Role::Serving,
            test_refill_config(CacheRefillPolicy::Disabled),
            mock_sstable_store().await,
            CacheRefiller::default_spawn_refill_task(),
        );

        refiller.replace_serving_table_vnode_mapping(HashMap::from([(table_id, serving_vnodes)]));
        refiller.replace_table_cache_refill_policies(HashMap::from([(
            table_id,
            CacheRefillPolicy::Serving,
        )]));
        let context_map = refiller.table_cache_refill_context_map().read();
        assert!(
            context_map
                .get(&table_id)
                .is_some_and(|context| context.serving_vnode_bitmap.is_some())
        );
        drop(context_map);

        refiller.replace_table_cache_refill_policies(HashMap::new());

        let context_map = refiller.table_cache_refill_context_map().read();
        assert!(context_map.is_empty());
    }

    #[tokio::test]
    async fn test_refill_task_materializes_context_for_delta_tables() {
        let explicit_table_id = TableId::from(233);
        let default_table_id = TableId::from(234);
        let excluded_table_id = TableId::from(235);
        let serving_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
        let captured_context = Arc::new(Mutex::new(None::<CacheRefillContext>));
        let captured_context_clone = captured_context.clone();
        let spawn_refill_task: SpawnRefillTask = Arc::new(move |_, context, _, _| {
            *captured_context_clone.lock() = Some(context);
            tokio::spawn(async {})
        });
        let mut refiller = CacheRefiller::new(
            Role::Both,
            test_refill_config(CacheRefillPolicy::Enabled),
            mock_sstable_store().await,
            spawn_refill_task,
        );

        refiller.replace_table_cache_refill_policies(HashMap::from([
            (explicit_table_id, CacheRefillPolicy::Serving),
            (excluded_table_id, CacheRefillPolicy::Serving),
        ]));
        refiller.update_streaming_table_vnodes(
            default_table_id,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
        );
        refiller.replace_serving_table_vnode_mapping(HashMap::from([
            (explicit_table_id, serving_vnodes.clone()),
            (excluded_table_id, serving_vnodes.clone()),
        ]));

        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: vec![SstableInfo::from(SstableInfoInner {
                    table_ids: vec![explicit_table_id, default_table_id],
                    ..Default::default()
                })],
                ..Default::default()
            }],
            pinned_version_for_test(),
            pinned_version_for_test(),
        );

        let captured_context = captured_context.lock();
        let table_cache_refill_context_map = &captured_context
            .as_ref()
            .unwrap()
            .table_cache_refill_context_map;
        let explicit_context = table_cache_refill_context_map
            .get(&explicit_table_id)
            .unwrap();
        assert_eq!(explicit_context.policy, CacheRefillPolicy::Serving);
        assert_eq!(
            explicit_context.serving_vnode_bitmap.as_ref(),
            Some(&serving_vnodes)
        );

        let default_context = table_cache_refill_context_map
            .get(&default_table_id)
            .unwrap();
        assert_eq!(default_context.policy, CacheRefillPolicy::Enabled);
        assert!(default_context.streaming_vnode_bitmap.is_none());
        assert!(default_context.serving_vnode_bitmap.is_none());
        assert!(!table_cache_refill_context_map.contains_key(&excluded_table_id));
    }

    #[tokio::test]
    async fn test_l0_normal_data_refill_applies_table_cache_refill_policy() {
        let table_id = TableId::from(233);
        let sstable_store = mock_sstable_store().await;
        let (sst, sst_info) = gen_test_sstable_with_table_ids(
            default_builder_opt_for_test(),
            1,
            (0..2).map(|idx| {
                (
                    FullKey {
                        user_key: risingwave_hummock_sdk::key::UserKey::for_test(
                            table_id,
                            iterator_test_table_key_of(idx),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(vec![idx as u8]),
                )
            }),
            sstable_store.clone(),
            vec![table_id.as_raw_id()],
        )
        .await;
        let sst_object_id = sst_info.object_id;
        let build_context = |policy| {
            let mut config = test_refill_config(policy);
            config.data_refill_levels.insert(0);
            CacheRefillContext {
                config: Arc::new(config),
                meta_refill_concurrency: None,
                concurrency: Arc::new(tokio::sync::Semaphore::new(1)),
                sstable_store: sstable_store.clone(),
                table_cache_refill_context_map: Arc::new(HashMap::from([(
                    table_id,
                    super::TableCacheRefillContext {
                        streaming_vnode_bitmap: None,
                        serving_vnode_bitmap: None,
                        policy,
                    },
                )])),
            }
        };
        let delta = SstDeltaInfo {
            insert_sst_infos: vec![sst_info],
            delete_sst_object_ids: vec![sst_object_id],
            insert_sst_level: 0,
        };
        let enabled_context = build_context(CacheRefillPolicy::Enabled);
        let enabled_tasks = DataCacheRefillTaskGenerator {
            context: &enabled_context,
            delta: &delta,
            ssts: std::slice::from_ref(&sst),
        }
        .generate()
        .await;
        assert!(
            !enabled_tasks.is_empty(),
            "L0 refill should reach task generation before the table policy filter"
        );

        let disabled_context = build_context(CacheRefillPolicy::Disabled);
        let disabled_tasks = DataCacheRefillTaskGenerator {
            context: &disabled_context,
            delta: &delta,
            ssts: std::slice::from_ref(&sst),
        }
        .generate()
        .await;

        assert!(
            disabled_tasks.is_empty(),
            "L0 should skip inheritance filter but still apply table cache refill policy"
        );
    }

    #[test]
    fn test_data_cache_refill_path_classifies_delta_shape() {
        let table_id = TableId::from(233);
        let sst_info = SstableInfo::from(SstableInfoInner {
            object_id: 233.into(),
            table_ids: vec![table_id],
            ..Default::default()
        });

        assert_eq!(
            classify_data_cache_refill_path(&SstDeltaInfo {
                insert_sst_infos: vec![],
                delete_sst_object_ids: vec![],
                insert_sst_level: 0,
            }),
            None
        );
        assert_eq!(
            classify_data_cache_refill_path(&SstDeltaInfo {
                insert_sst_infos: vec![sst_info.clone()],
                delete_sst_object_ids: vec![],
                insert_sst_level: 1,
            }),
            None
        );
        assert_eq!(
            classify_data_cache_refill_path(&SstDeltaInfo {
                insert_sst_infos: vec![sst_info.clone()],
                delete_sst_object_ids: vec![],
                insert_sst_level: 0,
            }),
            Some(DataCacheRefillPath::L0InsertOnly)
        );
        assert_eq!(
            classify_data_cache_refill_path(&SstDeltaInfo {
                insert_sst_infos: vec![sst_info.clone()],
                delete_sst_object_ids: vec![sst_info.object_id],
                insert_sst_level: 0,
            }),
            Some(DataCacheRefillPath::Normal)
        );
    }

    #[tokio::test]
    async fn test_l0_insert_only_refill_requires_serving_ownership() {
        let table_id = TableId::from(233);
        let sstable_store = mock_sstable_store().await;
        let (sst, sst_info) = gen_test_sstable_with_table_ids(
            default_builder_opt_for_test(),
            1,
            (0..2).map(|idx| {
                (
                    FullKey {
                        user_key: risingwave_hummock_sdk::key::UserKey::for_test(
                            table_id,
                            iterator_test_table_key_of(idx),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(vec![idx as u8]),
                )
            }),
            sstable_store.clone(),
            vec![table_id.as_raw_id()],
        )
        .await;
        let serving_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
        let build_context = |policy, streaming_vnode_bitmap, serving_vnode_bitmap| {
            let mut config = test_refill_config(policy);
            config.data_refill_levels.insert(0);
            CacheRefillContext {
                config: Arc::new(config),
                meta_refill_concurrency: None,
                concurrency: Arc::new(tokio::sync::Semaphore::new(1)),
                sstable_store: sstable_store.clone(),
                table_cache_refill_context_map: Arc::new(HashMap::from([(
                    table_id,
                    super::TableCacheRefillContext {
                        streaming_vnode_bitmap,
                        serving_vnode_bitmap,
                        policy,
                    },
                )])),
            }
        };
        let delta = SstDeltaInfo {
            insert_sst_infos: vec![sst_info],
            delete_sst_object_ids: vec![],
            insert_sst_level: 0,
        };

        let enabled_without_serving = build_context(CacheRefillPolicy::Enabled, None, None);
        let enabled_without_serving_tasks = DataCacheRefillTaskGenerator {
            context: &enabled_without_serving,
            delta: &delta,
            ssts: std::slice::from_ref(&sst),
        }
        .generate()
        .await;
        assert!(
            enabled_without_serving_tasks.is_empty(),
            "default Enabled must not unconditionally refill L0 insert-only SSTs"
        );

        let streaming_context = build_context(
            CacheRefillPolicy::Streaming,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            None,
        );
        let streaming_tasks = DataCacheRefillTaskGenerator {
            context: &streaming_context,
            delta: &delta,
            ssts: std::slice::from_ref(&sst),
        }
        .generate()
        .await;
        assert!(
            streaming_tasks.is_empty(),
            "pure streaming L0 insert-only SSTs are already warmed by the uploader"
        );

        let enabled_serving_context = build_context(
            CacheRefillPolicy::Enabled,
            None,
            Some(serving_vnodes.clone()),
        );
        let enabled_serving_tasks = DataCacheRefillTaskGenerator {
            context: &enabled_serving_context,
            delta: &delta,
            ssts: std::slice::from_ref(&sst),
        }
        .generate()
        .await;
        assert!(
            !enabled_serving_tasks.is_empty(),
            "Enabled is table-level allow, but L0 insert-only still needs serving ownership"
        );
    }

    #[tokio::test]
    async fn test_l0_insert_only_block_refill_decision_uses_serving_ownership() {
        let table_id = TableId::from(233);
        let sstable_store = mock_sstable_store().await;
        let (sst, _) = gen_test_sstable_with_table_ids(
            default_builder_opt_for_test(),
            1,
            (0..2).map(|idx| {
                (
                    FullKey {
                        user_key: risingwave_hummock_sdk::key::UserKey::for_test(
                            table_id,
                            iterator_test_table_key_of(idx),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(vec![idx as u8]),
                )
            }),
            sstable_store,
            vec![table_id.as_raw_id()],
        )
        .await;
        let build_context =
            |policy, streaming_vnode_bitmap, serving_vnode_bitmap| super::TableCacheRefillContext {
                streaming_vnode_bitmap,
                serving_vnode_bitmap,
                policy,
            };

        let cases = [
            (
                "Enabled + serving overlap",
                build_context(
                    CacheRefillPolicy::Enabled,
                    None,
                    Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                ),
                true,
            ),
            (
                "Enabled + streaming overlap only",
                build_context(
                    CacheRefillPolicy::Enabled,
                    Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                    None,
                ),
                false,
            ),
            (
                "Serving + serving overlap",
                build_context(
                    CacheRefillPolicy::Serving,
                    None,
                    Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                ),
                true,
            ),
            (
                "Streaming + streaming overlap",
                build_context(
                    CacheRefillPolicy::Streaming,
                    Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                    None,
                ),
                false,
            ),
            (
                "Both + streaming overlap + serving empty",
                build_context(
                    CacheRefillPolicy::Both,
                    Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                    Some(Bitmap::zeros(VirtualNode::COUNT_FOR_TEST)),
                ),
                false,
            ),
            (
                "Both + serving overlap",
                build_context(
                    CacheRefillPolicy::Both,
                    Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                    Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                ),
                true,
            ),
            (
                "Disabled + serving overlap",
                build_context(
                    CacheRefillPolicy::Disabled,
                    None,
                    Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                ),
                false,
            ),
        ];

        for (name, context, expected) in cases {
            assert_eq!(
                context.allows_insert_only_data_refill_block(&sst, 0),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn test_vnode_range_overlaps_bitmap_uses_right_exclusive_end() {
        let right_exclusive = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [12]);
        assert!(!vnode_range_overlaps_bitmap((10, 12), &right_exclusive));

        let inside_range = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [11]);
        assert!(vnode_range_overlaps_bitmap((10, 12), &inside_range));

        let last_vnode = Bitmap::from_indices(
            VirtualNode::COUNT_FOR_TEST,
            [VirtualNode::COUNT_FOR_TEST - 1],
        );
        assert!(vnode_range_overlaps_bitmap(
            (VirtualNode::COUNT_FOR_TEST - 1, VirtualNode::COUNT_FOR_TEST),
            &last_vnode
        ));
        assert!(!vnode_range_overlaps_bitmap(
            (VirtualNode::COUNT_FOR_TEST, VirtualNode::COUNT_FOR_TEST + 1),
            &last_vnode
        ));
    }
}
