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

use std::collections::hash_map::{Entry as HashMapEntry, HashMap};
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
use parking_lot::{RwLock, RwLockReadGuard};
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
use risingwave_pb::meta::subscribe_response::Operation;
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::hummock::event_handler::ReadVersionMappingType;
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
            unit: options.cache_refill_unit,
            threshold: options.cache_refill_threshold,
            skip_recent_filter: options.cache_refill_skip_recent_filter,
            skip_inheritance_filter: options.cache_refill_skip_inheritance_filter,
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

#[derive(Debug, Default)]
struct TableCacheRefillVnodes {
    streaming: HashMap<TableId, Bitmap>,
    serving: HashMap<TableId, Bitmap>,
}

impl TableCacheRefillVnodes {
    /// Check whether the given sstable block should be refilled according to the vnode mapping.
    fn check_table_refill_vnodes(&self, sstable: &Sstable, block_index: usize) -> bool {
        let block_smallest_key =
            FullKey::decode(&sstable.meta.block_metas[block_index].smallest_key)
                .to_vec()
                .into_bytes();
        let block_largest_key = FullKey::decode(
            sstable
                .meta
                .block_metas
                .get(block_index + 1)
                .map(|b| &b.smallest_key)
                .unwrap_or_else(|| &sstable.meta.largest_key),
        )
        .to_vec()
        .into_bytes();

        let table_id = block_smallest_key.user_key.table_id;
        let table_key_range = (
            Bound::Included(block_smallest_key.user_key.table_key),
            Bound::Excluded(block_largest_key.user_key.table_key),
        );
        let vnode_range = vnode_range(&table_key_range);

        let streaming_bitmap = self.streaming.get(&table_id);
        let serving_bitmap = self.serving.get(&table_id);

        let num_bits = streaming_bitmap
            .map(|b| b.len())
            .or(serving_bitmap.map(|b| b.len()))
            .unwrap_or(0);
        if num_bits == 0 {
            return false;
        }
        let bitmap = Bitmap::from_range(num_bits, vnode_range.0..vnode_range.1 + 1);
        if let Some(streaming_bitmap) = streaming_bitmap
            && (&bitmap & streaming_bitmap).any()
        {
            return true;
        }
        if let Some(serving_bitmap) = serving_bitmap
            && (&bitmap & serving_bitmap).any()
        {
            return true;
        }
        false
    }
}

/// A cache refiller for hummock data.
pub(crate) struct CacheRefiller {
    /// order: old => new
    queue: VecDeque<Item>,

    context: CacheRefillContext,

    spawn_refill_task: SpawnRefillTask,

    role: Role,
    table_cache_refill_policies: HashMap<TableId, CacheRefillPolicy>,
    table_cache_refill_vnodes: Arc<RwLock<TableCacheRefillVnodes>>,
    read_version_mapping: Arc<RwLock<ReadVersionMappingType>>,
    serving_table_vnode_mapping: HashMap<TableId, Bitmap>,
}

impl CacheRefiller {
    pub(crate) fn new(
        role: Role,
        config: CacheRefillConfig,
        sstable_store: SstableStoreRef,
        spawn_refill_task: SpawnRefillTask,
        read_version_mapping: Arc<RwLock<ReadVersionMappingType>>,
    ) -> Self {
        let config = Arc::new(config);
        let concurrency = Arc::new(Semaphore::new(config.concurrency));
        let table_cache_refill_vnodes = Arc::new(RwLock::new(TableCacheRefillVnodes::default()));
        Self {
            queue: VecDeque::new(),
            context: CacheRefillContext {
                config,
                concurrency,
                sstable_store,
                table_cache_refill_vnodes: table_cache_refill_vnodes.clone(),
            },
            spawn_refill_task,
            role,
            table_cache_refill_policies: HashMap::new(),
            table_cache_refill_vnodes,
            read_version_mapping,
            serving_table_vnode_mapping: HashMap::new(),
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

    pub(crate) fn update_table_cache_refill_policies(
        &mut self,
        policies: HashMap<TableId, CacheRefillPolicy>,
    ) {
        for (table_id, policy) in policies {
            if policy == CacheRefillPolicy::Default {
                self.table_cache_refill_policies.remove(&table_id);
            } else {
                self.table_cache_refill_policies.insert(table_id, policy);
            }
            self.update_table_cache_refill_vnodes(table_id);
        }
    }

    pub(crate) fn update_serving_table_vnode_mapping(
        &mut self,
        op: Operation,
        mapping: HashMap<TableId, Bitmap>,
    ) {
        match op {
            Operation::Snapshot => {
                self.serving_table_vnode_mapping = mapping.clone();
                for table_id in mapping.keys() {
                    self.update_table_cache_refill_vnodes(*table_id);
                }
            }
            Operation::Update => {
                for (table_id, bitmap) in mapping {
                    match self.serving_table_vnode_mapping.entry(table_id) {
                        HashMapEntry::Occupied(mut o) => *o.get_mut() |= bitmap,
                        HashMapEntry::Vacant(v) => {
                            v.insert(bitmap);
                        }
                    }
                    self.update_table_cache_refill_vnodes(table_id);
                }
            }
            Operation::Delete => {
                for table_id in mapping.keys() {
                    self.serving_table_vnode_mapping.remove(table_id);
                    self.update_table_cache_refill_vnodes(*table_id);
                }
            }
            _ => unreachable!(),
        }
    }

    pub(crate) fn update_table_cache_refill_vnodes(&self, table_id: TableId) {
        let policy = self
            .table_cache_refill_policies
            .get(&table_id)
            .copied()
            .unwrap_or_default();
        let mut table_cache_refill_vnodes = self.table_cache_refill_vnodes.write();

        table_cache_refill_vnodes.streaming.remove(&table_id);
        table_cache_refill_vnodes.serving.remove(&table_id);

        if self.role.for_streaming()
            && policy.for_streaming()
            && let Some(mapping) = self.read_version_mapping.read().get(&table_id)
        {
            for read_version in mapping.values() {
                let vnodes = read_version.read().vnodes();
                table_cache_refill_vnodes
                    .streaming
                    .entry(table_id)
                    .and_modify(|bitmap| *bitmap |= vnodes.as_ref())
                    .or_insert_with(|| vnodes.as_ref().clone());
            }
        }

        if self.role.for_serving()
            && policy.for_serving()
            && let Some(vnodes) = self.serving_table_vnode_mapping.get(&table_id)
        {
            table_cache_refill_vnodes
                .serving
                .insert(table_id, vnodes.clone());
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
    concurrency: Arc<Semaphore>,
    sstable_store: SstableStoreRef,
    table_cache_refill_vnodes: Arc<RwLock<TableCacheRefillVnodes>>,
}

struct DataCacheRefillTaskGenerator<'a> {
    context: &'a CacheRefillContext,
    delta: &'a SstDeltaInfo,
    ssts: &'a [TableHolder],
}

impl DataCacheRefillTaskGenerator<'_> {
    async fn generate(&self) -> Vec<DataCacheRefillTask> {
        let mut tasks = Vec::new();

        // Skip data cache refill if data disk cache is not enabled.
        if !self.context.sstable_store.block_cache().is_hybrid() {
            return tasks;
        }

        // Return if no data to refill.
        if self.delta.insert_sst_infos.is_empty() || self.delta.delete_sst_object_ids.is_empty() {
            return tasks;
        }

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
        if !self.context.config.skip_recent_filter {
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

        if self.delta.insert_sst_level != 0 {
            // Filter by inheritance filter, also requires recent filter enabled.
            if !self.context.config.skip_recent_filter
                && !self.context.config.skip_inheritance_filter
            {
                tasks = self.filter_by_inheritance_filter(tasks).await;
            }

            // Filter by table cache refill vnodes.
            tasks = self.filter_by_table_cache_refill_vnodes(tasks);
        }

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

        #[expect(clippy::mutable_key_type)]
        let mut filtered: HashSet<DataCacheRefillTask> = HashSet::default();
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
                    if filtered.contains(task) {
                        continue;
                    }
                    if recent_filter.contains(&(psst.id, pblk)) {
                        filtered.insert(task.clone());
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

        filtered.into_iter().collect()
    }

    fn filter_by_table_cache_refill_vnodes(
        &self,
        mut tasks: Vec<DataCacheRefillTask>,
    ) -> Vec<DataCacheRefillTask> {
        let table_cache_refill_vnodes = self.context.table_cache_refill_vnodes.read();
        let check = |vnodes: &RwLockReadGuard<'_, TableCacheRefillVnodes>,
                     task: &DataCacheRefillTask| {
            for blk in task.blks.start..task.blks.end {
                if vnodes.check_table_refill_vnodes(&task.sst, blk) {
                    return true;
                }
            }
            false
        };
        tasks.retain(|task| check(&table_cache_refill_vnodes, task));
        tasks
    }
}

#[derive(Debug, Clone)]
struct DataCacheRefillTask {
    sst: TableHolder,
    blks: Range<usize>,
}

impl PartialEq for DataCacheRefillTask {
    fn eq(&self, other: &Self) -> bool {
        self.sst.id == other.sst.id && self.blks == other.blks
    }
}

impl Eq for DataCacheRefillTask {}

impl Hash for DataCacheRefillTask {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.sst.id.hash(state);
        self.blks.hash(state);
    }
}

impl DataCacheRefillTask {
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

                let now = Instant::now();
                let res = context.sstable_store.sstable(info, &mut stats).await;
                stats.discard();
                GLOBAL_CACHE_REFILL_METRICS
                    .meta_refill_success_duration
                    .observe(now.elapsed().as_secs_f64());
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

        let handles = futures
            .into_iter()
            .map(|future| {
                tokio::spawn(async move {
                    if let Err(e) = future.await {
                        tracing::error!(error = %e.as_report(), "data cache refill task error");
                    }
                })
            })
            .collect_vec();

        join_all(handles).await;
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
