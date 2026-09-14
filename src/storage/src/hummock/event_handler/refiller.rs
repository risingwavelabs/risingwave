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

use std::collections::HashSet;
use std::collections::hash_map::HashMap;
use std::future::poll_fn;
use std::hash::Hash;
use std::ops::Range;
use std::sync::{Arc, LazyLock};
use std::task::Poll;
use std::time::{Duration, Instant};

use foyer::RangeBoundsExt;
use futures::future::{join_all, try_join_all};
use futures::{Future, FutureExt};
use itertools::Itertools;
use prometheus::core::{AtomicU64, GenericCounter, GenericCounterVec};
use prometheus::{
    Histogram, HistogramVec, IntGauge, Registry, exponential_buckets, histogram_opts,
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
    register_int_gauge_with_registry,
};
use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::Role;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::license::Feature;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::{HummockSstableObjectId, KeyComparator};
use risingwave_pb::id::TableId;
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::local_version::recent_versions::RecentVersions;
use crate::hummock::pin_cache_refill::{
    PinCacheMembershipUpdate, PinCacheRefillController, PinCacheRefillPlan,
};
use crate::hummock::refill_locality::{block_vnode_range, vnode_range_overlaps_bitmap};
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
    pub refill_version_batch_size: HistogramVec,

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
        let refill_version_batch_size = register_histogram_vec_with_registry!(
            histogram_opts!(
                "refill_version_batch_size",
                "Number of ordered version events released in one cache-refill batch",
                exponential_buckets(1.0, 2.0, 11).unwrap(),
            ),
            &["outcome"],
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
            refill_version_batch_size,

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

    /// Maximum version events merged into one Pin Cache refill batch.
    pub pin_cache_max_batch_size: usize,

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

    /// Skip inheritance filter.
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
            pin_cache_max_batch_size: options.cache_refill_pin_cache_max_batch_size.max(1),
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
    foyer_handle: JoinHandle<bool>,
    pin_plan: PinCacheRefillPlan,
    event: CacheRefillerEvent,
    received_at: tokio::time::Instant,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RefillBatchOutcome {
    Ready,
    DegradedTimeout,
    DegradedError,
    DegradedPressure,
}

struct ActiveBatch {
    handle: JoinHandle<RefillBatchOutcome>,
    events: Vec<CacheRefillerEvent>,
    oldest_received_at: tokio::time::Instant,
    pin_objects: u64,
    pin_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct PinRefillPlanStats {
    objects: u64,
    bytes: u64,
}

impl PinRefillPlanStats {
    fn add_plan(&mut self, plan: &PinCacheRefillPlan) {
        self.objects += plan.objects.len() as u64;
        self.bytes += plan
            .objects
            .values()
            .filter_map(|projections| projections.first())
            .map(|info| info.file_size)
            .sum::<u64>();
    }
}

pub(crate) struct CacheRefillPlan {
    deltas: Vec<SstDeltaInfo>,
}

pub(crate) type SpawnRefillTask = Arc<
    // first current version, second new version
    dyn Fn(CacheRefillPlan, CacheRefillContext, PinnedVersion, PinnedVersion) -> JoinHandle<bool>
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

/// A cache refiller for hummock data.
pub(crate) struct CacheRefiller {
    // One release gate. Only execution plans are compacted; metadata events stay ordered.
    active: Option<ActiveBatch>,
    pending: Vec<Item>,
    last_outcome: Option<RefillBatchOutcome>,

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
    pin_cache_refill: PinCacheRefillController,
}

impl CacheRefiller {
    pub(crate) fn new(
        role: Role,
        config: CacheRefillConfig,
        sstable_store: SstableStoreRef,
        spawn_refill_task: SpawnRefillTask,
        pin_cache_version: PinnedVersion,
    ) -> Self {
        let config = Arc::new(config);
        let concurrency = Arc::new(Semaphore::new(config.concurrency));
        let default_policy = config.table_cache_refill_default_policy;
        let pin_concurrency = concurrency.clone();
        let meta_refill_concurrency = if config.meta_refill_concurrency == 0 {
            None
        } else {
            Some(Arc::new(Semaphore::new(config.meta_refill_concurrency)))
        };
        Self {
            active: None,
            pending: Vec::new(),
            last_outcome: None,
            spawn_refill_task,
            config,
            meta_refill_concurrency,
            concurrency,
            pin_cache_refill: PinCacheRefillController::new(
                sstable_store.clone(),
                pin_cache_version,
                pin_concurrency,
            ),
            sstable_store,
            role,
            default_policy,
            table_cache_refill_policies: HashMap::new(),
            streaming_table_vnode_mapping: HashMap::new(),
            serving_table_vnode_mapping: HashMap::new(),
        }
    }

    pub(crate) fn default_spawn_refill_task() -> SpawnRefillTask {
        Arc::new(|plan, context, _, _| {
            let timeout = context.config.timeout;
            let task = CacheRefillTask { plan, context };
            tokio::spawn(async move {
                tokio::time::timeout(timeout, task.run())
                    .await
                    .unwrap_or(false)
            })
        })
    }

    pub(crate) fn start_cache_refill(
        &mut self,
        mut deltas: Vec<SstDeltaInfo>,
        pinned_version: PinnedVersion,
        new_pinned_version: PinnedVersion,
        pin_cache_membership_update: PinCacheMembershipUpdate,
    ) {
        if let Some(cache) = self.sstable_store.pin_cache() {
            cache.start_version_update(new_pinned_version.id());
        }
        // Capture pin admission with this delta. A later SET must not turn an already-running
        // refill into an implicit warm, while `PinCache::pin_sst` still rechecks current desired
        // membership before reserving or publishing.
        let pin_cache_refill_object_ids = self.pin_cache_refill.apply_version_update(
            &deltas,
            new_pinned_version.clone(),
            pin_cache_membership_update,
        );
        let pin_cache_refill = match pin_cache_membership_update {
            // A full snapshot can arrive after initial ownership. Its existing SSTs belong
            // to the same release gate, not an independent background bootstrap.
            PinCacheMembershipUpdate::Rebuild => self.pin_cache_refill.live_objects_plan(false),
            PinCacheMembershipUpdate::Delta => PinCacheRefillPlan::new(
                &deltas,
                &pin_cache_refill_object_ids,
                self.pin_cache_owned_vnodes(),
            ),
        };
        // A physical object can contain both pinned and ordinary tables. Preserve it here;
        // the immutable per-table Foyer context excludes only its pinned projection.
        for delta in &mut deltas {
            let for_serving = self.role.for_serving();
            // Writer-appended L0 SSTs are already warm on the streaming side. Their data refill
            // may therefore only be needed by serving workers.
            let for_streaming = self.role.for_streaming() && !delta.delete_sst_infos.is_empty();
            if !for_serving && !for_streaming {
                delta.insert_sst_infos.clear();
                continue;
            }

            // This is deliberately a whole-SST admission check before Meta load. The serving
            // mapping key set identifies result tables, but bitmap contents remain for the exact
            // block/vnode decision in DataCacheRefillTaskGenerator after Meta load. An SST stays
            // when any contained table matches either the serving or streaming refill lane.
            delta.insert_sst_infos.retain(|sst| {
                sst.table_ids.iter().any(|table_id| {
                    // A missing entry means there is no table override, not that the table is
                    // absent. Preserve the configured legacy/default policy in that case.
                    let policy = self
                        .table_cache_refill_policies
                        .get(table_id)
                        .copied()
                        .unwrap_or(self.default_policy);

                    // Enabled preserves legacy full refill. For scoped policies, mapping keys are
                    // only a whole-SST coarse gate; bitmap bits still filter blocks post-Meta.
                    match policy {
                        CacheRefillPolicy::Enabled => for_streaming || for_serving,
                        CacheRefillPolicy::Disabled => false,
                        CacheRefillPolicy::Streaming => {
                            for_streaming
                                && self.streaming_table_vnode_mapping.contains_key(table_id)
                        }
                        CacheRefillPolicy::Serving => {
                            for_serving && self.serving_table_vnode_mapping.contains_key(table_id)
                        }
                        CacheRefillPolicy::Both => {
                            (for_streaming
                                && self.streaming_table_vnode_mapping.contains_key(table_id))
                                || (for_serving
                                    && self.serving_table_vnode_mapping.contains_key(table_id))
                        }
                        CacheRefillPolicy::Pinned => false,
                    }
                })
            });
        }
        let context = self.new_cache_refill_context(&deltas);
        let plan = CacheRefillPlan { deltas };
        // Preserve the main-path behavior: Foyer refill starts when the delta arrives. Only the
        // Pin whole-SST plan waits in the merge queue.
        let foyer_handle = (self.spawn_refill_task)(
            plan,
            context,
            pinned_version.clone(),
            new_pinned_version.clone(),
        );
        let event = CacheRefillerEvent {
            pinned_version,
            new_pinned_version,
        };
        let item = Item {
            foyer_handle,
            pin_plan: pin_cache_refill,
            event,
            received_at: tokio::time::Instant::now(),
        };
        self.pending.push(item);
        GLOBAL_CACHE_REFILL_METRICS.refill_queue_total.add(1);
        if self.active.is_none() {
            self.activate_pending();
        }
    }

    fn activate_pending(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        let batch_size = self
            .config
            .pin_cache_max_batch_size
            .max(1)
            .min(self.pending.len());
        let items = self.pending.drain(..batch_size).collect::<Vec<_>>();
        let oldest_received_at = items[0].received_at;
        let deadline = oldest_received_at + self.config.timeout;
        // Keep the commit snapshots that RecentVersions may retain, as well as the final target.
        // Compaction-only intermediate outputs never exposed outside this batch can be omitted.
        let candidate_objects = items
            .iter()
            .flat_map(|item| item.pin_plan.objects.keys().copied())
            .collect::<HashSet<_>>();
        let mut required_objects = HashSet::new();
        if !candidate_objects.is_empty() {
            for (index, item) in items.iter().enumerate() {
                if index + 1 == items.len()
                    || RecentVersions::has_table_committed(
                        &item.event.pinned_version,
                        &item.event.new_pinned_version,
                    )
                {
                    for levels in item.event.new_pinned_version.levels.values() {
                        for sst in levels
                            .l0
                            .sub_levels
                            .iter()
                            .chain(&levels.levels)
                            .flat_map(|level| &level.table_infos)
                            .filter(|sst| candidate_objects.contains(&sst.object_id))
                        {
                            required_objects.insert(sst.object_id);
                        }
                    }
                }
            }
        }
        let mut pin_plans: HashMap<HummockSstableObjectId, PinCacheRefillPlan> = HashMap::new();
        let mut input_pin_plan = PinRefillPlanStats::default();
        let mut retained_pin_plan = PinRefillPlanStats::default();
        let mut foyer_handles = Vec::new();
        let mut events = Vec::new();
        for mut item in items {
            input_pin_plan.add_plan(&item.pin_plan);
            item.pin_plan
                .objects
                .retain(|object, _| required_objects.contains(object));
            retained_pin_plan.add_plan(&item.pin_plan);
            for (object, infos) in item.pin_plan.objects {
                let plan = pin_plans.entry(object).or_default();
                let projections = plan.objects.entry(object).or_default();
                for info in infos {
                    if !projections
                        .iter()
                        .any(|existing| existing.sst_id == info.sst_id)
                    {
                        projections.push(info);
                    }
                }
                for (&table, bitmap) in item.pin_plan.ownership.iter() {
                    Arc::make_mut(&mut plan.ownership)
                        .entry(table)
                        .and_modify(|owned| *owned |= bitmap)
                        .or_insert_with(|| bitmap.clone());
                }
            }
            foyer_handles.push(item.foyer_handle);
            events.push(item.event);
        }
        // Input and retained count per-event candidates. Submitted counts the final
        // cross-event-deduplicated physical plans sent to the executor.
        let mut submitted_pin_plan = PinRefillPlanStats::default();
        for plan in pin_plans.values() {
            submitted_pin_plan.add_plan(plan);
        }
        for (stage, stats) in [
            ("input", input_pin_plan),
            ("retained", retained_pin_plan),
            ("submitted", submitted_pin_plan),
        ] {
            GLOBAL_CACHE_REFILL_METRICS
                .refill_total
                .with_label_values(&["pin_plan", stage])
                .inc_by(stats.objects);
            GLOBAL_CACHE_REFILL_METRICS
                .refill_bytes
                .with_label_values(&["pin_plan", stage])
                .inc_by(stats.bytes);
        }
        let tickets = pin_plans
            .into_values()
            .map(|plan| self.pin_cache_refill.submit(plan))
            .collect::<Vec<_>>();
        let handle = tokio::spawn(async move {
            let mut foyer_handles = scopeguard::guard(foyer_handles, |handles| {
                for handle in handles {
                    handle.abort();
                }
            });
            match tokio::time::timeout_at(deadline, async move {
                // The executor owns uploads. Dropping deadline-bound tickets never cancels I/O.
                let pin_ready = join_all(tickets.into_iter().map(|ticket| ticket.wait()))
                    .await
                    .into_iter()
                    .all(|ready| ready);
                let foyer_ready = join_all(foyer_handles.iter_mut())
                    .await
                    .into_iter()
                    .all(|result| matches!(result, Ok(true)));
                pin_ready && foyer_ready
            })
            .await
            {
                Ok(true) => RefillBatchOutcome::Ready,
                Ok(false) => RefillBatchOutcome::DegradedError,
                Err(_) => RefillBatchOutcome::DegradedTimeout,
            }
        });
        self.active = Some(ActiveBatch {
            handle,
            events,
            oldest_received_at,
            pin_objects: submitted_pin_plan.objects,
            pin_bytes: submitted_pin_plan.bytes,
        });
    }

    pub(crate) fn on_version_applied(&self, version: risingwave_hummock_sdk::HummockVersionId) {
        self.pin_cache_refill.on_version_applied(version);
    }

    fn pin_cache_owned_vnodes(&self) -> HashMap<TableId, Bitmap> {
        let mut owned = HashMap::new();
        for (&table_id, policy) in &self.table_cache_refill_policies {
            if !policy.is_pinned() {
                continue;
            }
            for mapping in [
                self.role
                    .for_streaming()
                    .then_some(&self.streaming_table_vnode_mapping),
                self.role
                    .for_serving()
                    .then_some(&self.serving_table_vnode_mapping),
            ]
            .into_iter()
            .flatten()
            {
                if let Some(bitmap) = mapping.get(&table_id).filter(|bitmap| bitmap.any()) {
                    owned
                        .entry(table_id)
                        .and_modify(|existing: &mut Bitmap| *existing |= bitmap)
                        .or_insert_with(|| bitmap.clone());
                }
            }
        }
        owned
    }

    fn new_cache_refill_context(&self, deltas: &[SstDeltaInfo]) -> CacheRefillContext {
        let table_ids = deltas.iter().flat_map(|delta| {
            delta
                .insert_sst_infos
                .iter()
                .flat_map(|sst| sst.table_ids.iter().copied())
        });
        CacheRefillContext {
            config: self.config.clone(),
            meta_refill_concurrency: self.meta_refill_concurrency.clone(),
            concurrency: self.concurrency.clone(),
            sstable_store: self.sstable_store.clone(),
            table_cache_refill_context_map: Arc::new(self.table_cache_refill_contexts(table_ids)),
        }
    }

    pub(crate) fn last_new_pinned_version(&self) -> Option<&PinnedVersion> {
        self.pending
            .last()
            .map(|item| &item.event.new_pinned_version)
            .or_else(|| {
                self.active
                    .as_ref()
                    .and_then(|batch| batch.events.last())
                    .map(|event| &event.new_pinned_version)
            })
    }

    /// Replaces the complete policy snapshot applicable to this worker.
    pub(crate) fn replace_table_cache_refill_policies(
        &mut self,
        policies: HashMap<TableId, CacheRefillPolicy>,
    ) {
        let pinned_tables = policies
            .iter()
            .filter_map(|(&table, policy)| policy.is_pinned().then_some(table))
            .collect();
        for item in &mut self.pending {
            // Pending admission is monotonic: RESET can revoke it, while a later SET must not
            // turn an already-observed delta into an implicit Warm.
            item.pin_plan.retain_tables(&pinned_tables);
        }
        let initial = self.pin_cache_refill.replace_policies(&policies);
        self.table_cache_refill_policies = policies;
        self.pin_cache_refill
            .update_ownership(self.pin_cache_owned_vnodes(), initial);
    }

    /// Replaces the complete serving vnode mapping snapshot.
    pub(crate) fn replace_serving_table_vnode_mapping(
        &mut self,
        mapping: HashMap<TableId, Bitmap>,
    ) {
        self.serving_table_vnode_mapping = mapping;
        self.pin_cache_refill
            .update_ownership(self.pin_cache_owned_vnodes(), true);
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
        self.pin_cache_refill
            .update_ownership(self.pin_cache_owned_vnodes(), true);
    }

    fn table_cache_refill_contexts(
        &self,
        table_ids: impl IntoIterator<Item = TableId>,
    ) -> TableCacheRefillContextMap {
        let for_streaming = self.role.for_streaming();
        let for_serving = self.role.for_serving();
        table_ids
            .into_iter()
            .filter_map(|table_id| {
                let policy = self
                    .table_cache_refill_policies
                    .get(&table_id)
                    .copied()
                    .unwrap_or(self.default_policy);
                if policy.is_pinned()
                    || (for_serving
                        && !for_streaming
                        && !self.serving_table_vnode_mapping.contains_key(&table_id))
                {
                    return None;
                }
                let streaming_vnode_bitmap = (for_streaming && policy.is_streaming_scoped())
                    .then(|| self.streaming_table_vnode_mapping.get(&table_id).cloned())
                    .flatten();
                // `Enabled` normally does not use bitmap filtering. The only exception is L0
                // insert-only refill, where serving workers still need serving-locality evidence.
                let serving_vnode_bitmap = (for_serving
                    && (policy.is_serving_scoped() || policy.is_unscoped_enabled()))
                .then(|| self.serving_table_vnode_mapping.get(&table_id).cloned())
                .flatten();
                Some((
                    table_id,
                    TableCacheRefillContext {
                        streaming_vnode_bitmap,
                        serving_vnode_bitmap,
                        policy,
                    },
                ))
            })
            .collect()
    }

    pub(crate) fn table_cache_refill_monitor_snapshot(&self) -> TableCacheRefillMonitorSnapshot {
        let table_ids = self
            .table_cache_refill_policies
            .keys()
            .chain(self.streaming_table_vnode_mapping.keys())
            .chain(self.serving_table_vnode_mapping.keys())
            .copied();
        TableCacheRefillMonitorSnapshot {
            contexts: self.table_cache_refill_contexts(table_ids),
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
            if self.active.is_none() {
                self.activate_pending();
            }
            let Some(active) = &mut self.active else {
                return Poll::Pending;
            };
            let outcome = match active.handle.poll_unpin(cx) {
                Poll::Ready(result) => result.unwrap_or(RefillBatchOutcome::DegradedError),
                Poll::Pending if self.pending.len() >= 1024 => RefillBatchOutcome::DegradedPressure,
                Poll::Pending => return Poll::Pending,
            };
            let batch = self.active.take().unwrap();
            GLOBAL_CACHE_REFILL_METRICS
                .refill_queue_total
                .sub(batch.events.len() as i64);
            let label = match outcome {
                RefillBatchOutcome::Ready => "ready",
                RefillBatchOutcome::DegradedTimeout => "timeout",
                RefillBatchOutcome::DegradedError => "error",
                RefillBatchOutcome::DegradedPressure => "pressure",
            };
            GLOBAL_CACHE_REFILL_METRICS
                .refill_total
                .with_label_values(&["version", label])
                .inc();
            GLOBAL_CACHE_REFILL_METRICS
                .refill_duration
                .with_label_values(&["version", label])
                .observe(batch.oldest_received_at.elapsed().as_secs_f64());
            GLOBAL_CACHE_REFILL_METRICS
                .refill_version_batch_size
                .with_label_values(&[label])
                .observe(batch.events.len() as f64);
            if outcome != RefillBatchOutcome::Ready {
                tracing::warn!(
                    ?outcome,
                    batch_size = batch.events.len(),
                    pin_objects = batch.pin_objects,
                    pin_bytes = batch.pin_bytes,
                    elapsed_ms = batch.oldest_received_at.elapsed().as_millis(),
                    "publishing version batch with cache refill fallback"
                );
            }
            self.last_outcome = Some(outcome);
            Poll::Ready(batch.events)
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

impl DataCacheRefillTaskGenerator<'_> {
    fn generate_unfiltered_tasks(&self) -> Vec<DataCacheRefillTask> {
        let mut tasks = Vec::new();

        // Skip data cache refill if data disk cache is not enabled.
        if !self.context.sstable_store.block_cache().is_hybrid() {
            return tasks;
        }

        if self.delta.insert_sst_infos.is_empty() {
            return tasks;
        }

        let has_parent_ssts = !self.delta.delete_sst_infos.is_empty();
        // CN-written SSTs are appended to L0 without replacing parent SSTs. Other inserted SSTs
        // need delete-side evidence for recent and inheritance filtering.
        debug_assert!(has_parent_ssts || self.delta.insert_sst_level == 0);

        // Return if the target level is not in the refill levels
        if !self
            .context
            .config
            .data_refill_levels
            .contains(&self.delta.insert_sst_level)
        {
            return tasks;
        }

        // Cache refill units must not cross a table boundary. A logical SST projection still
        // decides whether to admit each single-table unit.
        let unit = self.context.config.unit;
        assert!(unit > 0, "cache refill unit must be positive");
        let table_cache_refill_context_map = &self.context.table_cache_refill_context_map;
        for (sst_info, sst) in self.delta.insert_sst_infos.iter().zip_eq_fast(self.ssts) {
            debug_assert_eq!(sst_info.object_id, sst.id);
            debug_assert!(sst_info.table_ids.is_sorted());
            let mut blk_start = 0;
            while blk_start < sst.block_count() {
                // SstableBuilder ends a block before the table ID changes, so block metadata
                // defines the exact physical boundary. `table_ids` below only admits logical
                // projections and must not make a unit span another table.
                let table_id = sst.meta.block_metas[blk_start].table_id();
                let mut blk_end = std::cmp::min(sst.block_count(), blk_start + unit);
                if let Some(table_boundary) = (blk_start + 1..blk_end)
                    .find(|&block_index| sst.meta.block_metas[block_index].table_id() != table_id)
                {
                    blk_end = table_boundary;
                }

                let should_refill = sst_info.table_ids.binary_search(&table_id).is_ok()
                    && (blk_start..blk_end).any(|block_index| {
                        table_cache_refill_context_map
                            .get(&table_id)
                            .is_some_and(|context| {
                                if has_parent_ssts {
                                    context.allows_normal_data_refill_block(sst, block_index)
                                } else {
                                    context.allows_insert_only_data_refill_block(sst, block_index)
                                }
                            })
                    });
                if should_refill {
                    tasks.push(DataCacheRefillTask {
                        sst: sst.clone(),
                        blks: blk_start..blk_end,
                    });
                }
                blk_start = blk_end;
            }
        }

        if tasks.is_empty() {
            return tasks;
        }

        // Policy/vnode ownership defines refill responsibility first, but it does not bypass
        // recent admission for normal insert+delete refill.
        if has_parent_ssts
            && !self.context.config.skip_recent_filter
            && !self.filter_by_recent_filter()
        {
            GLOBAL_CACHE_REFILL_METRICS
                .data_refill_filtered_total
                .inc_by(self.delta.delete_sst_infos.len() as u64);
            return vec![];
        }

        tasks
    }

    async fn filter_by_inheritance_if_needed(
        &self,
        tasks: Vec<DataCacheRefillTask>,
    ) -> Vec<DataCacheRefillTask> {
        // Skipping the recent filter selects full refill. Inheritance filtering only applies to
        // non-L0 normal refill after real recent-filter admission.
        let should_filter_by_inheritance = !tasks.is_empty()
            && !self.delta.delete_sst_infos.is_empty()
            && self.delta.insert_sst_level != 0
            && !self.context.config.skip_recent_filter
            && !self.context.config.skip_inheritance_filter;
        if should_filter_by_inheritance {
            self.filter_by_inheritance_filter(tasks).await
        } else {
            tasks
        }
    }

    // Return if recent filter is required and no deleted sst ids are in the recent filter.
    fn filter_by_recent_filter(&self) -> bool {
        let recent_filter = self.context.sstable_store.recent_filter();
        let targets = self
            .delta
            .delete_sst_infos
            .iter()
            .map(|sst| (sst.object_id, usize::MAX))
            .collect_vec();
        recent_filter.contains_any(targets.iter())
    }

    async fn filter_by_inheritance_filter(
        &self,
        originals: Vec<DataCacheRefillTask>,
    ) -> Vec<DataCacheRefillTask> {
        // Get parent sst metas from cache.
        let sstable_store = self.context.sstable_store.clone();
        let futures = self.delta.delete_sst_infos.iter().map(|sst| {
            let store = &sstable_store;
            let sst_obj_id = sst.object_id;
            async move {
                let res = store.sstable_cached(sst_obj_id).await;
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
    plan: CacheRefillPlan,
    context: CacheRefillContext,
}

impl CacheRefillTask {
    async fn run(self) -> bool {
        let CacheRefillPlan { deltas } = self.plan;
        let tasks = deltas
            .iter()
            .map(|delta| {
                let context = self.context.clone();
                async move {
                    let holders = match Self::meta_cache_refill(&context, delta).await {
                        Ok(holders) => holders,
                        Err(e) => {
                            tracing::warn!(error = %e.as_report(), "meta cache refill error");
                            return false;
                        }
                    };
                    let generator = DataCacheRefillTaskGenerator {
                        context: &context,
                        delta,
                        ssts: &holders,
                    };
                    let tasks = generator.generate_unfiltered_tasks();

                    // Main counts after recent admission but before inheritance.
                    let unfiltered_block_count =
                        tasks.iter().map(|task| task.blks.len() as u64).sum();
                    GLOBAL_CACHE_REFILL_METRICS
                        .data_refill_block_unfiltered_total
                        .inc_by(unfiltered_block_count);

                    let tasks = generator.filter_by_inheritance_if_needed(tasks).await;
                    Self::data_cache_refill(&context, tasks).await
                }
            })
            .collect_vec();
        // The Foyer task keeps its main-path timeout. The batch sequencer applies the same bound
        // to Pin tickets so whole-SST refill cannot indefinitely delay version publication.
        join_all(tasks).await.into_iter().all(|ready| ready)
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
                if res.is_ok() {
                    GLOBAL_CACHE_REFILL_METRICS
                        .meta_refill_success_duration
                        .observe(now.elapsed().as_secs_f64());
                }
                drop(permit);

                res
            })
            .collect_vec();
        let holders = try_join_all(tasks).await?;
        Ok(holders)
    }

    async fn data_cache_refill(
        context: &CacheRefillContext,
        tasks: Vec<DataCacheRefillTask>,
    ) -> bool {
        let mut futures = Vec::with_capacity(tasks.len());
        for task in tasks {
            // update filter for sst id only
            context
                .sstable_store
                .recent_filter()
                .insert((task.sst.id, usize::MAX));

            let blocks = task.blks.len();
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

                    let now = Instant::now();

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

                    GLOBAL_CACHE_REFILL_METRICS
                        .data_refill_success_duration
                        .observe(now.elapsed().as_secs_f64());
                    drop(permit);

                    Ok::<_, HummockError>(())
                };
                futures.push(future);
            }
        }

        let futures = futures.into_iter().map(|future| async move {
            if let Err(e) = future.await {
                tracing::error!(error = %e.as_report(), "data cache refill task error");
                return false;
            }
            true
        });

        join_all(futures).await.into_iter().all(|ready| ready)
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

    use bytes::Bytes;
    use parking_lot::Mutex;
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::config::streaming::CacheRefillPolicy;
    use risingwave_common::config::{ObjectStoreConfig, Role};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::compaction_group::group_split::split_sst_with_table_ids;
    use risingwave_hummock_sdk::key::{FullKey, UserKey, prefix_slice_with_vnode};
    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};
    use risingwave_hummock_sdk::version::HummockVersion;
    use risingwave_hummock_sdk::{EpochWithGap, HummockSstableObjectId};
    use risingwave_object_store::object::{
        InMemObjectStore, ObjectStore, ObjectStoreImpl, ObjectStoreRef,
    };
    use risingwave_pb::hummock::hummock_version::PbLevels;
    use risingwave_pb::hummock::{
        LevelType as PbLevelType, PbHummockVersion, PbLevel, PbOverlappingLevel, PbStateTableInfo,
    };
    use risingwave_pb::id::{CompactionGroupId, TableId};
    use tokio::sync::mpsc::unbounded_channel;

    use super::{
        CacheRefillConfig, CacheRefillContext, CacheRefiller, DataCacheRefillTaskGenerator,
        PinRefillPlanStats, SpawnRefillTask, SstDeltaInfo, block_vnode_range,
        vnode_range_overlaps_bitmap,
    };
    use crate::hummock::iterator::test_utils::{
        iterator_test_table_key_of, mock_sstable_store, mock_sstable_store_with_recent_filter,
    };
    use crate::hummock::local_version::pinned_version::PinnedVersion;
    use crate::hummock::pin_cache::PinCache;
    use crate::hummock::pin_cache_refill::{PinCacheMembershipUpdate, PinCacheRefillController};
    use crate::hummock::recent_filter::simple::SimpleRecentFilter;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_with_table_ids,
    };
    use crate::hummock::value::HummockValue;
    use crate::hummock::{
        CachePolicy, RecentFilter, RecentFilterTrait, SstableStoreRef, TableHolder,
    };
    use crate::monitor::{ObjectStoreMetrics, StoreLocalStatistic};

    fn test_refill_config(default_policy: CacheRefillPolicy) -> CacheRefillConfig {
        CacheRefillConfig {
            timeout: Duration::from_secs(1),
            pin_cache_max_batch_size: 16,
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

    fn deleted_sst(object_id: HummockSstableObjectId) -> SstableInfo {
        SstableInfo::from(SstableInfoInner {
            object_id,
            ..Default::default()
        })
    }

    fn pinned_version_for_test() -> PinnedVersion {
        PinnedVersion::new(
            HummockVersion::from(PbHummockVersion::default()),
            unbounded_channel().0,
        )
    }

    fn pin_cache_store_for_test() -> ObjectStoreRef {
        Arc::new(ObjectStoreImpl::InMem(
            InMemObjectStore::for_test().monitored(
                Arc::new(ObjectStoreMetrics::unused()),
                Arc::new(ObjectStoreConfig::default()),
            ),
        ))
    }

    fn pin_cache_for_test() -> Arc<PinCache> {
        PinCache::new(pin_cache_store_for_test(), u64::MAX)
    }

    #[allow(deprecated)]
    fn pinned_version_with_sst(table_id: TableId, sst: &SstableInfo) -> PinnedVersion {
        pinned_version_with_ssts(&[table_id], std::slice::from_ref(sst))
    }

    #[allow(deprecated)]
    fn pinned_version_with_ssts(table_ids: &[TableId], ssts: &[SstableInfo]) -> PinnedVersion {
        pinned_version_with_groups(&[(
            StaticCompactionGroupId::NewCompactionGroup,
            table_ids,
            ssts,
        )])
    }

    #[allow(deprecated)]
    fn pinned_version_with_groups(
        groups: &[(CompactionGroupId, &[TableId], &[SstableInfo])],
    ) -> PinnedVersion {
        let mut levels = HashMap::new();
        let mut state_table_info = HashMap::new();
        for &(compaction_group_id, table_ids, ssts) in groups {
            let total_file_size = ssts.iter().map(|sst| sst.file_size).sum();
            let uncompressed_file_size = ssts.iter().map(|sst| sst.uncompressed_file_size).sum();
            let level = PbLevel {
                level_idx: 0,
                level_type: PbLevelType::Overlapping as i32,
                table_infos: ssts.iter().cloned().map(Into::into).collect(),
                total_file_size,
                sub_level_id: 1,
                uncompressed_file_size,
                vnode_partition_count: 0,
            };
            levels.insert(
                compaction_group_id,
                PbLevels {
                    levels: vec![],
                    l0: Some(PbOverlappingLevel {
                        sub_levels: vec![level],
                        total_file_size,
                        uncompressed_file_size,
                    }),
                    group_id: compaction_group_id,
                    parent_group_id: compaction_group_id,
                    member_table_ids: vec![],
                    compaction_group_version_id: 0,
                },
            );
            state_table_info.extend(table_ids.iter().map(|&table_id| {
                (
                    table_id,
                    PbStateTableInfo {
                        committed_epoch: 0,
                        compaction_group_id,
                    },
                )
            }));
        }
        let version = HummockVersion::from_rpc_protobuf(&PbHummockVersion {
            id: 1.into(),
            levels,
            state_table_info,
            ..Default::default()
        });
        PinnedVersion::new(version, unbounded_channel().0)
    }

    async fn gen_test_sst_with_object_id(
        table_id: TableId,
        sstable_store: SstableStoreRef,
        object_id: u64,
    ) -> (TableHolder, SstableInfo) {
        gen_test_sstable_with_table_ids(
            default_builder_opt_for_test(),
            object_id,
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
        .await
    }

    struct DataRefillGeneratorTestFixture {
        table_id: TableId,
        sstable_store: SstableStoreRef,
        sst: TableHolder,
        sst_info: SstableInfo,
        deleted_sst_object_id: HummockSstableObjectId,
    }

    impl DataRefillGeneratorTestFixture {
        async fn new(
            recent_filter: Option<Arc<RecentFilter<(HummockSstableObjectId, usize)>>>,
        ) -> Self {
            let table_id = TableId::from(233);
            let sstable_store = match recent_filter {
                Some(recent_filter) => mock_sstable_store_with_recent_filter(recent_filter).await,
                None => mock_sstable_store().await,
            };
            let (sst, sst_info) =
                gen_test_sst_with_object_id(table_id, sstable_store.clone(), 1).await;
            Self {
                table_id,
                sstable_store,
                sst,
                sst_info,
                deleted_sst_object_id: 2330.into(),
            }
        }

        fn context(
            &self,
            policy: CacheRefillPolicy,
            streaming_vnode_bitmap: Option<Bitmap>,
            serving_vnode_bitmap: Option<Bitmap>,
            configure: impl FnOnce(&mut CacheRefillConfig),
        ) -> CacheRefillContext {
            let mut config = test_refill_config(CacheRefillPolicy::Enabled);
            config.data_refill_levels.insert(0);
            configure(&mut config);
            CacheRefillContext {
                config: Arc::new(config),
                meta_refill_concurrency: None,
                concurrency: Arc::new(tokio::sync::Semaphore::new(1)),
                sstable_store: self.sstable_store.clone(),
                table_cache_refill_context_map: Arc::new(HashMap::from([(
                    self.table_id,
                    super::TableCacheRefillContext {
                        streaming_vnode_bitmap,
                        serving_vnode_bitmap,
                        policy,
                    },
                )])),
            }
        }

        fn normal_delta(
            &self,
            insert_sst_level: u32,
            deleted_sst_object_id: HummockSstableObjectId,
        ) -> SstDeltaInfo {
            SstDeltaInfo {
                insert_sst_infos: vec![self.sst_info.clone()],
                delete_sst_infos: vec![deleted_sst(deleted_sst_object_id)],
                insert_sst_level,
            }
        }

        fn normal_l0_delta(&self) -> SstDeltaInfo {
            self.normal_delta(0, self.deleted_sst_object_id)
        }

        fn l0_insert_only_delta(&self) -> SstDeltaInfo {
            SstDeltaInfo {
                insert_sst_infos: vec![self.sst_info.clone()],
                delete_sst_infos: vec![],
                insert_sst_level: 0,
            }
        }

        async fn generate(
            &self,
            context: &CacheRefillContext,
            delta: &SstDeltaInfo,
        ) -> Vec<super::DataCacheRefillTask> {
            let generator = DataCacheRefillTaskGenerator {
                context,
                delta,
                ssts: std::slice::from_ref(&self.sst),
            };
            let tasks = generator.generate_unfiltered_tasks();
            generator.filter_by_inheritance_if_needed(tasks).await
        }
    }

    #[tokio::test]
    async fn test_table_cache_refill_contexts_by_role_and_policy() {
        struct Case {
            name: &'static str,
            role: Role,
            default_policy: CacheRefillPolicy,
            policy: Option<CacheRefillPolicy>,
            has_streaming_vnodes: bool,
            has_serving_vnodes: bool,
            expected: Option<(CacheRefillPolicy, bool, bool)>,
        }

        let cases = [
            Case {
                name: "streaming role uses streaming side of Both",
                role: Role::Streaming,
                default_policy: CacheRefillPolicy::Disabled,
                policy: Some(CacheRefillPolicy::Both),
                has_streaming_vnodes: true,
                has_serving_vnodes: true,
                expected: Some((CacheRefillPolicy::Both, true, false)),
            },
            Case {
                name: "serving role uses serving side of Both",
                role: Role::Serving,
                default_policy: CacheRefillPolicy::Disabled,
                policy: Some(CacheRefillPolicy::Both),
                has_streaming_vnodes: true,
                has_serving_vnodes: true,
                expected: Some((CacheRefillPolicy::Both, false, true)),
            },
            Case {
                name: "both role keeps both sides",
                role: Role::Both,
                default_policy: CacheRefillPolicy::Disabled,
                policy: Some(CacheRefillPolicy::Both),
                has_streaming_vnodes: true,
                has_serving_vnodes: true,
                expected: Some((CacheRefillPolicy::Both, true, true)),
            },
            Case {
                name: "both role keeps streaming-only ownership",
                role: Role::Both,
                default_policy: CacheRefillPolicy::Disabled,
                policy: Some(CacheRefillPolicy::Both),
                has_streaming_vnodes: true,
                has_serving_vnodes: false,
                expected: Some((CacheRefillPolicy::Both, true, false)),
            },
            Case {
                name: "streaming scope without ownership has no usable bitmap",
                role: Role::Streaming,
                default_policy: CacheRefillPolicy::Disabled,
                policy: Some(CacheRefillPolicy::Streaming),
                has_streaming_vnodes: false,
                has_serving_vnodes: false,
                expected: Some((CacheRefillPolicy::Streaming, false, false)),
            },
            Case {
                name: "pure serving worker excludes unmapped table",
                role: Role::Serving,
                default_policy: CacheRefillPolicy::Disabled,
                policy: Some(CacheRefillPolicy::Serving),
                has_streaming_vnodes: true,
                has_serving_vnodes: false,
                expected: None,
            },
            Case {
                name: "default Enabled retains serving ownership",
                role: Role::Serving,
                default_policy: CacheRefillPolicy::Enabled,
                policy: None,
                has_streaming_vnodes: false,
                has_serving_vnodes: true,
                expected: Some((CacheRefillPolicy::Enabled, false, true)),
            },
            Case {
                name: "explicit policy overrides default",
                role: Role::Serving,
                default_policy: CacheRefillPolicy::Enabled,
                policy: Some(CacheRefillPolicy::Disabled),
                has_streaming_vnodes: false,
                has_serving_vnodes: true,
                expected: Some((CacheRefillPolicy::Disabled, false, false)),
            },
            Case {
                name: "pinned policy is not serving-mapping scoped",
                role: Role::Serving,
                default_policy: CacheRefillPolicy::Disabled,
                policy: Some(CacheRefillPolicy::Pinned),
                has_streaming_vnodes: false,
                has_serving_vnodes: false,
                expected: None,
            },
        ];

        let table_id = TableId::from(233);
        let streaming_vnodes = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [1, 3]);
        let serving_vnodes = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [2, 4]);
        let sstable_store = mock_sstable_store().await;
        for case in cases {
            let mut refiller = CacheRefiller::new(
                case.role,
                test_refill_config(case.default_policy),
                sstable_store.clone(),
                CacheRefiller::default_spawn_refill_task(),
                pinned_version_for_test(),
            );
            if let Some(policy) = case.policy {
                refiller.replace_table_cache_refill_policies(HashMap::from([(table_id, policy)]));
            }
            if case.has_streaming_vnodes {
                refiller.update_streaming_table_vnodes(table_id, Some(streaming_vnodes.clone()));
            }
            if case.has_serving_vnodes {
                refiller.replace_serving_table_vnode_mapping(HashMap::from([(
                    table_id,
                    serving_vnodes.clone(),
                )]));
            }

            let contexts = refiller.table_cache_refill_contexts([table_id]);
            let actual = contexts.get(&table_id).map(|context| {
                (
                    context.policy,
                    context.streaming_vnode_bitmap.as_ref(),
                    context.serving_vnode_bitmap.as_ref(),
                )
            });
            let expected = case.expected.map(|(policy, streaming, serving)| {
                (
                    policy,
                    streaming.then_some(&streaming_vnodes),
                    serving.then_some(&serving_vnodes),
                )
            });
            assert_eq!(actual, expected, "{}", case.name);
        }
    }

    #[tokio::test]
    async fn test_refill_task_captures_runtime_context_snapshot() {
        let table_id = TableId::from(233);
        let old_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
        let new_vnodes = Bitmap::from_range(VirtualNode::COUNT_FOR_TEST, 0..8);
        let captured_context = Arc::new(Mutex::new(None::<CacheRefillContext>));
        let captured_context_clone = captured_context.clone();
        let spawn_refill_task: SpawnRefillTask = Arc::new(move |_, context, _, _| {
            *captured_context_clone.lock() = Some(context);
            tokio::spawn(async { true })
        });
        let mut refiller = CacheRefiller::new(
            Role::Serving,
            test_refill_config(CacheRefillPolicy::Enabled),
            mock_sstable_store().await,
            spawn_refill_task,
            pinned_version_for_test(),
        );

        refiller.replace_table_cache_refill_policies(HashMap::from([(
            table_id,
            CacheRefillPolicy::Serving,
        )]));
        refiller
            .replace_serving_table_vnode_mapping(HashMap::from([(table_id, old_vnodes.clone())]));

        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: vec![SstableInfo::from(SstableInfoInner {
                    table_ids: vec![table_id],
                    ..Default::default()
                })],
                ..Default::default()
            }],
            pinned_version_for_test(),
            pinned_version_for_test(),
            PinCacheMembershipUpdate::Delta,
        );
        refiller.replace_table_cache_refill_policies(HashMap::from([(
            table_id,
            CacheRefillPolicy::Disabled,
        )]));
        refiller.replace_serving_table_vnode_mapping(HashMap::from([(table_id, new_vnodes)]));

        let captured_context = captured_context.lock();
        let context = captured_context
            .as_ref()
            .unwrap()
            .table_cache_refill_context_map
            .get(&table_id)
            .unwrap();
        assert_eq!(context.policy, CacheRefillPolicy::Serving);
        assert_eq!(context.serving_vnode_bitmap.as_ref(), Some(&old_vnodes));
    }

    #[tokio::test]
    async fn test_pinned_insert_only_sst_uses_whole_object_refill() {
        let table_id = TableId::from(233);
        let sibling_table_id = TableId::from(234);
        let sstable_store = mock_sstable_store().await;
        let (sst, sst_info) =
            gen_test_sst_with_object_id(table_id, sstable_store.clone(), 1001).await;
        let pin_cache = pin_cache_for_test();
        sstable_store.set_pin_cache(pin_cache.clone());

        let mut refiller = CacheRefiller::new(
            Role::Streaming,
            test_refill_config(CacheRefillPolicy::Disabled),
            sstable_store.clone(),
            CacheRefiller::default_spawn_refill_task(),
            pinned_version_for_test(),
        );
        refiller.replace_table_cache_refill_policies(HashMap::from([
            (table_id, CacheRefillPolicy::Pinned),
            (sibling_table_id, CacheRefillPolicy::Pinned),
        ]));
        refiller.update_streaming_table_vnodes(
            table_id,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
        );
        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: vec![sst_info.clone()],
                delete_sst_infos: vec![],
                insert_sst_level: 0,
            }],
            pinned_version_for_test(),
            pinned_version_with_sst(table_id, &sst_info),
            PinCacheMembershipUpdate::Delta,
        );

        assert_eq!(refiller.next_events().await.len(), 1);
        assert!(pin_cache.get(sst_info.object_id).is_some());

        sstable_store.clear_block_cache().await.unwrap();
        sstable_store
            .store()
            .delete(&sstable_store.get_sst_data_path(sst_info.object_id))
            .await
            .unwrap();
        sstable_store
            .get(
                &sst,
                0,
                CachePolicy::NotFill,
                &mut StoreLocalStatistic::default(),
            )
            .await
            .expect("ready pinned SST should not probe Foyer or remote storage");

        refiller.replace_table_cache_refill_policies(HashMap::from([(
            sibling_table_id,
            CacheRefillPolicy::Pinned,
        )]));
        assert!(pin_cache.get(sst_info.object_id).is_none());
        assert_eq!(
            refiller.table_cache_refill_monitor_snapshot().policies,
            HashMap::from([(sibling_table_id, CacheRefillPolicy::Pinned)])
        );
        assert!(
            sstable_store
                .get(
                    &sst,
                    0,
                    CachePolicy::NotFill,
                    &mut StoreLocalStatistic::default(),
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_pin_set_does_not_reclassify_queued_delta_as_warm() {
        let table_id = TableId::from(233);
        let sstable_store = mock_sstable_store().await;
        let (_, sst_info) =
            gen_test_sst_with_object_id(table_id, sstable_store.clone(), 1001).await;
        let pin_cache = pin_cache_for_test();
        sstable_store.set_pin_cache(pin_cache.clone());

        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let task_gate = gate.clone();
        let spawn_refill_task: SpawnRefillTask = Arc::new(move |plan, context, _, _| {
            let task_gate = task_gate.clone();
            tokio::spawn(async move {
                let _permit = task_gate.acquire().await.unwrap();
                super::CacheRefillTask { plan, context }.run().await
            })
        });
        let mut refiller = CacheRefiller::new(
            Role::Streaming,
            test_refill_config(CacheRefillPolicy::Disabled),
            sstable_store,
            spawn_refill_task,
            pinned_version_for_test(),
        );
        refiller.replace_table_cache_refill_policies(HashMap::from([(
            table_id,
            CacheRefillPolicy::Disabled,
        )]));
        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: vec![sst_info.clone()],
                delete_sst_infos: vec![],
                insert_sst_level: 0,
            }],
            pinned_version_for_test(),
            pinned_version_with_sst(table_id, &sst_info),
            PinCacheMembershipUpdate::Delta,
        );

        refiller.replace_table_cache_refill_policies(HashMap::from([(
            table_id,
            CacheRefillPolicy::Pinned,
        )]));
        assert!(pin_cache.is_desired(sst_info.object_id));
        gate.add_permits(1);
        assert_eq!(refiller.next_events().await.len(), 1);
        assert!(pin_cache.get(sst_info.object_id).is_none());
    }

    #[tokio::test]
    async fn test_pin_reset_set_does_not_revive_queued_admission() {
        let table_id = TableId::from(233);
        let sstable_store = mock_sstable_store().await;
        let (_, sst_info) =
            gen_test_sst_with_object_id(table_id, sstable_store.clone(), 1002).await;
        let pin_cache = pin_cache_for_test();
        sstable_store.set_pin_cache(pin_cache.clone());

        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let task_gate = gate.clone();
        let spawn_refill_task: SpawnRefillTask = Arc::new(move |plan, context, _, _| {
            let task_gate = task_gate.clone();
            tokio::spawn(async move {
                let _permit = task_gate.acquire().await.unwrap();
                super::CacheRefillTask { plan, context }.run().await
            })
        });
        let base = pinned_version_for_test();
        let next = pinned_version_with_sst(table_id, &sst_info);
        let mut refiller = CacheRefiller::new(
            Role::Streaming,
            test_refill_config(CacheRefillPolicy::Disabled),
            sstable_store,
            spawn_refill_task,
            base.clone(),
        );
        refiller.replace_table_cache_refill_policies(HashMap::from([(
            table_id,
            CacheRefillPolicy::Pinned,
        )]));
        refiller.update_streaming_table_vnodes(
            table_id,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
        );

        // Keep one event active so the admitted delta remains in the pending batch.
        refiller.start_cache_refill(
            vec![],
            base.clone(),
            base.clone(),
            PinCacheMembershipUpdate::Delta,
        );
        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: vec![sst_info.clone()],
                ..Default::default()
            }],
            base,
            next,
            PinCacheMembershipUpdate::Delta,
        );

        refiller.replace_table_cache_refill_policies(HashMap::from([(
            table_id,
            CacheRefillPolicy::Disabled,
        )]));
        refiller.replace_table_cache_refill_policies(HashMap::from([(
            table_id,
            CacheRefillPolicy::Pinned,
        )]));
        assert!(pin_cache.is_desired(sst_info.object_id));

        gate.add_permits(1);
        assert_eq!(refiller.next_events().await.len(), 1);
        assert_eq!(refiller.next_events().await.len(), 1);
        assert!(
            pin_cache.get(sst_info.object_id).is_none(),
            "RESET must invalidate the old admission even when a later SET reuses the table"
        );
    }

    #[tokio::test]
    async fn test_pin_cache_desired_objects_follow_policy_and_version() {
        let table_id = TableId::from(233);
        let sstable_store = mock_sstable_store().await;
        let (_, sst_info) =
            gen_test_sst_with_object_id(table_id, sstable_store.clone(), 1001).await;
        let local_store = pin_cache_store_for_test();
        local_store
            .upload(
                &format!("{}-42.sst", sst_info.object_id.as_raw_id()),
                Bytes::from(vec![0; sst_info.file_size as usize]),
            )
            .await
            .unwrap();
        let pin_cache = PinCache::new(local_store, u64::MAX);
        sstable_store.set_pin_cache(pin_cache.clone());
        let mut refiller = CacheRefiller::new(
            Role::Streaming,
            test_refill_config(CacheRefillPolicy::Disabled),
            sstable_store.clone(),
            CacheRefiller::default_spawn_refill_task(),
            pinned_version_with_sst(table_id, &sst_info),
        );
        refiller.update_streaming_table_vnodes(
            table_id,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
        );

        refiller.replace_table_cache_refill_policies(HashMap::from([(
            table_id,
            CacheRefillPolicy::Pinned,
        )]));
        assert!(pin_cache.is_desired(sst_info.object_id));
        tokio::time::timeout(Duration::from_secs(1), async {
            while pin_cache.get(sst_info.object_id).is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        refiller.replace_table_cache_refill_policies(HashMap::from([(
            table_id,
            CacheRefillPolicy::Disabled,
        )]));
        assert!(!pin_cache.is_desired(sst_info.object_id));
        assert!(pin_cache.get(sst_info.object_id).is_none());

        refiller.replace_table_cache_refill_policies(HashMap::from([(
            table_id,
            CacheRefillPolicy::Pinned,
        )]));
        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                delete_sst_infos: vec![sst_info.clone()],
                ..Default::default()
            }],
            pinned_version_with_sst(table_id, &sst_info),
            pinned_version_for_test(),
            PinCacheMembershipUpdate::Delta,
        );
        assert!(!pin_cache.is_desired(sst_info.object_id));

        // Batched version deltas must preserve their order. An SST added and then removed before
        // this worker handles the notification must not remain desired.
        refiller.start_cache_refill(
            vec![
                SstDeltaInfo {
                    insert_sst_infos: vec![sst_info.clone()],
                    insert_sst_level: 0,
                    ..Default::default()
                },
                SstDeltaInfo {
                    delete_sst_infos: vec![sst_info.clone()],
                    ..Default::default()
                },
            ],
            pinned_version_for_test(),
            pinned_version_for_test(),
            PinCacheMembershipUpdate::Delta,
        );
        assert!(!pin_cache.is_desired(sst_info.object_id));
    }

    #[tokio::test]
    async fn test_pin_cache_keeps_split_object_until_last_logical_reference() {
        let table_a = TableId::from(233);
        let table_b = TableId::from(234);
        let object_id = HummockSstableObjectId::from(1001);
        let branch = |sst_id, table_id| {
            SstableInfo::from(SstableInfoInner {
                object_id,
                sst_id,
                file_size: 8,
                table_ids: vec![table_id],
                ..Default::default()
            })
        };
        let branch_a = branch(1001.into(), table_a);
        let branch_b = branch(1002.into(), table_b);
        let both =
            pinned_version_with_ssts(&[table_a, table_b], &[branch_a.clone(), branch_b.clone()]);
        let only_b = pinned_version_with_ssts(&[table_a, table_b], std::slice::from_ref(&branch_b));

        let sstable_store = mock_sstable_store().await;
        let pin_cache = pin_cache_for_test();
        sstable_store.set_pin_cache(pin_cache.clone());
        let mut refiller = CacheRefiller::new(
            Role::Streaming,
            test_refill_config(CacheRefillPolicy::Disabled),
            sstable_store,
            CacheRefiller::default_spawn_refill_task(),
            both.clone(),
        );
        refiller.replace_table_cache_refill_policies(HashMap::from([
            (table_a, CacheRefillPolicy::Pinned),
            (table_b, CacheRefillPolicy::Pinned),
        ]));
        assert!(pin_cache.is_desired(object_id));

        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                delete_sst_infos: vec![branch_a],
                ..Default::default()
            }],
            both,
            only_b.clone(),
            PinCacheMembershipUpdate::Delta,
        );
        assert_eq!(refiller.next_events().await.len(), 1);
        assert!(pin_cache.is_desired(object_id));

        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                delete_sst_infos: vec![branch_b],
                ..Default::default()
            }],
            only_b,
            pinned_version_for_test(),
            PinCacheMembershipUpdate::Delta,
        );
        assert_eq!(refiller.next_events().await.len(), 1);
        assert!(!pin_cache.is_desired(object_id));
    }

    #[tokio::test]
    async fn test_pin_cache_rebuild_counts_split_object_across_compaction_groups() {
        let table_a = TableId::from(233);
        let table_b = TableId::from(234);
        let object_id = HummockSstableObjectId::from(1001);
        let branch = |sst_id, table_id| {
            SstableInfo::from(SstableInfoInner {
                object_id,
                sst_id,
                file_size: 8,
                table_ids: vec![table_id],
                ..Default::default()
            })
        };
        let branch_a = branch(1001.into(), table_a);
        let branch_b = branch(1002.into(), table_b);
        let both = pinned_version_with_groups(&[
            (
                StaticCompactionGroupId::StateDefault,
                &[table_a],
                std::slice::from_ref(&branch_a),
            ),
            (
                StaticCompactionGroupId::MaterializedView,
                &[table_b],
                std::slice::from_ref(&branch_b),
            ),
        ]);
        let only_b = pinned_version_with_groups(&[(
            StaticCompactionGroupId::MaterializedView,
            &[table_b],
            std::slice::from_ref(&branch_b),
        )]);

        let sstable_store = mock_sstable_store().await;
        let pin_cache = pin_cache_for_test();
        sstable_store.set_pin_cache(pin_cache.clone());
        let mut refiller = CacheRefiller::new(
            Role::Streaming,
            test_refill_config(CacheRefillPolicy::Disabled),
            sstable_store,
            CacheRefiller::default_spawn_refill_task(),
            both.clone(),
        );
        refiller.replace_table_cache_refill_policies(HashMap::from([
            (table_a, CacheRefillPolicy::Pinned),
            (table_b, CacheRefillPolicy::Pinned),
        ]));
        assert!(pin_cache.is_desired(object_id));

        refiller.start_cache_refill(
            vec![],
            both,
            only_b.clone(),
            PinCacheMembershipUpdate::Rebuild,
        );
        assert_eq!(refiller.next_events().await.len(), 1);
        assert!(pin_cache.is_desired(object_id));

        refiller.start_cache_refill(
            vec![],
            only_b,
            pinned_version_for_test(),
            PinCacheMembershipUpdate::Rebuild,
        );
        assert_eq!(refiller.next_events().await.len(), 1);
        assert!(!pin_cache.is_desired(object_id));
    }

    #[tokio::test]
    async fn test_cache_refill_prunes_whole_ssts_before_meta_load() {
        let streaming_table = TableId::from(1);
        let serving_table = TableId::from(2);
        let disabled_table = TableId::from(3);
        let internal_table = TableId::from(4);
        let fallback_table = TableId::from(5);
        let serving_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
        let sstable_store = mock_sstable_store().await;
        let sst_info = |table_ids: Vec<TableId>| {
            SstableInfo::from(SstableInfoInner {
                table_ids,
                ..Default::default()
            })
        };
        let capture_pruned_table_ids = |role,
                                        default_policy,
                                        policies: HashMap<TableId, CacheRefillPolicy>,
                                        streaming_table_vnodes: HashMap<TableId, Bitmap>,
                                        serving_table_vnodes: HashMap<TableId, Bitmap>,
                                        delta| {
            let captured_deltas = Arc::new(Mutex::new(None::<Vec<SstDeltaInfo>>));
            let captured_deltas_clone = captured_deltas.clone();
            let spawn_refill_task: SpawnRefillTask = Arc::new(move |plan, _, _, _| {
                *captured_deltas_clone.lock() = Some(plan.deltas);
                tokio::spawn(async { true })
            });
            let mut refiller = CacheRefiller::new(
                role,
                test_refill_config(default_policy),
                sstable_store.clone(),
                spawn_refill_task,
                pinned_version_for_test(),
            );
            refiller.replace_table_cache_refill_policies(policies);
            for (table_id, vnodes) in streaming_table_vnodes {
                refiller.update_streaming_table_vnodes(table_id, Some(vnodes));
            }
            refiller.replace_serving_table_vnode_mapping(serving_table_vnodes);
            refiller.start_cache_refill(
                vec![delta],
                pinned_version_for_test(),
                pinned_version_for_test(),
                PinCacheMembershipUpdate::Delta,
            );
            captured_deltas
                .lock()
                .take()
                .unwrap()
                .pop()
                .unwrap()
                .insert_sst_infos
                .into_iter()
                .map(|sst| sst.table_ids.clone())
                .collect::<Vec<_>>()
        };

        let normal_delta = |insert_sst_infos| SstDeltaInfo {
            insert_sst_infos,
            delete_sst_infos: vec![deleted_sst(1.into())],
            insert_sst_level: 1,
        };
        let insert_only_delta = |insert_sst_infos| SstDeltaInfo {
            insert_sst_infos,
            delete_sst_infos: vec![],
            insert_sst_level: 0,
        };

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (disabled_table, CacheRefillPolicy::Disabled),
                    (streaming_table, CacheRefillPolicy::Enabled),
                ]),
                HashMap::new(),
                HashMap::new(),
                normal_delta(vec![
                    sst_info(vec![disabled_table]),
                    sst_info(vec![streaming_table]),
                ]),
            ),
            vec![vec![streaming_table]],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Streaming),
                    (internal_table, CacheRefillPolicy::Serving),
                ]),
                HashMap::from([(streaming_table, serving_vnodes.clone())]),
                HashMap::new(),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![internal_table]),
                ]),
            ),
            vec![vec![streaming_table]],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Streaming),
                    (serving_table, CacheRefillPolicy::Serving),
                    (internal_table, CacheRefillPolicy::Serving),
                ]),
                HashMap::from([(streaming_table, serving_vnodes.clone())]),
                HashMap::from([(serving_table, serving_vnodes.clone())]),
                insert_only_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                    sst_info(vec![internal_table]),
                ]),
            ),
            vec![vec![serving_table]],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Serving,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Streaming),
                    (serving_table, CacheRefillPolicy::Serving),
                ]),
                HashMap::new(),
                HashMap::from([(serving_table, serving_vnodes.clone())]),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                ]),
            ),
            vec![vec![serving_table]],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Serving,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Enabled),
                    (serving_table, CacheRefillPolicy::Enabled),
                ]),
                HashMap::new(),
                HashMap::from([(serving_table, serving_vnodes.clone())]),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                    sst_info(vec![streaming_table, serving_table]),
                ]),
            ),
            vec![
                vec![streaming_table],
                vec![serving_table],
                vec![streaming_table, serving_table],
            ],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Both),
                    (serving_table, CacheRefillPolicy::Both),
                ]),
                HashMap::new(),
                HashMap::new(),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                ]),
            ),
            Vec::<Vec<TableId>>::new(),
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Both),
                    (serving_table, CacheRefillPolicy::Both),
                ]),
                HashMap::from([(streaming_table, serving_vnodes.clone())]),
                HashMap::new(),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                ]),
            ),
            vec![vec![streaming_table]],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Both),
                    (serving_table, CacheRefillPolicy::Both),
                ]),
                HashMap::new(),
                HashMap::from([(serving_table, serving_vnodes.clone())]),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                ]),
            ),
            vec![vec![serving_table]],
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Streaming,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Streaming),
                    (serving_table, CacheRefillPolicy::Serving),
                ]),
                HashMap::new(),
                HashMap::new(),
                normal_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                ]),
            ),
            Vec::<Vec<TableId>>::new(),
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Streaming,
                CacheRefillPolicy::Disabled,
                HashMap::from([
                    (streaming_table, CacheRefillPolicy::Streaming),
                    (serving_table, CacheRefillPolicy::Serving),
                ]),
                HashMap::new(),
                HashMap::new(),
                insert_only_delta(vec![
                    sst_info(vec![streaming_table]),
                    sst_info(vec![serving_table]),
                ]),
            ),
            Vec::<Vec<TableId>>::new(),
        );

        assert_eq!(
            capture_pruned_table_ids(
                Role::Both,
                CacheRefillPolicy::Enabled,
                HashMap::from([(disabled_table, CacheRefillPolicy::Disabled)]),
                HashMap::new(),
                HashMap::new(),
                normal_delta(vec![
                    sst_info(vec![disabled_table]),
                    sst_info(vec![fallback_table]),
                    sst_info(vec![disabled_table, fallback_table]),
                ]),
            ),
            vec![vec![fallback_table], vec![disabled_table, fallback_table]],
        );
    }

    #[tokio::test]
    async fn test_normal_refill_applies_policy_and_vnode_ownership() {
        let fixture = DataRefillGeneratorTestFixture::new(None).await;
        let delta = fixture.normal_l0_delta();
        let owned = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [0]);
        let unowned = Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [1]);
        let cases = vec![
            ("Enabled", CacheRefillPolicy::Enabled, None, None, true),
            (
                "Disabled",
                CacheRefillPolicy::Disabled,
                Some(owned.clone()),
                Some(owned.clone()),
                false,
            ),
            (
                "Streaming match",
                CacheRefillPolicy::Streaming,
                Some(owned.clone()),
                None,
                true,
            ),
            (
                "Streaming miss",
                CacheRefillPolicy::Streaming,
                Some(unowned.clone()),
                None,
                false,
            ),
            (
                "Streaming ownership missing",
                CacheRefillPolicy::Streaming,
                None,
                None,
                false,
            ),
            (
                "Serving match",
                CacheRefillPolicy::Serving,
                None,
                Some(owned.clone()),
                true,
            ),
            (
                "Serving miss",
                CacheRefillPolicy::Serving,
                None,
                Some(unowned.clone()),
                false,
            ),
            (
                "Serving ownership missing",
                CacheRefillPolicy::Serving,
                None,
                None,
                false,
            ),
            (
                "Both streaming match",
                CacheRefillPolicy::Both,
                Some(owned.clone()),
                Some(unowned.clone()),
                true,
            ),
            (
                "Both serving match",
                CacheRefillPolicy::Both,
                Some(unowned.clone()),
                Some(owned),
                true,
            ),
            (
                "Both misses",
                CacheRefillPolicy::Both,
                Some(unowned.clone()),
                Some(unowned),
                false,
            ),
            (
                "Pinned is not a Foyer refill policy",
                CacheRefillPolicy::Pinned,
                None,
                None,
                false,
            ),
        ];

        for (name, policy, streaming_vnodes, serving_vnodes, should_refill) in cases {
            let context = fixture.context(policy, streaming_vnodes, serving_vnodes, |_| {});
            assert_eq!(
                !fixture.generate(&context, &delta).await.is_empty(),
                should_refill,
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn test_pinned_policy_does_not_also_refill_foyer_when_localfs_is_enabled() {
        let fixture = DataRefillGeneratorTestFixture::new(None).await;
        fixture.sstable_store.set_pin_cache(pin_cache_for_test());
        let context = fixture.context(
            CacheRefillPolicy::Pinned,
            None,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            |_| {},
        );

        assert!(
            fixture
                .generate(&context, &fixture.normal_l0_delta())
                .await
                .is_empty()
        );
        assert!(
            fixture
                .generate(&context, &fixture.l0_insert_only_delta())
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_normal_refill_applies_recent_and_inheritance_filters() {
        let recent_filter = SimpleRecentFilter::new(3, Duration::from_secs(60));
        let fixture =
            DataRefillGeneratorTestFixture::new(Some(Arc::new(recent_filter.clone().into()))).await;
        let delta = fixture.normal_l0_delta();

        let serving_context = fixture.context(
            CacheRefillPolicy::Serving,
            None,
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            |config| {
                config.skip_recent_filter = false;
            },
        );
        assert!(
            fixture.generate(&serving_context, &delta).await.is_empty(),
            "explicit Serving policy is not an implicit skip_recent_filter"
        );

        recent_filter.insert((fixture.deleted_sst_object_id, usize::MAX));
        assert!(
            !fixture.generate(&serving_context, &delta).await.is_empty(),
            "explicit Serving policy should produce tasks after recent admission hits"
        );

        let (_, parent_sst_info) =
            gen_test_sst_with_object_id(fixture.table_id, fixture.sstable_store.clone(), 2).await;
        let non_l0_delta = fixture.normal_delta(1, parent_sst_info.object_id);
        let non_l0_context = fixture.context(CacheRefillPolicy::Enabled, None, None, |config| {
            config.data_refill_levels.insert(1);
            config.skip_recent_filter = false;
            config.skip_inheritance_filter = false;
        });

        recent_filter.insert((parent_sst_info.object_id, usize::MAX));
        let generator = DataCacheRefillTaskGenerator {
            context: &non_l0_context,
            delta: &non_l0_delta,
            ssts: std::slice::from_ref(&fixture.sst),
        };
        let unfiltered_tasks = generator.generate_unfiltered_tasks();
        assert_eq!(
            unfiltered_tasks
                .iter()
                .map(|task| task.blks.len())
                .sum::<usize>(),
            fixture.sst.block_count(),
            "recent-admitted blocks should reach the inheritance stage"
        );
        assert!(
            generator
                .filter_by_inheritance_if_needed(unfiltered_tasks)
                .await
                .is_empty(),
            "after recent admission, parent block recent miss should filter non-L0 normal refill"
        );

        recent_filter.insert((parent_sst_info.object_id, 0));
        let tasks = fixture.generate(&non_l0_context, &non_l0_delta).await;
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].sst.id, fixture.sst.id);
        assert_eq!(tasks[0].blks, 0..1);
    }

    #[tokio::test]
    async fn test_l0_insert_only_refill_policy_uses_serving_ownership() {
        let fixture = DataRefillGeneratorTestFixture::new(None).await;
        let delta = fixture.l0_insert_only_delta();
        let cases = [
            (
                "Enabled + serving overlap",
                CacheRefillPolicy::Enabled,
                None,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                true,
            ),
            (
                "Enabled without serving ownership",
                CacheRefillPolicy::Enabled,
                None,
                None,
                false,
            ),
            (
                "Serving + serving overlap",
                CacheRefillPolicy::Serving,
                None,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                true,
            ),
            (
                "Streaming + streaming overlap",
                CacheRefillPolicy::Streaming,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                None,
                false,
            ),
            (
                "Both + streaming overlap + serving non-overlap",
                CacheRefillPolicy::Both,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [1])),
                false,
            ),
            (
                "Both + serving overlap",
                CacheRefillPolicy::Both,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                true,
            ),
            (
                "Disabled + serving overlap",
                CacheRefillPolicy::Disabled,
                None,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                false,
            ),
            (
                "Pinned does not use serving Foyer refill",
                CacheRefillPolicy::Pinned,
                None,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
                false,
            ),
        ];

        for (name, policy, streaming_vnodes, serving_vnodes, should_refill) in cases {
            let context = fixture.context(policy, streaming_vnodes, serving_vnodes, |config| {
                config.skip_recent_filter = false;
                config.skip_inheritance_filter = false;
            });
            assert_eq!(
                !fixture.generate(&context, &delta).await.is_empty(),
                should_refill,
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn test_refill_units_do_not_cross_table_projection_boundaries() {
        let table_a = TableId::from(233);
        let table_b = TableId::from(234);
        let sstable_store = mock_sstable_store().await;
        let (sst, sst_info) = gen_test_sstable_with_table_ids(
            default_builder_opt_for_test(),
            1,
            [table_a, table_b].into_iter().map(|table_id| {
                (
                    FullKey {
                        user_key: UserKey::for_test(table_id, iterator_test_table_key_of(0)),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(b"value".to_vec()),
                )
            }),
            sstable_store.clone(),
            vec![table_a.as_raw_id(), table_b.as_raw_id()],
        )
        .await;
        assert_eq!(sst.block_count(), 2, "table switch must form a new block");

        let mut next_sst_id = 100.into();
        let (table_a_projection, table_b_projection) =
            split_sst_with_table_ids(&sst_info, &mut next_sst_id, 1, 1, vec![table_b]);
        assert_eq!(table_a_projection.object_id, sst_info.object_id);
        assert_eq!(table_b_projection.object_id, sst_info.object_id);
        assert_ne!(table_a_projection.sst_id, table_b_projection.sst_id);
        assert_eq!(table_a_projection.table_ids, vec![table_a]);
        assert_eq!(table_b_projection.table_ids, vec![table_b]);

        let deltas = [table_a_projection, table_b_projection].map(|projection| SstDeltaInfo {
            insert_sst_infos: vec![projection],
            delete_sst_infos: vec![],
            insert_sst_level: 0,
        });
        let normal_deltas = deltas.clone().map(|mut delta| {
            // A synthetic delete marks this as a normal delta; recent and inheritance filters
            // are disabled below, so the test does not rely on a matching parent SST.
            delta.delete_sst_infos = vec![deleted_sst(999.into())];
            delta
        });
        let serving_vnodes = Bitmap::ones(VirtualNode::COUNT_FOR_TEST);
        let table_cache_refill_context_map = Arc::new(
            [table_a, table_b]
                .into_iter()
                .map(|table_id| {
                    (
                        table_id,
                        super::TableCacheRefillContext {
                            streaming_vnode_bitmap: None,
                            serving_vnode_bitmap: Some(serving_vnodes.clone()),
                            policy: CacheRefillPolicy::Serving,
                        },
                    )
                })
                .collect::<super::TableCacheRefillContextMap>(),
        );
        let make_context = |unit| {
            let mut config = test_refill_config(CacheRefillPolicy::Disabled);
            config.data_refill_levels.insert(0);
            config.unit = unit;
            CacheRefillContext {
                config: Arc::new(config),
                meta_refill_concurrency: None,
                concurrency: Arc::new(tokio::sync::Semaphore::new(1)),
                sstable_store: sstable_store.clone(),
                table_cache_refill_context_map: table_cache_refill_context_map.clone(),
            }
        };
        let generated_tasks = |context: &CacheRefillContext| {
            deltas
                .iter()
                .map(|delta| {
                    DataCacheRefillTaskGenerator {
                        context,
                        delta,
                        ssts: std::slice::from_ref(&sst),
                    }
                    .generate_unfiltered_tasks()
                })
                .collect::<Vec<_>>()
        };
        let generated_ranges = |context: &CacheRefillContext| {
            generated_tasks(context)
                .into_iter()
                .map(|tasks| tasks.into_iter().map(|task| task.blks).collect::<Vec<_>>())
                .collect::<Vec<_>>()
        };

        assert_eq!(
            generated_ranges(&make_context(1)),
            vec![vec![0..1], vec![1..2]],
            "each logical projection must select only its own block"
        );

        let wide_unit_context = make_context(2);
        assert_eq!(
            generated_ranges(&wide_unit_context),
            vec![vec![0..1], vec![1..2]],
            "units are clipped at table boundaries even when unit is larger than a table run"
        );

        let normal_ranges = normal_deltas
            .iter()
            .map(|delta| {
                DataCacheRefillTaskGenerator {
                    context: &wide_unit_context,
                    delta,
                    ssts: std::slice::from_ref(&sst),
                }
                .generate_unfiltered_tasks()
                .into_iter()
                .map(|task| task.blks)
                .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            normal_ranges,
            vec![vec![0..1], vec![1..2]],
            "normal refill uses the same table-boundary geometry"
        );

        for task in generated_tasks(&wide_unit_context).into_iter().flatten() {
            assert!(task.blks.len() <= wide_unit_context.config.unit);
            assert_eq!(
                task.sst.meta.block_metas[task.blks.start].table_id(),
                task.sst.meta.block_metas[task.blks.end - 1].table_id(),
                "a refill unit must not cross a table boundary"
            );
        }
    }

    #[tokio::test]
    async fn test_scoped_refill_handles_multi_table_vnode_boundary() {
        let table_a = TableId::from(233);
        let table_b = TableId::from(234);
        let vnode_a = VirtualNode::COUNT_FOR_TEST - 1;
        let sstable_store = mock_sstable_store().await;
        let (sst, sst_info) = gen_test_sstable_with_table_ids(
            default_builder_opt_for_test(),
            1,
            [
                (
                    FullKey {
                        user_key: UserKey::for_test(
                            table_a,
                            prefix_slice_with_vnode(VirtualNode::from_index(vnode_a), b"table_a"),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(Bytes::from_static(b"a")),
                ),
                (
                    FullKey {
                        user_key: UserKey::for_test(
                            table_b,
                            prefix_slice_with_vnode(VirtualNode::ZERO, b"table_b"),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(Bytes::from_static(b"b")),
                ),
            ]
            .into_iter(),
            sstable_store.clone(),
            vec![table_a.as_raw_id(), table_b.as_raw_id()],
        )
        .await;
        assert_eq!(sst.block_count(), 2, "table switch must form a new block");

        let generate = |streaming_vnodes| {
            let sstable_store = sstable_store.clone();
            let sst = sst.clone();
            let sst_info = sst_info.clone();
            let mut config = test_refill_config(CacheRefillPolicy::Streaming);
            config.data_refill_levels.insert(0);
            let context = CacheRefillContext {
                config: Arc::new(config),
                meta_refill_concurrency: None,
                concurrency: Arc::new(tokio::sync::Semaphore::new(1)),
                sstable_store,
                table_cache_refill_context_map: Arc::new(HashMap::from([(
                    table_a,
                    super::TableCacheRefillContext {
                        streaming_vnode_bitmap: Some(streaming_vnodes),
                        serving_vnode_bitmap: None,
                        policy: CacheRefillPolicy::Streaming,
                    },
                )])),
            };
            async move {
                let generator = DataCacheRefillTaskGenerator {
                    context: &context,
                    delta: &SstDeltaInfo {
                        insert_sst_infos: vec![sst_info.clone()],
                        delete_sst_infos: vec![deleted_sst(2330.into())],
                        insert_sst_level: 0,
                    },
                    ssts: std::slice::from_ref(&sst),
                };
                let tasks = generator.generate_unfiltered_tasks();
                generator.filter_by_inheritance_if_needed(tasks).await
            }
        };

        let matching_tasks =
            generate(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [vnode_a])).await;
        assert_eq!(matching_tasks.len(), 1);
        assert_eq!(matching_tasks[0].blks, 0..1);

        let non_matching_tasks = generate(Bitmap::from_indices(
            VirtualNode::COUNT_FOR_TEST,
            [VirtualNode::ZERO.to_index()],
        ))
        .await;
        assert!(non_matching_tasks.is_empty());
    }

    #[tokio::test]
    async fn test_block_vnode_range_handles_vnode_only_block_boundaries() {
        let table_id = TableId::from(233);
        let vnode = VirtualNode::ZERO;
        let sstable_store = mock_sstable_store().await;
        let mut builder_options = default_builder_opt_for_test();
        builder_options.block_capacity = 1;
        let (sst, _) = gen_test_sstable_with_table_ids(
            builder_options,
            1,
            [234, 233].into_iter().map(|epoch| {
                (
                    FullKey {
                        user_key: UserKey::for_test(table_id, prefix_slice_with_vnode(vnode, b"")),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(epoch)),
                    },
                    HummockValue::put(Bytes::from_static(b"value")),
                )
            }),
            sstable_store.clone(),
            vec![table_id.as_raw_id()],
        )
        .await;
        assert_eq!(sst.block_count(), 2);
        let expected = (vnode.to_index(), vnode.to_index() + 1);
        assert_eq!(block_vnode_range(&sst, 0), expected);
        assert_eq!(block_vnode_range(&sst, 1), expected);
    }

    #[tokio::test]
    async fn test_block_vnode_range_fails_open_for_shortened_meta_keys() {
        let table_id = TableId::from(233);
        let sstable_store = mock_sstable_store().await;
        let mut builder_options = default_builder_opt_for_test();
        builder_options.block_capacity = 1;
        builder_options.shorten_block_meta_key_threshold = Some(0);
        let (sst, _) = gen_test_sstable_with_table_ids(
            builder_options,
            1,
            [255, 256].into_iter().map(|vnode| {
                (
                    FullKey {
                        user_key: UserKey::for_test(
                            table_id,
                            prefix_slice_with_vnode(VirtualNode::from_index(vnode), b"long-key"),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(Bytes::from_static(b"value")),
                )
            }),
            sstable_store,
            vec![table_id.as_raw_id()],
        )
        .await;
        assert_eq!(sst.block_count(), 2);
        assert!(
            FullKey::decode(&sst.meta.block_metas[1].smallest_key)
                .user_key
                .table_key
                .as_ref()
                .len()
                < VirtualNode::SIZE
        );
        let full_range = (0, VirtualNode::MAX_REPRESENTABLE.to_index() + 1);
        assert_eq!(block_vnode_range(&sst, 0), full_range);
        assert_eq!(block_vnode_range(&sst, 1), full_range);
    }

    #[tokio::test]
    async fn test_foyer_timeout_is_degraded_not_ready() {
        let table = TableId::from(233);
        let spawn: SpawnRefillTask = Arc::new(|plan, mut context, _, _| {
            let mut config = test_refill_config(CacheRefillPolicy::Enabled);
            config.timeout = Duration::from_millis(1);
            context.config = Arc::new(config);
            context.meta_refill_concurrency = Some(Arc::new(tokio::sync::Semaphore::new(0)));
            tokio::spawn(super::CacheRefillTask { plan, context }.run())
        });
        let mut config = test_refill_config(CacheRefillPolicy::Enabled);
        config.timeout = Duration::from_millis(30);
        let mut refiller = CacheRefiller::new(
            Role::Serving,
            config,
            mock_sstable_store().await,
            spawn,
            pinned_version_for_test(),
        );
        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: vec![
                    risingwave_hummock_sdk::sstable_info::SstableInfoInner {
                        table_ids: vec![table],
                        ..Default::default()
                    }
                    .into(),
                ],
                ..Default::default()
            }],
            pinned_version_for_test(),
            pinned_version_for_test(),
            PinCacheMembershipUpdate::Delta,
        );
        assert_eq!(refiller.next_events().await.len(), 1);
        assert_eq!(
            refiller.last_outcome,
            Some(super::RefillBatchOutcome::DegradedTimeout)
        );
    }

    #[tokio::test]
    async fn test_mixed_object_preserves_foyer_sibling_with_disjoint_pin_owner() {
        // Put the pinned table last: a table-switch separator deliberately fails open for
        // the previous table's final block, whereas largest_key gives an exact vnode end.
        let pinned = TableId::from(234);
        let sibling = TableId::from(233);
        let store = mock_sstable_store().await;
        let (sst, info) = gen_test_sstable_with_table_ids(
            default_builder_opt_for_test(),
            860,
            [sibling, pinned].into_iter().map(|table| {
                (
                    FullKey {
                        user_key: UserKey::for_test(table, iterator_test_table_key_of(0)),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
                    },
                    HummockValue::put(vec![1]),
                )
            }),
            store.clone(),
            vec![sibling.as_raw_id(), pinned.as_raw_id()],
        )
        .await;
        let cache = pin_cache_for_test();
        store.set_pin_cache(cache.clone());
        let spawn: SpawnRefillTask = Arc::new(move |plan, context, _, _| {
            let sst = sst.clone();
            tokio::spawn(async move {
                assert_eq!(
                    plan.deltas[0].insert_sst_infos.len(),
                    1,
                    "retain the physical object for its sibling"
                );
                let tasks = DataCacheRefillTaskGenerator {
                    context: &context,
                    delta: &plan.deltas[0],
                    ssts: std::slice::from_ref(&sst),
                }
                .generate_unfiltered_tasks();
                assert!(!tasks.is_empty());
                for task in tasks {
                    for index in task.blks {
                        assert_eq!(sst.meta.block_metas[index].table_id(), sibling);
                    }
                }
                true
            })
        });
        let mut config = test_refill_config(CacheRefillPolicy::Disabled);
        config.data_refill_levels.insert(0);
        let mut refiller = CacheRefiller::new(
            Role::Serving,
            config,
            store,
            spawn,
            pinned_version_for_test(),
        );
        refiller.replace_table_cache_refill_policies(
            [
                (pinned, CacheRefillPolicy::Pinned),
                (sibling, CacheRefillPolicy::Serving),
            ]
            .into(),
        );
        refiller.replace_serving_table_vnode_mapping(
            [
                (
                    pinned,
                    Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [128]),
                ),
                (sibling, Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            ]
            .into(),
        );
        refiller.start_cache_refill(
            vec![SstDeltaInfo {
                insert_sst_infos: vec![info.clone()],
                ..Default::default()
            }],
            pinned_version_for_test(),
            pinned_version_with_ssts(&[pinned, sibling], std::slice::from_ref(&info)),
            PinCacheMembershipUpdate::Delta,
        );
        assert_eq!(refiller.next_events().await.len(), 1);
        assert_eq!(
            refiller.last_outcome,
            Some(super::RefillBatchOutcome::Ready)
        );
        assert!(cache.get(info.object_id).is_none());
    }

    #[tokio::test]
    async fn test_pin_existing_sst_bootstraps_on_ownership_but_not_set() {
        let table = TableId::from(233);
        for cold_worker in [false, true] {
            let store = mock_sstable_store().await;
            let (_, info) = gen_test_sst_with_object_id(table, store.clone(), 820).await;
            let cache = pin_cache_for_test();
            store.set_pin_cache(cache.clone());
            let mut refiller = CacheRefiller::new(
                Role::Streaming,
                test_refill_config(CacheRefillPolicy::Disabled),
                store,
                CacheRefiller::default_spawn_refill_task(),
                pinned_version_with_sst(table, &info),
            );
            if !cold_worker {
                refiller.replace_table_cache_refill_policies(HashMap::new());
                refiller.update_streaming_table_vnodes(
                    table,
                    Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [0])),
                );
            }
            refiller
                .replace_table_cache_refill_policies([(table, CacheRefillPolicy::Pinned)].into());
            if !cold_worker {
                tokio::time::sleep(Duration::from_millis(20)).await;
                assert!(cache.get(info.object_id).is_none(), "SET alone is not Warm");
                refiller.update_streaming_table_vnodes(
                    table,
                    Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [128])),
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
                assert!(
                    cache.get(info.object_id).is_none(),
                    "non-overlapping owner must not download"
                );
            }
            refiller.update_streaming_table_vnodes(
                table,
                Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [0])),
            );
            tokio::time::timeout(Duration::from_secs(1), async {
                while cache.get(info.object_id).is_none() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert!(
                refiller.active.is_none() && refiller.pending.is_empty(),
                "ownership does not manufacture version events"
            );
            refiller.update_streaming_table_vnodes(
                table,
                Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [128])),
            );
            tokio::time::timeout(Duration::from_secs(1), async {
                while cache.get(info.object_id).is_some() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("losing block ownership must reclaim the former owner's local file");
        }
    }

    #[tokio::test]
    async fn test_recovery_reconciles_empty_and_latest_ownership() {
        let table = TableId::from(233);
        for replace_stale_reconcile in [false, true] {
            let store = mock_sstable_store().await;
            let (_, info) = gen_test_sst_with_object_id(table, store.clone(), 826).await;
            let local_store = pin_cache_store_for_test();
            let bytes = store
                .store()
                .read(&store.get_sst_data_path(info.object_id), ..)
                .await
                .unwrap();
            local_store
                .upload(&format!("{}-42.sst", info.object_id.as_raw_id()), bytes)
                .await
                .unwrap();
            let cache = PinCache::new(local_store, u64::MAX);
            cache.wait_for_recovery().await;
            store.set_pin_cache(cache.clone());

            let concurrency = Arc::new(tokio::sync::Semaphore::new(if replace_stale_reconcile {
                0
            } else {
                1
            }));
            let mut controller = PinCacheRefillController::new(
                store,
                pinned_version_with_sst(table, &info),
                concurrency.clone(),
            );
            controller.replace_policies(&[(table, CacheRefillPolicy::Pinned)].into());
            assert!(cache.get(info.object_id).is_some());

            if replace_stale_reconcile {
                controller.update_ownership(
                    [(
                        table,
                        Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [128]),
                    )]
                    .into(),
                    false,
                );
                tokio::task::yield_now().await;
                controller.update_ownership(
                    [(table, Bitmap::ones(VirtualNode::COUNT_FOR_TEST))].into(),
                    false,
                );
                concurrency.add_permits(1);
                tokio::time::sleep(Duration::from_millis(20)).await;
                assert!(
                    cache.get(info.object_id).is_some(),
                    "a stale delayed reconcile must not overwrite newer ownership"
                );
            } else {
                controller.update_ownership(HashMap::new(), false);
                tokio::time::timeout(Duration::from_secs(1), async {
                    while cache.get(info.object_id).is_some() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("initial empty ownership must remove recovered routes");
            }
        }
    }

    #[tokio::test]
    async fn test_pin_full_snapshot_waits_and_late_old_owner_cannot_publish() {
        let table = TableId::from(233);
        let store = mock_sstable_store().await;
        let (_, info) = gen_test_sst_with_object_id(table, store.clone(), 825).await;
        let cache = pin_cache_for_test();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        cache.set_refill_gate_for_test(gate.clone());
        store.set_pin_cache(cache.clone());
        let mut config = test_refill_config(CacheRefillPolicy::Disabled);
        config.timeout = Duration::from_secs(1);
        let mut refiller = CacheRefiller::new(
            Role::Streaming,
            config,
            store,
            CacheRefiller::default_spawn_refill_task(),
            pinned_version_for_test(),
        );
        refiller.replace_table_cache_refill_policies([(table, CacheRefillPolicy::Pinned)].into());
        refiller.update_streaming_table_vnodes(
            table,
            Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [0])),
        );
        refiller.start_cache_refill(
            vec![],
            pinned_version_for_test(),
            pinned_version_with_sst(table, &info),
            PinCacheMembershipUpdate::Rebuild,
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), refiller.next_events())
                .await
                .is_err()
        );
        refiller.update_streaming_table_vnodes(
            table,
            Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [128])),
        );
        gate.add_permits(1);
        assert_eq!(refiller.next_events().await.len(), 1);
        assert_eq!(
            refiller.last_outcome,
            Some(super::RefillBatchOutcome::DegradedError)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            cache.get(info.object_id).is_none(),
            "late old-owner completion cannot publish"
        );
        refiller.update_streaming_table_vnodes(
            table,
            Some(Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [0])),
        );
        gate.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), async {
            while cache.get(info.object_id).is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_pin_capacity_debt_recovers_without_another_version_delta() {
        let table = TableId::from(233);
        let store = mock_sstable_store().await;
        let (_, blocker) = gen_test_sst_with_object_id(table, store.clone(), 830).await;
        let (_, target) = gen_test_sst_with_object_id(table, store.clone(), 831).await;
        let cache = PinCache::new(
            pin_cache_store_for_test(),
            blocker.file_size.max(target.file_size),
        );
        cache.replace_desired_objects([(blocker.object_id, blocker.file_size)]);
        cache
            .pin_sst(
                store.store(),
                store.get_sst_data_path(blocker.object_id),
                blocker.object_id,
            )
            .await
            .unwrap();
        store.set_pin_cache(cache.clone());
        let version = pinned_version_with_ssts(&[table], &[blocker.clone(), target.clone()]);
        let mut refiller = CacheRefiller::new(
            Role::Streaming,
            test_refill_config(CacheRefillPolicy::Disabled),
            store,
            CacheRefiller::default_spawn_refill_task(),
            version,
        );
        refiller.replace_table_cache_refill_policies([(table, CacheRefillPolicy::Pinned)].into());
        refiller
            .update_streaming_table_vnodes(table, Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(cache.get(target.object_id).is_none());
        // Reclaim capacity, but create no new version/ownership trigger for the failed target.
        cache.get(blocker.object_id).unwrap().invalidate();
        tokio::time::timeout(Duration::from_secs(2), async {
            while cache.get(target.object_id).is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            cache.get(blocker.object_id).is_none(),
            "an ordinary local failure is not read-through repair"
        );
    }

    #[tokio::test]
    async fn test_pin_version_gate_ready_and_timeout_does_not_cancel_upload() {
        for timeout in [false, true] {
            let table = TableId::from(233);
            let store = mock_sstable_store().await;
            let (_, info) = gen_test_sst_with_object_id(table, store.clone(), 810).await;
            let cache = pin_cache_for_test();
            let gate = Arc::new(tokio::sync::Semaphore::new(0));
            cache.set_refill_gate_for_test(gate.clone());
            store.set_pin_cache(cache.clone());
            let mut config = test_refill_config(CacheRefillPolicy::Disabled);
            config.timeout = Duration::from_millis(50);
            let mut refiller = CacheRefiller::new(
                Role::Streaming,
                config,
                store,
                CacheRefiller::default_spawn_refill_task(),
                pinned_version_for_test(),
            );
            refiller
                .replace_table_cache_refill_policies([(table, CacheRefillPolicy::Pinned)].into());
            refiller.update_streaming_table_vnodes(
                table,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            );
            refiller.start_cache_refill(
                vec![SstDeltaInfo {
                    insert_sst_infos: vec![info.clone()],
                    ..Default::default()
                }],
                pinned_version_for_test(),
                pinned_version_with_sst(table, &info),
                PinCacheMembershipUpdate::Delta,
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(5), refiller.next_events())
                    .await
                    .is_err()
            );
            assert!(cache.get(info.object_id).is_none());
            if !timeout {
                gate.add_permits(1);
            }
            assert_eq!(refiller.next_events().await.len(), 1);
            assert_eq!(
                refiller.last_outcome,
                Some(if timeout {
                    super::RefillBatchOutcome::DegradedTimeout
                } else {
                    super::RefillBatchOutcome::Ready
                })
            );
            if timeout {
                assert!(cache.get(info.object_id).is_none());
                gate.add_permits(1);
                tokio::time::timeout(Duration::from_secs(1), async {
                    while cache.get(info.object_id).is_none() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
            assert!(cache.get(info.object_id).is_some());
        }
    }

    #[tokio::test]
    async fn test_pending_batch_preserves_ordered_metadata_events() {
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let worker_gate = gate.clone();
        let spawn: SpawnRefillTask = Arc::new(move |_, _, _, _| {
            let gate = worker_gate.clone();
            tokio::spawn(async move {
                gate.acquire().await.unwrap().forget();
                true
            })
        });
        let base = pinned_version_for_test();
        let mut refiller = CacheRefiller::new(
            Role::Streaming,
            test_refill_config(CacheRefillPolicy::Disabled),
            mock_sstable_store().await,
            spawn,
            base.clone(),
        );
        let mut previous = base;
        for id in 1..=3 {
            let mut version = (*previous).clone();
            version.id = id.into();
            let next = previous.new_with_local_version(version).unwrap();
            refiller.start_cache_refill(
                vec![],
                previous,
                next.clone(),
                PinCacheMembershipUpdate::Delta,
            );
            previous = next;
        }
        gate.add_permits(1);
        let first = refiller.next_events().await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].new_pinned_version.id().as_raw_id(), 1);
        gate.add_permits(2);
        let rest = refiller.next_events().await;
        assert_eq!(
            rest.iter()
                .map(|event| event.new_pinned_version.id().as_raw_id())
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(rest[0].pinned_version.id().as_raw_id(), 1);
        assert_eq!(rest[1].pinned_version.id().as_raw_id(), 2);
    }

    #[tokio::test]
    async fn test_pin_pending_compacts_only_noncommitted_intermediates() {
        let table = TableId::from(233);
        for committed in [false, true] {
            let store = mock_sstable_store().await;
            let (_, middle) = gen_test_sst_with_object_id(table, store.clone(), 840).await;
            let (_, last) = gen_test_sst_with_object_id(table, store.clone(), 841).await;
            let cache = pin_cache_for_test();
            store.set_pin_cache(cache.clone());
            let base = pinned_version_with_ssts(&[table], &[]);
            let make_version = |info: &SstableInfo| {
                let mut pb = pinned_version_with_sst(table, info).to_protobuf();
                pb.state_table_info.get_mut(&table).unwrap().committed_epoch = u64::from(committed);
                PinnedVersion::new(
                    HummockVersion::from_rpc_protobuf(&pb),
                    unbounded_channel().0,
                )
            };
            let middle_version = make_version(&middle);
            let last_version = make_version(&last);
            let foyer_gate = Arc::new(tokio::sync::Semaphore::new(0));
            let worker_gate = foyer_gate.clone();
            let spawn: SpawnRefillTask = Arc::new(move |_, _, _, _| {
                let gate = worker_gate.clone();
                tokio::spawn(async move {
                    gate.acquire().await.unwrap().forget();
                    true
                })
            });
            let mut config = test_refill_config(CacheRefillPolicy::Disabled);
            config.timeout = Duration::from_secs(1);
            let mut refiller =
                CacheRefiller::new(Role::Streaming, config, store, spawn, base.clone());
            refiller
                .replace_table_cache_refill_policies([(table, CacheRefillPolicy::Pinned)].into());
            refiller.update_streaming_table_vnodes(
                table,
                Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)),
            );
            refiller.start_cache_refill(
                vec![],
                base.clone(),
                base.clone(),
                PinCacheMembershipUpdate::Delta,
            );
            refiller.start_cache_refill(
                vec![SstDeltaInfo {
                    insert_sst_infos: vec![middle.clone()],
                    ..Default::default()
                }],
                base,
                middle_version.clone(),
                PinCacheMembershipUpdate::Delta,
            );
            refiller.start_cache_refill(
                vec![SstDeltaInfo {
                    insert_sst_infos: vec![last.clone()],
                    delete_sst_infos: vec![middle.clone()],
                    ..Default::default()
                }],
                middle_version,
                last_version,
                PinCacheMembershipUpdate::Delta,
            );
            foyer_gate.add_permits(3);
            assert_eq!(refiller.next_events().await.len(), 1);
            assert_eq!(refiller.next_events().await.len(), 2);
            assert_eq!(
                refiller.last_outcome,
                Some(super::RefillBatchOutcome::Ready)
            );
            assert!(cache.get(last.object_id).is_some());
            assert_eq!(
                cache.get(middle.object_id).is_some(),
                committed,
                "only commit-visible intermediates require admission"
            );
        }
    }

    #[tokio::test]
    async fn test_pin_projection_uses_owned_blocks_and_deduplicates_whole_object() {
        use crate::hummock::pin_cache_refill::PinCacheRefillPlan;
        let table = TableId::from(233);
        let store = mock_sstable_store().await;
        let mut options = default_builder_opt_for_test();
        options.block_capacity = 1;
        let (sst, info) = gen_test_sstable_with_table_ids(
            options,
            701,
            [0, 128].into_iter().map(|vnode| {
                (
                    FullKey {
                        user_key: UserKey::for_test(
                            table,
                            prefix_slice_with_vnode(VirtualNode::from_index(vnode), b"key"),
                        ),
                        epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(233)),
                    },
                    HummockValue::put(Bytes::from_static(b"value")),
                )
            }),
            store,
            vec![table.as_raw_id()],
        )
        .await;
        let projections = vec![info.clone(), info.clone()];
        for (vnode, expected) in [(0, true), (128, true), (255, false)] {
            let ownership = HashMap::from([(
                table,
                Bitmap::from_indices(VirtualNode::COUNT_FOR_TEST, [vnode]),
            )]);
            assert_eq!(
                PinCacheRefillPlan::owns_object(&sst, &projections, &ownership),
                expected
            );
            let plan = PinCacheRefillPlan::new(
                &[SstDeltaInfo {
                    insert_sst_infos: projections.clone(),
                    ..Default::default()
                }],
                &[info.object_id].into(),
                ownership,
            );
            assert_eq!(plan.objects.len(), 1, "physical downloads are deduplicated");
            let mut stats = PinRefillPlanStats::default();
            stats.add_plan(&plan);
            assert_eq!(stats.objects, 1);
            assert_eq!(stats.bytes, info.file_size);
        }
        assert!(!PinCacheRefillPlan::owns_object(
            &sst,
            &projections,
            &HashMap::new()
        ));
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
