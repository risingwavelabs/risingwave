// Copyright 2026 RisingWave Labs
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

use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use ::foyer::RangeBoundsExt;
use futures::future::{join_all, try_join_all};
use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::{HummockSstableObjectId, KeyComparator};
use risingwave_pb::id::TableId;
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use super::CacheRefillConfig;
use crate::hummock::refill_locality::{block_vnode_range, vnode_range_overlaps_bitmap};
use crate::hummock::table_cache_metadata::TableCacheMetadata;
use crate::hummock::{
    Block, HummockError, HummockResult, RecentFilterTrait, Sstable, SstableBlockIndex,
    SstableStoreRef, TableHolder,
};
use crate::monitor::{GLOBAL_CACHE_REFILL_METRICS, StoreLocalStatistic};

#[cfg(test)]
pub(super) type SpawnRefillTask =
    Arc<dyn Fn(Vec<SstDeltaInfo>, CacheRefillContext) -> JoinHandle<()> + Send + Sync + 'static>;

#[cfg(test)]
pub(super) fn default_spawn_refill_task() -> SpawnRefillTask {
    Arc::new(spawn_refill_task)
}

fn spawn_refill_task(plan: Vec<SstDeltaInfo>, context: CacheRefillContext) -> JoinHandle<()> {
    let timeout = context.config.timeout;
    let task = CacheRefillTask { plan, context };
    tokio::spawn(async move {
        if tokio::time::timeout(timeout, task.run()).await.is_err() {
            tracing::warn!("Foyer cache refill timed out");
        }
    })
}

/// Owns Foyer admission, immutable task inputs and per-event attempts.
/// Table policy/ownership is borrowed only while starting an attempt.
pub(super) struct FoyerRefiller {
    #[cfg(test)]
    pub(super) spawn_refill_task: Option<SpawnRefillTask>,
    config: Arc<CacheRefillConfig>,
    pub(super) meta_refill_concurrency: Option<Arc<Semaphore>>,
    pub(super) concurrency: Arc<Semaphore>,
    sstable_store: SstableStoreRef,
    // Keep main's detached-on-drop and JoinError propagation when no Pin backend is installed.
    legacy: bool,
}

impl FoyerRefiller {
    pub(super) fn new(
        config: Arc<CacheRefillConfig>,
        sstable_store: SstableStoreRef,
        meta_refill_concurrency: Option<Arc<Semaphore>>,
        concurrency: Arc<Semaphore>,
        legacy: bool,
    ) -> Self {
        Self {
            #[cfg(test)]
            spawn_refill_task: None,
            config,
            meta_refill_concurrency,
            concurrency,
            sstable_store,
            legacy,
        }
    }

    /// Starts immediately; the returned future only waits for this event's attempt.
    pub(super) fn start_cache_refill(
        &self,
        mut deltas: Vec<SstDeltaInfo>,
        metadata: &TableCacheMetadata,
    ) -> impl Future<Output = ()> + Send + 'static + use<> {
        // A physical object can contain both pinned and ordinary tables. Preserve it here;
        // the immutable per-table Foyer context excludes only its pinned projection.
        for delta in &mut deltas {
            let for_serving = metadata.role.for_serving();
            // Writer-appended L0 SSTs are already warm on the streaming side. Their data refill
            // may therefore only be needed by serving workers.
            let for_streaming = metadata.role.for_streaming() && !delta.delete_sst_infos.is_empty();
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
                    let policy = metadata
                        .table_cache_refill_policies
                        .get(table_id)
                        .copied()
                        .unwrap_or(metadata.default_policy);

                    // Enabled preserves legacy full refill. For scoped policies, mapping keys are
                    // only a whole-SST coarse gate; bitmap bits still filter blocks post-Meta.
                    if metadata.is_pinned(table_id) {
                        return false;
                    }
                    match policy {
                        CacheRefillPolicy::Enabled => for_streaming || for_serving,
                        CacheRefillPolicy::Disabled => false,
                        CacheRefillPolicy::Streaming => {
                            for_streaming
                                && metadata
                                    .streaming_table_vnode_mapping
                                    .contains_key(table_id)
                        }
                        CacheRefillPolicy::Serving => {
                            for_serving
                                && metadata.serving_table_vnode_mapping.contains_key(table_id)
                        }
                        CacheRefillPolicy::Both => {
                            (for_streaming
                                && metadata
                                    .streaming_table_vnode_mapping
                                    .contains_key(table_id))
                                || (for_serving
                                    && metadata.serving_table_vnode_mapping.contains_key(table_id))
                        }
                    }
                })
            });
        }
        let context = self.new_cache_refill_context(&deltas, metadata);
        #[cfg(test)]
        let handle = match &self.spawn_refill_task {
            Some(spawn) => spawn(deltas, context),
            None => spawn_refill_task(deltas, context),
        };
        #[cfg(not(test))]
        let handle = spawn_refill_task(deltas, context);
        let legacy = self.legacy;
        let mut handle = scopeguard::guard(handle, move |handle| {
            if !legacy {
                handle.abort();
            }
        });
        async move {
            let result = (&mut *handle).await;
            if legacy {
                result.unwrap();
            } else if let Err(error) = result {
                tracing::warn!(error = %error.as_report(), "Foyer refill task failed; using remote fallback");
            }
        }
    }

    fn new_cache_refill_context(
        &self,
        deltas: &[SstDeltaInfo],
        metadata: &TableCacheMetadata,
    ) -> CacheRefillContext {
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
            table_cache_refill_context_map: table_cache_refill_contexts(metadata, table_ids),
        }
    }
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

pub(crate) struct CacheRefillContext {
    pub(super) config: Arc<CacheRefillConfig>,
    pub(super) meta_refill_concurrency: Option<Arc<Semaphore>>,
    pub(super) concurrency: Arc<Semaphore>,
    pub(super) sstable_store: SstableStoreRef,
    pub(super) table_cache_refill_context_map: TableCacheRefillContextMap,
}

pub(super) struct DataCacheRefillTaskGenerator<'a> {
    pub(super) context: &'a CacheRefillContext,
    pub(super) delta: &'a SstDeltaInfo,
    pub(super) ssts: &'a [TableHolder],
}

impl DataCacheRefillTaskGenerator<'_> {
    pub(super) fn generate_unfiltered_tasks(&self) -> Vec<DataCacheRefillTask> {
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
        let sstable_store = &self.context.sstable_store;
        let futures = self.delta.delete_sst_infos.iter().map(|sst| {
            let sst_obj_id = sst.object_id;
            async move {
                let res = sstable_store.sstable_cached(sst_obj_id).await;
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
pub(super) struct DataCacheRefillTask {
    pub(super) sst: TableHolder,
    pub(super) blks: Range<usize>,
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
    plan: Vec<SstDeltaInfo>,
    context: CacheRefillContext,
}

impl CacheRefillTask {
    async fn run(self) {
        let tasks = self
            .plan
            .iter()
            .map(|delta| {
                let context = &self.context;
                async move {
                    let holders = match Self::meta_cache_refill(context, delta).await {
                        Ok(holders) => holders,
                        Err(e) => {
                            tracing::warn!(error = %e.as_report(), "meta cache refill error");
                            return;
                        }
                    };
                    let generator = DataCacheRefillTaskGenerator {
                        context,
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
                    Self::data_cache_refill(context, tasks).await;
                }
            })
            .collect_vec();
        // Await every Foyer delta. Its task timeout bounds this work independently of Pin.
        join_all(tasks).await;
    }

    async fn meta_cache_refill(
        context: &CacheRefillContext,
        delta: &SstDeltaInfo,
    ) -> HummockResult<Vec<TableHolder>> {
        let tasks = delta.insert_sst_infos.iter().map(|info| async {
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
        });
        try_join_all(tasks).await
    }

    async fn data_cache_refill(context: &CacheRefillContext, tasks: Vec<DataCacheRefillTask>) {
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
                let sstable_store = &context.sstable_store;
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
            }
        });

        join_all(futures).await;
    }
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

pub type TableCacheRefillContextMap = HashMap<TableId, TableCacheRefillContext>;

/// Immutable table inputs captured for an asynchronous Foyer attempt.
#[derive(Clone)]
pub struct TableCacheRefillContext {
    pub streaming_vnode_bitmap: Option<Bitmap>,
    pub serving_vnode_bitmap: Option<Bitmap>,
    pub policy: CacheRefillPolicy,
}

/// A read-only view of CN metadata for monitor/debugging APIs.
#[derive(Clone)]
pub struct TableCacheRefillMonitorSnapshot {
    pub contexts: TableCacheRefillContextMap,
    pub policies: HashMap<TableId, CacheRefillPolicy>,
    pub default_policy: CacheRefillPolicy,
    pub streaming_table_vnode_mapping: HashMap<TableId, Bitmap>,
    pub serving_table_vnode_mapping: HashMap<TableId, Bitmap>,
}

pub(super) fn table_cache_refill_contexts(
    metadata: &TableCacheMetadata,
    table_ids: impl IntoIterator<Item = TableId>,
) -> TableCacheRefillContextMap {
    let ownership = metadata.ownership();
    let mut contexts = HashMap::new();
    for table_id in table_ids {
        if metadata.is_pinned(&table_id)
            || ownership.serving.is_some()
                && ownership.streaming.is_none()
                && !metadata.serving_table_vnode_mapping.contains_key(&table_id)
        {
            continue;
        }
        contexts.entry(table_id).or_insert_with(|| {
            let policy = metadata
                .table_cache_refill_policies
                .get(&table_id)
                .copied()
                .unwrap_or(metadata.default_policy);
            let streaming_vnode_bitmap = ownership
                .streaming
                .filter(|_| policy.is_streaming_scoped())
                .and_then(|map| map.get(&table_id))
                .cloned();
            // `Enabled` normally does not use bitmap filtering. The only exception is L0
            // insert-only refill, where serving workers still need serving-locality evidence.
            let serving_vnode_bitmap = ownership
                .serving
                .filter(|_| policy.is_serving_scoped() || policy.is_unscoped_enabled())
                .and_then(|map| map.get(&table_id))
                .cloned();
            TableCacheRefillContext {
                streaming_vnode_bitmap,
                serving_vnode_bitmap,
                policy,
            }
        });
    }
    contexts
}

pub(crate) fn table_cache_refill_monitor_snapshot(
    metadata: &TableCacheMetadata,
) -> TableCacheRefillMonitorSnapshot {
    let tables = metadata
        .table_cache_refill_policies
        .keys()
        .chain(metadata.streaming_table_vnode_mapping.keys())
        .chain(metadata.serving_table_vnode_mapping.keys())
        .copied();
    TableCacheRefillMonitorSnapshot {
        contexts: table_cache_refill_contexts(metadata, tables),
        policies: metadata.table_cache_refill_policies.clone(),
        default_policy: metadata.default_policy,
        streaming_table_vnode_mapping: metadata.streaming_table_vnode_mapping.clone(),
        serving_table_vnode_mapping: metadata.serving_table_vnode_mapping.clone(),
    }
}

#[cfg(test)]
mod tests;
