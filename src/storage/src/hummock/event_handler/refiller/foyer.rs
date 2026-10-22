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

//! Executes Foyer refill tasks from snapshots captured before spawning.

use std::collections::HashSet;
use std::hash::Hash;
use std::ops::{Bound, Range};
use std::sync::Arc;
use std::time::Instant;

use ::foyer::RangeBoundsExt;
use futures::future::{join_all, try_join_all};
use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::VirtualNode;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::key::{FullKey, vnode_range};
use risingwave_hummock_sdk::{HummockSstableObjectId, KeyComparator};
use thiserror_ext::AsReport;

use super::{CacheRefillContext, SpawnRefillTask, TableCacheRefillContext};
use crate::hummock::{
    Block, HummockError, HummockResult, RecentFilterTrait, Sstable, SstableBlockIndex, TableHolder,
};
use crate::monitor::{GLOBAL_CACHE_REFILL_METRICS, StoreLocalStatistic};

pub(super) fn default_spawn_refill_task() -> SpawnRefillTask {
    Arc::new(|deltas, context| {
        let task = CacheRefillTask { deltas, context };
        tokio::spawn(task.run())
    })
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
    let block_meta = &sstable.meta.block_metas[block_index];
    let block_smallest_key = FullKey::decode(&block_meta.smallest_key);
    let table_key_end = match sstable.meta.block_metas.get(block_index + 1) {
        // A table switch always starts a new block. The next table's smallest key has an
        // unrelated vnode, so use the current table's terminal range instead.
        Some(next_block_meta) if next_block_meta.table_id() != block_meta.table_id() => {
            Bound::Unbounded
        }
        // Full-key versions of the same table key may span adjacent blocks. After projecting
        // away the epoch, the boundary vnode therefore remains part of the current block.
        Some(next_block_meta) => Bound::Included(
            FullKey::decode(&next_block_meta.smallest_key)
                .user_key
                .table_key,
        ),
        // `SstableMeta::largest_key` is the actual last key, unlike the next block's smallest
        // key above. Keep it inclusive, especially for singleton tables whose key contains only
        // the vnode prefix.
        None => Bound::Included(
            FullKey::decode(&sstable.meta.largest_key)
                .user_key
                .table_key,
        ),
    };

    let table_key_range = (
        Bound::Included(block_smallest_key.user_key.table_key),
        table_key_end,
    );
    // Block-meta separators may shorten the table key below the vnode prefix. They are valid
    // full-key search boundaries but cannot identify a vnode, so fail open instead of panicking
    // or dropping a block that may belong to this worker.
    if match &table_key_range.0 {
        Bound::Included(key) | Bound::Excluded(key) => key.as_ref().len() < VirtualNode::SIZE,
        Bound::Unbounded => false,
    } || match &table_key_range.1 {
        Bound::Included(key) | Bound::Excluded(key) => key.as_ref().len() < VirtualNode::SIZE,
        Bound::Unbounded => false,
    } {
        return (0, VirtualNode::MAX_REPRESENTABLE.to_index() + 1);
    }
    vnode_range(&table_key_range)
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

        let has_parent_ssts = !self.delta.delete_sst_object_ids.is_empty();
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
                .inc_by(self.delta.delete_sst_object_ids.len() as u64);
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
            && !self.delta.delete_sst_object_ids.is_empty()
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
                    Self::data_cache_refill(&context, tasks).await;
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

#[cfg(test)]
mod tests;
