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
use std::sync::Arc;

use risingwave_common::bitmap::Bitmap;
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::id::TableId;

use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::pin_cache::PinCacheRefillOutcome;
use crate::hummock::refill_locality::{block_vnode_range, vnode_range_overlaps_bitmap};
use crate::hummock::{HummockResult, Sstable, SstableStoreRef};
use crate::monitor::StoreLocalStatistic;

mod executor;
use executor::{PinCacheRefillExecutor, Ticket};

/// Immutable worker-local admission. A matching block admits the complete physical object.
#[derive(Clone, Default)]
pub(crate) struct PinCacheRefillPlan {
    pub objects: HashMap<HummockSstableObjectId, Vec<SstableInfo>>,
    pub ownership: Arc<HashMap<TableId, Bitmap>>,
}

impl PinCacheRefillPlan {
    pub(crate) fn new(
        deltas: &[SstDeltaInfo],
        candidates: &HashSet<HummockSstableObjectId>,
        ownership: HashMap<TableId, Bitmap>,
    ) -> Self {
        let mut objects: HashMap<_, Vec<_>> = HashMap::new();
        for sst in deltas.iter().flat_map(|delta| &delta.insert_sst_infos) {
            if candidates.contains(&sst.object_id)
                && sst
                    .table_ids
                    .iter()
                    .any(|table| ownership.contains_key(table))
            {
                objects.entry(sst.object_id).or_default().push(sst.clone());
            }
        }
        Self {
            objects,
            ownership: Arc::new(ownership),
        }
    }

    pub(crate) fn owns_object(
        sst: &Sstable,
        projections: &[SstableInfo],
        ownership: &HashMap<TableId, Bitmap>,
    ) -> bool {
        sst.meta
            .block_metas
            .iter()
            .enumerate()
            .any(|(index, block)| {
                let table = block.table_id();
                projections
                    .iter()
                    .any(|info| info.table_ids.contains(&table))
                    && ownership.get(&table).is_some_and(|bitmap| {
                        vnode_range_overlaps_bitmap(block_vnode_range(sst, index), bitmap)
                    })
            })
    }
}

async fn refill_pin_cache_object(
    store: &SstableStoreRef,
    projections: &[SstableInfo],
    ownership: &HashMap<TableId, Bitmap>,
    generation: u64,
) -> HummockResult<PinCacheRefillOutcome> {
    let Some(info) = projections.first() else {
        return Ok(PinCacheRefillOutcome::Obsolete);
    };
    let mut stats = StoreLocalStatistic::default();
    let sst = store.sstable(info, &mut stats).await;
    stats.discard();
    let sst = sst?;
    if !PinCacheRefillPlan::owns_object(&sst, projections, ownership) {
        return Ok(PinCacheRefillOutcome::Obsolete);
    }
    store
        .pin_sst_at_generation(info.object_id, generation)
        .await
}

#[derive(Clone, Copy)]
pub(crate) enum PinCacheMembershipUpdate {
    Delta,
    Rebuild,
}

/// Maintains logical pin-cache membership separately from Foyer block-refill policy.
pub(crate) struct PinCacheRefillController {
    sstable_store: SstableStoreRef,
    pinned_table_ids: Option<HashSet<TableId>>,
    object_ref_counts: HashMap<HummockSstableObjectId, u32>,
    version: PinnedVersion,
    ownership: Arc<HashMap<TableId, Bitmap>>,
    executor: PinCacheRefillExecutor,
}

impl PinCacheRefillController {
    pub(crate) fn new(
        sstable_store: SstableStoreRef,
        version: PinnedVersion,
        concurrency: usize,
    ) -> Self {
        let executor = PinCacheRefillExecutor::new(sstable_store.clone(), concurrency);
        Self {
            sstable_store,
            pinned_table_ids: None,
            object_ref_counts: HashMap::new(),
            version,
            ownership: Arc::default(),
            executor,
        }
    }

    pub(crate) fn replace_policies(
        &mut self,
        policies: &HashMap<TableId, CacheRefillPolicy>,
    ) -> bool {
        let initial = self.pinned_table_ids.is_none();
        let pinned_table_ids = policies
            .iter()
            .filter_map(|(&table_id, policy)| policy.is_pinned().then_some(table_id))
            .collect();
        if self.pinned_table_ids.as_ref() == Some(&pinned_table_ids) {
            return false;
        }
        self.pinned_table_ids = Some(pinned_table_ids);
        self.rebuild_desired_objects(false);
        initial
    }

    pub(crate) fn update_ownership(
        &mut self,
        ownership: HashMap<TableId, Bitmap>,
        bootstrap: bool,
    ) {
        let changed = self.ownership.as_ref() != &ownership;
        self.ownership = Arc::new(ownership);
        self.executor.reproject(self.ownership.clone());
        if !bootstrap || !changed {
            return;
        }
        self.bootstrap_owned_objects();
    }

    fn bootstrap_owned_objects(&self) {
        self.executor.submit(self.live_objects_plan());
    }

    pub(crate) fn live_objects_plan(&self) -> PinCacheRefillPlan {
        // Ownership acquisition is a separate trigger from SET: new workers and newly acquired
        // vnodes must cover already-live SSTs even if no future version delta arrives.
        let inserts = self
            .version
            .levels
            .values()
            .flat_map(|levels| levels.l0.sub_levels.iter().chain(&levels.levels))
            .flat_map(|level| &level.table_infos)
            .filter(|sst| self.object_ref_counts.contains_key(&sst.object_id))
            .cloned()
            .collect();
        let plan = PinCacheRefillPlan::new(
            &[SstDeltaInfo {
                insert_sst_infos: inserts,
                ..Default::default()
            }],
            &self.object_ref_counts.keys().copied().collect(),
            self.ownership.as_ref().clone(),
        );
        // Project every logical reference before retiring a shared physical object.
        if let Some(cache) = self.sstable_store.pin_cache() {
            for &object in self
                .object_ref_counts
                .keys()
                .filter(|object| !plan.objects.contains_key(*object))
            {
                cache.revoke_inflight(object);
                if let Some(route) = cache.get(object) {
                    route.invalidate();
                }
            }
        }
        plan
    }

    pub(crate) fn submit(&self, mut plan: PinCacheRefillPlan) -> Ticket {
        // Retain policy admission while projecting unstarted work onto current ownership.
        plan.ownership = Arc::new(
            self.ownership
                .iter()
                .filter(|(table, _)| plan.ownership.contains_key(*table))
                .map(|(&table, bitmap)| (table, bitmap.clone()))
                .collect(),
        );
        plan.objects.retain(|_, infos| {
            infos.iter().any(|info| {
                info.table_ids
                    .iter()
                    .any(|table| plan.ownership.contains_key(table))
            })
        });
        self.executor.submit(plan)
    }

    pub(crate) fn on_version_applied(&self, version: risingwave_hummock_sdk::HummockVersionId) {
        if let Some(cache) = self.sstable_store.pin_cache() {
            cache.release_retired(version);
        }
        self.executor.reproject(self.ownership.clone());
    }

    pub(crate) fn apply_version_update(
        &mut self,
        deltas: &[SstDeltaInfo],
        new_version: PinnedVersion,
        membership_update: PinCacheMembershipUpdate,
    ) -> HashSet<HummockSstableObjectId> {
        self.version = new_version;
        let Some(pin_cache) = self.sstable_store.pin_cache().cloned() else {
            return HashSet::new();
        };
        match membership_update {
            PinCacheMembershipUpdate::Delta => {
                if self.apply_desired_object_delta(deltas).is_none() {
                    tracing::warn!(
                        "pin-cache object reference count is inconsistent; rebuilding membership"
                    );
                    self.rebuild_desired_objects(true);
                }
            }
            PinCacheMembershipUpdate::Rebuild => {
                self.rebuild_desired_objects(true);
            }
        };

        deltas
            .iter()
            .flat_map(|delta| &delta.insert_sst_infos)
            .filter(|sst| pin_cache.is_desired(sst.object_id))
            .map(|sst| sst.object_id)
            .collect()
    }

    fn rebuild_desired_objects(
        &mut self,
        preserve_versions: bool,
    ) -> HashSet<HummockSstableObjectId> {
        let Some(pin_cache) = self.sstable_store.pin_cache().cloned() else {
            self.object_ref_counts.clear();
            return HashSet::new();
        };
        let Some(pinned_table_ids) = &self.pinned_table_ids else {
            self.object_ref_counts.clear();
            return HashSet::new();
        };

        let compaction_group_ids = pinned_table_ids
            .iter()
            .filter_map(|table_id| {
                self.version
                    .state_table_info
                    .info()
                    .get(table_id)
                    .map(|info| info.compaction_group_id)
            })
            .collect::<HashSet<_>>();
        let mut objects = HashMap::new();
        let mut object_ref_counts = HashMap::new();
        for sst in compaction_group_ids
            .into_iter()
            .flat_map(|compaction_group_id| {
                let levels = self
                    .version
                    .get_compaction_group_levels(compaction_group_id);
                levels.l0.sub_levels.iter().chain(levels.levels.iter())
            })
            .flat_map(|level| &level.table_infos)
            .filter(|sst| Self::is_pinned(sst, pinned_table_ids))
        {
            objects
                .entry(sst.object_id)
                .and_modify(|size| {
                    assert_eq!(
                        *size, sst.file_size,
                        "one object must have one physical size"
                    )
                })
                .or_insert(sst.file_size);
            *object_ref_counts.entry(sst.object_id).or_insert(0) += 1;
        }
        if preserve_versions {
            pin_cache.replace_version_objects(objects.clone());
        } else {
            pin_cache.replace_desired_objects(objects.iter().map(|(&id, &size)| (id, size)));
        }
        self.object_ref_counts = object_ref_counts;
        objects.into_keys().collect()
    }

    fn apply_desired_object_delta(
        &mut self,
        deltas: &[SstDeltaInfo],
    ) -> Option<(
        HashSet<HummockSstableObjectId>,
        HashSet<HummockSstableObjectId>,
    )> {
        let Some(pin_cache) = self.sstable_store.pin_cache().cloned() else {
            return Some((HashSet::new(), HashSet::new()));
        };
        let Some(pinned_table_ids) = &self.pinned_table_ids else {
            return Some((HashSet::new(), HashSet::new()));
        };

        let mut initial_counts = HashMap::new();
        let mut inserted_sizes = HashMap::new();
        for delta in deltas {
            for sst in delta
                .delete_sst_infos
                .iter()
                .filter(|sst| Self::is_pinned(sst, pinned_table_ids))
            {
                let count = self.object_ref_counts.get_mut(&sst.object_id)?;
                initial_counts.entry(sst.object_id).or_insert(*count);
                *count = count.checked_sub(1)?;
                if *count == 0 {
                    self.object_ref_counts.remove(&sst.object_id);
                }
            }
            for sst in delta
                .insert_sst_infos
                .iter()
                .filter(|sst| Self::is_pinned(sst, pinned_table_ids))
            {
                let count = self.object_ref_counts.entry(sst.object_id).or_insert(0);
                initial_counts.entry(sst.object_id).or_insert(*count);
                *count = count.checked_add(1)?;
                inserted_sizes
                    .entry(sst.object_id)
                    .and_modify(|size| {
                        assert_eq!(
                            *size, sst.file_size,
                            "one object must have one physical size"
                        )
                    })
                    .or_insert(sst.file_size);
            }
        }

        let mut removed = HashSet::new();
        let mut inserted = HashMap::new();
        let mut refill_objects = HashSet::new();
        for (object_id, initial_count) in initial_counts {
            let final_count = self.object_ref_counts.get(&object_id).copied().unwrap_or(0);
            if initial_count > 0 && final_count == 0 {
                removed.insert(object_id);
            }
            if final_count > initial_count {
                refill_objects.insert(object_id);
            }
            if initial_count == 0 && final_count > 0 {
                inserted.insert(object_id, inserted_sizes[&object_id]);
            }
        }
        pin_cache.apply_desired_object_delta(removed.iter().copied(), inserted);
        Some((removed, refill_objects))
    }

    fn is_pinned(sst: &SstableInfo, pinned_table_ids: &HashSet<TableId>) -> bool {
        sst.table_ids
            .iter()
            .any(|table_id| pinned_table_ids.contains(table_id))
    }
}
