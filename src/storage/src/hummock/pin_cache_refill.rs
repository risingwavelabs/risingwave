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

//! The controller plans physical membership changes; the version gate applies them.
//! Policy changes take effect immediately, while version inserts wait for activation and
//! removals wait for application. The Pin backend owns cached bytes and file retirement.

use std::collections::{HashMap, HashSet};

use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::version::{GroupDelta, HummockVersion, HummockVersionDelta};
use risingwave_pb::id::TableId;

use crate::hummock::SstableStoreRef;
use crate::hummock::local_version::pinned_version::PinnedVersion;

#[cfg(test)]
mod tests;

/// Physical membership changes for the version gate to apply at the appropriate boundary.
#[derive(Default)]
#[must_use]
pub(crate) struct PinCacheObjectChanges {
    pub inserted: HashMap<HummockSstableObjectId, u64>,
    pub removed: HashSet<HummockSstableObjectId>,
}

impl PinCacheObjectChanges {
    fn between(
        before: HashMap<HummockSstableObjectId, u64>,
        after: HashMap<HummockSstableObjectId, u64>,
    ) -> Self {
        Self {
            removed: before
                .keys()
                .filter(|id| !after.contains_key(*id))
                .copied()
                .collect(),
            inserted: after
                .into_iter()
                .filter(|(id, _)| !before.contains_key(id))
                .collect(),
        }
    }
}

/// Maintains logical pin-cache membership separately from Foyer block-refill policy.
pub(crate) struct PinCacheRefillController {
    sstable_store: SstableStoreRef,
    pinned_table_ids: HashSet<TableId>,
    object_ref_counts: HashMap<HummockSstableObjectId, u32>,
    pub(crate) version: PinnedVersion,
}

impl PinCacheRefillController {
    pub(crate) fn new(sstable_store: SstableStoreRef, version: PinnedVersion) -> Self {
        Self {
            sstable_store,
            pinned_table_ids: HashSet::new(),
            object_ref_counts: HashMap::new(),
            version,
        }
    }

    /// Reconciles policy against applied/active snapshots supplied by the version gate.
    /// The planning version may be ahead of these snapshots and must not define current residency.
    pub(crate) fn replace_pinned_tables(
        &mut self,
        pinned_table_ids: HashSet<TableId>,
        resident_versions: &[PinnedVersion],
    ) -> PinCacheObjectChanges {
        if self.pinned_table_ids == pinned_table_ids {
            return PinCacheObjectChanges::default();
        }
        let before = resident_versions
            .iter()
            .flat_map(|version| Self::pinned_objects(version, &self.pinned_table_ids))
            .collect();
        let after = resident_versions
            .iter()
            .flat_map(|version| Self::pinned_objects(version, &pinned_table_ids))
            .collect();
        let changes = PinCacheObjectChanges::between(before, after);
        self.pinned_table_ids = pinned_table_ids;
        self.rebuild_object_ref_counts();
        changes
    }

    /// Called after version application (or immediate policy revocation), never at enqueue time.
    pub(crate) fn unregister_objects(
        &self,
        objects: impl IntoIterator<Item = HummockSstableObjectId>,
    ) {
        if let Some(cache) = self.sstable_store.pin_cache() {
            cache.unregister_objects(objects);
        }
    }

    /// `None` denotes a full version snapshot; `Some` carries every raw delta in order,
    /// including changes omitted by SST extraction. An empty delta batch is not a snapshot.
    pub(crate) fn apply_version_update(
        &mut self,
        deltas: &[SstDeltaInfo],
        new_version: PinnedVersion,
        version_deltas: Option<&[HummockVersionDelta]>,
    ) -> (HashSet<HummockSstableObjectId>, PinCacheObjectChanges) {
        // Any membership change in the batch requires a rebuild. Compare table assignments
        // against the starting version so even a change that is later reversed is detected.
        let requires_rebuild = version_deltas.is_none_or(|version_deltas| {
            version_deltas.iter().any(|delta| {
                !delta.removed_table_ids.is_empty()
                    || delta.state_table_info_delta.iter().any(|(table_id, info)| {
                        self.version
                            .state_table_info
                            .info()
                            .get(table_id)
                            .is_none_or(|previous| {
                                previous.compaction_group_id != info.compaction_group_id
                            })
                    })
                    || delta.group_deltas.values().any(|deltas| {
                        deltas.group_deltas.iter().any(|delta| {
                            !matches!(
                                delta,
                                GroupDelta::IntraLevel(_) | GroupDelta::NewL0SubLevel(_)
                            )
                        })
                    })
            })
        });
        let previous_version = std::mem::replace(&mut self.version, new_version);
        let changes = if requires_rebuild {
            None
        } else {
            self.apply_desired_object_delta(deltas)
        }
        .unwrap_or_else(|| {
            // A malformed delta may have partially changed counts. Derive the fallback diff
            // from authoritative snapshots, never from those partial counts.
            if !requires_rebuild {
                tracing::warn!(
                    "pin-cache object reference count is inconsistent; rebuilding membership"
                );
            }
            self.rebuild_object_ref_counts();
            self.changes_between_versions(&previous_version, &self.version)
        });
        let candidates = deltas
            .iter()
            .flat_map(|delta| &delta.insert_sst_infos)
            .filter(|sst| self.object_ref_counts.contains_key(&sst.object_id))
            .map(|sst| sst.object_id)
            .collect();
        (candidates, changes)
    }

    pub(crate) fn changes_between_versions(
        &self,
        before: &HummockVersion,
        after: &HummockVersion,
    ) -> PinCacheObjectChanges {
        PinCacheObjectChanges::between(
            Self::pinned_objects(before, &self.pinned_table_ids),
            Self::pinned_objects(after, &self.pinned_table_ids),
        )
    }

    fn pinned_objects(
        version: &HummockVersion,
        tables: &HashSet<TableId>,
    ) -> HashMap<HummockSstableObjectId, u64> {
        let mut objects = HashMap::new();
        for sst in Self::pinned_ssts(version, tables) {
            if let Some(size) = objects.insert(sst.object_id, sst.file_size) {
                assert_eq!(
                    size, sst.file_size,
                    "one object must have one physical size"
                );
            }
        }
        objects
    }

    /// Enumerates pinned logical SST references from a version without reading object storage.
    /// Startup recovery and subsequent policy rebuilds use the same membership selection.
    pub(crate) fn pinned_ssts<'a>(
        version: &'a HummockVersion,
        pinned_table_ids: &'a HashSet<TableId>,
    ) -> impl Iterator<Item = &'a SstableInfo> {
        let compaction_group_ids = pinned_table_ids
            .iter()
            .filter_map(|table_id| {
                version
                    .state_table_info
                    .info()
                    .get(table_id)
                    .map(|info| info.compaction_group_id)
            })
            .collect::<HashSet<_>>();
        compaction_group_ids
            .into_iter()
            .flat_map(move |compaction_group_id| {
                let levels = version.get_compaction_group_levels(compaction_group_id);
                levels.l0.sub_levels.iter().chain(levels.levels.iter())
            })
            .flat_map(|level| &level.table_infos)
            .filter(move |sst| Self::is_pinned(sst, pinned_table_ids))
    }

    fn rebuild_object_ref_counts(&mut self) {
        self.object_ref_counts.clear();
        for sst in Self::pinned_ssts(&self.version, &self.pinned_table_ids) {
            *self.object_ref_counts.entry(sst.object_id).or_insert(0) += 1;
        }
    }

    fn apply_desired_object_delta(
        &mut self,
        deltas: &[SstDeltaInfo],
    ) -> Option<PinCacheObjectChanges> {
        let pinned_table_ids = &self.pinned_table_ids;
        if pinned_table_ids.is_empty() {
            return Some(PinCacheObjectChanges::default());
        }

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
        for (object_id, initial_count) in initial_counts {
            let final_count = self.object_ref_counts.get(&object_id).copied().unwrap_or(0);
            if initial_count > 0 && final_count == 0 {
                removed.insert(object_id);
            }
            if initial_count == 0 && final_count > 0 {
                inserted.insert(object_id, inserted_sizes[&object_id]);
            }
        }
        Some(PinCacheObjectChanges { inserted, removed })
    }

    fn is_pinned(sst: &SstableInfo, pinned_table_ids: &HashSet<TableId>) -> bool {
        sst.table_ids
            .iter()
            .any(|table_id| pinned_table_ids.contains(table_id))
    }
}
