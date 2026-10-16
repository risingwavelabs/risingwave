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

//! Tracks physical object membership from pin policy and Hummock versions.
//! Version updates either rebuild membership from a snapshot or update counts from SST deltas.
//! Reports object additions/removals separately from refill candidates.
//! The caller controls when to apply these changes to the backend and schedule refill.

use std::collections::{HashMap, HashSet};

use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::version::{GroupDelta, HummockVersion, HummockVersionDelta};
use risingwave_pb::id::TableId;

use crate::hummock::local_version::pinned_version::PinnedVersion;

#[cfg(test)]
mod tests;

/// Physical membership changes for the version gate to apply at the appropriate boundary.
#[derive(Default)]
#[must_use]
pub(crate) struct PinCacheObjectChanges {
    pub inserted: HashSet<HummockSstableObjectId>,
    pub removed: HashSet<HummockSstableObjectId>,
}

/// Tracks physical object membership through pinned logical SST references.
///
/// `object_ref_counts` counts references in `planning_version` under `pinned_table_ids`.
/// Each logical SST contributes once even if several of its tables are pinned. These counts
/// are a derived membership index, not reader counts or the backend's registered object set.
/// The planning version may be ahead of the resident snapshots supplied by the caller.
pub(crate) struct PinCacheRefillController {
    pinned_table_ids: HashSet<TableId>,
    object_ref_counts: HashMap<HummockSstableObjectId, u32>,
    planning_version: PinnedVersion,
}

impl PinCacheRefillController {
    pub(crate) fn new(planning_version: PinnedVersion) -> Self {
        Self {
            pinned_table_ids: HashSet::new(),
            object_ref_counts: HashMap::new(),
            planning_version,
        }
    }

    /// Advances the planning version and returns `(candidates, membership_changes)`.
    /// Candidates appeared in SST insertions and remain referenced in the new version;
    /// an already-referenced object can be a candidate without being newly inserted.
    ///
    /// `None` denotes a full version snapshot; `Some` carries every raw delta in order,
    /// including changes omitted by SST extraction. An empty delta batch is not a snapshot.
    pub(crate) fn apply_version_update(
        &mut self,
        deltas: &[SstDeltaInfo],
        new_version: PinnedVersion,
        version_deltas: Option<&[HummockVersionDelta]>,
    ) -> (HashSet<HummockSstableObjectId>, PinCacheObjectChanges) {
        let changes = match version_deltas {
            Some(raw_deltas) if !self.requires_membership_rebuild(raw_deltas) => {
                self.apply_sst_deltas(deltas)
            }
            _ => self.rebuild_membership(&new_version),
        };
        self.planning_version = new_version;
        let candidates = deltas
            .iter()
            .flat_map(|delta| &delta.insert_sst_infos)
            .filter(|sst| self.object_ref_counts.contains_key(&sst.object_id))
            .map(|sst| sst.object_id)
            .collect();
        (candidates, changes)
    }

    /// Full update: count references in the new version and compare object membership.
    fn rebuild_membership(&mut self, version: &HummockVersion) -> PinCacheObjectChanges {
        let before = self.object_ref_counts.keys().copied().collect();
        self.object_ref_counts = Self::count_pinned_objects(version, &self.pinned_table_ids);
        let after = self.object_ref_counts.keys().copied().collect();
        PinCacheObjectChanges::between(before, after)
    }

    /// Incremental update: adjust logical reference counts and return the batch's net changes.
    /// SST deltas must describe the transition from the planning version to the new version.
    fn apply_sst_deltas(&mut self, deltas: &[SstDeltaInfo]) -> PinCacheObjectChanges {
        let pinned_table_ids = &self.pinned_table_ids;
        if pinned_table_ids.is_empty() {
            return PinCacheObjectChanges::default();
        }

        let mut initial_counts = HashMap::new();
        for delta in deltas {
            for sst in delta
                .delete_sst_infos
                .iter()
                .filter(|sst| Self::is_pinned(sst, pinned_table_ids))
            {
                let count = self
                    .object_ref_counts
                    .get_mut(&sst.object_id)
                    .expect("deleted pinned SST must have a reference");
                initial_counts.entry(sst.object_id).or_insert(*count);
                *count -= 1;
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
                *count = count
                    .checked_add(1)
                    .expect("pinned SST reference count overflow");
            }
        }

        let mut removed = HashSet::new();
        let mut inserted = HashSet::new();
        for (object_id, initial_count) in initial_counts {
            let final_count = self.object_ref_counts.get(&object_id).copied().unwrap_or(0);
            if initial_count > 0 && final_count == 0 {
                removed.insert(object_id);
            }
            if initial_count == 0 && final_count > 0 {
                inserted.insert(object_id);
            }
        }
        PinCacheObjectChanges { inserted, removed }
    }

    /// Detects membership changes that SST insertion/deletion alone cannot describe.
    fn requires_membership_rebuild(&self, version_deltas: &[HummockVersionDelta]) -> bool {
        for delta in version_deltas {
            if !delta.removed_table_ids.is_empty() {
                return true;
            }
            // Compare every delta against the starting version, including changes that
            // are reversed later in the same batch.
            for (table_id, info) in &delta.state_table_info_delta {
                let previous_group = self
                    .planning_version
                    .state_table_info
                    .info()
                    .get(table_id)
                    .map(|previous| previous.compaction_group_id);
                if previous_group != Some(info.compaction_group_id) {
                    return true;
                }
            }
            for group_delta in delta
                .group_deltas
                .values()
                .flat_map(|deltas| &deltas.group_deltas)
            {
                if !matches!(
                    group_delta,
                    GroupDelta::IntraLevel(_) | GroupDelta::NewL0SubLevel(_)
                ) {
                    return true;
                }
            }
        }
        false
    }

    /// Updates pin policy and returns membership changes over the supplied resident snapshots.
    /// Reference counts are rebuilt for `planning_version`, which may be ahead of residency.
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
        self.object_ref_counts =
            Self::count_pinned_objects(&self.planning_version, &self.pinned_table_ids);
        changes
    }

    /// Recomputes a pending version transition after a pin-policy change.
    /// This comparison does not advance the planning version or its reference counts.
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

    fn pinned_objects(
        version: &HummockVersion,
        tables: &HashSet<TableId>,
    ) -> HashSet<HummockSstableObjectId> {
        Self::pinned_ssts(version, tables)
            .map(|sst| sst.object_id)
            .collect()
    }

    fn count_pinned_objects(
        version: &HummockVersion,
        tables: &HashSet<TableId>,
    ) -> HashMap<HummockSstableObjectId, u32> {
        let mut counts = HashMap::new();
        for sst in Self::pinned_ssts(version, tables) {
            *counts.entry(sst.object_id).or_insert(0) += 1;
        }
        counts
    }

    fn is_pinned(sst: &SstableInfo, pinned_table_ids: &HashSet<TableId>) -> bool {
        sst.table_ids
            .iter()
            .any(|table_id| pinned_table_ids.contains(table_id))
    }
}

impl PinCacheObjectChanges {
    fn between(
        before: HashSet<HummockSstableObjectId>,
        after: HashSet<HummockSstableObjectId>,
    ) -> Self {
        Self {
            removed: before.difference(&after).copied().collect(),
            inserted: after.difference(&before).copied().collect(),
        }
    }
}
