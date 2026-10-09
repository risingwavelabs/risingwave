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
//! Reports membership changes and candidates from observed SST insertions.
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
        let requires_rebuild = self.requires_membership_rebuild(version_deltas);
        let previous_version = std::mem::replace(&mut self.planning_version, new_version);
        let changes = if requires_rebuild {
            self.rebuild_object_ref_counts();
            self.changes_between_versions(&previous_version, &self.planning_version)
        } else if let Some(changes) = self.try_apply_sst_deltas(deltas) {
            changes
        } else {
            // A malformed delta may have partially changed counts. Derive the fallback diff
            // from authoritative snapshots, never from those partial counts.
            tracing::warn!(
                "pin-cache object reference count is inconsistent; rebuilding membership"
            );
            self.rebuild_object_ref_counts();
            self.changes_between_versions(&previous_version, &self.planning_version)
        };
        let candidates = deltas
            .iter()
            .flat_map(|delta| &delta.insert_sst_infos)
            .filter(|sst| self.object_ref_counts.contains_key(&sst.object_id))
            .map(|sst| sst.object_id)
            .collect();
        (candidates, changes)
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
        self.rebuild_object_ref_counts();
        changes
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

    fn rebuild_object_ref_counts(&mut self) {
        self.object_ref_counts.clear();
        for sst in Self::pinned_ssts(&self.planning_version, &self.pinned_table_ids) {
            *self.object_ref_counts.entry(sst.object_id).or_insert(0) += 1;
        }
    }

    /// Detects membership changes that SST insertion/deletion alone cannot describe.
    fn requires_membership_rebuild(&self, version_deltas: Option<&[HummockVersionDelta]>) -> bool {
        let Some(version_deltas) = version_deltas else {
            return true;
        };
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

    /// Applies SST deltas to reference counts and returns the net membership change.
    /// On `None`, counts may be partially updated and must be rebuilt from `planning_version`.
    fn try_apply_sst_deltas(&mut self, deltas: &[SstDeltaInfo]) -> Option<PinCacheObjectChanges> {
        let pinned_table_ids = &self.pinned_table_ids;
        if pinned_table_ids.is_empty() {
            return Some(PinCacheObjectChanges::default());
        }

        let mut initial_counts = HashMap::new();
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
        Some(PinCacheObjectChanges { inserted, removed })
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
