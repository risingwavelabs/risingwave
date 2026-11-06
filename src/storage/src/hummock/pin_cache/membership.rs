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

use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::version::{GroupDelta, HummockVersion, HummockVersionDelta};
use risingwave_pb::id::TableId;

use super::PinCache;
use crate::hummock::local_version::pinned_version::PinnedVersion;

impl PinCache {
    /// Registers objects for explicit refill without downloading them.
    /// Registering an existing object preserves its state and refill tokens; it does not repair
    /// an invalidated file. A registered object is readable only after publication.
    pub(crate) fn register_objects(
        &self,
        objects: impl IntoIterator<Item = HummockSstableObjectId>,
    ) {
        // Release the shard between objects so a large update does not hold up lookups.
        for id in objects {
            self.shard(id).state.write().register_object(id);
        }
    }

    /// Unregisters objects, withdrawing their publications and revoking all refill tokens.
    /// Existing read handles retain their files. The CN membership owner decides when to apply delta
    /// deletions relative to version publication; this method does not wait for a version.
    pub(crate) fn unregister_objects(
        &self,
        objects: impl IntoIterator<Item = HummockSstableObjectId>,
    ) {
        for id in objects {
            // Release the shard before handing the removed publication to GC.
            let object = self.shard(id).state.write().objects.remove(&id);
            if let Some(mut object) = object
                && let Some(file) = object.unpublish()
            {
                self.enqueue_delete(file);
            }
        }
    }

    /// Whether an object is registered, regardless of whether it has a readable local file.
    pub(crate) fn is_registered(&self, object_id: HummockSstableObjectId) -> bool {
        self.shard(object_id)
            .state
            .read()
            .objects
            .contains_key(&object_id)
    }
}

/// Added/removed physical objects in the set referenced by pinned logical SSTs.
/// This describes membership, not downloaded files or refill work. The caller chooses
/// when to register additions and when resident versions no longer need removals.
#[derive(Default)]
#[must_use]
pub(crate) struct PinCacheMembershipChanges {
    pub inserted: HashSet<HummockSstableObjectId>,
    pub removed: HashSet<HummockSstableObjectId>,
}

/// Tracks physical object membership through pinned logical SST references.
///
/// `object_ref_counts` counts references in `tracked_version` under the pin policy supplied by the caller.
/// The caller rebuilds this derived index whenever that policy changes.
/// Each logical SST contributes once even if several of its tables are pinned. These counts
/// are a derived membership index, not reader counts or the backend's registered object set.
/// The tracked version is independent of the snapshots supplied for membership comparisons.
pub(crate) struct PinCacheMembershipIndex {
    object_ref_counts: HashMap<HummockSstableObjectId, u32>,
    tracked_version: PinnedVersion,
}

impl PinCacheMembershipIndex {
    pub(crate) fn new(
        tracked_version: PinnedVersion,
        is_pinned: impl Fn(&TableId) -> bool + Copy,
    ) -> Self {
        Self {
            object_ref_counts: Self::count_pinned_objects(&tracked_version, is_pinned),
            tracked_version,
        }
    }

    /// Advances reference counts under an unchanged pin policy and returns object-set changes.
    /// All inputs must describe the same transition from `tracked_version`. `None` denotes
    /// a full snapshot; `Some` contains every raw delta in order, including structural changes.
    /// The caller applies these changes to the backend at its chosen lifecycle boundary.
    pub(crate) fn advance_version(
        &mut self,
        sst_deltas: &[SstDeltaInfo],
        new_version: PinnedVersion,
        version_deltas: Option<&[HummockVersionDelta]>,
        is_pinned: impl Fn(&TableId) -> bool + Copy,
    ) -> PinCacheMembershipChanges {
        let changes = match version_deltas {
            Some(raw) if !self.requires_membership_rebuild(raw) => {
                self.apply_sst_deltas(sst_deltas, is_pinned)
            }
            _ => self.rebuild_membership(&new_version, is_pinned),
        };
        self.tracked_version = new_version;
        changes
    }

    /// Full update: count references in the new version and compare object membership.
    fn rebuild_membership(
        &mut self,
        version: &HummockVersion,
        is_pinned: impl Fn(&TableId) -> bool + Copy,
    ) -> PinCacheMembershipChanges {
        let before = self.object_ref_counts.keys().copied().collect();
        self.object_ref_counts = Self::count_pinned_objects(version, is_pinned);
        let after = self.object_ref_counts.keys().copied().collect();
        PinCacheMembershipChanges::between_objects(before, after)
    }

    /// Incremental update: adjust logical reference counts and return the batch's net changes.
    /// SST deltas must describe the transition from the tracked version to the new version.
    fn apply_sst_deltas(
        &mut self,
        deltas: &[SstDeltaInfo],
        is_pinned: impl Fn(&TableId) -> bool + Copy,
    ) -> PinCacheMembershipChanges {
        let mut initial_counts = HashMap::new();
        for delta in deltas {
            for sst in delta
                .delete_sst_infos
                .iter()
                .filter(|sst| Self::is_pinned(sst, is_pinned))
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
                .filter(|sst| Self::is_pinned(sst, is_pinned))
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
        PinCacheMembershipChanges { inserted, removed }
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
                    .tracked_version
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

    /// Rebuilds derived reference counts at the tracked version after a policy change.
    pub(crate) fn rebuild_for_policy(&mut self, is_pinned: impl Fn(&TableId) -> bool + Copy) {
        self.object_ref_counts = Self::count_pinned_objects(&self.tracked_version, is_pinned);
    }

    /// Enumerates pinned logical SST references from a version without reading object storage.
    /// Startup recovery and subsequent policy rebuilds use the same membership selection.
    pub(crate) fn pinned_ssts<'a>(
        version: &'a HummockVersion,
        is_pinned: impl Fn(&TableId) -> bool + Copy + 'a,
    ) -> impl Iterator<Item = &'a SstableInfo> {
        let compaction_group_ids = version
            .state_table_info
            .info()
            .iter()
            .filter(|(table, _)| is_pinned(table))
            .map(|(_, info)| info.compaction_group_id)
            .collect::<HashSet<_>>();
        compaction_group_ids
            .into_iter()
            .flat_map(move |compaction_group_id| {
                let levels = version.get_compaction_group_levels(compaction_group_id);
                levels.l0.sub_levels.iter().chain(levels.levels.iter())
            })
            .flat_map(|level| &level.table_infos)
            .filter(move |sst| Self::is_pinned(sst, is_pinned))
    }

    fn pinned_objects(
        version: &HummockVersion,
        is_pinned: impl Fn(&TableId) -> bool + Copy,
    ) -> HashSet<HummockSstableObjectId> {
        Self::pinned_ssts(version, is_pinned)
            .map(|sst| sst.object_id)
            .collect()
    }

    fn count_pinned_objects(
        version: &HummockVersion,
        is_pinned: impl Fn(&TableId) -> bool + Copy,
    ) -> HashMap<HummockSstableObjectId, u32> {
        let mut counts = HashMap::new();
        for sst in Self::pinned_ssts(version, is_pinned) {
            *counts.entry(sst.object_id).or_insert(0) += 1;
        }
        counts
    }

    fn is_pinned(sst: &SstableInfo, is_pinned: impl Fn(&TableId) -> bool + Copy) -> bool {
        sst.table_ids.iter().any(is_pinned)
    }
}

impl PinCacheMembershipChanges {
    /// Compares versions under one fixed pin policy, without advancing the reference index.
    pub(crate) fn between_versions(
        before: &HummockVersion,
        after: &HummockVersion,
        is_pinned: impl Fn(&TableId) -> bool + Copy,
    ) -> Self {
        Self::between_objects(
            PinCacheMembershipIndex::pinned_objects(before, is_pinned),
            PinCacheMembershipIndex::pinned_objects(after, is_pinned),
        )
    }

    /// Compares pin policies over the same supplied snapshots, independently of the tracked version.
    /// Union objects across all snapshots before taking the difference: an object can
    /// remain resident through a different logical reference in another snapshot.
    pub(crate) fn between_policies(
        versions: &[PinnedVersion],
        old_is_pinned: impl Fn(&TableId) -> bool + Copy,
        new_is_pinned: impl Fn(&TableId) -> bool + Copy,
    ) -> Self {
        Self::between_objects(
            versions
                .iter()
                .flat_map(|version| PinCacheMembershipIndex::pinned_objects(version, old_is_pinned))
                .collect(),
            versions
                .iter()
                .flat_map(|version| PinCacheMembershipIndex::pinned_objects(version, new_is_pinned))
                .collect(),
        )
    }

    fn between_objects(
        before: HashSet<HummockSstableObjectId>,
        after: HashSet<HummockSstableObjectId>,
    ) -> Self {
        Self {
            removed: before.difference(&after).copied().collect(),
            inserted: after.difference(&before).copied().collect(),
        }
    }
}

#[cfg(test)]
mod tests;
