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

//! Builds event-scoped Pin refill candidates from observed SST insertions.
//! Plans are narrowed against an explicit target version. Reference tracking is owned
//! separately by the Pin-cache membership index. Constructing plans starts no refill work.

use std::collections::{HashMap, HashSet};

use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::version::{GroupDelta, HummockVersion, HummockVersionDelta};
use risingwave_pb::id::TableId;

pub(crate) mod executor;

use crate::hummock::pin_cache::membership::PinCacheMembershipIndex;

/// Refill candidates captured from observed SST insertions under the current pin policy.
/// This is independent of membership changes: a full snapshot can add membership without
/// refill intent, and an already-referenced object can acquire new refill intent.
/// Candidates retain only table projections referenced by this event's target version.
/// Pending plans may only lose admitted tables; metadata admission resolves vnode ownership
/// before the caller constructs download jobs.
#[derive(Default)]
pub(crate) struct PinCacheRefillPlan {
    pub objects: HashMap<HummockSstableObjectId, PinCacheRefillObject>,
}

/// One physical object's refill intent, independent of current vnode ownership.
/// `source_sst` supplies physical metadata from one logical SST reference. Its `table_ids`
/// do not define admission: other references to the same object may add admitted tables.
pub(crate) struct PinCacheRefillObject {
    pub source_sst: SstableInfo,
    pub admitted_tables: HashSet<TableId>,
}

impl PinCacheRefillPlan {
    pub(crate) fn from_version_update(
        sst_deltas: &[SstDeltaInfo],
        version: &HummockVersion,
        version_deltas: Option<&[HummockVersionDelta]>,
        pinned_tables: &HashSet<TableId>,
    ) -> Self {
        let mut plan = match version_deltas {
            // Structural group changes can omit insertions from the SST summary.
            Some(raw)
                if raw
                    .iter()
                    .flat_map(|delta| delta.group_deltas.values())
                    .flat_map(|group| &group.group_deltas)
                    .any(|delta| {
                        !matches!(
                            delta,
                            GroupDelta::IntraLevel(_) | GroupDelta::NewL0SubLevel(_)
                        )
                    }) =>
            {
                Self::new(
                    raw.iter()
                        .filter(|delta| !delta.trivial_move)
                        .flat_map(HummockVersionDelta::newly_added_sst_infos),
                    pinned_tables,
                )
            }
            Some(_) => Self::new(
                sst_deltas.iter().flat_map(|delta| &delta.insert_sst_infos),
                pinned_tables,
            ),
            // A snapshot restores membership without creating download intent.
            None => Self::default(),
        };
        plan.retain_in_version(version, pinned_tables);
        plan
    }

    fn new<'a>(
        ssts: impl IntoIterator<Item = &'a SstableInfo>,
        pinned_tables: &HashSet<TableId>,
    ) -> Self {
        let mut objects: HashMap<_, PinCacheRefillObject> = HashMap::new();
        for sst in ssts {
            let mut admitted_tables = sst
                .table_ids
                .iter()
                .copied()
                .filter(|table| pinned_tables.contains(table))
                .peekable();
            if admitted_tables.peek().is_none() {
                continue;
            }
            objects
                .entry(sst.object_id)
                .or_insert_with(|| PinCacheRefillObject {
                    source_sst: sst.clone(),
                    admitted_tables: HashSet::new(),
                })
                .admitted_tables
                .extend(admitted_tables);
        }
        Self { objects }
    }

    /// A payload may contain several deltas or structural changes. Keep admission only
    /// through references surviving in this event's target, even if another table keeps
    /// the same physical object alive.
    pub(crate) fn retain_in_version(
        &mut self,
        version: &HummockVersion,
        pinned_tables: &HashSet<TableId>,
    ) {
        if self.objects.is_empty() {
            return;
        }
        let mut required: HashMap<_, HashSet<_>> = self
            .objects
            .keys()
            .map(|object| (*object, HashSet::new()))
            .collect();
        for sst in PinCacheMembershipIndex::pinned_ssts(version, pinned_tables) {
            if let Some(tables) = required.get_mut(&sst.object_id) {
                tables.extend(
                    sst.table_ids
                        .iter()
                        .copied()
                        .filter(|table| pinned_tables.contains(table)),
                );
            }
        }
        self.objects.retain(|object, candidate| {
            candidate
                .admitted_tables
                .retain(|table| required[object].contains(table));
            !candidate.admitted_tables.is_empty()
        });
    }

    pub(crate) fn retain_tables(&mut self, pinned_tables: &HashSet<TableId>) {
        self.objects.retain(|_, object| {
            object
                .admitted_tables
                .retain(|table| pinned_tables.contains(table));
            !object.admitted_tables.is_empty()
        });
    }
}

#[cfg(test)]
mod tests;
