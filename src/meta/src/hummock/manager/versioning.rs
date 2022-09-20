// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp;
use std::collections::BTreeMap;
use std::ops::RangeBounds;

use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionDeltaExt;
use risingwave_hummock_sdk::{HummockContextId, HummockSstableId, HummockVersionId};
use risingwave_pb::hummock::{
    HummockPinnedSnapshot, HummockPinnedVersion, HummockVersion, HummockVersionDelta,
};

#[derive(Default)]
pub struct Versioning {
    // Volatile states below
    pub ignore_empty_epoch: bool,

    // Newest version
    pub current_version: HummockVersion,
    // These SSTs should be deleted from object store.
    // Mapping from a SST to the version that has marked it stale. See `ack_deleted_ssts`.
    pub ssts_to_delete: BTreeMap<HummockSstableId, HummockVersionId>,
    // These deltas should be deleted from meta store.
    // A delta can be deleted if
    // - It's version id <= checkpoint version id. Currently we only make checkpoint for version id
    //   <= min_pinned_version_id.
    // - AND It either contains no SST to delete, or all these SSTs has been deleted. See
    //   `extend_ssts_to_delete_from_deltas`.
    pub deltas_to_delete: Vec<HummockVersionId>,

    // Persistent states below

    // Mapping from id of each hummock version which succeeds checkpoint to its
    // `HummockVersionDelta`
    pub hummock_version_deltas: BTreeMap<HummockVersionId, HummockVersionDelta>,
    pub pinned_versions: BTreeMap<HummockContextId, HummockPinnedVersion>,
    pub pinned_snapshots: BTreeMap<HummockContextId, HummockPinnedSnapshot>,
    pub checkpoint_version: HummockVersion,
}

impl Versioning {
    pub fn min_pinned_version_id(&self) -> HummockVersionId {
        let mut min_pinned_version_id = HummockVersionId::MAX;
        for version_pin in self.pinned_versions.values() {
            min_pinned_version_id = cmp::min(version_pin.min_pinned_id, min_pinned_version_id);
        }
        min_pinned_version_id
    }

    /// Extends `ssts_to_delete` according to given deltas.
    /// Possibly extends `deltas_to_delete`.
    pub fn extend_ssts_to_delete_from_deltas(
        &mut self,
        delta_range: impl RangeBounds<HummockVersionId>,
    ) {
        for (_, delta) in self.hummock_version_deltas.range(delta_range) {
            if delta.trivial_move {
                self.deltas_to_delete.push(delta.id);
                continue;
            }
            let removed_sst_ids = delta.get_removed_sst_ids();
            for sst_id in &removed_sst_ids {
                let duplicate_insert = self.ssts_to_delete.insert(*sst_id, delta.id);
                debug_assert!(duplicate_insert.is_none());
            }
            // If no_sst_to_delete, the delta is qualified for deletion now.
            if removed_sst_ids.is_empty() {
                self.deltas_to_delete.push(delta.id);
            }
            // Otherwise, the delta is qualified for deletion after all its sst_to_delete is
            // deleted.
        }
    }
}
