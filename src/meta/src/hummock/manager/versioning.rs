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
use std::collections::{BTreeMap, HashMap};
use std::ops::RangeBounds;

use function_name::named;
use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockVersionDeltaExt;
use risingwave_hummock_sdk::{HummockContextId, HummockSstableId, HummockVersionId};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::{
    HummockPinnedSnapshot, HummockPinnedVersion, HummockVersion, HummockVersionDelta,
};

use crate::hummock::manager::read_lock;
use crate::hummock::HummockManager;
use crate::storage::MetaStore;

#[derive(Default)]
pub struct Versioning {
    // Volatile states below

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

impl<S> HummockManager<S>
where
    S: MetaStore,
{
    #[named]
    pub async fn list_pinned_version(&self) -> Vec<HummockPinnedVersion> {
        read_lock!(self, versioning)
            .await
            .pinned_versions
            .values()
            .cloned()
            .collect_vec()
    }

    #[named]
    pub async fn list_pinned_snapshot(&self) -> Vec<HummockPinnedSnapshot> {
        read_lock!(self, versioning)
            .await
            .pinned_snapshots
            .values()
            .cloned()
            .collect_vec()
    }

    pub async fn list_workers(
        &self,
        context_ids: &[HummockContextId],
    ) -> HashMap<HummockContextId, WorkerNode> {
        let mut workers = HashMap::new();
        for context_id in context_ids {
            if let Some(worker) = self.cluster_manager.get_worker_by_id(*context_id).await {
                workers.insert(*context_id, worker.worker_node);
            }
        }
        workers
    }
}

#[cfg(test)]
mod tests {
    use risingwave_pb::hummock::HummockVersionDelta;

    use crate::hummock::manager::versioning::Versioning;

    #[tokio::test]
    async fn test_extend_ssts_to_delete_from_deltas_trivial_move() {
        let mut versioning = Versioning::default();

        // trivial_move
        versioning.hummock_version_deltas.insert(
            2,
            HummockVersionDelta {
                id: 2,
                prev_id: 1,
                trivial_move: false,
                ..Default::default()
            },
        );
        assert_eq!(versioning.deltas_to_delete.len(), 0);
        versioning.extend_ssts_to_delete_from_deltas(1..=2);
        assert_eq!(versioning.deltas_to_delete.len(), 1);
    }
}
