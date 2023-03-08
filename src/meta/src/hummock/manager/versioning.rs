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

use std::cmp;
use std::collections::{BTreeMap, HashMap};
use std::ops::RangeBounds;

use function_name::named;
use itertools::Itertools;
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockContextId, HummockSstableId, HummockVersionId,
};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::{
    HummockPinnedSnapshot, HummockPinnedVersion, HummockVersion, HummockVersionDelta,
    HummockVersionStats,
};

use crate::hummock::manager::worker::{HummockManagerEvent, HummockManagerEventSender};
use crate::hummock::manager::{read_lock, write_lock};
use crate::hummock::metrics_utils::{trigger_safepoint_stat, trigger_stale_ssts_stat};
use crate::hummock::HummockManager;
use crate::rpc::metrics::MetaMetrics;
use crate::storage::MetaStore;

/// `HummockVersionSafePoint` prevents hummock versions GE than it from being GC.
/// It's used by meta node itself to temporarily pin versions.
pub struct HummockVersionSafePoint {
    pub id: HummockVersionId,
    event_sender: HummockManagerEventSender,
}

impl Drop for HummockVersionSafePoint {
    fn drop(&mut self) {
        if let Err(e) = self
            .event_sender
            .send(HummockManagerEvent::DropSafePoint(self.id))
        {
            tracing::debug!(
                "failed to drop hummock version safe point {}. {}",
                self.id,
                e
            );
        }
    }
}

pub type BranchedSstInfo = HashMap<CompactionGroupId, /* divide version */ u64>;

#[derive(Default)]
pub struct Versioning {
    // Volatile states below
    /// Avoide commit epoch epochs
    /// Don't persist compaction version delta to meta store
    pub disable_commit_epochs: bool,
    /// Latest hummock version
    pub current_version: HummockVersion,
    /// These SSTs should be deleted from object store.
    /// Mapping from a SST to the version that has marked it stale. See `ack_deleted_ssts`.
    pub ssts_to_delete: BTreeMap<HummockSstableId, HummockVersionId>,
    /// These deltas should be deleted from meta store.
    /// A delta can be deleted if
    /// - Its version id <= checkpoint version id. Currently we only make checkpoint for version id
    ///   <= min_pinned_version_id.
    /// - AND It either contains no SST to delete, or all these SSTs has been deleted. See
    ///   `extend_ssts_to_delete_from_deltas`.
    pub deltas_to_delete: Vec<HummockVersionId>,
    /// SST which is referenced more than once
    pub branched_ssts: BTreeMap<HummockSstableId, BranchedSstInfo>,
    /// `version_safe_points` is similar to `pinned_versions` expect for being a transient state.
    /// Hummock versions GE than min(safe_point) should not be GCed.
    pub version_safe_points: Vec<HummockVersionId>,

    // Persistent states below
    /// Mapping from id of each hummock version which succeeds checkpoint to its
    /// `HummockVersionDelta`
    pub hummock_version_deltas: BTreeMap<HummockVersionId, HummockVersionDelta>,
    pub pinned_versions: BTreeMap<HummockContextId, HummockPinnedVersion>,
    pub pinned_snapshots: BTreeMap<HummockContextId, HummockPinnedSnapshot>,
    pub checkpoint_version: HummockVersion,
    /// Stats for latest hummock version.
    pub version_stats: HummockVersionStats,
}

impl Versioning {
    pub fn min_pinned_version_id(&self) -> HummockVersionId {
        let mut min_pinned_version_id = HummockVersionId::MAX;
        for id in self
            .pinned_versions
            .values()
            .map(|v| v.min_pinned_id)
            .chain(self.version_safe_points.iter().cloned())
        {
            min_pinned_version_id = cmp::min(id, min_pinned_version_id);
        }
        min_pinned_version_id
    }

    pub fn extend_ssts_to_delete_from_deltas(
        &mut self,
        delta_range: impl RangeBounds<HummockVersionId>,
        metric: &MetaMetrics,
    ) {
        self.extend_ssts_to_delete_from_deltas_impl(delta_range);
        trigger_stale_ssts_stat(metric, self.ssts_to_delete.len());
    }

    /// Extends `ssts_to_delete` according to given deltas.
    /// Possibly extends `deltas_to_delete`.
    fn extend_ssts_to_delete_from_deltas_impl(
        &mut self,
        delta_range: impl RangeBounds<HummockVersionId>,
    ) {
        for (_, delta) in self.hummock_version_deltas.range(delta_range) {
            if delta.trivial_move {
                self.deltas_to_delete.push(delta.id);
                continue;
            }
            let removed_sst_ids = delta.get_gc_sst_ids().clone();
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

    #[named]
    pub async fn get_version_stats(&self) -> HummockVersionStats {
        read_lock!(self, versioning).await.version_stats.clone()
    }

    #[named]
    pub async fn register_safe_point(&self) -> HummockVersionSafePoint {
        let mut wl = write_lock!(self, versioning).await;
        let safe_point = HummockVersionSafePoint {
            id: wl.current_version.id,
            event_sender: self.event_sender.clone(),
        };
        wl.version_safe_points.push(safe_point.id);
        trigger_safepoint_stat(&self.metrics, &wl.version_safe_points);
        safe_point
    }

    #[named]
    pub async fn unregister_safe_point(&self, safe_point: HummockVersionId) {
        let mut wl = write_lock!(self, versioning).await;
        let version_safe_points = &mut wl.version_safe_points;
        if let Some(pos) = version_safe_points.iter().position(|sp| *sp == safe_point) {
            version_safe_points.remove(pos);
        }
        trigger_safepoint_stat(&self.metrics, &wl.version_safe_points);
    }
}

#[cfg(test)]
mod tests {
    use risingwave_hummock_sdk::HummockVersionId;
    use risingwave_pb::hummock::{HummockPinnedVersion, HummockVersionDelta};

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
        versioning.extend_ssts_to_delete_from_deltas_impl(1..=2);
        assert_eq!(versioning.deltas_to_delete.len(), 1);
    }

    #[test]
    fn test_min_pinned_version_id() {
        let mut versioning = Versioning::default();
        assert_eq!(versioning.min_pinned_version_id(), HummockVersionId::MAX);
        versioning.pinned_versions.insert(
            1,
            HummockPinnedVersion {
                context_id: 1,
                min_pinned_id: 10,
            },
        );
        assert_eq!(versioning.min_pinned_version_id(), 10);
        versioning.version_safe_points.push(5);
        assert_eq!(versioning.min_pinned_version_id(), 5);
        versioning.version_safe_points.clear();
        assert_eq!(versioning.min_pinned_version_id(), 10);
        versioning.pinned_versions.clear();
        assert_eq!(versioning.min_pinned_version_id(), HummockVersionId::MAX);
    }
}
