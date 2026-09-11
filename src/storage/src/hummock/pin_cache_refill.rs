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
use std::sync::{Arc, LazyLock};

use parking_lot::Mutex;
use prometheus::{
    IntCounterVec, IntGauge, Registry, register_int_counter_vec_with_registry,
    register_int_gauge_with_registry,
};
use risingwave_common::config::streaming::CacheRefillPolicy;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_pb::id::TableId;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;

use crate::hummock::SstableStoreRef;
use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::pin_cache::PinCacheRefillOutcome;

static GLOBAL_PIN_CACHE_REFILL_METRICS: LazyLock<PinCacheRefillMetrics> =
    LazyLock::new(|| PinCacheRefillMetrics::new(&GLOBAL_METRICS_REGISTRY));

struct PinCacheRefillMetrics {
    pending_objects: IntGauge,
    inflight_objects: IntGauge,
    refill_total: IntCounterVec,
}

impl PinCacheRefillMetrics {
    fn new(registry: &Registry) -> Self {
        let pending_objects = register_int_gauge_with_registry!(
            "pin_cache_refill_pending_objects",
            "Number of SST objects waiting in Pin Cache refill schedulers.",
            registry,
        )
        .unwrap();
        let inflight_objects = register_int_gauge_with_registry!(
            "pin_cache_refill_inflight_objects",
            "Number of SST objects currently processed by Pin Cache refill workers.",
            registry,
        )
        .unwrap();
        let refill_total = register_int_counter_vec_with_registry!(
            "pin_cache_refill_total",
            "Number of completed Pin Cache refill attempts by result.",
            &["result"],
            registry,
        )
        .unwrap();
        Self {
            pending_objects,
            inflight_objects,
            refill_total,
        }
    }

    fn record(&self, outcome: PinCacheRefillOutcome) {
        let result = match outcome {
            PinCacheRefillOutcome::Published => "published",
            PinCacheRefillOutcome::Skipped => "skipped",
            PinCacheRefillOutcome::CapacityRejected => "capacity_rejected",
            PinCacheRefillOutcome::Obsolete => "obsolete",
        };
        self.refill_total.with_label_values(&[result]).inc();
    }
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
    scheduler: PinCacheRefillScheduler,
}

impl PinCacheRefillController {
    pub(crate) fn new(sstable_store: SstableStoreRef, version: PinnedVersion) -> Self {
        let scheduler = PinCacheRefillScheduler::new(sstable_store.clone());
        Self {
            sstable_store,
            pinned_table_ids: None,
            object_ref_counts: HashMap::new(),
            version,
            scheduler,
        }
    }

    pub(crate) fn replace_policies(&mut self, policies: &HashMap<TableId, CacheRefillPolicy>) {
        let pinned_table_ids = policies
            .iter()
            .filter_map(|(&table_id, policy)| policy.is_pinned().then_some(table_id))
            .collect();
        if self.pinned_table_ids.as_ref() == Some(&pinned_table_ids) {
            return;
        }
        self.pinned_table_ids = Some(pinned_table_ids);
        let desired = self.rebuild_desired_objects();
        // SET only changes membership. Existing SSTs require an explicit future Warm operation.
        self.scheduler.retain(desired);
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
        let (removed, refill_objects) = match membership_update {
            PinCacheMembershipUpdate::Delta => {
                if let Some((removed, refill_objects)) = self.apply_desired_object_delta(deltas) {
                    (removed, Some(refill_objects))
                } else {
                    tracing::warn!(
                        "pin-cache object reference count is inconsistent; rebuilding membership"
                    );
                    let desired = self.rebuild_desired_objects();
                    self.scheduler.retain(desired);
                    (HashSet::new(), None)
                }
            }
            PinCacheMembershipUpdate::Rebuild => {
                let previous_counts = self.object_ref_counts.clone();
                let desired = self.rebuild_desired_objects();
                let refill_objects = deltas
                    .iter()
                    .flat_map(|delta| &delta.insert_sst_infos)
                    .map(|sst| sst.object_id)
                    .filter(|object_id| {
                        desired.contains(object_id)
                            && self.object_ref_counts.get(object_id).copied().unwrap_or(0)
                                > previous_counts.get(object_id).copied().unwrap_or(0)
                    })
                    .collect();
                self.scheduler.retain(desired);
                (HashSet::new(), Some(refill_objects))
            }
        };

        let pinned_insert_objects = deltas
            .iter()
            .flat_map(|delta| &delta.insert_sst_infos)
            .filter(|sst| pin_cache.is_desired(sst.object_id))
            .map(|sst| sst.object_id)
            .collect::<HashSet<_>>();
        let refill_objects = refill_objects.unwrap_or_else(|| pinned_insert_objects.clone());
        self.scheduler.update(removed, refill_objects);
        pinned_insert_objects
    }

    fn rebuild_desired_objects(&mut self) -> HashSet<HummockSstableObjectId> {
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
        pin_cache.replace_desired_objects(objects.iter().map(|(&id, &size)| (id, size)));
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

#[derive(Default)]
struct PendingPinObjects {
    objects: HashSet<HummockSstableObjectId>,
}

impl PendingPinObjects {
    fn update(
        &mut self,
        removed: HashSet<HummockSstableObjectId>,
        inserted: HashSet<HummockSstableObjectId>,
    ) {
        let old_len = self.objects.len();
        for object_id in removed {
            self.objects.remove(&object_id);
        }
        self.objects.extend(inserted);
        self.update_metric(old_len);
    }

    fn retain(&mut self, desired: &HashSet<HummockSstableObjectId>) {
        let old_len = self.objects.len();
        self.objects.retain(|object_id| desired.contains(object_id));
        self.update_metric(old_len);
    }

    fn pop(&mut self) -> Option<HummockSstableObjectId> {
        let object_id = self.objects.iter().next().copied()?;
        self.objects.remove(&object_id);
        GLOBAL_PIN_CACHE_REFILL_METRICS.pending_objects.dec();
        Some(object_id)
    }

    fn update_metric(&self, old_len: usize) {
        GLOBAL_PIN_CACHE_REFILL_METRICS
            .pending_objects
            .add(self.objects.len() as i64 - old_len as i64);
    }
}

impl Drop for PendingPinObjects {
    fn drop(&mut self) {
        GLOBAL_PIN_CACHE_REFILL_METRICS
            .pending_objects
            .sub(self.objects.len() as i64);
    }
}

struct PinCacheRefillScheduler {
    pending: Arc<Mutex<PendingPinObjects>>,
    wakeup: mpsc::Sender<()>,
}

impl PinCacheRefillScheduler {
    fn new(sstable_store: SstableStoreRef) -> Self {
        let pending = Arc::new(Mutex::new(PendingPinObjects::default()));
        let worker_pending = pending.clone();
        // Only one wakeup is needed while the worker is busy. The pending set itself is the
        // authoritative queue and is compacted synchronously by every version update.
        let (wakeup, mut receiver) = mpsc::channel(1);
        tokio::spawn(async move {
            while receiver.recv().await.is_some() {
                loop {
                    let object_id = { worker_pending.lock().pop() };
                    let Some(object_id) = object_id else {
                        break;
                    };
                    GLOBAL_PIN_CACHE_REFILL_METRICS.inflight_objects.inc();
                    let _inflight_guard = scopeguard::guard((), |_| {
                        GLOBAL_PIN_CACHE_REFILL_METRICS.inflight_objects.dec();
                    });
                    match sstable_store.pin_sst(object_id).await {
                        Ok(outcome) => GLOBAL_PIN_CACHE_REFILL_METRICS.record(outcome),
                        Err(error) => {
                            GLOBAL_PIN_CACHE_REFILL_METRICS
                                .refill_total
                                .with_label_values(&["error"])
                                .inc();
                            tracing::warn!(
                                object_id = object_id.as_raw_id(),
                                error = %error.as_report(),
                                "pin cache refill failed"
                            );
                        }
                    }
                }
            }
        });
        Self { pending, wakeup }
    }

    fn update(
        &self,
        removed: HashSet<HummockSstableObjectId>,
        inserted: HashSet<HummockSstableObjectId>,
    ) {
        if removed.is_empty() && inserted.is_empty() {
            return;
        }
        self.pending.lock().update(removed, inserted);
        self.wake();
    }

    fn retain(&self, desired: HashSet<HummockSstableObjectId>) {
        self.pending.lock().retain(&desired);
        self.wake();
    }

    fn wake(&self) {
        match self.wakeup.try_send(()) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(())) => {}
            Err(mpsc::error::TrySendError::Closed(())) => {
                tracing::error!(
                    "pin cache refill worker stopped; pending work will not be processed"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_hummock_sdk::HummockSstableObjectId;

    use super::PendingPinObjects;

    #[test]
    fn test_pending_objects_compact_superseded_deltas() {
        let object = |id| HummockSstableObjectId::from(id);
        let mut pending = PendingPinObjects::default();
        pending.update(Default::default(), [object(1)].into());
        pending.update([object(1)].into(), [object(2)].into());
        pending.update([object(2)].into(), [object(3)].into());

        assert_eq!(pending.pop(), Some(object(3)));
        assert_eq!(pending.pop(), None);
    }
}
