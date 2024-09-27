// Copyright 2024 RisingWave Labs
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
use std::collections::HashSet;
use std::ops::Bound::{Excluded, Included};
use std::ops::DerefMut;
use std::time::Duration;

use anyhow::Context;
use futures::{stream, StreamExt};
use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;
use risingwave_pb::hummock::FullScanTask;

use crate::hummock::error::{Error, Result};
use crate::hummock::manager::commit_multi_var;
use crate::hummock::HummockManager;
use crate::manager::MetadataManager;
use crate::model::BTreeMapTransaction;

#[derive(Default)]
pub(super) struct DeleteObjectTracker {
    /// Objects that waits to be deleted from object store. It comes from either compaction, or
    /// full GC (listing object store).
    objects_to_delete: Mutex<HashSet<HummockSstableObjectId>>,
}

impl DeleteObjectTracker {
    pub(super) fn add(&self, objects: impl Iterator<Item = HummockSstableObjectId>) {
        self.objects_to_delete.lock().extend(objects)
    }

    pub(super) fn current(&self) -> HashSet<HummockSstableObjectId> {
        self.objects_to_delete.lock().clone()
    }

    pub(super) fn clear(&self) {
        self.objects_to_delete.lock().clear();
    }

    pub(super) fn ack<'a>(&self, objects: impl Iterator<Item = &'a HummockSstableObjectId>) {
        let mut lock = self.objects_to_delete.lock();
        for object in objects {
            lock.remove(object);
        }
    }
}

impl HummockManager {
    /// Gets SST objects that is safe to be deleted from object store.
    pub fn get_objects_to_delete(&self) -> Vec<HummockSstableObjectId> {
        self.delete_object_tracker
            .current()
            .iter()
            .cloned()
            .collect_vec()
    }

    /// Acknowledges SSTs have been deleted from object store.
    pub async fn ack_deleted_objects(&self, object_ids: &[HummockSstableObjectId]) -> Result<()> {
        self.delete_object_tracker.ack(object_ids.iter());
        let mut versioning_guard = self.versioning.write().await;
        for stale_objects in versioning_guard.checkpoint.stale_objects.values_mut() {
            stale_objects.id.retain(|id| !object_ids.contains(id));
        }
        versioning_guard
            .checkpoint
            .stale_objects
            .retain(|_, stale_objects| !stale_objects.id.is_empty());
        drop(versioning_guard);
        Ok(())
    }

    /// Deletes at most `batch_size` deltas.
    ///
    /// Returns (number of deleted deltas, number of remain `deltas_to_delete`).
    pub async fn delete_version_deltas(&self, batch_size: usize) -> Result<(usize, usize)> {
        let mut versioning_guard = self.versioning.write().await;
        let versioning = versioning_guard.deref_mut();
        let context_info = self.context_info.read().await;
        let deltas_to_delete = versioning
            .hummock_version_deltas
            .range(..=versioning.checkpoint.version.id)
            .map(|(k, _)| *k)
            .collect_vec();
        // If there is any safe point, skip this to ensure meta backup has required delta logs to
        // replay version.
        if !context_info.version_safe_points.is_empty() {
            return Ok((0, deltas_to_delete.len()));
        }
        let mut hummock_version_deltas =
            BTreeMapTransaction::new(&mut versioning.hummock_version_deltas);
        let batch = deltas_to_delete
            .iter()
            .take(batch_size)
            .cloned()
            .collect_vec();
        if batch.is_empty() {
            return Ok((0, 0));
        }
        for delta_id in &batch {
            hummock_version_deltas.remove(*delta_id);
        }
        commit_multi_var!(self.meta_store_ref(), hummock_version_deltas)?;
        #[cfg(test)]
        {
            drop(context_info);
            drop(versioning_guard);
            self.check_state_consistency().await;
        }
        Ok((batch.len(), deltas_to_delete.len() - batch.len()))
    }

    /// Extends `objects_to_delete` according to object store full scan result.
    /// Caller should ensure `object_ids` doesn't include any SST objects belong to a on-going
    /// version write. That's to say, these `object_ids` won't appear in either `commit_epoch` or
    /// `report_compact_task`.
    pub async fn extend_objects_to_delete_from_scan(
        &self,
        object_ids: &[HummockSstableObjectId],
    ) -> usize {
        let tracked_object_ids: HashSet<HummockSstableObjectId> = {
            let versioning = self.versioning.read().await;
            let context_info = self.context_info.read().await;

            // object ids in checkpoint version
            let mut tracked_object_ids = versioning.checkpoint.version.get_object_ids();
            // add object ids added between checkpoint version and current version
            for (_, delta) in versioning.hummock_version_deltas.range((
                Excluded(versioning.checkpoint.version.id),
                Included(versioning.current_version.id),
            )) {
                tracked_object_ids.extend(delta.newly_added_object_ids());
            }
            // add stale object ids before the checkpoint version
            let min_pinned_version_id = context_info.min_pinned_version_id();
            tracked_object_ids.extend(
                versioning
                    .checkpoint
                    .stale_objects
                    .iter()
                    .filter(|(version_id, _)| **version_id >= min_pinned_version_id)
                    .flat_map(|(_, objects)| objects.id.iter())
                    .cloned(),
            );
            tracked_object_ids
        };
        let to_delete = object_ids
            .iter()
            .filter(|object_id| !tracked_object_ids.contains(object_id))
            .collect_vec();
        self.delete_object_tracker
            .add(to_delete.iter().map(|id| **id));
        to_delete.len()
    }

    /// Starts a full GC.
    /// 1. Meta node sends a `FullScanTask` to a compactor in this method.
    /// 2. The compactor returns scan result of object store to meta node. See
    ///    `HummockManager::full_scan_inner` in storage crate.
    /// 3. Meta node decides which SSTs to delete. See `HummockManager::complete_full_gc`.
    ///
    /// Returns Ok(false) if there is no worker available.
    pub fn start_full_gc(
        &self,
        sst_retention_time: Duration,
        prefix: Option<String>,
    ) -> Result<bool> {
        self.metrics.full_gc_trigger_count.inc();
        // Set a minimum sst_retention_time to avoid deleting SSTs of on-going write op.
        let sst_retention_time = cmp::max(
            sst_retention_time,
            Duration::from_secs(self.env.opts.min_sst_retention_time_sec),
        );
        let start_after = self.full_gc_state.next_start_after();
        let limit = self.full_gc_state.limit;
        tracing::info!(
            retention_sec = sst_retention_time.as_secs(),
            prefix = prefix.as_ref().unwrap_or(&String::from("")),
            start_after,
            limit,
            "run full GC"
        );

        let compactor = match self.compactor_manager.next_compactor() {
            None => {
                tracing::warn!("full GC attempt but no available idle worker");
                return Ok(false);
            }
            Some(compactor) => compactor,
        };
        let sst_retention_watermark = self.now().saturating_sub(sst_retention_time.as_secs());
        compactor
            .send_event(ResponseEvent::FullScanTask(FullScanTask {
                sst_retention_watermark,
                prefix,
                start_after,
                limit,
            }))
            .map_err(|_| Error::CompactorUnreachable(compactor.context_id()))?;
        Ok(true)
    }

    /// Given candidate SSTs to GC, filter out false positive.
    /// Returns number of SSTs to GC.
    pub async fn complete_full_gc(
        &self,
        object_ids: Vec<HummockSstableObjectId>,
        next_start_after: Option<String>,
    ) -> Result<usize> {
        self.full_gc_state.set_next_start_after(next_start_after);
        if object_ids.is_empty() {
            tracing::info!("SST full scan returns no SSTs.");
            return Ok(0);
        }
        let metrics = &self.metrics;
        let spin_interval =
            Duration::from_secs(self.env.opts.collect_gc_watermark_spin_interval_sec);
        let watermark =
            collect_global_gc_watermark(self.metadata_manager().clone(), spin_interval).await?;
        metrics.full_gc_last_object_id_watermark.set(watermark as _);
        let candidate_object_number = object_ids.len();
        metrics
            .full_gc_candidate_object_count
            .observe(candidate_object_number as _);
        let pinned_object_ids = self
            .all_object_ids_in_time_travel()
            .await?
            .collect::<HashSet<_>>();
        // 1. filter by watermark
        let object_ids = object_ids
            .into_iter()
            .filter(|s| *s < watermark)
            .collect_vec();
        let after_watermark = object_ids.len();
        // 2. filter by time travel archive
        let object_ids = object_ids
            .into_iter()
            .filter(|s| !pinned_object_ids.contains(s))
            .collect_vec();
        let after_time_travel = object_ids.len();
        // 3. filter by version
        let selected_object_number = self.extend_objects_to_delete_from_scan(&object_ids).await;
        metrics
            .full_gc_selected_object_count
            .observe(selected_object_number as _);
        tracing::info!("GC watermark is {watermark}. Object full scan returns {candidate_object_number} objects. {after_watermark} remains after filtered by GC watermark. {after_time_travel} remains after filtered by time travel archives. {selected_object_number} remains after filtered by hummock version.");
        Ok(selected_object_number)
    }

    pub fn now(&self) -> u64 {
        // TODO: persist now to maintain non-decreasing even after a meta node reboot.
        let mut guard = self.now.lock();
        let new_now = chrono::Utc::now().timestamp().try_into().unwrap();
        if new_now < *guard {
            tracing::warn!(old = *guard, new = new_now, "unexpected decreasing now");
            return *guard;
        }
        *guard = new_now;
        new_now
    }
}

pub struct FullGcState {
    next_start_after: Mutex<Option<String>>,
    limit: Option<u64>,
}

impl FullGcState {
    pub fn new(limit: Option<u64>) -> Self {
        Self {
            next_start_after: Mutex::new(None),
            limit,
        }
    }

    pub fn set_next_start_after(&self, next_start_after: Option<String>) {
        *self.next_start_after.lock() = next_start_after;
    }

    pub fn next_start_after(&self) -> Option<String> {
        self.next_start_after.lock().clone()
    }
}

/// Collects SST GC watermark from related cluster nodes and calculates a global one.
///
/// It must wait enough heartbeats first. This precondition is checked at `spin_interval`.
///
/// Returns a global GC watermark. The watermark only guards SSTs created before this
/// invocation.
pub async fn collect_global_gc_watermark(
    metadata_manager: MetadataManager,
    spin_interval: Duration,
) -> Result<HummockSstableObjectId> {
    let mut global_watermark = HummockSstableObjectId::MAX;
    let workers = [
        metadata_manager
            .list_active_streaming_compute_nodes()
            .await
            .map_err(|err| Error::MetaStore(err.into()))?,
        metadata_manager
            .list_worker_node(Some(WorkerType::Compactor), Some(Running))
            .await
            .map_err(|err| Error::MetaStore(err.into()))?,
    ]
    .concat();

    if workers.is_empty() {
        return Ok(global_watermark);
    }

    let mut worker_futures = vec![];
    for worker in &workers {
        // For each cluster node, its watermark is collected after waiting for 2 heartbeats.
        // The first heartbeat may carry watermark took before the start of this method,
        // which doesn't correctly guard target SSTs.
        // The second heartbeat guarantees its watermark is took after the start of this method.
        let worker_id = worker.id;
        let metadata_manager_clone = metadata_manager.clone();
        worker_futures.push(tokio::spawn(async move {
            let mut init_version_id: Option<u64> = None;
            loop {
                let worker_info = match metadata_manager_clone
                    .get_worker_info_by_id(worker_id)
                    .await
                {
                    None => {
                        return None;
                    }
                    Some(worker_info) => worker_info,
                };
                match init_version_id.as_ref() {
                    None => {
                        init_version_id = Some(worker_info.info_version_id);
                    }
                    Some(init_version_id) => {
                        if worker_info.info_version_id >= *init_version_id + 2 {
                            return worker_info.hummock_gc_watermark;
                        }
                    }
                }
                tokio::time::sleep(spin_interval).await;
            }
        }));
    }
    let mut buffered = stream::iter(worker_futures).buffer_unordered(workers.len());
    while let Some(worker_result) = buffered.next().await {
        let worker_watermark = worker_result.context("Failed to collect GC watermark")?;
        // None means either the worker has gone or the worker has not set a watermark.
        global_watermark = cmp::min(
            global_watermark,
            worker_watermark.unwrap_or(HummockSstableObjectId::MAX),
        );
    }
    Ok(global_watermark)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use itertools::Itertools;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::HummockSstableObjectId;
    use risingwave_rpc_client::HummockMetaClient;

    use super::ResponseEvent;
    use crate::hummock::test_utils::{add_test_tables, setup_compute_env};
    use crate::hummock::MockHummockMetaClient;
    use crate::MetaOpts;

    #[tokio::test]
    async fn test_full_gc() {
        let (mut env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let hummock_meta_client: Arc<dyn HummockMetaClient> = Arc::new(MockHummockMetaClient::new(
            hummock_manager.clone(),
            worker_node.id,
        ));
        let compaction_group_id = StaticCompactionGroupId::StateDefault.into();
        let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
        // Use smaller spin interval to accelerate test.
        env.opts = Arc::new(MetaOpts {
            collect_gc_watermark_spin_interval_sec: 1,
            ..(*env.opts).clone()
        });

        // No task scheduled because no available worker.
        assert!(!hummock_manager
            .start_full_gc(
                Duration::from_secs(hummock_manager.env.opts.min_sst_retention_time_sec - 1,),
                None
            )
            .unwrap());

        let mut receiver = compactor_manager.add_compactor(context_id);

        assert!(hummock_manager
            .start_full_gc(
                Duration::from_secs(hummock_manager.env.opts.min_sst_retention_time_sec - 1),
                None
            )
            .unwrap());
        let full_scan_task = match receiver.recv().await.unwrap().unwrap().event.unwrap() {
            ResponseEvent::FullScanTask(task) => task,
            _ => {
                panic!()
            }
        };
        // min_sst_retention_time_sec override user provided value.
        assert_eq!(
            hummock_manager.env.opts.min_sst_retention_time_sec,
            full_scan_task.sst_retention_time_sec
        );

        assert!(hummock_manager
            .start_full_gc(
                Duration::from_secs(hummock_manager.env.opts.min_sst_retention_time_sec + 1),
                None
            )
            .unwrap());
        let full_scan_task = match receiver.recv().await.unwrap().unwrap().event.unwrap() {
            ResponseEvent::FullScanTask(task) => task,
            _ => {
                panic!()
            }
        };
        // min_sst_retention_time_sec doesn't override user provided value.
        assert_eq!(
            hummock_manager.env.opts.min_sst_retention_time_sec + 1,
            full_scan_task.sst_retention_time_sec
        );

        // Empty input results immediate return, without waiting heartbeat.
        hummock_manager
            .complete_full_gc(vec![], None)
            .await
            .unwrap();

        // mimic CN heartbeat
        use risingwave_pb::meta::heartbeat_request::extra_info::Info;
        let heartbeat_interval = hummock_manager
            .env
            .opts
            .collect_gc_watermark_spin_interval_sec;
        tokio::spawn(async move {
            loop {
                cluster_manager
                    .heartbeat(
                        context_id,
                        vec![Info::HummockGcWatermark(HummockSstableObjectId::MAX)],
                    )
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_secs(heartbeat_interval)).await;
            }
        });

        // LSMtree is empty. All input SST ids should be treated as garbage.
        assert_eq!(
            3,
            hummock_manager
                .complete_full_gc(vec![1, 2, 3], None)
                .await
                .unwrap()
        );

        // All committed SST ids should be excluded from GC.
        let sst_infos = add_test_tables(
            hummock_manager.as_ref(),
            hummock_meta_client.clone(),
            compaction_group_id,
        )
        .await;
        let committed_object_ids = sst_infos
            .into_iter()
            .flatten()
            .map(|s| s.object_id)
            .sorted()
            .collect_vec();
        assert!(!committed_object_ids.is_empty());
        let max_committed_object_id = *committed_object_ids.iter().max().unwrap();
        assert_eq!(
            1,
            hummock_manager
                .complete_full_gc(
                    [committed_object_ids, vec![max_committed_object_id + 1]].concat(),
                    None
                )
                .await
                .unwrap()
        );
    }
}
