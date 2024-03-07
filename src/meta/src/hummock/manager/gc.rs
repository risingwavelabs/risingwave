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
use std::ops::DerefMut;
use std::time::Duration;

use anyhow::Context;
use function_name::named;
use futures::{stream, StreamExt};
use itertools::Itertools;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;
use risingwave_pb::hummock::FullScanTask;

use crate::hummock::error::{Error, Result};
use crate::hummock::manager::{commit_multi_var, create_trx_wrapper, read_lock, write_lock};
use crate::hummock::HummockManager;
use crate::manager::MetadataManager;
use crate::model::{BTreeMapTransaction, BTreeMapTransactionWrapper, ValTransaction};
use crate::storage::MetaStore;

impl HummockManager {
    /// Gets SST objects that is safe to be deleted from object store.
    #[named]
    pub async fn get_objects_to_delete(&self) -> Vec<HummockSstableObjectId> {
        read_lock!(self, versioning)
            .await
            .objects_to_delete
            .iter()
            .cloned()
            .collect_vec()
    }

    /// Acknowledges SSTs have been deleted from object store.
    #[named]
    pub async fn ack_deleted_objects(&self, object_ids: &[HummockSstableObjectId]) -> Result<()> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        for object_id in object_ids {
            versioning_guard.objects_to_delete.remove(object_id);
        }
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
    #[named]
    pub async fn delete_version_deltas(&self, batch_size: usize) -> Result<(usize, usize)> {
        let mut versioning_guard = write_lock!(self, versioning).await;
        let versioning = versioning_guard.deref_mut();
        let deltas_to_delete = versioning
            .hummock_version_deltas
            .range(..=versioning.checkpoint.version.id)
            .map(|(k, _)| *k)
            .collect_vec();
        // If there is any safe point, skip this to ensure meta backup has required delta logs to
        // replay version.
        if !versioning.version_safe_points.is_empty() {
            return Ok((0, deltas_to_delete.len()));
        }
        let mut hummock_version_deltas = create_trx_wrapper!(
            self.sql_meta_store(),
            BTreeMapTransactionWrapper,
            BTreeMapTransaction::new(&mut versioning.hummock_version_deltas,)
        );
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
        commit_multi_var!(
            self.env.meta_store(),
            self.sql_meta_store(),
            hummock_version_deltas
        )?;
        #[cfg(test)]
        {
            drop(versioning_guard);
            self.check_state_consistency().await;
        }
        Ok((batch.len(), deltas_to_delete.len() - batch.len()))
    }

    /// Extends `objects_to_delete` according to object store full scan result.
    /// Caller should ensure `object_ids` doesn't include any SST objects belong to a on-going
    /// version write. That's to say, these object_ids won't appear in either `commit_epoch` or
    /// `report_compact_task`.
    #[named]
    pub async fn extend_objects_to_delete_from_scan(
        &self,
        object_ids: &[HummockSstableObjectId],
    ) -> usize {
        let tracked_object_ids: HashSet<HummockSstableObjectId> = {
            let versioning_guard = read_lock!(self, versioning).await;
            let mut tracked_object_ids =
                HashSet::from_iter(versioning_guard.current_version.get_object_ids());
            for delta in versioning_guard.hummock_version_deltas.values() {
                tracked_object_ids.extend(delta.gc_object_ids.iter().cloned());
            }
            tracked_object_ids
        };
        let to_delete = object_ids
            .iter()
            .filter(|object_id| !tracked_object_ids.contains(object_id));
        let mut versioning_guard = write_lock!(self, versioning).await;
        versioning_guard.objects_to_delete.extend(to_delete.clone());
        drop(versioning_guard);
        to_delete.count()
    }

    /// Starts a full GC.
    /// 1. Meta node sends a `FullScanTask` to a compactor in this method.
    /// 2. The compactor returns scan result of object store to meta node. See
    /// `HummockManager::full_scan_inner` in storage crate.
    /// 3. Meta node decides which SSTs to delete. See `HummockManager::complete_full_gc`.
    ///
    /// Returns Ok(false) if there is no worker available.
    pub fn start_full_gc(&self, sst_retention_time: Duration) -> Result<bool> {
        self.metrics.full_gc_trigger_count.inc();
        // Set a minimum sst_retention_time to avoid deleting SSTs of on-going write op.
        let sst_retention_time = cmp::max(
            sst_retention_time,
            Duration::from_secs(self.env.opts.min_sst_retention_time_sec),
        );
        tracing::info!(
            "run full GC with sst_retention_time = {} secs",
            sst_retention_time.as_secs()
        );
        let compactor = match self.compactor_manager.next_compactor() {
            None => {
                tracing::warn!("Try full GC but no available idle worker.");
                return Ok(false);
            }
            Some(compactor) => compactor,
        };
        compactor
            .send_event(ResponseEvent::FullScanTask(FullScanTask {
                sst_retention_time_sec: sst_retention_time.as_secs(),
            }))
            .map_err(|_| Error::CompactorUnreachable(compactor.context_id()))?;
        Ok(true)
    }

    /// Given candidate SSTs to GC, filter out false positive.
    /// Returns number of SSTs to GC.
    pub async fn complete_full_gc(&self, object_ids: Vec<HummockSstableObjectId>) -> Result<usize> {
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
        let candidate_sst_number = object_ids.len();
        metrics
            .full_gc_candidate_object_count
            .observe(candidate_sst_number as _);
        // 1. filter by watermark
        let object_ids = object_ids
            .into_iter()
            .filter(|s| *s < watermark)
            .collect_vec();
        // 2. filter by version
        let selected_sst_number = self.extend_objects_to_delete_from_scan(&object_ids).await;
        metrics
            .full_gc_selected_object_count
            .observe(selected_sst_number as _);
        tracing::info!("GC watermark is {}. SST full scan returns {} SSTs. {} remains after filtered by GC watermark. {} remains after filtered by hummock version.",
            watermark, candidate_sst_number, object_ids.len(), selected_sst_number);
        Ok(selected_sst_number)
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
    use risingwave_hummock_sdk::HummockSstableObjectId;

    use crate::hummock::manager::ResponseEvent;
    use crate::hummock::test_utils::{add_test_tables, setup_compute_env};
    use crate::MetaOpts;

    #[tokio::test]
    async fn test_full_gc() {
        let (mut env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
        // Use smaller spin interval to accelerate test.
        env.opts = Arc::new(MetaOpts {
            collect_gc_watermark_spin_interval_sec: 1,
            ..(*env.opts).clone()
        });

        // No task scheduled because no available worker.
        assert!(!hummock_manager
            .start_full_gc(Duration::from_secs(
                hummock_manager.env.opts.min_sst_retention_time_sec - 1
            ))
            .unwrap());

        let mut receiver = compactor_manager.add_compactor(context_id);

        assert!(hummock_manager
            .start_full_gc(Duration::from_secs(
                hummock_manager.env.opts.min_sst_retention_time_sec - 1
            ))
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
            .start_full_gc(Duration::from_secs(
                hummock_manager.env.opts.min_sst_retention_time_sec + 1
            ))
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
        hummock_manager.complete_full_gc(vec![]).await.unwrap();

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
                .complete_full_gc(vec![1, 2, 3])
                .await
                .unwrap()
        );

        // All committed SST ids should be excluded from GC.
        let sst_infos = add_test_tables(hummock_manager.as_ref(), context_id).await;
        let committed_object_ids = sst_infos
            .into_iter()
            .flatten()
            .map(|s| s.get_object_id())
            .sorted()
            .collect_vec();
        assert!(!committed_object_ids.is_empty());
        let max_committed_object_id = *committed_object_ids.iter().max().unwrap();
        assert_eq!(
            1,
            hummock_manager
                .complete_full_gc(
                    [committed_object_ids, vec![max_committed_object_id + 1]].concat()
                )
                .await
                .unwrap()
        );
    }
}
