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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::{stream, StreamExt};
use itertools::Itertools;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_pb::common::WorkerType;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{FullScanTask, VacuumTask};

use super::CompactorManagerRef;
use crate::backup_restore::BackupManagerRef;
use crate::hummock::error::{Error, Result};
use crate::hummock::HummockManagerRef;
use crate::manager::{ClusterManagerRef, MetaSrvEnv};
use crate::storage::MetaStore;
use crate::MetaResult;

pub type VacuumManagerRef<S> = Arc<VacuumManager<S>>;

pub struct VacuumManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    hummock_manager: HummockManagerRef<S>,
    backup_manager: BackupManagerRef<S>,
    /// Use the CompactorManager to dispatch VacuumTask.
    compactor_manager: CompactorManagerRef,
    /// SST object ids which have been dispatched to vacuum nodes but are not replied yet.
    pending_object_ids: parking_lot::RwLock<HashSet<HummockSstableObjectId>>,
}

impl<S> VacuumManager<S>
where
    S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        hummock_manager: HummockManagerRef<S>,
        backup_manager: BackupManagerRef<S>,
        compactor_manager: CompactorManagerRef,
    ) -> Self {
        Self {
            env,
            hummock_manager,
            backup_manager,
            compactor_manager,
            pending_object_ids: Default::default(),
        }
    }

    /// Tries to make checkpoint at the minimum pinned version.
    ///
    /// Returns number of deleted deltas
    pub async fn vacuum_metadata(&self) -> MetaResult<usize> {
        let batch_size = 64usize;
        let mut total_deleted = 0;
        loop {
            let (deleted, remain) = self
                .hummock_manager
                .delete_version_deltas(batch_size)
                .await?;
            total_deleted += deleted;
            if total_deleted == 0 || remain < batch_size {
                break;
            }
        }
        Ok(total_deleted)
    }

    /// Schedules deletion of SST objects from object store
    ///
    /// Returns SST objects scheduled in worker node.
    pub async fn vacuum_sst_data(&self) -> MetaResult<Vec<HummockSstableObjectId>> {
        // Select SST objects to delete.
        let objects_to_delete = {
            // 1. Retry the pending SST objects first.
            // It is possible some vacuum workers have been asked to vacuum these SST objects
            // previously, but they don't report the results yet due to either latency
            // or failure. This is OK since trying to delete the same SST object
            // multiple times is safe.
            let pending_object_ids = self.pending_object_ids.read().iter().cloned().collect_vec();
            if !pending_object_ids.is_empty() {
                pending_object_ids
            } else {
                // 2. If no pending SST objects, then fetch new ones.
                let mut objects_to_delete = self.hummock_manager.get_objects_to_delete().await;
                self.filter_out_pinned_ssts(&mut objects_to_delete).await?;
                if objects_to_delete.is_empty() {
                    return Ok(vec![]);
                }
                // Track these SST object ids, so that we can remove them from metadata later.
                self.pending_object_ids
                    .write()
                    .extend(objects_to_delete.clone());
                objects_to_delete
            }
        };

        // Dispatch the vacuum task
        let mut batch_idx = 0;
        let batch_size = 500usize;
        let mut sent_batch = Vec::with_capacity(objects_to_delete.len());
        while batch_idx < objects_to_delete.len() {
            let delete_batch = objects_to_delete
                .iter()
                .skip(batch_idx)
                .take(batch_size)
                .cloned()
                .collect_vec();
            // 1. Pick a worker.
            let compactor = match self.compactor_manager.next_compactor() {
                None => {
                    tracing::warn!("No vacuum worker is available.");
                    break;
                }
                Some(compactor) => compactor,
            };

            // 2. Send task.
            match compactor
                .send_task(Task::VacuumTask(VacuumTask {
                    // The SST id doesn't necessarily have a counterpart SST file in S3, but
                    // it's OK trying to delete it.
                    sstable_object_ids: delete_batch.clone(),
                }))
                .await
            {
                Ok(_) => {
                    tracing::debug!(
                        "Try to vacuum SSTs {:?} in worker {}.",
                        delete_batch,
                        compactor.context_id()
                    );
                    batch_idx += batch_size;
                    sent_batch.extend(delete_batch);
                }
                Err(err) => {
                    tracing::warn!(
                        "Failed to send vacuum task to worker {}: {:#?}",
                        compactor.context_id(),
                        err
                    );
                    self.compactor_manager
                        .pause_compactor(compactor.context_id());
                }
            }
        }
        Ok(sent_batch)
    }

    async fn filter_out_pinned_ssts(
        &self,
        objects_to_delete: &mut Vec<HummockSstableObjectId>,
    ) -> MetaResult<()> {
        let reject: HashSet<HummockSstableObjectId> =
            self.backup_manager.list_pinned_ssts().into_iter().collect();
        // Ack these pinned SSTs directly. Otherwise delta log containing them cannot be GCed.
        // These SSTs will be GCed during full GC when they are no longer pinned.
        let to_ack = objects_to_delete
            .iter()
            .filter(|s| reject.contains(s))
            .cloned()
            .collect_vec();
        if to_ack.is_empty() {
            return Ok(());
        }
        self.hummock_manager.ack_deleted_objects(&to_ack).await?;
        objects_to_delete.retain(|s| !reject.contains(s));
        Ok(())
    }

    /// Acknowledges deletion of SSTs and deletes corresponding metadata.
    pub async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> MetaResult<()> {
        let deleted_object_ids = self
            .pending_object_ids
            .read()
            .iter()
            .filter(|p| vacuum_task.sstable_object_ids.contains(p))
            .cloned()
            .collect_vec();
        if !deleted_object_ids.is_empty() {
            self.hummock_manager
                .ack_deleted_objects(&deleted_object_ids)
                .await?;
            self.pending_object_ids
                .write()
                .retain(|p| !deleted_object_ids.contains(p));
        }
        tracing::info!("Finish vacuuming SSTs {:?}", vacuum_task.sstable_object_ids);
        Ok(())
    }

    /// Starts a full GC.
    /// 1. Meta node sends a `FullScanTask` to a compactor in this method.
    /// 2. The compactor returns scan result of object store to meta node. See
    /// `Vacuum::full_scan_inner` in storage crate.
    /// 3. Meta node decides which SSTs to delete. See `VacuumManager::complete_full_gc`.
    ///
    /// Returns Ok(false) if there is no worker available.
    pub async fn start_full_gc(&self, sst_retention_time: Duration) -> Result<bool> {
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
            .send_task(Task::FullScanTask(FullScanTask {
                sst_retention_time_sec: sst_retention_time.as_secs(),
            }))
            .await
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
        let spin_interval =
            Duration::from_secs(self.env.opts.collect_gc_watermark_spin_interval_sec);
        let watermark = collect_global_gc_watermark(
            self.hummock_manager.cluster_manager().clone(),
            spin_interval,
        )
        .await?;
        let sst_number = object_ids.len();
        // 1. filter by watermark
        let object_ids = object_ids
            .into_iter()
            .filter(|s| *s < watermark)
            .collect_vec();
        // 2. filter by version
        let number = self
            .hummock_manager
            .extend_objects_to_delete_from_scan(&object_ids)
            .await;
        tracing::info!("GC watermark is {}. SST full scan returns {} SSTs. {} remains after filtered by GC watermark. {} remains after filtered by hummock version.",
            watermark, sst_number, object_ids.len(), number);
        Ok(number)
    }
}

/// Collects SST GC watermark from related cluster nodes and calculates a global one.
///
/// It must wait enough heartbeats first. This precondition is checked at `spin_interval`.
///
/// Returns a global GC watermark. The watermark only guards SSTs created before this
/// invocation.
pub async fn collect_global_gc_watermark<S>(
    cluster_manager: ClusterManagerRef<S>,
    spin_interval: Duration,
) -> Result<HummockSstableObjectId>
where
    S: MetaStore,
{
    let mut global_watermark = HummockSstableObjectId::MAX;
    let workers = vec![
        cluster_manager
            .list_worker_node(WorkerType::ComputeNode, None)
            .await,
        cluster_manager
            .list_worker_node(WorkerType::Compactor, None)
            .await,
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
        let cluster_manager_clone = cluster_manager.clone();
        worker_futures.push(tokio::spawn(async move {
            let mut init_version_id: Option<u64> = None;
            loop {
                let worker_info = match cluster_manager_clone.get_worker_by_id(worker_id).await {
                    None => {
                        return None;
                    }
                    Some(worker_info) => worker_info,
                };
                match init_version_id.as_ref() {
                    None => {
                        init_version_id = Some(worker_info.info_version_id());
                    }
                    Some(init_version_id) => {
                        if worker_info.info_version_id() >= *init_version_id + 2 {
                            return worker_info.hummock_gc_watermark();
                        }
                    }
                }
                tokio::time::sleep(spin_interval).await;
            }
        }));
    }
    let mut buffered = stream::iter(worker_futures).buffer_unordered(workers.len());
    while let Some(worker_result) = buffered.next().await {
        let worker_watermark = worker_result
            .map_err(|e| anyhow::anyhow!("Failed to collect GC watermark: {:#?}", e))?;
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
    use risingwave_hummock_sdk::{HummockSstableObjectId, HummockVersionId};
    use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
    use risingwave_pb::hummock::VacuumTask;

    use crate::backup_restore::BackupManager;
    use crate::hummock::test_utils::{add_test_tables, setup_compute_env};
    use crate::hummock::VacuumManager;
    use crate::MetaOpts;

    #[tokio::test]
    async fn test_vacuum() {
        let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
        let backup_manager = Arc::new(BackupManager::for_test(
            env.clone(),
            hummock_manager.clone(),
        ));
        let vacuum = Arc::new(VacuumManager::new(
            env,
            hummock_manager.clone(),
            backup_manager,
            compactor_manager.clone(),
        ));
        assert_eq!(VacuumManager::vacuum_metadata(&vacuum).await.unwrap(), 0);
        assert_eq!(
            VacuumManager::vacuum_sst_data(&vacuum).await.unwrap().len(),
            0
        );
        hummock_manager.pin_version(context_id).await.unwrap();
        let sst_infos = add_test_tables(hummock_manager.as_ref(), context_id).await;
        assert_eq!(VacuumManager::vacuum_metadata(&vacuum).await.unwrap(), 0);
        hummock_manager.create_version_checkpoint(1).await.unwrap();
        assert_eq!(VacuumManager::vacuum_metadata(&vacuum).await.unwrap(), 6);
        assert_eq!(VacuumManager::vacuum_metadata(&vacuum).await.unwrap(), 0);

        assert!(hummock_manager.get_objects_to_delete().await.is_empty());
        hummock_manager
            .unpin_version_before(context_id, HummockVersionId::MAX)
            .await
            .unwrap();
        assert!(!hummock_manager.get_objects_to_delete().await.is_empty());
        // No SST deletion is scheduled because no available worker.
        assert_eq!(
            VacuumManager::vacuum_sst_data(&vacuum).await.unwrap().len(),
            0
        );
        let _receiver = compactor_manager.add_compactor(context_id, u64::MAX);
        // SST deletion is scheduled.
        assert_eq!(
            VacuumManager::vacuum_sst_data(&vacuum).await.unwrap().len(),
            3
        );
        // The deletion is not acked yet.
        assert_eq!(
            VacuumManager::vacuum_sst_data(&vacuum).await.unwrap().len(),
            3
        );
        // The vacuum task is reported.
        vacuum
            .report_vacuum_task(VacuumTask {
                sstable_object_ids: sst_infos
                    .first()
                    .unwrap()
                    .iter()
                    .map(|s| s.get_object_id())
                    .collect_vec(),
            })
            .await
            .unwrap();
        // No objects_to_delete.
        assert_eq!(
            VacuumManager::vacuum_sst_data(&vacuum).await.unwrap().len(),
            0
        );
    }

    #[tokio::test]
    async fn test_full_gc() {
        let (mut env, hummock_manager, cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
        let backup_manager = Arc::new(BackupManager::for_test(
            env.clone(),
            hummock_manager.clone(),
        ));
        // Use smaller spin interval to accelerate test.
        env.opts = Arc::new(MetaOpts {
            collect_gc_watermark_spin_interval_sec: 1,
            ..(*env.opts).clone()
        });
        let vacuum = Arc::new(VacuumManager::new(
            env,
            hummock_manager.clone(),
            backup_manager,
            compactor_manager.clone(),
        ));

        // No task scheduled because no available worker.
        assert!(!vacuum
            .start_full_gc(Duration::from_secs(
                vacuum.env.opts.min_sst_retention_time_sec - 1
            ))
            .await
            .unwrap());

        let mut receiver = compactor_manager.add_compactor(context_id, u64::MAX);

        assert!(vacuum
            .start_full_gc(Duration::from_secs(
                vacuum.env.opts.min_sst_retention_time_sec - 1
            ))
            .await
            .unwrap());
        let full_scan_task = match receiver.recv().await.unwrap().unwrap().task.unwrap() {
            Task::FullScanTask(task) => task,
            _ => {
                panic!()
            }
        };
        // min_sst_retention_time_sec override user provided value.
        assert_eq!(
            vacuum.env.opts.min_sst_retention_time_sec,
            full_scan_task.sst_retention_time_sec
        );

        assert!(vacuum
            .start_full_gc(Duration::from_secs(
                vacuum.env.opts.min_sst_retention_time_sec + 1
            ))
            .await
            .unwrap());
        let full_scan_task = match receiver.recv().await.unwrap().unwrap().task.unwrap() {
            Task::FullScanTask(task) => task,
            _ => {
                panic!()
            }
        };
        // min_sst_retention_time_sec doesn't override user provided value.
        assert_eq!(
            vacuum.env.opts.min_sst_retention_time_sec + 1,
            full_scan_task.sst_retention_time_sec
        );

        // Empty input results immediate return, without waiting heartbeat.
        vacuum.complete_full_gc(vec![]).await.unwrap();

        // mimic CN heartbeat
        use risingwave_pb::meta::heartbeat_request::extra_info::Info;
        let heartbeat_interval = vacuum.env.opts.collect_gc_watermark_spin_interval_sec;
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
        assert_eq!(3, vacuum.complete_full_gc(vec![1, 2, 3]).await.unwrap());

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
            vacuum
                .complete_full_gc(
                    [committed_object_ids, vec![max_committed_object_id + 1]].concat()
                )
                .await
                .unwrap()
        );
    }
}
