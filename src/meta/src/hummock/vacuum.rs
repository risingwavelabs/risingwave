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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use futures::{stream, StreamExt};
use itertools::Itertools;
use risingwave_hummock_sdk::HummockSstableId;
use risingwave_pb::common::WorkerType;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{FullScanTask, VacuumTask};

use crate::hummock::error::{Error, Result};
use crate::hummock::{CompactorManager, HummockManagerRef};
use crate::manager::{ClusterManagerRef, MetaSrvEnv};
use crate::storage::MetaStore;
use crate::MetaResult;

pub struct VacuumManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    hummock_manager: HummockManagerRef<S>,
    /// Use the CompactorManager to dispatch VacuumTask.
    compactor_manager: Arc<CompactorManager>,
    /// SST ids which have been dispatched to vacuum nodes but are not replied yet.
    pending_sst_ids: parking_lot::RwLock<HashSet<HummockSstableId>>,
}

impl<S> VacuumManager<S>
where
    S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        hummock_manager: HummockManagerRef<S>,
        compactor_manager: Arc<CompactorManager>,
    ) -> Self {
        Self {
            env,
            hummock_manager,
            compactor_manager,
            pending_sst_ids: Default::default(),
        }
    }

    /// Tries to make checkpoint at the minimum pinned version.
    ///
    /// Returns number of deleted deltas
    pub async fn vacuum_metadata(&self) -> MetaResult<usize> {
        self.hummock_manager.proceed_version_checkpoint().await?;
        let batch_size = 64usize;
        let mut total_deleted = 0;
        loop {
            let (deleted, remain) = self
                .hummock_manager
                .delete_version_deltas(batch_size)
                .await?;
            total_deleted += deleted;
            if remain < batch_size {
                break;
            }
        }
        Ok(total_deleted)
    }

    /// Schedules deletion of SSTs from object store
    ///
    /// Returns SSTs scheduled in worker node.
    pub async fn vacuum_sst_data(&self) -> MetaResult<Vec<HummockSstableId>> {
        // Select SSTs to delete.
        let ssts_to_delete = {
            // 1. Retry the pending SSTs first.
            // It is possible some vacuum workers have been asked to vacuum these SSTs previously,
            // but they don't report the results yet due to either latency or failure.
            // This is OK since trying to delete the same SST multiple times is safe.
            let pending_sst_ids = self.pending_sst_ids.read().iter().cloned().collect_vec();
            if !pending_sst_ids.is_empty() {
                pending_sst_ids
            } else {
                // 2. If no pending SSTs, then fetch new ones.
                let ssts_to_delete = self.hummock_manager.get_ssts_to_delete().await;
                if ssts_to_delete.is_empty() {
                    return Ok(vec![]);
                }
                // Track these SST ids, so that we can remove them from metadata later.
                self.pending_sst_ids.write().extend(ssts_to_delete.clone());
                ssts_to_delete
            }
        };

        // Dispatch the vacuum task
        let mut batch_idx = 0;
        let batch_size = 32usize;
        let mut sent_batch = Vec::with_capacity(ssts_to_delete.len());
        while batch_idx < ssts_to_delete.len() {
            let delete_batch = ssts_to_delete
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
                    sstable_ids: delete_batch.clone(),
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
                        .remove_compactor(compactor.context_id());
                }
            }
        }
        Ok(sent_batch)
    }

    /// Acknowledges deletion of SSTs and deletes corresponding metadata.
    pub async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> MetaResult<()> {
        let deleted_sst_ids = self
            .pending_sst_ids
            .read()
            .iter()
            .filter(|p| vacuum_task.sstable_ids.contains(p))
            .cloned()
            .collect_vec();
        if !deleted_sst_ids.is_empty() {
            self.hummock_manager
                .ack_deleted_ssts(&deleted_sst_ids)
                .await?;
            self.pending_sst_ids
                .write()
                .retain(|p| !deleted_sst_ids.contains(p));
        }
        tracing::info!("Finish vacuuming SSTs {:?}", vacuum_task.sstable_ids);
        Ok(())
    }

    /// Starts a full GC.
    /// 1. Meta node sends a `FullScanTask` to a compactor in this method.
    /// 2. The compactor returns scan result of object store to meta node. See
    /// `Vacuum::full_scan_inner` in storage crate.
    /// 3. Meta node decides which SSTs to delete. See `VacuumManager::complete_full_gc`.
    pub async fn start_full_gc(&self, sst_retention_time: Duration) -> Result<()> {
        // Set a minimum sst_retention_time to avoid deleting SSTs of on-going write op.
        let sst_retention_time = cmp::max(
            sst_retention_time,
            Duration::from_secs(self.env.opts.min_sst_retention_time_sec),
        );
        tracing::info!(
            "run full GC with sst_retention_time = {} secs",
            sst_retention_time.as_secs()
        );
        let compactor = match self
            .compactor_manager
            .next_idle_compactor(&self.hummock_manager)
            .await
        {
            None => {
                tracing::warn!("Try full GC but no available idle worker.");
                return Ok(());
            }
            Some(compactor) => compactor,
        };
        compactor
            .send_task(Task::FullScanTask(FullScanTask {
                sst_retention_time_sec: sst_retention_time.as_secs(),
            }))
            .await
            .map_err(|_| Error::CompactorUnreachable(compactor.context_id()))
    }

    pub async fn complete_full_gc(&self, sst_ids: Vec<HummockSstableId>) -> Result<usize> {
        tracing::info!("SST full scan returns {} SSTs", sst_ids.len());
        let spin_interval =
            Duration::from_secs(self.env.opts.collect_gc_watermark_spin_interval_sec);
        let watermark = collect_global_gc_watermark(
            self.hummock_manager.cluster_manager().clone(),
            spin_interval,
        )
        .await?;
        // 1. filter by watermark
        let sst_ids = sst_ids.into_iter().filter(|s| *s < watermark).collect_vec();
        // 2. filter by version
        let number = self
            .hummock_manager
            .extend_ssts_to_delete_from_scan(&sst_ids)
            .await;
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
) -> Result<HummockSstableId>
where
    S: MetaStore,
{
    let mut global_watermark = HummockSstableId::MAX;
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
            worker_watermark.unwrap_or(HummockSstableId::MAX),
        );
    }
    Ok(global_watermark)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use itertools::Itertools;
    use risingwave_pb::hummock::VacuumTask;

    use crate::hummock::test_utils::{add_test_tables, setup_compute_env};
    use crate::hummock::{start_vacuum_scheduler, CompactorManager, VacuumManager};

    #[tokio::test]
    async fn test_shutdown_vacuum() {
        let (env, hummock_manager, _cluster_manager, _worker_node) = setup_compute_env(80).await;
        let compactor_manager = Arc::new(CompactorManager::new(1));
        let vacuum = Arc::new(VacuumManager::new(env, hummock_manager, compactor_manager));
        let (join_handle, shutdown_sender) =
            start_vacuum_scheduler(vacuum, Duration::from_secs(60));
        shutdown_sender.send(()).unwrap();
        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_vacuum_basic() {
        let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = Arc::new(CompactorManager::new(1));
        let vacuum = Arc::new(VacuumManager::new(
            env,
            hummock_manager.clone(),
            compactor_manager.clone(),
        ));
        let _receiver = compactor_manager.add_compactor(0, u64::MAX);

        assert_eq!(VacuumManager::vacuum_metadata(&vacuum).await.unwrap(), 0);

        assert_eq!(
            VacuumManager::vacuum_sst_data(&vacuum).await.unwrap().len(),
            0
        );

        let sst_infos = add_test_tables(hummock_manager.as_ref(), context_id).await;
        // Current state: {v0: [], v1: [test_tables], v2: [test_tables_2, to_delete:test_tables],
        // v3: [test_tables_2, test_tables_3]}

        // Makes checkpoint and extends deltas_to_delete. Deletes deltas of v0->v1 and v2->v3.
        // Delta of v1->v2 cannot be deleted yet because it's used by ssts_to_delete.
        assert_eq!(VacuumManager::vacuum_metadata(&vacuum).await.unwrap(), 2);

        // Found ssts_to_delete
        assert_eq!(
            VacuumManager::vacuum_sst_data(&vacuum).await.unwrap().len(),
            3
        );

        // The deletion is not acked yet.
        assert_eq!(
            VacuumManager::vacuum_sst_data(&vacuum).await.unwrap().len(),
            3
        );

        // The delta cannot be deleted yet.
        assert_eq!(VacuumManager::vacuum_metadata(&vacuum).await.unwrap(), 0);

        // The vacuum task is reported.
        vacuum
            .report_vacuum_task(VacuumTask {
                sstable_ids: sst_infos
                    .first()
                    .unwrap()
                    .iter()
                    .map(|s| s.id)
                    .collect_vec(),
            })
            .await
            .unwrap();

        // The delta can be deleted now.
        assert_eq!(VacuumManager::vacuum_metadata(&vacuum).await.unwrap(), 1);

        // No ssts_to_delete.
        assert_eq!(
            VacuumManager::vacuum_sst_data(&vacuum).await.unwrap().len(),
            0
        );
    }
}
