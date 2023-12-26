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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_pb::hummock::subscribe_compaction_event_response::Event as ResponseEvent;
use risingwave_pb::hummock::VacuumTask;

use super::CompactorManagerRef;
use crate::backup_restore::BackupManagerRef;
use crate::hummock::HummockManagerRef;
use crate::manager::MetaSrvEnv;
use crate::MetaResult;

pub type VacuumManagerRef = Arc<VacuumManager>;

pub struct VacuumManager {
    env: MetaSrvEnv,
    hummock_manager: HummockManagerRef,
    backup_manager: BackupManagerRef,
    /// Use the CompactorManager to dispatch VacuumTask.
    compactor_manager: CompactorManagerRef,
    /// SST object ids which have been dispatched to vacuum nodes but are not replied yet.
    pending_object_ids: parking_lot::RwLock<HashSet<HummockSstableObjectId>>,
}

impl VacuumManager {
    pub fn new(
        env: MetaSrvEnv,
        hummock_manager: HummockManagerRef,
        backup_manager: BackupManagerRef,
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

    /// Tries to delete stale Hummock metadata.
    ///
    /// Returns number of deleted deltas
    pub async fn vacuum_metadata(&self) -> MetaResult<usize> {
        let batch_size = 64usize;
        let mut total_deleted = 0;
        loop {
            if total_deleted != 0 && self.env.opts.vacuum_spin_interval_ms != 0 {
                tokio::time::sleep(Duration::from_millis(self.env.opts.vacuum_spin_interval_ms))
                    .await;
            }
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

    /// Schedules deletion of SST objects from object store.
    ///
    /// Returns SST objects scheduled in worker node.
    pub async fn vacuum_object(&self) -> MetaResult<Vec<HummockSstableObjectId>> {
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
            if batch_idx != 0 && self.env.opts.vacuum_spin_interval_ms != 0 {
                tokio::time::sleep(Duration::from_millis(self.env.opts.vacuum_spin_interval_ms))
                    .await;
            }
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
            match compactor.send_event(ResponseEvent::VacuumTask(VacuumTask {
                // The SST id doesn't necessarily have a counterpart SST file in S3, but
                // it's OK trying to delete it.
                sstable_object_ids: delete_batch.clone(),
            })) {
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

    async fn filter_out_pinned_ssts(
        &self,
        objects_to_delete: &mut Vec<HummockSstableObjectId>,
    ) -> MetaResult<()> {
        let reject = self.backup_manager.list_pinned_ssts();
        // Ack these SSTs immediately, because they tend to be pinned for long time.
        // They will be GCed during full GC when they are no longer pinned.
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;
    use risingwave_hummock_sdk::HummockVersionId;
    use risingwave_pb::hummock::VacuumTask;

    use crate::backup_restore::BackupManager;
    use crate::hummock::test_utils::{add_test_tables, setup_compute_env};
    use crate::hummock::VacuumManager;

    #[tokio::test]
    async fn test_vacuum() {
        let (env, hummock_manager, _cluster_manager, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = hummock_manager.compactor_manager_ref_for_test();
        let backup_manager =
            Arc::new(BackupManager::for_test(env.clone(), hummock_manager.clone()).await);
        let vacuum = Arc::new(VacuumManager::new(
            env,
            hummock_manager.clone(),
            backup_manager,
            compactor_manager.clone(),
        ));
        assert_eq!(VacuumManager::vacuum_metadata(&vacuum).await.unwrap(), 0);
        assert_eq!(
            VacuumManager::vacuum_object(&vacuum).await.unwrap().len(),
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
            VacuumManager::vacuum_object(&vacuum).await.unwrap().len(),
            0
        );
        let _receiver = compactor_manager.add_compactor(context_id);
        // SST deletion is scheduled.
        assert_eq!(
            VacuumManager::vacuum_object(&vacuum).await.unwrap().len(),
            3
        );
        // The deletion is not acked yet.
        assert_eq!(
            VacuumManager::vacuum_object(&vacuum).await.unwrap().len(),
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
            VacuumManager::vacuum_object(&vacuum).await.unwrap().len(),
            0
        );
    }
}
