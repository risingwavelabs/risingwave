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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use rand::Rng;
use risingwave_hummock_sdk::HummockContextId;
use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
use risingwave_pb::hummock::{
    CancelCompactTask, CompactTask, CompactTaskAssignment, CompactTaskProgress,
    SubscribeCompactTasksResponse,
};
use tokio::sync::mpsc::{Receiver, Sender};

use crate::hummock::HummockManager;
use crate::manager::MetaSrvEnv;
use crate::model::MetadataModel;
use crate::storage::MetaStore;
use crate::MetaResult;

const STREAM_BUFFER_SIZE: usize = 4;

pub type CompactorManagerRef = Arc<CompactorManager>;
type TaskId = u64;

/// Wraps the stream between meta node and compactor node.
/// Compactor node will re-establish the stream when the previous one fails.
pub struct Compactor {
    context_id: HummockContextId,
    sender: Sender<MetaResult<SubscribeCompactTasksResponse>>,
    max_concurrent_task_number: u64,
}

struct TaskHeartbeat {
    task: CompactTask,
    num_ssts_sealed: u32,
    num_ssts_uploaded: u32,
    expire_at: u64,
}

impl Compactor {
    pub async fn send_task(&self, task: Task) -> MetaResult<()> {
        self.sender
            .send(Ok(SubscribeCompactTasksResponse { task: Some(task) }))
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    pub async fn cancel_task(&self, task_id: u64) -> MetaResult<()> {
        self.sender
            .send(Ok(SubscribeCompactTasksResponse {
                task: Some(Task::CancelCompactTask(CancelCompactTask {
                    context_id: self.context_id,
                    task_id,
                })),
            }))
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }

    pub fn context_id(&self) -> HummockContextId {
        self.context_id
    }

    pub fn max_concurrent_task_number(&self) -> u64 {
        self.max_concurrent_task_number
    }
}

struct CompactorManagerInner {
    /// Senders of stream to available compactors
    compactors: Vec<HummockContextId>,

    /// TODO: Let each compactor have its own Mutex, we should not need to lock whole thing.
    /// The outer lock is a RwLock, so we should still be able to modify each compactor
    compactor_map: HashMap<HummockContextId, Arc<Compactor>>,

    /// We use round-robin approach to assign tasks to compactors.
    /// This field indexes the compactor which the next task should be assigned to.
    next_compactor: usize,
}

impl CompactorManagerInner {
    pub fn new() -> Self {
        Self {
            compactors: vec![],
            compactor_map: HashMap::new(),
            next_compactor: 0,
        }
    }
}

/// `CompactorManager` maintains compactors which can process compact task.
/// A compact task is tracked in `HummockManager::Compaction` via both `CompactStatus` and
/// `CompactTaskAssignment`. A compact task can be in one of these states:
/// - 1. Assigned: a compact task is assigned to a compactor via `HummockManager::get_compact_task`.
///   Assigned-->Finished/Cancelled.
/// - 2. Finished: an assigned task is reported as finished via
///   `CompactStatus::report_compact_task`. It's the final state.
/// - 3. Cancelled: an assigned task is reported as cancelled via
///   `CompactStatus::report_compact_task`. It's the final state.
pub struct CompactorManager {
    inner: parking_lot::RwLock<CompactorManagerInner>,
    pub task_expiry_seconds: u64,
    // A map: { context_id -> { task_id -> heartbeat } }
    task_heartbeats: parking_lot::RwLock<HashMap<HummockContextId, HashMap<TaskId, TaskHeartbeat>>>,
}

impl CompactorManager {
    pub async fn new_with_meta<S: MetaStore>(
        env: MetaSrvEnv<S>,
        task_expiry_seconds: u64,
    ) -> MetaResult<Self> {
        let manager = Self::new(task_expiry_seconds);
        // Initialize the existing task assignments from metastore
        CompactTaskAssignment::list(env.meta_store())
            .await?
            .into_iter()
            .for_each(|assignment| {
                manager.initiate_task_heartbeat(
                    assignment.context_id,
                    assignment.compact_task.unwrap(),
                );
            });
        Ok(manager)
    }

    pub fn new(task_expiry_seconds: u64) -> Self {
        Self {
            inner: parking_lot::RwLock::new(CompactorManagerInner::new()),
            task_expiry_seconds,
            task_heartbeats: Default::default(),
        }
    }

    pub async fn next_idle_compactor<S: MetaStore>(
        &self,
        hummock_manager: &HummockManager<S>,
    ) -> Option<Arc<Compactor>> {
        let mut visited = HashSet::new();
        loop {
            match self.next_compactor() {
                None => {
                    return None;
                }
                Some(compactor) => {
                    if visited.contains(&compactor.context_id()) {
                        return None;
                    }
                    if hummock_manager
                        .get_assigned_tasks_number(compactor.context_id())
                        .await
                        <= compactor.max_concurrent_task_number()
                    {
                        return Some(compactor);
                    }
                    visited.insert(compactor.context_id());
                }
            }
        }
    }

    /// Gets next compactor to assign task.
    pub fn next_compactor(&self) -> Option<Arc<Compactor>> {
        let mut guard = self.inner.write();
        if guard.compactors.is_empty() {
            return None;
        }
        let compactor_index = guard.next_compactor % guard.compactors.len();
        let compactor = guard.compactors[compactor_index];
        guard.next_compactor += 1;
        Some(guard.compactor_map.get(&compactor).unwrap().clone())
    }

    pub fn random_compactor(&self) -> Option<Arc<Compactor>> {
        let guard = self.inner.read();
        if guard.compactors.is_empty() {
            return None;
        }

        let compactor_index = rand::thread_rng().gen::<usize>() % guard.compactors.len();
        let compactor = guard.compactors[compactor_index];
        Some(guard.compactor_map.get(&compactor).unwrap().clone())
    }

    pub fn add_compactor(
        &self,
        context_id: HummockContextId,
        max_concurrent_task_number: u64,
    ) -> Receiver<MetaResult<SubscribeCompactTasksResponse>> {
        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_BUFFER_SIZE);
        let mut guard = self.inner.write();
        guard.compactors.retain(|c| *c != context_id);
        guard.compactor_map.remove(&context_id);
        guard.compactors.push(context_id);
        guard.compactor_map.insert(
            context_id,
            Arc::new(Compactor {
                context_id,
                sender: tx,
                max_concurrent_task_number,
            }),
        );
        tracing::info!("Added compactor session {}", context_id);
        rx
    }

    pub fn remove_compactor(&self, context_id: HummockContextId) {
        let mut guard = self.inner.write();
        guard.compactors.retain(|c| *c != context_id);
        guard.compactor_map.remove(&context_id);
        // To remove the heartbeats, they need to be forcefully purged,
        // which is only safe when the context has been completely removed from meta.
        tracing::info!("Removed compactor session {}", context_id);
    }

    /// Forcefully purging the heartbeats for a task is only safe when the
    /// context has been completely removed from meta.
    /// Returns true if there were remanining heartbeats for the task.
    pub fn purge_heartbeats_for_context(&self, context_id: HummockContextId) -> bool {
        self.task_heartbeats.write().remove(&context_id).is_some()
    }

    pub fn get_compactor(&self, context_id: u32) -> Option<Arc<Compactor>> {
        self.inner.read().compactor_map.get(&context_id).cloned()
    }

    pub fn get_expired_tasks(&self) -> Vec<(HummockContextId, CompactTask)> {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let mut cancellable_tasks = vec![];
        {
            let guard = self.task_heartbeats.read();
            for (context_id, heartbeats) in guard.iter() {
                {
                    for TaskHeartbeat {
                        expire_at, task, ..
                    } in heartbeats.values()
                    {
                        if *expire_at < now {
                            cancellable_tasks.push((*context_id, task.clone()));
                        }
                    }
                }
            }
        }
        cancellable_tasks
    }

    pub fn initiate_task_heartbeat(&self, context_id: HummockContextId, task: CompactTask) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let mut guard = self.task_heartbeats.write();
        let entry = guard.entry(context_id).or_insert_with(HashMap::new);
        entry.insert(
            task.task_id,
            TaskHeartbeat {
                task,
                num_ssts_sealed: 0,
                num_ssts_uploaded: 0,
                expire_at: now + self.task_expiry_seconds,
            },
        );
    }

    pub fn remove_task_heartbeat(&self, context_id: HummockContextId, task_id: u64) {
        let mut guard = self.task_heartbeats.write();
        let mut garbage_collect = false;
        if let Some(heartbeats) = guard.get_mut(&context_id) {
            heartbeats.remove(&task_id);
            if heartbeats.is_empty() {
                garbage_collect = true;
            }
        }
        if garbage_collect {
            guard.remove(&context_id);
        }
    }

    pub fn update_task_heartbeats(
        &self,
        context_id: HummockContextId,
        progress_list: &Vec<CompactTaskProgress>,
    ) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        let mut guard = self.task_heartbeats.write();
        if let Some(heartbeats) = guard.get_mut(&context_id) {
            for progress in progress_list {
                if let Some(task_ref) = heartbeats.get_mut(&progress.task_id) {
                    if task_ref.num_ssts_sealed < progress.num_ssts_sealed
                        || task_ref.num_ssts_uploaded < progress.num_ssts_uploaded
                    {
                        // Refresh the expiry of the task as it is showing progress.
                        task_ref.expire_at = now + self.task_expiry_seconds;
                        // Update the task state to the latest state.
                        task_ref.num_ssts_sealed = progress.num_ssts_sealed;
                        task_ref.num_ssts_uploaded = progress.num_ssts_uploaded;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::try_match_expand;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_pb::hummock::compact_task::TaskStatus;
    use risingwave_pb::hummock::subscribe_compact_tasks_response::Task;
    use risingwave_pb::hummock::CompactTask;
    use tokio::sync::mpsc::error::TryRecvError;

    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::test_utils::{
        commit_from_meta_node, generate_test_tables, get_sst_ids,
        register_sstable_infos_to_compaction_group, setup_compute_env_with_config,
        to_local_sstable_info,
    };
    use crate::hummock::{CompactorManager, HummockManager};
    use crate::storage::MetaStore;

    async fn add_compact_task<S>(hummock_manager: &HummockManager<S>, _context_id: u32, epoch: u64)
    where
        S: MetaStore,
    {
        let original_tables = generate_test_tables(epoch, get_sst_ids(hummock_manager, 2).await);
        register_sstable_infos_to_compaction_group(
            hummock_manager.compaction_group_manager_ref_for_test(),
            &original_tables,
            StaticCompactionGroupId::StateDefault.into(),
        )
        .await;
        commit_from_meta_node(
            hummock_manager,
            epoch,
            to_local_sstable_info(&original_tables),
        )
        .await
        .unwrap();
    }

    fn dummy_compact_task(task_id: u64) -> CompactTask {
        CompactTask {
            input_ssts: vec![],
            splits: vec![],
            watermark: 0,
            sorted_output_ssts: vec![],
            task_id,
            target_level: 0,
            gc_delete_keys: false,
            task_status: TaskStatus::Pending as i32,
            compaction_group_id: StaticCompactionGroupId::StateDefault.into(),
            existing_table_ids: vec![],
            compression_algorithm: 0,
            target_file_size: 1,
            compaction_filter_mask: 0,
            table_options: HashMap::default(),
            current_epoch_time: 0,
            target_sub_level_id: 0,
        }
    }

    #[tokio::test]
    async fn test_add_remove_compactor() {
        let compactor_manager = CompactorManager::new(1);
        // No compactors by default.
        assert_eq!(compactor_manager.inner.read().compactors.len(), 0);

        let mut receiver = compactor_manager.add_compactor(1, u64::MAX);
        assert_eq!(compactor_manager.inner.read().compactors.len(), 1);
        let _receiver_2 = compactor_manager.add_compactor(2, u64::MAX);
        assert_eq!(compactor_manager.inner.read().compactors.len(), 2);
        compactor_manager.remove_compactor(2);
        assert_eq!(compactor_manager.inner.read().compactors.len(), 1);

        // No compact task there.
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));

        let task = dummy_compact_task(123);
        let compactor = {
            let guard = compactor_manager.inner.write();
            let compactor_id = guard.compactors.first().unwrap();
            guard.compactor_map.get(compactor_id).unwrap().clone()
        };
        compactor
            .send_task(Task::CompactTask(task.clone()))
            .await
            .unwrap();
        // Receive a compact task.
        let received_task = receiver.try_recv().unwrap().unwrap().task.unwrap();
        let received_compact_task = try_match_expand!(received_task, Task::CompactTask).unwrap();
        assert_eq!(received_compact_task, task);

        compactor_manager.remove_compactor(compactor.context_id);
        assert_eq!(compactor_manager.inner.read().compactors.len(), 0);
        drop(compactor);
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Disconnected
        ));
    }

    #[tokio::test]
    async fn test_next_compactor() {
        let config = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(1)
            .max_bytes_for_level_base(1)
            .build();
        let (_, hummock_manager, _, worker_node) = setup_compute_env_with_config(80, config).await;
        let context_id = worker_node.id;
        let compactor_manager = CompactorManager::new(1);
        add_compact_task(hummock_manager.as_ref(), context_id, 1).await;

        // No compactor available.
        assert!(compactor_manager.next_compactor().is_none());

        // Add a compactor.
        let mut receiver = compactor_manager.add_compactor(context_id, u64::MAX);
        assert_eq!(compactor_manager.inner.read().compactors.len(), 1);
        let compactor = compactor_manager.next_compactor().unwrap();
        // No compact task.
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));

        let task = hummock_manager
            .get_compact_task(StaticCompactionGroupId::StateDefault.into())
            .await
            .unwrap()
            .unwrap();
        compactor
            .send_task(Task::CompactTask(task.clone()))
            .await
            .unwrap();

        // Get a compact task.
        let received_task = receiver.try_recv().unwrap().unwrap().task.unwrap();
        let received_compact_task = try_match_expand!(received_task, Task::CompactTask).unwrap();
        assert_eq!(received_compact_task, task);

        compactor_manager.remove_compactor(compactor.context_id());
        assert_eq!(compactor_manager.inner.read().compactors.len(), 0);
        assert!(compactor_manager.next_compactor().is_none());
    }

    #[tokio::test]
    async fn test_next_compactor_round_robin() {
        let compactor_manager = CompactorManager::new(1);
        let mut receivers = vec![];
        for context_id in 0..5 {
            receivers.push(compactor_manager.add_compactor(context_id, u64::MAX));
        }
        assert_eq!(compactor_manager.inner.read().compactors.len(), 5);
        for i in 0..receivers.len() * 3 {
            let compactor = compactor_manager.next_compactor().unwrap();
            assert_eq!(compactor.context_id as usize, i % receivers.len());
        }
    }
}
