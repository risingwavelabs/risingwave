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

use risingwave_common::error::{ErrorCode, Result, ToErrorStr};
use risingwave_pb::hummock::{CompactTask, SubscribeCompactTasksResponse, VacuumTask};
use risingwave_storage::hummock::HummockContextId;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tracing::warn;

use crate::hummock::HummockManager;
use crate::storage::MetaStore;

const STREAM_BUFFER_SIZE: usize = 4;

struct Compactor {
    context_id: HummockContextId,
    sender: Sender<Result<SubscribeCompactTasksResponse>>,
}

impl Compactor {
    async fn send_task(
        &self,
        compact_task: Option<CompactTask>,
        vacuum_task: Option<VacuumTask>,
    ) -> Result<()> {
        self.sender
            .send(Ok(SubscribeCompactTasksResponse {
                compact_task,
                vacuum_task,
            }))
            .await
            .map_err(|e| ErrorCode::InternalError(e.to_error_str()).into())
    }
}

struct CompactorManagerInner {
    /// Senders of stream to available compactors
    compactors: Vec<Compactor>,

    /// We use round-robin approach to assign tasks to compactors.
    /// This field indexes the compactor which the next task should be assigned to.
    next_compactor: usize,
    // TODO: #1263 subscribe to cluster membership change.
}

impl CompactorManagerInner {
    pub fn new() -> Self {
        Self {
            compactors: vec![],
            next_compactor: 0,
        }
    }
}

/// `CompactorManager` maintains compactors which can process compact task.
/// A compact task is tracked in `HummockManager::Compaction` via both `CompactStatus` and
/// `CompactTaskAssignment`. A compact task's lifecycle contains 4 states:
/// - 1. Unassigned: a compact task is initiated via `CompactStatus::get_compact_task`, which is not
///   assigned to any compactor yet.
/// - 2. Assigned: a compact task is assigned to a compactor via
///   `HummockManager::assign_compact_task`. An assigned compact task can become unassigned again,
///   for example when the assignee compactor is invalidated.
/// - 3. Finished: a assigned task is reported as finished via `CompactStatus::report_compact_task`.
///   It's the final state.
/// - 4. Cancelled: a assigned task is reported as cancelled via
///   `CompactStatus::report_compact_task`. It's the final state.
pub struct CompactorManager {
    inner: RwLock<CompactorManagerInner>,
}

impl Default for CompactorManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CompactorManager {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(CompactorManagerInner::new()),
        }
    }

    /// Try to assign a compact task.
    /// Return assignee's `HummockContextId` if any.
    /// Return None if no compactor is available.
    pub async fn try_assign_compact_task<S>(
        &self,
        compact_task: Option<CompactTask>,
        vacuum_task: Option<VacuumTask>,
        hummock_manager_ref: &HummockManager<S>,
    ) -> Result<Option<HummockContextId>>
    where
        S: MetaStore,
    {
        assert!(
            compact_task.is_some() || vacuum_task.is_some(),
            "Either compact_task or vacuum_task should be provided."
        );
        let mut guard = self.inner.write().await;
        // Pick a compactor
        loop {
            // No available compactor
            if guard.compactors.is_empty() {
                tracing::warn!("No compactor is available.");
                return Ok(None);
            }
            let compactor_index = guard.next_compactor % guard.compactors.len();
            let compactor = &guard.compactors[compactor_index];
            match compactor
                .send_task(compact_task.clone(), vacuum_task.clone())
                .await
            {
                Ok(_) => {
                    // Vacuum task assignment is tracked in `VacuumTrigger`.
                    // Track compact task in `HummockManager`.
                    if let Some(compact_task) = compact_task.as_ref() {
                        hummock_manager_ref
                            .assign_compact_task(compactor.context_id, compact_task.task_id)
                            .await?;
                    }
                    let assignee = Some(compactor.context_id);
                    guard.next_compactor += 1;
                    return Ok(assignee);
                }
                Err(err) => {
                    warn!("Failed to send compaction task. {}", err);
                    guard.compactors.remove(compactor_index);
                    continue;
                }
            }
        }
    }

    /// A new compactor is registered.
    pub async fn add_compactor(
        &self,
        context_id: HummockContextId,
    ) -> Receiver<Result<SubscribeCompactTasksResponse>> {
        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_BUFFER_SIZE);
        self.inner.write().await.compactors.push(Compactor {
            context_id,
            sender: tx,
        });
        rx
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::error::Result;
    use risingwave_pb::hummock::{CompactMetrics, CompactTask, TableSetStatistics};
    use tokio::sync::mpsc::error::TryRecvError;

    use crate::hummock::test_utils::{generate_test_tables, setup_compute_env};
    use crate::hummock::{CompactorManager, HummockManager};
    use crate::storage::MetaStore;

    async fn add_compact_task<S>(
        hummock_manager_ref: &HummockManager<S>,
        context_id: u32,
        epoch: u64,
    ) where
        S: MetaStore,
    {
        let (original_tables, _) = generate_test_tables(
            epoch,
            vec![hummock_manager_ref.get_new_table_id().await.unwrap()],
        );
        hummock_manager_ref
            .add_tables(context_id, original_tables.clone(), epoch)
            .await
            .unwrap();
        hummock_manager_ref.commit_epoch(epoch).await.unwrap();
    }

    fn dummy_compact_task(task_id: u64) -> CompactTask {
        CompactTask {
            input_ssts: vec![],
            splits: vec![],
            watermark: 0,
            sorted_output_ssts: vec![],
            task_id,
            target_level: 0,
            is_target_ultimate_and_leveling: false,
            metrics: Some(CompactMetrics {
                read_level_n: Some(TableSetStatistics::default()),
                read_level_nplus1: Some(TableSetStatistics::default()),
                write: Some(TableSetStatistics::default()),
            }),
        }
    }

    #[tokio::test]
    async fn test_add_compactor() -> Result<()> {
        let compactor_manager = CompactorManager::new();
        // No compactors by default.
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 0);

        let mut receiver = compactor_manager.add_compactor(0).await;
        // A compactor is added.
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 1);
        // No compact task there.
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));

        let task = dummy_compact_task(123);
        compactor_manager
            .inner
            .write()
            .await
            .compactors
            .first()
            .unwrap()
            .send_task(Some(task.clone()), None)
            .await
            .unwrap();
        // Receive a compact task.
        assert_eq!(
            receiver.try_recv().unwrap().unwrap().compact_task.unwrap(),
            task
        );

        drop(compactor_manager);
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Disconnected
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_try_assign_compact_task() -> Result<()> {
        let (_, hummock_manager, _, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let compactor_manager = CompactorManager::new();
        add_compact_task(hummock_manager.as_ref(), context_id, 1).await;
        let task = hummock_manager.get_compact_task().await.unwrap().unwrap();
        // No compactor available.
        assert!(compactor_manager
            .try_assign_compact_task(Some(task.clone()), None, hummock_manager.as_ref())
            .await
            .unwrap()
            .is_none());

        let mut receiver = compactor_manager.add_compactor(0).await;
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 1);
        assert!(compactor_manager
            .try_assign_compact_task(Some(task.clone()), None, hummock_manager.as_ref())
            .await
            .unwrap()
            .is_some());
        assert_eq!(
            receiver.try_recv().unwrap().unwrap().compact_task.unwrap(),
            task
        );

        drop(receiver);
        // CompactorManager will find the receiver is dropped when trying to assign a task to it.
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 1);
        assert!(compactor_manager
            .try_assign_compact_task(Some(task.clone()), None, hummock_manager.as_ref())
            .await
            .unwrap()
            .is_none());
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_try_assign_compact_task_round_robin() -> Result<()> {
        let (_, hummock_manager, _, worker_node) = setup_compute_env(80).await;
        let context_id = worker_node.id;
        let mut epoch = 1;
        let compactor_manager = CompactorManager::new();
        let mut receivers = vec![];
        for _ in 0..5 {
            receivers.push(compactor_manager.add_compactor(0).await);
        }
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 5);
        for i in 0..receivers.len() * 3 {
            add_compact_task(hummock_manager.as_ref(), context_id, epoch).await;
            epoch += 1;
            let task = hummock_manager.get_compact_task().await.unwrap().unwrap();
            assert!(compactor_manager
                .try_assign_compact_task(Some(task.clone()), None, hummock_manager.as_ref())
                .await
                .unwrap()
                .is_some());
            for j in 0..receivers.len() {
                if j == i % receivers.len() {
                    assert_eq!(
                        receivers[j]
                            .try_recv()
                            .unwrap()
                            .unwrap()
                            .compact_task
                            .unwrap(),
                        task
                    );
                } else {
                    assert!(matches!(
                        receivers[j].try_recv().unwrap_err(),
                        TryRecvError::Empty
                    ));
                }
            }
        }
        Ok(())
    }
}
