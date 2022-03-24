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

use risingwave_common::error::Result;
use risingwave_pb::hummock::{CompactTask, SubscribeCompactTasksResponse, VacuumTask};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tracing::warn;

const STREAM_BUFFER_SIZE: usize = 4;

struct CompactorManagerInner {
    /// Senders of stream to available compactors
    compactors: Vec<Sender<Result<SubscribeCompactTasksResponse>>>,

    /// We use round-robin approach to assign tasks to compactors.
    /// This field indexes the compactor which the next task should be assigned to.
    next_compactor: usize,
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
    /// Return false if no compactor is available.
    pub async fn try_assign_compact_task(
        &self,
        compact_task: Option<CompactTask>,
        vacuum_task: Option<VacuumTask>,
    ) -> bool {
        let mut guard = self.inner.write().await;
        // Pick a compactor
        loop {
            // No available compactor
            if guard.compactors.is_empty() {
                tracing::warn!("No compactor is available.");
                return false;
            }
            let compactor_index = guard.next_compactor % guard.compactors.len();
            let compactor = &guard.compactors[compactor_index];
            if let Err(err) = compactor
                .send(Ok(SubscribeCompactTasksResponse {
                    compact_task: compact_task.clone(),
                    vacuum_task: vacuum_task.clone(),
                }))
                .await
            {
                warn!("failed to send compaction task. {}", err);
                guard.compactors.remove(compactor_index);
                continue;
            }
            break;
        }
        guard.next_compactor += 1;

        true
    }

    /// A new compactor is registered.
    pub async fn add_compactor(&self) -> Receiver<Result<SubscribeCompactTasksResponse>> {
        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_BUFFER_SIZE);
        self.inner.write().await.compactors.push(tx);
        rx
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::error::Result;
    use risingwave_pb::hummock::{
        CompactMetrics, CompactTask, SubscribeCompactTasksResponse, TableSetStatistics,
    };
    use tokio::sync::mpsc::error::TryRecvError;

    use crate::hummock::CompactorManager;

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

        let mut receiver = compactor_manager.add_compactor().await;
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
            .send(Ok(SubscribeCompactTasksResponse {
                compact_task: Some(task.clone()),
                vacuum_task: None,
            }))
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
        let compactor_manager = CompactorManager::new();
        let task = dummy_compact_task(123);
        // No compactor available.
        assert!(
            !compactor_manager
                .try_assign_compact_task(Some(task.clone()), None)
                .await
        );

        let mut receiver = compactor_manager.add_compactor().await;
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 1);
        assert!(
            compactor_manager
                .try_assign_compact_task(Some(task.clone()), None)
                .await
        );
        assert_eq!(
            receiver.try_recv().unwrap().unwrap().compact_task.unwrap(),
            task
        );

        drop(receiver);
        // CompactorManager will find the receiver is dropped when trying to assign a task to it.
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 1);
        assert!(
            !compactor_manager
                .try_assign_compact_task(Some(task.clone()), None)
                .await
        );
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_try_assign_compact_task_round_robin() -> Result<()> {
        let compactor_manager = CompactorManager::new();
        let mut receivers = vec![];
        for _ in 0..5 {
            receivers.push(compactor_manager.add_compactor().await);
        }
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 5);
        for i in 0..receivers.len() * 3 {
            let task = dummy_compact_task(2 * i as u64);
            assert!(
                compactor_manager
                    .try_assign_compact_task(Some(task.clone()), None)
                    .await
            );
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
