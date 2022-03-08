use std::ops::Add;
use std::time::{Duration, Instant};

use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_pb::hummock::{CompactTask, SubscribeCompactTasksResponse, VacuumTask};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tracing::warn;

const STREAM_BUFFER_SIZE: usize = 4;
const COMPACT_TASK_TIMEOUT: Duration = Duration::from_secs(60);

struct CompactorManagerInner {
    /// Senders of stream to available compactors
    compactors: Vec<Sender<Result<SubscribeCompactTasksResponse>>>,

    // Although we can find all ongoing compact tasks in CompactStatus, this field can help in
    // tracking compact tasks. We don't persist this field because all assigned tasks would be
    // cancelled at HummockManager initialization.
    /// Ongoing compact tasks with instant to expire.
    assigned_tasks: Vec<(u64, Instant)>,

    /// We use round-robin approach to assign tasks to compactors.
    /// This field indexes the compactor which the next task should be assigned to.
    next_compactor: usize,
}

impl CompactorManagerInner {
    pub fn new() -> Self {
        Self {
            compactors: vec![],
            assigned_tasks: vec![],
            next_compactor: 0,
        }
    }
}

/// `CompactorManager` maintains compactors which can process compact task.
pub struct CompactorManager {
    inner: RwLock<CompactorManagerInner>,
    compact_task_timeout: Duration,
}

impl Default for CompactorManager {
    fn default() -> Self {
        Self::new(COMPACT_TASK_TIMEOUT)
    }
}

impl CompactorManager {
    pub fn new(compact_task_timeout: Duration) -> Self {
        Self {
            inner: RwLock::new(CompactorManagerInner::new()),
            compact_task_timeout,
        }
    }

    /// Try to assign a compact task.
    /// It will return false when no compactor is available.
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
        if let Some(compact_task) = compact_task {
            // Track the compact task.
            guard.assigned_tasks.push((
                compact_task.task_id,
                Instant::now().add(self.compact_task_timeout),
            ));
        }

        true
    }

    /// A new compactor is registered.
    pub async fn add_compactor(&self) -> Receiver<Result<SubscribeCompactTasksResponse>> {
        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_BUFFER_SIZE);
        self.inner.write().await.compactors.push(tx);
        rx
    }

    /// Don't track the compact task any more.
    pub async fn unassign_compact_task(&self, compact_task_id: u64) {
        let mut guard = self.inner.write().await;
        guard.assigned_tasks.retain(|e| e.0 != compact_task_id);
    }

    /// Get timeout compact tasks.
    pub async fn get_timeout_compact_task(&self) -> Vec<u64> {
        let now = Instant::now();
        self.inner
            .read()
            .await
            .assigned_tasks
            .iter()
            .filter(|e| now.saturating_duration_since(e.1) > self.compact_task_timeout)
            .map(|e| e.0)
            .collect_vec()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use risingwave_pb::hummock::{CompactTask, SubscribeCompactTasksResponse};
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
        }
    }

    #[tokio::test]
    async fn test_add_compactor() {
        let compactor_manager = CompactorManager::default();
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
    }

    #[tokio::test]
    async fn test_try_assign_compact_task() {
        let compactor_manager = CompactorManager::default();
        let task = dummy_compact_task(123);
        // No compactor available.
        assert!(
            !compactor_manager
                .try_assign_compact_task(Some(task.clone()), None)
                .await
        );

        let mut receiver = compactor_manager.add_compactor().await;
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 1);
        assert!(compactor_manager
            .inner
            .read()
            .await
            .assigned_tasks
            .is_empty());
        assert!(
            compactor_manager
                .try_assign_compact_task(Some(task.clone()), None)
                .await
        );
        assert_eq!(
            receiver.try_recv().unwrap().unwrap().compact_task.unwrap(),
            task
        );
        assert_eq!(compactor_manager.inner.read().await.assigned_tasks.len(), 1);

        drop(receiver);
        // CompactorManager will find the receiver is dropped when trying to assign a task to it.
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 1);
        assert!(
            !compactor_manager
                .try_assign_compact_task(Some(task.clone()), None)
                .await
        );
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 0);
    }

    #[tokio::test]
    async fn test_try_assign_compact_task_round_robin() {
        let compactor_manager = CompactorManager::default();
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
    }

    #[tokio::test]
    async fn test_cancel_compact_task() {
        let compact_task_timeout = Duration::from_millis(100);
        let compactor_manager = CompactorManager::new(compact_task_timeout);
        let task = dummy_compact_task(123);
        let _receiver = compactor_manager.add_compactor().await;
        assert!(
            compactor_manager
                .try_assign_compact_task(Some(task.clone()), None)
                .await
        );
        assert_eq!(compactor_manager.inner.read().await.compactors.len(), 1);
        assert_eq!(compactor_manager.inner.read().await.assigned_tasks.len(), 1);

        // Cancel compact task via report explicitly
        compactor_manager.unassign_compact_task(task.task_id).await;
        assert!(compactor_manager
            .inner
            .read()
            .await
            .assigned_tasks
            .is_empty());

        // Cancel compact task via timeout checker
        assert!(
            compactor_manager
                .try_assign_compact_task(Some(task.clone()), None)
                .await
        );
        assert_eq!(compactor_manager.inner.read().await.assigned_tasks.len(), 1);

        // Within TTL.
        tokio::time::sleep(compactor_manager.compact_task_timeout / 2).await;
        assert_eq!(compactor_manager.get_timeout_compact_task().await.len(), 0);

        // TTL exceeded
        tokio::time::sleep(compactor_manager.compact_task_timeout * 2).await;
        assert_eq!(compactor_manager.get_timeout_compact_task().await.len(), 1);
    }
}
