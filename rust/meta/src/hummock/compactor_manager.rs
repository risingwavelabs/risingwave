use std::ops::Add;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use risingwave_common::error::Result;
use risingwave_pb::hummock::{CompactTask, CompactTasksResponse};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;

const STREAM_BUFFER_SIZE: usize = 4;
const COMPACT_TASK_TIMEOUT: Duration = Duration::from_secs(60);

/// `CompactorManager` maintains compactors which can process compact task.
pub struct CompactorManager<E> {
    /// Stores senders of stream to available compactors
    compactors: RwLock<Vec<Sender<std::result::Result<CompactTasksResponse, E>>>>,

    // Although we can find all ongoing compact tasks in CompactStatus, this field can help in
    // tracking compact tasks. We don't persist this field because all assigned tasks would be
    // cancelled at HummockManager initialization. TODO #546: start a worker to handle expired
    // tasks.
    /// Stores ongoing compact tasks with instant to expire.
    assigned_tasks: parking_lot::RwLock<Vec<(CompactTask, Instant)>>,

    /// We use round-robin approach to assign tasks to compactors.
    /// This field indexes the compactor which the next task should be assigned to.
    next_compactor: AtomicUsize,
}

impl<E> CompactorManager<E> {
    pub fn new() -> Self {
        Self {
            compactors: Default::default(),
            assigned_tasks: Default::default(),
            next_compactor: Default::default(),
        }
    }

    /// Try to assign a compact task.
    /// It will return Ok(false) when no compactor is available.
    pub async fn try_assign_compact_task(&self, compact_task: CompactTask) -> Result<bool> {
        // Pick a compactor
        loop {
            let mut guard = self.compactors.write().await;
            // No available compactor
            if guard.is_empty() {
                return Ok(false);
            }
            let compactor_index = self.next_compactor.load(Ordering::Relaxed) % guard.len();
            let compactor = &guard[compactor_index];
            if compactor
                .send(Ok(CompactTasksResponse {
                    compact_task: Some(compact_task.clone()),
                }))
                .await
                .is_err()
            {
                guard.remove(compactor_index);
                continue;
            }
            break;
        }
        self.next_compactor.fetch_add(1, Ordering::Relaxed);
        // TODO #546: Cancel a task only requires task_id. compact_task.clone() can be avoided.
        self.assigned_tasks
            .write()
            .push((compact_task, Instant::now().add(COMPACT_TASK_TIMEOUT)));

        Ok(true)
    }

    /// A new compactor is registered.
    pub async fn add_compactor(&self) -> Receiver<std::result::Result<CompactTasksResponse, E>> {
        let mut compactor_gurad = self.compactors.write().await;
        let (tx, rx) = tokio::sync::mpsc::channel(STREAM_BUFFER_SIZE);
        compactor_gurad.push(tx);
        rx
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::error::Result;
    use risingwave_pb::hummock::{CompactTask, CompactTasksResponse};
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
    async fn test_add_compactor() -> Result<()> {
        let compactor_manager = CompactorManager::<()>::new();
        // No compactors by default.
        assert_eq!(compactor_manager.compactors.read().await.len(), 0);

        let mut receiver = compactor_manager.add_compactor().await;
        // A compactor is added.
        assert_eq!(compactor_manager.compactors.read().await.len(), 1);
        // No compact task there.
        assert!(matches!(
            receiver.try_recv().unwrap_err(),
            TryRecvError::Empty
        ));

        let task = dummy_compact_task(123);
        compactor_manager
            .compactors
            .write()
            .await
            .first()
            .unwrap()
            .send(Ok(CompactTasksResponse {
                compact_task: Some(task.clone()),
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
        let compactor_manager = CompactorManager::<()>::new();
        let task = dummy_compact_task(123);
        // No compactor available.
        assert!(!compactor_manager
            .try_assign_compact_task(task.clone())
            .await
            .unwrap());

        let mut receiver = compactor_manager.add_compactor().await;
        assert_eq!(compactor_manager.compactors.read().await.len(), 1);
        assert!(compactor_manager
            .try_assign_compact_task(task.clone())
            .await
            .unwrap());
        assert_eq!(
            receiver.try_recv().unwrap().unwrap().compact_task.unwrap(),
            task
        );

        drop(receiver);
        // CompactorManager will find the receiver is dropped when trying to assign a task to it.
        assert_eq!(compactor_manager.compactors.read().await.len(), 1);
        assert!(!compactor_manager
            .try_assign_compact_task(task.clone())
            .await
            .unwrap());
        assert_eq!(compactor_manager.compactors.read().await.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_try_assign_compact_task_round_robin() -> Result<()> {
        let compactor_manager = CompactorManager::<()>::new();
        let mut receivers = vec![];
        for _ in 0..5 {
            receivers.push(compactor_manager.add_compactor().await);
        }
        assert_eq!(compactor_manager.compactors.read().await.len(), 5);
        for i in 0..receivers.len() * 3 {
            let task = dummy_compact_task(2 * i as u64);
            assert!(compactor_manager
                .try_assign_compact_task(task.clone())
                .await
                .unwrap());
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
