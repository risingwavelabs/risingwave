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

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use log::error;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_pb::meta::scale_task::TaskType;
use risingwave_pb::meta::{ScaleTask, TaskStatus};
use tokio::sync::{Mutex, Semaphore};

use crate::model::{MetadataModel, ScaleTaskId};
use crate::storage::MetaStore;

pub type ScaleManagerRef<S> = Arc<ScaleManager<S>>;

/// [`ScaleManager`] is used to receive scale tasks.
#[allow(dead_code)]
pub struct ScaleManager<S: MetaStore> {
    meta_store: Arc<S>,

    next_task_id: AtomicU32,
    task_queue: Arc<Mutex<VecDeque<ScaleTask>>>,
    task_semaphore: Arc<Semaphore>,
}

impl<S> ScaleManager<S>
where
    S: MetaStore,
{
    pub fn new(meta_store: Arc<S>) -> Self {
        // TODO(zehua): read task from meta_store.
        let task_queue = Arc::new(Mutex::new(VecDeque::new()));
        let task_semaphore = Arc::new(Semaphore::new(0));
        let scale_manager = ScaleManager {
            meta_store: meta_store.clone(),
            next_task_id: AtomicU32::new(1),
            task_queue: task_queue.clone(),
            task_semaphore: task_semaphore.clone(),
        };

        tokio::spawn(async move {
            while let Ok(permit) = task_semaphore.acquire().await {
                if let Err(err) = Self::solve_task(meta_store.clone(), task_queue.clone()).await {
                    error!("Failed to solve scale task: {}", err);
                }
                permit.forget();
            }
        });

        scale_manager
    }

    async fn solve_task(
        meta_store: Arc<S>,
        task_queue: Arc<Mutex<VecDeque<ScaleTask>>>,
    ) -> Result<()> {
        let mut task_queue_guard = task_queue.lock().await;
        // Be called after `task_semaphore.acquire()`, so we can use `unwrap()`.
        let mut task = task_queue_guard.front_mut().unwrap();
        task.task_status = TaskStatus::Building as i32;
        task.insert(&*meta_store).await?;

        match task.get_task_type()? {
            TaskType::Invalid => unreachable!(),
            TaskType::ScaleIn => {
                let _hosts = task.get_hosts();
                // TODO: call some method to scale in

                task.task_status = TaskStatus::Finished as i32;
                task.insert(&*meta_store).await?;
                task_queue_guard.pop_front();
            }
            TaskType::ScaleOut => {
                let _hosts = task.get_hosts();
                let _fragment_parallelism = task.get_fragment_parallelism();
                // TODO: call some method to scale out

                let task_id = task.task_id;
                drop(task_queue_guard);

                // TODO: wait before CN starts.
                // Temporarily replace them with `sleep` in unit tests.
                #[cfg(test)]
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                task_queue_guard = task_queue.lock().await;
                task = task_queue_guard.front_mut().unwrap();
                if task_id == task.task_id {
                    task.task_status = TaskStatus::Finished as i32;
                    task.insert(&*meta_store).await?;
                    task_queue_guard.pop_front();
                }
            }
        }

        Ok(())
    }

    pub async fn add_scale_task(&self, mut task: ScaleTask) -> Result<ScaleTaskId> {
        let task_id = self.next_task_id.fetch_add(1, Ordering::Relaxed);
        task.task_id = task_id;
        task.task_status = TaskStatus::Pending as i32;
        task.insert(&*self.meta_store).await?;
        self.task_queue.lock().await.push_back(task);
        self.task_semaphore.add_permits(1);
        Ok(task_id)
    }

    pub async fn get_task_status(&self, task_id: ScaleTaskId) -> Result<TaskStatus> {
        ScaleTask::select(&*self.meta_store, &task_id)
            .await?
            .map_or(Ok(TaskStatus::NotFound), |task| Ok(task.get_task_status()?))
    }

    pub async fn abort_task(&self, task_id: ScaleTaskId) -> Result<()> {
        let mut task_queue_guard = self.task_queue.lock().await;
        let index = task_queue_guard
            .iter()
            .position(|task| task.task_id == task_id);
        if index.is_some() {
            let mut task = task_queue_guard.remove(index.unwrap()).unwrap();
            let task_status = task.get_task_status()?;
            task.task_status = TaskStatus::Cancelled as i32;
            task.insert(&*self.meta_store).await?;

            if TaskStatus::Building != task_status {
                self.task_semaphore
                    .try_acquire()
                    .map_err(|e| ErrorCode::InternalError(e.to_string()))?
                    .forget();
            }
        }
        Ok(())
    }

    pub async fn remove_task(&self, _task_id: ScaleTaskId) -> Result<()> {
        // TODO
        Ok(())
    }

    #[cfg(test)]
    async fn get_task_queue_length(&self) -> usize {
        self.task_queue.lock().await.len()
    }

    #[cfg(test)]
    fn available_permits(&self) -> usize {
        self.task_semaphore.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_pb::common::HostAddress;

    use super::*;
    use crate::manager::MetaSrvEnv;

    #[tokio::test]
    async fn test_scale_out_task() -> Result<()> {
        let env = MetaSrvEnv::for_test().await;
        let scale_manager = ScaleManager::new(env.meta_store_ref());

        let task1 = ScaleTask {
            task_id: 0,
            task_type: TaskType::ScaleOut as i32,
            hosts: vec![
                HostAddress {
                    host: "test".to_string(),
                    port: 0,
                },
                HostAddress {
                    host: "test".to_string(),
                    port: 0,
                },
            ],
            fragment_parallelism: HashMap::new(),
            task_status: TaskStatus::NotFound as i32,
        };
        let task2 = task1.clone();
        let task3 = task1.clone();

        let task1_id = scale_manager.add_scale_task(task1).await?;
        assert_eq!(1, task1_id);
        let task2_id = scale_manager.add_scale_task(task2).await?;
        assert_eq!(2, task2_id);
        let task3_id = scale_manager.add_scale_task(task3).await?;
        assert_eq!(3, task3_id);

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(2, scale_manager.available_permits());
        assert_eq!(
            TaskStatus::Building,
            scale_manager.get_task_status(task1_id).await?
        );
        assert_eq!(
            TaskStatus::Pending,
            scale_manager.get_task_status(task2_id).await?
        );

        scale_manager.abort_task(task2_id).await?;
        assert_eq!(1, scale_manager.available_permits());
        assert_eq!(
            TaskStatus::Building,
            scale_manager.get_task_status(task1_id).await?
        );
        assert_eq!(
            TaskStatus::Cancelled,
            scale_manager.get_task_status(task2_id).await?
        );

        scale_manager.abort_task(task1_id).await?;
        assert_eq!(1, scale_manager.available_permits());
        assert_eq!(
            TaskStatus::Cancelled,
            scale_manager.get_task_status(task1_id).await?
        );

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        assert_eq!(0, scale_manager.get_task_queue_length().await);
        assert_eq!(0, scale_manager.available_permits());
        assert_eq!(
            TaskStatus::Finished,
            scale_manager.get_task_status(task3_id).await?
        );

        Ok(())
    }
}
