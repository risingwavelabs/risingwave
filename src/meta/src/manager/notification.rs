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

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::{oneshot, Mutex};
use tonic::Status;

use crate::manager::cluster::WorkerKey;

pub type MessageStatus = Status;
pub type Notification = Result<SubscribeResponse, Status>;

pub type NotificationVersion = u64;

use risingwave_pb::common::WorkerType;

#[derive(Clone)]
pub enum LocalNotification {
    WorkerDeletion(WorkerNode),
}

#[derive(Debug)]
struct Task {
    target: WorkerType,
    callback_tx: Option<oneshot::Sender<NotificationVersion>>,
    operation: Operation,
    info: Info,
}

/// [`NotificationManager`] is used to send notification to frontends and compute nodes.
pub struct NotificationManager {
    core: Arc<Mutex<NotificationManagerCore>>,
    /// Sender used to add a notification into the waiting queue.
    task_tx: UnboundedSender<Task>,
}

pub type NotificationManagerRef = Arc<NotificationManager>;

impl NotificationManager {
    pub fn new() -> Self {
        // notification waiting queue.
        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task>();
        let core = Arc::new(Mutex::new(NotificationManagerCore::new()));
        let core_clone = core.clone();

        tokio::spawn(async move {
            while let Some(task) = task_rx.recv().await {
                let mut guard = core.lock().await;
                let version = match task.target {
                    WorkerType::Generic => guard.notify_all(task.operation, &task.info),

                    _ => guard.notify(task.target, task.operation, &task.info),
                };
                if let Some(tx) = task.callback_tx {
                    tx.send(version).unwrap();
                }
            }
        });

        Self {
            core: core_clone,
            task_tx,
        }
    }

    /// Add a notification to the waiting queue and return immediately
    fn notify_asynchronously(&self, target: WorkerType, operation: Operation, info: Info) {
        let task = Task {
            target,
            callback_tx: None,
            operation,
            info,
        };
        self.task_tx.send(task).unwrap();
    }

    /// Add a notification to the waiting queue, and will not return until the notification is
    /// sent successfully
    async fn notify(
        &self,
        target: WorkerType,
        operation: Operation,
        info: Info,
    ) -> NotificationVersion {
        let (callback_tx, callback_rx) = oneshot::channel();
        let task = Task {
            target,
            callback_tx: Some(callback_tx),
            operation,
            info,
        };
        self.task_tx.send(task).unwrap();
        callback_rx.await.unwrap()
    }

    pub fn notify_frontend_asynchronously(&self, operation: Operation, info: Info) {
        self.notify_asynchronously(WorkerType::Frontend, operation, info);
    }

    pub async fn notify_frontend(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify(WorkerType::Frontend, operation, info).await
    }

    pub async fn notify_compute(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify(WorkerType::ComputeNode, operation, info).await
    }

    pub async fn notify_compactor(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify(WorkerType::Compactor, operation, info).await
    }

    pub fn notify_compute_asynchronously(&self, operation: Operation, info: Info) {
        self.notify_asynchronously(WorkerType::ComputeNode, operation, info);
    }

    /// To notify all the `worker_type` sender
    pub async fn notify_all_node(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify(WorkerType::Generic, operation, info).await
    }

    pub async fn notify_local_subscribers(&self, notification: LocalNotification) {
        let mut core_guard = self.core.lock().await;
        core_guard.local_senders.retain(|sender| {
            if let Err(err) = sender.send(notification.clone()) {
                tracing::warn!("Failed to notify local subscriber. {}", err);
                return false;
            }
            true
        });
    }

    /// Tell `NotificationManagerCore` to delete sender.
    pub async fn delete_sender(&self, worker_key: WorkerKey) {
        let mut core_guard = self.core.lock().await;
        core_guard.compute_senders.remove(&worker_key);
        core_guard.frontend_senders.remove(&worker_key);
    }

    /// Tell `NotificationManagerCore` to insert sender by `worker_type`.
    pub async fn insert_sender(
        &self,
        worker_type: WorkerType,
        worker_key: WorkerKey,
        sender: UnboundedSender<Notification>,
    ) {
        let mut core_guard = self.core.lock().await;
        let senders = match worker_type {
            WorkerType::Frontend => &mut core_guard.frontend_senders,
            WorkerType::ComputeNode => &mut core_guard.compute_senders,
            WorkerType::Compactor => &mut core_guard.compactor_senders,

            _ => unreachable!(),
        };

        senders.insert(worker_key, sender);
    }

    pub async fn insert_local_sender(&self, sender: UnboundedSender<LocalNotification>) {
        let mut core_guard = self.core.lock().await;
        core_guard.local_senders.push(sender);
    }

    pub async fn current_version(&self) -> NotificationVersion {
        let core_guard = self.core.lock().await;
        core_guard.current_version
    }
}

impl Default for NotificationManager {
    fn default() -> Self {
        Self::new()
    }
}

struct NotificationManagerCore {
    /// The notification sender to frontends.
    frontend_senders: HashMap<WorkerKey, UnboundedSender<Notification>>,
    /// The notification sender to compute nodes.
    compute_senders: HashMap<WorkerKey, UnboundedSender<Notification>>,
    /// The notification sender to compactor nodes.
    compactor_senders: HashMap<WorkerKey, UnboundedSender<Notification>>,

    /// The notification sender to local subscribers.
    local_senders: Vec<UnboundedSender<LocalNotification>>,

    /// The current notification version.
    current_version: NotificationVersion,
}

impl NotificationManagerCore {
    fn new() -> Self {
        Self {
            frontend_senders: HashMap::new(),
            compute_senders: HashMap::new(),
            compactor_senders: HashMap::new(),
            local_senders: vec![],
            current_version: 0,
        }
    }

    fn notify(
        &mut self,
        worker_type: WorkerType,
        operation: Operation,
        info: &Info,
    ) -> NotificationVersion {
        self.current_version += 1;

        let senders = match worker_type {
            WorkerType::Frontend => &mut self.frontend_senders,
            WorkerType::ComputeNode => &mut self.compute_senders,
            WorkerType::Compactor => &mut self.compactor_senders,

            _ => unreachable!(),
        };

        let mut bad_keys = vec![];
        for (worker_key, sender) in senders.iter() {
            if let Err(err) = sender.send(Ok(SubscribeResponse {
                status: None,
                operation: operation as i32,
                info: Some(info.clone()),
                version: self.current_version,
            })) {
                tracing::warn!(
                    "Failed to notify {:?} {:?}: {}",
                    worker_type,
                    worker_key,
                    err
                );
                bad_keys.push(worker_key.clone());
            }
        }
        for worker_key in bad_keys {
            senders.remove(&worker_key);
        }

        self.current_version
    }

    fn notify_all(&mut self, operation: Operation, info: &Info) -> NotificationVersion {
        self.current_version += 1;

        let mut bad_keys = vec![];
        for (worker_type, worker_key, sender) in self
            .frontend_senders
            .iter()
            .map(|(worker_key, sender)| (WorkerType::Frontend, worker_key, sender))
            .chain(
                self.compute_senders
                    .iter()
                    .map(|(worker_key, sender)| (WorkerType::ComputeNode, worker_key, sender)),
            )
            .chain(
                self.compactor_senders
                    .iter()
                    .map(|(worker_key, sender)| (WorkerType::Compactor, worker_key, sender)),
            )
        {
            if let Err(err) = sender.send(Ok(SubscribeResponse {
                status: None,
                operation: operation as i32,
                info: Some(info.clone()),
                version: self.current_version,
            })) {
                tracing::warn!("Failed to notify_all {:?}: {}", worker_key, err);
                bad_keys.push((worker_type, worker_key.clone()));
            }
        }
        for (worker_type, worker_key) in bad_keys {
            let senders = match worker_type {
                WorkerType::Frontend => &mut self.frontend_senders,
                WorkerType::ComputeNode => &mut self.compute_senders,
                WorkerType::Compactor => &mut self.compactor_senders,

                _ => unreachable!(),
            };
            senders.remove(&worker_key);
        }

        self.current_version
    }
}
