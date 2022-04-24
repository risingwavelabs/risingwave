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
use std::time::Duration;

use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{oneshot, Mutex};
use tokio::time;
use tonic::Status;

use crate::cluster::WorkerKey;

pub type Notification = std::result::Result<SubscribeResponse, Status>;

type NotificationVersion = u64;

#[derive(Clone)]
pub enum LocalNotification {
    WorkerDeletion(WorkerNode),
}

/// Interval before retry when notify fail.
const NOTIFY_RETRY_INTERVAL: Duration = Duration::from_micros(10);

#[derive(Debug)]
enum Target {
    Frontend,
    Compute,
}

#[derive(Debug)]
struct Task {
    target: Target,
    callback_tx: Option<oneshot::Sender<NotificationVersion>>,
    operation: Operation,
    info: Info,
}

/// [`NotificationManager`] is used to send notification to frontends and compute nodes.
pub struct NotificationManager {
    core: Arc<Mutex<NotificationManagerCore>>,
    /// Sender used to add a notification into the waiting queue.
    task_tx: UnboundedSender<Task>,
    /// Sender used in `Self::delete_sender` method.
    /// Tell `NotificationManagerCore` to skip some retry and delete senders.
    stop_retry_tx: UnboundedSender<WorkerKey>,
}

pub type NotificationManagerRef = Arc<NotificationManager>;

impl NotificationManager {
    pub fn new() -> Self {
        // notification waiting queue.
        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task>();
        let (stop_retry_tx, stop_retry_rx) = mpsc::unbounded_channel();
        let core = Arc::new(Mutex::new(NotificationManagerCore::new(stop_retry_rx)));
        let core_clone = core.clone();

        tokio::spawn(async move {
            while let Some(task) = task_rx.recv().await {
                let version = match task.target {
                    Target::Frontend => {
                        core.lock()
                            .await
                            .notify_frontend(task.operation, &task.info)
                            .await
                    }
                    Target::Compute => {
                        core.lock()
                            .await
                            .notify_compute(task.operation, &task.info)
                            .await
                    }
                };
                if let Some(tx) = task.callback_tx {
                    tx.send(version).unwrap();
                }
            }
        });

        Self {
            core: core_clone,
            task_tx,
            stop_retry_tx,
        }
    }

    /// Add a notifcation to the waiting queue and return immediately
    fn notify_asynchronously(&self, target: Target, operation: Operation, info: Info) {
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
        target: Target,
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
        self.notify_asynchronously(Target::Frontend, operation, info);
    }

    pub async fn notify_frontend(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify(Target::Frontend, operation, info).await
    }

    pub fn notify_compute_asynchronously(&self, operation: Operation, info: Info) {
        self.notify_asynchronously(Target::Compute, operation, info);
    }

    pub async fn notify_compute(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify(Target::Compute, operation, info).await
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

    /// Tell `NotificationManagerCore` to skip some retry and delete senders.
    pub fn delete_sender(&self, worker_key: WorkerKey) {
        self.stop_retry_tx.send(worker_key).unwrap();
    }

    pub async fn insert_frontend_sender(
        &self,
        worker_key: WorkerKey,
        sender: UnboundedSender<Notification>,
    ) {
        let mut core_guard = self.core.lock().await;
        core_guard.frontend_senders.insert(worker_key, sender);
    }

    pub async fn insert_compute_sender(
        &self,
        worker_key: WorkerKey,
        sender: UnboundedSender<Notification>,
    ) {
        let mut core_guard = self.core.lock().await;
        core_guard.compute_senders.insert(worker_key, sender);
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
    /// The notification sender to local subscribers.
    local_senders: Vec<UnboundedSender<LocalNotification>>,
    /// Receiver used in heartbeat check. Receive the worker keys of disconnected workers from
    /// `StoredClusterManager::start_heartbeat_checker`.
    stop_retry_rx: UnboundedReceiver<WorkerKey>,

    /// The current notification version.
    current_version: NotificationVersion,
}

impl NotificationManagerCore {
    fn new(stop_retry_rx: UnboundedReceiver<WorkerKey>) -> Self {
        Self {
            frontend_senders: HashMap::new(),
            compute_senders: HashMap::new(),
            local_senders: vec![],
            stop_retry_rx,
            current_version: 0,
        }
    }

    async fn notify_frontend(&mut self, operation: Operation, info: &Info) -> NotificationVersion {
        let mut keys = HashSet::new();
        self.current_version += 1;
        for (worker_key, sender) in &self.frontend_senders {
            loop {
                // Heartbeat may delete worker.
                // We assume that after a worker is disconnected, before it recalls subscribe, we
                // will call notify.
                while let Ok(x) = self.stop_retry_rx.try_recv() {
                    keys.insert(x);
                }
                if keys.contains(worker_key) {
                    break;
                }
                let result = sender.send(Ok(SubscribeResponse {
                    status: None,
                    operation: operation as i32,
                    info: Some(info.clone()),
                    version: self.current_version,
                }));
                if result.is_ok() {
                    break;
                }
                time::sleep(NOTIFY_RETRY_INTERVAL).await;
            }
        }
        self.remove_by_key(keys);

        self.current_version
    }

    /// Send a `SubscribeResponse` to backend.
    async fn notify_compute(&mut self, operation: Operation, info: &Info) -> NotificationVersion {
        let mut keys = HashSet::new();
        self.current_version += 1;
        for (worker_key, sender) in &self.compute_senders {
            loop {
                // Heartbeat may delete worker.
                // We assume that after a worker is disconnected, before it recalls subscribe, we
                // will call notify.
                while let Ok(x) = self.stop_retry_rx.try_recv() {
                    keys.insert(x);
                }
                if keys.contains(worker_key) {
                    break;
                }
                let result = sender.send(Ok(SubscribeResponse {
                    status: None,
                    operation: operation as i32,
                    info: Some(info.clone()),
                    version: self.current_version,
                }));
                if result.is_ok() {
                    break;
                }
                time::sleep(NOTIFY_RETRY_INTERVAL).await;
            }
        }
        self.remove_by_key(keys);

        self.current_version
    }

    fn remove_by_key(&mut self, keys: HashSet<WorkerKey>) {
        keys.into_iter().for_each(|key| {
            self.frontend_senders
                .remove(&key)
                .or_else(|| self.compute_senders.remove(&key));
        });
    }
}
