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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::hummock::CompactTask;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{MetaSnapshot, SubscribeResponse, SubscribeType};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::Mutex;
use tonic::Status;

use crate::manager::cluster::WorkerKey;
use crate::model::NotificationVersion as Version;
use crate::storage::MetaStore;

pub type MessageStatus = Status;
pub type Notification = Result<SubscribeResponse, Status>;
pub type NotificationManagerRef<S> = Arc<NotificationManager<S>>;
pub type NotificationVersion = u64;

#[derive(Clone, Debug)]
pub enum LocalNotification {
    WorkerNodeIsDeleted(WorkerNode),
    CompactionTaskNeedCancel(CompactTask),
    SystemParamsChange(SystemParamsReader),
}

#[derive(Debug)]
enum Target {
    SubscribeType(SubscribeType),
    WorkerKey(WorkerKey),
}

impl From<SubscribeType> for Target {
    fn from(value: SubscribeType) -> Self {
        Self::SubscribeType(value)
    }
}

#[derive(Debug)]
struct Task {
    target: Target,
    operation: Operation,
    info: Info,
    version: Option<NotificationVersion>,
}

/// [`NotificationManager`] is used to send notification to frontends and compute nodes.
pub struct NotificationManager<S> {
    core: Arc<Mutex<NotificationManagerCore>>,
    /// Sender used to add a notification into the waiting queue.
    task_tx: UnboundedSender<Task>,
    /// The current notification version.
    current_version: Mutex<Version>,
    meta_store: Arc<S>,
}

impl<S> NotificationManager<S>
where
    S: MetaStore,
{
    pub async fn new(meta_store: Arc<S>) -> Self {
        // notification waiting queue.
        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task>();
        let core = Arc::new(Mutex::new(NotificationManagerCore::new()));
        let core_clone = core.clone();

        tokio::spawn(async move {
            while let Some(task) = task_rx.recv().await {
                let response = SubscribeResponse {
                    status: None,
                    operation: task.operation as i32,
                    info: Some(task.info),
                    version: task.version.unwrap_or_default(),
                };

                let mut guard = core.lock().await;
                match task.target {
                    Target::SubscribeType(subscribe_type) => {
                        guard.notify(subscribe_type, response);
                    }
                    Target::WorkerKey(worker_key) => {
                        guard.notify_with_worker_key(worker_key, response);
                    }
                }
            }
        });

        Self {
            core: core_clone,
            task_tx,
            current_version: Mutex::new(Version::new(&*meta_store).await),
            meta_store,
        }
    }

    pub async fn abort_all(&self) {
        let mut guard = self.core.lock().await;
        *guard = NotificationManagerCore::new();
        guard.exiting = true;
    }

    #[inline(always)]
    fn notify(
        &self,
        target: Target,
        operation: Operation,
        info: Info,
        version: Option<NotificationVersion>,
    ) {
        let task = Task {
            target,
            operation,
            info,
            version,
        };
        self.task_tx.send(task).unwrap();
    }

    /// Add a notification to the waiting queue and increase notification version.
    async fn notify_with_version(
        &self,
        target: Target,
        operation: Operation,
        info: Info,
    ) -> NotificationVersion {
        let mut version_guard = self.current_version.lock().await;
        version_guard.increase_version(&*self.meta_store).await;
        let version = version_guard.version();
        self.notify(target, operation, info, Some(version));
        version
    }

    /// Add a notification to the waiting queue and return immediately
    #[inline(always)]
    fn notify_without_version(&self, target: Target, operation: Operation, info: Info) {
        self.notify(target, operation, info, None);
    }

    pub fn notify_snapshot(&self, worker_key: WorkerKey, meta_snapshot: MetaSnapshot) {
        self.notify_without_version(
            Target::WorkerKey(worker_key),
            Operation::Snapshot,
            Info::Snapshot(meta_snapshot),
        )
    }

    pub async fn notify_frontend(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify_with_version(SubscribeType::Frontend.into(), operation, info)
            .await
    }

    pub async fn notify_hummock(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify_with_version(SubscribeType::Hummock.into(), operation, info)
            .await
    }

    pub async fn notify_compactor(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify_with_version(SubscribeType::Compactor.into(), operation, info)
            .await
    }

    pub async fn notify_compute(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify_with_version(SubscribeType::Compute.into(), operation, info)
            .await
    }

    pub fn notify_frontend_without_version(&self, operation: Operation, info: Info) {
        self.notify_without_version(SubscribeType::Frontend.into(), operation, info)
    }

    pub fn notify_hummock_without_version(&self, operation: Operation, info: Info) {
        self.notify_without_version(SubscribeType::Hummock.into(), operation, info)
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
    pub async fn delete_sender(&self, worker_type: WorkerType, worker_key: WorkerKey) {
        let mut core_guard = self.core.lock().await;
        // TODO: we may avoid passing the worker_type and remove the `worker_key` in all sender
        // holders anyway
        match worker_type {
            WorkerType::Frontend => core_guard.frontend_senders.remove(&worker_key),
            WorkerType::ComputeNode | WorkerType::RiseCtl => {
                core_guard.hummock_senders.remove(&worker_key)
            }
            WorkerType::Compactor => core_guard.compactor_senders.remove(&worker_key),
            _ => unreachable!(),
        };
    }

    /// Tell `NotificationManagerCore` to insert sender by `worker_type`.
    pub async fn insert_sender(
        &self,
        subscribe_type: SubscribeType,
        worker_key: WorkerKey,
        sender: UnboundedSender<Notification>,
    ) {
        let mut core_guard = self.core.lock().await;
        if core_guard.exiting {
            tracing::warn!("notification manager exiting.");
            return;
        }
        let senders = match subscribe_type {
            SubscribeType::Frontend => &mut core_guard.frontend_senders,
            SubscribeType::Hummock => &mut core_guard.hummock_senders,
            SubscribeType::Compactor => &mut core_guard.compactor_senders,
            SubscribeType::Compute => &mut core_guard.compute_senders,
            SubscribeType::Unspecified => unreachable!(),
        };

        senders.insert(worker_key, sender);
    }

    pub async fn insert_local_sender(&self, sender: UnboundedSender<LocalNotification>) {
        let mut core_guard = self.core.lock().await;
        if core_guard.exiting {
            tracing::warn!("notification manager exiting.");
            return;
        }
        core_guard.local_senders.push(sender);
    }

    pub async fn current_version(&self) -> NotificationVersion {
        let version_guard = self.current_version.lock().await;
        version_guard.version()
    }
}

struct NotificationManagerCore {
    /// The notification sender to frontends.
    frontend_senders: HashMap<WorkerKey, UnboundedSender<Notification>>,
    /// The notification sender to nodes that subscribes the hummock.
    hummock_senders: HashMap<WorkerKey, UnboundedSender<Notification>>,
    /// The notification sender to compactor nodes.
    compactor_senders: HashMap<WorkerKey, UnboundedSender<Notification>>,
    /// The notification sender to compute nodes.
    compute_senders: HashMap<WorkerKey, UnboundedSender<Notification>>,
    /// The notification sender to local subscribers.
    local_senders: Vec<UnboundedSender<LocalNotification>>,
    exiting: bool,
}

impl NotificationManagerCore {
    fn new() -> Self {
        Self {
            frontend_senders: HashMap::new(),
            hummock_senders: HashMap::new(),
            compactor_senders: HashMap::new(),
            compute_senders: HashMap::new(),
            local_senders: vec![],
            exiting: false,
        }
    }

    fn notify_with_worker_key(&mut self, worker_key: WorkerKey, response: SubscribeResponse) {
        for senders in [
            &mut self.frontend_senders,
            &mut self.hummock_senders,
            &mut self.compactor_senders,
        ] {
            match senders.entry(worker_key.clone()) {
                Entry::Occupied(entry) => {
                    entry.get().send(Ok(response)).unwrap_or_else(|_| {
                        entry.remove_entry();
                    });
                    return;
                }
                Entry::Vacant(_) => continue,
            }
        }

        tracing::warn!("Failed to find notification sender of {:?}", worker_key);
    }

    fn notify(&mut self, subscribe_type: SubscribeType, response: SubscribeResponse) {
        let senders = match subscribe_type {
            SubscribeType::Frontend => &mut self.frontend_senders,
            SubscribeType::Hummock => &mut self.hummock_senders,
            SubscribeType::Compactor => &mut self.compactor_senders,
            SubscribeType::Compute => &mut self.compute_senders,
            SubscribeType::Unspecified => unreachable!(),
        };

        senders.retain(|worker_key, sender| {
            sender
                .send(Ok(response.clone()))
                .inspect_err(|err| {
                    tracing::warn!(
                        "Failed to notify {:?} {:?}: {}",
                        subscribe_type,
                        worker_key,
                        err
                    )
                })
                .is_ok()
        });
    }
}
