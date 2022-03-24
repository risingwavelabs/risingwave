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

use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio::time;
use tonic::Status;

use super::{Epoch, EpochGeneratorRef};
use crate::cluster::WorkerKey;

pub type Notification = std::result::Result<SubscribeResponse, Status>;

/// Interval before retry when notify fail.
const NOTIFY_RETRY_INTERVAL: u64 = 10;

/// [`NotificationManager`] is used to send notification to frontends and compute nodes.
pub struct NotificationManager {
    core: Mutex<NotificationManagerCore>,
    /// Sender used `Self::delete_sender` method.
    /// Tell `NotificationManagerCore` to skip some retry and delete senders.
    tx: UnboundedSender<WorkerKey>,
}

pub type NotificationManagerRef = Arc<NotificationManager>;

impl NotificationManager {
    pub fn new(epoch_generator: EpochGeneratorRef) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            core: Mutex::new(NotificationManagerCore::new(rx, epoch_generator)),
            tx,
        }
    }

    /// Send a `SubscribeResponse` to frontends.
    pub async fn notify_frontend(&self, operation: Operation, info: &Info) -> Epoch {
        let mut core_guard = self.core.lock().await;
        core_guard.notify_frontend(operation, info).await
    }

    /// Send a `SubscribeResponse` to compute nodes.
    pub async fn notify_compute(&self, operation: Operation, info: &Info) -> Epoch {
        let mut core_guard = self.core.lock().await;
        core_guard.notify_compute(operation, info).await
    }

    /// Send a `SubscribeResponse` to frontends and compute nodes.
    pub async fn notify_all(&self, operation: Operation, info: &Info) {
        let mut core_guard = self.core.lock().await;
        core_guard.notify_frontend(operation, info).await;
        core_guard.notify_compute(operation, info).await;
    }

    /// Tell `NotificationManagerCore` to skip some retry and delete senders.
    pub fn delete_sender(&self, worker_key: WorkerKey) {
        self.tx.send(worker_key).unwrap();
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
}

struct NotificationManagerCore {
    /// The notification sender to frontends.
    frontend_senders: HashMap<WorkerKey, UnboundedSender<Notification>>,
    /// The notification sender to compute nodes.
    compute_senders: HashMap<WorkerKey, UnboundedSender<Notification>>,
    /// Receiver used in heartbeat check. Receive the worker keys of disconnected workers from
    /// `StoredClusterManager::start_heartbeat_checker`.
    rx: UnboundedReceiver<WorkerKey>,
    /// Use epoch as notification version.
    epoch_generator: EpochGeneratorRef,
}

impl NotificationManagerCore {
    fn new(rx: UnboundedReceiver<WorkerKey>, epoch_generator: EpochGeneratorRef) -> Self {
        Self {
            frontend_senders: HashMap::new(),
            compute_senders: HashMap::new(),
            rx,
            epoch_generator,
        }
    }

    async fn notify_frontend(&mut self, operation: Operation, info: &Info) -> Epoch {
        let epoch = self.epoch_generator.generate();
        let mut keys = HashSet::new();
        for (worker_key, sender) in &self.frontend_senders {
            loop {
                // Heartbeat may delete worker.
                // We assume that after a worker is disconnected, before it recalls subscribe, we
                // will call notify.
                while let Ok(x) = self.rx.try_recv() {
                    keys.insert(x);
                }
                if keys.contains(worker_key) {
                    break;
                }
                let result = sender.send(Ok(SubscribeResponse {
                    status: None,
                    operation: operation as i32,
                    info: Some(info.clone()),
                    version: epoch.into_inner(),
                }));
                if result.is_ok() {
                    break;
                }
                time::sleep(Duration::from_micros(NOTIFY_RETRY_INTERVAL)).await;
            }
        }
        self.remove_by_key(keys);
        epoch
    }

    /// Send a `SubscribeResponse` to backend.
    async fn notify_compute(&mut self, operation: Operation, info: &Info) -> Epoch {
        let epoch = self.epoch_generator.generate();
        let mut keys = HashSet::new();
        for (worker_key, sender) in &self.compute_senders {
            loop {
                // Heartbeat may delete worker.
                // We assume that after a worker is disconnected, before it recalls subscribe, we
                // will call notify.
                while let Ok(x) = self.rx.try_recv() {
                    keys.insert(x);
                }
                if keys.contains(worker_key) {
                    break;
                }
                let result = sender.send(Ok(SubscribeResponse {
                    status: None,
                    operation: operation as i32,
                    info: Some(info.clone()),
                    version: epoch.into_inner(),
                }));
                if result.is_ok() {
                    break;
                }
                time::sleep(Duration::from_micros(NOTIFY_RETRY_INTERVAL)).await;
            }
        }
        self.remove_by_key(keys);
        epoch
    }

    fn remove_by_key(&mut self, keys: HashSet<WorkerKey>) {
        keys.into_iter().for_each(|key| {
            self.frontend_senders
                .remove(&key)
                .or_else(|| self.compute_senders.remove(&key));
        });
    }
}
