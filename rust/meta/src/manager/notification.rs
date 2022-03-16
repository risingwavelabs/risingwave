use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use risingwave_common::error::Result;
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use tokio::sync::mpsc::{self, Sender, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;
use tokio::time;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

use crate::cluster::WorkerKey;

pub type Notification = std::result::Result<SubscribeResponse, Status>;

const BUFFER_SIZE: usize = 4;
/// Interval before retry when notify fail.
const NOTIFY_RETRY_INTERVAL: u64 = 10;

/// [`NotificationManager`] is used to send notification to frontend and backend.
pub struct NotificationManager {
    core: Mutex<NotificationManagerCore>,
    /// Sender used `Self::delete_sender` method.
    /// Tell `NotificationManagerCore` to skip some retry and delete senders.
    tx: UnboundedSender<WorkerKey>,
}

pub type NotificationManagerRef = Arc<NotificationManager>;

impl NotificationManager {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            core: Mutex::new(NotificationManagerCore {
                fe_senders: HashMap::new(),
                be_senders: HashMap::new(),
                rx,
            }),
            tx,
        }
    }

    /// Create a pair of sender and receiver using `mpsc::channel`.
    /// Save sender in `NotificationManager` and use receiver to create a stream sent to rpc client.
    pub async fn subscribe(
        &self,
        host_address: HostAddress,
        worker_type: WorkerType,
    ) -> Result<ReceiverStream<Notification>> {
        let mut core_guard = self.core.lock().await;
        let (tx, rx) = mpsc::channel(BUFFER_SIZE);
        match worker_type {
            WorkerType::ComputeNode => core_guard.be_senders.insert(WorkerKey(host_address), tx),
            WorkerType::Frontend => core_guard.fe_senders.insert(WorkerKey(host_address), tx),
            _ => unreachable!(),
        };

        Ok(ReceiverStream::new(rx))
    }

    /// Send a `SubscribeResponse` to frontend.
    pub async fn notify_fe(&self, operation: Operation, info: &Info) {
        let mut core_guard = self.core.lock().await;
        core_guard.notify_fe(operation, info).await;
    }

    /// Send a `SubscribeResponse` to backend.
    pub async fn notify_be(&self, operation: Operation, info: &Info) {
        let mut core_guard = self.core.lock().await;
        core_guard.notify_be(operation, info).await;
    }

    /// Send a `SubscribeResponse` to frontend and backend.
    pub async fn notify_all(&self, operation: Operation, info: &Info) {
        let mut core_guard = self.core.lock().await;
        core_guard.notify_fe(operation, info).await;
        core_guard.notify_be(operation, info).await;
    }

    /// Tell `NotificationManagerCore` to skip some retry and delete senders.
    pub fn delete_sender(&self, worker_key: WorkerKey) {
        self.tx.send(worker_key).unwrap();
    }
}

impl Default for NotificationManager {
    fn default() -> Self {
        Self::new()
    }
}

struct NotificationManagerCore {
    /// The notification sender to frontends.
    fe_senders: HashMap<WorkerKey, Sender<Notification>>,
    /// The notification sender to backends.
    be_senders: HashMap<WorkerKey, Sender<Notification>>,
    /// Receiver used in heartbeat check. Receive the worker keys of disconnected workers from
    /// `StoredClusterManager::start_heartbeat_checker`.
    rx: UnboundedReceiver<WorkerKey>,
}

impl NotificationManagerCore {
    async fn notify_fe(&mut self, operation: Operation, info: &Info) {
        let mut keys = HashSet::new();
        for (worker_key, sender) in &self.fe_senders {
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
                let result = sender
                    .send(Ok(SubscribeResponse {
                        status: None,
                        operation: operation as i32,
                        info: Some(info.clone()),
                    }))
                    .await;
                if result.is_ok() {
                    break;
                }
                time::sleep(Duration::from_micros(NOTIFY_RETRY_INTERVAL)).await;
            }
        }
        self.remove_by_key(keys);
    }

    /// Send a `SubscribeResponse` to backend.
    async fn notify_be(&mut self, operation: Operation, info: &Info) {
        let mut keys = HashSet::new();
        for (worker_key, sender) in &self.be_senders {
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
                let result = sender
                    .send(Ok(SubscribeResponse {
                        status: None,
                        operation: operation as i32,
                        info: Some(info.clone()),
                    }))
                    .await;
                if result.is_ok() {
                    break;
                }
                time::sleep(Duration::from_micros(NOTIFY_RETRY_INTERVAL)).await;
            }
        }
        self.remove_by_key(keys);
    }

    fn remove_by_key(&mut self, keys: HashSet<WorkerKey>) {
        keys.into_iter().for_each(|key| {
            self.fe_senders
                .remove(&key)
                .or_else(|| self.be_senders.remove(&key));
        });
    }
}
