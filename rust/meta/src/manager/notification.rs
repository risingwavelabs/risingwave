use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use risingwave_common::error::Result;
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::SubscribeResponse;
use tokio::sync::mpsc::{self, Sender, UnboundedReceiver};
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
pub struct NotificationManager(Mutex<NotificationManagerCore>);

pub type NotificationManagerRef = Arc<NotificationManager>;

impl NotificationManager {
    pub fn new(rx: UnboundedReceiver<WorkerKey>) -> Self {
        Self(Mutex::new(NotificationManagerCore {
            fe_observers: HashMap::new(),
            be_observers: HashMap::new(),
            rx,
        }))
    }

    /// Create a pair of sender and receiver using `mpsc::channel`.
    /// Save sender in `NotificationManager` and use receiver to create a stream sent to rpc client.
    pub async fn subscribe(
        &self,
        host_address: HostAddress,
        worker_type: WorkerType,
    ) -> Result<ReceiverStream<Notification>> {
        let mut core_guard = self.0.lock().await;
        let (tx, rx) = mpsc::channel(BUFFER_SIZE);
        match worker_type {
            WorkerType::ComputeNode => core_guard.be_observers.insert(WorkerKey(host_address), tx),
            WorkerType::Frontend => core_guard.fe_observers.insert(WorkerKey(host_address), tx),
            _ => unreachable!(),
        };

        Ok(ReceiverStream::new(rx))
    }

    /// Send a `SubscribeResponse` to frontend.
    pub async fn notify_fe(&self, operation: Operation, info: &Info) {
        let mut core_guard = self.0.lock().await;
        core_guard.notify_fe(operation, info).await;
    }

    /// Send a `SubscribeResponse` to backend.
    pub async fn notify_be(&self, operation: Operation, info: &Info) {
        let mut core_guard = self.0.lock().await;
        core_guard.notify_be(operation, info).await;
    }

    /// Send a `SubscribeResponse` to frontend and backend.
    pub async fn notify_all(&self, operation: Operation, info: &Info) {
        let mut core_guard = self.0.lock().await;
        core_guard.notify_fe(operation, info).await;
        core_guard.notify_be(operation, info).await;
    }
}

struct NotificationManagerCore {
    fe_observers: HashMap<WorkerKey, Sender<Notification>>,
    be_observers: HashMap<WorkerKey, Sender<Notification>>,
    rx: UnboundedReceiver<WorkerKey>,
}

impl NotificationManagerCore {
    async fn notify_fe(&mut self, operation: Operation, info: &Info) {
        let mut keys = HashSet::new();
        for (worker_key, sender) in &self.fe_observers {
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
        for (worker_key, sender) in &self.be_observers {
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
            self.fe_observers
                .remove(&key)
                .or_else(|| self.be_observers.remove(&key));
        });
    }
}
