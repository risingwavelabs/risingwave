// Copyright 2022 RisingWave Labs
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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use risingwave_common::id::JobId;
use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_meta_model::ObjectId;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::object::PbObjectInfo;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{
    MetaSnapshot, PbObject, PbObjectGroup, SubscribeResponse, SubscribeType,
};
use thiserror_ext::AsReport;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, UnboundedSender};
use tonic::Status;

use crate::controller::SqlMetaStore;
use crate::manager::WorkerKey;
use crate::manager::catalog_version::PersistedCatalogVersion;
use crate::model::FragmentId;

pub type MessageStatus = Status;
pub type Notification = Result<SubscribeResponse, Status>;
pub type NotificationManagerRef = Arc<NotificationManager>;
pub type NotificationVersion = u64;
/// NOTE(kwannoel): This is just ignored, used in background DDL
pub const IGNORED_NOTIFICATION_VERSION: u64 = 0;

/// A batch of frontend notifications that belong to the same catalog update.
///
/// Publishing the batch assigns consecutive catalog notification versions to its notifications.
/// Therefore, waiting for the returned final version also waits for every earlier notification in
/// the batch to be processed.
pub struct FrontendNotificationBatch<'a> {
    notification_manager: &'a NotificationManager,
    pending: Vec<(Operation, Info)>,
}

impl<'a> FrontendNotificationBatch<'a> {
    pub fn new(notification_manager: &'a NotificationManager) -> Self {
        Self {
            notification_manager,
            pending: Vec::new(),
        }
    }

    pub fn add(&mut self, operation: Operation, info: Info) {
        self.pending.push((operation, info));
    }

    #[must_use]
    pub async fn publish(self) -> Option<NotificationVersion> {
        self.notification_manager
            .notify_frontend_batch(self.pending)
            .await
    }
}

#[derive(Clone, Debug)]
pub enum LocalNotification {
    WorkerNodeDeleted(WorkerNode),
    WorkerNodeActivated(WorkerNode),
    SystemParamsChange(SystemParamsReader),
    BatchParallelismChange,
    ServingFragmentMappingsUpsert(Vec<FragmentId>),
    ServingFragmentMappingsDelete(Vec<FragmentId>),
    SourceDropped(ObjectId),
    StreamingJobBackfillFinished(JobId),
}

#[derive(Debug)]
struct Target {
    subscribe_type: SubscribeType,
    // `None` indicates sending to all subscribers of `subscribe_type`.
    worker_key: Option<WorkerKey>,
}

impl From<SubscribeType> for Target {
    fn from(value: SubscribeType) -> Self {
        Self {
            subscribe_type: value,
            worker_key: None,
        }
    }
}

#[derive(Debug)]
struct Task {
    target: Target,
    operation: Operation,
    info: Info,
    version: NotificationVersion,
}

/// [`NotificationManager`] is used to send notification to frontends and compute nodes.
///
/// Every notification takes a new version from one sequence, whichever subscribers it goes to. The
/// sequence is used in two ways:
/// - Each part of a snapshot records the latest version when it is read. A subscriber drops a
///   notification about that part if its version is not greater, because the snapshot includes it.
/// - After a DDL, frontends wait for the version of the latest catalog notification. They do not
///   receive every notification, so they cannot wait for the latest version of any kind.
pub struct NotificationManager {
    core: Arc<parking_lot::Mutex<NotificationManagerCore>>,
    /// Sender used to add a notification into the waiting queue.
    task_tx: UnboundedSender<Task>,
    /// The version of the latest notification. It is not persisted, because subscribers get a new
    /// snapshot after meta restarts.
    notification_version: AtomicU64,
    /// The version of the latest catalog notification.
    catalog_version: Mutex<PersistedCatalogVersion>,
}

impl NotificationManager {
    pub async fn new(meta_store_impl: SqlMetaStore) -> Self {
        // notification waiting queue.
        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task>();
        let core = Arc::new(parking_lot::Mutex::new(NotificationManagerCore::new()));
        let core_clone = core.clone();
        let catalog_version = PersistedCatalogVersion::new(meta_store_impl).await.unwrap();
        let notification_version = AtomicU64::new(catalog_version.get());

        tokio::spawn(async move {
            while let Some(task) = task_rx.recv().await {
                let response = SubscribeResponse {
                    status: None,
                    operation: task.operation as i32,
                    info: Some(task.info),
                    version: task.version,
                };
                core.lock().notify(task.target, response);
            }
        });

        Self {
            core: core_clone,
            task_tx,
            notification_version,
            catalog_version: Mutex::new(catalog_version),
        }
    }

    pub fn abort_all(&self) {
        let mut guard = self.core.lock();
        *guard = NotificationManagerCore::new();
        guard.exiting = true;
    }

    /// Allocate `count` consecutive versions and return the last one.
    fn allocate_versions(&self, count: NotificationVersion) -> NotificationVersion {
        self.notification_version
            .fetch_add(count, Ordering::Relaxed)
            + count
    }

    #[inline(always)]
    fn enqueue(
        &self,
        target: Target,
        operation: Operation,
        info: Info,
        version: NotificationVersion,
    ) {
        let task = Task {
            target,
            operation,
            info,
            version,
        };
        self.task_tx.send(task).unwrap();
    }

    /// Add catalog notifications to the waiting queue. Assign consecutive versions to the batch and
    /// enqueue the complete batch while holding the catalog version lock.
    async fn notify_frontend_batch(
        &self,
        notifications: Vec<(Operation, Info)>,
    ) -> Option<NotificationVersion> {
        let batch_len: NotificationVersion = notifications.len().try_into().unwrap();
        if batch_len == 0 {
            return None;
        }

        let mut catalog_version = self.catalog_version.lock().await;
        let final_version = self.allocate_versions(batch_len);
        let first_version = final_version - batch_len + 1;
        catalog_version.set(final_version).await;

        for ((operation, info), version) in notifications
            .into_iter()
            .zip_eq_debug(first_version..=final_version)
        {
            self.enqueue(SubscribeType::Frontend.into(), operation, info, version);
        }

        Some(final_version)
    }

    /// Add a notification that is not a catalog change to the waiting queue.
    #[inline(always)]
    fn notify(&self, target: Target, operation: Operation, info: Info) {
        let version = self.allocate_versions(1);
        self.enqueue(target, operation, info, version);
    }

    pub fn notify_snapshot(
        &self,
        sender: UnboundedSender<Notification>,
        meta_snapshot: MetaSnapshot,
    ) {
        let response = SubscribeResponse {
            status: None,
            operation: Operation::Snapshot as i32,
            info: Some(Info::Snapshot(meta_snapshot)),
            version: Default::default(),
        };
        let _ = sender.send(Ok(response)).inspect_err(|err| {
            tracing::warn!("Failed to notify direct subscriber: {}", err.as_report());
        });
    }

    pub fn notify_all(&self, operation: Operation, info: Info) {
        for subscribe_type in [
            SubscribeType::Frontend,
            SubscribeType::Hummock,
            SubscribeType::Compactor,
            SubscribeType::Compute,
        ] {
            self.notify(subscribe_type.into(), operation, info.clone());
        }
    }

    /// Notify frontends of a catalog change. They can wait for the returned version.
    #[must_use]
    pub async fn notify_frontend(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify_frontend_batch(vec![(operation, info)])
            .await
            .expect("the batch contains a notification")
    }

    #[must_use]
    pub async fn notify_frontend_object_info(
        &self,
        operation: Operation,
        object_info: PbObjectInfo,
    ) -> NotificationVersion {
        self.notify_frontend(
            operation,
            Info::ObjectGroup(PbObjectGroup {
                objects: vec![PbObject {
                    object_info: object_info.into(),
                }],
                dependencies: vec![],
            }),
        )
        .await
    }

    pub(crate) fn notify_hummock_targeted_update(&self, worker_key: WorkerKey, info: Info) {
        self.notify(
            Target {
                subscribe_type: SubscribeType::Hummock,
                worker_key: Some(worker_key),
            },
            Operation::Update,
            info,
        );
    }

    pub fn notify_compute(&self, operation: Operation, info: Info) {
        self.notify(SubscribeType::Compute.into(), operation, info)
    }

    pub fn notify_frontend_non_catalog(&self, operation: Operation, info: Info) {
        self.notify(SubscribeType::Frontend.into(), operation, info)
    }

    pub fn notify_hummock(&self, operation: Operation, info: Info) {
        self.notify(SubscribeType::Hummock.into(), operation, info)
    }

    pub fn notify_compactor(&self, operation: Operation, info: Info) {
        self.notify(SubscribeType::Compactor.into(), operation, info)
    }

    #[cfg(any(test, feature = "test"))]
    pub fn notify_hummock_with_version(
        &self,
        operation: Operation,
        info: Info,
        version: NotificationVersion,
    ) {
        self.enqueue(SubscribeType::Hummock.into(), operation, info, version)
    }

    pub fn notify_local_subscribers(&self, notification: LocalNotification) {
        let mut core_guard = self.core.lock();
        core_guard.local_senders.retain(|sender| {
            if let Err(err) = sender.send(notification.clone()) {
                tracing::warn!(error = %err.as_report(), "Failed to notify local subscriber");
                return false;
            }
            true
        });
    }

    /// Tell `NotificationManagerCore` to delete sender.
    pub fn delete_sender(&self, worker_type: WorkerType, worker_key: WorkerKey) {
        let mut core_guard = self.core.lock();
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
    pub fn insert_sender(
        &self,
        subscribe_type: SubscribeType,
        worker_key: WorkerKey,
        sender: UnboundedSender<Notification>,
    ) {
        let mut core_guard = self.core.lock();
        if core_guard.exiting {
            tracing::warn!("notification manager exiting.");
            return;
        }
        let senders = core_guard.senders_of(subscribe_type);

        senders.insert(worker_key, sender);
    }

    pub fn insert_local_sender(&self, sender: UnboundedSender<LocalNotification>) {
        let mut core_guard = self.core.lock();
        if core_guard.exiting {
            tracing::warn!("notification manager exiting.");
            return;
        }
        core_guard.local_senders.push(sender);
    }

    #[cfg(test)]
    pub fn clear_local_sender(&self) {
        self.core.lock().local_senders.clear();
    }

    pub async fn current_catalog_version(&self) -> NotificationVersion {
        self.catalog_version.lock().await.get()
    }

    /// The version of the latest notification. Read it together with a part of a snapshot under
    /// the read lock of that part, and send notifications about that part under its write lock, so
    /// that the version tells exactly which notifications the snapshot includes.
    pub fn current_notification_version(&self) -> NotificationVersion {
        self.notification_version.load(Ordering::Relaxed)
    }
}

type SenderMap = HashMap<WorkerKey, UnboundedSender<Notification>>;

struct NotificationManagerCore {
    /// The notification sender to frontends.
    frontend_senders: SenderMap,
    /// The notification sender to nodes that subscribes the hummock.
    hummock_senders: SenderMap,
    /// The notification sender to compactor nodes.
    compactor_senders: SenderMap,
    /// The notification sender to compute nodes.
    compute_senders: SenderMap,
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

    fn notify(&mut self, target: Target, response: SubscribeResponse) {
        macro_rules! warn_send_failure {
            ($subscribe_type:expr, $worker_key:expr, $err:expr) => {
                tracing::warn!(
                    "Failed to notify {:?} {:?}: {}",
                    $subscribe_type,
                    $worker_key,
                    $err
                );
            };
        }

        let senders = self.senders_of(target.subscribe_type);

        if let Some(worker_key) = target.worker_key {
            match senders.entry(worker_key.clone()) {
                Entry::Occupied(entry) => {
                    let _ = entry.get().send(Ok(response)).inspect_err(|err| {
                        warn_send_failure!(target.subscribe_type, &worker_key, err.as_report());
                        entry.remove_entry();
                    });
                }
                Entry::Vacant(_) => {
                    tracing::warn!("Failed to find notification sender of {:?}", worker_key)
                }
            }
        } else {
            senders.retain(|worker_key, sender| {
                sender
                    .send(Ok(response.clone()))
                    .inspect_err(|err| {
                        warn_send_failure!(target.subscribe_type, &worker_key, err.as_report());
                    })
                    .is_ok()
            });
        }
    }

    fn senders_of(&mut self, subscribe_type: SubscribeType) -> &mut SenderMap {
        match subscribe_type {
            SubscribeType::Frontend => &mut self.frontend_senders,
            SubscribeType::Hummock => &mut self.hummock_senders,
            SubscribeType::Compactor => &mut self.compactor_senders,
            SubscribeType::Compute => &mut self.compute_senders,
            SubscribeType::Unspecified => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::id::JobId;
    use risingwave_pb::common::HostAddress;

    use super::*;
    use crate::manager::WorkerKey;

    #[tokio::test]
    async fn test_multiple_subscribers_one_worker() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let worker_key1 = WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 1,
        });
        let worker_key2 = WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 2,
        });
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        let (tx3, mut rx3) = mpsc::unbounded_channel();
        mgr.insert_sender(SubscribeType::Hummock, worker_key1.clone(), tx1.clone());
        mgr.insert_sender(SubscribeType::Frontend, worker_key1.clone(), tx2);
        mgr.insert_sender(SubscribeType::Frontend, worker_key2, tx3);
        mgr.notify_snapshot(tx1, MetaSnapshot::default());
        assert!(rx1.recv().await.is_some());
        assert!(rx2.try_recv().is_err());
        assert!(rx3.try_recv().is_err());

        let version = mgr
            .notify_frontend(Operation::Add, Info::Database(Default::default()))
            .await;
        assert!(rx1.try_recv().is_err());
        let frontend_notification = rx2.recv().await.unwrap().unwrap();
        assert_eq!(frontend_notification.version, version);
        assert_eq!(
            rx3.recv().await.unwrap().unwrap().version,
            frontend_notification.version
        );

        mgr.notify_hummock(Operation::Add, Info::Database(Default::default()));
        assert_eq!(rx1.recv().await.unwrap().unwrap().version, version + 1);
    }

    #[tokio::test]
    async fn test_every_notification_has_its_own_version() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let worker_key = WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 1,
        });
        let (tx, mut rx) = mpsc::unbounded_channel();
        mgr.insert_sender(SubscribeType::Frontend, worker_key, tx);

        let catalog_version = mgr
            .notify_frontend(Operation::Add, Info::Database(Default::default()))
            .await;
        assert_eq!(rx.recv().await.unwrap().unwrap().version, catalog_version);

        mgr.notify_frontend_non_catalog(Operation::Add, Info::Node(Default::default()));
        mgr.notify_frontend_non_catalog(Operation::Add, Info::Node(Default::default()));
        assert_eq!(
            rx.recv().await.unwrap().unwrap().version,
            catalog_version + 1
        );
        assert_eq!(
            rx.recv().await.unwrap().unwrap().version,
            catalog_version + 2
        );
        assert_eq!(mgr.current_notification_version(), catalog_version + 2);
        // Frontends never wait for those two, so `WAIT` must not read their versions.
        assert_eq!(mgr.current_catalog_version().await, catalog_version);

        let next_catalog_version = mgr
            .notify_frontend(Operation::Add, Info::Database(Default::default()))
            .await;
        assert_eq!(next_catalog_version, catalog_version + 3);
        assert_eq!(mgr.current_catalog_version().await, next_catalog_version);
    }

    #[tokio::test]
    async fn test_versions_after_restart() {
        let meta_store = SqlMetaStore::for_test().await;
        let mgr = NotificationManager::new(meta_store.clone()).await;
        let catalog_version = mgr
            .notify_frontend(Operation::Add, Info::Database(Default::default()))
            .await;
        mgr.notify_hummock(Operation::Add, Info::Database(Default::default()));
        assert_eq!(mgr.current_notification_version(), catalog_version + 1);

        let restarted = NotificationManager::new(meta_store).await;
        assert_eq!(restarted.current_catalog_version().await, catalog_version);
        assert_eq!(restarted.current_notification_version(), catalog_version);
    }

    #[tokio::test]
    async fn test_frontend_notification_batch_assigns_consecutive_versions() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let worker_key = WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 1,
        });
        let (tx, mut rx) = mpsc::unbounded_channel();
        mgr.insert_sender(SubscribeType::Frontend, worker_key, tx);

        let initial_version = mgr.current_catalog_version().await;
        let mut batch = FrontendNotificationBatch::new(&mgr);
        batch.add(Operation::Add, Info::Database(Default::default()));
        batch.add(Operation::Add, Info::Schema(Default::default()));
        let published_version = batch
            .publish()
            .await
            .expect("the test notification batch contains two notifications");

        assert_eq!(published_version, initial_version + 2);
        assert_eq!(mgr.current_catalog_version().await, published_version);
        assert_eq!(
            rx.recv().await.unwrap().unwrap().version,
            initial_version + 1
        );
        assert_eq!(rx.recv().await.unwrap().unwrap().version, published_version);
    }

    #[tokio::test]
    async fn test_notify_hummock_targeted_update() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let worker_key1 = WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 1,
        });
        let worker_key2 = WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 2,
        });
        let (hummock_tx1, mut hummock_rx1) = mpsc::unbounded_channel();
        let (hummock_tx2, mut hummock_rx2) = mpsc::unbounded_channel();
        let (frontend_tx1, mut frontend_rx1) = mpsc::unbounded_channel();
        mgr.insert_sender(SubscribeType::Hummock, worker_key1.clone(), hummock_tx1);
        mgr.insert_sender(SubscribeType::Hummock, worker_key2.clone(), hummock_tx2);
        mgr.insert_sender(SubscribeType::Frontend, worker_key1.clone(), frontend_tx1);

        let version = mgr.current_catalog_version().await;
        mgr.notify_hummock_targeted_update(worker_key1, Info::Database(Default::default()));

        let notification = hummock_rx1.recv().await.unwrap().unwrap();
        assert_eq!(notification.version, version + 1);
        assert_eq!(mgr.current_catalog_version().await, version);
        assert!(hummock_rx2.try_recv().is_err());
        assert!(frontend_rx1.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_snapshot_uses_original_sender_after_resubscribe() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let worker_key = WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 1,
        });
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        mgr.insert_sender(SubscribeType::Hummock, worker_key.clone(), tx1.clone());
        mgr.notify_snapshot(tx1, MetaSnapshot::default());

        let (tx2, mut rx2) = mpsc::unbounded_channel();
        mgr.insert_sender(SubscribeType::Hummock, worker_key, tx2);

        assert!(rx1.recv().await.is_some());
        assert!(rx2.try_recv().is_err());

        let version = mgr.current_catalog_version().await;
        mgr.notify_hummock(Operation::Add, Info::Database(Default::default()));
        assert!(rx1.try_recv().is_err());
        let notification = rx2.recv().await.unwrap().unwrap();
        assert_eq!(notification.version, version + 1);
        assert_eq!(mgr.current_catalog_version().await, version);
    }

    #[tokio::test]
    async fn test_local_notification_backfill_finished() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        mgr.insert_local_sender(tx);

        let job_id = JobId::new(42);
        mgr.notify_local_subscribers(LocalNotification::StreamingJobBackfillFinished(job_id));

        match rx.recv().await.expect("should receive notification") {
            LocalNotification::StreamingJobBackfillFinished(received) => {
                assert_eq!(received, job_id);
            }
            other => panic!("unexpected notification: {other:?}"),
        }
    }
}
