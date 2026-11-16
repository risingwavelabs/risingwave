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
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tonic::Status;

use crate::controller::SqlMetaStore;
use crate::manager::WorkerKey;
use crate::manager::notification_version::NotificationVersionGenerator;
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

/// [`NotificationManager`] is used to send notification to frontends and compute nodes.
pub struct NotificationManager {
    core: parking_lot::Mutex<NotificationManagerCore>,
    /// The current notification version generator.
    version_generator: Mutex<NotificationVersionGenerator>,
}

impl NotificationManager {
    pub async fn new(meta_store_impl: SqlMetaStore) -> Self {
        let version_generator = NotificationVersionGenerator::new(meta_store_impl)
            .await
            .unwrap();
        let core = NotificationManagerCore::new(version_generator.current_version());

        Self {
            core: parking_lot::Mutex::new(core),
            version_generator: Mutex::new(version_generator),
        }
    }

    pub fn abort_all(&self) {
        let mut guard = self.core.lock();
        *guard = NotificationManagerCore::new(guard.latest_version);
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
        // Publish immediately so callers can order notifications with snapshots by retaining
        // the corresponding state guard, without leaving work pending in a background queue.
        let mut core = self.core.lock();
        if let Some(version) = version {
            core.latest_version = version;
        }
        let response = SubscribeResponse {
            status: None,
            operation: operation as i32,
            info: Some(info),
            version: core.latest_version,
        };
        core.notify(target, response);
    }

    /// Add a notification and increase notification version.
    async fn notify_with_version(
        &self,
        target: Target,
        operation: Operation,
        info: Info,
    ) -> NotificationVersion {
        let mut version_guard = self.version_generator.lock().await;
        version_guard.increase_version().await;
        let version = version_guard.current_version();
        self.notify(target, operation, info, Some(version));
        version
    }

    /// Assign consecutive versions to a frontend notification batch and publish the complete batch
    /// while holding the version generator lock.
    async fn notify_frontend_batch(
        &self,
        notifications: Vec<(Operation, Info)>,
    ) -> Option<NotificationVersion> {
        let batch_len: NotificationVersion = notifications.len().try_into().unwrap();
        if batch_len == 0 {
            return None;
        }

        let mut version_guard = self.version_generator.lock().await;
        let first_version = version_guard.current_version() + 1;
        version_guard.increase_version_by(batch_len).await;
        let final_version = version_guard.current_version();

        for ((operation, info), version) in notifications
            .into_iter()
            .zip_eq_debug(first_version..=final_version)
        {
            self.notify(
                SubscribeType::Frontend.into(),
                operation,
                info,
                Some(version),
            );
        }

        Some(final_version)
    }

    /// Add a notification using the latest version published to the queue without incrementing it.
    #[inline(always)]
    fn notify_without_version(&self, target: Target, operation: Operation, info: Info) {
        self.notify(target, operation, info, None);
    }

    /// Create a channel, send the initial snapshot, and register its sender in one critical section.
    /// The caller must retain all snapshot state guards until this returns, and acquire `core` last.
    pub fn subscribe_with_snapshot(
        &self,
        subscribe_type: SubscribeType,
        worker_key: WorkerKey,
        meta_snapshot: MetaSnapshot,
    ) -> UnboundedReceiver<Notification> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut core = self.core.lock();
        if core.exiting {
            tracing::warn!("notification manager exiting.");
            return receiver;
        }
        let response = SubscribeResponse {
            status: None,
            operation: Operation::Snapshot as i32,
            info: Some(Info::Snapshot(meta_snapshot)),
            version: Default::default(),
        };
        sender
            .send(Ok(response))
            .expect("new notification receiver is open");
        core.senders_of(subscribe_type).insert(worker_key, sender);
        receiver
    }

    pub fn notify_all_without_version(&self, operation: Operation, info: Info) {
        for subscribe_type in [
            SubscribeType::Frontend,
            SubscribeType::Hummock,
            SubscribeType::Compactor,
            SubscribeType::Compute,
        ] {
            self.notify_without_version(subscribe_type.into(), operation, info.clone());
        }
    }

    #[must_use]
    pub async fn notify_frontend(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.notify_with_version(SubscribeType::Frontend.into(), operation, info)
            .await
    }

    #[must_use]
    pub async fn notify_frontend_object_info(
        &self,
        operation: Operation,
        object_info: PbObjectInfo,
    ) -> NotificationVersion {
        self.notify_with_version(
            SubscribeType::Frontend.into(),
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
        self.notify_without_version(
            Target {
                subscribe_type: SubscribeType::Hummock,
                worker_key: Some(worker_key),
            },
            Operation::Update,
            info,
        );
    }

    pub fn notify_compute_without_version(&self, operation: Operation, info: Info) {
        self.notify_without_version(SubscribeType::Compute.into(), operation, info)
    }

    pub fn notify_frontend_without_version(&self, operation: Operation, info: Info) {
        self.notify_without_version(SubscribeType::Frontend.into(), operation, info)
    }

    pub fn notify_hummock(&self, operation: Operation, info: Info) {
        self.notify_without_version(SubscribeType::Hummock.into(), operation, info)
    }

    pub fn notify_compactor(&self, operation: Operation, info: Info) {
        self.notify_without_version(SubscribeType::Compactor.into(), operation, info)
    }

    #[cfg(any(test, feature = "test"))]
    pub fn notify_hummock_with_version(
        &self,
        operation: Operation,
        info: Info,
        version: Option<NotificationVersion>,
    ) {
        self.notify(SubscribeType::Hummock.into(), operation, info, version)
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

    pub async fn current_version(&self) -> NotificationVersion {
        let version_guard = self.version_generator.lock().await;
        version_guard.current_version()
    }
}

type SenderMap = HashMap<WorkerKey, UnboundedSender<Notification>>;

struct NotificationManagerCore {
    /// Catalog version of the latest synchronously published notification batch entry.
    latest_version: NotificationVersion,
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
    fn new(latest_version: NotificationVersion) -> Self {
        Self {
            latest_version,
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
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        let (tx3, mut rx3) = mpsc::unbounded_channel();
        let mut rx1 = mgr.subscribe_with_snapshot(
            SubscribeType::Hummock,
            worker_key1.clone(),
            MetaSnapshot::default(),
        );
        mgr.insert_sender(SubscribeType::Frontend, worker_key1.clone(), tx2);
        mgr.insert_sender(SubscribeType::Frontend, worker_key2, tx3);
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
        assert_eq!(
            rx1.recv().await.unwrap().unwrap().version,
            frontend_notification.version
        );
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

        let initial_version = mgr.current_version().await;
        let mut batch = FrontendNotificationBatch::new(&mgr);
        batch.add(Operation::Add, Info::Database(Default::default()));
        batch.add(Operation::Add, Info::Schema(Default::default()));
        let published_version = batch
            .publish()
            .await
            .expect("the test notification batch contains two notifications");

        assert_eq!(published_version, initial_version + 2);
        assert_eq!(mgr.current_version().await, published_version);
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

        let version = mgr.current_version().await;
        mgr.notify_hummock_targeted_update(worker_key1, Info::Database(Default::default()));

        let notification = hummock_rx1.recv().await.unwrap().unwrap();
        assert_eq!(notification.version, version);
        assert_eq!(mgr.current_version().await, version);
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
        let mut rx1 = mgr.subscribe_with_snapshot(
            SubscribeType::Hummock,
            worker_key.clone(),
            MetaSnapshot::default(),
        );

        let mut rx2 = mgr.subscribe_with_snapshot(
            SubscribeType::Hummock,
            worker_key,
            MetaSnapshot::default(),
        );

        for rx in [&mut rx1, &mut rx2] {
            assert!(matches!(
                rx.try_recv().unwrap().unwrap().info,
                Some(Info::Snapshot(_))
            ));
            assert!(rx.try_recv().is_err());
        }

        let version = mgr.current_version().await;
        mgr.notify_hummock(Operation::Add, Info::Database(Default::default()));
        assert!(rx1.try_recv().is_err());
        let notification = rx2.recv().await.unwrap().unwrap();
        assert_eq!(notification.version, version);
        assert_eq!(mgr.current_version().await, version);
    }

    #[tokio::test]
    async fn test_snapshot_registration_excludes_earlier_notifications() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let key = WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 1,
        });
        let (old_tx, mut old_rx) = mpsc::unbounded_channel();
        mgr.insert_sender(SubscribeType::Frontend, key.clone(), old_tx);

        let database = risingwave_pb::catalog::Database {
            id: 123.into(),
            name: "before_snapshot".to_owned(),
            ..Default::default()
        };
        let version = mgr
            .notify_frontend(Operation::Add, Info::Database(database.clone()))
            .await;
        // Publication has completed before a replacement subscriber can be registered.
        assert_eq!(old_rx.try_recv().unwrap().unwrap().version, version);

        let mut rx = mgr.subscribe_with_snapshot(
            SubscribeType::Frontend,
            key,
            MetaSnapshot {
                databases: vec![database],
                version: Some(risingwave_pb::meta::meta_snapshot::SnapshotVersion {
                    catalog_version: version,
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        assert!(matches!(
            rx.try_recv().unwrap().unwrap().info,
            Some(Info::Snapshot(_))
        ));
        // Give any background work a chance to run: no older update may arrive after snapshot.
        tokio::task::yield_now().await;
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        let next_version = mgr
            .notify_frontend(Operation::Delete, Info::Database(Default::default()))
            .await;
        assert!(next_version > version);
        assert_eq!(rx.try_recv().unwrap().unwrap().version, next_version);
        assert!(old_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_snapshot_subscription_after_abort_is_closed() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let key = WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 1,
        });
        mgr.abort_all();
        let mut rx =
            mgr.subscribe_with_snapshot(SubscribeType::Frontend, key, MetaSnapshot::default());

        assert!(rx.recv().await.is_none());
        assert!(mgr.core.lock().frontend_senders.is_empty());
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
