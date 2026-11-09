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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use risingwave_common::id::JobId;
use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_meta_model::ObjectId;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{MetaSnapshot, SubscribeResponse, SubscribeType};
use thiserror_ext::AsReport;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, UnboundedSender};
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
    pub async fn publish<S: SnapshotPartState>(
        self,
        _state: &mut S,
    ) -> Option<NotificationVersion> {
        const { assert!(matches!(S::PART, SnapshotPart::Catalog)) };
        for (_, info) in &self.pending {
            assert_eq!(SnapshotPart::of(info), Some(S::PART));
        }
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
enum Task {
    Notify {
        target: Target,
        operation: Operation,
        info: Info,
        /// `None` means using the latest version published to the notification queue without
        /// incrementing it.
        version: Option<NotificationVersion>,
    },
    /// Forward the notifications behind this task about `part` to the subscriber.
    StartForwarding {
        subscribe_type: SubscribeType,
        worker_key: WorkerKey,
        /// The sender of one subscribe call. The task is ignored if the worker has subscribed
        /// again and replaced its subscriber.
        sender: UnboundedSender<Notification>,
        part: SnapshotPart,
    },
    /// Send a snapshot behind the notifications published before it. The sender belongs to one
    /// subscribe call, because the worker may have subscribed again when this task is reached.
    SendSnapshot {
        sender: UnboundedSender<Notification>,
        snapshot: MetaSnapshot,
    },
}

/// A part of the meta state protected by one lock, which subscribers copy from a snapshot and then
/// update from notifications.
///
/// A snapshot reads its parts one by one, each under the lock of that part. The read queues a task
/// that starts forwarding the part before releasing the read lock, and a change to the part queues
/// its notifications before releasing the write lock, so the queue has the same order as the lock.
/// Notifications ahead of the task describe changes that the read has seen and are dropped. The
/// ones behind it describe changes that the read has not seen and are forwarded.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SnapshotPart {
    Catalog,
    Cluster,
    StreamingMappings,
    ServingMappings,
    HummockVersion,
    HummockWriteLimits,
    SessionParams,
}

/// The state behind the lock of a [`SnapshotPart`]. It is only reachable through a guard of that
/// lock, so `&Self` proves that the lock is held, and `&mut Self` that it is held for writing.
pub trait SnapshotPartState {
    const PART: SnapshotPart;
}

impl SnapshotPart {
    /// The part that a notification changes. Notifications of `None` are always forwarded.
    fn of(info: &Info) -> Option<Self> {
        match info {
            Info::Database(_)
            | Info::Schema(_)
            | Info::ObjectGroup(_)
            | Info::User(_)
            | Info::Connection(_)
            | Info::Secret(_)
            | Info::Function(_) => Some(Self::Catalog),
            Info::TableRefillRuntimeConfig(config) => {
                if config.serving_table_vnode_mappings.is_some() {
                    Some(Self::ServingMappings)
                } else {
                    Some(Self::Catalog)
                }
            }
            Info::Node(_) | Info::ClusterResource(_) => Some(Self::Cluster),
            Info::StreamingWorkerSlotMapping(_) => Some(Self::StreamingMappings),
            Info::ServingWorkerSlotMappings(_) => Some(Self::ServingMappings),
            Info::HummockVersionDeltas(_) => Some(Self::HummockVersion),
            Info::HummockWriteLimits(_) => Some(Self::HummockWriteLimits),
            Info::SessionParam(_) => Some(Self::SessionParams),
            // Not included in a snapshot.
            Info::SystemParams(_)
            | Info::HummockStats(_)
            | Info::Recovery(_)
            | Info::Snapshot(_) => None,
            // Subscribers ignore a manifest that is not newer than theirs.
            Info::MetaBackupManifestId(_) => None,
        }
    }

    /// The parts in the snapshot of a subscriber.
    fn of_subscriber(subscribe_type: SubscribeType) -> HashSet<Self> {
        match subscribe_type {
            SubscribeType::Frontend => HashSet::from([
                Self::Catalog,
                Self::Cluster,
                Self::StreamingMappings,
                Self::ServingMappings,
                Self::HummockVersion,
                Self::SessionParams,
            ]),
            SubscribeType::Hummock => HashSet::from([
                Self::Catalog,
                Self::Cluster,
                Self::ServingMappings,
                Self::HummockVersion,
                Self::HummockWriteLimits,
            ]),
            SubscribeType::Compactor | SubscribeType::Compute => {
                HashSet::from([Self::Catalog, Self::Cluster])
            }
            SubscribeType::Unspecified => unreachable!(),
        }
    }
}

/// A subscriber whose snapshot is being read.
pub struct SnapshotSubscription {
    task_tx: UnboundedSender<Task>,
    subscribe_type: SubscribeType,
    worker_key: WorkerKey,
    sender: UnboundedSender<Notification>,
    unread_parts: HashSet<SnapshotPart>,
}

impl SnapshotSubscription {
    /// Start forwarding notifications about the part of `S`.
    pub fn start_forwarding<S: SnapshotPartState>(&mut self, _state: &S) {
        self.start_forwarding_part(S::PART);
    }

    fn start_forwarding_part(&mut self, part: SnapshotPart) {
        assert!(
            self.unread_parts.remove(&part),
            "snapshot part {part:?} is unexpected or read twice"
        );
        self.task_tx
            .send(Task::StartForwarding {
                subscribe_type: self.subscribe_type,
                worker_key: self.worker_key.clone(),
                sender: self.sender.clone(),
                part,
            })
            .unwrap();
    }

    /// Queue the snapshot, so that the observer manager receives the notifications published
    /// before it first and applies them together with it.
    pub fn send_snapshot(self, snapshot: MetaSnapshot) {
        assert!(
            self.unread_parts.is_empty(),
            "snapshot parts not read: {:?}",
            self.unread_parts
        );
        self.task_tx
            .send(Task::SendSnapshot {
                sender: self.sender,
                snapshot,
            })
            .unwrap();
    }
}

/// [`NotificationManager`] is used to send notification to frontends and compute nodes.
pub struct NotificationManager {
    core: Arc<parking_lot::Mutex<NotificationManagerCore>>,
    /// Sender used to add a notification into the waiting queue.
    task_tx: UnboundedSender<Task>,
    /// The current notification version generator.
    version_generator: Mutex<NotificationVersionGenerator>,
}

impl NotificationManager {
    pub async fn new(meta_store_impl: SqlMetaStore) -> Self {
        // notification waiting queue.
        let (task_tx, mut task_rx) = mpsc::unbounded_channel::<Task>();
        let core = Arc::new(parking_lot::Mutex::new(NotificationManagerCore::new()));
        let core_clone = core.clone();
        let version_generator = NotificationVersionGenerator::new(meta_store_impl)
            .await
            .unwrap();
        let mut latest_version = version_generator.current_version();

        tokio::spawn(async move {
            while let Some(task) = task_rx.recv().await {
                match task {
                    Task::Notify {
                        target,
                        operation,
                        info,
                        version,
                    } => {
                        if let Some(version) = version {
                            latest_version = version;
                        }
                        let response = SubscribeResponse {
                            status: None,
                            operation: operation as i32,
                            info: Some(info),
                            version: latest_version,
                        };
                        core.lock().notify(target, response);
                    }
                    Task::StartForwarding {
                        subscribe_type,
                        worker_key,
                        sender,
                        part,
                    } => {
                        core.lock()
                            .start_forwarding(subscribe_type, worker_key, sender, part);
                    }
                    Task::SendSnapshot { sender, snapshot } => {
                        let response = SubscribeResponse {
                            status: None,
                            operation: Operation::Snapshot as i32,
                            info: Some(Info::Snapshot(snapshot)),
                            version: Default::default(),
                        };
                        let _ = sender.send(Ok(response)).inspect_err(|err| {
                            tracing::warn!("Failed to send snapshot: {}", err.as_report());
                        });
                    }
                }
            }
        });

        Self {
            core: core_clone,
            task_tx,
            version_generator: Mutex::new(version_generator),
        }
    }

    pub fn abort_all(&self) {
        let mut guard = self.core.lock();
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
        let task = Task::Notify {
            target,
            operation,
            info,
            version,
        };
        self.task_tx.send(task).unwrap();
    }

    /// Assign consecutive versions to a frontend notification batch and enqueue the complete batch
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

    /// Add a notification that no snapshot part covers, using the latest version published to the
    /// queue without incrementing it.
    #[inline(always)]
    fn notify_without_version(&self, target: Target, operation: Operation, info: Info) {
        assert_eq!(SnapshotPart::of(&info), None);
        self.notify(target, operation, info, None);
    }

    /// Notify subscribers of a change to the snapshot part of `S`, without incrementing the
    /// version.
    pub fn notify_part<S: SnapshotPartState>(
        &self,
        state: &mut S,
        subscribe_type: SubscribeType,
        operation: Operation,
        info: Info,
    ) {
        self.notify_part_target(state, subscribe_type.into(), operation, info);
    }

    fn notify_part_target<S: SnapshotPartState>(
        &self,
        _state: &mut S,
        target: Target,
        operation: Operation,
        info: Info,
    ) {
        assert_eq!(SnapshotPart::of(&info), Some(S::PART));
        assert!(
            S::PART != SnapshotPart::Catalog || target.subscribe_type != SubscribeType::Frontend,
            "catalog notifications to frontends must increase the version"
        );
        self.notify(target, operation, info, None);
    }

    /// Like [`Self::notify_part`], for every type of subscribers.
    pub fn notify_part_to_all<S: SnapshotPartState>(
        &self,
        state: &mut S,
        operation: Operation,
        info: Info,
    ) {
        for subscribe_type in [
            SubscribeType::Frontend,
            SubscribeType::Hummock,
            SubscribeType::Compactor,
            SubscribeType::Compute,
        ] {
            self.notify_part(state, subscribe_type, operation, info.clone());
        }
    }

    pub(crate) fn notify_hummock_targeted_update<S: SnapshotPartState>(
        &self,
        state: &mut S,
        worker_key: WorkerKey,
        info: Info,
    ) {
        self.notify_part_target(
            state,
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
            WorkerType::Frontend => core_guard.frontend_subscribers.remove(&worker_key),
            WorkerType::ComputeNode | WorkerType::RiseCtl => {
                core_guard.hummock_subscribers.remove(&worker_key)
            }
            WorkerType::Compactor => core_guard.compactor_subscribers.remove(&worker_key),
            _ => unreachable!(),
        };
    }

    /// Register a subscriber before reading its snapshot. Notifications about the parts in its
    /// snapshot are dropped until [`SnapshotSubscription::start_forwarding`] is reached in the queue.
    pub fn start_subscription(
        &self,
        subscribe_type: SubscribeType,
        worker_key: WorkerKey,
        sender: UnboundedSender<Notification>,
    ) -> SnapshotSubscription {
        let unread_parts = SnapshotPart::of_subscriber(subscribe_type);
        let mut core_guard = self.core.lock();
        if core_guard.exiting {
            tracing::warn!("notification manager exiting.");
        } else {
            core_guard.subscribers_of(subscribe_type).insert(
                worker_key.clone(),
                Subscriber {
                    sender: sender.clone(),
                    not_forwarded: unread_parts.clone(),
                },
            );
        }
        drop(core_guard);
        SnapshotSubscription {
            task_tx: self.task_tx.clone(),
            subscribe_type,
            worker_key,
            sender,
            unread_parts,
        }
    }

    /// Register a subscriber as if every part of its snapshot had been read.
    #[cfg(any(test, feature = "test"))]
    pub fn insert_sender(
        &self,
        subscribe_type: SubscribeType,
        worker_key: WorkerKey,
        sender: UnboundedSender<Notification>,
    ) -> SnapshotSubscription {
        let mut subscription = self.start_subscription(subscribe_type, worker_key, sender);
        for part in subscription.unread_parts.clone() {
            subscription.start_forwarding_part(part);
        }
        subscription
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

struct Subscriber {
    sender: UnboundedSender<Notification>,
    /// The parts in its snapshot that are not forwarded yet.
    not_forwarded: HashSet<SnapshotPart>,
}

impl Subscriber {
    fn forwards(&self, part: Option<SnapshotPart>) -> bool {
        part.is_none_or(|part| !self.not_forwarded.contains(&part))
    }
}

type SubscriberMap = HashMap<WorkerKey, Subscriber>;

struct NotificationManagerCore {
    /// The subscribers of frontends.
    frontend_subscribers: SubscriberMap,
    /// The subscribers of nodes that subscribe the hummock.
    hummock_subscribers: SubscriberMap,
    /// The subscribers of compactor nodes.
    compactor_subscribers: SubscriberMap,
    /// The subscribers of compute nodes.
    compute_subscribers: SubscriberMap,
    /// The notification sender to local subscribers.
    local_senders: Vec<UnboundedSender<LocalNotification>>,
    exiting: bool,
}

impl NotificationManagerCore {
    fn new() -> Self {
        Self {
            frontend_subscribers: HashMap::new(),
            hummock_subscribers: HashMap::new(),
            compactor_subscribers: HashMap::new(),
            compute_subscribers: HashMap::new(),
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

        let Target {
            subscribe_type,
            worker_key,
        } = target;
        let part = response.info.as_ref().and_then(SnapshotPart::of);
        let subscribers = self.subscribers_of(subscribe_type);

        if let Some(worker_key) = worker_key {
            match subscribers.entry(worker_key.clone()) {
                Entry::Occupied(entry) => {
                    if !entry.get().forwards(part) {
                        return;
                    }
                    let _ = entry.get().sender.send(Ok(response)).inspect_err(|err| {
                        warn_send_failure!(subscribe_type, &worker_key, err.as_report());
                        entry.remove_entry();
                    });
                }
                Entry::Vacant(_) => {
                    tracing::warn!("Failed to find notification sender of {:?}", worker_key)
                }
            }
        } else {
            subscribers.retain(|worker_key, subscriber| {
                !subscriber.forwards(part)
                    || subscriber
                        .sender
                        .send(Ok(response.clone()))
                        .inspect_err(|err| {
                            warn_send_failure!(subscribe_type, &worker_key, err.as_report());
                        })
                        .is_ok()
            });
        }
    }

    fn start_forwarding(
        &mut self,
        subscribe_type: SubscribeType,
        worker_key: WorkerKey,
        sender: UnboundedSender<Notification>,
        part: SnapshotPart,
    ) {
        if let Some(subscriber) = self.subscribers_of(subscribe_type).get_mut(&worker_key)
            && subscriber.sender.same_channel(&sender)
        {
            subscriber.not_forwarded.remove(&part);
        }
    }

    fn subscribers_of(&mut self, subscribe_type: SubscribeType) -> &mut SubscriberMap {
        match subscribe_type {
            SubscribeType::Frontend => &mut self.frontend_subscribers,
            SubscribeType::Hummock => &mut self.hummock_subscribers,
            SubscribeType::Compactor => &mut self.compactor_subscribers,
            SubscribeType::Compute => &mut self.compute_subscribers,
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

    struct TestCatalog;
    impl SnapshotPartState for TestCatalog {
        const PART: SnapshotPart = SnapshotPart::Catalog;
    }

    struct TestCluster;
    impl SnapshotPartState for TestCluster {
        const PART: SnapshotPart = SnapshotPart::Cluster;
    }

    struct TestServing;
    impl SnapshotPartState for TestServing {
        const PART: SnapshotPart = SnapshotPart::ServingMappings;
    }

    fn worker_key(port: i32) -> WorkerKey {
        WorkerKey(HostAddress {
            host: "a".to_owned(),
            port,
        })
    }

    fn subscribe(
        mgr: &NotificationManager,
        subscribe_type: SubscribeType,
    ) -> (SnapshotSubscription, mpsc::UnboundedReceiver<Notification>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            mgr.start_subscription(subscribe_type, worker_key(1), tx),
            rx,
        )
    }

    async fn publish_catalog_change(mgr: &NotificationManager) -> NotificationVersion {
        let mut batch = FrontendNotificationBatch::new(mgr);
        batch.add(Operation::Add, Info::Database(Default::default()));
        batch.publish(&mut TestCatalog).await.unwrap()
    }

    fn node() -> Info {
        Info::Node(Default::default())
    }

    fn notify_node(mgr: &NotificationManager, operation: Operation) {
        mgr.notify_part(&mut TestCluster, SubscribeType::Frontend, operation, node());
    }

    async fn next_notification(
        rx: &mut mpsc::UnboundedReceiver<Notification>,
    ) -> SubscribeResponse {
        rx.recv().await.unwrap().unwrap()
    }

    /// Checks that the next notification is one queued after everything queued so far.
    async fn assert_nothing_more(
        mgr: &NotificationManager,
        rx: &mut mpsc::UnboundedReceiver<Notification>,
    ) {
        mgr.notify_frontend_without_version(Operation::Update, Info::Recovery(Default::default()));
        assert!(matches!(
            next_notification(rx).await.info,
            Some(Info::Recovery(_))
        ));
    }

    #[tokio::test]
    async fn test_multiple_subscribers_one_worker() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        let (tx3, mut rx3) = mpsc::unbounded_channel();
        let subscription = mgr.insert_sender(SubscribeType::Hummock, worker_key(1), tx1);
        mgr.insert_sender(SubscribeType::Frontend, worker_key(1), tx2);
        mgr.insert_sender(SubscribeType::Frontend, worker_key(2), tx3);
        subscription.send_snapshot(MetaSnapshot::default());
        assert!(rx1.recv().await.is_some());
        assert!(rx2.try_recv().is_err());
        assert!(rx3.try_recv().is_err());

        let version = publish_catalog_change(&mgr).await;
        assert!(rx1.try_recv().is_err());
        let frontend_notification = rx2.recv().await.unwrap().unwrap();
        assert_eq!(frontend_notification.version, version);
        assert_eq!(
            rx3.recv().await.unwrap().unwrap().version,
            frontend_notification.version
        );

        mgr.notify_part(
            &mut TestCatalog,
            SubscribeType::Hummock,
            Operation::Add,
            Info::Database(Default::default()),
        );
        assert_eq!(
            rx1.recv().await.unwrap().unwrap().version,
            frontend_notification.version
        );
    }

    #[tokio::test]
    async fn test_frontend_notification_batch_assigns_consecutive_versions() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        mgr.insert_sender(SubscribeType::Frontend, worker_key(1), tx);

        let initial_version = mgr.current_version().await;
        let mut batch = FrontendNotificationBatch::new(&mgr);
        batch.add(Operation::Add, Info::Database(Default::default()));
        batch.add(Operation::Add, Info::Schema(Default::default()));
        let published_version = batch
            .publish(&mut TestCatalog)
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
        let (hummock_tx1, mut hummock_rx1) = mpsc::unbounded_channel();
        let (hummock_tx2, mut hummock_rx2) = mpsc::unbounded_channel();
        let (frontend_tx1, mut frontend_rx1) = mpsc::unbounded_channel();
        mgr.insert_sender(SubscribeType::Hummock, worker_key(1), hummock_tx1);
        mgr.insert_sender(SubscribeType::Hummock, worker_key(2), hummock_tx2);
        mgr.insert_sender(SubscribeType::Frontend, worker_key(1), frontend_tx1);

        let version = mgr.current_version().await;
        mgr.notify_hummock_targeted_update(
            &mut TestCatalog,
            worker_key(1),
            Info::Database(Default::default()),
        );

        let notification = hummock_rx1.recv().await.unwrap().unwrap();
        assert_eq!(notification.version, version);
        assert_eq!(mgr.current_version().await, version);
        assert!(hummock_rx2.try_recv().is_err());
        assert!(frontend_rx1.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_snapshot_uses_original_sender_after_resubscribe() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let subscription = mgr.insert_sender(SubscribeType::Hummock, worker_key(1), tx1);
        subscription.send_snapshot(MetaSnapshot::default());

        let (tx2, mut rx2) = mpsc::unbounded_channel();
        mgr.insert_sender(SubscribeType::Hummock, worker_key(1), tx2);

        assert!(rx1.recv().await.is_some());
        assert!(rx2.try_recv().is_err());

        let version = mgr.current_version().await;
        mgr.notify_part(
            &mut TestCatalog,
            SubscribeType::Hummock,
            Operation::Add,
            Info::Database(Default::default()),
        );
        assert!(rx1.try_recv().is_err());
        let notification = rx2.recv().await.unwrap().unwrap();
        assert_eq!(notification.version, version);
        assert_eq!(mgr.current_version().await, version);
    }

    #[tokio::test]
    async fn test_snapshot_is_queued_behind_forwarded_notifications() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let subscription = mgr.insert_sender(SubscribeType::Frontend, worker_key(1), tx);

        // The test runtime has one thread and publishing does not yield after queueing, so the
        // notification is still in the queue when the snapshot is sent.
        let version = publish_catalog_change(&mgr).await;
        subscription.send_snapshot(MetaSnapshot::default());

        let first = next_notification(&mut rx).await;
        assert!(matches!(first.info, Some(Info::Database(_))));
        assert_eq!(first.version, version);
        assert!(matches!(
            next_notification(&mut rx).await.info,
            Some(Info::Snapshot(_))
        ));
    }

    #[tokio::test]
    async fn test_table_refill_config_follows_the_part_of_its_payload() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let (mut subscription, mut rx) = subscribe(&mgr, SubscribeType::Hummock);
        let serving_config = || {
            Info::TableRefillRuntimeConfig(risingwave_pb::meta::PbTableRefillRuntimeConfig {
                serving_table_vnode_mappings: Some(Default::default()),
                ..Default::default()
            })
        };

        subscription.start_forwarding(&TestCatalog);
        mgr.notify_hummock_targeted_update(&mut TestServing, worker_key(1), serving_config());
        subscription.start_forwarding(&TestServing);
        mgr.notify_hummock_targeted_update(&mut TestServing, worker_key(1), serving_config());
        mgr.notify_hummock_targeted_update(
            &mut TestCatalog,
            worker_key(1),
            Info::TableRefillRuntimeConfig(Default::default()),
        );

        for serving in [true, false] {
            let Some(Info::TableRefillRuntimeConfig(config)) =
                next_notification(&mut rx).await.info
            else {
                panic!("expect a table refill runtime config");
            };
            assert_eq!(config.serving_table_vnode_mappings.is_some(), serving);
        }
    }

    #[tokio::test]
    async fn test_each_part_starts_forwarding_on_its_own() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let (mut subscription, mut rx) = subscribe(&mgr, SubscribeType::Frontend);
        let notify_mapping = || {
            mgr.notify_part(
                &mut TestServing,
                SubscribeType::Frontend,
                Operation::Add,
                Info::ServingWorkerSlotMappings(Default::default()),
            )
        };

        subscription.start_forwarding(&TestCluster);
        notify_mapping();
        notify_node(&mgr, Operation::Add);
        assert!(matches!(
            next_notification(&mut rx).await.info,
            Some(Info::Node(_))
        ));
        assert_nothing_more(&mgr, &mut rx).await;

        subscription.start_forwarding(&TestServing);
        notify_mapping();
        assert!(matches!(
            next_notification(&mut rx).await.info,
            Some(Info::ServingWorkerSlotMappings(_))
        ));
    }

    #[tokio::test]
    async fn test_start_forwarding_is_ignored_after_resubscribe() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let (mut old_subscription, _old_rx) = subscribe(&mgr, SubscribeType::Frontend);
        let (mut subscription, mut rx) = subscribe(&mgr, SubscribeType::Frontend);

        old_subscription.start_forwarding(&TestCluster);
        notify_node(&mgr, Operation::Add);
        subscription.start_forwarding(&TestCluster);
        notify_node(&mgr, Operation::Delete);

        assert_eq!(
            next_notification(&mut rx).await.operation(),
            Operation::Delete
        );
        assert_nothing_more(&mgr, &mut rx).await;
    }

    #[tokio::test]
    async fn test_catalog_snapshot_version_splits_notifications() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let (mut subscription, mut rx) = subscribe(&mgr, SubscribeType::Frontend);

        publish_catalog_change(&mgr).await;
        subscription.start_forwarding(&TestCatalog);
        let catalog_version = mgr.current_version().await;
        let forwarded = publish_catalog_change(&mgr).await;

        assert_eq!(forwarded, catalog_version + 1);
        assert_eq!(next_notification(&mut rx).await.version, forwarded);
        assert_nothing_more(&mgr, &mut rx).await;
    }

    #[tokio::test]
    #[should_panic(expected = "assertion `left == right` failed")]
    async fn test_snapshot_part_cannot_be_notified_without_its_lock() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;

        mgr.notify_frontend_without_version(Operation::Add, node());
    }

    #[tokio::test]
    #[should_panic(expected = "assertion `left == right` failed")]
    async fn test_notification_needs_the_lock_of_its_part() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;

        mgr.notify_part(
            &mut TestCatalog,
            SubscribeType::Frontend,
            Operation::Add,
            node(),
        );
    }

    #[tokio::test]
    #[should_panic(expected = "snapshot parts not read")]
    async fn test_snapshot_requires_every_part() {
        let mgr = NotificationManager::new(SqlMetaStore::for_test().await).await;
        let (subscription, _rx) = subscribe(&mgr, SubscribeType::Frontend);

        subscription.send_snapshot(MetaSnapshot::default());
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
