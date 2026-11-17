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

use futures_async_stream::try_stream;
use risingwave_common::id::JobId;
use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_meta_model::ObjectId;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{MetaSnapshot, SubscribeResponse, SubscribeType};
use thiserror_ext::AsReport;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tonic::Status;

use crate::MetaResult;
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

/// A state domain whose notifications are ordered against its initial snapshot.
/// Each subscriber reads the fields it consumes from that domain and emits one marker for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NotificationPart {
    Catalog,
    Cluster,
    StreamingMappings,
    ServingMappings,
    HummockVersion,
    HummockWriteLimits,
    SessionParams,
}

impl NotificationPart {
    pub fn for_subscriber(subscribe_type: SubscribeType) -> &'static [Self] {
        use NotificationPart::*;
        match subscribe_type {
            SubscribeType::Frontend => &[
                Catalog,
                Cluster,
                StreamingMappings,
                ServingMappings,
                HummockVersion,
                SessionParams,
            ],
            SubscribeType::Hummock => &[
                Catalog,
                Cluster,
                ServingMappings,
                HummockVersion,
                HummockWriteLimits,
            ],
            SubscribeType::Compactor | SubscribeType::Compute => &[Catalog, Cluster],
            SubscribeType::Unspecified => unreachable!(),
        }
    }

    /// The part a notification changes. Subscribers always keep notifications of `None`.
    pub fn of(info: &Info) -> Option<Self> {
        use NotificationPart::*;
        let part = match info {
            Info::Database(_)
            | Info::Schema(_)
            | Info::ObjectGroup(_)
            | Info::User(_)
            | Info::Connection(_)
            | Info::Secret(_)
            | Info::Function(_) => Catalog,
            Info::Node(_) | Info::ClusterResource(_) => Cluster,
            Info::StreamingWorkerSlotMapping(_) => StreamingMappings,
            Info::ServingWorkerSlotMappings(_) => ServingMappings,
            Info::HummockVersionDeltas(_) => HummockVersion,
            Info::HummockWriteLimits(_) => HummockWriteLimits,
            Info::SessionParam(_) => SessionParams,
            Info::TableRefillRuntimeConfig(config) => {
                assert!(
                    config.table_cache_refill_policies.is_none()
                        || config.serving_table_vnode_mappings.is_none(),
                    "refill deltas must belong to a single notification part"
                );
                if config.serving_table_vnode_mappings.is_some() {
                    ServingMappings
                } else {
                    Catalog
                }
            }
            // Manifest IDs are monotonic refresh hints; the receiver ignores older IDs.
            // System params are absent from MetaSnapshot and periodically sent in full.
            Info::MetaBackupManifestId(_)
            | Info::HummockStats(_)
            | Info::Recovery(_)
            | Info::SystemParams(_) => return None,
            Info::Snapshot(_) => unreachable!("initial snapshots bypass delta publishing"),
        };
        Some(part)
    }
}

/// The state behind the lock of a [`NotificationPart`]. It is only reachable through a guard of
/// that lock, so `&Self` proves that the lock is held, and `&mut Self` that it is held for writing.
pub trait NotificationPartState {
    const PART: NotificationPart;
}

/// Internal channel events. Snapshot payloads stay in the snapshot-building methods.
#[derive(Clone, Debug)]
pub enum SubscriptionEvent {
    SnapshotMarker(NotificationPart),
    Delta {
        part: NotificationPart,
        response: SubscribeResponse,
    },
    /// Notifications without a corresponding snapshot part must always be retained.
    Unscoped(SubscribeResponse),
}

impl SubscriptionEvent {
    fn delta(response: SubscribeResponse) -> Self {
        let info = response
            .info
            .as_ref()
            .expect("notification must contain info");
        match NotificationPart::of(info) {
            Some(part) => Self::Delta { part, response },
            None => Self::Unscoped(response),
        }
    }

    /// Convert a live channel event, rejecting unexpected initialization markers.
    pub fn into_notification(self) -> Notification {
        match self {
            Self::Delta { response, .. } | Self::Unscoped(response) => Ok(response),
            Self::SnapshotMarker(part) => Err(Status::internal(format!(
                "unexpected snapshot marker for {part:?}"
            ))),
        }
    }
}

/// Marks reads on this exact subscription's channel.
pub struct SnapshotMarkerSender {
    sender: UnboundedSender<SubscriptionEvent>,
    marked: parking_lot::Mutex<HashSet<NotificationPart>>,
}

impl SnapshotMarkerSender {
    /// Call after reading every field of the part from `state`, before releasing its guard.
    pub fn snapshot_read<S: NotificationPartState>(&self, _state: &S) {
        self.mark(S::PART);
    }

    /// Mark a part whose snapshot fields are constants rather than state read under its lock.
    #[cfg(any(test, feature = "test"))]
    pub fn mark_constant_part(&self, part: NotificationPart) {
        self.mark(part);
    }

    fn mark(&self, part: NotificationPart) {
        assert!(
            self.marked.lock().insert(part),
            "snapshot part {part:?} read twice"
        );
        // A closed receiver means the subscription has already been cancelled.
        let _ = self.sender.send(SubscriptionEvent::SnapshotMarker(part));
    }

    /// Check that the snapshot read every part of its subscriber, so the stream does not wait
    /// for a marker that never comes.
    pub fn finish(self, subscribe_type: SubscribeType) {
        let expected: HashSet<_> = NotificationPart::for_subscriber(subscribe_type)
            .iter()
            .copied()
            .collect();
        assert_eq!(
            self.marked.into_inner(),
            expected,
            "snapshot parts read for {subscribe_type:?}"
        );
    }
}

/// Emit the assembled snapshot first, then post-marker deltas in their original channel order.
/// This does no snapshot reads and retains no state locks across stream yields.
#[try_stream(ok = SubscribeResponse, error = Status)]
pub async fn notification_stream(
    subscribe_type: SubscribeType,
    mut receiver: UnboundedReceiver<SubscriptionEvent>,
    snapshot: MetaSnapshot,
) {
    let mut remaining_parts = NotificationPart::for_subscriber(subscribe_type)
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let mut buffered = Vec::new();
    while !remaining_parts.is_empty() {
        match receiver.recv().await.ok_or_else(|| {
            Status::unavailable("notification channel closed before all snapshot markers")
        })? {
            SubscriptionEvent::SnapshotMarker(part) => {
                if !remaining_parts.remove(&part) {
                    Err(Status::internal(format!(
                        "unexpected or duplicate snapshot marker for {part:?}"
                    )))?;
                }
            }
            SubscriptionEvent::Delta { part, response } => {
                if !remaining_parts.contains(&part) {
                    buffered.push(response);
                }
            }
            SubscriptionEvent::Unscoped(response) => buffered.push(response),
        }
    }
    yield SubscribeResponse {
        status: None,
        operation: Operation::Snapshot as i32,
        info: Some(Info::Snapshot(snapshot)),
        // Receivers initialize from the catalog/Hummock versions inside MetaSnapshot.
        version: Default::default(),
    };
    for response in buffered {
        yield response;
    }
    while let Some(event) = receiver.recv().await {
        yield event.into_notification()?;
    }
}

/// Mark the parts of `subscribe_type` that `tx` has not read as constants, then return the stream
/// of the subscription with an empty snapshot.
#[cfg(any(test, feature = "test"))]
pub fn finish_test_subscription(
    tx: SnapshotMarkerSender,
    subscribe_type: SubscribeType,
    receiver: UnboundedReceiver<SubscriptionEvent>,
) -> futures::stream::BoxStream<'static, Notification> {
    use futures::StreamExt;

    let unread = NotificationPart::for_subscriber(subscribe_type)
        .iter()
        .copied()
        .filter(|part| !tx.marked.lock().contains(part))
        .collect::<Vec<_>>();
    for part in unread {
        tx.mark_constant_part(part);
    }
    tx.finish(subscribe_type);
    notification_stream(subscribe_type, receiver, MetaSnapshot::default()).boxed()
}

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
    pub async fn publish<S: NotificationPartState>(
        self,
        _state: &mut S,
    ) -> Option<NotificationVersion> {
        const { assert!(matches!(S::PART, NotificationPart::Catalog)) };
        for (_, info) in &self.pending {
            assert_eq!(NotificationPart::of(info), Some(S::PART));
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

    /// Add a notification that no snapshot part covers, using the latest published version
    /// without incrementing it.
    #[inline(always)]
    fn notify_without_version(&self, target: Target, operation: Operation, info: Info) {
        assert_eq!(NotificationPart::of(&info), None);
        self.notify(target, operation, info, None);
    }

    /// Notify subscribers of a change to the part of `S`, without incrementing the version.
    pub fn notify_part<S: NotificationPartState>(
        &self,
        state: &mut S,
        subscribe_type: SubscribeType,
        operation: Operation,
        info: Info,
    ) {
        self.notify_part_target(state, subscribe_type.into(), operation, info);
    }

    fn notify_part_target<S: NotificationPartState>(
        &self,
        _state: &mut S,
        target: Target,
        operation: Operation,
        info: Info,
    ) {
        assert_eq!(NotificationPart::of(&info), Some(S::PART));
        assert!(
            S::PART != NotificationPart::Catalog
                || target.subscribe_type != SubscribeType::Frontend,
            "catalog notifications to frontends must increase the version"
        );
        self.notify(target, operation, info, None);
    }

    /// Like [`Self::notify_part`], for every type of subscribers.
    pub fn notify_part_to_all<S: NotificationPartState>(
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

    /// Register before reading any snapshot part. Boundaries and deltas share this channel;
    /// filtering is local to the returned subscription, never part of the sender map.
    pub fn start_subscription(
        &self,
        subscribe_type: SubscribeType,
        worker_key: WorkerKey,
    ) -> MetaResult<(SnapshotMarkerSender, UnboundedReceiver<SubscriptionEvent>)> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut core = self.core.lock();
        if core.exiting {
            return Err(anyhow::anyhow!("notification manager exiting").into());
        }
        core.senders_of(subscribe_type)
            .insert(worker_key, sender.clone());
        Ok((
            SnapshotMarkerSender {
                sender,
                marked: Default::default(),
            },
            receiver,
        ))
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

    pub(crate) fn notify_hummock_targeted_update<S: NotificationPartState>(
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
        sender: UnboundedSender<SubscriptionEvent>,
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

type SenderMap = HashMap<WorkerKey, UnboundedSender<SubscriptionEvent>>;

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

        let event = SubscriptionEvent::delta(response);
        let senders = self.senders_of(target.subscribe_type);

        if let Some(worker_key) = target.worker_key {
            match senders.entry(worker_key.clone()) {
                Entry::Occupied(entry) => {
                    let _ = entry.get().send(event).inspect_err(|err| {
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
                    .send(event.clone())
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
    use futures::{StreamExt, TryStreamExt};
    use risingwave_common::id::JobId;
    use risingwave_pb::common::HostAddress;

    use super::*;
    use crate::manager::WorkerKey;

    struct TestCatalog;
    impl NotificationPartState for TestCatalog {
        const PART: NotificationPart = NotificationPart::Catalog;
    }

    struct TestCluster;
    impl NotificationPartState for TestCluster {
        const PART: NotificationPart = NotificationPart::Cluster;
    }

    struct TestServing;
    impl NotificationPartState for TestServing {
        const PART: NotificationPart = NotificationPart::ServingMappings;
    }

    async fn notify_frontend(
        mgr: &NotificationManager,
        operation: Operation,
        info: Info,
    ) -> NotificationVersion {
        let mut batch = FrontendNotificationBatch::new(mgr);
        batch.add(operation, info);
        batch.publish(&mut TestCatalog).await.unwrap()
    }

    fn notify_compute_cluster_resource(mgr: &NotificationManager) {
        mgr.notify_part(
            &mut TestCluster,
            SubscribeType::Compute,
            Operation::Update,
            Info::ClusterResource(Default::default()),
        );
    }

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
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        mgr.insert_sender(SubscribeType::Hummock, worker_key1.clone(), tx1);
        mgr.insert_sender(SubscribeType::Frontend, worker_key1.clone(), tx2);
        mgr.insert_sender(SubscribeType::Frontend, worker_key2, tx3);
        assert!(rx2.try_recv().is_err());
        assert!(rx3.try_recv().is_err());

        let version =
            notify_frontend(&mgr, Operation::Add, Info::Database(Default::default())).await;
        assert!(rx1.try_recv().is_err());
        let frontend_notification = rx2.recv().await.unwrap().into_notification().unwrap();
        assert_eq!(frontend_notification.version, version);
        assert_eq!(
            rx3.recv()
                .await
                .unwrap()
                .into_notification()
                .unwrap()
                .version,
            frontend_notification.version
        );

        mgr.notify_part(
            &mut TestCatalog,
            SubscribeType::Hummock,
            Operation::Add,
            Info::Database(Default::default()),
        );
        assert_eq!(
            rx1.recv()
                .await
                .unwrap()
                .into_notification()
                .unwrap()
                .version,
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
            .publish(&mut TestCatalog)
            .await
            .expect("the test notification batch contains two notifications");

        assert_eq!(published_version, initial_version + 2);
        assert_eq!(mgr.current_version().await, published_version);
        assert_eq!(
            rx.recv()
                .await
                .unwrap()
                .into_notification()
                .unwrap()
                .version,
            initial_version + 1
        );
        assert_eq!(
            rx.recv()
                .await
                .unwrap()
                .into_notification()
                .unwrap()
                .version,
            published_version
        );
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
        mgr.notify_hummock_targeted_update(
            &mut TestCatalog,
            worker_key1,
            Info::Database(Default::default()),
        );

        let notification = hummock_rx1
            .recv()
            .await
            .unwrap()
            .into_notification()
            .unwrap();
        assert_eq!(notification.version, version);
        assert_eq!(mgr.current_version().await, version);
        assert!(hummock_rx2.try_recv().is_err());
        assert!(frontend_rx1.try_recv().is_err());
    }

    fn key() -> WorkerKey {
        WorkerKey(HostAddress {
            host: "a".to_owned(),
            port: 1,
        })
    }

    fn delta(tx: &SnapshotMarkerSender, part: NotificationPart, version: u64) {
        tx.sender
            .send(SubscriptionEvent::Delta {
                part,
                response: SubscribeResponse {
                    version,
                    ..Default::default()
                },
            })
            .unwrap();
    }

    fn complete_markers(tx: &SnapshotMarkerSender, subscribe_type: SubscribeType) {
        for &part in NotificationPart::for_subscriber(subscribe_type) {
            tx.mark_constant_part(part);
        }
    }

    #[tokio::test]
    async fn test_snapshot_markers_filter_each_part_and_preserve_fifo() {
        let mgr = Arc::new(NotificationManager::new(SqlMetaStore::for_test().await).await);
        let (tx, rx) = mgr
            .start_subscription(SubscribeType::Compute, key())
            .unwrap();
        delta(&tx, NotificationPart::Catalog, 1);
        delta(&tx, NotificationPart::Cluster, 2);
        tx.mark_constant_part(NotificationPart::Catalog);
        delta(&tx, NotificationPart::Catalog, 3);
        delta(&tx, NotificationPart::Cluster, 4);
        tx.sender
            .send(SubscriptionEvent::Unscoped(SubscribeResponse {
                version: 5,
                ..Default::default()
            }))
            .unwrap();
        tx.mark_constant_part(NotificationPart::Cluster);
        // This is already in the channel when initialization finishes, but behind its last marker.
        delta(&tx, NotificationPart::Cluster, 6);
        drop(tx);
        let mut stream = Box::pin(notification_stream(
            SubscribeType::Compute,
            rx,
            MetaSnapshot::default(),
        ));
        assert!(matches!(
            stream.next().await.unwrap().unwrap().info,
            Some(Info::Snapshot(_))
        ));
        for version in [3, 5, 6] {
            assert_eq!(stream.next().await.unwrap().unwrap().version, version);
        }
        // Equal catalog versions must not cause a newer unversioned state update to be dropped.
        notify_compute_cluster_resource(&mgr);
        assert!(matches!(
            stream.next().await.unwrap().unwrap().info,
            Some(Info::ClusterResource(_))
        ));
    }

    #[tokio::test]
    async fn test_snapshot_filters_published_catalog_notifications() {
        let mgr = Arc::new(NotificationManager::new(SqlMetaStore::for_test().await).await);
        let (tx, rx) = mgr
            .start_subscription(SubscribeType::Frontend, key())
            .unwrap();
        let before =
            notify_frontend(&mgr, Operation::Add, Info::Database(Default::default())).await;
        tx.mark_constant_part(NotificationPart::Catalog);
        let after = notify_frontend(&mgr, Operation::Add, Info::Schema(Default::default())).await;
        for &part in NotificationPart::for_subscriber(SubscribeType::Frontend) {
            if part != NotificationPart::Catalog {
                tx.mark_constant_part(part);
            }
        }
        let mut stream = Box::pin(notification_stream(
            SubscribeType::Frontend,
            rx,
            MetaSnapshot {
                version: Some(risingwave_pb::meta::meta_snapshot::SnapshotVersion {
                    catalog_version: before,
                    ..Default::default()
                }),
                ..Default::default()
            },
        ));
        assert!(matches!(
            stream.next().await.unwrap().unwrap().info,
            Some(Info::Snapshot(_))
        ));
        let notification = stream.next().await.unwrap().unwrap();
        assert_eq!(notification.version, after);
        assert!(matches!(notification.info, Some(Info::Schema(_))));
    }

    #[tokio::test]
    async fn test_snapshot_stream_preserves_cross_part_delta_order() {
        let mgr = Arc::new(NotificationManager::new(SqlMetaStore::for_test().await).await);
        let (tx, rx) = mgr
            .start_subscription(SubscribeType::Frontend, key())
            .unwrap();
        tx.mark_constant_part(NotificationPart::Catalog);
        tx.mark_constant_part(NotificationPart::Cluster);
        delta(&tx, NotificationPart::Cluster, 10);
        delta(&tx, NotificationPart::Catalog, 11);
        delta(&tx, NotificationPart::Cluster, 12);
        for part in [
            NotificationPart::StreamingMappings,
            NotificationPart::ServingMappings,
            NotificationPart::HummockVersion,
            NotificationPart::SessionParams,
        ] {
            tx.mark_constant_part(part);
        }
        let mut stream = Box::pin(notification_stream(
            SubscribeType::Frontend,
            rx,
            MetaSnapshot::default(),
        ));
        stream.next().await.unwrap().unwrap();
        for version in [10, 11, 12] {
            assert_eq!(stream.next().await.unwrap().unwrap().version, version);
        }
    }

    #[tokio::test]
    async fn test_snapshot_markers_reject_duplicate_or_unexpected_parts() {
        let mgr = Arc::new(NotificationManager::new(SqlMetaStore::for_test().await).await);
        for invalid in [NotificationPart::Catalog, NotificationPart::HummockVersion] {
            let (tx, rx) = mgr
                .start_subscription(SubscribeType::Compute, key())
                .unwrap();
            tx.mark_constant_part(NotificationPart::Catalog);
            // Send the marker directly, bypassing the sender's own check against duplicates.
            tx.sender
                .send(SubscriptionEvent::SnapshotMarker(invalid))
                .unwrap();
            let mut stream = Box::pin(notification_stream(
                SubscribeType::Compute,
                rx,
                MetaSnapshot::default(),
            ));
            assert_eq!(
                stream.next().await.unwrap().unwrap_err().code(),
                tonic::Code::Internal
            );
        }
        notify_compute_cluster_resource(&mgr);
        assert!(mgr.core.lock().compute_senders.is_empty());
    }

    #[tokio::test]
    async fn test_snapshot_subscription_after_abort_fails() {
        let mgr = Arc::new(NotificationManager::new(SqlMetaStore::for_test().await).await);
        mgr.abort_all();
        assert!(
            mgr.start_subscription(SubscribeType::Compute, key())
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_snapshot_operation_for_serving_is_a_delta() {
        let mgr = Arc::new(NotificationManager::new(SqlMetaStore::for_test().await).await);
        let (tx, rx) = mgr
            .start_subscription(SubscribeType::Frontend, key())
            .unwrap();
        let notify_mapping_snapshot = || {
            mgr.notify_part(
                &mut TestServing,
                SubscribeType::Frontend,
                Operation::Snapshot,
                Info::ServingWorkerSlotMappings(Default::default()),
            )
        };
        notify_mapping_snapshot();
        tx.mark_constant_part(NotificationPart::ServingMappings);
        notify_mapping_snapshot();
        for &part in NotificationPart::for_subscriber(SubscribeType::Frontend) {
            if part != NotificationPart::ServingMappings {
                tx.mark_constant_part(part);
            }
        }
        let mut stream = Box::pin(notification_stream(
            SubscribeType::Frontend,
            rx,
            MetaSnapshot::default(),
        ));
        assert!(matches!(
            stream.next().await.unwrap().unwrap().info,
            Some(Info::Snapshot(_))
        ));
        let notification = stream.next().await.unwrap().unwrap();
        assert_eq!(notification.operation(), Operation::Snapshot);
        assert!(matches!(
            notification.info,
            Some(Info::ServingWorkerSlotMappings(_))
        ));
    }

    #[tokio::test]
    async fn test_snapshot_stream_requires_all_markers() {
        let mgr = Arc::new(NotificationManager::new(SqlMetaStore::for_test().await).await);
        let (tx, rx) = mgr
            .start_subscription(SubscribeType::Compute, key())
            .unwrap();
        tx.mark_constant_part(NotificationPart::Catalog);
        drop(tx);
        mgr.abort_all();
        let mut stream = Box::pin(notification_stream(
            SubscribeType::Compute,
            rx,
            MetaSnapshot::default(),
        ));
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::Unavailable
        );
    }

    #[tokio::test]
    async fn test_snapshot_uses_original_sender_after_resubscribe() {
        let mgr = Arc::new(NotificationManager::new(SqlMetaStore::for_test().await).await);
        let (old_tx, old_rx) = mgr
            .start_subscription(SubscribeType::Compute, key())
            .unwrap();
        let (new_tx, new_rx) = mgr
            .start_subscription(SubscribeType::Compute, key())
            .unwrap();
        complete_markers(&old_tx, SubscribeType::Compute);
        complete_markers(&new_tx, SubscribeType::Compute);
        drop(old_tx);
        drop(new_tx);
        let mut old_stream = Box::pin(notification_stream(
            SubscribeType::Compute,
            old_rx,
            MetaSnapshot::default(),
        ));
        assert!(matches!(
            old_stream.next().await.unwrap().unwrap().info,
            Some(Info::Snapshot(_))
        ));
        assert!(old_stream.next().await.is_none());
        drop(old_stream);
        assert_eq!(mgr.core.lock().compute_senders.len(), 1);
        notify_compute_cluster_resource(&mgr);
        let mut new_stream = Box::pin(notification_stream(
            SubscribeType::Compute,
            new_rx,
            MetaSnapshot::default(),
        ));
        assert!(matches!(
            new_stream.next().await.unwrap().unwrap().info,
            Some(Info::Snapshot(_))
        ));
        assert!(matches!(
            new_stream.next().await.unwrap().unwrap().info,
            Some(Info::ClusterResource(_))
        ));
        drop(new_stream);
        notify_compute_cluster_resource(&mgr);
        assert!(mgr.core.lock().compute_senders.is_empty());
    }

    #[tokio::test]
    async fn test_cancelled_subscription_is_removed_on_next_notification() {
        let mgr = Arc::new(NotificationManager::new(SqlMetaStore::for_test().await).await);
        let (tx, rx) = mgr
            .start_subscription(SubscribeType::Compute, key())
            .unwrap();
        drop(rx);
        assert_eq!(mgr.core.lock().compute_senders.len(), 1);
        // Reading a snapshot after cancellation does not need to expose a send error.
        tx.mark_constant_part(NotificationPart::Catalog);
        notify_compute_cluster_resource(&mgr);
        assert!(mgr.core.lock().compute_senders.is_empty());
    }

    #[tokio::test]
    async fn test_snapshot_filters_refill_notifications_by_part() {
        let mgr = Arc::new(NotificationManager::new(SqlMetaStore::for_test().await).await);
        let (tx, rx) = mgr
            .start_subscription(SubscribeType::Hummock, key())
            .unwrap();
        tx.mark_constant_part(NotificationPart::Catalog);
        mgr.notify_part(
            &mut TestCatalog,
            SubscribeType::Hummock,
            Operation::Update,
            Info::TableRefillRuntimeConfig(risingwave_pb::meta::PbTableRefillRuntimeConfig {
                table_cache_refill_policies: Some(Default::default()),
                ..Default::default()
            }),
        );
        mgr.notify_part(
            &mut TestServing,
            SubscribeType::Hummock,
            Operation::Update,
            Info::TableRefillRuntimeConfig(risingwave_pb::meta::PbTableRefillRuntimeConfig {
                serving_table_vnode_mappings: Some(Default::default()),
                ..Default::default()
            }),
        );
        for part in [
            NotificationPart::Cluster,
            NotificationPart::ServingMappings,
            NotificationPart::HummockVersion,
            NotificationPart::HummockWriteLimits,
        ] {
            tx.mark_constant_part(part);
        }
        let stream = notification_stream(SubscribeType::Hummock, rx, MetaSnapshot::default());
        let responses = Box::pin(stream)
            .take(2)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let Some(Info::TableRefillRuntimeConfig(config)) = &responses[1].info else {
            panic!()
        };
        assert!(config.table_cache_refill_policies.is_some());
        assert!(config.serving_table_vnode_mappings.is_none());
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
