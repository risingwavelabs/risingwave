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

use std::time::Duration;

use risingwave_pb::meta::subscribe_response::Info;
use risingwave_pb::meta::{SubscribeResponse, SubscribeType};
use risingwave_rpc_client::MetaClient;
use risingwave_rpc_client::error::RpcError;
use thiserror_ext::AsReport;
use tokio::task::JoinHandle;
use tonic::{Status, Streaming};

/// `ObserverManager` is used to update data based on notification from meta.
/// Call `start` to spawn a new asynchronous task
/// We can write the notification logic by implementing `ObserverNodeImpl`.
pub struct ObserverManager<T: NotificationClient, S: ObserverState> {
    rx: T::Channel,
    client: T,
    observer_states: S,
}

pub trait ObserverState: Send + 'static {
    fn subscribe_type() -> SubscribeType;
    /// modify data after receiving notification from meta
    fn handle_notification(&mut self, resp: SubscribeResponse);

    /// Initialize data from the meta. It will be called at start or resubscribe
    fn handle_initialization_notification(&mut self, resp: SubscribeResponse);

    /// Called after the initialization snapshot and all buffered notifications collected before
    /// that snapshot have been replayed.
    fn handle_initialization_finished(&mut self) {}
}

impl<S: ObserverState> ObserverManager<RpcNotificationClient, S> {
    pub async fn new_with_meta_client(meta_client: MetaClient, observer_states: S) -> Self {
        let client = RpcNotificationClient { meta_client };
        Self::new(client, observer_states).await
    }
}

/// Error type for [`ObserverManager`].
#[derive(thiserror::Error, Debug)]
pub enum ObserverError {
    #[error("notification channel closed")]
    ChannelClosed,

    #[error(transparent)]
    Rpc(
        #[from]
        #[backtrace]
        RpcError,
    ),
}

impl From<tonic::Status> for ObserverError {
    fn from(status: tonic::Status) -> Self {
        Self::Rpc(RpcError::from_meta_status(status))
    }
}

impl<T, S> ObserverManager<T, S>
where
    T: NotificationClient,
    S: ObserverState,
{
    pub async fn new(client: T, observer_states: S) -> Self {
        let rx = client.subscribe(S::subscribe_type()).await.unwrap();
        Self {
            rx,
            client,
            observer_states,
        }
    }

    async fn wait_init_notification(&mut self) -> Result<(), ObserverError> {
        let mut notification_vec = Vec::new();
        let init_notification = loop {
            // notification before init notification must be received successfully.
            match self.rx.message().await? {
                Some(notification) => {
                    if !matches!(notification.info.as_ref().unwrap(), &Info::Snapshot(_)) {
                        notification_vec.push(notification);
                    } else {
                        break notification;
                    }
                }
                None => return Err(ObserverError::ChannelClosed),
            }
        };

        let Info::Snapshot(info) = init_notification.info.as_ref().unwrap() else {
            unreachable!();
        };

        notification_vec.retain_mut(|notification| match notification.info.as_ref().unwrap() {
            Info::Database(_)
            | Info::Schema(_)
            | Info::ObjectGroup(_)
            | Info::User(_)
            | Info::Connection(_)
            | Info::Secret(_)
            | Info::Function(_) => {
                notification.version > info.version.as_ref().unwrap().catalog_version
            }
            Info::Node(_) => {
                notification.version > info.version.as_ref().unwrap().worker_node_version
            }
            Info::HummockVersionDeltas(version_delta) => {
                version_delta.version_deltas[0].id > info.hummock_version.as_ref().unwrap().id
            }
            Info::MetaBackupManifestId(_) => true,
            Info::SystemParams(_) | Info::SessionParam(_) => true,
            Info::Snapshot(_) | Info::HummockWriteLimits(_) => unreachable!(),
            Info::HummockStats(_) => true,
            Info::Recovery(_) => true,
            Info::ClusterResource(_) => true,
            Info::StreamingWorkerSlotMapping(_) => {
                notification.version
                    > info
                        .version
                        .as_ref()
                        .unwrap()
                        .streaming_worker_slot_mapping_version
            }
            Info::ServingWorkerSlotMappings(_) => true,
        });

        self.observer_states
            .handle_initialization_notification(init_notification);

        for notification in notification_vec {
            self.observer_states.handle_notification(notification);
        }

        self.observer_states.handle_initialization_finished();

        Ok(())
    }

    /// `start` is used to spawn a new asynchronous task which receives meta's notification and
    /// call the `handle_initialization_notification` and `handle_notification` to update node data.
    pub async fn start(mut self) -> JoinHandle<()> {
        if let Err(err) = self.wait_init_notification().await {
            tracing::warn!(error = %err.as_report(), "Receives meta's notification err");
            self.re_subscribe().await;
        }

        tokio::spawn(async move {
            loop {
                match self.rx.message().await {
                    Ok(resp) => {
                        if resp.is_none() {
                            tracing::warn!("Stream of notification terminated.");
                            self.re_subscribe().await;
                            continue;
                        }
                        self.observer_states.handle_notification(resp.unwrap());
                    }
                    Err(err) => {
                        tracing::warn!(error = %err.as_report(), "Receives meta's notification err");
                        self.re_subscribe().await;
                    }
                }
            }
        })
    }

    /// `re_subscribe` is used to re-subscribe to the meta's notification.
    async fn re_subscribe(&mut self) {
        loop {
            match self.client.subscribe(S::subscribe_type()).await {
                Ok(rx) => {
                    tracing::debug!("re-subscribe success");
                    self.rx = rx;
                    match self.wait_init_notification().await {
                        Err(err) => {
                            tracing::warn!(error = %err.as_report(), "Receives meta's notification err");
                            tokio::time::sleep(RE_SUBSCRIBE_RETRY_INTERVAL).await;
                            continue;
                        }
                        _ => {
                            break;
                        }
                    }
                }
                Err(_) => {
                    tokio::time::sleep(RE_SUBSCRIBE_RETRY_INTERVAL).await;
                }
            }
        }
    }
}

const RE_SUBSCRIBE_RETRY_INTERVAL: Duration = Duration::from_millis(100);

#[async_trait::async_trait]
pub trait Channel: Send + 'static {
    type Item;
    async fn message(&mut self) -> std::result::Result<Option<Self::Item>, Status>;
}

#[async_trait::async_trait]
impl<T: Send + 'static> Channel for Streaming<T> {
    type Item = T;

    async fn message(&mut self) -> std::result::Result<Option<T>, Status> {
        self.message().await
    }
}

#[async_trait::async_trait]
pub trait NotificationClient: Send + Sync + 'static {
    type Channel: Channel<Item = SubscribeResponse>;
    async fn subscribe(
        &self,
        subscribe_type: SubscribeType,
    ) -> Result<Self::Channel, ObserverError>;
}

pub struct RpcNotificationClient {
    meta_client: MetaClient,
}

impl RpcNotificationClient {
    pub fn new(meta_client: MetaClient) -> Self {
        Self { meta_client }
    }
}

#[async_trait::async_trait]
impl NotificationClient for RpcNotificationClient {
    type Channel = Streaming<SubscribeResponse>;

    async fn subscribe(
        &self,
        subscribe_type: SubscribeType,
    ) -> Result<Self::Channel, ObserverError> {
        self.meta_client
            .subscribe(subscribe_type)
            .await
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use risingwave_pb::meta::meta_snapshot::SnapshotVersion;
    use risingwave_pb::meta::subscribe_response::{Info, Operation};
    use risingwave_pb::meta::{
        FragmentWorkerSlotMapping, MetaSnapshot, SubscribeResponse, SubscribeType,
    };
    use tonic::Status;

    use super::{Channel, NotificationClient, ObserverError, ObserverManager, ObserverState};

    #[derive(Clone, Default)]
    struct TestState {
        init_mapping_versions: Arc<Mutex<Vec<u64>>>,
        handled_mapping_versions: Arc<Mutex<Vec<u64>>>,
        events: Arc<Mutex<Vec<TestEvent>>>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TestEvent {
        Snapshot,
        Notification,
        Finished,
    }

    impl ObserverState for TestState {
        fn subscribe_type() -> SubscribeType {
            SubscribeType::Frontend
        }

        fn handle_notification(&mut self, resp: SubscribeResponse) {
            self.events.lock().unwrap().push(TestEvent::Notification);
            self.handled_mapping_versions
                .lock()
                .unwrap()
                .push(resp.version);
        }

        fn handle_initialization_notification(&mut self, resp: SubscribeResponse) {
            let Some(Info::Snapshot(snapshot)) = resp.info else {
                panic!("expected snapshot");
            };
            self.events.lock().unwrap().push(TestEvent::Snapshot);
            self.init_mapping_versions.lock().unwrap().push(
                snapshot
                    .version
                    .as_ref()
                    .unwrap()
                    .streaming_worker_slot_mapping_version,
            );
        }

        fn handle_initialization_finished(&mut self) {
            self.events.lock().unwrap().push(TestEvent::Finished);
        }
    }

    struct TestClient;

    #[async_trait::async_trait]
    impl NotificationClient for TestClient {
        type Channel = TestChannel;

        async fn subscribe(
            &self,
            _subscribe_type: SubscribeType,
        ) -> Result<Self::Channel, ObserverError> {
            unreachable!("subscribe should not be called in this test")
        }
    }

    struct TestChannel {
        notifications: VecDeque<SubscribeResponse>,
    }

    #[async_trait::async_trait]
    impl Channel for TestChannel {
        type Item = SubscribeResponse;

        async fn message(&mut self) -> std::result::Result<Option<Self::Item>, Status> {
            Ok(self.notifications.pop_front())
        }
    }

    fn snapshot_notification(streaming_worker_slot_mapping_version: u64) -> SubscribeResponse {
        SubscribeResponse {
            status: None,
            operation: Operation::Snapshot as _,
            version: 0,
            info: Some(Info::Snapshot(MetaSnapshot {
                version: Some(SnapshotVersion {
                    catalog_version: 0,
                    worker_node_version: 0,
                    streaming_worker_slot_mapping_version,
                }),
                ..Default::default()
            })),
        }
    }

    fn mapping_notification(version: u64) -> SubscribeResponse {
        SubscribeResponse {
            status: None,
            operation: Operation::Add as _,
            version,
            info: Some(Info::StreamingWorkerSlotMapping(
                FragmentWorkerSlotMapping {
                    fragment_id: 1.into(),
                    mapping: None,
                },
            )),
        }
    }

    #[tokio::test]
    async fn keeps_newer_streaming_mapping_notification_during_initialization() {
        let state = TestState::default();
        let mut observer_manager = ObserverManager {
            rx: TestChannel {
                notifications: VecDeque::from([mapping_notification(2), snapshot_notification(1)]),
            },
            client: TestClient,
            observer_states: state.clone(),
        };

        observer_manager.wait_init_notification().await.unwrap();

        assert_eq!(state.init_mapping_versions.lock().unwrap().as_slice(), &[1]);
        assert_eq!(
            state.handled_mapping_versions.lock().unwrap().as_slice(),
            &[2]
        );
        assert_eq!(
            state.events.lock().unwrap().as_slice(),
            &[
                TestEvent::Snapshot,
                TestEvent::Notification,
                TestEvent::Finished,
            ]
        );
    }

    #[tokio::test]
    async fn drops_streaming_mapping_notification_already_covered_by_snapshot() {
        let state = TestState::default();
        let mut observer_manager = ObserverManager {
            rx: TestChannel {
                notifications: VecDeque::from([mapping_notification(1), snapshot_notification(1)]),
            },
            client: TestClient,
            observer_states: state.clone(),
        };

        observer_manager.wait_init_notification().await.unwrap();

        assert_eq!(state.init_mapping_versions.lock().unwrap().as_slice(), &[1]);
        assert!(state.handled_mapping_versions.lock().unwrap().is_empty());
        assert_eq!(
            state.events.lock().unwrap().as_slice(),
            &[TestEvent::Snapshot, TestEvent::Finished]
        );
    }

    #[tokio::test]
    async fn marks_initialization_finished_after_replaying_buffered_notifications() {
        let state = TestState::default();
        let mut observer_manager = ObserverManager {
            rx: TestChannel {
                notifications: VecDeque::from([mapping_notification(3), snapshot_notification(1)]),
            },
            client: TestClient,
            observer_states: state.clone(),
        };

        observer_manager.wait_init_notification().await.unwrap();

        assert_eq!(
            state.events.lock().unwrap().as_slice(),
            &[
                TestEvent::Snapshot,
                TestEvent::Notification,
                TestEvent::Finished,
            ]
        );
    }
}
