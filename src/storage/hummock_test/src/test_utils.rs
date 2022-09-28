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

use std::sync::Arc;

use risingwave_common::error::Result;
use risingwave_common::util::addr::HostAddr;
use risingwave_common_service::observer_manager::{
    Channel, NotificationClient, ObserverManager, ObserverNodeImpl,
};
use risingwave_compute::compute_observer::observer_manager::ComputeObserverNode;
use risingwave_hummock_sdk::filter_key_extractor::FilterKeyExtractorManager;
use risingwave_meta::hummock::{HummockManager, HummockManagerRef};
use risingwave_meta::manager::{MessageStatus, MetaSrvEnv, NotificationManagerRef, WorkerKey};
use risingwave_meta::storage::{MemStore, MetaStore};
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{MetaSnapshot, SubscribeResponse};
use risingwave_storage::hummock::compaction_group_client::{
    CompactionGroupClientImpl, DummyCompactionGroupClient,
};
use risingwave_storage::hummock::local_version::local_version_manager::LocalVersionManagerRef;
use tokio::sync::mpsc::UnboundedReceiver;

pub struct TestNotificationClient<S: MetaStore> {
    notification_manager: NotificationManagerRef,
    hummock_manager: HummockManagerRef<S>,
}

pub struct TestChannel<T>(UnboundedReceiver<std::result::Result<T, MessageStatus>>);

#[async_trait::async_trait]
impl<T: Send> Channel<T> for TestChannel<T> {
    async fn message(&mut self) -> std::result::Result<Option<T>, MessageStatus> {
        match self.0.recv().await {
            None => Ok(None),
            Some(result) => result.map(|r| Some(r)),
        }
    }
}

impl<S: MetaStore> TestNotificationClient<S> {
    pub fn new(
        notification_manager: NotificationManagerRef,
        hummock_manager: HummockManagerRef<S>,
    ) -> Self {
        Self {
            notification_manager,
            hummock_manager,
        }
    }
}

#[async_trait::async_trait]
impl<S: MetaStore> NotificationClient for TestNotificationClient<S> {
    type Channel = TestChannel<SubscribeResponse>;

    async fn subscribe(&self, addr: &HostAddr, worker_type: WorkerType) -> Result<Self::Channel> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let hummock_manager_guard = self.hummock_manager.get_read_guard().await;
        let meta_snapshot = MetaSnapshot {
            hummock_version: Some(hummock_manager_guard.current_version.clone()),
            ..Default::default()
        };
        tx.send(Ok(SubscribeResponse {
            status: None,
            operation: Operation::Snapshot as i32,
            info: Some(Info::Snapshot(meta_snapshot)),
            version: self.notification_manager.current_version().await,
        }))
        .unwrap();
        self.notification_manager
            .insert_sender(worker_type, WorkerKey(addr.to_protobuf()), tx)
            .await;
        Ok(TestChannel(rx))
    }
}

pub async fn get_test_observer_manager<S: MetaStore>(
    client: TestNotificationClient<S>,
    addr: HostAddr,
    observer_states: Box<dyn ObserverNodeImpl + Send>,
    worker_type: WorkerType,
) -> ObserverManager<TestNotificationClient<S>> {
    let rx = client.subscribe(&addr, worker_type).await.unwrap();
    ObserverManager::with_subscriber(rx, client, addr, observer_states, worker_type)
}

pub async fn get_observer_manager(
    env: MetaSrvEnv<MemStore>,
    hummock_manager_ref: Arc<HummockManager<MemStore>>,
    filter_key_extractor_manager: Arc<FilterKeyExtractorManager>,
    local_version_manager: LocalVersionManagerRef,
    worker_node: WorkerNode,
    compaction_group_client: Option<Arc<CompactionGroupClientImpl>>,
) -> ObserverManager<TestNotificationClient<MemStore>> {
    let client = TestNotificationClient::new(env.notification_manager_ref(), hummock_manager_ref);
    let compute_observer_node = ComputeObserverNode::new(
        filter_key_extractor_manager,
        local_version_manager,
        compaction_group_client.unwrap_or_else(|| {
            Arc::new(CompactionGroupClientImpl::Dummy(
                DummyCompactionGroupClient::new(0),
            ))
        }),
    );
    get_test_observer_manager(
        client,
        worker_node.get_host().unwrap().into(),
        Box::new(compute_observer_node),
        worker_node.get_type().unwrap(),
    )
    .await
}
