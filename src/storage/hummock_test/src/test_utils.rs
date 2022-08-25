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

use risingwave_common::error::Result;
use risingwave_common::util::addr::HostAddr;
use risingwave_common_service::observer_manager::{
    Channel, NotificationClient, ObserverManager, ObserverNodeImpl,
};
use risingwave_meta::hummock::test_utils::setup_compute_env;
use risingwave_meta::manager::{MessageStatus, NotificationManagerRef, WorkerKey};
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::SubscribeResponse;
use tokio::sync::mpsc::UnboundedReceiver;

pub struct TestNotificationClient {
    notification_manager: NotificationManagerRef,
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

impl TestNotificationClient {
    pub fn new(notification_manager: NotificationManagerRef) -> Self {
        Self {
            notification_manager,
        }
    }
}

#[async_trait::async_trait]
impl NotificationClient for TestNotificationClient {
    type Channel = TestChannel<SubscribeResponse>;

    async fn subscribe(&self, addr: &HostAddr, worker_type: WorkerType) -> Result<Self::Channel> {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        self.notification_manager
            .insert_sender(worker_type, WorkerKey(addr.to_protobuf()), tx)
            .await;
        Ok(TestChannel(rx))
    }
}

pub async fn get_test_observer_manager(
    client: TestNotificationClient,
    addr: HostAddr,
    observer_states: Box<dyn ObserverNodeImpl + Send>,
    worker_type: WorkerType,
) -> ObserverManager<TestNotificationClient> {
    let rx = client.subscribe(&addr, worker_type).await.unwrap();
    ObserverManager::new_with(rx, client, addr, observer_states, worker_type)
}

#[tokio::test]
async fn test_observer_manager() {
    let (env, _hummock_manager_ref, _cluster_manager_ref, _worker_node) =
        setup_compute_env(8080).await;
    let _client = TestNotificationClient::new(env.notification_manager_ref());
    // let compute_observer_node = ComputeObserverNode::new(filter_key_extractor_manager.clone());
    // ObserverManager::new_with(rx, client, addr, observer_states, worker_type)
}
