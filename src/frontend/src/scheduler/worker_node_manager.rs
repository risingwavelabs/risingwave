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

use std::sync::{Arc, RwLock};

use rand::distributions::{Distribution as RandDistribution, Uniform};
use risingwave_common::error::Result;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_rpc_client::MetaClient;

/// `WorkerNodeManager` manages live worker nodes.
pub struct WorkerNodeManager {
    worker_nodes: RwLock<Vec<WorkerNode>>,
}

pub type WorkerNodeManagerRef = Arc<WorkerNodeManager>;

impl WorkerNodeManager {
    pub async fn new(client: MetaClient) -> Result<Self> {
        let worker_nodes = RwLock::new(
            client
                .list_all_nodes(WorkerType::ComputeNode, false)
                .await?,
        );
        Ok(Self { worker_nodes })
    }

    /// Used in tests.
    pub fn mock(worker_nodes: Vec<WorkerNode>) -> Self {
        let worker_nodes = RwLock::new(worker_nodes);
        Self { worker_nodes }
    }

    pub fn list_worker_nodes(&self) -> Vec<WorkerNode> {
        self.worker_nodes.read().unwrap().clone()
    }

    pub fn add_worker_node(&self, node: WorkerNode) {
        self.worker_nodes.write().unwrap().push(node);
    }

    pub fn remove_worker_node(&self, node: WorkerNode) {
        self.worker_nodes.write().unwrap().retain(|x| *x != node);
    }

    pub fn refresh_worker_node(&self, nodes: Vec<WorkerNode>) {
        let mut write_guard = self.worker_nodes.write().unwrap();
        *write_guard = nodes;
    }

    /// Get a random worker node.
    pub fn next_random(&self) -> WorkerNode {
        let current_nodes = self.worker_nodes.read().unwrap();
        let mut rng = rand::thread_rng();

        let die = Uniform::from(0..current_nodes.len());
        current_nodes.get(die.sample(&mut rng)).unwrap().clone()
    }

    pub fn worker_node_count(&self) -> usize {
        self.worker_nodes.read().unwrap().len()
    }
}

// TODO: with a good MockMeta and then we can open the tests.
// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;
//     use std::time::Duration;

//     use risingwave_pb::common::{HostAddress, WorkerType};
//     use risingwave_rpc_client::MetaClient;
//     use tokio::sync::watch;

//     use super::WorkerNodeManager;
//     use crate::observer::observer_manager::ObserverManager;
//     use crate::test_utils::FrontendMockMetaClient;

//     #[tokio::test]
//     async fn test_add_and_delete_worker_node() {
//         let mut meta_client = MetaClient::mock(FrontendMockMetaClient::new().await);

//         let (catalog_updated_tx, _) = watch::channel(0);
//         let catalog_cache = Arc::new(RwLock::new(
//             CatalogCache::new(meta_client.clone()).await.unwrap(),
//         ));
//         let worker_node_manager =
//             Arc::new(WorkerNodeManager::new(meta_client.clone()).await.unwrap());

//         let observer_manager = ObserverManager::new(
//             meta_client.clone(),
//             "127.0.0.1:12345".parse().unwrap(),
//             worker_node_manager.clone(),
//             catalog_cache,
//             catalog_updated_tx,
//         )
//         .await;
//         observer_manager.start();

//         // Add worker node
//         let socket_addr = "127.0.0.1:6789".parse().unwrap();
//         meta_client
//             .register(socket_addr, WorkerType::ComputeNode)
//             .await
//             .unwrap();
//         meta_client.activate(socket_addr).await.unwrap();
//         tokio::time::sleep(Duration::from_millis(100)).await;
//         let mut worker_nodes = worker_node_manager.list_worker_nodes();
//         assert_eq!(1, worker_nodes.len());
//         let worker_node_0 = worker_nodes.pop().unwrap();
//         assert_eq!(WorkerType::ComputeNode, worker_node_0.r#type());
//         assert_eq!(
//             &HostAddress {
//                 host: "127.0.0.1".to_string(),
//                 port: 6789
//             },
//             worker_node_0.get_host().unwrap()
//         );

//         // Delete worker node
//         meta_client.unregister(socket_addr).await.unwrap();
//         tokio::time::sleep(Duration::from_millis(100)).await;
//         let worker_nodes = worker_node_manager.list_worker_nodes();
//         assert_eq!(0, worker_nodes.len());
//     }
// }
