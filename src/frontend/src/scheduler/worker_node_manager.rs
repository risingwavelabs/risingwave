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

use anyhow::anyhow;
use rand::distributions::{Distribution as RandDistribution, Uniform};
use risingwave_common::error::Result;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_rpc_client::MetaClient;

use crate::scheduler::SchedulerError::Internal;
use crate::scheduler::SchedulerResult;

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
    pub fn next_random(&self) -> SchedulerResult<WorkerNode> {
        let current_nodes = self.worker_nodes.read().unwrap();
        let mut rng = rand::thread_rng();
        if current_nodes.is_empty() {
            tracing::error!("No worker node available.");
            return Err(Internal(anyhow!("No worker node available")));
        }

        let die = Uniform::from(0..current_nodes.len());
        Ok(current_nodes.get(die.sample(&mut rng)).unwrap().clone())
    }

    pub fn worker_node_count(&self) -> usize {
        self.worker_nodes.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::util::addr::HostAddr;
    use risingwave_pb::common::worker_node;

    #[test]
    fn test_worker_node_manager() {
        use super::*;

        let manager = WorkerNodeManager::mock(vec![]);
        assert_eq!(manager.worker_node_count(), 0);
        assert_eq!(manager.list_worker_nodes(), vec![]);

        let worker_nodes = vec![
            WorkerNode {
                id: 1,
                r#type: WorkerType::ComputeNode as i32,
                host: Some(HostAddr::try_from("127.0.0.1:1234").unwrap().to_protobuf()),
                state: worker_node::State::Running as i32,
                parallel_units: vec![],
            },
            WorkerNode {
                id: 2,
                r#type: WorkerType::ComputeNode as i32,
                host: Some(HostAddr::try_from("127.0.0.1:1235").unwrap().to_protobuf()),
                state: worker_node::State::Running as i32,
                parallel_units: vec![],
            },
        ];
        worker_nodes
            .iter()
            .for_each(|w| manager.add_worker_node(w.clone()));
        assert_eq!(manager.worker_node_count(), 2);
        assert_eq!(manager.list_worker_nodes(), worker_nodes);

        manager.remove_worker_node(worker_nodes[0].clone());
        assert_eq!(manager.worker_node_count(), 1);
        assert_eq!(
            manager.list_worker_nodes(),
            worker_nodes.as_slice()[1..].to_vec()
        );
    }
}
