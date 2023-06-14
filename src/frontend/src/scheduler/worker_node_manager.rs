// Copyright 2023 RisingWave Labs
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
use std::sync::{Arc, RwLock};

use rand::seq::SliceRandom;
use risingwave_common::bail;
use risingwave_common::hash::{ParallelUnitId, ParallelUnitMapping};
use risingwave_common::util::worker_util::get_pu_to_worker_mapping;
use risingwave_pb::common::{WorkerNode, WorkerType};

use crate::catalog::FragmentId;
use crate::scheduler::{SchedulerError, SchedulerResult};

/// `WorkerNodeManager` manages live worker nodes and table vnode mapping information.
pub struct WorkerNodeManager {
    inner: RwLock<WorkerNodeManagerInner>,
}

struct WorkerNodeManagerInner {
    worker_nodes: Vec<WorkerNode>,
    /// fragment vnode mapping info for streaming
    streaming_fragment_vnode_mapping: HashMap<FragmentId, ParallelUnitMapping>,
    /// fragment vnode mapping info for serving
    serving_fragment_vnode_mapping: HashMap<FragmentId, ParallelUnitMapping>,
}

pub type WorkerNodeManagerRef = Arc<WorkerNodeManager>;

impl Default for WorkerNodeManager {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerNodeManager {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(WorkerNodeManagerInner {
                worker_nodes: Default::default(),
                streaming_fragment_vnode_mapping: Default::default(),
                serving_fragment_vnode_mapping: Default::default(),
            }),
        }
    }

    /// Used in tests.
    pub fn mock(worker_nodes: Vec<WorkerNode>) -> Self {
        let inner = RwLock::new(WorkerNodeManagerInner {
            worker_nodes,
            streaming_fragment_vnode_mapping: HashMap::new(),
            serving_fragment_vnode_mapping: HashMap::new(),
        });
        Self { inner }
    }

    pub fn list_worker_nodes(&self) -> Vec<WorkerNode> {
        self.inner
            .read()
            .unwrap()
            .worker_nodes
            .iter()
            .filter(|w| w.r#type() == WorkerType::ComputeNode)
            .cloned()
            .collect()
    }

    fn list_serving_worker_nodes(&self) -> Vec<WorkerNode> {
        self.list_worker_nodes()
            .into_iter()
            .filter(|w| w.property.as_ref().map_or(false, |p| p.is_serving))
            .collect()
    }

    fn list_streaming_worker_nodes(&self) -> Vec<WorkerNode> {
        self.list_worker_nodes()
            .into_iter()
            .filter(|w| w.property.as_ref().map_or(false, |p| p.is_streaming))
            .collect()
    }

    pub fn add_worker_node(&self, node: WorkerNode) {
        let mut write_guard = self.inner.write().unwrap();
        // update
        for w in &mut write_guard.worker_nodes {
            if w.id == node.id {
                *w = node;
                return;
            }
        }
        // insert
        write_guard.worker_nodes.push(node);
    }

    pub fn remove_worker_node(&self, node: WorkerNode) {
        let mut write_guard = self.inner.write().unwrap();
        write_guard.worker_nodes.retain(|x| x.id != node.id);
    }

    pub fn refresh(
        &self,
        nodes: Vec<WorkerNode>,
        streaming_mapping: HashMap<FragmentId, ParallelUnitMapping>,
        serving_mapping: HashMap<FragmentId, ParallelUnitMapping>,
    ) {
        let mut write_guard = self.inner.write().unwrap();
        tracing::debug!("Refresh worker nodes {:?}.", nodes);
        tracing::debug!(
            "Refresh streaming vnode mapping for fragments {:?}.",
            streaming_mapping.keys()
        );
        tracing::debug!(
            "Refresh serving vnode mapping for fragments {:?}.",
            serving_mapping.keys()
        );
        write_guard.worker_nodes = nodes;
        write_guard.streaming_fragment_vnode_mapping = streaming_mapping;
        write_guard.serving_fragment_vnode_mapping = serving_mapping;
    }

    /// If parallel unit ids is empty, the scheduler may fail to schedule any task and stuck at
    /// schedule next stage. If we do not return error in this case, needs more complex control
    /// logic above. Report in this function makes the schedule root fail reason more clear.
    pub fn get_workers_by_parallel_unit_ids(
        &self,
        parallel_unit_ids: &[ParallelUnitId],
    ) -> SchedulerResult<Vec<WorkerNode>> {
        if parallel_unit_ids.is_empty() {
            return Err(SchedulerError::EmptyWorkerNodes);
        }
        let pu_to_worker = get_pu_to_worker_mapping(&self.inner.read().unwrap().worker_nodes);

        let mut workers = Vec::with_capacity(parallel_unit_ids.len());
        for parallel_unit_id in parallel_unit_ids {
            match pu_to_worker.get(parallel_unit_id) {
                Some(worker) => workers.push(worker.clone()),
                None => bail!(
                    "No worker node found for parallel unit id: {}",
                    parallel_unit_id
                ),
            }
        }
        Ok(workers)
    }

    pub fn get_streaming_fragment_mapping(
        &self,
        fragment_id: &FragmentId,
    ) -> SchedulerResult<ParallelUnitMapping> {
        self.inner
            .read()
            .unwrap()
            .streaming_fragment_vnode_mapping
            .get(fragment_id)
            .cloned()
            .ok_or_else(|| SchedulerError::StreamingVnodeMappingNotFound(*fragment_id))
    }

    pub fn insert_streaming_fragment_mapping(
        &self,
        fragment_id: FragmentId,
        vnode_mapping: ParallelUnitMapping,
    ) {
        self.inner
            .write()
            .unwrap()
            .streaming_fragment_vnode_mapping
            .try_insert(fragment_id, vnode_mapping)
            .unwrap();
    }

    pub fn update_streaming_fragment_mapping(
        &self,
        fragment_id: FragmentId,
        vnode_mapping: ParallelUnitMapping,
    ) {
        let mut guard = self.inner.write().unwrap();
        guard
            .streaming_fragment_vnode_mapping
            .insert(fragment_id, vnode_mapping)
            .unwrap();
    }

    pub fn remove_streaming_fragment_mapping(&self, fragment_id: &FragmentId) {
        let mut guard = self.inner.write().unwrap();
        guard
            .streaming_fragment_vnode_mapping
            .remove(fragment_id)
            .unwrap();
    }

    /// Returns fragment's vnode mapping for serving.
    fn serving_fragment_mapping(
        &self,
        fragment_id: FragmentId,
    ) -> SchedulerResult<ParallelUnitMapping> {
        self.inner
            .read()
            .unwrap()
            .get_serving_fragment_mapping(fragment_id)
            .ok_or_else(|| SchedulerError::ServingVnodeMappingNotFound(fragment_id))
    }

    pub fn set_serving_fragment_mapping(&self, mappings: HashMap<FragmentId, ParallelUnitMapping>) {
        let mut guard = self.inner.write().unwrap();
        tracing::debug!(
            "Set serving vnode mapping for fragments {:?}",
            mappings.keys()
        );
        guard.serving_fragment_vnode_mapping = mappings;
    }

    pub fn upsert_serving_fragment_mapping(
        &self,
        mappings: HashMap<FragmentId, ParallelUnitMapping>,
    ) {
        let mut guard = self.inner.write().unwrap();
        tracing::debug!(
            "Upsert serving vnode mapping for fragments {:?}",
            mappings.keys()
        );
        for (fragment_id, mapping) in mappings {
            guard
                .serving_fragment_vnode_mapping
                .insert(fragment_id, mapping);
        }
    }

    pub fn remove_serving_fragment_mapping(&self, fragment_ids: &[FragmentId]) {
        let mut guard = self.inner.write().unwrap();
        tracing::debug!(
            "Delete serving vnode mapping for fragments {:?}",
            fragment_ids
        );
        for fragment_id in fragment_ids {
            guard.serving_fragment_vnode_mapping.remove(fragment_id);
        }
    }
}

impl WorkerNodeManagerInner {
    fn get_serving_fragment_mapping(&self, fragment_id: FragmentId) -> Option<ParallelUnitMapping> {
        self.serving_fragment_vnode_mapping
            .get(&fragment_id)
            .cloned()
    }
}

/// Selects workers for query according to `enable_barrier_read`
#[derive(Clone)]
pub struct WorkerNodeSelector {
    pub manager: WorkerNodeManagerRef,
    enable_barrier_read: bool,
}

impl WorkerNodeSelector {
    pub fn new(manager: WorkerNodeManagerRef, enable_barrier_read: bool) -> Self {
        Self {
            manager,
            enable_barrier_read,
        }
    }

    pub fn worker_node_count(&self) -> usize {
        if self.enable_barrier_read {
            self.manager.list_streaming_worker_nodes().len()
        } else {
            self.manager.list_serving_worker_nodes().len()
        }
    }

    pub fn schedule_unit_count(&self) -> usize {
        let worker_nodes = if self.enable_barrier_read {
            self.manager.list_streaming_worker_nodes()
        } else {
            self.manager.list_serving_worker_nodes()
        };
        worker_nodes
            .iter()
            .map(|node| node.parallel_units.len())
            .sum()
    }

    pub fn fragment_mapping(
        &self,
        fragment_id: FragmentId,
    ) -> SchedulerResult<ParallelUnitMapping> {
        if self.enable_barrier_read {
            self.manager.get_streaming_fragment_mapping(&fragment_id)
        } else {
            self.manager.serving_fragment_mapping(fragment_id)
        }
    }

    pub fn next_random_worker(&self) -> SchedulerResult<WorkerNode> {
        let worker_nodes = if self.enable_barrier_read {
            self.manager.list_streaming_worker_nodes()
        } else {
            self.manager.list_serving_worker_nodes()
        };
        worker_nodes
            .choose(&mut rand::thread_rng())
            .ok_or_else(|| SchedulerError::EmptyWorkerNodes)
            .map(|w| (*w).clone())
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::util::addr::HostAddr;
    use risingwave_pb::common::worker_node;
    use risingwave_pb::common::worker_node::Property;

    #[test]
    fn test_worker_node_manager() {
        use super::*;

        let manager = WorkerNodeManager::mock(vec![]);
        assert_eq!(manager.list_serving_worker_nodes().len(), 0);
        assert_eq!(manager.list_streaming_worker_nodes().len(), 0);
        assert_eq!(manager.list_worker_nodes(), vec![]);

        let worker_nodes = vec![
            WorkerNode {
                id: 1,
                r#type: WorkerType::ComputeNode as i32,
                host: Some(HostAddr::try_from("127.0.0.1:1234").unwrap().to_protobuf()),
                state: worker_node::State::Running as i32,
                parallel_units: vec![],
                property: Some(Property {
                    is_streaming: true,
                    is_schedulable: true,
                    is_serving: true,
                }),
            },
            WorkerNode {
                id: 2,
                r#type: WorkerType::ComputeNode as i32,
                host: Some(HostAddr::try_from("127.0.0.1:1235").unwrap().to_protobuf()),
                state: worker_node::State::Running as i32,
                parallel_units: vec![],
                property: Some(Property {
                    is_streaming: false,
                    is_schedulable: true,
                    is_serving: true,
                }),
            },
        ];
        worker_nodes
            .iter()
            .for_each(|w| manager.add_worker_node(w.clone()));
        assert_eq!(manager.list_serving_worker_nodes().len(), 2);
        assert_eq!(manager.list_streaming_worker_nodes().len(), 1);
        assert_eq!(manager.list_worker_nodes(), worker_nodes);

        manager.remove_worker_node(worker_nodes[0].clone());
        assert_eq!(manager.list_serving_worker_nodes().len(), 1);
        assert_eq!(manager.list_streaming_worker_nodes().len(), 0);
        assert_eq!(
            manager.list_worker_nodes(),
            worker_nodes.as_slice()[1..].to_vec()
        );
    }
}
