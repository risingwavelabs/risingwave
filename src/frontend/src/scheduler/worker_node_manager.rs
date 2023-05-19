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

use std::collections::{HashMap, HashSet, LinkedList};
use std::num::Wrapping;
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::Duration;

use anyhow::anyhow;
use itertools::Itertools;
use rand::seq::SliceRandom;
use risingwave_common::bail;
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::hash::{ParallelUnitId, ParallelUnitMapping, VirtualNode};
use risingwave_common::util::worker_util::get_pu_to_worker_mapping;
use risingwave_pb::common::{ParallelUnit, WorkerNode, WorkerType};

use crate::catalog::FragmentId;
use crate::scheduler::{SchedulerError, SchedulerResult};

/// `WorkerNodeManager` manages live worker nodes and table vnode mapping information.
pub struct WorkerNodeManager {
    inner: RwLock<WorkerNodeManagerInner>,
    /// Temporarily make worker invisible from serving cluster.
    worker_node_mask: Arc<RwLock<HashSet<u32>>>,
}

struct WorkerNodeManagerInner {
    worker_nodes: Vec<WorkerNode>,
    /// fragment vnode mapping info for streaming. It's from meta node.
    streaming_fragment_vnode_mapping: HashMap<FragmentId, ParallelUnitMapping>,
    /// fragment vnode mapping info for serving. It's calculated locally.
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
            worker_node_mask: Arc::new(Default::default()),
        }
    }

    /// Used in tests.
    pub fn mock(worker_nodes: Vec<WorkerNode>) -> Self {
        let inner = RwLock::new(WorkerNodeManagerInner {
            worker_nodes,
            streaming_fragment_vnode_mapping: HashMap::new(),
            serving_fragment_vnode_mapping: HashMap::new(),
        });
        Self {
            inner,
            worker_node_mask: Arc::new(Default::default()),
        }
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
        if node.property.as_ref().map_or(false, |p| p.is_serving) {
            write_guard.serving_fragment_vnode_mapping.clear();
        }
        write_guard.worker_nodes.push(node);
    }

    pub fn remove_worker_node(&self, node: WorkerNode) {
        let mut write_guard = self.inner.write().unwrap();
        if node.property.as_ref().map_or(false, |p| p.is_serving) {
            write_guard.serving_fragment_vnode_mapping.clear();
        }
        write_guard.worker_nodes.retain(|x| *x != node);
    }

    pub fn refresh(
        &self,
        nodes: Vec<WorkerNode>,
        mapping: HashMap<FragmentId, ParallelUnitMapping>,
    ) {
        let mut write_guard = self.inner.write().unwrap();
        write_guard.worker_nodes = nodes;
        write_guard.streaming_fragment_vnode_mapping = mapping;
        write_guard.serving_fragment_vnode_mapping.clear();
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
    ) -> Option<ParallelUnitMapping> {
        self.inner
            .read()
            .unwrap()
            .streaming_fragment_vnode_mapping
            .get(fragment_id)
            .cloned()
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
        guard.serving_fragment_vnode_mapping.remove(&fragment_id);
    }

    pub fn remove_streaming_fragment_mapping(&self, fragment_id: &FragmentId) {
        let mut guard = self.inner.write().unwrap();
        guard
            .streaming_fragment_vnode_mapping
            .remove(fragment_id)
            .unwrap();
        guard.serving_fragment_vnode_mapping.remove(fragment_id);
    }

    /// Returns fragment's vnode mapping for serving.
    fn get_serving_fragment_mapping(
        &self,
        fragment_id: FragmentId,
    ) -> SchedulerResult<ParallelUnitMapping> {
        if let Some(pu_mapping) = self
            .inner
            .read()
            .unwrap()
            .get_serving_fragment_mapping(fragment_id)
        {
            return Ok(pu_mapping);
        }
        let mut guard = self.inner.write().unwrap();
        if let Some(pu_mapping) = guard.get_serving_fragment_mapping(fragment_id) {
            return Ok(pu_mapping);
        }
        let pu_mapping = guard.map_vnode_for_serving(fragment_id)?;
        Ok(pu_mapping)
    }

    fn worker_node_mask(&self) -> RwLockReadGuard<'_, HashSet<u32>> {
        self.worker_node_mask.read().unwrap()
    }

    pub fn mask_worker_node(&self, worker_node_id: u32, duration: Duration) {
        let mut worker_node_mask = self.worker_node_mask.write().unwrap();
        if worker_node_mask.contains(&worker_node_id) {
            return;
        }
        worker_node_mask.insert(worker_node_id);
        let worker_node_mask_ref = self.worker_node_mask.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            worker_node_mask_ref
                .write()
                .unwrap()
                .remove(&worker_node_id);
        });
    }
}

impl WorkerNodeManagerInner {
    fn get_serving_fragment_mapping(&self, fragment_id: FragmentId) -> Option<ParallelUnitMapping> {
        self.serving_fragment_vnode_mapping
            .get(&fragment_id)
            .cloned()
    }

    /// Calculates serving vnode mappings for `fragment_id`.
    /// The `serving_parallelism` is set to min(`all_serving_pus`, `streaming_parallelism`).
    fn map_vnode_for_serving(
        &mut self,
        fragment_id: FragmentId,
    ) -> SchedulerResult<ParallelUnitMapping> {
        // Use streaming parallelism as a hint
        let streaming_vnode_mapping = self
            .streaming_fragment_vnode_mapping
            .get(&fragment_id)
            .ok_or_else(|| {
                anyhow!(
                    "streaming vnode mapping for fragment {} not found",
                    fragment_id
                )
            })?;
        let mapping = map_vnode_for_serving(
            &round_robin_parallel_units(
                &self.worker_nodes,
                streaming_vnode_mapping.iter_unique().count(),
            ),
            fragment_id,
        )
        .ok_or_else(|| SchedulerError::EmptyWorkerNodes)?;
        self.serving_fragment_vnode_mapping
            .insert(fragment_id, mapping.clone());
        Ok(mapping)
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
            self.apply_worker_node_mask(self.manager.list_serving_worker_nodes())
                .len()
        }
    }

    pub fn schedule_unit_count(&self) -> usize {
        let worker_nodes = if self.enable_barrier_read {
            self.manager.list_streaming_worker_nodes()
        } else {
            self.apply_worker_node_mask(self.manager.list_serving_worker_nodes())
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
            self.manager
                .get_streaming_fragment_mapping(&fragment_id)
                .ok_or_else(|| {
                    SchedulerError::Internal(anyhow!(
                        "vnode mapping for fragment {} not found",
                        fragment_id
                    ))
                })
        } else {
            let origin = self.manager.get_serving_fragment_mapping(fragment_id)?;
            if self.manager.worker_node_mask().is_empty() {
                return Ok(origin);
            }
            let new_workers = self.apply_worker_node_mask(self.manager.list_serving_worker_nodes());
            let masked_mapping = map_vnode_for_serving(
                &round_robin_parallel_units(&new_workers, origin.iter_unique().count()),
                fragment_id,
            );
            masked_mapping.ok_or_else(|| SchedulerError::EmptyWorkerNodes)
        }
    }

    pub fn next_random_worker(&self) -> SchedulerResult<WorkerNode> {
        let worker_nodes = if self.enable_barrier_read {
            self.manager.list_streaming_worker_nodes()
        } else {
            self.apply_worker_node_mask(self.manager.list_serving_worker_nodes())
        };
        worker_nodes
            .choose(&mut rand::thread_rng())
            .ok_or_else(|| SchedulerError::EmptyWorkerNodes)
            .map(|w| (*w).clone())
    }

    fn apply_worker_node_mask(&self, origin: Vec<WorkerNode>) -> Vec<WorkerNode> {
        let mask = self.manager.worker_node_mask();
        origin
            .into_iter()
            .filter(|w| !mask.contains(&w.id))
            .collect()
    }
}

/// Selects at most `max_parallelism` parallel units from `worker_nodes` in a round-robin fashion.
fn round_robin_parallel_units(
    worker_nodes: &[WorkerNode],
    max_parallelism: usize,
) -> Vec<ParallelUnit> {
    let mut parallel_units: LinkedList<_> = worker_nodes
        .iter()
        .sorted_by_key(|w| w.id)
        .filter(|w| w.property.as_ref().map_or(false, |p| p.is_serving))
        .map(|w| w.parallel_units.iter().cloned().sorted_by_key(|p| p.id))
        .collect();
    let mut round_robin = Vec::new();
    while !parallel_units.is_empty() {
        parallel_units.drain_filter(|ps| {
            if let Some(p) = ps.next() {
                round_robin.push(p);
                false
            } else {
                true
            }
        });
    }
    round_robin.truncate(max_parallelism);
    round_robin
}

/// Hash function used to calculate score in `map_vnode_for_serving`.
///
/// <https://github.com/ceph/libcrush/blob/de2e859acd5e52b6db3cf8cde08834e09b8d4ead/crush/hash.c#L48>
fn hash32_rjenkins1_3(a: u32, b: u32, c: u32) -> u32 {
    let hashmix = |mut a: Wrapping<u32>, mut b: Wrapping<u32>, mut c: Wrapping<u32>| {
        a -= b;
        a -= c;
        a ^= c >> 13;
        b -= c;
        b -= a;
        b ^= a << 8;
        c -= a;
        c -= b;
        c ^= b >> 13;
        a -= b;
        a -= c;
        a ^= c >> 12;
        b -= c;
        b -= a;
        b ^= a << 16;
        c -= a;
        c -= b;
        c ^= b >> 5;
        a -= b;
        a -= c;
        a ^= c >> 3;
        b -= c;
        b -= a;
        b ^= a << 10;
        c -= a;
        c -= b;
        c ^= b >> 15;
        c
    };
    let a = Wrapping(a);
    let b = Wrapping(b);
    let c = Wrapping(c);
    let hash_seed = Wrapping(1315423911);
    let mut hash = hash_seed ^ a ^ b ^ c;
    let x = Wrapping(231232);
    let y = Wrapping(1232);
    hash = hashmix(a, b, hash);
    hash = hashmix(c, x, hash);
    hash = hashmix(y, a, hash);
    hash = hashmix(b, x, hash);
    hash = hashmix(y, c, hash);
    hash.0
}

/// Maps all virtual nodes to `parallel_units` deterministically, through a process analogous to
/// a draw of straws. It leads to a probabilistically balanced distribution. It minimizes data
/// migration to restore a balanced distribution after addition or removal of parallel units.
///
/// It's a simplified version of CRUSH's Straw Buckets in Ceph.
pub fn map_vnode_for_serving(
    parallel_units: &[ParallelUnit],
    extra_seed: u32,
) -> Option<ParallelUnitMapping> {
    if parallel_units.is_empty() {
        return None;
    }
    let mut pu_vnodes: HashMap<ParallelUnitId, BitmapBuilder> = HashMap::new();
    for vnode in VirtualNode::all() {
        let mut select = 0;
        let mut high_score = 0;
        for (i, pu) in parallel_units.iter().enumerate() {
            let score = hash32_rjenkins1_3(pu.id, vnode.to_index() as u32, extra_seed);
            if i == 0 || score > high_score {
                high_score = score;
                select = i;
            }
        }
        pu_vnodes
            .entry(parallel_units[select].id)
            .or_insert(BitmapBuilder::zeroed(VirtualNode::COUNT))
            .set(vnode.to_index(), true);
    }
    let pu_vnodes = pu_vnodes
        .into_iter()
        .map(|(pu_id, builder)| (pu_id, builder.finish()))
        .collect();
    Some(ParallelUnitMapping::from_bitmaps(&pu_vnodes))
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::hash::ParallelUnitId;
    use risingwave_common::util::addr::HostAddr;
    use risingwave_pb::common::worker_node::Property;
    use risingwave_pb::common::{worker_node, ParallelUnit, WorkerNode};

    use crate::scheduler::worker_node_manager::round_robin_parallel_units;

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

    #[test]
    fn test_round_robin_parallel_units() {
        let serving_property = Property {
            is_streaming: false,
            is_serving: true,
        };
        let mut pu_id_counter: ParallelUnitId = 1;
        let mut gen_pus_for_worker = |worker_node_id: u32, number: u32| {
            let mut results = vec![];
            for i in 0..number {
                results.push(ParallelUnit {
                    id: pu_id_counter + i,
                    worker_node_id,
                })
            }
            pu_id_counter += number;
            results
        };
        let worker_nodes = vec![
            WorkerNode {
                id: 1,
                parallel_units: gen_pus_for_worker(1, 3),
                property: Some(serving_property.clone()),
                ..Default::default()
            },
            WorkerNode {
                id: 2,
                parallel_units: gen_pus_for_worker(2, 2),
                property: Some(serving_property.clone()),
                ..Default::default()
            },
            WorkerNode {
                id: 3,
                parallel_units: gen_pus_for_worker(3, 1),
                property: Some(serving_property),
                ..Default::default()
            },
        ];
        assert!(round_robin_parallel_units(&worker_nodes, 0).is_empty());
        assert_eq!(
            round_robin_parallel_units(&worker_nodes, 1)
                .into_iter()
                .map(|pu| pu.id)
                .collect_vec(),
            vec![1]
        );
        assert_eq!(
            round_robin_parallel_units(&worker_nodes, 2)
                .into_iter()
                .map(|pu| pu.id)
                .collect_vec(),
            vec![1, 4]
        );
        assert_eq!(
            round_robin_parallel_units(&worker_nodes, 3)
                .into_iter()
                .map(|pu| pu.id)
                .collect_vec(),
            vec![1, 4, 6]
        );
        assert_eq!(
            round_robin_parallel_units(&worker_nodes, 4)
                .into_iter()
                .map(|pu| pu.id)
                .collect_vec(),
            vec![1, 4, 6, 2]
        );
        assert_eq!(
            round_robin_parallel_units(&worker_nodes, 1000)
                .into_iter()
                .map(|pu| pu.id)
                .collect_vec(),
            vec![1, 4, 6, 2, 5, 3]
        );
    }
}
