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

use std::collections::{BTreeMap, HashMap, HashSet, LinkedList, VecDeque};
use std::sync::{Arc, RwLock};

use anyhow::anyhow;
use itertools::Itertools;
use num_integer::Integer;
use rand::seq::SliceRandom;
use risingwave_common::bail;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::hash::{ParallelUnitId, ParallelUnitMapping, VirtualNode};
use risingwave_common::util::worker_util::get_pu_to_worker_mapping;
use risingwave_pb::common::{ParallelUnit, WorkerNode, WorkerType};

use crate::catalog::FragmentId;
use crate::scheduler::{SchedulerError, SchedulerResult};

/// `WorkerNodeManager` manages live worker nodes and table vnode mapping information.
pub struct WorkerNodeManager {
    inner: RwLock<WorkerNodeManagerInner>,
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
        let pu_mapping = guard.reschedule_serving(fragment_id)?;
        Ok(pu_mapping)
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
    fn reschedule_serving(
        &mut self,
        fragment_id: FragmentId,
    ) -> SchedulerResult<ParallelUnitMapping> {
        let all_serving_pus: BTreeMap<u32, Vec<ParallelUnit>> = self
            .worker_nodes
            .iter()
            .filter(|w| w.property.as_ref().map_or(false, |p| p.is_serving))
            .map(|w| (w.id, w.parallel_units.clone()))
            .collect();
        let serving_pus_total_num = all_serving_pus.values().map(|p| p.len()).sum::<usize>();
        if serving_pus_total_num == 0 {
            return Err(SchedulerError::EmptyWorkerNodes);
        }
        let streaming_vnode_mapping = self
            .streaming_fragment_vnode_mapping
            .get(&fragment_id)
            .ok_or_else(|| {
                anyhow!(
                    "streaming vnode mapping for fragment {} not found",
                    fragment_id
                )
            })?;
        let serving_parallelism = std::cmp::min(
            std::cmp::min(serving_pus_total_num, VirtualNode::COUNT),
            streaming_vnode_mapping.iter_unique().count(),
        );
        assert!(serving_parallelism > 0);
        // round-robin similar to `Scheduler` in meta node.
        let mut parallel_units: LinkedList<_> = all_serving_pus
            .into_values()
            .map(|v| v.into_iter().sorted_by_key(|p| p.id))
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
        round_robin.truncate(serving_parallelism);
        round_robin.sort_unstable_by_key(|p| p.id);
        let selected_serving_pus = ParallelUnitMapping::build(&round_robin);
        self.serving_fragment_vnode_mapping
            .insert(fragment_id, selected_serving_pus.clone());
        Ok(selected_serving_pus)
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
            self.manager
                .get_streaming_fragment_mapping(&fragment_id)
                .ok_or_else(|| {
                    SchedulerError::Internal(anyhow!(
                        "vnode mapping for fragment {} not found",
                        fragment_id
                    ))
                })
        } else {
            self.manager.get_serving_fragment_mapping(fragment_id)
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

/// Calculate a new vnode mapping, keeping locality and balance on a best effort basis.
/// The strategy is similar to `rebalance_actor_vnode` used in meta node, but is modified to meet
/// new constraints accordingly.
fn rebalance_serving_vnode(
    old_pu_mapping: &ParallelUnitMapping,
    old_workers: &[WorkerNode],
    added_workers: &[WorkerNode],
    removed_workers: &[WorkerNode],
    max_parallelism: usize,
) -> Option<ParallelUnitMapping> {
    let get_pu_map = |worker_nodes: &[WorkerNode]| {
        worker_nodes
            .iter()
            .filter(|w| w.property.as_ref().map_or(false, |p| p.is_serving))
            .map(|w| (w.id, w.parallel_units.clone()))
            .collect::<BTreeMap<u32, Vec<ParallelUnit>>>()
    };
    let removed_pu_map = get_pu_map(removed_workers);
    let mut new_pus: LinkedList<_> = get_pu_map(old_workers)
        .into_iter()
        .filter(|(w_id, _)| !removed_pu_map.contains_key(w_id))
        .chain(get_pu_map(added_workers))
        .map(|(_, pus)| pus.into_iter().sorted_by_key(|p| p.id))
        .collect();
    let serving_parallelism = std::cmp::min(
        new_pus.iter().map(|pus| pus.len()).sum(),
        std::cmp::min(max_parallelism, VirtualNode::COUNT),
    );
    let mut selected_pu_ids = Vec::new();
    while !new_pus.is_empty() {
        new_pus.drain_filter(|ps| {
            if let Some(p) = ps.next() {
                selected_pu_ids.push(p.id);
                false
            } else {
                true
            }
        });
    }
    // According to `reschedule_serving`, we know that inside a worker, parallel unit with smaller
    // id must have been used before one with larger id. So here we also prefer parallel units
    // with smaller id, to avoid unnecessary vnode reassignment.
    selected_pu_ids.drain(serving_parallelism..);
    let selected_pu_id_set: HashSet<ParallelUnitId> = selected_pu_ids.iter().cloned().collect();
    if selected_pu_id_set.is_empty() {
        return None;
    }

    #[derive(Debug)]
    struct Balance {
        pu_id: ParallelUnitId,
        balance: i32,
        builder: BitmapBuilder,
        is_temp: bool,
    }
    let (expected, mut remain) = VirtualNode::COUNT.div_rem(&selected_pu_ids.len());
    assert!(expected <= i32::MAX as usize);
    // TODO comments
    let mut balances: HashMap<ParallelUnitId, Balance> = HashMap::default();
    for pu_id in &selected_pu_ids {
        let mut balance = Balance {
            pu_id: *pu_id,
            balance: -(expected as i32),
            builder: BitmapBuilder::zeroed(VirtualNode::COUNT),
            is_temp: false,
        };
        if remain > 0 {
            balance.balance -= 1;
            remain -= 1;
        }
        balances.insert(*pu_id, balance);
    }

    // Assign pending vnodes to `temp_pu` temporarily. These vnodes will be reassigned later.
    let mut temp_pu = Balance {
        pu_id: 0,
        balance: 0,
        builder: BitmapBuilder::zeroed(VirtualNode::COUNT),
        is_temp: true,
    };
    for (vnode, pu_id) in old_pu_mapping.iter_with_vnode() {
        let b = if selected_pu_id_set.contains(&pu_id) {
            balances.get_mut(&pu_id).unwrap()
        } else {
            &mut temp_pu
        };
        b.balance += 1;
        b.builder.set(vnode.to_index(), true);
    }
    let mut balances: VecDeque<_> = balances
        .into_values()
        .chain(std::iter::once(temp_pu))
        .sorted_by_key(|b| b.balance)
        .rev()
        .collect();
    let mut results: HashMap<ParallelUnitId, Bitmap> = HashMap::default();
    while !balances.is_empty() {
        if balances.len() == 1 {
            let single = balances.pop_front().unwrap();
            assert_eq!(single.balance, 0);
            if !single.is_temp {
                results.insert(single.pu_id, single.builder.finish());
            }
            continue;
        }
        let mut src = balances.pop_front().unwrap();
        let mut dst = balances.pop_back().unwrap();
        let n = std::cmp::min(src.balance.abs(), dst.balance.abs());
        let mut moved = 0;
        for idx in 0..VirtualNode::COUNT {
            if moved >= n {
                break;
            }
            if src.builder.is_set(idx) {
                src.builder.set(idx, false);
                assert!(!dst.builder.is_set(idx));
                dst.builder.set(idx, true);
                moved += 1;
            }
        }
        src.balance -= n;
        dst.balance += n;
        if src.balance != 0 {
            balances.push_front(src);
        } else if !src.is_temp {
            results.insert(src.pu_id, src.builder.finish());
        }

        if dst.balance != 0 {
            balances.push_back(dst);
        } else if !dst.is_temp {
            results.insert(dst.pu_id, dst.builder.finish());
        }
    }

    Some(ParallelUnitMapping::from_bitmaps(&results))
}

#[cfg(test)]
mod tests {
    use risingwave_common::hash::{ParallelUnitId, ParallelUnitMapping, VirtualNode};
    use risingwave_common::util::addr::HostAddr;
    use risingwave_pb::common::worker_node::Property;
    use risingwave_pb::common::{worker_node, ParallelUnit, WorkerNode};

    use crate::scheduler::worker_node_manager::rebalance_serving_vnode;

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
    fn test_rebalance_serving_vnode() {
        assert_eq!(VirtualNode::COUNT, 256);
        let mut pu_id_counter: ParallelUnitId = 0;
        let serving_property = Property {
            is_streaming: false,
            is_serving: true,
        };
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
        let count_same_vnode_mapping = |pm1: &ParallelUnitMapping, pm2: &ParallelUnitMapping| {
            let mut count: usize = 0;
            for idx in 0..VirtualNode::COUNT {
                let vnode = VirtualNode::from_index(idx);
                if pm1.get(vnode.clone()) == pm2.get(vnode.clone()) {
                    count += 1;
                }
            }
            count
        };
        let worker_1 = WorkerNode {
            id: 1,
            parallel_units: gen_pus_for_worker(1, 1),
            property: Some(serving_property.clone()),
            ..Default::default()
        };
        let pu_mapping = ParallelUnitMapping::build(&worker_1.parallel_units);
        assert!(rebalance_serving_vnode(&pu_mapping, &[worker_1.clone()], &[], &[], 0).is_none());
        let re_pu_mapping =
            rebalance_serving_vnode(&pu_mapping, &[worker_1.clone()], &[], &[], 10000).unwrap();
        assert_eq!(re_pu_mapping, pu_mapping);
        assert_eq!(re_pu_mapping.iter_unique().count(), 1);
        let worker_2 = WorkerNode {
            id: 2,
            parallel_units: gen_pus_for_worker(2, 50),
            property: Some(serving_property.clone()),
            ..Default::default()
        };
        let re_pu_mapping = rebalance_serving_vnode(
            &re_pu_mapping,
            &[worker_1.clone()],
            &[worker_2.clone()],
            &[],
            10000,
        )
        .unwrap();
        assert_ne!(re_pu_mapping, pu_mapping);
        assert_eq!(re_pu_mapping.iter_unique().count(), 51);
        // 1*256+0 -> 5*51+1
        assert_eq!(count_same_vnode_mapping(&pu_mapping, &re_pu_mapping), 5 + 1);

        let worker_3 = WorkerNode {
            id: 3,
            parallel_units: gen_pus_for_worker(3, 60),
            property: Some(serving_property.clone()),
            ..Default::default()
        };
        let re_pu_mapping_2 = rebalance_serving_vnode(
            &re_pu_mapping,
            &[worker_1.clone(), worker_2.clone()],
            &[worker_3.clone()],
            &[],
            10000,
        )
        .unwrap();
        // limited by total pu number
        assert_eq!(re_pu_mapping_2.iter_unique().count(), 111);
        // TODO count_same_vnode_mapping
        let re_pu_mapping = rebalance_serving_vnode(
            &re_pu_mapping_2,
            &[worker_1.clone(), worker_2.clone(), worker_3.clone()],
            &[],
            &[],
            50,
        )
        .unwrap();
        // limited by max_parallelism
        assert_eq!(re_pu_mapping.iter_unique().count(), 50);
        // TODO count_same_vnode_mapping
        let re_pu_mapping_2 = rebalance_serving_vnode(
            &re_pu_mapping,
            &[worker_1.clone(), worker_2.clone(), worker_3.clone()],
            &[],
            &[],
            10000,
        )
        .unwrap();
        assert_eq!(re_pu_mapping_2.iter_unique().count(), 111);
        // TODO count_same_vnode_mapping
        let re_pu_mapping = rebalance_serving_vnode(
            &re_pu_mapping_2,
            &[worker_1.clone(), worker_2.clone(), worker_3.clone()],
            &[],
            &[worker_2.clone()],
            10000,
        )
        .unwrap();
        // limited by total pu number
        assert_eq!(re_pu_mapping.iter_unique().count(), 61);
        // TODO count_same_vnode_mapping
        assert!(rebalance_serving_vnode(
            &re_pu_mapping,
            &[worker_1.clone(), worker_3.clone()],
            &[],
            &[worker_1.clone(), worker_3.clone()],
            10000
        )
        .is_none());
        let re_pu_mapping = rebalance_serving_vnode(
            &re_pu_mapping,
            &[worker_1.clone(), worker_3.clone()],
            &[],
            &[worker_1.clone()],
            10000,
        )
        .unwrap();
        assert_eq!(re_pu_mapping.iter_unique().count(), 60);
        assert!(rebalance_serving_vnode(
            &re_pu_mapping,
            &[worker_3.clone()],
            &[],
            &[worker_3.clone()],
            10000
        )
        .is_none());
    }
}
