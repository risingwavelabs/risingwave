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

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_common::hash::{VirtualNode, VIRTUAL_NODE_COUNT};
use risingwave_pb::common::{ParallelUnit, ParallelUnitType, WorkerNode, WorkerType};
use tokio::sync::Mutex;

use crate::cluster::ParallelUnitId;
use crate::storage::MetaStore;

/// Basic information to maintain a load-balanced hash mapping for stateful operator. This should
/// vary based on custom hash mapping strategy. Currently, all operators use the default strategy,
/// so they can share one `HashMappingInfo`.
struct HashMappingInfo {
    /// Mapping from virtual node to parallel unit, where the index is vnode. This should be
    /// persisted to storage.
    vnode_mapping: Vec<ParallelUnitId>,
    /// Mappings from parallel unit to virtual node.
    owner_mapping: HashMap<ParallelUnitId, Vec<VirtualNode>>,
    /// Mapping from vnode count to parallel unit, aiming to maintain load balance.
    load_balancer: BTreeMap<usize, Vec<ParallelUnitId>>,
    /// Total number of hash parallel units in cluster.
    total_hash_parallels: usize,
}

impl HashMappingInfo {
    fn new(
        vnode_mapping: Vec<ParallelUnitId>,
        owner_mapping: HashMap<ParallelUnitId, Vec<VirtualNode>>,
        load_balancer: BTreeMap<usize, Vec<ParallelUnitId>>,
        total_hash_parallels: usize,
    ) -> Self {
        Self {
            vnode_mapping,
            owner_mapping,
            load_balancer,
            total_hash_parallels,
        }
    }
}

#[derive(Eq, Hash, PartialEq, Clone)]
pub enum HashMappingStrategy {
    /// By default we use all the hash parallel units in the cluster to construct the hash mapping.
    Default,
    /// Not in use now. `TableId` might be changed into a struct describing the strategy in detail
    /// in the future.
    Custom(TableId),
}

impl HashMappingStrategy {
    fn satisfies(&self) -> bool {
        true
    }
}

pub type HashMappingManagerRef<S> = Arc<HashMappingManager<S>>;

/// `HashMappingManager` maintains a load-balanced hash mapping for each stateful operator based on
/// consistent hash. The mappings might change when one or more nodes enter or leave the cluster.
/// If we introduce custom hash mapping strategy for different MVs (or stateful operators) later,
/// the relationship between stateful operator and custom strategy should be maintained by
/// `HashMappingManager` as well.
pub struct HashMappingManager<S> {
    core: Mutex<HashMappingManagerCore<S>>,
}

impl<S> HashMappingManager<S>
where
    S: MetaStore,
{
    pub async fn new(meta_store: Arc<S>) -> Result<Self> {
        let core = HashMappingManagerCore::new(meta_store).await?;
        Ok(Self {
            core: Mutex::new(core),
        })
    }

    pub async fn build_table_mapping(&self, table_id: TableId, _strategy: HashMappingStrategy) -> Result<()> {
        let mut core = self.core.lock().await;
        // Currently, all tables share the default mapping.
        core.build_table_mapping(table_id, HashMappingStrategy::Default)
            .await
    }

    /// Modifies hash mappings for all operators when a node enters the cluster.
    pub async fn add_worker_mapping(&self, compute_node: &WorkerNode) -> Result<()> {
        assert_eq!(compute_node.r#type, WorkerType::ComputeNode as i32);
        let mut core = self.core.lock().await;
        let hash_parallel_units = compute_node
            .parallel_units
            .clone()
            .into_iter()
            .filter(|parallel_unit| parallel_unit.r#type == ParallelUnitType::Hash as i32)
            .collect_vec();
        // Currently, we only modify default mapping.
        match core.mapping_infos.get(&HashMappingStrategy::Default) {
            Some(_) => core.add_mapping(&hash_parallel_units).await,
            None => core.add_mapping_from_empty(&hash_parallel_units).await,
        }
    }

    /// Modifies hash mappings for all operators when a node leaves the cluster.
    pub async fn delete_worker_mapping(&self, compute_node: &WorkerNode) -> Result<()> {
        assert_eq!(compute_node.r#type, WorkerType::ComputeNode as i32);
        let mut core = self.core.lock().await;
        core.delete_mapping(
            &compute_node
                .parallel_units
                .clone()
                .into_iter()
                .filter(|parallel_unit| parallel_unit.r#type == ParallelUnitType::Hash as i32)
                .collect_vec(),
        )
        .await
    }

    pub async fn get_default_mapping(&self) -> Vec<ParallelUnitId> {
        let core = self.core.lock().await;
        core.default_mapping_info().vnode_mapping.clone()
    }
}

/// [`HashDispatchManagerCore`] contains the core logic for mapping change when one or more nodes
/// enter or leave the cluster.
struct HashMappingManagerCore<S> {
    /// Meta store used for persistence.
    meta_store: Arc<S>,
    /// Mapping from stateful operator table to its mapping strategy.
    // TODO: Persist to storage.
    operator_strategies: HashMap<TableId, HashMappingStrategy>,
    /// Mapping from mapping strategy to mapping info.
    mapping_infos: HashMap<HashMappingStrategy, HashMappingInfo>,
}

// FIXME:
// 1. IO: Currently, `HashDispatchManager` only supports adding or deleting compute node one by one.
// Namely, IO occurs when each node is added or deleted, rather than flushing when the whole cluster
// has been updated. Therefore, the upper layer might need to provide an API for batch change of
// cluster.
// 2. Transaction: The logic in `HashDispatchManager` is just part of a transaction, but it is not
// currently supported.
impl<S> HashMappingManagerCore<S>
where
    S: MetaStore,
{
    async fn new(meta_store: Arc<S>) -> Result<Self> {
        // TODO: read from storage
        Ok(Self {
            meta_store,
            operator_strategies: HashMap::new(),
            mapping_infos: HashMap::new(),
        })
    }

    fn get_table_mapping(&self, table_id: &TableId) -> Option<Vec<ParallelUnitId>> {
        self.operator_strategies.get(table_id).map(|strategy| {
            self.mapping_infos
                .get(strategy)
                .unwrap_or_else(|| panic!("expect hash mapping for table {}", table_id))
                .vnode_mapping
                .clone()
        })
    }

    async fn build_table_mapping(
        &mut self,
        table_id: TableId,
        _strategy: HashMappingStrategy,
    ) -> Result<()> {
        // Currently, we use default hash mappings for all the stateful operator.
        self.operator_strategies
            .insert(table_id, HashMappingStrategy::Default);
        // TODO: persist the info
        Ok(())
    }

    /// Called when the first compute node is added to the cluster. There should be nothing in hash mapping manager at this time, since user is not allowed to do computations without a compute node.
    async fn add_mapping_from_empty(
        &mut self,
        parallel_units: &[ParallelUnit],
    ) -> Result<()> {
        assert!(self.operator_strategies.is_empty());
        assert!(self.mapping_infos.is_empty());

        let mut vnode_mapping = Vec::with_capacity(VIRTUAL_NODE_COUNT);
        let mut owner_mapping: HashMap<ParallelUnitId, Vec<VirtualNode>> = HashMap::new();
        let mut load_balancer: BTreeMap<usize, Vec<ParallelUnitId>> = BTreeMap::new();
        let total_hash_parallels = parallel_units.len();
        let hash_shard_size = VIRTUAL_NODE_COUNT / total_hash_parallels;
        let mut init_bound = hash_shard_size;

        parallel_units.iter().for_each(|parallel_unit| {
            let parallel_unit_id = parallel_unit.id;
            vnode_mapping.resize(init_bound, parallel_unit_id);
            let vnodes = (init_bound - hash_shard_size..init_bound)
                .map(|id| id as VirtualNode)
                .collect();
            owner_mapping.insert(parallel_unit_id, vnodes);
            init_bound += hash_shard_size;
        });

        let mut parallel_unit_iter = parallel_units.iter().cycle();
        for vnode in init_bound - hash_shard_size..VIRTUAL_NODE_COUNT {
            let id = parallel_unit_iter.next().unwrap().id;
            vnode_mapping.push(id);
            owner_mapping
                .entry(id)
                .or_default()
                .push(vnode as VirtualNode);
        }

        owner_mapping.iter().for_each(|(parallel_unit_id, vnodes)| {
            let vnode_count = vnodes.len();
            load_balancer
                .entry(vnode_count)
                .or_default()
                .push(*parallel_unit_id);
        });

        assert!(
            !load_balancer.is_empty(),
            "cannot construct consistent hash mapping on cluster initialization."
        );

        let hash_mapping_info = HashMappingInfo::new(
            vnode_mapping,
            owner_mapping,
            load_balancer,
            total_hash_parallels,
        );

        // Since no hash mapping info has been constructed yet (as the name of the function
        // indicates), we should construct the mapping for default strategy.
        self.mapping_infos
            .insert(HashMappingStrategy::Default, hash_mapping_info);

        // TODO: persist the info

        Ok(())
    }

    async fn add_mapping(&mut self, parallel_units: &[ParallelUnit]) -> Result<()> {
        for (strategy, hash_mapping_info) in &mut self.mapping_infos {
            if strategy.satisfies() {
                hash_mapping_info.total_hash_parallels += parallel_units.len();

                let hash_shard_size = (VIRTUAL_NODE_COUNT as f64
                    / hash_mapping_info.total_hash_parallels as f64)
                    .round() as usize;
                let mut new_vnodes_count = parallel_units.len() * hash_shard_size;
                let mut new_vnodes = Vec::new();

                while new_vnodes_count != 0 {
                    let mut entry = hash_mapping_info
                        .load_balancer
                        .last_entry()
                        .expect("load balancer should have at least one entry");
                    let load_count = *entry.key();

                    // Delete candidate parallel unit from load balancer
                    let candidate_parallel_unit = entry.get_mut().pop().unwrap_or_else(|| {
                        panic!("expect to get some candidate parallel unit");
                    });
                    if entry.get().is_empty() {
                        hash_mapping_info.load_balancer.pop_last();
                    }

                    let candidate_vnodes = hash_mapping_info
                        .owner_mapping
                        .get_mut(&candidate_parallel_unit)
                        .unwrap_or_else(|| {
                            panic!(
                                "expect virtual nodes owned by parallel unit {} but got nothing",
                                candidate_parallel_unit
                            );
                        });

                    // Delete candidate vnode from owner mapping
                    let candidate_vnode = candidate_vnodes.pop().unwrap_or_else(|| {
                        panic!(
                            "expect parallel units that own {} virtual nodes but got nothing.",
                            candidate_parallel_unit
                        );
                    });

                    // Add candidate vnode to new vnodes for future vnode allocation
                    new_vnodes.push(candidate_vnode);

                    // Update candidate parallel unit in load balancer
                    hash_mapping_info
                        .load_balancer
                        .entry(load_count - 1)
                        .or_default()
                        .push(candidate_parallel_unit);

                    new_vnodes_count -= 1;
                }

                for i in 0..parallel_units.len() {
                    let parallel_unit_id = parallel_units[i].id;
                    let allocated_vnodes =
                        new_vnodes[i * hash_shard_size..(i + 1) * hash_shard_size].to_vec();

                    // Update vnode mapping
                    for vnode in allocated_vnodes.clone() {
                        let vnode = vnode as usize;
                        assert!(vnode < hash_mapping_info.vnode_mapping.len());
                        hash_mapping_info.vnode_mapping[vnode] = parallel_unit_id;
                    }

                    // Add new vnodes to owner mapping
                    hash_mapping_info
                        .owner_mapping
                        .insert(parallel_unit_id, allocated_vnodes);

                    // Add new parallel unit to load balancer
                    hash_mapping_info
                        .load_balancer
                        .entry(hash_shard_size)
                        .or_default()
                        .push(parallel_unit_id);
                }
            }
        }

        // TODO: persist the info

        Ok(())
    }

    async fn delete_mapping(&mut self, parallel_units: &[ParallelUnit]) -> Result<()> {
        for (strategy, hash_mapping_info) in &mut self.mapping_infos {
            if strategy.satisfies() {
                assert!(
                    !hash_mapping_info.owner_mapping.is_empty(),
                    "mapping is currently empty, cannot delete worker mapping"
                );

                let mut released_vnodes = Vec::new();

                parallel_units.iter().for_each(|parallel_unit| {
                    // Delete parallel unit from owner mapping
                    let parallel_unit_id = parallel_unit.id;
                    let owned_vnodes = hash_mapping_info.owner_mapping.remove(&parallel_unit_id).unwrap();

                    // Delete parallel unit from load balancer
                    let owned_vnode_count = owned_vnodes.len();
                    hash_mapping_info.load_balancer
                        .get_mut(&owned_vnode_count)
                        .unwrap_or_else(|| {
                            panic!("expect parallel units that own {} virtual nodes but got nothing", owned_vnode_count);
                        })
                        .retain(|&candidate_parallel_unit_id| candidate_parallel_unit_id != parallel_unit_id);

                    if hash_mapping_info.load_balancer.get(&owned_vnode_count).unwrap().is_empty() {
                        hash_mapping_info.load_balancer.remove(&owned_vnode_count);
                    }

                    // Add to released vnodes for future reallocation
                    released_vnodes.extend(owned_vnodes);
                });

                // All compute nodes have been deleted from the cluster.
                if hash_mapping_info.load_balancer.is_empty() {
                    hash_mapping_info.vnode_mapping.clear();
                    return Ok(());
                }

                for released_vnode in released_vnodes {
                    let mut entry = hash_mapping_info
                        .load_balancer
                        .first_entry()
                        .expect("load balancer should have at least one entry");
                    let load_count = *entry.key();
                    let candidate_parallel_units = entry.get_mut();

                    // Delete candidate parallel unit from load balancer
                    let candidate_parallel_unit = candidate_parallel_units
                        .pop()
                        .expect("expect a parallel unit for virtual node allocation");
                    if candidate_parallel_units.is_empty() {
                        hash_mapping_info.load_balancer.pop_first();
                    }

                    // Update vnode mapping
                    assert!((released_vnode as usize) < hash_mapping_info.vnode_mapping.len());
                    hash_mapping_info.vnode_mapping[released_vnode as usize] =
                        candidate_parallel_unit;

                    // Update owner mapping
                    hash_mapping_info
                        .owner_mapping
                        .entry(candidate_parallel_unit)
                        .or_default()
                        .push(released_vnode);

                    // Update candidate parallel unit in load balancer
                    hash_mapping_info
                        .load_balancer
                        .entry(load_count + 1)
                        .or_default()
                        .push(candidate_parallel_unit);
                }

                hash_mapping_info.total_hash_parallels -= parallel_units.len();
            }
        }

        // TODO: persist the info

        Ok(())
    }

    fn default_mapping_info(&self) -> &HashMappingInfo {
        self.mapping_infos
            .get(&HashMappingStrategy::Default)
            .unwrap()
    }
}
