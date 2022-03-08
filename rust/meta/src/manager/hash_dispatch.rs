#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::slice;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_pb::common::{ParallelUnit, ParallelUnitType, WorkerNode, WorkerType};
use tokio::sync::Mutex;

use crate::cluster::ParallelUnitId;
use crate::model::{ConsistentHashMapping, MetadataModel, VirtualKey};
use crate::storage::MetaStore;

const VIRTUAL_KEY_COUNT: usize = 2048;

pub type HashDispatchManagerRef<S> = Arc<HashDispatchManager<S>>;

/// `HashDispatchManager` maintains a load-balanced hash mapping based on consistent hash.
/// The mapping changes when one or more nodes enter or leave the cluster.
pub struct HashDispatchManager<S> {
    core: Mutex<HashDispatchManagerCore<S>>,
}

impl<S> HashDispatchManager<S>
where
    S: MetaStore,
{
    pub async fn new(compute_nodes: &[WorkerNode], meta_store_ref: Arc<S>) -> Result<Self> {
        let mut core = HashDispatchManagerCore::new(meta_store_ref);
        if !compute_nodes.is_empty() {
            let parallel_units = compute_nodes
                .iter()
                .flat_map(|node| node.parallel_units.to_owned())
                .filter(|parallel_unit| parallel_unit.r#type == ParallelUnitType::Hash as i32)
                .collect_vec();
            core.add_worker_mapping_from_empty(&parallel_units).await?;
        }
        Ok(Self {
            core: Mutex::new(core),
        })
    }

    pub async fn add_worker_mapping(&self, compute_node: &WorkerNode) -> Result<()> {
        assert_eq!(compute_node.r#type, WorkerType::ComputeNode as i32);
        let mut core = self.core.lock().await;
        let hash_parallel_units = compute_node
            .parallel_units
            .clone()
            .into_iter()
            .filter(|parallel_unit| parallel_unit.r#type == ParallelUnitType::Hash as i32)
            .collect_vec();
        if core.load_balancer.is_empty() {
            core.add_worker_mapping_from_empty(&hash_parallel_units)
                .await
        } else {
            core.add_worker_mapping(&hash_parallel_units).await
        }
    }

    pub async fn delete_worker_mapping(&self, compute_node: &WorkerNode) -> Result<()> {
        assert_eq!(compute_node.r#type, WorkerType::ComputeNode as i32);
        let mut core = self.core.lock().await;
        core.delete_worker_mapping(
            &compute_node
                .parallel_units
                .clone()
                .into_iter()
                .filter(|parallel_unit| parallel_unit.r#type == ParallelUnitType::Hash as i32)
                .collect_vec(),
        )
        .await
    }

    pub async fn delete_parallel_unit_mapping(&self, parallel_unit: &ParallelUnit) -> Result<()> {
        let mut core = self.core.lock().await;
        core.delete_worker_mapping(slice::from_ref(parallel_unit))
            .await
    }

    pub async fn get_worker_mapping(&self) -> Result<Vec<ParallelUnitId>> {
        let core = self.core.lock().await;
        Ok(core.key_mapping.get_mapping())
    }
}

/// [`HashDispatchManagerCore`] contains the core logic for mapping change when one or more nodes
/// enter or leave the cluster.
struct HashDispatchManagerCore<S> {
    /// Total number of hash parallel units in cluster.
    total_hash_parallels: usize,
    /// Mapping from virtual key to parallel unit, which is persistent.
    key_mapping: ConsistentHashMapping,
    /// Mapping from parallel unit to virtual key, which is volatile.
    owner_mapping: HashMap<ParallelUnitId, Vec<VirtualKey>>,
    /// Mapping from key count to parallel unit, aiming to maintain load balance.
    load_balancer: BTreeMap<usize, Vec<ParallelUnitId>>,
    /// Meta store used for persistency.
    meta_store_ref: Arc<S>,
}

// FIXME:
// 1. IO: Currently, `HashDispatchManager` only supports adding or deleting compute node one by one.
// Namely, IO occurs when each node is added or deleted, rather than flushing when the whole cluster
// has been updated. Therefore, the upper layer might need to provide an API for batch change of
// cluster.
// 2. Transaction: The logic in `HashDispatchManager` is just part of a transaction, but it is not
// currently supported.
// 3. Scale Out: Mapping should increase its number of slots when the cluster becomes too large, so
// that new nodes can still get some virtual key slots.
impl<S> HashDispatchManagerCore<S>
where
    S: MetaStore,
{
    fn new(meta_store_ref: Arc<S>) -> Self {
        let key_mapping = ConsistentHashMapping::new();
        let owner_mapping: HashMap<ParallelUnitId, Vec<VirtualKey>> = HashMap::new();
        let load_balancer: BTreeMap<usize, Vec<ParallelUnitId>> = BTreeMap::new();

        Self {
            total_hash_parallels: 0,
            key_mapping,
            owner_mapping,
            load_balancer,
            meta_store_ref,
        }
    }

    async fn add_worker_mapping_from_empty(
        &mut self,
        parallel_units: &[ParallelUnit],
    ) -> Result<()> {
        let mut key_mapping = Vec::with_capacity(VIRTUAL_KEY_COUNT);
        self.total_hash_parallels = parallel_units.len();
        let hash_shard_size = VIRTUAL_KEY_COUNT / self.total_hash_parallels;
        let mut init_bound = hash_shard_size;

        parallel_units.iter().for_each(|parallel_unit| {
            let parallel_unit_id = parallel_unit.id;
            key_mapping.resize(init_bound, parallel_unit_id);
            let virtual_keys = (init_bound - hash_shard_size..init_bound).collect();
            self.owner_mapping.insert(parallel_unit_id, virtual_keys);
            init_bound += hash_shard_size;
        });

        let mut parallel_unit_iter = parallel_units.iter().cycle();
        for virtual_key in init_bound - hash_shard_size..VIRTUAL_KEY_COUNT {
            let id = parallel_unit_iter.next().unwrap().id;
            key_mapping.push(id);
            self.owner_mapping.entry(id).or_default().push(virtual_key);
        }

        self.owner_mapping
            .iter()
            .for_each(|(parallel_unit_id, virtual_keys)| {
                let virtual_key_count = virtual_keys.len();
                self.load_balancer
                    .entry(virtual_key_count)
                    .or_default()
                    .push(*parallel_unit_id);
            });

        assert!(
            !self.load_balancer.is_empty(),
            "Cannot construct consistent hash mapping on cluster initialization."
        );

        self.key_mapping.set_mapping(key_mapping)?;

        self.key_mapping.insert(&*self.meta_store_ref).await?;

        dbg!(&self.load_balancer);

        Ok(())
    }

    async fn add_worker_mapping(&mut self, parallel_units: &[ParallelUnit]) -> Result<()> {
        self.total_hash_parallels += parallel_units.len();

        let hash_shard_size =
            (VIRTUAL_KEY_COUNT as f64 / self.total_hash_parallels as f64).round() as usize;
        let mut new_keys_count = parallel_units.len() * hash_shard_size;
        let mut new_keys = Vec::new();

        while new_keys_count != 0 {
            let mut entry = self
                .load_balancer
                .last_entry()
                .expect("HashDispatcherManager: load balancer should have at least one entry.");
            let load_count = *entry.key();

            // Delete candidate parallel unit from load balancer
            let candidate_parallel_unit = entry.get_mut().pop().unwrap_or_else(|| {
                panic!("HashDispatcherManager: expect to get some candidate parallel unit.");
            });
            if entry.get().is_empty() {
                self.load_balancer.pop_last();
            }

            let candidate_virtual_keys = self.owner_mapping
                .get_mut(&candidate_parallel_unit)
                .unwrap_or_else(|| {
                    panic!("HashDispatcherManager: expect virtual keys owned by parallel unit {} but got nothing.", candidate_parallel_unit);
                });

            // Delete candidate key from owner mapping
            let candidate_key = candidate_virtual_keys
                .pop()
                .unwrap_or_else(|| {
                    panic!("HashDispatcherManager: expect parallel units that own {} virtual keys but got nothing.", candidate_parallel_unit);
                });

            // Add candidate key to new keys for future key allocation
            new_keys.push(candidate_key);

            // Update candidate parallel unit in load balancer
            self.load_balancer
                .entry(load_count - 1)
                .or_default()
                .push(candidate_parallel_unit);

            new_keys_count -= 1;
        }

        for i in 0..parallel_units.len() {
            let parallel_unit_id = parallel_units[i].id;
            let allocated_keys = new_keys[i * hash_shard_size..(i + 1) * hash_shard_size].to_vec();

            // Update key mapping
            for key in allocated_keys.clone() {
                self.key_mapping.update_mapping(key, parallel_unit_id)?;
            }

            // Add new keys to owner mapping
            self.owner_mapping.insert(parallel_unit_id, allocated_keys);

            // Add new parallel unit to load balancer
            self.load_balancer
                .entry(hash_shard_size)
                .or_default()
                .push(parallel_unit_id);
        }

        // Persist mapping
        self.key_mapping.insert(&*self.meta_store_ref).await?;

        dbg!(&self.load_balancer);

        Ok(())
    }

    async fn delete_worker_mapping(&mut self, parallel_units: &[ParallelUnit]) -> Result<()> {
        assert!(
            !self.owner_mapping.is_empty(),
            "HashDispatcherManager: mapping is currently empty, cannot delete worker mapping."
        );

        let mut released_keys = Vec::new();

        parallel_units.iter().for_each(|parallel_unit| {
            // Delete parallel unit from owner mapping
            let parallel_unit_id = parallel_unit.id;
            let owned_keys = self.owner_mapping.remove(&parallel_unit_id).unwrap();

            // Delete parallel unit from load balancer
            let owned_key_count = owned_keys.len();
            self.load_balancer
                .get_mut(&owned_key_count)
                .unwrap_or_else(|| {
                    panic!("HashDispatcherManager: expect parallel units that own {} virtual keys but got nothing.", owned_key_count);
                })
                .retain(|&candidate_parallel_unit_id| candidate_parallel_unit_id != parallel_unit_id);

            if self.load_balancer.get(&owned_key_count).unwrap().is_empty() {
                self.load_balancer.remove(&owned_key_count);
            }

            // Add to released keys for future reallocation
            released_keys.extend(owned_keys);
        });

        // All compute nodes have been deleted from the cluster.
        if self.load_balancer.is_empty() {
            self.key_mapping.clear_mapping();
            return Ok(());
        }

        for released_key in released_keys {
            let mut entry = self
                .load_balancer
                .first_entry()
                .expect("HashDispatcherManager: load balancer should have at least one entry.");
            let load_count = *entry.key();
            let candidate_parallel_units = entry.get_mut();

            // Delete candidate parallel unit from load balancer
            let candidate_parallel_unit = candidate_parallel_units.pop().expect(
                "HashDispatcherManager: expect a parallel unit for virtual key allocation.",
            );
            if candidate_parallel_units.is_empty() {
                self.load_balancer.pop_first();
            }

            // Update key mapping
            self.key_mapping
                .update_mapping(released_key, candidate_parallel_unit)?;

            // Update owner mapping
            self.owner_mapping
                .entry(candidate_parallel_unit)
                .or_default()
                .push(released_key);

            // Update candidate parallel unit in load balancer
            self.load_balancer
                .entry(load_count + 1)
                .or_default()
                .push(candidate_parallel_unit);
        }

        // Persist mapping
        self.key_mapping.insert(&*self.meta_store_ref).await?;

        self.total_hash_parallels -= parallel_units.len();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_pb::common::worker_node::State;
    use risingwave_pb::common::{HostAddress, ParallelUnit, WorkerNode, WorkerType};

    use super::*;
    use crate::storage::MemStore;

    #[tokio::test]
    async fn test_hash_dispatch_manager() -> Result<()> {
        let meta_store_ref = Arc::new(MemStore::default());
        let mut current_id = 0u32;
        let worker_count = 10u32;
        let parallel_unit_per_node = 3u32;
        let host_address = HostAddress {
            host: "127.0.0.1".to_string(),
            port: 80,
        };
        let worker_nodes = (0..worker_count)
            .map(|node_id| {
                let parallel_units = (0..parallel_unit_per_node)
                    .map(|_| {
                        let parallel_unit = ParallelUnit {
                            id: current_id,
                            r#type: ParallelUnitType::Hash as i32,
                            node_host: None,
                        };
                        current_id += 1;
                        parallel_unit
                    })
                    .collect_vec();
                WorkerNode {
                    id: node_id,
                    r#type: WorkerType::ComputeNode as i32,
                    host: Some(host_address.clone()),
                    state: State::Starting as i32,
                    parallel_units,
                }
            })
            .collect_vec();

        let hash_dispatch_manager = HashDispatchManager::new(&[], meta_store_ref).await?;

        for node in &worker_nodes {
            hash_dispatch_manager.add_worker_mapping(node).await?;
            assert_core(&hash_dispatch_manager).await;
        }
        assert_parallel_unit_count(
            &hash_dispatch_manager,
            worker_count * parallel_unit_per_node,
        )
        .await;

        // Delete half of the nodes
        let mut deleted_count = 0u32;
        for node in &worker_nodes {
            if node.get_id() % 2 == 0 {
                hash_dispatch_manager.delete_worker_mapping(node).await?;
                deleted_count += 1;
                if deleted_count != worker_count {
                    assert_core(&hash_dispatch_manager).await;
                }
            }
        }
        assert_parallel_unit_count(
            &hash_dispatch_manager,
            (worker_count - deleted_count) * parallel_unit_per_node,
        )
        .await;

        // Delete the rest of the nodes
        for node in &worker_nodes {
            if node.get_id() % 2 == 1 {
                hash_dispatch_manager.delete_worker_mapping(node).await?;
                deleted_count += 1;
                if deleted_count != worker_count {
                    assert_core(&hash_dispatch_manager).await;
                }
            }
        }
        assert_parallel_unit_count(
            &hash_dispatch_manager,
            (worker_count - deleted_count) * parallel_unit_per_node,
        )
        .await;

        Ok(())
    }

    #[tokio::test]
    async fn test_hash_dispatch_manager_reboot() -> Result<()> {
        let meta_store_ref = Arc::new(MemStore::default());
        let mut current_id = 0u32;
        let init_worker_count = 3u32;
        let init_parallel_unit_per_node = 4u32;
        let host_address = HostAddress {
            host: "127.0.0.1".to_string(),
            port: 80,
        };
        let init_worker_nodes = (0..init_worker_count)
            .map(|node_id| {
                let parallel_units = (0..init_parallel_unit_per_node)
                    .map(|_| {
                        let parallel_unit = ParallelUnit {
                            id: current_id,
                            r#type: ParallelUnitType::Hash as i32,
                            node_host: None,
                        };
                        current_id += 1;
                        parallel_unit
                    })
                    .collect_vec();
                WorkerNode {
                    id: node_id,
                    r#type: WorkerType::ComputeNode as i32,
                    host: Some(host_address.clone()),
                    state: State::Starting as i32,
                    parallel_units,
                }
            })
            .collect_vec();

        let hash_dispatch_manager =
            HashDispatchManager::new(&init_worker_nodes, meta_store_ref).await?;
        assert_core(&hash_dispatch_manager).await;
        assert_parallel_unit_count(
            &hash_dispatch_manager,
            init_worker_count * init_parallel_unit_per_node,
        )
        .await;

        let worker_count = 10u32;
        let parallel_unit_per_node = 5u32;
        let worker_nodes = (init_worker_count..worker_count)
            .map(|node_id| {
                let parallel_units = (0..parallel_unit_per_node)
                    .map(|_| {
                        let parallel_unit = ParallelUnit {
                            id: current_id,
                            r#type: ParallelUnitType::Hash as i32,
                            node_host: None,
                        };
                        current_id += 1;
                        parallel_unit
                    })
                    .collect_vec();
                WorkerNode {
                    id: node_id,
                    r#type: WorkerType::ComputeNode as i32,
                    host: Some(host_address.clone()),
                    state: State::Starting as i32,
                    parallel_units,
                }
            })
            .collect_vec();

        for node in &worker_nodes {
            hash_dispatch_manager.add_worker_mapping(node).await?;
            assert_core(&hash_dispatch_manager).await;
        }
        assert_parallel_unit_count(
            &hash_dispatch_manager,
            init_worker_count * init_parallel_unit_per_node
                + (worker_count - init_worker_count) * parallel_unit_per_node,
        )
        .await;

        Ok(())
    }

    async fn assert_core(hash_dispatch_manager: &HashDispatchManager<MemStore>) {
        let core = hash_dispatch_manager.core.lock().await;
        assert_eq!(
            core.owner_mapping
                .iter()
                .map(|(_, keys)| { keys.len() })
                .sum::<usize>(),
            VIRTUAL_KEY_COUNT
        );
        assert_eq!(
            core.load_balancer
                .iter()
                .map(|(load_count, parallel_units)| { load_count * parallel_units.len() })
                .sum::<usize>(),
            VIRTUAL_KEY_COUNT
        );
        let key_mapping = core.key_mapping.get_mapping();
        let load_balancer = &core.load_balancer;
        let owner_mapping = &core.owner_mapping;
        for (&load_count, parallel_units) in load_balancer {
            for parallel_unit_id in parallel_units {
                assert_eq!(
                    key_mapping
                        .iter()
                        .filter(|&id| *id == *parallel_unit_id)
                        .count(),
                    load_count
                );
                assert_eq!(
                    owner_mapping.get(parallel_unit_id).unwrap().len(),
                    load_count
                );
            }
        }
    }

    async fn assert_parallel_unit_count(
        hash_dispatch_manager: &HashDispatchManager<MemStore>,
        parallel_unit_count: u32,
    ) {
        let core = hash_dispatch_manager.core.lock().await;
        assert_eq!(core.owner_mapping.keys().len() as u32, parallel_unit_count);
    }
}
