use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use itertools::Itertools;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_pb::common::worker_node::State;
use risingwave_pb::common::{HostAddress, ParallelUnit, ParallelUnitType, WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};

use crate::hummock::HummockManager;
use crate::manager::{
    HashDispatchManager, HashDispatchManagerRef, IdCategory, IdGeneratorManagerRef, MetaSrvEnv,
    NotificationManagerRef,
};
use crate::model::{MetadataModel, Worker};
use crate::storage::MetaStore;

pub type NodeId = u32;
pub type ParallelUnitId = u32;
pub type NodeLocations = HashMap<NodeId, WorkerNode>;
pub type StoredClusterManagerRef<S> = Arc<StoredClusterManager<S>>;

const DEFAULT_WORKNODE_PARALLEL_DEGREE: usize = 8;
const DEFAULT_HASH_SIMPLE_RATIO: u32 = 10;

/// [`StoredClusterManager`] manager cluster/worker meta data in [`MetaStore`].
pub struct StoredClusterManager<S> {
    meta_store_ref: Arc<S>,
    id_gen_manager_ref: IdGeneratorManagerRef<S>,
    hummock_manager_ref: Option<Arc<HummockManager<S>>>,
    dispatch_manager_ref: HashDispatchManagerRef<S>,
    workers: DashMap<WorkerKey, Worker>,
    nm: NotificationManagerRef,
    /// Map from parallel unit type to corresponding parallel units.
    parallel_unit_map: DashMap<i32, Vec<ParallelUnit>>,
}

pub struct WorkerKey(pub HostAddress);

impl PartialEq<Self> for WorkerKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}
impl Eq for WorkerKey {}

impl Hash for WorkerKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.host.hash(state);
        self.0.port.hash(state);
    }
}

impl<S> StoredClusterManager<S>
where
    S: MetaStore,
{
    pub async fn new(
        env: MetaSrvEnv<S>,
        hummock_manager_ref: Option<Arc<HummockManager<S>>>,
        nm: NotificationManagerRef,
    ) -> Result<Self> {
        let meta_store_ref = env.meta_store_ref();
        let workers = try_match_expand!(
            Worker::list(&*meta_store_ref).await,
            Ok,
            "Worker::list fail"
        )?;
        let worker_map = DashMap::new();
        let parallel_unit_map: DashMap<i32, Vec<ParallelUnit>> = DashMap::new();
        let mut compute_nodes = Vec::new();

        workers.iter().for_each(|w| {
            worker_map.insert(WorkerKey(w.key().unwrap()), w.clone());
            let node = w.to_protobuf();
            node.parallel_units.iter().for_each(|parallel_unit| {
                parallel_unit_map
                    .entry(parallel_unit.r#type)
                    .or_default()
                    .push(parallel_unit.to_owned());
            });
            if w.worker_type() == WorkerType::ComputeNode {
                compute_nodes.push(node);
            }
        });

        let dispatch_manager_ref =
            Arc::new(HashDispatchManager::new(&compute_nodes, meta_store_ref.clone()).await?);

        Ok(Self {
            meta_store_ref,
            id_gen_manager_ref: env.id_gen_manager_ref(),
            hummock_manager_ref,
            dispatch_manager_ref,
            workers: worker_map,
            nm,
            parallel_unit_map,
        })
    }

    pub async fn add_worker_node(
        &self,
        host_address: HostAddress,
        r#type: WorkerType,
    ) -> Result<(WorkerNode, bool)> {
        match self.workers.entry(WorkerKey(host_address.clone())) {
            Entry::Occupied(o) => Ok((o.get().to_protobuf(), false)),
            Entry::Vacant(v) => {
                let worker_id = self
                    .id_gen_manager_ref
                    .generate::<{ IdCategory::Worker }>()
                    .await?;
                let parallel_degree = DEFAULT_WORKNODE_PARALLEL_DEGREE as usize;
                let start_id = self
                    .id_gen_manager_ref
                    .generate_interval::<{ IdCategory::ParallelUnit }>(parallel_degree as i32)
                    .await? as usize;
                let parallel_units = (start_id..start_id + parallel_degree)
                    .map(|id| {
                        let id = id as u32;
                        let r#type = if id % DEFAULT_HASH_SIMPLE_RATIO == 0 {
                            ParallelUnitType::Simple as i32
                        } else {
                            ParallelUnitType::Hash as i32
                        };
                        let parallel_unit = ParallelUnit {
                            id,
                            r#type,
                            node_host: Some(host_address.clone()),
                        };
                        self.parallel_unit_map
                            .entry(r#type)
                            .or_default()
                            .push(parallel_unit.clone());
                        parallel_unit
                    })
                    .collect_vec();

                let worker_node = WorkerNode {
                    id: worker_id as u32,
                    r#type: r#type as i32,
                    host: Some(host_address.clone()),
                    state: State::Starting as i32,
                    parallel_units,
                };

                let worker = Worker::from_protobuf(worker_node.clone());
                worker.insert(&*self.meta_store_ref).await?;

                // Alter consistent hash mapping
                if r#type == WorkerType::ComputeNode {
                    self.dispatch_manager_ref
                        .add_worker_mapping(&worker.to_protobuf())
                        .await?;
                }

                Ok((v.insert(worker).to_protobuf(), true))
            }
        }
    }

    pub async fn activate_worker_node(&self, host_address: HostAddress) -> Result<()> {
        match self.workers.entry(WorkerKey(host_address.clone())) {
            Entry::Occupied(mut entry) => {
                let mut worker_node = entry.get().to_protobuf();
                if worker_node.state == State::Running as i32 {
                    return Ok(());
                }
                worker_node.state = State::Running as i32;
                let worker = Worker::from_protobuf(worker_node.clone());
                worker.insert(self.meta_store_ref.as_ref()).await?;
                entry.insert(worker);

                // Notify frontends of new compute node.
                if worker_node.r#type == WorkerType::ComputeNode as i32 {
                    self.nm
                        .notify(
                            Operation::Add,
                            &Info::Node(worker_node),
                            crate::manager::NotificationTarget::Frontend,
                        )
                        .await?
                }

                Ok(())
            }
            Entry::Vacant(_) => Err(RwError::from(InternalError(
                "Worker node does not exist!".to_string(),
            ))),
        }
    }

    pub async fn delete_worker_node(&self, host_address: HostAddress) -> Result<()> {
        match self.workers.remove(&WorkerKey(host_address.clone())) {
            None => Err(RwError::from(InternalError(
                "Worker node does not exist!".to_string(),
            ))),
            Some(entry) => {
                let worker_node = entry.1.to_protobuf();

                // Notify frontends to delete compute node.
                if worker_node.r#type == WorkerType::ComputeNode as i32 {
                    self.nm
                        .notify(
                            Operation::Delete,
                            &Info::Node(worker_node),
                            crate::manager::NotificationTarget::Frontend,
                        )
                        .await?
                }

                if let Some(hummock_manager_ref) = self.hummock_manager_ref.as_ref() {
                    // It's desirable these operations are committed atomically.
                    // But meta store transaction across *Manager is not intuitive.
                    // TODO #93: So we rely on a safe guard that periodically purges hummock context
                    // resource owned by stale worker nodes.
                    hummock_manager_ref
                        .release_context_resource(entry.1.to_protobuf().id)
                        .await?;
                }

                // Delete and maybe reallocate parallel units in `parallel_unit_map` and
                // `dispatch_manager_ref`.
                let worker = entry.1;
                if worker.worker_type() == WorkerType::ComputeNode {
                    let node = worker.to_protobuf();
                    node.parallel_units.iter().for_each(|parallel_unit| {
                        match self.parallel_unit_map.entry(parallel_unit.r#type) {
                            Entry::Occupied(mut entry) => {
                                entry.get_mut().retain(|p| p != parallel_unit)
                            }
                            Entry::Vacant(_) => unreachable!(),
                        }
                    });
                    if self
                        .parallel_unit_map
                        .entry(ParallelUnitType::Simple as i32)
                        .or_default()
                        .is_empty()
                    {
                        self.reassign_hash_to_simple().await?;
                    }
                    self.dispatch_manager_ref
                        .delete_worker_mapping(&node)
                        .await?;
                }

                Worker::delete(&*self.meta_store_ref, &host_address).await?;

                Ok(())
            }
        }
    }

    /// Get live nodes with the specified type and state.
    /// # Arguments
    /// * `worker_type` `WorkerType` of the nodes
    /// * `worker_state` Filter by this state if it is not None.
    pub fn list_worker_node(
        &self,
        worker_type: WorkerType,
        worker_state: Option<State>,
    ) -> Vec<WorkerNode> {
        self.workers
            .iter()
            .map(|entry| entry.value().to_protobuf())
            .filter(|w| w.r#type == worker_type as i32)
            .filter(|w| match worker_state {
                None => true,
                Some(state) => state as i32 == w.state,
            })
            .collect::<Vec<_>>()
    }

    pub fn get_worker_count(&self, worker_type: WorkerType) -> usize {
        self.workers
            .iter()
            .filter(|entry| entry.value().worker_type() == worker_type)
            .count()
    }

    pub fn list_parallel_units(&self, parallel_unit_type: ParallelUnitType) -> Vec<ParallelUnit> {
        match self.parallel_unit_map.get(&(parallel_unit_type as i32)) {
            Some(kv_ref) => kv_ref.value().clone(),
            None => Vec::new(),
        }
    }

    /// When there is no parallel unit in responsible for simple dispatch due to a node deletion,
    /// one parallel unit of hash dispatch must be reassigned to simple dispatch.
    async fn reassign_hash_to_simple(&self) -> Result<()> {
        if let Some(candidate_parallel_unit) = self
            .parallel_unit_map
            .entry(ParallelUnitType::Hash as i32)
            .or_default()
            .pop()
        {
            let host_address = candidate_parallel_unit.node_host.clone().unwrap();
            match self.workers.entry(WorkerKey(host_address.clone())) {
                Entry::Occupied(mut entry) => {
                    let mut worker_node = entry.get().to_protobuf();
                    let idx = worker_node
                        .parallel_units
                        .iter()
                        .position(|parallel_unit| parallel_unit.id == candidate_parallel_unit.id)
                        .ok_or_else(|| {
                            RwError::from(InternalError(format!(
                                "Parallel unit {} does not exist in node with address {}:{}",
                                candidate_parallel_unit.id, host_address.host, host_address.port
                            )))
                        })?;
                    let node_parallel_unit =
                        worker_node.parallel_units.get_mut(idx).ok_or_else(|| {
                            RwError::from(InternalError(format!(
                                "Parallel unit {} does not exist in node with address {}:{}",
                                candidate_parallel_unit.id, host_address.host, host_address.port
                            )))
                        })?;
                    node_parallel_unit.r#type = ParallelUnitType::Simple as i32;

                    self.parallel_unit_map
                        .entry(ParallelUnitType::Simple as i32)
                        .or_default()
                        .push(node_parallel_unit.clone());

                    let worker = Worker::from_protobuf(worker_node.clone());
                    worker.insert(self.meta_store_ref.as_ref()).await?;
                    entry.insert(worker);

                    self.dispatch_manager_ref
                        .delete_parallel_unit_mapping(&candidate_parallel_unit)
                        .await?;

                    Ok(())
                }
                Entry::Vacant(_) => Err(RwError::from(InternalError(
                    "Worker node does not exist!".to_string(),
                ))),
            }
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::manager::NotificationManager;
    use crate::rpc::metrics::MetaMetrics;
    use crate::storage::MemStore;

    #[tokio::test]
    async fn test_cluster_manager() -> Result<()> {
        let env = MetaSrvEnv::for_test().await;
        let hummock_manager = Arc::new(
            HummockManager::new(env.clone(), Arc::new(MetaMetrics::new()))
                .await
                .unwrap(),
        );
        let cluster_manager = Arc::new(
            StoredClusterManager::new(
                env.clone(),
                Some(hummock_manager.clone()),
                Arc::new(NotificationManager::new()),
            )
            .await
            .unwrap(),
        );

        let mut worker_nodes = Vec::new();
        let worker_count = 5usize;
        for i in 0..worker_count {
            let fake_host_address = HostAddress {
                host: "127.0.0.1".to_string(),
                port: 5000 + i as i32,
            };
            let (worker_node, _) = cluster_manager
                .add_worker_node(fake_host_address, WorkerType::ComputeNode)
                .await
                .unwrap();
            worker_nodes.push(worker_node);
        }

        let simple_parallel_count = (DEFAULT_WORKNODE_PARALLEL_DEGREE * worker_count - 1)
            / DEFAULT_WORKNODE_PARALLEL_DEGREE;
        let hash_parallel_count =
            DEFAULT_WORKNODE_PARALLEL_DEGREE * worker_count - simple_parallel_count;
        assert_cluster_manager(&cluster_manager, simple_parallel_count, hash_parallel_count);

        let worker_to_delete_count = 4usize;
        for i in 0..worker_to_delete_count {
            let fake_host_address = HostAddress {
                host: "127.0.0.1".to_string(),
                port: 5000 + i as i32,
            };
            cluster_manager
                .delete_worker_node(fake_host_address)
                .await
                .unwrap();
        }
        assert_cluster_manager(&cluster_manager, 1, DEFAULT_WORKNODE_PARALLEL_DEGREE - 1);

        let mapping = cluster_manager
            .dispatch_manager_ref
            .get_worker_mapping()
            .await
            .unwrap();
        let unique_parallel_units = HashSet::<u32>::from_iter(mapping.into_iter());
        assert_eq!(
            unique_parallel_units.len(),
            DEFAULT_WORKNODE_PARALLEL_DEGREE - 1
        );

        Ok(())
    }

    fn assert_cluster_manager(
        cluster_manager: &StoredClusterManager<MemStore>,
        simple_parallel_count: usize,
        hash_parallel_count: usize,
    ) {
        let simple_parallel_units = cluster_manager.list_parallel_units(ParallelUnitType::Simple);
        let hash_parallel_units = cluster_manager.list_parallel_units(ParallelUnitType::Hash);
        assert_eq!(simple_parallel_units.len(), simple_parallel_count);
        assert_eq!(hash_parallel_units.len(), hash_parallel_count);
    }
}
