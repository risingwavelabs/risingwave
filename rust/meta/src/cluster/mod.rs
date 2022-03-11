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
use risingwave_pb::common::{HostAddress, ParallelUnit, WorkerNode, WorkerType};
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

const DEFAULT_WORK_NODE_PARALLEL_DEGREE: usize = 8;

/// [`StoredClusterManager`] manager cluster/worker meta data in [`MetaStore`].
pub struct StoredClusterManager<S> {
    meta_store_ref: Arc<S>,
    id_gen_manager_ref: IdGeneratorManagerRef<S>,
    hummock_manager_ref: Option<Arc<HummockManager<S>>>,
    dispatch_manager_ref: HashDispatchManagerRef<S>,
    workers: DashMap<WorkerKey, Worker>,
    nm: NotificationManagerRef,
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
        let mut compute_nodes = Vec::new();

        workers.iter().for_each(|w| {
            worker_map.insert(WorkerKey(w.key().unwrap()), w.clone());
            if w.worker_type() == WorkerType::ComputeNode {
                compute_nodes.push(w.worker_node());
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

                let start_id = self
                    .id_gen_manager_ref
                    .generate_interval::<{ IdCategory::ParallelUnit }>(
                        DEFAULT_WORK_NODE_PARALLEL_DEGREE as i32,
                    )
                    .await? as usize;
                let parallel_units = (start_id..start_id + DEFAULT_WORK_NODE_PARALLEL_DEGREE)
                    .map(|id| ParallelUnit { id: id as u32 })
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
                        .add_worker_mapping(&worker.worker_node())
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
                Worker::delete(&*self.meta_store_ref, &host_address).await?;

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
                let worker = entry.1;
                if worker.worker_type() == WorkerType::ComputeNode {
                    self.dispatch_manager_ref
                        .delete_worker_mapping(&worker.worker_node())
                        .await?;
                }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_manager() -> Result<()> {
        Ok(())
    }
}
