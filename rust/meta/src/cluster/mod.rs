use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use itertools::Itertools;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::try_match_expand;
use risingwave_pb::common::worker_node::State;
use risingwave_pb::common::{HostAddress, ParallelUnit, WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;

use crate::hummock::HummockManager;
use crate::manager::{
    HashDispatchManager, HashDispatchManagerRef, IdCategory, IdGeneratorManagerRef, MetaSrvEnv,
    NotificationManagerRef,
};
use crate::model::{MetadataModel, Worker, INVALID_EXPIRE_AT};
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
    max_heartbeat_interval: Duration,
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
        max_heartbeat_interval: Duration,
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
            max_heartbeat_interval,
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

    pub fn heartbeat(&self, worker_id: u32) -> Result<()> {
        // 1. Get unique key. TODO: avoid this step
        let key = match self
            .workers
            .iter()
            .find(|worker| worker.worker_id() == worker_id)
        {
            None => {
                return Ok(());
            }
            Some(worker_2) => worker_2.value().key()?,
        };
        // 2. Update expire_at
        let expire_at = SystemTime::now()
            .add(self.max_heartbeat_interval)
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("Clock may have gone backwards")
            .as_secs();
        self.update_worker_ttl(key, expire_at);
        Ok(())
    }

    fn update_worker_ttl(&self, host_address: HostAddress, expire_at: u64) {
        match self.workers.entry(WorkerKey(host_address)) {
            Entry::Occupied(mut worker) => {
                worker.get_mut().set_expire_at(expire_at);
            }
            Entry::Vacant(_) => {}
        }
    }

    pub fn start_heartbeat_checker(
        cluster_manager_ref: StoredClusterManagerRef<S>,
        check_interval: Duration,
    ) -> (JoinHandle<()>, UnboundedSender<()>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
        let join_handle = tokio::spawn(async move {
            let mut min_interval = tokio::time::interval(check_interval);
            loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_interval.tick() => {},
                    // Shutdown
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Heartbeat checker is shutting down");
                        return;
                    }
                }
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Clock may have gone backwards")
                    .as_secs();
                let workers_to_init_or_delete = cluster_manager_ref
                    .workers
                    .iter()
                    // TODO: Java frontend doesn't send heartbeat. Remove this line after we use
                    // Rust frontend.
                    .filter(|worker| worker.worker_type() != WorkerType::Frontend)
                    .filter(|worker| worker.expire_at() < now)
                    .map(|worker| worker.value().clone())
                    .collect_vec();
                for worker in workers_to_init_or_delete {
                    let key = worker.key().expect("illegal key");
                    if worker.expire_at() == INVALID_EXPIRE_AT {
                        // Initialize expire_at
                        cluster_manager_ref.update_worker_ttl(
                            key,
                            now + cluster_manager_ref.max_heartbeat_interval.as_secs(),
                        );
                        continue;
                    }
                    match cluster_manager_ref.delete_worker_node(key.clone()).await {
                        Ok(_) => {
                            tracing::debug!(
                                "Deleted expired worker {} {}:{}",
                                worker.worker_id(),
                                key.host,
                                key.port
                            );
                        }
                        Err(err) => {
                            tracing::warn!(
                                "Failed to delete worker {} {}:{}. {}",
                                worker.worker_id(),
                                key.host,
                                key.port,
                                err
                            );
                        }
                    }
                }
            }
        });
        (join_handle, shutdown_tx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::test_utils::setup_compute_env;

    #[tokio::test]
    async fn test_cluster_manager() -> Result<()> {
        Ok(())
    }

    // This test takes seconds because the TTL is measured in seconds.
    #[tokio::test]
    async fn test_heartbeat() {
        let (_env, _hummock_manager, cluster_manager, worker_node) = setup_compute_env(1).await;
        let context_id_1 = worker_node.id;
        let fake_host_address_2 = HostAddress {
            host: "127.0.0.1".to_string(),
            port: 2,
        };
        let (_worker_node_2, _) = cluster_manager
            .add_worker_node(fake_host_address_2, WorkerType::ComputeNode)
            .await
            .unwrap();
        // Two live nodes
        assert_eq!(
            cluster_manager
                .list_worker_node(WorkerType::ComputeNode, None)
                .len(),
            2
        );

        let ttl = cluster_manager.max_heartbeat_interval;
        let check_interval = std::cmp::min(Duration::from_millis(100), ttl / 4);

        // Keep worker 1 alive
        let cluster_manager_ref = cluster_manager.clone();
        let keep_alive_join_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(cluster_manager_ref.max_heartbeat_interval / 3).await;
                cluster_manager_ref.heartbeat(context_id_1).unwrap();
            }
        });

        tokio::time::sleep(ttl * 2 + check_interval).await;

        // One node has actually expired but still got two, because heartbeat check is not started.
        assert_eq!(
            cluster_manager
                .list_worker_node(WorkerType::ComputeNode, None)
                .len(),
            2
        );

        let (join_handle, shutdown_sender) =
            StoredClusterManager::start_heartbeat_checker(cluster_manager.clone(), check_interval);
        tokio::time::sleep(ttl * 2 + check_interval).await;

        // One live node left.
        assert_eq!(
            cluster_manager
                .list_worker_node(WorkerType::ComputeNode, None)
                .len(),
            1
        );

        shutdown_sender.send(()).unwrap();
        join_handle.await.unwrap();
        keep_alive_join_handle.abort();
    }
}
