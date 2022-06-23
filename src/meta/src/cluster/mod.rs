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

use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use itertools::Itertools;
use risingwave_common::error::{internal_error, ErrorCode, Result};
use risingwave_common::try_match_expand;
use risingwave_common::types::ParallelUnitId;
use risingwave_pb::common::worker_node::State;
use risingwave_pb::common::{HostAddress, ParallelUnit, ParallelUnitType, WorkerNode, WorkerType};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use tokio::sync::oneshot::Sender;
use tokio::sync::{RwLock, RwLockReadGuard};
use tokio::task::JoinHandle;

use crate::manager::{IdCategory, LocalNotification, MetaSrvEnv};
use crate::model::{MetadataModel, Worker, INVALID_EXPIRE_AT};
use crate::storage::MetaStore;

pub type WorkerId = u32;
pub type WorkerLocations = HashMap<WorkerId, WorkerNode>;
pub type ClusterManagerRef<S> = Arc<ClusterManager<S>>;

pub const DEFAULT_WORK_NODE_PARALLEL_DEGREE: usize = 4;

#[derive(Debug)]
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

/// The id preserved for the meta node. Note that there's no such entry in cluster manager.
pub const META_NODE_ID: u32 = 0;

/// [`ClusterManager`] manager cluster/worker meta data in [`MetaStore`].
pub struct ClusterManager<S: MetaStore> {
    env: MetaSrvEnv<S>,

    max_heartbeat_interval: Duration,

    core: RwLock<ClusterManagerCore>,
}

impl<S> ClusterManager<S>
where
    S: MetaStore,
{
    pub async fn new(env: MetaSrvEnv<S>, max_heartbeat_interval: Duration) -> Result<Self> {
        let meta_store = env.meta_store_ref();
        let core = ClusterManagerCore::new(meta_store.clone()).await?;

        Ok(Self {
            env,
            max_heartbeat_interval,
            core: RwLock::new(core),
        })
    }

    /// Used in `NotificationService::subscribe`.
    /// Need to pay attention to the order of acquiring locks to prevent deadlock problems.
    pub async fn get_cluster_core_guard(&self) -> RwLockReadGuard<'_, ClusterManagerCore> {
        self.core.read().await
    }

    /// A worker node will immediately register itself to meta when it bootstraps.
    /// The meta will assign it with a unique ID and set its state as `Starting`.
    /// When the worker node is fully ready to serve, it will request meta again
    /// (via `activate_worker_node`) to set its state to `Running`.
    pub async fn add_worker_node(
        &self,
        host_address: HostAddress,
        r#type: WorkerType,
    ) -> Result<(WorkerNode, bool)> {
        let mut core = self.core.write().await;
        match core.get_worker_by_host(host_address.clone()) {
            Some(worker) => Ok((worker.to_protobuf(), false)),
            None => {
                // Generate worker id.
                let worker_id = self
                    .env
                    .id_gen_manager()
                    .generate::<{ IdCategory::Worker }>()
                    .await? as WorkerId;

                // Generate parallel units.
                let parallel_units = if r#type == WorkerType::ComputeNode {
                    self.generate_cn_parallel_units(DEFAULT_WORK_NODE_PARALLEL_DEGREE, worker_id)
                        .await?
                } else {
                    vec![]
                };

                // Construct worker.
                let worker_node = WorkerNode {
                    id: worker_id,
                    r#type: r#type as i32,
                    host: Some(host_address.clone()),
                    state: State::Starting as i32,
                    parallel_units,
                };

                let worker = Worker::from_protobuf(worker_node.clone());

                // Persist worker node.
                worker.insert(self.env.meta_store()).await?;

                // Update core.
                core.add_worker_node(worker);

                Ok((worker_node, true))
            }
        }
    }

    pub async fn activate_worker_node(&self, host_address: HostAddress) -> Result<()> {
        let mut core = self.core.write().await;
        let mut worker = core.get_worker_by_host_checked(host_address.clone())?;
        if worker.worker_node.state == State::Running as i32 {
            return Ok(());
        }
        worker.worker_node.state = State::Running as i32;
        worker.insert(self.env.meta_store()).await?;

        core.update_worker_node(worker.clone());

        // Notify frontends of new compute node.
        if worker.worker_node.r#type == WorkerType::ComputeNode as i32 {
            self.env
                .notification_manager()
                .notify_frontend(Operation::Add, Info::Node(worker.worker_node))
                .await;
        }

        Ok(())
    }

    pub async fn delete_worker_node(&self, host_address: HostAddress) -> Result<()> {
        let mut core = self.core.write().await;
        let worker = core.get_worker_by_host_checked(host_address.clone())?;
        let worker_type = worker.worker_type();
        let worker_node = worker.to_protobuf();

        // Persist deletion.
        Worker::delete(self.env.meta_store(), &host_address).await?;

        // Update core.
        core.delete_worker_node(worker);

        // Notify frontends to delete compute node.
        if worker_type == WorkerType::ComputeNode {
            self.env
                .notification_manager()
                .notify_frontend(Operation::Delete, Info::Node(worker_node.clone()))
                .await;
        }

        // Notify local subscribers.
        self.env
            .notification_manager()
            .notify_local_subscribers(LocalNotification::WorkerDeletion(worker_node))
            .await;

        Ok(())
    }

    /// Invoked when it receives a heartbeat from a worker node.
    pub async fn heartbeat(&self, worker_id: WorkerId) -> Result<()> {
        tracing::trace!(target: "events::meta::server_heartbeat", worker_id = worker_id, "receive heartbeat");
        let mut core = self.core.write().await;

        if let Some(worker) = core.get_worker_by_id(worker_id) {
            core.update_worker_ttl(worker.key().unwrap(), self.max_heartbeat_interval);
            Ok(())
        } else {
            Err(ErrorCode::UnknownWorker.into())
        }
    }

    pub async fn start_heartbeat_checker(
        cluster_manager: ClusterManagerRef<S>,
        check_interval: Duration,
    ) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            let mut min_interval = tokio::time::interval(check_interval);
            loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_interval.tick() => {},
                    // Shutdown
                    _ = &mut shutdown_rx => {
                        tracing::info!("Heartbeat checker is shutting down");
                        return;
                    }
                }
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Clock may have gone backwards")
                    .as_secs();
                let mut workers_to_init_or_delete = cluster_manager
                    .core
                    .read()
                    .await
                    .workers
                    .values()
                    .filter(|worker| worker.expire_at() < now)
                    .cloned()
                    .collect_vec();
                // 1. Initialize new workers' expire_at.
                for worker in
                    workers_to_init_or_delete.drain_filter(|w| w.expire_at() == INVALID_EXPIRE_AT)
                {
                    cluster_manager.core.write().await.update_worker_ttl(
                        worker.key().expect("illegal key"),
                        cluster_manager.max_heartbeat_interval,
                    );
                }
                // 2. Delete expired workers.
                for worker in workers_to_init_or_delete {
                    let key = worker.key().expect("illegal key");
                    match cluster_manager.delete_worker_node(key.clone()).await {
                        Ok(_) => {
                            cluster_manager
                                .env
                                .notification_manager()
                                .delete_sender(WorkerKey(key.clone()))
                                .await;
                            tracing::warn!(
                                "Deleted expired worker {} {}:{}; expired at {}, now {}",
                                worker.worker_id(),
                                key.host,
                                key.port,
                                worker.expire_at(),
                                now,
                            );
                        }
                        Err(err) => {
                            tracing::warn!(
                                "Failed to delete expired worker {} {}:{}; expired at {}, now {}. {:?}",
                                worker.worker_id(),
                                key.host,
                                key.port,
                                worker.expire_at(),
                                now,
                                err,
                            );
                        }
                    }
                }
            }
        });
        (join_handle, shutdown_tx)
    }

    /// Get live nodes with the specified type and state.
    /// # Arguments
    /// * `worker_type` `WorkerType` of the nodes
    /// * `worker_state` Filter by this state if it is not None.
    pub async fn list_worker_node(
        &self,
        worker_type: WorkerType,
        worker_state: Option<State>,
    ) -> Vec<WorkerNode> {
        let core = self.core.read().await;
        core.list_worker_node(worker_type, worker_state)
    }

    pub async fn list_parallel_units(
        &self,
        parallel_unit_type: Option<ParallelUnitType>,
    ) -> Vec<ParallelUnit> {
        let core = self.core.read().await;
        core.list_parallel_units(parallel_unit_type)
    }

    pub async fn get_parallel_unit_count(
        &self,
        parallel_unit_type: Option<ParallelUnitType>,
    ) -> usize {
        let core = self.core.read().await;
        core.get_parallel_unit_count(parallel_unit_type)
    }

    async fn generate_cn_parallel_units(
        &self,
        parallel_degree: usize,
        worker_id: WorkerId,
    ) -> Result<Vec<ParallelUnit>> {
        let start_id = self
            .env
            .id_gen_manager()
            .generate_interval::<{ IdCategory::ParallelUnit }>(parallel_degree as i32)
            .await? as ParallelUnitId;
        let mut parallel_units = Vec::with_capacity(parallel_degree);
        let single_parallel_unit = ParallelUnit {
            id: start_id,
            r#type: ParallelUnitType::Single as i32,
            worker_node_id: worker_id,
        };
        parallel_units.push(single_parallel_unit);
        (start_id + 1..start_id + parallel_degree as ParallelUnitId).for_each(|id| {
            let hash_parallel_unit = ParallelUnit {
                id,
                r#type: ParallelUnitType::Hash as i32,
                worker_node_id: worker_id,
            };
            parallel_units.push(hash_parallel_unit);
        });
        Ok(parallel_units)
    }

    pub async fn get_worker_by_id(&self, worker_id: WorkerId) -> Option<Worker> {
        self.core.read().await.get_worker_by_id(worker_id)
    }
}

pub struct ClusterManagerCore {
    /// Record for workers in the cluster.
    workers: HashMap<WorkerKey, Worker>,

    /// Record for parallel units of different types.
    single_parallel_units: Vec<ParallelUnit>,
    hash_parallel_units: Vec<ParallelUnit>,
}

impl ClusterManagerCore {
    async fn new<S>(meta_store: Arc<S>) -> Result<Self>
    where
        S: MetaStore,
    {
        let workers = try_match_expand!(Worker::list(&*meta_store).await, Ok, "Worker::list fail")?;
        let mut worker_map = HashMap::new();
        let mut single_parallel_units = Vec::new();
        let mut hash_parallel_units = Vec::new();

        workers.iter().for_each(|w| {
            worker_map.insert(WorkerKey(w.key().unwrap()), w.clone());
            w.worker_node
                .parallel_units
                .iter()
                .for_each(|parallel_unit| {
                    if parallel_unit.r#type == ParallelUnitType::Single as i32 {
                        single_parallel_units.push(parallel_unit.clone());
                    } else {
                        hash_parallel_units.push(parallel_unit.clone());
                    }
                });
        });

        Ok(Self {
            workers: worker_map,
            single_parallel_units,
            hash_parallel_units,
        })
    }

    /// If no worker exists, return an error.
    fn get_worker_by_host_checked(&self, host_address: HostAddress) -> Result<Worker> {
        self.get_worker_by_host(host_address)
            .ok_or_else(|| internal_error("Worker node does not exist!"))
    }

    fn get_worker_by_host(&self, host_address: HostAddress) -> Option<Worker> {
        self.workers.get(&WorkerKey(host_address)).cloned()
    }

    fn get_worker_by_id(&self, id: WorkerId) -> Option<Worker> {
        self.workers
            .iter()
            .find(|(_, worker)| worker.worker_id() == id)
            .map(|(_, worker)| worker.clone())
    }

    fn add_worker_node(&mut self, worker: Worker) {
        worker
            .worker_node
            .parallel_units
            .iter()
            .for_each(|parallel_unit| {
                if parallel_unit.r#type == ParallelUnitType::Single as i32 {
                    self.single_parallel_units.push(parallel_unit.clone());
                } else {
                    self.hash_parallel_units.push(parallel_unit.clone());
                }
            });
        self.workers
            .insert(WorkerKey(worker.key().unwrap()), worker);
    }

    fn update_worker_node(&mut self, worker: Worker) {
        self.workers
            .insert(WorkerKey(worker.key().unwrap()), worker);
    }

    fn delete_worker_node(&mut self, worker: Worker) {
        worker
            .worker_node
            .parallel_units
            .iter()
            .for_each(|parallel_unit| {
                if parallel_unit.r#type == ParallelUnitType::Single as i32 {
                    self.single_parallel_units
                        .retain(|p| p.id != parallel_unit.id);
                } else {
                    self.hash_parallel_units
                        .retain(|p| p.id != parallel_unit.id);
                }
            });
        self.workers.remove(&WorkerKey(worker.key().unwrap()));
    }

    pub fn list_worker_node(
        &self,
        worker_type: WorkerType,
        worker_state: Option<State>,
    ) -> Vec<WorkerNode> {
        self.workers
            .iter()
            .map(|(_, worker)| worker.to_protobuf())
            .filter(|w| w.r#type == worker_type as i32)
            .filter(|w| match worker_state {
                None => true,
                Some(state) => state as i32 == w.state,
            })
            .collect::<Vec<_>>()
    }

    fn list_parallel_units(
        &self,
        parallel_unit_type: Option<ParallelUnitType>,
    ) -> Vec<ParallelUnit> {
        match parallel_unit_type {
            Some(ParallelUnitType::Single) => self.single_parallel_units.clone(),
            Some(ParallelUnitType::Hash) => self.hash_parallel_units.clone(),
            None => {
                let mut all_parallel_units = self.single_parallel_units.clone();
                all_parallel_units.extend(self.hash_parallel_units.clone());
                all_parallel_units
            }
        }
    }

    fn get_parallel_unit_count(&self, parallel_unit_type: Option<ParallelUnitType>) -> usize {
        match parallel_unit_type {
            Some(ParallelUnitType::Single) => self.single_parallel_units.len(),
            Some(ParallelUnitType::Hash) => self.hash_parallel_units.len(),
            None => self.single_parallel_units.len() + self.hash_parallel_units.len(),
        }
    }

    fn update_worker_ttl(&mut self, host_address: HostAddress, ttl: Duration) {
        match self.workers.entry(WorkerKey(host_address)) {
            Entry::Occupied(mut worker) => {
                let expire_at = cmp::max(
                    worker.get().expire_at(),
                    SystemTime::now()
                        .add(ttl)
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("Clock may have gone backwards")
                        .as_secs(),
                );
                worker.get_mut().set_expire_at(expire_at);
            }
            Entry::Vacant(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::test_utils::setup_compute_env;
    use crate::storage::MemStore;

    #[tokio::test]
    async fn test_cluster_manager() -> Result<()> {
        let env = MetaSrvEnv::for_test().await;

        let cluster_manager = Arc::new(
            ClusterManager::new(env.clone(), Duration::new(0, 0))
                .await
                .unwrap(),
        );

        let mut worker_nodes = Vec::new();
        let worker_count = 5usize;
        for i in 0..worker_count {
            let fake_host_address = HostAddress {
                host: "localhost".to_string(),
                port: 5000 + i as i32,
            };
            let (worker_node, _) = cluster_manager
                .add_worker_node(fake_host_address, WorkerType::ComputeNode)
                .await
                .unwrap();
            worker_nodes.push(worker_node);
        }

        let single_parallel_count = worker_count;
        let hash_parallel_count = (DEFAULT_WORK_NODE_PARALLEL_DEGREE - 1) * worker_count;
        assert_cluster_manager(&cluster_manager, single_parallel_count, hash_parallel_count).await;

        let worker_to_delete_count = 4usize;
        for i in 0..worker_to_delete_count {
            let fake_host_address = HostAddress {
                host: "localhost".to_string(),
                port: 5000 + i as i32,
            };
            cluster_manager
                .delete_worker_node(fake_host_address)
                .await
                .unwrap();
        }
        assert_cluster_manager(&cluster_manager, 1, DEFAULT_WORK_NODE_PARALLEL_DEGREE - 1).await;

        Ok(())
    }

    async fn assert_cluster_manager(
        cluster_manager: &ClusterManager<MemStore>,
        single_parallel_count: usize,
        hash_parallel_count: usize,
    ) {
        let single_parallel_units = cluster_manager
            .list_parallel_units(Some(ParallelUnitType::Single))
            .await;
        let hash_parallel_units = cluster_manager
            .list_parallel_units(Some(ParallelUnitType::Hash))
            .await;
        assert_eq!(single_parallel_units.len(), single_parallel_count);
        assert_eq!(hash_parallel_units.len(), hash_parallel_count);
    }

    // This test takes seconds because the TTL is measured in seconds.
    #[tokio::test]
    #[ignore]
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
                .await
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
                cluster_manager_ref.heartbeat(context_id_1).await.unwrap();
            }
        });

        tokio::time::sleep(ttl * 2 + check_interval).await;

        // One node has actually expired but still got two, because heartbeat check is not
        // started.
        assert_eq!(
            cluster_manager
                .list_worker_node(WorkerType::ComputeNode, None)
                .await
                .len(),
            2
        );

        let (join_handle, shutdown_sender) =
            ClusterManager::start_heartbeat_checker(cluster_manager.clone(), check_interval).await;
        tokio::time::sleep(ttl * 2 + check_interval).await;

        // One live node left.
        assert_eq!(
            cluster_manager
                .list_worker_node(WorkerType::ComputeNode, None)
                .await
                .len(),
            1
        );

        shutdown_sender.send(()).unwrap();
        join_handle.await.unwrap();
        keep_alive_join_handle.abort();
    }
}
