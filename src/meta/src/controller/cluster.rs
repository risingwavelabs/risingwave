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

use std::cmp;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use itertools::Itertools;
use risingwave_common::hash::ParallelUnitId;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_meta_model_v2::prelude::{Worker, WorkerProperty};
use risingwave_meta_model_v2::worker::{WorkerStatus, WorkerType};
use risingwave_meta_model_v2::{worker, worker_property, I32Array, TransactionId, WorkerId};
use risingwave_pb::common::worker_node::{PbProperty, PbState};
use risingwave_pb::common::{
    HostAddress, ParallelUnit, PbHostAddress, PbParallelUnit, PbWorkerNode, PbWorkerType,
};
use risingwave_pb::meta::add_worker_node_request::Property as AddNodeProperty;
use risingwave_pb::meta::heartbeat_request;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::update_worker_node_schedulability_request::Schedulability;
use sea_orm::prelude::Expr;
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QuerySelect,
    TransactionTrait,
};
use tokio::sync::oneshot::Sender;
use tokio::sync::{RwLock, RwLockReadGuard};
use tokio::task::JoinHandle;

use crate::manager::{LocalNotification, MetaSrvEnv, WorkerKey};
use crate::{MetaError, MetaResult};

pub type ClusterControllerRef = Arc<ClusterController>;

pub struct ClusterController {
    env: MetaSrvEnv,
    max_heartbeat_interval: Duration,
    inner: RwLock<ClusterControllerInner>,
}

struct WorkerInfo(worker::Model, Option<worker_property::Model>);

impl From<WorkerInfo> for PbWorkerNode {
    fn from(info: WorkerInfo) -> Self {
        Self {
            id: info.0.worker_id as _,
            r#type: PbWorkerType::from(info.0.worker_type) as _,
            host: Some(PbHostAddress {
                host: info.0.host,
                port: info.0.port,
            }),
            state: PbState::from(info.0.status) as _,
            parallel_units: info
                .1
                .as_ref()
                .map(|p| {
                    p.parallel_unit_ids
                        .0
                        .iter()
                        .map(|&id| PbParallelUnit {
                            id: id as _,
                            worker_node_id: info.0.worker_id as _,
                        })
                        .collect_vec()
                })
                .unwrap_or_default(),
            property: info.1.as_ref().map(|p| PbProperty {
                is_streaming: p.is_streaming,
                is_serving: p.is_serving,
                is_unschedulable: p.is_unschedulable,
            }),
            transactional_id: info.0.transaction_id.map(|id| id as _),
        }
    }
}

impl ClusterController {
    pub async fn new(env: MetaSrvEnv, max_heartbeat_interval: Duration) -> MetaResult<Self> {
        let meta_store = env
            .sql_meta_store()
            .expect("sql meta store is not initialized");
        let inner = ClusterControllerInner::new(meta_store.conn).await?;
        Ok(Self {
            env,
            max_heartbeat_interval,
            inner: RwLock::new(inner),
        })
    }

    /// Used in `NotificationService::subscribe`.
    /// Need to pay attention to the order of acquiring locks to prevent deadlock problems.
    pub async fn get_inner_guard(&self) -> RwLockReadGuard<'_, ClusterControllerInner> {
        self.inner.read().await
    }

    pub async fn count_worker_by_type(&self) -> MetaResult<HashMap<WorkerType, i32>> {
        self.inner.read().await.count_worker_by_type().await
    }

    /// A worker node will immediately register itself to meta when it bootstraps.
    /// The meta will assign it with a unique ID and set its state as `Starting`.
    /// When the worker node is fully ready to serve, it will request meta again
    /// (via `activate_worker_node`) to set its state to `Running`.
    pub async fn add_worker(
        &self,
        r#type: PbWorkerType,
        host_address: HostAddress,
        property: AddNodeProperty,
    ) -> MetaResult<WorkerId> {
        self.inner
            .write()
            .await
            .add_worker(r#type, host_address, property, self.max_heartbeat_interval)
            .await
    }

    pub async fn activate_worker(&self, worker_id: WorkerId) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let worker = inner.activate_worker(worker_id).await?;

        // Notify frontends of new compute node.
        // Always notify because a running worker's property may have been changed.
        if worker.r#type() == PbWorkerType::ComputeNode {
            self.env
                .notification_manager()
                .notify_frontend(Operation::Add, Info::Node(worker.clone()))
                .await;
        }
        self.env
            .notification_manager()
            .notify_local_subscribers(LocalNotification::WorkerNodeActivated(worker))
            .await;

        Ok(())
    }

    pub async fn delete_worker(&self, host_address: HostAddress) -> MetaResult<PbWorkerNode> {
        let mut inner = self.inner.write().await;
        let worker = inner.delete_worker(host_address).await?;
        if worker.r#type() == PbWorkerType::ComputeNode {
            self.env
                .notification_manager()
                .notify_frontend(Operation::Delete, Info::Node(worker.clone()))
                .await;
        }

        // Notify local subscribers.
        // Note: Any type of workers may pin some hummock resource. So `HummockManager` expect this
        // local notification.
        self.env
            .notification_manager()
            .notify_local_subscribers(LocalNotification::WorkerNodeDeleted(worker.clone()))
            .await;

        Ok(worker)
    }

    pub async fn update_schedulability(
        &self,
        worker_ids: Vec<WorkerId>,
        schedulability: Schedulability,
    ) -> MetaResult<()> {
        self.inner
            .write()
            .await
            .update_schedulability(worker_ids, schedulability)
            .await
    }

    /// Invoked when it receives a heartbeat from a worker node.
    pub async fn heartbeat(
        &self,
        worker_id: WorkerId,
        info: Vec<heartbeat_request::extra_info::Info>,
    ) {
        tracing::trace!(target: "events::meta::server_heartbeat", worker_id = worker_id, "receive heartbeat");
        self.inner
            .write()
            .await
            .heartbeat(worker_id, self.max_heartbeat_interval, info)
    }

    pub fn start_heartbeat_checker(
        cluster_controller: ClusterController,
        check_interval: Duration,
    ) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            let mut min_interval = tokio::time::interval(check_interval);
            min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_interval.tick() => {},
                    // Shutdown
                    _ = &mut shutdown_rx => {
                        tracing::info!("Heartbeat checker is stopped");
                        return;
                    }
                }

                let mut inner = cluster_controller.inner.write().await;
                // 1. Initialize new workers' TTL.
                for worker in inner
                    .worker_extra_info
                    .values_mut()
                    .filter(|worker| worker.expire_at.is_none())
                {
                    worker.update_ttl(cluster_controller.max_heartbeat_interval);
                }

                // 2. Collect expired workers.
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Clock may have gone backwards")
                    .as_secs();
                let worker_to_delete = inner
                    .worker_extra_info
                    .iter()
                    .filter(|(_, info)| info.expire_at.unwrap() < now)
                    .map(|(id, _)| *id)
                    .collect_vec();

                // 3. Delete expired workers.
                let worker_infos = match Worker::find()
                    .select_only()
                    .column(worker::Column::WorkerType)
                    .column(worker::Column::Host)
                    .column(worker::Column::Port)
                    .into_tuple::<(WorkerType, String, i32)>()
                    .all(&inner.db)
                    .await
                {
                    Ok(keys) => keys,
                    Err(err) => {
                        tracing::warn!("Failed to load expire worker info from db: {}", err);
                        continue;
                    }
                };

                if let Err(err) = Worker::delete_many()
                    .filter(worker::Column::WorkerId.is_in(worker_to_delete))
                    .exec(&inner.db)
                    .await
                {
                    tracing::warn!("Failed to delete expire workers from db: {}", err);
                    continue;
                }

                for (worker_type, host, port) in worker_infos {
                    match worker_type {
                        WorkerType::Frontend
                        | WorkerType::ComputeNode
                        | WorkerType::Compactor
                        | WorkerType::RiseCtl => {
                            cluster_controller
                                .env
                                .notification_manager()
                                .delete_sender(
                                    worker_type.into(),
                                    WorkerKey(HostAddress { host, port }),
                                )
                                .await
                        }
                        _ => {}
                    };
                }
            }
        });

        (join_handle, shutdown_tx)
    }

    /// Get live nodes with the specified type and state.
    /// # Arguments
    /// * `worker_type` `WorkerType` of the nodes
    /// * `worker_state` Filter by this state if it is not None.
    pub async fn list_workers(
        &self,
        worker_type: WorkerType,
        worker_status: Option<WorkerStatus>,
    ) -> MetaResult<Vec<PbWorkerNode>> {
        self.inner
            .read()
            .await
            .list_workers(worker_type, worker_status)
            .await
    }

    /// A convenient method to get all running compute nodes that may have running actors on them
    /// i.e. CNs which are running
    pub async fn list_active_streaming_workers(&self) -> MetaResult<Vec<PbWorkerNode>> {
        self.inner
            .read()
            .await
            .list_active_streaming_workers()
            .await
    }

    pub async fn list_active_parallel_units(&self) -> MetaResult<Vec<ParallelUnit>> {
        self.inner.read().await.list_active_parallel_units().await
    }

    /// Get the cluster info used for scheduling a streaming job, containing all nodes that are
    /// running and schedulable
    pub async fn list_active_serving_workers(&self) -> MetaResult<Vec<PbWorkerNode>> {
        self.inner.read().await.list_active_serving_workers().await
    }

    /// Get the cluster info used for scheduling a streaming job.
    pub async fn get_streaming_cluster_info(&self) -> MetaResult<StreamingClusterInfo> {
        self.inner.read().await.get_streaming_cluster_info().await
    }

    pub async fn get_worker_by_id(&self, worker_id: WorkerId) -> MetaResult<Option<PbWorkerNode>> {
        self.inner.read().await.get_worker_by_id(worker_id).await
    }

    pub async fn get_worker_info_by_id(&self, worker_id: WorkerId) -> Option<WorkerExtraInfo> {
        self.inner
            .read()
            .await
            .get_worker_extra_info_by_id(worker_id)
    }
}

#[derive(Default, Clone)]
pub struct WorkerExtraInfo {
    // Volatile values updated by meta node as follows.
    //
    // Unix timestamp that the worker will expire at.
    expire_at: Option<u64>,
    // Monotonic increasing id since meta node bootstrap.
    info_version_id: u64,
    // GC watermark.
    hummock_gc_watermark: Option<HummockSstableObjectId>,
}

impl WorkerExtraInfo {
    fn update_ttl(&mut self, ttl: Duration) {
        let expire = cmp::max(
            self.expire_at.unwrap_or_default(),
            SystemTime::now()
                .add(ttl)
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Clock may have gone backwards")
                .as_secs(),
        );
        self.expire_at = Some(expire);
    }

    fn update_hummock_info(&mut self, info: Vec<heartbeat_request::extra_info::Info>) {
        self.info_version_id += 1;
        for i in info {
            match i {
                heartbeat_request::extra_info::Info::HummockGcWatermark(watermark) => {
                    self.hummock_gc_watermark = Some(watermark);
                }
            }
        }
    }
}

/// The cluster info used for scheduling a streaming job.
#[derive(Debug, Clone)]
pub struct StreamingClusterInfo {
    /// All **active** compute nodes in the cluster.
    pub worker_nodes: HashMap<u32, PbWorkerNode>,

    /// All parallel units of the **active** compute nodes in the cluster.
    pub parallel_units: HashMap<ParallelUnitId, ParallelUnit>,

    /// All unschedulable parallel units of compute nodes in the cluster.
    pub unschedulable_parallel_units: HashMap<ParallelUnitId, ParallelUnit>,
}

pub struct ClusterControllerInner {
    db: DatabaseConnection,
    /// Record for tracking available machine ids, one is available.
    available_transactional_ids: VecDeque<TransactionId>,
    worker_extra_info: HashMap<WorkerId, WorkerExtraInfo>,
}

impl ClusterControllerInner {
    pub const MAX_WORKER_REUSABLE_ID_BITS: usize = 10;
    pub const MAX_WORKER_REUSABLE_ID_COUNT: usize = 1 << Self::MAX_WORKER_REUSABLE_ID_BITS;

    pub async fn new(db: DatabaseConnection) -> MetaResult<Self> {
        let workers: Vec<(WorkerId, Option<TransactionId>)> = Worker::find()
            .select_only()
            .column(worker::Column::WorkerId)
            .column(worker::Column::TransactionId)
            .into_tuple()
            .all(&db)
            .await?;
        let inuse_txn_ids: HashSet<_> = workers
            .iter()
            .cloned()
            .filter_map(|(_, txn_id)| txn_id)
            .collect();
        let available_transactional_ids = (0..Self::MAX_WORKER_REUSABLE_ID_COUNT as TransactionId)
            .filter(|id| !inuse_txn_ids.contains(id))
            .collect();

        let worker_extra_info = workers
            .into_iter()
            .map(|(w, _)| (w, WorkerExtraInfo::default()))
            .collect();

        Ok(Self {
            db,
            available_transactional_ids,
            worker_extra_info,
        })
    }

    pub async fn count_worker_by_type(&self) -> MetaResult<HashMap<WorkerType, i32>> {
        let workers: Vec<(WorkerType, i32)> = Worker::find()
            .select_only()
            .column(worker::Column::WorkerType)
            .column_as(worker::Column::WorkerId.count(), "count")
            .group_by(worker::Column::WorkerType)
            .into_tuple()
            .all(&self.db)
            .await?;

        Ok(workers.into_iter().collect())
    }

    pub fn update_worker_ttl(&mut self, worker_id: WorkerId, ttl: Duration) -> MetaResult<()> {
        if let Some(info) = self.worker_extra_info.get_mut(&worker_id) {
            let expire = cmp::max(
                info.expire_at.unwrap_or_default(),
                SystemTime::now()
                    .add(ttl)
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .expect("Clock may have gone backwards")
                    .as_secs(),
            );
            info.expire_at = Some(expire);
            Ok(())
        } else {
            Err(MetaError::invalid_worker(
                worker_id as _,
                "worker not found".into(),
            ))
        }
    }

    fn apply_transaction_id(&self, r#type: PbWorkerType) -> MetaResult<Option<TransactionId>> {
        match (self.available_transactional_ids.front(), r#type) {
            (None, _) => Err(MetaError::unavailable(
                "no available reusable machine id".to_string(),
            )),
            // We only assign transactional id to compute node and frontend.
            (Some(id), PbWorkerType::ComputeNode | PbWorkerType::Frontend) => Ok(Some(*id)),
            _ => Ok(None),
        }
    }

    pub async fn add_worker(
        &mut self,
        r#type: PbWorkerType,
        host_address: HostAddress,
        add_property: AddNodeProperty,
        ttl: Duration,
    ) -> MetaResult<WorkerId> {
        let txn = self.db.begin().await?;

        // TODO: remove this workaround when we deprecate parallel unit ids.
        let derive_parallel_units = |txn_id: TransactionId, start: i32, end: i32| {
            (start..end)
                .map(|idx| (idx << Self::MAX_WORKER_REUSABLE_ID_BITS) + txn_id)
                .collect_vec()
        };

        let worker = Worker::find()
            .filter(
                worker::Column::Host
                    .eq(host_address.host.clone())
                    .and(worker::Column::Port.eq(host_address.port)),
            )
            .find_also_related(WorkerProperty)
            .one(&txn)
            .await?;
        // Worker already exist.
        if let Some((worker, property)) = worker {
            assert_eq!(worker.worker_type, r#type.into());
            return if worker.worker_type == WorkerType::ComputeNode {
                let property = property.unwrap();
                let txn_id = worker.transaction_id.unwrap();
                let mut current_parallelism = property.parallel_unit_ids.0.clone();
                let new_parallelism = add_property.worker_node_parallelism as usize;

                match new_parallelism.cmp(&current_parallelism.len()) {
                    Ordering::Less => {
                        // Warn and keep the original parallelism if the worker registered with a
                        // smaller parallelism.
                        tracing::warn!(
                            "worker {} parallelism is less than current, current is {}, but received {}",
                            worker.worker_id,
                            current_parallelism.len(),
                            new_parallelism
                        );
                    }
                    Ordering::Greater => {
                        tracing::info!(
                            "worker {} parallelism updated from {} to {}",
                            worker.worker_id,
                            current_parallelism.len(),
                            new_parallelism
                        );
                        current_parallelism.extend(derive_parallel_units(
                            txn_id,
                            current_parallelism.len() as _,
                            new_parallelism as _,
                        ));
                    }
                    Ordering::Equal => {}
                }
                let mut property: worker_property::ActiveModel = property.into();

                // keep `is_unschedulable` unchanged.
                property.is_streaming = Set(add_property.is_streaming);
                property.is_serving = Set(add_property.is_serving);
                property.parallel_unit_ids = Set(I32Array(current_parallelism));

                WorkerProperty::update(property).exec(&txn).await?;
                txn.commit().await?;
                self.update_worker_ttl(worker.worker_id, ttl)?;
                Ok(worker.worker_id)
            } else {
                self.update_worker_ttl(worker.worker_id, ttl)?;
                Ok(worker.worker_id)
            };
        }
        let txn_id = self.apply_transaction_id(r#type)?;

        let worker = worker::ActiveModel {
            worker_id: Default::default(),
            worker_type: Set(r#type.into()),
            host: Set(host_address.host),
            port: Set(host_address.port),
            status: Set(WorkerStatus::Starting),
            transaction_id: Set(txn_id),
        };
        let insert_res = Worker::insert(worker).exec(&txn).await?;
        let worker_id = insert_res.last_insert_id as WorkerId;
        if r#type == PbWorkerType::ComputeNode {
            let property = worker_property::ActiveModel {
                worker_id: Set(worker_id),
                parallel_unit_ids: Set(I32Array(derive_parallel_units(
                    *txn_id.as_ref().unwrap(),
                    0,
                    add_property.worker_node_parallelism as _,
                ))),
                is_streaming: Set(add_property.is_streaming),
                is_serving: Set(add_property.is_streaming),
                is_unschedulable: Set(add_property.is_streaming),
            };
            WorkerProperty::insert(property).exec(&txn).await?;
        }

        txn.commit().await?;
        if let Some(txn_id) = txn_id {
            self.available_transactional_ids.retain(|id| *id != txn_id);
        }
        self.worker_extra_info
            .insert(worker_id, WorkerExtraInfo::default());

        Ok(worker_id)
    }

    pub async fn activate_worker(&self, worker_id: WorkerId) -> MetaResult<PbWorkerNode> {
        let worker = worker::ActiveModel {
            worker_id: Set(worker_id),
            status: Set(WorkerStatus::Running),
            ..Default::default()
        };

        let worker = worker.update(&self.db).await?;
        let worker_property = WorkerProperty::find_by_id(worker.worker_id)
            .one(&self.db)
            .await?;
        Ok(WorkerInfo(worker, worker_property).into())
    }

    pub async fn update_schedulability(
        &self,
        worker_ids: Vec<WorkerId>,
        schedulability: Schedulability,
    ) -> MetaResult<()> {
        let is_unschedulable = schedulability == Schedulability::Unschedulable;
        WorkerProperty::update_many()
            .col_expr(
                worker_property::Column::IsUnschedulable,
                Expr::value(is_unschedulable),
            )
            .filter(worker_property::Column::WorkerId.is_in(worker_ids))
            .exec(&self.db)
            .await?;

        Ok(())
    }

    pub async fn delete_worker(&mut self, host_addr: HostAddress) -> MetaResult<PbWorkerNode> {
        let worker = Worker::find()
            .filter(
                worker::Column::Host
                    .eq(host_addr.host)
                    .and(worker::Column::Port.eq(host_addr.port)),
            )
            .find_also_related(WorkerProperty)
            .one(&self.db)
            .await?;
        let Some((worker, property)) = worker else {
            return Err(MetaError::invalid_parameter("worker not found!"));
        };

        let res = Worker::delete_by_id(worker.worker_id)
            .exec(&self.db)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::invalid_parameter("worker not found!"));
        }

        self.worker_extra_info.remove(&worker.worker_id);
        if let Some(txn_id) = &worker.transaction_id {
            self.available_transactional_ids.push_back(*txn_id);
        }
        Ok(WorkerInfo(worker, property).into())
    }

    pub fn heartbeat(
        &mut self,
        worker_id: WorkerId,
        ttl: Duration,
        info: Vec<heartbeat_request::extra_info::Info>,
    ) {
        if let Some(worker_info) = self.worker_extra_info.get_mut(&worker_id) {
            worker_info.update_ttl(ttl);
            worker_info.update_hummock_info(info);
        }
    }

    pub async fn list_workers(
        &self,
        worker_type: WorkerType,
        worker_status: Option<WorkerStatus>,
    ) -> MetaResult<Vec<PbWorkerNode>> {
        let workers = if let Some(status) = worker_status {
            Worker::find()
                .filter(
                    worker::Column::WorkerType
                        .eq(worker_type)
                        .and(worker::Column::Status.eq(status)),
                )
                .find_also_related(WorkerProperty)
                .all(&self.db)
                .await?
        } else {
            Worker::find()
                .filter(worker::Column::WorkerType.eq(worker_type))
                .find_also_related(WorkerProperty)
                .all(&self.db)
                .await?
        };

        Ok(workers
            .into_iter()
            .map(|(worker, property)| WorkerInfo(worker, property).into())
            .collect_vec())
    }

    pub async fn list_active_streaming_workers(&self) -> MetaResult<Vec<PbWorkerNode>> {
        let workers = Worker::find()
            .filter(
                worker::Column::WorkerType
                    .eq(WorkerType::ComputeNode)
                    .and(worker::Column::Status.eq(WorkerStatus::Running)),
            )
            .inner_join(WorkerProperty)
            .select_also(WorkerProperty)
            .filter(worker_property::Column::IsStreaming.eq(true))
            .all(&self.db)
            .await?;

        Ok(workers
            .into_iter()
            .map(|(worker, property)| WorkerInfo(worker, property).into())
            .collect_vec())
    }

    pub async fn list_active_parallel_units(&self) -> MetaResult<Vec<ParallelUnit>> {
        let parallel_units: Vec<(WorkerId, I32Array)> = WorkerProperty::find()
            .select_only()
            .column(worker_property::Column::WorkerId)
            .column(worker_property::Column::ParallelUnitIds)
            .inner_join(Worker)
            .filter(worker::Column::Status.eq(WorkerStatus::Running))
            .into_tuple()
            .all(&self.db)
            .await?;
        Ok(parallel_units
            .into_iter()
            .flat_map(|(id, pu)| {
                pu.0.into_iter().map(move |parallel_unit_id| ParallelUnit {
                    id: parallel_unit_id as _,
                    worker_node_id: id as _,
                })
            })
            .collect_vec())
    }

    pub async fn list_active_serving_workers(&self) -> MetaResult<Vec<PbWorkerNode>> {
        let workers = Worker::find()
            .filter(
                worker::Column::WorkerType
                    .eq(WorkerType::ComputeNode)
                    .and(worker::Column::Status.eq(WorkerStatus::Running)),
            )
            .inner_join(WorkerProperty)
            .select_also(WorkerProperty)
            .filter(worker_property::Column::IsServing.eq(true))
            .all(&self.db)
            .await?;

        Ok(workers
            .into_iter()
            .map(|(worker, property)| WorkerInfo(worker, property).into())
            .collect_vec())
    }

    pub async fn get_streaming_cluster_info(&self) -> MetaResult<StreamingClusterInfo> {
        let mut streaming_workers = self.list_active_streaming_workers().await?;

        let unschedulable_worker_node = streaming_workers
            .extract_if(|worker| {
                worker
                    .property
                    .as_ref()
                    .map_or(false, |p| p.is_unschedulable)
            })
            .collect_vec();

        let active_workers: HashMap<_, _> =
            streaming_workers.into_iter().map(|w| (w.id, w)).collect();

        let active_parallel_units = active_workers
            .values()
            .flat_map(|worker| worker.parallel_units.iter().map(|p| (p.id, p.clone())))
            .collect();

        let unschedulable_parallel_units = unschedulable_worker_node
            .iter()
            .flat_map(|worker| worker.parallel_units.iter().map(|p| (p.id, p.clone())))
            .collect();

        Ok(StreamingClusterInfo {
            worker_nodes: active_workers,
            parallel_units: active_parallel_units,
            unschedulable_parallel_units,
        })
    }

    pub async fn get_worker_by_id(&self, worker_id: WorkerId) -> MetaResult<Option<PbWorkerNode>> {
        let worker = Worker::find_by_id(worker_id)
            .find_also_related(WorkerProperty)
            .one(&self.db)
            .await?;
        Ok(worker.map(|(w, p)| WorkerInfo(w, p).into()))
    }

    pub fn get_worker_extra_info_by_id(&self, worker_id: WorkerId) -> Option<WorkerExtraInfo> {
        self.worker_extra_info.get(&worker_id).cloned()
    }
}

#[cfg(test)]
#[cfg(not(madsim))]
mod tests {
    use super::*;

    fn mock_worker_hosts_for_test(count: usize) -> Vec<HostAddress> {
        (0..count)
            .map(|i| HostAddress {
                host: "localhost".to_string(),
                port: 5000 + i as i32,
            })
            .collect_vec()
    }

    #[tokio::test]
    async fn test_cluster_controller() -> MetaResult<()> {
        let env = MetaSrvEnv::for_test().await;
        let cluster_ctl = ClusterController::new(env, Duration::from_secs(1)).await?;

        let parallelism_num = 4_usize;
        let worker_count = 5_usize;
        let property = AddNodeProperty {
            worker_node_parallelism: parallelism_num as _,
            is_streaming: true,
            is_serving: true,
            is_unschedulable: false,
        };
        let hosts = mock_worker_hosts_for_test(worker_count);
        let mut worker_ids = vec![];
        for host in &hosts {
            worker_ids.push(
                cluster_ctl
                    .add_worker(PbWorkerType::ComputeNode, host.clone(), property.clone())
                    .await?,
            );
        }

        // Since no worker is active, the parallel unit count should be 0.
        assert_eq!(cluster_ctl.list_active_parallel_units().await?.len(), 0);

        for id in &worker_ids {
            cluster_ctl.activate_worker(*id).await?;
        }
        let worker_cnt_map = cluster_ctl.count_worker_by_type().await?;
        assert_eq!(
            *worker_cnt_map.get(&WorkerType::ComputeNode).unwrap() as usize,
            worker_count
        );
        assert_eq!(
            cluster_ctl.list_active_streaming_workers().await?.len(),
            worker_count
        );
        assert_eq!(
            cluster_ctl.list_active_serving_workers().await?.len(),
            worker_count
        );
        assert_eq!(
            cluster_ctl.list_active_parallel_units().await?.len(),
            parallelism_num * worker_count
        );

        // re-register existing worker node with larger parallelism and change its serving mode.
        let mut new_property = property.clone();
        new_property.worker_node_parallelism = (parallelism_num * 2) as _;
        new_property.is_serving = false;
        cluster_ctl
            .add_worker(PbWorkerType::ComputeNode, hosts[0].clone(), new_property)
            .await?;

        assert_eq!(
            cluster_ctl.list_active_streaming_workers().await?.len(),
            worker_count
        );
        assert_eq!(
            cluster_ctl.list_active_serving_workers().await?.len(),
            worker_count - 1
        );
        let parallel_units = cluster_ctl.list_active_parallel_units().await?;
        assert!(parallel_units.iter().map(|pu| pu.id).all_unique());
        assert_eq!(parallel_units.len(), parallelism_num * (worker_count + 1));

        // delete workers.
        for host in hosts {
            cluster_ctl.delete_worker(host).await?;
        }
        assert_eq!(cluster_ctl.list_active_streaming_workers().await?.len(), 0);
        assert_eq!(cluster_ctl.list_active_serving_workers().await?.len(), 0);
        assert_eq!(cluster_ctl.list_active_parallel_units().await?.len(), 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_update_schedulability() -> MetaResult<()> {
        let env = MetaSrvEnv::for_test().await;
        let cluster_ctl = ClusterController::new(env, Duration::from_secs(1)).await?;

        let host = HostAddress {
            host: "localhost".to_string(),
            port: 5001,
        };
        let mut property = AddNodeProperty {
            worker_node_parallelism: 4,
            is_streaming: true,
            is_serving: true,
            is_unschedulable: false,
        };
        let worker_id = cluster_ctl
            .add_worker(PbWorkerType::ComputeNode, host.clone(), property.clone())
            .await?;

        cluster_ctl.activate_worker(worker_id).await?;
        cluster_ctl
            .update_schedulability(vec![worker_id], Schedulability::Unschedulable)
            .await?;

        let workers = cluster_ctl.list_active_streaming_workers().await?;
        assert_eq!(workers.len(), 1);
        assert!(workers[0].property.as_ref().unwrap().is_unschedulable);

        // re-register existing worker node and change its serving mode, the schedulable state should not be changed.
        property.is_unschedulable = false;
        property.is_serving = false;
        let new_worker_id = cluster_ctl
            .add_worker(PbWorkerType::ComputeNode, host.clone(), property)
            .await?;
        assert_eq!(worker_id, new_worker_id);

        let workers = cluster_ctl.list_active_streaming_workers().await?;
        assert_eq!(workers.len(), 1);
        assert!(workers[0].property.as_ref().unwrap().is_unschedulable);

        cluster_ctl.delete_worker(host).await?;

        Ok(())
    }
}
