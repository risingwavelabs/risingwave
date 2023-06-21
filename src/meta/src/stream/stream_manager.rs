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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::future::{try_join_all, BoxFuture};
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_pb::catalog::Table;
use risingwave_pb::stream_plan::update_mutation::MergeUpdate;
use risingwave_pb::stream_plan::Dispatcher;
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, DropActorsRequest, UpdateActorsRequest,
};
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use uuid::Uuid;

use super::Locations;
use crate::barrier::{BarrierScheduler, Command};
use crate::hummock::HummockManagerRef;
use crate::manager::{ClusterManagerRef, FragmentManagerRef, MetaSrvEnv};
use crate::model::{ActorId, TableFragments};
use crate::storage::MetaStore;
use crate::stream::SourceManagerRef;
use crate::{MetaError, MetaResult};

pub type GlobalStreamManagerRef<S> = Arc<GlobalStreamManager<S>>;

/// [`CreateStreamingJobContext`] carries one-time infos for creating a streaming job.
///
/// Note: for better readability, keep this struct complete and immutable once created.
#[cfg_attr(test, derive(Default))]
pub struct CreateStreamingJobContext {
    /// New dispatchers to add from upstream actors to downstream actors.
    pub dispatchers: HashMap<ActorId, Vec<Dispatcher>>,

    /// Upstream mview actor ids grouped by table id.
    pub upstream_mview_actors: HashMap<TableId, Vec<ActorId>>,

    /// Internal tables in the streaming job.
    pub internal_tables: HashMap<u32, Table>,

    /// The locations of the actors to build in the streaming job.
    pub building_locations: Locations,

    /// The locations of the existing actors, essentially the upstream mview actors to update.
    pub existing_locations: Locations,

    /// The properties of the streaming job.
    // TODO: directly store `StreamingJob` here.
    pub table_properties: HashMap<String, String>,

    /// DDL definition.
    pub definition: String,

    pub mv_table_id: Option<u32>,
}

impl CreateStreamingJobContext {
    pub fn internal_tables(&self) -> Vec<Table> {
        self.internal_tables.values().cloned().collect()
    }
}

pub enum CreatingState {
    Failed { reason: MetaError },
    Canceling,
    Created,
}

struct StreamingJobExecution {
    id: TableId,
    shutdown_tx: Option<Sender<CreatingState>>,
}

impl StreamingJobExecution {
    fn new(id: TableId, shutdown_tx: Sender<CreatingState>) -> Self {
        Self {
            id,
            shutdown_tx: Some(shutdown_tx),
        }
    }
}

#[derive(Default)]
struct CreatingStreamingJobInfo {
    streaming_jobs: Mutex<HashMap<TableId, StreamingJobExecution>>,
}

impl CreatingStreamingJobInfo {
    async fn add_job(&self, job: StreamingJobExecution) {
        let mut jobs = self.streaming_jobs.lock().await;
        jobs.insert(job.id, job);
    }

    async fn delete_job(&self, job_id: TableId) {
        let mut jobs = self.streaming_jobs.lock().await;
        jobs.remove(&job_id);
    }

    async fn cancel_jobs(&self, job_ids: Vec<TableId>) {
        let mut jobs = self.streaming_jobs.lock().await;
        for job_id in job_ids {
            if let Some(job) = jobs.get_mut(&job_id)
                && let Some(shutdown_tx) = job.shutdown_tx.take()
            {
                let _ = shutdown_tx
                    .send(CreatingState::Canceling)
                    .await
                    .inspect_err(|_| {
                        tracing::warn!("failed to send canceling state");
                    });
            }
        }
    }
}

type CreatingStreamingJobInfoRef = Arc<CreatingStreamingJobInfo>;

/// [`ReplaceTableContext`] carries one-time infos for replacing the plan of an existing table.
///
/// Note: for better readability, keep this struct complete and immutable once created.
pub struct ReplaceTableContext {
    /// The old table fragments to be replaced.
    pub old_table_fragments: TableFragments,

    /// The updates to be applied to the downstream chain actors. Used for schema change.
    pub merge_updates: Vec<MergeUpdate>,

    /// The locations of the actors to build in the new table to replace.
    pub building_locations: Locations,

    /// The locations of the existing actors, essentially the downstream chain actors to update.
    pub existing_locations: Locations,

    /// The properties of the streaming job.
    // TODO: directly store `StreamingJob here.
    pub table_properties: HashMap<String, String>,
}

/// `GlobalStreamManager` manages all the streams in the system.
pub struct GlobalStreamManager<S: MetaStore> {
    pub(crate) env: MetaSrvEnv<S>,

    /// Manages definition and status of fragments and actors
    pub(super) fragment_manager: FragmentManagerRef<S>,

    /// Broadcasts and collect barriers
    pub(crate) barrier_scheduler: BarrierScheduler<S>,

    /// Maintains information of the cluster
    pub(crate) cluster_manager: ClusterManagerRef<S>,

    /// Maintains streaming sources from external system like kafka
    pub(crate) source_manager: SourceManagerRef<S>,

    /// Creating streaming job info.
    creating_job_info: CreatingStreamingJobInfoRef,

    hummock_manager: HummockManagerRef<S>,

    pub(crate) streaming_job_lock: Mutex<()>,
}

impl<S> GlobalStreamManager<S>
where
    S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        fragment_manager: FragmentManagerRef<S>,
        barrier_scheduler: BarrierScheduler<S>,
        cluster_manager: ClusterManagerRef<S>,
        source_manager: SourceManagerRef<S>,
        hummock_manager: HummockManagerRef<S>,
    ) -> MetaResult<Self> {
        Ok(Self {
            env,
            fragment_manager,
            barrier_scheduler,
            cluster_manager,
            source_manager,
            hummock_manager,
            creating_job_info: Arc::new(CreatingStreamingJobInfo::default()),
            streaming_job_lock: Mutex::new(()),
        })
    }

    /// Create streaming job, it works as follows:
    ///
    /// 1. Broadcast the actor info based on the scheduling result in the context, build the hanging
    /// channels in upstream worker nodes.
    /// 2. (optional) Get the split information of the `StreamSource` via source manager and patch
    /// actors.
    /// 3. Notify related worker nodes to update and build the actors.
    /// 4. Store related meta data.
    pub async fn create_streaming_job(
        self: &Arc<Self>,
        table_fragments: TableFragments,
        ctx: CreateStreamingJobContext,
    ) -> MetaResult<()> {
        let table_id = table_fragments.table_id();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(10);
        let execution = StreamingJobExecution::new(table_id, sender.clone());
        self.creating_job_info.add_job(execution).await;

        let stream_manager = self.clone();
        tokio::spawn(async move {
            let mut revert_funcs = vec![];
            let res = stream_manager
                .create_streaming_job_impl(&mut revert_funcs, table_fragments, ctx)
                .await;
            match res {
                Ok(_) => {
                    let _ = sender
                        .send(CreatingState::Created)
                        .await
                        .inspect_err(|_| tracing::warn!("failed to notify created: {table_id}"));
                }
                Err(err) => {
                    for revert_func in revert_funcs.into_iter().rev() {
                        revert_func.await;
                    }
                    let _ = sender
                        .send(CreatingState::Failed {
                            reason: err.clone(),
                        })
                        .await
                        .inspect_err(|_| {
                            tracing::warn!("failed to notify failed: {table_id}, err: {err}")
                        });
                }
            }
        });

        let res = try {
            while let Some(state) = receiver.recv().await {
                match state {
                    CreatingState::Failed { reason } => {
                        return Err(reason);
                    }
                    CreatingState::Canceling => {
                        if let Ok(table_fragments) = self
                            .fragment_manager
                            .select_table_fragments_by_table_id(&table_id)
                            .await
                        {
                            // try to cancel buffered creating command.
                            if self
                                .barrier_scheduler
                                .try_cancel_scheduled_create(table_id)
                                .await
                            {
                                tracing::debug!(
                                    "cancelling streaming job {table_id} in buffer queue."
                                );
                                let node_actors = table_fragments.worker_actor_ids();
                                let cluster_info =
                                    self.cluster_manager.get_streaming_cluster_info().await;
                                let node_actors = node_actors
                                    .into_iter()
                                    .map(|(id, actor_ids)| {
                                        (
                                            cluster_info.worker_nodes.get(&id).cloned().unwrap(),
                                            actor_ids,
                                        )
                                    })
                                    .collect_vec();
                                let futures = node_actors.into_iter().map(|(node, actor_ids)| {
                                    let request_id = Uuid::new_v4().to_string();
                                    async move {
                                        let client =
                                            self.env.stream_client_pool().get(&node).await?;
                                        let request = DropActorsRequest {
                                            request_id,
                                            actor_ids,
                                        };
                                        client.drop_actors(request).await
                                    }
                                });
                                try_join_all(futures).await?;

                                self.fragment_manager
                                    .drop_table_fragments_vec(&HashSet::from_iter(std::iter::once(
                                        table_id,
                                    )))
                                    .await?;
                                return Err(MetaError::cancelled("create".into()));
                            }
                            if !table_fragments.is_created() {
                                tracing::debug!(
                                    "cancelling streaming job {table_id} by issue cancel command."
                                );
                                self.barrier_scheduler
                                    .run_command(Command::CancelStreamingJob(table_fragments))
                                    .await?;
                                return Err(MetaError::cancelled("create".into()));
                            }
                        }
                    }
                    CreatingState::Created => return Ok(()),
                }
            }
        };

        self.creating_job_info.delete_job(table_id).await;
        res
    }

    async fn build_actors(
        &self,
        table_fragments: &TableFragments,
        building_locations: &Locations,
        existing_locations: &Locations,
    ) -> MetaResult<()> {
        let actor_map = table_fragments.actor_map();

        // Actors on each stream node will need to know where their upstream lies. `actor_info`
        // includes such information. It contains:
        // 1. actors in the current create-streaming-job request.
        // 2. all upstream actors.
        let actor_infos_to_broadcast = building_locations
            .actor_infos()
            .chain(existing_locations.actor_infos())
            .collect_vec();

        let building_worker_actors = building_locations.worker_actors();

        // We send RPC request in two stages.
        // The first stage does 2 things: broadcast actor info, and send local actor ids to
        // different WorkerNodes. Such that each WorkerNode knows the overall actor
        // allocation, but not actually builds it. We initialize all channels in this stage.
        for (worker_id, actors) in &building_worker_actors {
            let worker_node = building_locations.worker_locations.get(worker_id).unwrap();
            let client = self.env.stream_client_pool().get(worker_node).await?;

            client
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_infos_to_broadcast.clone(),
                })
                .await?;

            let stream_actors = actors
                .iter()
                .map(|actor_id| actor_map[actor_id].clone())
                .collect::<Vec<_>>();

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "update actors");
            client
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: stream_actors.clone(),
                })
                .await?;
        }

        // In the second stage, each [`WorkerNode`] builds local actors and connect them with
        // channels.
        for (worker_id, actors) in building_worker_actors {
            let worker_node = building_locations.worker_locations.get(&worker_id).unwrap();
            let client = self.env.stream_client_pool().get(worker_node).await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "build actors");
            client
                .build_actors(BuildActorsRequest {
                    request_id,
                    actor_id: actors,
                })
                .await?;
        }

        Ok(())
    }

    async fn create_streaming_job_impl(
        &self,
        revert_funcs: &mut Vec<BoxFuture<'_, ()>>,
        table_fragments: TableFragments,
        CreateStreamingJobContext {
            dispatchers,
            upstream_mview_actors,
            table_properties,
            building_locations,
            existing_locations,
            definition,
            mv_table_id,
            internal_tables,
            ..
        }: CreateStreamingJobContext,
    ) -> MetaResult<()> {
        // Register to compaction group beforehand.
        let hummock_manager_ref = self.hummock_manager.clone();
        let registered_table_ids = hummock_manager_ref
            .register_table_fragments(
                mv_table_id,
                internal_tables.keys().copied().collect(),
                &table_properties,
            )
            .await?;
        debug_assert_eq!(
            registered_table_ids.len(),
            table_fragments.internal_table_ids().len() + mv_table_id.map_or(0, |_| 1)
        );
        revert_funcs.push(Box::pin(async move {
            if let Err(e) = hummock_manager_ref.unregister_table_ids(&registered_table_ids).await {
                tracing::warn!("Failed to unregister compaction group for {:#?}. They will be cleaned up on node restart. {:#?}", registered_table_ids, e);
            }
        }));

        self.build_actors(&table_fragments, &building_locations, &existing_locations)
            .await?;

        // Add table fragments to meta store with state: `State::Initial`.
        self.fragment_manager
            .start_create_table_fragments(table_fragments.clone())
            .await?;

        let table_id = table_fragments.table_id();

        let init_split_assignment = self.source_manager.pre_allocate_splits(&table_id).await?;

        if let Err(err) = self
            .barrier_scheduler
            .run_command(Command::CreateStreamingJob {
                table_fragments,
                upstream_mview_actors,
                dispatchers,
                init_split_assignment,
                definition: definition.to_string(),
            })
            .await
        {
            self.fragment_manager
                .drop_table_fragments_vec(&HashSet::from_iter(std::iter::once(table_id)))
                .await?;
            return Err(err);
        }

        Ok(())
    }

    pub async fn replace_table(
        &self,
        table_fragments: TableFragments,
        ReplaceTableContext {
            old_table_fragments,
            merge_updates,
            building_locations,
            existing_locations,
            table_properties: _,
        }: ReplaceTableContext,
    ) -> MetaResult<()> {
        self.build_actors(&table_fragments, &building_locations, &existing_locations)
            .await?;

        // Add table fragments to meta store with state: `State::Initial`.
        self.fragment_manager
            .start_create_table_fragments(table_fragments.clone())
            .await?;

        let dummy_table_id = table_fragments.table_id();

        if let Err(err) = self
            .barrier_scheduler
            .run_command_with_paused(Command::ReplaceTable {
                old_table_fragments,
                new_table_fragments: table_fragments,
                merge_updates,
            })
            .await
        {
            self.fragment_manager
                .drop_table_fragments_vec(&HashSet::from_iter(std::iter::once(dummy_table_id)))
                .await?;
            return Err(err);
        }

        Ok(())
    }

    /// Drop streaming jobs by barrier manager, and clean up all related resources. The error will
    /// be ignored because the recovery process will take over it in cleaning part. Check
    /// [`Command::DropStreamingJobs`] for details.
    pub async fn drop_streaming_jobs(&self, streaming_job_ids: Vec<TableId>) {
        let _ = self
            .drop_streaming_jobs_impl(streaming_job_ids)
            .await
            .inspect_err(|err| {
                tracing::error!(error = ?err, "Failed to drop streaming jobs");
            });
    }

    pub async fn drop_streaming_jobs_impl(&self, table_ids: Vec<TableId>) -> MetaResult<()> {
        let table_fragments_vec = self
            .fragment_manager
            .select_table_fragments_by_ids(&table_ids)
            .await?;

        self.source_manager
            .drop_source_change(&table_fragments_vec)
            .await;

        self.barrier_scheduler
            .run_command(Command::DropStreamingJobs(table_ids.into_iter().collect()))
            .await?;

        // Unregister from compaction group afterwards.
        if let Err(e) = self
            .hummock_manager
            .unregister_table_fragments_vec(&table_fragments_vec)
            .await
        {
            tracing::warn!(
                    "Failed to unregister compaction group for {:#?}. They will be cleaned up on node restart. {:#?}",
                    table_fragments_vec,
                    e
                );
        }

        Ok(())
    }

    pub async fn cancel_streaming_jobs(&self, table_ids: Vec<TableId>) {
        self.creating_job_info.cancel_jobs(table_ids).await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::ParallelUnitMapping;
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::meta::add_worker_node_request::Property;
    use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
    use risingwave_pb::meta::table_fragments::Fragment;
    use risingwave_pb::stream_plan::stream_node::NodeBody;
    use risingwave_pb::stream_plan::*;
    use risingwave_pb::stream_service::stream_service_server::{
        StreamService, StreamServiceServer,
    };
    use risingwave_pb::stream_service::{
        BroadcastActorInfoTableResponse, BuildActorsResponse, DropActorsRequest,
        DropActorsResponse, InjectBarrierRequest, InjectBarrierResponse, UpdateActorsResponse, *,
    };
    use tokio::sync::oneshot::Sender;
    #[cfg(feature = "failpoints")]
    use tokio::sync::Notify;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::barrier::GlobalBarrierManager;
    use crate::hummock::{CompactorManager, HummockManager};
    use crate::manager::{
        CatalogManager, CatalogManagerRef, ClusterManager, FragmentManager, MetaSrvEnv,
        StreamingClusterInfo,
    };
    use crate::model::{ActorId, FragmentId};
    use crate::rpc::metrics::MetaMetrics;
    use crate::storage::MemStore;
    use crate::stream::SourceManager;
    use crate::MetaOpts;

    struct FakeFragmentState {
        actor_streams: Mutex<HashMap<ActorId, StreamActor>>,
        actor_ids: Mutex<HashSet<ActorId>>,
        actor_infos: Mutex<HashMap<ActorId, HostAddress>>,
    }

    struct FakeStreamService {
        inner: Arc<FakeFragmentState>,
    }

    #[async_trait::async_trait]
    impl StreamService for FakeStreamService {
        async fn update_actors(
            &self,
            request: Request<UpdateActorsRequest>,
        ) -> std::result::Result<Response<UpdateActorsResponse>, Status> {
            let req = request.into_inner();
            let mut guard = self.inner.actor_streams.lock().unwrap();
            for actor in req.get_actors() {
                guard.insert(actor.get_actor_id(), actor.clone());
            }

            Ok(Response::new(UpdateActorsResponse { status: None }))
        }

        async fn build_actors(
            &self,
            request: Request<BuildActorsRequest>,
        ) -> std::result::Result<Response<BuildActorsResponse>, Status> {
            let req = request.into_inner();
            let mut guard = self.inner.actor_ids.lock().unwrap();
            for id in req.get_actor_id() {
                guard.insert(*id);
            }

            Ok(Response::new(BuildActorsResponse {
                request_id: "".to_string(),
                status: None,
            }))
        }

        async fn broadcast_actor_info_table(
            &self,
            request: Request<BroadcastActorInfoTableRequest>,
        ) -> std::result::Result<Response<BroadcastActorInfoTableResponse>, Status> {
            let req = request.into_inner();
            let mut guard = self.inner.actor_infos.lock().unwrap();
            for info in req.get_info() {
                guard.insert(info.get_actor_id(), info.get_host()?.clone());
            }

            Ok(Response::new(BroadcastActorInfoTableResponse {
                status: None,
            }))
        }

        async fn drop_actors(
            &self,
            _request: Request<DropActorsRequest>,
        ) -> std::result::Result<Response<DropActorsResponse>, Status> {
            Ok(Response::new(DropActorsResponse::default()))
        }

        async fn force_stop_actors(
            &self,
            _request: Request<ForceStopActorsRequest>,
        ) -> std::result::Result<Response<ForceStopActorsResponse>, Status> {
            Ok(Response::new(ForceStopActorsResponse::default()))
        }

        async fn inject_barrier(
            &self,
            _request: Request<InjectBarrierRequest>,
        ) -> std::result::Result<Response<InjectBarrierResponse>, Status> {
            Ok(Response::new(InjectBarrierResponse::default()))
        }

        async fn barrier_complete(
            &self,
            _request: Request<BarrierCompleteRequest>,
        ) -> std::result::Result<Response<BarrierCompleteResponse>, Status> {
            Ok(Response::new(BarrierCompleteResponse::default()))
        }

        async fn wait_epoch_commit(
            &self,
            _request: Request<WaitEpochCommitRequest>,
        ) -> std::result::Result<Response<WaitEpochCommitResponse>, Status> {
            unimplemented!()
        }
    }

    struct MockServices {
        global_stream_manager: GlobalStreamManagerRef<MemStore>,
        catalog_manager: CatalogManagerRef<MemStore>,
        fragment_manager: FragmentManagerRef<MemStore>,
        state: Arc<FakeFragmentState>,
        join_handle_shutdown_txs: Vec<(JoinHandle<()>, Sender<()>)>,
    }

    impl MockServices {
        async fn start(host: &str, port: u16, enable_recovery: bool) -> MetaResult<Self> {
            let addr = SocketAddr::new(host.parse().unwrap(), port);
            let state = Arc::new(FakeFragmentState {
                actor_streams: Mutex::new(HashMap::new()),
                actor_ids: Mutex::new(HashSet::new()),
                actor_infos: Mutex::new(HashMap::new()),
            });

            let fake_service = FakeStreamService {
                inner: state.clone(),
            };

            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let stream_srv = StreamServiceServer::new(fake_service);
            let join_handle = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(stream_srv)
                    .serve_with_shutdown(addr, async move { shutdown_rx.await.unwrap() })
                    .await
                    .unwrap();
            });

            sleep(Duration::from_secs(1)).await;

            let env = MetaSrvEnv::for_test_opts(Arc::new(MetaOpts::test(enable_recovery))).await;
            let system_params = env.system_params_manager().get_params().await;
            let meta_metrics = Arc::new(MetaMetrics::new());
            let cluster_manager =
                Arc::new(ClusterManager::new(env.clone(), Duration::from_secs(3600)).await?);
            let host = HostAddress {
                host: host.to_string(),
                port: port as i32,
            };
            let fake_parallelism = 4;
            cluster_manager
                .add_worker_node(
                    WorkerType::ComputeNode,
                    host.clone(),
                    Property {
                        worker_node_parallelism: fake_parallelism,
                        is_streaming: true,
                        is_serving: true,
                        is_unschedulable: false,
                    },
                )
                .await?;
            cluster_manager.activate_worker_node(host).await?;

            let catalog_manager = Arc::new(CatalogManager::new(env.clone()).await?);
            let fragment_manager = Arc::new(FragmentManager::new(env.clone()).await?);

            // TODO: what should we choose the task heartbeat interval to be? Anyway, we don't run a
            // heartbeat thread here, so it doesn't matter.
            let compactor_manager =
                Arc::new(CompactorManager::with_meta(env.clone(), 1).await.unwrap());

            let hummock_manager = HummockManager::new(
                env.clone(),
                cluster_manager.clone(),
                meta_metrics.clone(),
                compactor_manager.clone(),
                catalog_manager.clone(),
            )
            .await?;

            let (barrier_scheduler, scheduled_barriers) = BarrierScheduler::new_pair(
                hummock_manager.clone(),
                meta_metrics.clone(),
                system_params.checkpoint_frequency() as usize,
            );

            let source_manager = Arc::new(
                SourceManager::new(
                    None,
                    barrier_scheduler.clone(),
                    catalog_manager.clone(),
                    fragment_manager.clone(),
                    meta_metrics.clone(),
                )
                .await?,
            );

            let barrier_manager = Arc::new(GlobalBarrierManager::new(
                scheduled_barriers,
                env.clone(),
                cluster_manager.clone(),
                catalog_manager.clone(),
                fragment_manager.clone(),
                hummock_manager.clone(),
                source_manager.clone(),
                meta_metrics.clone(),
            ));

            let stream_manager = GlobalStreamManager::new(
                env.clone(),
                fragment_manager.clone(),
                barrier_scheduler.clone(),
                cluster_manager.clone(),
                source_manager.clone(),
                hummock_manager,
            )?;

            let (join_handle_2, shutdown_tx_2) = GlobalBarrierManager::start(barrier_manager).await;

            Ok(Self {
                global_stream_manager: Arc::new(stream_manager),
                catalog_manager,
                fragment_manager,
                state,
                join_handle_shutdown_txs: vec![
                    (join_handle_2, shutdown_tx_2),
                    (join_handle, shutdown_tx),
                ],
            })
        }

        async fn create_materialized_view(
            &self,
            table_id: TableId,
            fragments: BTreeMap<FragmentId, Fragment>,
        ) -> MetaResult<()> {
            // Create fake locations where all actors are scheduled to the same parallel unit.
            let locations = {
                let StreamingClusterInfo {
                    worker_nodes,
                    parallel_units,
                } = self
                    .global_stream_manager
                    .cluster_manager
                    .get_streaming_cluster_info()
                    .await;

                let actor_locations = fragments
                    .values()
                    .flat_map(|f| &f.actors)
                    .map(|a| (a.actor_id, parallel_units[&0].clone()))
                    .collect();

                Locations {
                    actor_locations,
                    worker_locations: worker_nodes,
                }
            };

            let table = Table {
                id: table_id.table_id(),
                ..Default::default()
            };
            let table_fragments = TableFragments::new(
                table_id,
                fragments,
                &locations.actor_locations,
                Default::default(),
            );
            let ctx = CreateStreamingJobContext {
                building_locations: locations,
                ..Default::default()
            };

            self.catalog_manager
                .start_create_table_procedure(&table)
                .await?;
            self.global_stream_manager
                .create_streaming_job(table_fragments, ctx)
                .await?;
            self.catalog_manager
                .finish_create_table_procedure(vec![], &table)
                .await?;
            Ok(())
        }

        async fn drop_materialized_views(&self, table_ids: Vec<TableId>) -> MetaResult<()> {
            for table_id in &table_ids {
                self.catalog_manager
                    .drop_table(table_id.table_id, vec![], self.fragment_manager.clone())
                    .await?;
            }
            self.global_stream_manager
                .drop_streaming_jobs_impl(table_ids)
                .await?;
            Ok(())
        }

        async fn stop(self) {
            for (join_handle, shutdown_tx) in self.join_handle_shutdown_txs {
                shutdown_tx.send(()).unwrap();
                join_handle.await.unwrap();
            }
        }
    }

    fn make_mview_stream_actors(table_id: &TableId, count: usize) -> Vec<StreamActor> {
        (0..count)
            .map(|i| StreamActor {
                actor_id: i as u32,
                // A dummy node to avoid panic.
                nodes: Some(StreamNode {
                    node_body: Some(NodeBody::Materialize(MaterializeNode {
                        table_id: table_id.table_id(),
                        ..Default::default()
                    })),
                    operator_id: 1,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect_vec()
    }

    #[tokio::test]
    async fn test_drop_materialized_view() -> MetaResult<()> {
        let services = MockServices::start("127.0.0.1", 12334, false).await?;

        let table_id = TableId::new(0);
        let actors = make_mview_stream_actors(&table_id, 4);

        let mut fragments = BTreeMap::default();
        fragments.insert(
            0,
            Fragment {
                fragment_id: 0,
                fragment_type_mask: FragmentTypeFlag::Mview as u32,
                distribution_type: FragmentDistributionType::Hash as i32,
                actors: actors.clone(),
                state_table_ids: vec![0],
                vnode_mapping: Some(ParallelUnitMapping::new_single(0).to_protobuf()),
                ..Default::default()
            },
        );
        services
            .create_materialized_view(table_id, fragments)
            .await?;

        let actor_len = services.state.actor_streams.lock().unwrap().len();
        assert_eq!(actor_len, 4); // assert that actors are created

        let mview_actor_ids = services
            .fragment_manager
            .get_table_mview_actor_ids(&table_id)
            .await?;
        let actor_ids = services
            .fragment_manager
            .get_table_actor_ids(&HashSet::from([table_id]))
            .await?;
        assert_eq!(mview_actor_ids, (0..=3).collect::<Vec<u32>>());
        assert_eq!(actor_ids, (0..=3).collect::<Vec<u32>>());

        // test drop materialized_view
        services.drop_materialized_views(vec![table_id]).await?;

        // test get table_fragment;
        let select_err_1 = services
            .global_stream_manager
            .fragment_manager
            .select_table_fragments_by_table_id(&table_id)
            .await
            .unwrap_err();

        assert_eq!(select_err_1.to_string(), "table_fragment not exist: id=0");

        services.stop().await;
        Ok(())
    }

    #[tokio::test]
    #[cfg(all(test, feature = "failpoints"))]
    async fn test_failpoints_drop_mv_recovery() {
        let inject_barrier_err = "inject_barrier_err";
        let inject_barrier_err_success = "inject_barrier_err_success";
        let services = MockServices::start("127.0.0.1", 12335, true).await.unwrap();

        let table_id = TableId::new(0);
        let actors = make_mview_stream_actors(&table_id, 4);

        let mut fragments = BTreeMap::default();
        fragments.insert(
            0,
            Fragment {
                fragment_id: 0,
                fragment_type_mask: FragmentTypeFlag::Mview as u32,
                distribution_type: FragmentDistributionType::Hash as i32,
                actors: actors.clone(),
                state_table_ids: vec![0],
                vnode_mapping: Some(ParallelUnitMapping::new_single(0).to_protobuf()),
                ..Default::default()
            },
        );

        services
            .create_materialized_view(table_id, fragments)
            .await
            .unwrap();

        let actor_len = services.state.actor_streams.lock().unwrap().len();
        assert_eq!(actor_len, 4); // assert that actors are created

        let mview_actor_ids = services
            .fragment_manager
            .get_table_mview_actor_ids(&table_id)
            .await
            .unwrap();
        let actor_ids = services
            .fragment_manager
            .get_table_actor_ids(&HashSet::from([table_id]))
            .await
            .unwrap();
        assert_eq!(mview_actor_ids, (0..=3).collect::<Vec<u32>>());
        assert_eq!(actor_ids, (0..=3).collect::<Vec<u32>>());
        let notify = Arc::new(Notify::new());
        let notify1 = notify.clone();

        // test recovery.
        fail::cfg(inject_barrier_err, "return").unwrap();
        tokio::spawn(async move {
            fail::cfg_callback(inject_barrier_err_success, move || {
                fail::remove(inject_barrier_err);
                fail::remove(inject_barrier_err_success);
                notify.notify_one();
            })
            .unwrap();
        });
        notify1.notified().await;

        let table_fragments = services
            .global_stream_manager
            .fragment_manager
            .select_table_fragments_by_table_id(&table_id)
            .await
            .unwrap();
        assert_eq!(table_fragments.actor_ids(), (0..=3).collect_vec());

        // test drop materialized_view
        services
            .drop_materialized_views(vec![table_id])
            .await
            .unwrap();

        // test get table_fragment;
        let select_err_1 = services
            .global_stream_manager
            .fragment_manager
            .select_table_fragments_by_table_id(&table_fragments.table_id())
            .await
            .unwrap_err();

        assert_eq!(select_err_1.to_string(), "table_fragment not exist: id=0");

        services.stop().await;
    }
}
