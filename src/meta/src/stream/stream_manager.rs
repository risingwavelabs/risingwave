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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use itertools::Itertools;
use log::{debug, info};
use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::common::{ActorInfo, WorkerType};
use risingwave_pb::meta::table_fragments::{ActorState, ActorStatus};
use risingwave_pb::stream_plan::source_node;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, HangingChannel, UpdateActorsRequest,
};
use uuid::Uuid;

use super::ScheduledLocations;
use crate::barrier::{BarrierManagerRef, Command};
use crate::cluster::{ClusterManagerRef, WorkerId};
use crate::manager::{MetaSrvEnv, StreamClientsRef};
use crate::model::{ActorId, TableFragments};
use crate::storage::MetaStore;
use crate::stream::{FragmentManagerRef, Scheduler, SourceManagerRef};

pub type GlobalStreamManagerRef<S> = Arc<GlobalStreamManager<S>>;

/// [`CreateMaterializedViewContext`] carries one-time infos.
#[derive(Default)]
pub struct CreateMaterializedViewContext {
    /// New dispatches to add from upstream actors to downstream actors.
    pub dispatches: HashMap<ActorId, Vec<ActorId>>,
    /// Upstream mview actor ids grouped by node id.
    pub upstream_node_actors: HashMap<WorkerId, Vec<ActorId>>,
    /// Upstream mview actor ids grouped by table id.
    pub table_sink_map: HashMap<TableId, Vec<ActorId>>,
}

/// `GlobalStreamManager` manages all the streams in the system.
pub struct GlobalStreamManager<S: MetaStore> {
    /// Manages definition and status of fragments and actors
    fragment_manager: FragmentManagerRef<S>,

    /// Broadcasts and collect barriers
    barrier_manager: BarrierManagerRef<S>,

    /// Maintains information of the cluster
    cluster_manager: ClusterManagerRef<S>,

    /// Maintains streaming sources from external system like kafka
    source_manager: SourceManagerRef<S>,

    /// Schedules streaming actors into compute nodes
    scheduler: Scheduler<S>,

    /// Clients to stream service on compute nodes
    clients: StreamClientsRef,
}

impl<S> GlobalStreamManager<S>
where
    S: MetaStore,
{
    pub async fn new(
        env: MetaSrvEnv<S>,
        fragment_manager: FragmentManagerRef<S>,
        barrier_manager: BarrierManagerRef<S>,
        cluster_manager: ClusterManagerRef<S>,
        source_manager: SourceManagerRef<S>,
    ) -> Result<Self> {
        Ok(Self {
            fragment_manager,
            barrier_manager,
            scheduler: Scheduler::new(cluster_manager.clone()),
            cluster_manager,
            clients: env.stream_clients_ref(),
            source_manager,
        })
    }

    /// Create materialized view, it works as follows:
    /// 1. schedule the actors to nodes in the cluster.
    /// 2. broadcast the actor info table.
    /// 3. notify related nodes to update and build the actors.
    /// 4. store related meta data.
    pub async fn create_materialized_view(
        &self,
        mut table_fragments: TableFragments,
        ctx: CreateMaterializedViewContext,
    ) -> Result<()> {
        let nodes = self
            .cluster_manager
            .list_worker_node(
                WorkerType::ComputeNode,
                Some(risingwave_pb::common::worker_node::State::Running),
            )
            .await;
        if nodes.is_empty() {
            return Err(InternalError("no available node exist".to_string()).into());
        }

        let mut locations = ScheduledLocations::new();
        locations.node_locations = nodes.into_iter().map(|node| (node.id, node)).collect();

        for fragment in table_fragments.fragments() {
            self.scheduler
                .schedule(fragment.clone(), &mut locations)
                .await?;
        }

        let actor_info = locations
            .actor_locations
            .iter()
            .map(|(&actor_id, parallel_unit)| {
                (
                    actor_id,
                    ActorStatus {
                        node_id: parallel_unit.worker_node_id,
                        state: ActorState::Inactive as i32,
                    },
                )
            })
            .collect();

        table_fragments.set_actor_status(actor_info);
        let actor_map = table_fragments.actor_map();

        // Actors on each stream node will need to know where their upstream lies. `actor_info`
        // includes such information. It contains: 1. actors in the current create
        // materialized view request. 2. all upstream actors.
        let mut actor_infos_to_broadcast = locations.actor_infos();
        actor_infos_to_broadcast.extend(ctx.upstream_node_actors.iter().flat_map(
            |(node_id, upstreams)| {
                upstreams.iter().map(|up_id| ActorInfo {
                    actor_id: *up_id,
                    host: locations.node_locations.get(node_id).unwrap().host.clone(),
                })
            },
        ));

        let actor_host_infos = locations.actor_info_map();

        let node_actors = locations.node_actors();

        let dispatches = ctx
            .dispatches
            .iter()
            .map(|(up_id, down_ids)| {
                (
                    *up_id,
                    down_ids
                        .iter()
                        .map(|down_id| {
                            actor_host_infos
                                .get(down_id)
                                .expect("downstream actor info not exist")
                                .clone()
                        })
                        .collect_vec(),
                )
            })
            .collect::<HashMap<_, _>>();

        let mut node_hanging_channels = ctx
            .upstream_node_actors
            .iter()
            .map(|(node_id, up_ids)| {
                (
                    *node_id,
                    up_ids
                        .iter()
                        .flat_map(|up_id| {
                            dispatches
                                .get(up_id)
                                .expect("expected dispatches info")
                                .iter()
                                .map(|down_info| HangingChannel {
                                    upstream: Some(ActorInfo {
                                        actor_id: *up_id,
                                        host: None,
                                    }),
                                    downstream: Some(down_info.clone()),
                                })
                        })
                        .collect_vec(),
                )
            })
            .collect::<HashMap<_, _>>();

        // We send RPC request in two stages.
        // The first stage does 2 things: broadcast actor info, and send local actor ids to
        // different WorkerNodes. Such that each WorkerNode knows the overall actor
        // allocation, but not actually builds it. We initialize all channels in this stage.
        for (node_id, actors) in &node_actors {
            let node = locations.node_locations.get(node_id).unwrap();

            let client = self.clients.get(node).await?;

            client
                .to_owned()
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_infos_to_broadcast.clone(),
                })
                .await
                .to_rw_result_with(|| format!("failed to connect to {}", node_id))?;

            let stream_actors = actors
                .iter()
                .map(|actor_id| actor_map.get(actor_id).cloned().unwrap())
                .collect::<Vec<_>>();

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "update actors");
            client
                .to_owned()
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: stream_actors.clone(),
                    hanging_channels: node_hanging_channels.remove(node_id).unwrap_or_default(),
                })
                .await
                .to_rw_result_with(|| format!("failed to connect to {}", node_id))?;
        }

        for (node_id, hanging_channels) in node_hanging_channels {
            let node = locations.node_locations.get(&node_id).unwrap();

            let client = self.clients.get(node).await?;
            let request_id = Uuid::new_v4().to_string();

            client
                .to_owned()
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: vec![],
                    hanging_channels,
                })
                .await
                .to_rw_result_with(|| format!("failed to connect to {}", node_id))?;
        }

        // In the second stage, each [`WorkerNode`] builds local actors and connect them with
        // channels.
        for (node_id, actors) in node_actors {
            let node = locations.node_locations.get(&node_id).unwrap();

            let client = self.clients.get(node).await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "build actors");
            client
                .to_owned()
                .build_actors(BuildActorsRequest {
                    request_id,
                    actor_id: actors,
                })
                .await
                .to_rw_result_with(|| format!("failed to connect to {}", node_id))?;
        }

        // Add table fragments to meta store with state: `State::Creating`.
        self.fragment_manager
            .start_create_table_fragments(table_fragments.clone())
            .await?;
        self.barrier_manager
            .run_command(Command::CreateMaterializedView {
                table_fragments,
                table_sink_map: ctx.table_sink_map,
                dispatches,
            })
            .await?;

        let mut source_actors_group_by_frag = HashMap::new();

        for (_actor_id, actor) in actor_map {
            let mut node = actor.get_nodes().unwrap();
            while !node.get_input().is_empty() {
                node = node.get_input().get(0).unwrap();
            }
            if let Node::SourceNode(n) = node.get_node().unwrap() {
                let source_type = n.get_source_type().unwrap();
                if matches!(source_type, source_node::SourceType::Source) {
                    let source_id = n.table_ref_id.as_ref().unwrap().table_id as u32;
                    let frag_id = actor.fragment_id;
                    let actor_id = actor.actor_id;
                    source_actors_group_by_frag
                        .entry(source_id)
                        .or_insert_with(HashMap::new)
                        .entry(frag_id)
                        .or_insert(vec![])
                        .push(actor_id);
                }
            }
        }

        self.source_manager
            .register_source_discovery(
                source_actors_group_by_frag
                    .into_iter()
                    .map(|(actor_id, frag)| (actor_id, frag.into_values().collect_vec()))
                    .collect(),
            )
            .await
    }

    /// Dropping materialized view is done by barrier manager. Check
    /// [`Command::DropMaterializedView`] for details.
    pub async fn drop_materialized_view(&self, table_id: &TableId) -> Result<()> {
        self.barrier_manager
            .run_command(Command::DropMaterializedView(*table_id))
            .await?;

        Ok(())
    }

    /// Flush means waiting for the next barrier to collect.
    pub async fn flush(&self) -> Result<()> {
        let start = Instant::now();

        debug!("start barrier flush");
        self.barrier_manager
            .wait_for_next_barrier_to_collect()
            .await?;

        let elapsed = Instant::now().duration_since(start);
        info!("barrier flushed in {:?}", elapsed);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::thread::sleep;
    use std::time::Duration;

    use risingwave_common::catalog::TableId;
    use risingwave_common::error::tonic_err;
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::meta::table_fragments::fragment::{FragmentDistributionType, FragmentType};
    use risingwave_pb::meta::table_fragments::Fragment;
    use risingwave_pb::plan::TableRefId;
    use risingwave_pb::stream_plan::*;
    use risingwave_pb::stream_service::stream_service_server::{
        StreamService, StreamServiceServer,
    };
    use risingwave_pb::stream_service::{
        BroadcastActorInfoTableResponse, BuildActorsResponse, DropActorsRequest,
        DropActorsResponse, InjectBarrierRequest, InjectBarrierResponse, UpdateActorsResponse, *,
    };
    use tokio::sync::mpsc::UnboundedSender;
    use tokio::task::JoinHandle;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::barrier::GlobalBarrierManager;
    use crate::cluster::ClusterManager;
    use crate::hummock::HummockManager;
    use crate::manager::{CatalogManager, MetaSrvEnv};
    use crate::model::ActorId;
    use crate::rpc::metrics::MetaMetrics;
    use crate::storage::MemStore;
    use crate::stream::{FragmentManager, SourceManager};

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
                guard.insert(
                    info.get_actor_id(),
                    info.get_host().map_err(tonic_err)?.clone(),
                );
            }

            Ok(Response::new(BroadcastActorInfoTableResponse {
                status: None,
            }))
        }

        async fn drop_actors(
            &self,
            _request: Request<DropActorsRequest>,
        ) -> std::result::Result<Response<DropActorsResponse>, Status> {
            panic!("not implemented")
        }

        async fn inject_barrier(
            &self,
            _request: Request<InjectBarrierRequest>,
        ) -> std::result::Result<Response<InjectBarrierResponse>, Status> {
            Ok(Response::new(InjectBarrierResponse::default()))
        }

        async fn create_source(
            &self,
            _request: Request<CreateSourceRequest>,
        ) -> std::result::Result<Response<CreateSourceResponse>, Status> {
            unimplemented!()
        }

        async fn drop_source(
            &self,
            _request: Request<DropSourceRequest>,
        ) -> std::result::Result<Response<DropSourceResponse>, Status> {
            unimplemented!()
        }

        async fn force_stop_actors(
            &self,
            _request: Request<ForceStopActorsRequest>,
        ) -> std::result::Result<Response<ForceStopActorsResponse>, Status> {
            Ok(Response::new(ForceStopActorsResponse::default()))
        }

        async fn sync_sources(
            &self,
            _request: Request<SyncSourcesRequest>,
        ) -> std::result::Result<Response<SyncSourcesResponse>, Status> {
            Ok(Response::new(SyncSourcesResponse::default()))
        }
    }

    struct MockServices {
        global_stream_manager: GlobalStreamManager<MemStore>,
        fragment_manager: FragmentManagerRef<MemStore>,
        state: Arc<FakeFragmentState>,
        join_handles: Vec<JoinHandle<()>>,
        shutdown_txs: Vec<UnboundedSender<()>>,
    }

    impl MockServices {
        async fn start(host: &str, port: u16) -> Result<Self> {
            let addr = SocketAddr::new(host.parse().unwrap(), port);
            let state = Arc::new(FakeFragmentState {
                actor_streams: Mutex::new(HashMap::new()),
                actor_ids: Mutex::new(HashSet::new()),
                actor_infos: Mutex::new(HashMap::new()),
            });
            let fake_service = FakeStreamService {
                inner: state.clone(),
            };

            let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
            let stream_srv = StreamServiceServer::new(fake_service);
            let join_handle = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(stream_srv)
                    .serve_with_shutdown(addr, async move {
                        shutdown_rx.recv().await;
                    })
                    .await
                    .unwrap();
            });
            sleep(Duration::from_secs(1));

            let env = MetaSrvEnv::for_test().await;
            let cluster_manager =
                Arc::new(ClusterManager::new(env.clone(), Duration::from_secs(3600)).await?);
            let host = HostAddress {
                host: host.to_string(),
                port: port as i32,
            };
            cluster_manager
                .add_worker_node(host.clone(), WorkerType::ComputeNode)
                .await?;
            cluster_manager.activate_worker_node(host).await?;

            let catalog_manager = Arc::new(CatalogManager::new(env.clone()).await?);
            let fragment_manager = Arc::new(FragmentManager::new(env.meta_store_ref()).await?);
            let meta_metrics = Arc::new(MetaMetrics::new());
            let hummock_manager = Arc::new(
                HummockManager::new(env.clone(), cluster_manager.clone(), meta_metrics.clone())
                    .await?,
            );
            let barrier_manager = Arc::new(GlobalBarrierManager::new(
                env.clone(),
                cluster_manager.clone(),
                catalog_manager.clone(),
                fragment_manager.clone(),
                hummock_manager,
                meta_metrics.clone(),
            ));

            let source_manager = Arc::new(
                SourceManager::new(
                    env.clone(),
                    cluster_manager.clone(),
                    barrier_manager.clone(),
                    catalog_manager.clone(),
                )
                .await?,
            );

            let stream_manager = GlobalStreamManager::new(
                env.clone(),
                fragment_manager.clone(),
                barrier_manager.clone(),
                cluster_manager.clone(),
                source_manager.clone(),
            )
            .await?;

            let (join_handle_2, shutdown_tx_2) = GlobalBarrierManager::start(barrier_manager).await;

            Ok(Self {
                global_stream_manager: stream_manager,
                fragment_manager,
                state,
                join_handles: vec![join_handle_2, join_handle],
                shutdown_txs: vec![shutdown_tx_2, shutdown_tx],
            })
        }

        async fn stop(self) {
            for shutdown_tx in self.shutdown_txs {
                shutdown_tx.send(()).unwrap();
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            for join_handle in self.join_handles {
                join_handle.await.unwrap();
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_materialized_view() -> Result<()> {
        let services = MockServices::start("127.0.0.1", 12333).await?;

        let table_ref_id = TableRefId {
            schema_ref_id: None,
            table_id: 0,
        };
        let table_id = TableId::from(&Some(table_ref_id.clone()));

        let actors = (0..5)
            .map(|i| StreamActor {
                actor_id: i,
                // A dummy node to avoid panic.
                nodes: Some(risingwave_pb::stream_plan::StreamNode {
                    node: Some(
                        risingwave_pb::stream_plan::stream_node::Node::MaterializeNode(
                            risingwave_pb::stream_plan::MaterializeNode {
                                table_ref_id: Some(table_ref_id.clone()),
                                ..Default::default()
                            },
                        ),
                    ),
                    operator_id: 1,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let mut fragments = BTreeMap::default();
        fragments.insert(
            0,
            Fragment {
                fragment_id: 0,
                fragment_type: FragmentType::Sink as i32,
                distribution_type: FragmentDistributionType::Hash as i32,
                actors: actors.clone(),
            },
        );
        let table_fragments = TableFragments::new(table_id, fragments);

        let ctx = CreateMaterializedViewContext::default();

        services
            .global_stream_manager
            .create_materialized_view(table_fragments, ctx)
            .await?;

        for actor in actors {
            assert_eq!(
                services
                    .state
                    .actor_streams
                    .lock()
                    .unwrap()
                    .get(&actor.get_actor_id())
                    .cloned()
                    .unwrap(),
                actor
            );
            assert!(services
                .state
                .actor_ids
                .lock()
                .unwrap()
                .contains(&actor.get_actor_id()));
            assert_eq!(
                services
                    .state
                    .actor_infos
                    .lock()
                    .unwrap()
                    .get(&actor.get_actor_id())
                    .cloned()
                    .unwrap(),
                HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 12333,
                }
            );
        }

        let sink_actor_ids = services
            .fragment_manager
            .get_table_sink_actor_ids(&table_id)
            .await?;
        let actor_ids = services
            .fragment_manager
            .get_table_actor_ids(&table_id)
            .await?;
        assert_eq!(sink_actor_ids, (0..5).collect::<Vec<u32>>());
        assert_eq!(actor_ids, (0..5).collect::<Vec<u32>>());

        services.stop().await;
        Ok(())
    }
}
