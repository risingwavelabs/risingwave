use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use itertools::Itertools;
use log::{debug, info};
use risingwave_common::error::{Result, ToRwResult};
use risingwave_common::try_match_expand;
use risingwave_pb::common::{ActorInfo, WorkerNode};
use risingwave_pb::meta::ActorLocation;
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::StreamNode;
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, UpdateActorsRequest,
};
use uuid::Uuid;

use crate::barrier::{BarrierManagerRef, Command};
use crate::cluster::{NodeId, StoredClusterManager};
use crate::manager::{MetaSrvEnv, StreamClientsRef};
use crate::model::{ActorId, TableFragments};
use crate::stream::{FragmentManagerRef, ScheduleCategory, Scheduler};

pub type StreamManagerRef = Arc<StreamManager>;

pub struct StreamManager {
    fragment_manager_ref: FragmentManagerRef,

    barrier_manager_ref: BarrierManagerRef,
    scheduler: Scheduler,
    clients: StreamClientsRef,
}

impl StreamManager {
    pub async fn new(
        env: MetaSrvEnv,
        fragment_manager_ref: FragmentManagerRef,
        barrier_manager_ref: BarrierManagerRef,
        cluster_manager: Arc<StoredClusterManager>,
    ) -> Result<Self> {
        Ok(Self {
            fragment_manager_ref,
            barrier_manager_ref,
            scheduler: Scheduler::new(ScheduleCategory::RoundRobin, cluster_manager),
            clients: env.stream_clients_ref(),
        })
    }

    /// Search upstream actor ids of chain node in the stream node.
    fn search_chain_upstream_actor_ids(
        &self,
        stream_node: &StreamNode,
        upstream_actor_ids: &mut Vec<ActorId>,
    ) {
        match stream_node.node.as_ref().unwrap() {
            Node::ChainNode(_) => {
                let merge_node = try_match_expand!(
                    stream_node.input.get(0).unwrap().node.as_ref().unwrap(),
                    Node::MergeNode,
                    "first input of chain node should should be merge node"
                )
                .unwrap();
                upstream_actor_ids.extend(merge_node.upstream_actor_id.iter());
            }
            _ => {
                for child in &stream_node.input {
                    self.search_chain_upstream_actor_ids(child, upstream_actor_ids);
                }
            }
        }
    }

    /// Create materialized view, it works as follows:
    /// 1. schedule the actors to nodes in the cluster.
    /// 2. broadcast the actor info table.
    /// 3. notify related nodes to update and build the actors.
    /// 4. store related meta data.
    pub async fn create_materialized_view(
        &self,
        mut table_fragments: TableFragments,
    ) -> Result<()> {
        // TODO(MrCroxx): refine this mess after mv on mv can RUN.
        let actors = table_fragments.actors();
        let actor_ids = table_fragments.actor_ids();
        let source_actor_ids = table_fragments.source_actor_ids();

        // Collect (upstream actor id, downstream actor id) pairs of chain nodes.
        let mut up_down_ids = vec![];
        for actor in &actors {
            let stream_node = actor.nodes.as_ref().unwrap();
            let mut upstream_actor_ids = vec![];
            self.search_chain_upstream_actor_ids(stream_node, &mut upstream_actor_ids);
            for up_id in upstream_actor_ids {
                up_down_ids.push((up_id, actor.actor_id));
            }
        }

        // Divide all actors into source and non-source actors.
        let non_source_actor_ids = actor_ids
            .clone()
            .into_iter()
            .filter(|id| !source_actor_ids.contains(id))
            .collect::<Vec<_>>();

        let nodes = self
            .scheduler
            .schedule(&non_source_actor_ids, &source_actor_ids)
            .await?;

        // Re-sort actors by `non_source_actor_ids`::`source_actor_ids`.
        let mut sorted_actor_ids = non_source_actor_ids.clone();
        sorted_actor_ids.extend(source_actor_ids.iter().cloned());

        let mut node_actors_map = HashMap::new();
        let mut actor_locations = BTreeMap::new();
        for (node, actor) in nodes.iter().zip_eq(sorted_actor_ids) {
            node_actors_map
                .entry(node.get_id())
                .or_insert_with(Vec::new)
                .push(actor);
            actor_locations.insert(actor, node.get_id());
        }

        table_fragments.set_locations(actor_locations);
        let actor_map = table_fragments.actor_map();

        let node_map = nodes
            .iter()
            .map(|n| (n.get_id(), n.clone()))
            .collect::<HashMap<NodeId, WorkerNode>>();

        let actor_infos = nodes
            .iter()
            .zip_eq(actor_ids.clone())
            .map(|(n, actor_id)| ActorInfo {
                actor_id,
                host: n.host.clone(),
            })
            .collect_vec();

        let actor_info_map = actor_infos
            .iter()
            .map(|actor_info| (actor_info.actor_id, actor_info.clone()))
            .collect::<HashMap<ActorId, ActorInfo>>();

        let dispatches = up_down_ids.into_iter().into_grouping_map().fold(
            vec![],
            |mut actors, _up_id, down_id| {
                let info = actor_info_map
                    .get(&down_id)
                    .expect("downstream actor info not exist")
                    .clone();
                actors.push(info);
                actors
            },
        );

        let mut actor_locations = Vec::with_capacity(actors.len());

        // Debug usage: print the actor dependencies in log.
        actors.iter().for_each(|e| {
            debug!(
                "actor {} with downstreams: {:?}",
                e.actor_id, e.downstream_actor_id
            );
        });

        // We send RPC request in two stages.
        // The first stage does 2 things: broadcast actor info, and send local actor ids to
        // different WorkerNodes. Such that each WorkerNode knows the overall actor
        // allocation, but not actually builds it. We initialize all channels in this stage.
        for (node_id, actors) in node_actors_map.clone() {
            let node = node_map.get(&node_id).unwrap();

            let client = self.clients.get(node).await?;
            client
                .to_owned()
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_infos.clone(),
                })
                .await
                .to_rw_result_with(format!("failed to connect to {}", node_id))?;

            let stream_actors = actors
                .iter()
                .map(|actor_id| actor_map.get(actor_id).cloned().unwrap())
                .collect::<Vec<_>>();

            let request_id = Uuid::new_v4().to_string();
            debug!("[{}]update actors: {:?}", request_id, actors);
            client
                .to_owned()
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: stream_actors.clone(),
                })
                .await
                .to_rw_result_with(format!("failed to connect to {}", node_id))?;

            actor_locations.push(ActorLocation {
                node: Some(node.clone()),
                actors: stream_actors,
            });
        }

        // In the second stage, each [`WorkerNode`] builds local actors and connect them with
        // channels.
        for (node_id, actors) in node_actors_map {
            let node = node_map.get(&node_id).unwrap();

            let client = self.clients.get(node).await?;
            let request_id = Uuid::new_v4().to_string();
            debug!("[{}]build actors: {:?}", request_id, actors);
            client
                .to_owned()
                .build_actors(BuildActorsRequest {
                    request_id,
                    actor_id: actors,
                })
                .await
                .to_rw_result_with(format!("failed to connect to {}", node_id))?;
        }

        // Add table fragments to meta store with state: `State::Creating`.
        self.fragment_manager_ref
            .add_table_fragments(table_fragments.clone())
            .await?;
        self.barrier_manager_ref
            .run_command(Command::CreateMaterializedView {
                table_fragments,
                dispatches,
            })
            .await?;

        Ok(())
    }

    /// Dropping materialized view is done by barrier manager. Check
    /// [`Command::DropMaterializedView`] for details.
    pub async fn drop_materialized_view(&self, table_id: &TableRefId) -> Result<()> {
        self.barrier_manager_ref
            .run_command(Command::DropMaterializedView(table_id.clone()))
            .await?;

        Ok(())
    }

    /// Flush means waiting for the next barrier to finish.
    pub async fn flush(&self) -> Result<()> {
        let start = Instant::now();

        info!("start barrier flush");
        self.barrier_manager_ref.wait_for_next_barrier().await?;

        let elapsed = Instant::now().duration_since(start);
        info!("barrier flushed in {:?}", elapsed);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::thread::sleep;
    use std::time::Duration;

    use risingwave_common::catalog::TableId;
    use risingwave_common::error::tonic_err;
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::meta::table_fragments::fragment::FragmentType;
    use risingwave_pb::meta::table_fragments::Fragment;
    use risingwave_pb::stream_plan::*;
    use risingwave_pb::stream_service::stream_service_server::{
        StreamService, StreamServiceServer,
    };
    use risingwave_pb::stream_service::{
        BroadcastActorInfoTableResponse, BuildActorsResponse, DropActorsRequest,
        DropActorsResponse, InjectBarrierRequest, InjectBarrierResponse, UpdateActorsResponse,
    };
    use tokio::sync::mpsc::UnboundedSender;
    use tokio::task::JoinHandle;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::barrier::BarrierManager;
    use crate::manager::MetaSrvEnv;
    use crate::stream::FragmentManager;

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
            Ok(Response::new(InjectBarrierResponse {
                request_id: "".to_string(),
                status: None,
            }))
        }
    }

    struct MockServices {
        stream_manager: StreamManager,
        fragment_manager: FragmentManagerRef,
        state: Arc<FakeFragmentState>,
        join_handle: JoinHandle<()>,
        shutdown_tx: UnboundedSender<()>,
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
            let cluster_manager = Arc::new(StoredClusterManager::new(env.clone(), None).await?);
            cluster_manager
                .add_worker_node(
                    HostAddress {
                        host: host.to_string(),
                        port: port as i32,
                    },
                    WorkerType::ComputeNode,
                )
                .await?;

            let fragment_manager = Arc::new(FragmentManager::new(env.clone()).await?);

            let barrier_manager_ref = Arc::new(BarrierManager::new(
                env.clone(),
                cluster_manager.clone(),
                fragment_manager.clone(),
                env.epoch_generator_ref(),
            ));

            let stream_manager = StreamManager::new(
                env.clone(),
                fragment_manager.clone(),
                barrier_manager_ref.clone(),
                cluster_manager.clone(),
            )
            .await?;

            // TODO: join barrier service back to local thread
            tokio::spawn(async move { barrier_manager_ref.run().await.unwrap() });

            Ok(Self {
                stream_manager,
                fragment_manager,
                state,
                join_handle,
                shutdown_tx,
            })
        }

        async fn stop(self) {
            self.shutdown_tx.send(()).unwrap();
            self.join_handle.await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_materialized_view() -> Result<()> {
        let services = MockServices::start("127.0.0.1", 12345).await?;

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
                    node: Some(risingwave_pb::stream_plan::stream_node::Node::MviewNode(
                        risingwave_pb::stream_plan::MViewNode {
                            table_ref_id: Some(table_ref_id.clone()),
                            ..Default::default()
                        },
                    )),
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
                actors: actors.clone(),
            },
        );
        let table_fragments = TableFragments::new(table_id.clone(), fragments);

        services
            .stream_manager
            .create_materialized_view(table_fragments)
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
                    port: 12345,
                }
            );
        }

        let sink_actor_ids = services
            .fragment_manager
            .get_table_sink_actor_ids(&table_id)?;
        let actor_ids = services.fragment_manager.get_table_actor_ids(&table_id)?;
        assert_eq!(sink_actor_ids, (0..5).collect::<Vec<u32>>());
        assert_eq!(actor_ids, (0..5).collect::<Vec<u32>>());

        services.stop().await;
        Ok(())
    }
}
