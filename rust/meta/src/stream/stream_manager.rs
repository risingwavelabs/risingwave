use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use itertools::Itertools;
use log::{debug, info};
use risingwave_common::catalog::TableId;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_common::{ensure, gen_error, try_match_expand};
use risingwave_pb::common::{ActorInfo, WorkerNode};
use risingwave_pb::meta::table_fragments::State;
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::StreamNode;
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, UpdateActorsRequest,
};
use uuid::Uuid;

use crate::barrier::{BarrierManagerRef, Command};
use crate::cluster::StoredClusterManager;
use crate::manager::{MetaSrvEnv, StreamClientsRef};
use crate::model::TableFragments;
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

    fn search_chain_table_ref_ids(&self, stream_node: &StreamNode) -> Vec<TableRefId> {
        let mut table_ref_ids = vec![];
        if let Node::ChainNode(chain) = stream_node.node.as_ref().unwrap() {
            table_ref_ids.push(chain.table_ref_id.clone().unwrap());
        }
        for child in &stream_node.input {
            table_ref_ids.extend(self.search_chain_table_ref_ids(child));
        }
        table_ref_ids
    }

    async fn lookup_actor_ids(
        &self,
        table_ref_ids: Vec<TableRefId>,
        table_sink_map: &mut HashMap<i32, u32>,
    ) -> Result<()> {
        for table_ref_id in table_ref_ids {
            let table_id = table_ref_id.table_id;
            if let std::collections::hash_map::Entry::Vacant(e) = table_sink_map.entry(table_id) {
                // TODO(august): Currently we assume table only have one actor for MView layer.
                let sink_actors = try_match_expand!(
                    self.fragment_manager_ref
                        .get_table_sink_actor_ids(&TableId::from(&Some(table_ref_id)))
                        .await,
                    Ok
                )?;
                ensure!(!sink_actors.is_empty());
                e.insert(*sink_actors.get(0).unwrap());
            }
        }
        Ok(())
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
        let _table_id = table_fragments.table_id();
        let mut actors = table_fragments.actors();
        let actor_ids = table_fragments.actor_ids();
        let source_actor_ids = table_fragments.source_actor_ids();
        // Fill `upstream_actor_id` of [`ChainNode`].
        let mut table_sink_map = HashMap::default();
        for actor in &mut actors {
            let stream_node = actor.nodes.as_mut().unwrap();
            let table_ref_ids = self.search_chain_table_ref_ids(stream_node);
            self.lookup_actor_ids(table_ref_ids, &mut table_sink_map)
                .await?;
        }
        table_fragments.update_chain_upstream_actor_ids(&table_sink_map)?;

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
            .collect::<HashMap<u32, WorkerNode>>();

        let actor_info = nodes
            .iter()
            .zip_eq(actor_ids.clone())
            .map(|(n, f)| ActorInfo {
                actor_id: f,
                host: n.host.clone(),
            })
            .collect::<Vec<_>>();

        for (node_id, actors) in node_actors_map {
            let node = node_map.get(&node_id).unwrap();

            let client = self.clients.get(node).await?;
            client
                .to_owned()
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_info.clone(),
                })
                .await
                .to_rw_result_with(format!("failed to connect to {}", node_id))?;

            let stream_actors = actors
                .iter()
                .map(|f| actor_map.get(f).cloned().unwrap())
                .collect::<Vec<_>>();

            let request_id = Uuid::new_v4().to_string();
            debug!("[{}]update actors: {:?}", request_id, actors);
            client
                .to_owned()
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: stream_actors,
                })
                .await
                .to_rw_result_with(format!("failed to connect to {}", node_id))?;

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
        // TODO: should be triggered by barrier manager when migrate MV create to barrier cmd, and
        //  `update_state` should also moves to fragment_manager to keep consistency.
        table_fragments.update_state(State::Created);

        self.fragment_manager_ref
            .add_table_fragments(table_fragments)
            .await?;

        Ok(())
    }

    /// Droping materialized view is done by barrier manager. Check
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

    use risingwave_common::error::{tonic_err, ErrorCode};
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::meta::table_fragments::fragment::FragmentType;
    use risingwave_pb::meta::table_fragments::Fragment;
    use risingwave_pb::stream_plan::stream_node::Node;
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
        actor_streams: Mutex<HashMap<u32, StreamActor>>,
        actor_ids: Mutex<HashSet<u32>>,
        actor_infos: Mutex<HashMap<u32, HostAddress>>,
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
            panic!("not implemented")
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
            let cluster_manager = Arc::new(StoredClusterManager::new(env.clone()).await?);
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
                barrier_manager_ref,
                cluster_manager.clone(),
            )
            .await?;

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
                    node_id: 1,
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

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_materialized_view_on_materialized_view() -> Result<()> {
        let services = MockServices::start("127.0.0.1", 12346).await?;

        let table_ref_id_1 = TableRefId {
            schema_ref_id: None,
            table_id: 1,
        };
        let table_id_1 = TableId::from(&Some(table_ref_id_1.clone()));
        let table_ref_id_2 = TableRefId {
            schema_ref_id: None,
            table_id: 2,
        };
        let table_id_2 = TableId::from(&Some(table_ref_id_2.clone()));

        let mut fragments_1 = BTreeMap::default();
        fragments_1.insert(
            0,
            Fragment {
                fragment_id: 0,
                fragment_type: FragmentType::Sink as i32,
                actors: vec![StreamActor {
                    actor_id: 1,
                    nodes: Some(StreamNode {
                        node_id: 1,
                        node: Some(Node::MviewNode(MViewNode {
                            table_ref_id: Some(table_ref_id_1.clone()),
                            ..Default::default()
                        })),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
            },
        );

        let mut fragments_2 = BTreeMap::default();
        fragments_2.insert(
            1,
            Fragment {
                fragment_id: 1,
                fragment_type: FragmentType::Sink as i32,
                actors: vec![StreamActor {
                    actor_id: 2,
                    nodes: Some(StreamNode {
                        node_id: 2,
                        node: Some(Node::MviewNode(MViewNode {
                            table_ref_id: Some(table_ref_id_2.clone()),
                            ..Default::default()
                        })),
                        input: vec![StreamNode {
                            node_id: 3,
                            node: Some(Node::ChainNode(ChainNode {
                                table_ref_id: Some(table_ref_id_1.clone()),
                                upstream_actor_id: 0,
                                ..Default::default()
                            })),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
            },
        );

        let table_fragments_1 = TableFragments::new(table_id_1.clone(), fragments_1);
        services
            .stream_manager
            .create_materialized_view(table_fragments_1)
            .await?;

        let table_fragments_2 = TableFragments::new(table_id_2.clone(), fragments_2);
        services
            .stream_manager
            .create_materialized_view(table_fragments_2)
            .await?;

        let stored_actor_2 = services
            .state
            .actor_streams
            .lock()
            .unwrap()
            .get(&2)
            .cloned()
            .unwrap();
        if let Node::ChainNode(chain) = stored_actor_2
            .nodes
            .as_ref()
            .unwrap()
            .input
            .get(0)
            .unwrap()
            .node
            .as_ref()
            .unwrap()
        {
            assert_eq!(chain.upstream_actor_id, 1);
        } else {
            return Err(ErrorCode::UnknownError("chain node is expected".to_owned()).into());
        }

        services.stop().await;
        Ok(())
    }
}
