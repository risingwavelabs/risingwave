use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use itertools::Itertools;
use log::{debug, info};
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::common::{ActorInfo, WorkerNode};
use risingwave_pb::meta::{ActorLocation, TableActors};
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::{StreamActor, StreamNode};
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, UpdateActorsRequest,
};
use uuid::Uuid;

use crate::barrier::{BarrierManagerRef, Command};
use crate::cluster::StoredClusterManager;
use crate::manager::{MetaSrvEnv, StreamClientsRef};
use crate::stream::{ScheduleCategory, Scheduler, StreamMetaManagerRef};

pub type StreamManagerRef = Arc<StreamManager>;

pub struct StreamManager {
    smm: StreamMetaManagerRef,
    barrier_manager_ref: BarrierManagerRef,
    scheduler: Scheduler,
    clients: StreamClientsRef,
}

impl StreamManager {
    pub fn new(
        env: MetaSrvEnv,
        smm: StreamMetaManagerRef,
        barrier_manager_ref: BarrierManagerRef,
        cluster_manager: Arc<StoredClusterManager>,
    ) -> Self {
        Self {
            smm,
            barrier_manager_ref,
            scheduler: Scheduler::new(ScheduleCategory::RoundRobin, cluster_manager),
            clients: env.stream_clients_ref(),
        }
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

    async fn lookup_actor_ids(&self, table_ref_ids: Vec<TableRefId>) -> Result<HashMap<i32, u32>> {
        let mut result = HashMap::default();
        for table_ref_id in table_ref_ids {
            // TODO(MrCroxx): A create materialized view plan will be split into multiple stages.
            // Currently, we simply assume that MView lays on the actor with min actor id. We need
            // an interface in [`StreamMetaManager`] to query the actor id of the MView
            // node.
            let table_actors = self.smm.get_table_actors(&table_ref_id).await?;
            result.insert(
                table_ref_id.table_id,
                *table_actors.actor_ids.iter().min().unwrap(),
            );
        }
        Ok(result)
    }

    fn update_chain_upstream_actor_ids(
        &self,
        stream_node: &mut StreamNode,
        table_actor_map: &HashMap<i32, u32>,
    ) {
        if let Node::ChainNode(chain) = stream_node.node.as_mut().unwrap() {
            chain.upstream_actor_id = *table_actor_map
                .get(&chain.table_ref_id.as_ref().unwrap().table_id)
                .expect("table id not exists");
        }
        for child in &mut stream_node.input {
            self.update_chain_upstream_actor_ids(child, table_actor_map);
        }
    }

    /// Create materialized view, it works as follows:
    /// 1. schedule the actors to nodes in the cluster.
    /// 2. broadcast the actor info table.
    /// 3. notify related nodes to update and build the actors.
    /// 4. store related meta data.
    pub async fn create_materialized_view(
        &self,
        table_id: &TableRefId,
        actors: &mut [StreamActor],
        source_actor_ids: Vec<u32>,
    ) -> Result<()> {
        // Fill `upstream_actor_id` of [`ChainNode`].
        for actor in actors.iter_mut() {
            let stream_node = actor.nodes.as_mut().unwrap();
            let table_ref_ids = self.search_chain_table_ref_ids(stream_node);
            let table_actor_map = self.lookup_actor_ids(table_ref_ids).await?;
            self.update_chain_upstream_actor_ids(stream_node, &table_actor_map);
        }

        // Divide all actors into source and non-source actors.
        let actor_ids = actors.iter().map(|a| a.actor_id).collect::<Vec<_>>();
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
        for (node, actor) in nodes.iter().zip_eq(sorted_actor_ids) {
            node_actors_map
                .entry(node.get_id())
                .or_insert_with(Vec::new)
                .push(actor);
        }

        let node_map = nodes
            .iter()
            .map(|n| (n.get_id(), n.clone()))
            .collect::<HashMap<u32, WorkerNode>>();

        let actor_map = actors
            .iter()
            .map(|f| (f.actor_id, f.clone()))
            .collect::<HashMap<u32, StreamActor>>();

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
                    actors: stream_actors.clone(),
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

            self.smm
                .add_actors_to_node(&ActorLocation {
                    node: Some(node.clone()),
                    actors: stream_actors,
                })
                .await?;
        }

        self.smm
            .add_table_actors(
                &table_id.clone(),
                &TableActors {
                    table_ref_id: Some(table_id.clone()),
                    actor_ids,
                },
            )
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
    use crate::stream::{StoredStreamMetaManager, StreamMetaManager};

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
        meta_manager: Arc<StoredStreamMetaManager>,
        cluster_manager: Arc<StoredClusterManager>,
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
            let meta_manager = Arc::new(StoredStreamMetaManager::new(env.clone()));
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

            let barrier_manager_ref = Arc::new(BarrierManager::new(
                env.clone(),
                cluster_manager.clone(),
                meta_manager.clone(),
                env.epoch_generator_ref(),
            ));

            let stream_manager = StreamManager::new(
                env.clone(),
                meta_manager.clone(),
                barrier_manager_ref,
                cluster_manager.clone(),
            );

            Ok(Self {
                stream_manager,
                meta_manager,
                cluster_manager,
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
        let mut actors = (0..5)
            .map(|i| StreamActor {
                actor_id: i,
                // A dummy node to avoid panic.
                nodes: Some(risingwave_pb::stream_plan::StreamNode {
                    input: vec![],
                    pk_indices: vec![],
                    node: Some(risingwave_pb::stream_plan::stream_node::Node::MviewNode(
                        risingwave_pb::stream_plan::MViewNode {
                            table_ref_id: Some(table_ref_id.clone()),
                            associated_table_ref_id: None,
                            column_descs: vec![],
                            pk_indices: vec![],
                            column_orders: vec![],
                        },
                    )),
                    node_id: 1,
                }),
                dispatcher: None,
                downstream_actor_id: vec![],
            })
            .collect::<Vec<_>>();

        services
            .stream_manager
            .create_materialized_view(&table_ref_id, &mut actors, vec![])
            .await?;

        for actor in actors.clone() {
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

        let locations = services.meta_manager.load_all_actors().await?;
        assert_eq!(locations.len(), 1);
        assert_eq!(locations.get(0).unwrap().get_node().unwrap().get_id(), 0);
        assert_eq!(locations.get(0).unwrap().actors, actors);
        let table_actors = services
            .meta_manager
            .get_table_actors(&table_ref_id)
            .await?;
        assert_eq!(table_actors.actor_ids, (0..5).collect::<Vec<u32>>());

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
        let table_ref_id_2 = TableRefId {
            schema_ref_id: None,
            table_id: 2,
        };

        let actor_1 = StreamActor {
            actor_id: 1,
            nodes: Some(StreamNode {
                node_id: 1,
                node: Some(Node::MviewNode(MViewNode {
                    table_ref_id: Some(table_ref_id_1.clone()),
                    associated_table_ref_id: None,
                    column_descs: vec![],
                    pk_indices: vec![],
                    column_orders: vec![],
                })),
                pk_indices: vec![],
                input: vec![],
            }),
            dispatcher: None,
            downstream_actor_id: vec![],
        };

        let actor_2 = StreamActor {
            actor_id: 2,
            nodes: Some(StreamNode {
                node_id: 2,
                node: Some(Node::MviewNode(MViewNode {
                    table_ref_id: Some(table_ref_id_2.clone()),
                    associated_table_ref_id: None,
                    column_descs: vec![],
                    pk_indices: vec![],
                    column_orders: vec![],
                })),
                pk_indices: vec![],
                input: vec![StreamNode {
                    node_id: 3,
                    node: Some(Node::ChainNode(ChainNode {
                        table_ref_id: Some(table_ref_id_1.clone()),
                        upstream_actor_id: 0,
                        pk_indices: vec![],
                        column_ids: vec![],
                    })),
                    pk_indices: vec![],
                    input: vec![],
                }],
            }),
            dispatcher: None,
            downstream_actor_id: vec![],
        };

        services
            .stream_manager
            .create_materialized_view(&table_ref_id_1, &mut [actor_1], vec![])
            .await?;

        services
            .stream_manager
            .create_materialized_view(&table_ref_id_2, &mut [actor_2], vec![])
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
