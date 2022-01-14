use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::debug;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_common::util::addr::get_host_port;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::{ActorLocation, TableActors};
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::StreamActor;
use risingwave_pb::stream_service::stream_service_client::StreamServiceClient;
use risingwave_pb::stream_service::{
    ActorInfo, BroadcastActorInfoTableRequest, BuildActorsRequest, UpdateActorsRequest,
};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

use crate::cluster::StoredClusterManager;
use crate::stream::{ScheduleCategory, Scheduler, StreamMetaManagerRef};

#[async_trait]
pub trait StreamManager: Sync + Send + 'static {
    /// [`create_materialized_view`] creates materialized view using its stream actors. Stream
    /// graph is generated in frontend, we only handle schedule and persistence here.
    async fn create_materialized_view(
        &self,
        table_id: &TableRefId,
        actors: &mut [StreamActor],
    ) -> Result<()>;
    /// [`drop_materialized_view`] drops materialized view.
    async fn drop_materialized_view(&self, table_id: &TableRefId, actors: &[u32]) -> Result<()>;
}

pub type StreamManagerRef = Arc<dyn StreamManager>;

pub struct DefaultStreamManager {
    smm: StreamMetaManagerRef,
    scheduler: Scheduler,
    /// [`clients`] stores the StreamServiceClient mapping: `node_id` => client.
    clients: RwLock<HashMap<u32, StreamServiceClient<Channel>>>,
}

impl DefaultStreamManager {
    pub fn new(smm: StreamMetaManagerRef, cluster_manager: Arc<StoredClusterManager>) -> Self {
        Self {
            smm,
            scheduler: Scheduler::new(ScheduleCategory::Simple, cluster_manager),
            clients: RwLock::new(HashMap::new()),
        }
    }

    async fn get_client(&self, node: WorkerNode) -> Result<StreamServiceClient<Channel>> {
        {
            let guard = self.clients.read().await;
            let client = guard.get(&node.get_id());
            if client.is_some() {
                return Ok(client.cloned().unwrap());
            }
        }

        let mut guard = self.clients.write().await;
        let client = guard.get(&node.get_id());
        if client.is_some() {
            Ok(client.cloned().unwrap())
        } else {
            let addr = get_host_port(
                format!(
                    "{}:{}",
                    node.get_host().get_host(),
                    node.get_host().get_port()
                )
                .as_str(),
            )
            .unwrap();
            let endpoint = Endpoint::from_shared(format!("http://{}", addr));
            let client = StreamServiceClient::new(
                endpoint
                    .map_err(|e| InternalError(format!("{}", e)))?
                    .connect_timeout(Duration::from_secs(5))
                    .connect()
                    .await
                    .to_rw_result_with(format!("failed to connect to {}", node.get_id()))?,
            );
            guard.insert(node.get_id(), client.clone());
            Ok(client)
        }
    }
}

#[async_trait]
impl StreamManager for DefaultStreamManager {
    /// Create materialized view, it works as follows:
    /// 1. schedule the actors to nodes in the cluster.
    /// 2. broadcast the actor info table.
    /// 3. notify related nodes to update and build the actors.
    /// 4. store related meta data.
    async fn create_materialized_view(
        &self,
        table_id: &TableRefId,
        actors: &mut [StreamActor],
    ) -> Result<()> {
        // Fill `upstream_actor_id` of [`ChainNode`].
        for actor in actors.iter_mut() {
            if let risingwave_pb::stream_plan::stream_node::Node::ChainNode(chain) =
                actor.nodes.as_mut().unwrap().node.as_mut().unwrap()
            {
                // TODO(MrCroxx): A create materialized view plan will be splited into multiple
                // stages. Currently, we simply assume that MView lays on the first
                // actor. We need an interface in [`StreamMetaManager`] to query the
                // actor id of the MView node.
                let table_fragments = self
                    .smm
                    .get_table_actors(&chain.table_ref_id.clone().unwrap())
                    .await?;
                chain.upstream_actor_id = table_fragments.actor_ids[0];
            }
        }

        let actor_ids = actors.iter().map(|a| a.get_actor_id()).collect::<Vec<_>>();

        let nodes = self.scheduler.schedule(&actor_ids).await?;

        let mut node_actors_map = HashMap::new();
        for (node, actor) in nodes.iter().zip(actor_ids.clone()) {
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
            .zip(actor_ids.clone())
            .map(|(n, f)| ActorInfo {
                actor_id: f,
                host: n.host.clone(),
            })
            .collect::<Vec<_>>();

        for (node_id, actors) in node_actors_map {
            let node = node_map.get(&node_id).cloned().unwrap();

            let client = self.get_client(node.clone()).await?;
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
                    node: Some(node),
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

    /// Drop materialized view, it works as follows:
    /// 1. notify related node local stream manger to drop actor by inject barrier.
    /// 2. wait and collect drop state from local stream manager.
    /// 3. delete actor location and node/table actors info.
    async fn drop_materialized_view(&self, _table_id: &TableRefId, _actors: &[u32]) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::thread::sleep;

    use risingwave_pb::common::HostAddress;
    use risingwave_pb::meta::ClusterType;
    use risingwave_pb::stream_service::stream_service_server::{
        StreamService, StreamServiceServer,
    };
    use risingwave_pb::stream_service::{
        BroadcastActorInfoTableResponse, BuildActorsResponse, DropActorsRequest,
        DropActorsResponse, UpdateActorsResponse,
    };
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::cluster::WorkerNodeMetaManager;
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
                actor_id: vec![],
            }))
        }

        async fn broadcast_actor_info_table(
            &self,
            request: Request<BroadcastActorInfoTableRequest>,
        ) -> std::result::Result<Response<BroadcastActorInfoTableResponse>, Status> {
            let req = request.into_inner();
            let mut guard = self.inner.actor_infos.lock().unwrap();
            for info in req.get_info() {
                guard.insert(info.get_actor_id(), info.get_host().clone());
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
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_materialized_view() -> Result<()> {
        // Start fake stream service.
        let addr = get_host_port("127.0.0.1:12345").unwrap();
        let state = Arc::new(FakeFragmentState {
            actor_streams: Mutex::new(HashMap::new()),
            actor_ids: Mutex::new(HashSet::new()),
            actor_infos: Mutex::new(HashMap::new()),
        });
        let fake_service = FakeStreamService {
            inner: state.clone(),
        };

        let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
        let stream_srv = StreamServiceServer::new(fake_service);
        let join_handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(stream_srv)
                .serve_with_shutdown(addr, async move {
                    shutdown_recv.recv().await;
                })
                .await
                .unwrap();
        });
        sleep(Duration::from_secs(1));

        let env = MetaSrvEnv::for_test().await;
        let meta_manager = Arc::new(StoredStreamMetaManager::new(env.clone()));
        let cluster_manager = Arc::new(StoredClusterManager::new(env));
        cluster_manager
            .add_worker_node(
                HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 12345,
                },
                ClusterType::ComputeNode,
            )
            .await?;

        let stream_manager = DefaultStreamManager::new(meta_manager.clone(), cluster_manager);

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

        stream_manager
            .create_materialized_view(&table_ref_id, &mut actors)
            .await?;

        for actor in actors.clone() {
            assert_eq!(
                state
                    .actor_streams
                    .lock()
                    .unwrap()
                    .get(&actor.get_actor_id())
                    .cloned()
                    .unwrap(),
                actor
            );
            assert!(state
                .actor_ids
                .lock()
                .unwrap()
                .contains(&actor.get_actor_id()));
            assert_eq!(
                state
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

        let locations = meta_manager.load_all_actors().await?;
        assert_eq!(locations.len(), 1);
        assert_eq!(locations.get(0).unwrap().get_node().get_id(), 0);
        assert_eq!(locations.get(0).unwrap().actors, actors);
        let table_actors = meta_manager.get_table_actors(&table_ref_id).await?;
        assert_eq!(table_actors.actor_ids, (0..5).collect::<Vec<u32>>());

        // Gracefully terminate the server.
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();

        Ok(())
    }
}
