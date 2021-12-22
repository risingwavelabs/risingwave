use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use log::debug;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_common::util::addr::get_host_port;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::{FragmentLocation, TableFragments};
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::StreamFragment;
use risingwave_pb::stream_service::stream_service_client::StreamServiceClient;
use risingwave_pb::stream_service::{
    ActorInfo, BroadcastActorInfoTableRequest, BuildFragmentRequest, UpdateFragmentRequest,
};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;

use crate::stream::{NodeManagerRef, ScheduleCategory, Scheduler, StreamMetaManagerRef};

#[async_trait]
pub trait StreamManager: Sync + Send {
    /// [`create_materialized_view`] creates materialized view using its stream fragments. Stream
    /// graph is generated in frontend, we only handle schedule and persistence here.
    async fn create_materialized_view(
        &self,
        table_id: &TableRefId,
        fragments: &[StreamFragment],
    ) -> Result<()>;
    /// [`drop_materialized_view`] drops materialized view.
    async fn drop_materialized_view(&self, table_id: &TableRefId, fragments: &[u32]) -> Result<()>;
}

pub struct DefaultStreamManager {
    smm: StreamMetaManagerRef,
    scheduler: Scheduler,
    /// [`clients`] stores the StreamServiceClient mapping: `node_id` => client.
    clients: RwLock<HashMap<u32, StreamServiceClient<Channel>>>,
}

impl DefaultStreamManager {
    /// FIXME: replace node manager when cluster management ready.
    pub fn new(smm: StreamMetaManagerRef, node_manager_ref: NodeManagerRef) -> Self {
        Self {
            smm,
            scheduler: Scheduler::new(ScheduleCategory::Simple, node_manager_ref),
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
    /// 1. schedule the fragments to nodes in the cluster.
    /// 2. broadcast the actor info table.
    /// 3. notify related nodes to update and build the fragments.
    /// 4. store related meta data.
    async fn create_materialized_view(
        &self,
        table_id: &TableRefId,
        fragments: &[StreamFragment],
    ) -> Result<()> {
        let fragment_ids = fragments
            .iter()
            .map(|f| f.get_fragment_id())
            .collect::<Vec<_>>();

        let nodes = self.scheduler.schedule(&fragment_ids).await?;

        let mut node_fragments_map = HashMap::new();
        for (node, fragment) in nodes.iter().zip(fragment_ids.clone()) {
            node_fragments_map
                .entry(node.get_id())
                .or_insert_with(Vec::new)
                .push(fragment);
        }

        let node_map = nodes
            .iter()
            .map(|n| (n.get_id(), n.clone()))
            .collect::<HashMap<u32, WorkerNode>>();

        let fragment_map = fragments
            .iter()
            .map(|f| (f.fragment_id, f.clone()))
            .collect::<HashMap<u32, StreamFragment>>();

        let actor_info = nodes
            .iter()
            .zip(fragment_ids.clone())
            .map(|(n, f)| ActorInfo {
                fragment_id: f,
                host: n.host.clone(),
            })
            .collect::<Vec<_>>();

        for (node_id, fragments) in node_fragments_map {
            let node = node_map.get(&node_id).cloned().unwrap();

            let client = self.get_client(node.clone()).await?;
            client
                .to_owned()
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_info.clone(),
                })
                .await
                .to_rw_result_with(format!("failed to connect to {}", node_id))?;

            let steam_fragments = fragments
                .iter()
                .map(|f| fragment_map.get(f).cloned().unwrap())
                .collect::<Vec<_>>();

            let request_id = Uuid::new_v4().to_string();
            debug!("[{}]update fragments: {:?}", request_id, fragments);
            client
                .to_owned()
                .update_fragment(UpdateFragmentRequest {
                    request_id,
                    fragment: steam_fragments.clone(),
                })
                .await
                .to_rw_result_with(format!("failed to connect to {}", node_id))?;

            let request_id = Uuid::new_v4().to_string();
            debug!("[{}]build fragments: {:?}", request_id, fragments);
            client
                .to_owned()
                .build_fragment(BuildFragmentRequest {
                    request_id,
                    fragment_id: fragments,
                })
                .await
                .to_rw_result_with(format!("failed to connect to {}", node_id))?;

            self.smm
                .add_fragments_to_node(&FragmentLocation {
                    node: Some(node),
                    fragments: steam_fragments,
                })
                .await?;
        }

        self.smm
            .add_table_fragments(
                &table_id.clone(),
                &TableFragments {
                    table_ref_id: Some(table_id.clone()),
                    fragment_ids,
                },
            )
            .await?;

        Ok(())
    }

    /// Drop materialized view, it works as follows:
    /// 1. notify related node local stream manger to drop fragment by inject barrier.
    /// 2. wait and collect drop state from local stream manager.
    /// 3. delete fragment location and node/table fragments info.
    async fn drop_materialized_view(
        &self,
        _table_id: &TableRefId,
        _fragments: &[u32],
    ) -> Result<()> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};
    use std::thread::sleep;

    use risingwave_pb::common::HostAddress;
    use risingwave_pb::stream_service::stream_service_server::{
        StreamService, StreamServiceServer,
    };
    use risingwave_pb::stream_service::{
        BroadcastActorInfoTableResponse, BuildFragmentResponse, DropFragmentsRequest,
        DropFragmentsResponse, UpdateFragmentResponse,
    };
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::manager::Config;
    use crate::storage::MemStore;
    use crate::stream::{NodeManager, StoredStreamMetaManager, StreamMetaManager};

    pub struct MockNodeManager {}

    #[async_trait]
    impl NodeManager for MockNodeManager {
        async fn list_nodes(&self) -> Result<Vec<WorkerNode>> {
            Ok(vec![WorkerNode {
                id: 0,
                host: Some(HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 12345,
                }),
            }])
        }
    }

    struct FakeFragmentState {
        fragment_streams: Mutex<HashMap<u32, StreamFragment>>,
        fragment_ids: Mutex<HashSet<u32>>,
        actor_infos: Mutex<HashMap<u32, HostAddress>>,
    }

    struct FakeStreamService {
        inner: Arc<FakeFragmentState>,
    }

    #[async_trait::async_trait]
    impl StreamService for FakeStreamService {
        async fn update_fragment(
            &self,
            request: Request<UpdateFragmentRequest>,
        ) -> std::result::Result<Response<UpdateFragmentResponse>, Status> {
            let req = request.into_inner();
            let mut guard = self.inner.fragment_streams.lock().unwrap();
            for fragment in req.get_fragment() {
                guard.insert(fragment.get_fragment_id(), fragment.clone());
            }

            Ok(Response::new(UpdateFragmentResponse { status: None }))
        }

        async fn build_fragment(
            &self,
            request: Request<BuildFragmentRequest>,
        ) -> std::result::Result<Response<BuildFragmentResponse>, Status> {
            let req = request.into_inner();
            let mut guard = self.inner.fragment_ids.lock().unwrap();
            for id in req.get_fragment_id() {
                guard.insert(*id);
            }

            Ok(Response::new(BuildFragmentResponse {
                request_id: "".to_string(),
                fragment_id: vec![],
            }))
        }

        async fn broadcast_actor_info_table(
            &self,
            request: Request<BroadcastActorInfoTableRequest>,
        ) -> std::result::Result<Response<BroadcastActorInfoTableResponse>, Status> {
            let req = request.into_inner();
            let mut guard = self.inner.actor_infos.lock().unwrap();
            for info in req.get_info() {
                guard.insert(info.get_fragment_id(), info.get_host().clone());
            }

            Ok(Response::new(BroadcastActorInfoTableResponse {
                status: None,
            }))
        }

        async fn drop_fragment(
            &self,
            _request: Request<DropFragmentsRequest>,
        ) -> std::result::Result<Response<DropFragmentsResponse>, Status> {
            panic!("not implemented")
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_materialized_view() -> Result<()> {
        // Start fake stream service.
        let addr = get_host_port("127.0.0.1:12345").unwrap();
        let state = Arc::new(FakeFragmentState {
            fragment_streams: Mutex::new(HashMap::new()),
            fragment_ids: Mutex::new(HashSet::new()),
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

        let meta_store_ref = Arc::new(MemStore::new());
        let meta_manager = Arc::new(StoredStreamMetaManager::new(
            Config::default(),
            meta_store_ref,
        ));
        let node_manager_ref = Arc::new(MockNodeManager {});
        let stream_manager =
            DefaultStreamManager::new(meta_manager.clone(), node_manager_ref.clone());

        let table_ref_id = TableRefId {
            schema_ref_id: None,
            table_id: 0,
        };
        let fragments = (0..5)
            .map(|i| StreamFragment {
                fragment_id: i,
                nodes: None,
                dispatcher: None,
                downstream_fragment_id: vec![],
            })
            .collect::<Vec<_>>();

        stream_manager
            .create_materialized_view(&table_ref_id, &fragments)
            .await?;

        for f in fragments.clone() {
            assert_eq!(
                state
                    .fragment_streams
                    .lock()
                    .unwrap()
                    .get(&f.get_fragment_id())
                    .cloned()
                    .unwrap(),
                f
            );
            assert!(state
                .fragment_ids
                .lock()
                .unwrap()
                .contains(&f.get_fragment_id()));
            assert_eq!(
                state
                    .actor_infos
                    .lock()
                    .unwrap()
                    .get(&f.get_fragment_id())
                    .cloned()
                    .unwrap(),
                HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 12345,
                }
            );
        }

        let locations = meta_manager.load_all_fragments().await?;
        assert_eq!(locations.len(), 1);
        assert_eq!(locations.get(0).unwrap().get_node().get_id(), 0);
        assert_eq!(locations.get(0).unwrap().fragments, fragments);
        let table_fragments = meta_manager.get_table_fragments(&table_ref_id).await?;
        assert_eq!(table_fragments.fragment_ids, (0..5).collect::<Vec<u32>>());

        // Gracefully terminate the server.
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();

        Ok(())
    }
}
