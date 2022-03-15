use risingwave_pb::meta::heartbeat_service_server::HeartbeatService;
use risingwave_pb::meta::{HeartbeatRequest, HeartbeatResponse};
use tonic::{Request, Response, Status};

use crate::cluster::StoredClusterManagerRef;
use crate::storage::MetaStore;

#[derive(Clone)]
pub struct HeartbeatServiceImpl<S>
where
    S: MetaStore,
{
    cluster_manager_ref: StoredClusterManagerRef<S>,
}

impl<S> HeartbeatServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(cluster_manager_ref: StoredClusterManagerRef<S>) -> Self {
        HeartbeatServiceImpl {
            cluster_manager_ref,
        }
    }
}

#[async_trait::async_trait]
impl<S> HeartbeatService for HeartbeatServiceImpl<S>
where
    S: MetaStore,
{
    #[cfg(not(tarpaulin_include))]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let req = request.into_inner();
        let result = self.cluster_manager_ref.heartbeat(req.node_id).await;
        match result {
            Ok(_) => Ok(Response::new(HeartbeatResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }
}
