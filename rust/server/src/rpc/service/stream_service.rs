use std::sync::Arc;

use tonic::{Request, Response, Status};

use risingwave_pb::stream_service::stream_service_server::StreamService;
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BroadcastActorInfoTableResponse, BuildFragmentRequest,
    BuildFragmentResponse, DropFragmentsRequest, DropFragmentsResponse, UpdateFragmentRequest,
    UpdateFragmentResponse,
};

use crate::stream::StreamManager;
use crate::task::GlobalTaskEnv;

#[derive(Clone)]
pub struct StreamServiceImpl {
    mgr: Arc<StreamManager>,
    env: GlobalTaskEnv,
}

impl StreamServiceImpl {
    pub fn new(mgr: Arc<StreamManager>, env: GlobalTaskEnv) -> Self {
        StreamServiceImpl { mgr, env }
    }
}

#[async_trait::async_trait]
impl StreamService for StreamServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn update_fragment(
        &self,
        request: Request<UpdateFragmentRequest>,
    ) -> std::result::Result<Response<UpdateFragmentResponse>, Status> {
        let req = request.into_inner();
        let res = self.mgr.update_fragment(&req.fragment);
        match res {
            Err(e) => {
                error!("failed to update stream actor {}", e);
                Err(e.to_grpc_status())
            }
            Ok(()) => Ok(Response::new(UpdateFragmentResponse { status: None })),
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn build_fragment(
        &self,
        request: Request<BuildFragmentRequest>,
    ) -> std::result::Result<Response<BuildFragmentResponse>, Status> {
        let req = request.into_inner();

        let fragment_id = req.fragment_id;
        let res = self
            .mgr
            .build_fragment(fragment_id.as_slice(), self.env.clone());
        match res {
            Err(e) => {
                error!("failed to build fragments {}", e);
                Err(e.to_grpc_status())
            }
            Ok(()) => Ok(Response::new(BuildFragmentResponse {
                request_id: 0,
                fragment_id: Vec::new(),
            })),
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn broadcast_actor_info_table(
        &self,
        request: Request<BroadcastActorInfoTableRequest>,
    ) -> std::result::Result<Response<BroadcastActorInfoTableResponse>, Status> {
        let table = request.into_inner();

        let res = self.mgr.update_actor_info(table);
        match res {
            Err(e) => {
                error!("failed to update actor info table actor {}", e);
                Err(e.to_grpc_status())
            }
            Ok(()) => Ok(Response::new(BroadcastActorInfoTableResponse {
                status: None,
            })),
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn drop_fragment(
        &self,
        request: Request<DropFragmentsRequest>,
    ) -> std::result::Result<Response<DropFragmentsResponse>, Status> {
        let fragments = request.into_inner().fragment_ids;
        self.mgr
            .drop_fragment(fragments.as_slice())
            .map_err(|e| e.to_grpc_status())?;
        Ok(Response::new(DropFragmentsResponse::default()))
    }
}
