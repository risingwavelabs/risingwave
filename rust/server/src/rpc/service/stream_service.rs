use std::sync::Arc;

use risingwave_pb::stream_service::stream_service_server::StreamService;
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BroadcastActorInfoTableResponse, BuildActorsRequest,
    BuildActorsResponse, DropActorsRequest, DropActorsResponse, UpdateActorsRequest,
    UpdateActorsResponse,
};
use risingwave_stream::task::{StreamManager, StreamTaskEnv};
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct StreamServiceImpl {
    mgr: Arc<StreamManager>,
    env: StreamTaskEnv,
}

impl StreamServiceImpl {
    pub fn new(mgr: Arc<StreamManager>, env: StreamTaskEnv) -> Self {
        StreamServiceImpl { mgr, env }
    }
}

#[async_trait::async_trait]
impl StreamService for StreamServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn update_actors(
        &self,
        request: Request<UpdateActorsRequest>,
    ) -> std::result::Result<Response<UpdateActorsResponse>, Status> {
        let req = request.into_inner();
        let res = self.mgr.update_actors(&req.actors);
        match res {
            Err(e) => {
                error!("failed to update stream actor {}", e);
                Err(e.to_grpc_status())
            }
            Ok(()) => Ok(Response::new(UpdateActorsResponse { status: None })),
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn build_actors(
        &self,
        request: Request<BuildActorsRequest>,
    ) -> std::result::Result<Response<BuildActorsResponse>, Status> {
        let req = request.into_inner();

        let actor_id = req.actor_id;
        let res = self.mgr.build_actors(actor_id.as_slice(), self.env.clone());
        match res {
            Err(e) => {
                error!("failed to build actors {}", e);
                Err(e.to_grpc_status())
            }
            Ok(()) => Ok(Response::new(BuildActorsResponse {
                request_id: "".to_string(),
                actor_id: Vec::new(),
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
    async fn drop_actors(
        &self,
        request: Request<DropActorsRequest>,
    ) -> std::result::Result<Response<DropActorsResponse>, Status> {
        let actors = request.into_inner().actor_ids;
        self.mgr
            .drop_actor(&actors)
            .map_err(|e| e.to_grpc_status())?;
        Ok(Response::new(DropActorsResponse::default()))
    }
}
