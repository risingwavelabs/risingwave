use crate::storage::TableManagerRef;
use crate::stream::StreamManager;
use risingwave_pb::stream_service::stream_service_server::StreamService;
use risingwave_pb::stream_service::{
    ActorInfoTable, BroadcastActorInfoTableResponse, BuildFragmentRequest, BuildFragmentResponse,
    UpdateFragmentRequest, UpdateFragmentResponse,
};
use risingwave_pb::ToProto;
use risingwave_proto::stream_plan::StreamFragment;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct StreamServiceImpl {
    mgr: Arc<StreamManager>,
    table_manager: TableManagerRef,
}

impl StreamServiceImpl {
    pub fn new(mgr: Arc<StreamManager>, table_manager: TableManagerRef) -> Self {
        StreamServiceImpl { mgr, table_manager }
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

        let fragments = req
            .fragment
            .iter()
            .map(|f| f.to_proto())
            .collect::<Vec<StreamFragment>>();
        let res = self.mgr.update_fragment(fragments.as_slice());
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
            .build_fragment(fragment_id.as_slice(), self.table_manager.clone());
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
        request: Request<ActorInfoTable>,
    ) -> std::result::Result<Response<BroadcastActorInfoTableResponse>, Status> {
        let table = request.into_inner();

        let res = self.mgr.update_actor_info(table.to_proto());
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
}
