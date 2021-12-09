use crate::meta::{MetaManager, StreamMetaManager};
use risingwave_pb::meta::stream_manager_service_server::StreamManagerService;
use risingwave_pb::meta::{
    AddFragmentToWorkerRequest, AddFragmentToWorkerResponse, FetchActorInfoTableRequest,
    FetchActorInfoTableResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct StreamServiceImpl {
    mmc: Arc<MetaManager>,
}

impl StreamServiceImpl {
    pub fn new(mmc: Arc<MetaManager>) -> Self {
        StreamServiceImpl { mmc }
    }
}

#[async_trait::async_trait]
impl StreamManagerService for StreamServiceImpl {
    async fn fetch_actor_info_table(
        &self,
        request: Request<FetchActorInfoTableRequest>,
    ) -> Result<Response<FetchActorInfoTableResponse>, Status> {
        let _req = request.into_inner();
        let result = self.mmc.fetch_actor_info().await;
        match result {
            Ok(actor_info) => Ok(Response::new(FetchActorInfoTableResponse {
                status: None,
                info: actor_info,
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn add_fragment_to_worker(
        &self,
        request: Request<AddFragmentToWorkerRequest>,
    ) -> Result<Response<AddFragmentToWorkerResponse>, Status> {
        let _req = request.into_inner();
        let result = self.mmc.add_fragment_to_worker().await;
        match result {
            Ok(()) => Ok(Response::new(AddFragmentToWorkerResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }
}
