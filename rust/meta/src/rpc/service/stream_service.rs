use risingwave_pb::meta::stream_manager_service_server::StreamManagerService;
use risingwave_pb::meta::{
    AddFragmentsToNodeRequest, AddFragmentsToNodeResponse, LoadAllFragmentsRequest,
    LoadAllFragmentsResponse,
};
use tonic::{Request, Response, Status};

use crate::stream::StreamMetaManagerRef;

#[derive(Clone)]
pub struct StreamServiceImpl {
    smm: StreamMetaManagerRef,
}

impl StreamServiceImpl {
    pub fn new(smm: StreamMetaManagerRef) -> Self {
        StreamServiceImpl { smm }
    }
}

#[async_trait::async_trait]
impl StreamManagerService for StreamServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn add_fragments_to_node(
        &self,
        request: Request<AddFragmentsToNodeRequest>,
    ) -> Result<Response<AddFragmentsToNodeResponse>, Status> {
        let req = request.into_inner();
        let result = self.smm.add_fragments_to_node(req.get_location()).await;
        match result {
            Ok(()) => Ok(Response::new(AddFragmentsToNodeResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn load_all_fragments(
        &self,
        request: Request<LoadAllFragmentsRequest>,
    ) -> Result<Response<LoadAllFragmentsResponse>, Status> {
        let _req = request.into_inner();
        Ok(Response::new(LoadAllFragmentsResponse {
            status: None,
            locations: self
                .smm
                .load_all_fragments()
                .await
                .map_err(|e| e.to_grpc_status())?,
        }))
    }
}
