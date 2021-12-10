use crate::manager::MetaManager;
use crate::stream::StreamMetaManager;
use risingwave_pb::meta::stream_manager_service_server::StreamManagerService;
use risingwave_pb::meta::{
    AddFragmentsToNodeRequest, AddFragmentsToNodeResponse, LoadAllFragmentsRequest,
    LoadAllFragmentsResponse,
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
    #[cfg(not(tarpaulin_include))]
    async fn add_fragments_to_node(
        &self,
        request: Request<AddFragmentsToNodeRequest>,
    ) -> Result<Response<AddFragmentsToNodeResponse>, Status> {
        let req = request.into_inner();
        self.mmc.fragment_lock.write().await;
        let result = self.mmc.add_fragments_to_node(req.get_location()).await;
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
        self.mmc.fragment_lock.read().await;
        Ok(Response::new(LoadAllFragmentsResponse {
            status: None,
            locations: self
                .mmc
                .load_all_fragments()
                .await
                .map_err(|e| e.to_grpc_status())?,
        }))
    }
}
