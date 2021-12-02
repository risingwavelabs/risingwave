use crate::meta::MetaManager;
use risingwave_pb::meta::id_generator_service_server::IdGeneratorService;
use risingwave_pb::meta::{GetIdRequest, GetIdResponse};
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct IdGeneratorServiceImpl {
    mmc: Arc<MetaManager>,
}

impl IdGeneratorServiceImpl {
    pub fn new(mmc: Arc<MetaManager>) -> Self {
        IdGeneratorServiceImpl { mmc }
    }
}

#[async_trait::async_trait]
impl IdGeneratorService for IdGeneratorServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn get_id(
        &self,
        request: Request<GetIdRequest>,
    ) -> Result<Response<GetIdResponse>, Status> {
        let req = request.into_inner();
        let category = req.get_category();
        Ok(Response::new(GetIdResponse {
            status: None,
            id: self
                .mmc
                .id_gen_manager
                .generate(category)
                .await
                .map_err(|e| e.to_grpc_status())?,
        }))
    }
}
