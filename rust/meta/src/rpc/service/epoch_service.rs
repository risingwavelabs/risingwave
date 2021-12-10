use crate::manager::MetaManager;
use risingwave_pb::meta::epoch_service_server::EpochService;
use risingwave_pb::meta::{GetEpochRequest, GetEpochResponse};
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct EpochServiceImpl {
    mmc: Arc<MetaManager>,
}

impl EpochServiceImpl {
    pub fn new(mmc: Arc<MetaManager>) -> Self {
        EpochServiceImpl { mmc }
    }
}

#[async_trait::async_trait]
impl EpochService for EpochServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn get_epoch(
        &self,
        request: Request<GetEpochRequest>,
    ) -> Result<Response<GetEpochResponse>, Status> {
        let _req = request.into_inner();
        Ok(Response::new(GetEpochResponse {
            status: None,
            epoch: self
                .mmc
                .epoch_generator
                .generate()
                .map_err(|e| e.to_grpc_status())?
                .into_inner(),
        }))
    }
}
