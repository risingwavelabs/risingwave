use risingwave_pb::meta::epoch_service_server::EpochService;
use risingwave_pb::meta::{GetEpochRequest, GetEpochResponse};
use tonic::{Request, Response, Status};

use crate::manager::MetaSrvEnv;

#[derive(Clone)]
pub struct EpochServiceImpl {
    env: MetaSrvEnv,
}

impl EpochServiceImpl {
    pub fn new(env: MetaSrvEnv) -> Self {
        EpochServiceImpl { env }
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
                .env
                .epoch_generator()
                .generate()
                .map_err(|e| e.to_grpc_status())?
                .into_inner(),
        }))
    }
}
