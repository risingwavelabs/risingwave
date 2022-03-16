use risingwave_pb::meta::epoch_service_server::EpochService;
use risingwave_pb::meta::{GetEpochRequest, GetEpochResponse};
use tonic::{Request, Response, Status};

use crate::manager::EpochGeneratorRef;

#[derive(Clone)]
pub struct EpochServiceImpl {
    epoch_generator: EpochGeneratorRef,
}

impl EpochServiceImpl {
    pub fn new(epoch_generator: EpochGeneratorRef) -> Self {
        EpochServiceImpl { epoch_generator }
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
            epoch: self.epoch_generator.generate().into_inner(),
        }))
    }
}
