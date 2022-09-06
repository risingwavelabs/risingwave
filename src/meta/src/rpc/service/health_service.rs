use risingwave_pb::health::health_check_response::ServingStatus;
use risingwave_pb::health::health_server::Health;
use risingwave_pb::health::{HealthCheckRequest, HealthCheckResponse};
use tonic::{Request, Response, Status};

pub struct HealthServiceImpl {}

impl HealthServiceImpl {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl Health for HealthServiceImpl {
    async fn check(
        &self,
        _request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        // Reply serving as long as tonic service is started
        Ok(Response::new(HealthCheckResponse {
            status: ServingStatus::Serving as i32,
        }))
    }
}
