use risingwave_pb::meta::heartbeat_service_server::HeartbeatService;
use risingwave_pb::meta::{HeartbeatRequest, HeartbeatResponse};
use tonic::{Request, Response, Status};

#[derive(Clone, Default)]
pub struct HeartbeatServiceImpl {}

impl HeartbeatServiceImpl {
    pub fn new() -> Self {
        HeartbeatServiceImpl {}
    }
}

#[async_trait::async_trait]
impl HeartbeatService for HeartbeatServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        let _req = request.into_inner();
        Ok(Response::new(HeartbeatResponse { status: None }))
    }
}
