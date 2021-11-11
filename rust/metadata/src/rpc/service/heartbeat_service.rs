use crate::metadata::MetaManager;
use risingwave_pb::metadata::heartbeat_service_server::HeartbeatService;
use risingwave_pb::metadata::{HeartbeatRequest, HeartbeatResponse};
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct HeartbeatServiceImpl {
    mmc: Arc<MetaManager>,
}

impl HeartbeatServiceImpl {
    pub fn new(mmc: Arc<MetaManager>) -> Self {
        HeartbeatServiceImpl { mmc }
    }
}

#[async_trait::async_trait]
impl HeartbeatService for HeartbeatServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        todo!()
    }
}
