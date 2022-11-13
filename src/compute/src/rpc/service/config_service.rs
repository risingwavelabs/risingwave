use std::sync::Arc;

use risingwave_batch::task::BatchManager;
use risingwave_pb::compute::config_service_server::ConfigService;
use risingwave_pb::compute::{ShowConfigRequest, ShowConfigResponse};
use risingwave_stream::task::LocalStreamManager;
use serde_json;
use tonic::{Request, Response, Status};

pub struct ConfigServiceImpl {
    batch_mgr: Arc<BatchManager>,
    stream_mgr: Arc<LocalStreamManager>,
}

#[async_trait::async_trait]
impl ConfigService for ConfigServiceImpl {
    async fn show_config(
        &self,
        _request: Request<ShowConfigRequest>,
    ) -> Result<Response<ShowConfigResponse>, Status> {
        let batch_config = serde_json::to_string(self.batch_mgr.config())
            .map_err(|e| tonic::Status::internal(format!("{}", e)))?;
        let stream_config = serde_json::to_string(&self.stream_mgr.config().await)
            .map_err(|e| tonic::Status::internal(format!("{}", e)))?;

        let show_config_response = ShowConfigResponse {
            batch_config,
            stream_config,
        };
        Ok(Response::new(show_config_response))
    }
}

impl ConfigServiceImpl {
    pub fn new(batch_mgr: Arc<BatchManager>, stream_mgr: Arc<LocalStreamManager>) -> Self {
        Self {
            batch_mgr,
            stream_mgr,
        }
    }
}
