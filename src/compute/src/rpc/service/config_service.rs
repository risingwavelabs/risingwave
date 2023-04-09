// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
