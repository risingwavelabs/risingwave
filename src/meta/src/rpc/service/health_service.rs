// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
