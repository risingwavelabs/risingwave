// Copyright 2024 RisingWave Labs
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

use async_trait::async_trait;
use risingwave_meta::controller::session_params::SessionParamsControllerRef;
use risingwave_meta::manager::SessionParamsManagerRef;
use risingwave_pb::meta::session_param_service_server::SessionParamService;
use risingwave_pb::meta::{
    GetSessionParamsRequest, GetSessionParamsResponse, SetSessionParamRequest, SetSessionParamResponse,
};
use tonic::{Request, Response, Status};
use serde_json;

pub enum SessionParamsServiceImpl {
    Controller(SessionParamsControllerRef),
    Manager(SessionParamsManagerRef),
}

#[async_trait]
impl SessionParamService for SessionParamsServiceImpl {
    async fn get_session_params(
        &self,
        _request: Request<GetSessionParamsRequest>,
    ) -> Result<Response<GetSessionParamsResponse>, Status> {
        let params = match self {
            SessionParamsServiceImpl::Controller(controller) => {
                controller.get_params().await
            }
            SessionParamsServiceImpl::Manager(manager) => {
                manager.get_params().await
            }
        };
        let params_str = serde_json::to_string(&params).map_err(|e| Status::internal(format!("Failed to parse session config: {}", e)))?;

        Ok(Response::new(GetSessionParamsResponse {
            params: params_str,
        }))
    }

    async fn set_session_param(
        &self,
        request: Request<SetSessionParamRequest>,
    ) -> Result<Response<SetSessionParamResponse>, Status> {
        let req = request.into_inner();
        let req_param = req.get_param();

        let param_value = match self {
            SessionParamsServiceImpl::Controller(controller) => {
                controller.set_param(req_param, req.value.clone()).await
            }
            SessionParamsServiceImpl::Manager(manager) => {
                manager.set_param(req_param, req.value.clone()).await
            }
        };

        Ok(Response::new(SetSessionParamResponse {
            params: param_value?,
        }))
    }
}
