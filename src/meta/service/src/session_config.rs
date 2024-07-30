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
use risingwave_meta::manager::SessionParamsManagerImpl;
use risingwave_pb::meta::session_param_service_server::SessionParamService;
use risingwave_pb::meta::{
    GetSessionParamsRequest, GetSessionParamsResponse, SetSessionParamRequest,
    SetSessionParamResponse,
};
use thiserror_ext::AsReport;
use tonic::{Request, Response, Status};

pub struct SessionParamsServiceImpl {
    session_params_manager: SessionParamsManagerImpl,
}

impl SessionParamsServiceImpl {
    pub fn new(session_params_manager: SessionParamsManagerImpl) -> Self {
        Self {
            session_params_manager,
        }
    }
}

#[async_trait]
impl SessionParamService for SessionParamsServiceImpl {
    async fn get_session_params(
        &self,
        _request: Request<GetSessionParamsRequest>,
    ) -> Result<Response<GetSessionParamsResponse>, Status> {
        let params = match &self.session_params_manager {
            SessionParamsManagerImpl::Kv(controller) => controller.get_params().await,
            SessionParamsManagerImpl::Sql(manager) => manager.get_params().await,
        };
        let params_str = serde_json::to_string(&params).map_err(|e| {
            Status::internal(format!("Failed to parse session config: {}", e.as_report()))
        })?;

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

        let param_value = match &self.session_params_manager {
            SessionParamsManagerImpl::Kv(controller) => {
                controller.set_param(req_param, req.value.clone()).await
            }
            SessionParamsManagerImpl::Sql(manager) => {
                manager.set_param(req_param, req.value.clone()).await
            }
        };

        Ok(Response::new(SetSessionParamResponse {
            param: param_value?,
        }))
    }
}
