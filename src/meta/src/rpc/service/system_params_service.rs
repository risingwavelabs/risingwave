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

use async_trait::async_trait;
use risingwave_pb::meta::system_params_service_server::SystemParamsService;
use risingwave_pb::meta::{
    GetSystemParamsRequest, GetSystemParamsResponse, SetSystemParamRequest, SetSystemParamResponse,
};
use tonic::{Request, Response, Status};

use crate::controller::system_param::SystemParamsControllerRef;
use crate::manager::SystemParamsManagerRef;

pub struct SystemParamsServiceImpl {
    system_params_manager: SystemParamsManagerRef,
    system_params_controller: Option<SystemParamsControllerRef>,
}

impl SystemParamsServiceImpl {
    pub fn new(
        system_params_manager: SystemParamsManagerRef,
        system_params_controller: Option<SystemParamsControllerRef>,
    ) -> Self {
        Self {
            system_params_manager,
            system_params_controller,
        }
    }
}

#[async_trait]
impl SystemParamsService for SystemParamsServiceImpl {
    async fn get_system_params(
        &self,
        _request: Request<GetSystemParamsRequest>,
    ) -> Result<Response<GetSystemParamsResponse>, Status> {
        let params = if let Some(ctl) = &self.system_params_controller {
            ctl.get_pb_params().await
        } else {
            self.system_params_manager.get_pb_params().await
        };

        Ok(Response::new(GetSystemParamsResponse {
            params: Some(params),
        }))
    }

    async fn set_system_param(
        &self,
        request: Request<SetSystemParamRequest>,
    ) -> Result<Response<SetSystemParamResponse>, Status> {
        let req = request.into_inner();
        let params = if let Some(ctl) = &self.system_params_controller {
            ctl.set_param(&req.param, req.value).await?
        } else {
            self.system_params_manager
                .set_param(&req.param, req.value)
                .await?
        };

        Ok(Response::new(SetSystemParamResponse {
            params: Some(params),
        }))
    }
}
