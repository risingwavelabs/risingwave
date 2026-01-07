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
use risingwave_common::system_param::LICENSE_KEY_KEY;
use risingwave_meta::controller::system_param::SystemParamsControllerRef;
use risingwave_pb::meta::system_params_service_server::SystemParamsService;
use risingwave_pb::meta::{
    GetSystemParamsRequest, GetSystemParamsResponse, SetSystemParamRequest, SetSystemParamResponse,
};
use tonic::{Request, Response, Status};

pub struct SystemParamsServiceImpl {
    system_params_manager: SystemParamsControllerRef,

    /// Whether the license key is managed by license key file, i.e., `--license-key-path` is set.
    managed_license_key: bool,
}

impl SystemParamsServiceImpl {
    pub fn new(
        system_params_manager: SystemParamsControllerRef,
        managed_license_key: bool,
    ) -> Self {
        Self {
            system_params_manager,
            managed_license_key,
        }
    }
}

#[async_trait]
impl SystemParamsService for SystemParamsServiceImpl {
    async fn get_system_params(
        &self,
        _request: Request<GetSystemParamsRequest>,
    ) -> Result<Response<GetSystemParamsResponse>, Status> {
        let params = self.system_params_manager.get_pb_params().await;

        Ok(Response::new(GetSystemParamsResponse {
            params: Some(params),
        }))
    }

    async fn set_system_param(
        &self,
        request: Request<SetSystemParamRequest>,
    ) -> Result<Response<SetSystemParamResponse>, Status> {
        let req = request.into_inner();

        // When license key path is specified, license key from system parameters can be easily
        // overwritten. So we simply reject this case.
        if self.managed_license_key && req.param == LICENSE_KEY_KEY {
            return Err(Status::permission_denied(
                "cannot alter license key manually when \
                argument `--license-key-path` (or env var `RW_LICENSE_KEY_PATH`) is set, \
                please update the license key file instead",
            ));
        }

        let params = self
            .system_params_manager
            .set_param(&req.param, req.value)
            .await?;

        Ok(Response::new(SetSystemParamResponse {
            params: Some(params),
        }))
    }
}
