// Copyright 2025 RisingWave Labs
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

use itertools::Itertools;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta_model::UserId;
use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_pb::user::user_service_server::UserService;
use risingwave_pb::user::{
    CreateUserRequest, CreateUserResponse, DropUserRequest, DropUserResponse,
    GrantPrivilegeRequest, GrantPrivilegeResponse, RevokePrivilegeRequest, RevokePrivilegeResponse,
    UpdateUserRequest, UpdateUserResponse,
};
use tonic::{Request, Response, Status};

pub struct UserServiceImpl {
    metadata_manager: MetadataManager,
}

impl UserServiceImpl {
    pub fn new(metadata_manager: MetadataManager) -> Self {
        Self { metadata_manager }
    }
}

#[async_trait::async_trait]
impl UserService for UserServiceImpl {
    #[cfg_attr(coverage, coverage(off))]
    async fn create_user(
        &self,
        request: Request<CreateUserRequest>,
    ) -> Result<Response<CreateUserResponse>, Status> {
        let req = request.into_inner();
        let version = self
            .metadata_manager
            .catalog_controller
            .create_user(req.get_user()?.clone())
            .await?;

        Ok(Response::new(CreateUserResponse {
            status: None,
            version,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn drop_user(
        &self,
        request: Request<DropUserRequest>,
    ) -> Result<Response<DropUserResponse>, Status> {
        let req = request.into_inner();
        let version = self
            .metadata_manager
            .catalog_controller
            .drop_user(req.user_id as _)
            .await?;

        Ok(Response::new(DropUserResponse {
            status: None,
            version,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn update_user(
        &self,
        request: Request<UpdateUserRequest>,
    ) -> Result<Response<UpdateUserResponse>, Status> {
        let req = request.into_inner();
        let update_fields = req
            .update_fields
            .iter()
            .map(|i| UpdateField::try_from(*i).unwrap())
            .collect_vec();
        let user = req.get_user()?.clone();

        let version = self
            .metadata_manager
            .catalog_controller
            .update_user(user, &update_fields)
            .await?;

        Ok(Response::new(UpdateUserResponse {
            status: None,
            version,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn grant_privilege(
        &self,
        request: Request<GrantPrivilegeRequest>,
    ) -> Result<Response<GrantPrivilegeResponse>, Status> {
        let req = request.into_inner();
        let user_ids: Vec<_> = req.get_user_ids().iter().map(|id| *id as UserId).collect();
        let version = self
            .metadata_manager
            .catalog_controller
            .grant_privilege(
                user_ids,
                req.get_privileges(),
                req.granted_by as _,
                req.with_grant_option,
            )
            .await?;

        Ok(Response::new(GrantPrivilegeResponse {
            status: None,
            version,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn revoke_privilege(
        &self,
        request: Request<RevokePrivilegeRequest>,
    ) -> Result<Response<RevokePrivilegeResponse>, Status> {
        let req = request.into_inner();
        let user_ids: Vec<_> = req.get_user_ids().iter().map(|id| *id as UserId).collect();
        let version = self
            .metadata_manager
            .catalog_controller
            .revoke_privilege(
                user_ids,
                req.get_privileges(),
                req.granted_by as _,
                req.revoke_by as _,
                req.revoke_grant_option,
                req.cascade,
            )
            .await?;

        Ok(Response::new(RevokePrivilegeResponse {
            status: None,
            version,
        }))
    }
}
