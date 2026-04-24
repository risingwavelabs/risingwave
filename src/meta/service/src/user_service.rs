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

use itertools::Itertools;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta_model::UserId;
use risingwave_pb::user::alter_default_privilege_request::Operation;
use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_pb::user::user_service_server::UserService;
use risingwave_pb::user::{
    AlterDefaultPrivilegeRequest, AlterDefaultPrivilegeResponse, CreateUserRequest,
    CreateUserResponse, DropUserRequest, DropUserResponse, GrantPrivilegeRequest,
    GrantPrivilegeResponse, GrantRoleRequest, GrantRoleResponse, ListRoleMembershipsRequest,
    ListRoleMembershipsResponse, RevokePrivilegeRequest, RevokePrivilegeResponse,
    RevokeRoleRequest, RevokeRoleResponse, UpdateUserRequest, UpdateUserResponse,
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
                UserId::from(req.granted_by),
                req.with_grant_option,
            )
            .await?;

        Ok(Response::new(GrantPrivilegeResponse {
            status: None,
            version,
        }))
    }

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
                UserId::from(req.granted_by),
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

    async fn grant_role(
        &self,
        request: Request<GrantRoleRequest>,
    ) -> Result<Response<GrantRoleResponse>, Status> {
        let req = request.into_inner();
        let role_ids = req.role_ids.iter().map(|id| UserId::from(*id)).collect();
        let member_ids = req.member_ids.iter().map(|id| UserId::from(*id)).collect();
        let (version, memberships) = self
            .metadata_manager
            .catalog_controller
            .grant_role(
                role_ids,
                member_ids,
                UserId::from(req.granted_by),
                req.admin_option,
                req.inherit_option,
                req.set_option,
            )
            .await?;

        Ok(Response::new(GrantRoleResponse {
            status: None,
            version,
            memberships,
        }))
    }

    async fn revoke_role(
        &self,
        request: Request<RevokeRoleRequest>,
    ) -> Result<Response<RevokeRoleResponse>, Status> {
        let req = request.into_inner();
        let role_ids = req.role_ids.iter().map(|id| UserId::from(*id)).collect();
        let member_ids = req.member_ids.iter().map(|id| UserId::from(*id)).collect();
        let (version, memberships) = self
            .metadata_manager
            .catalog_controller
            .revoke_role(
                role_ids,
                member_ids,
                UserId::from(req.granted_by),
                UserId::from(req.revoked_by),
                req.revoke_admin_option,
                req.revoke_inherit_option,
                req.revoke_set_option,
                req.cascade,
            )
            .await?;

        Ok(Response::new(RevokeRoleResponse {
            status: None,
            version,
            memberships,
        }))
    }

    async fn list_role_memberships(
        &self,
        request: Request<ListRoleMembershipsRequest>,
    ) -> Result<Response<ListRoleMembershipsResponse>, Status> {
        let req = request.into_inner();
        let member_ids = req
            .member_ids
            .iter()
            .map(|id| UserId::from(*id))
            .collect_vec();
        let memberships = self
            .metadata_manager
            .catalog_controller
            .list_role_memberships(&member_ids)
            .await?;

        Ok(Response::new(ListRoleMembershipsResponse { memberships }))
    }

    async fn alter_default_privilege(
        &self,
        request: Request<AlterDefaultPrivilegeRequest>,
    ) -> Result<Response<AlterDefaultPrivilegeResponse>, Status> {
        let req = request.into_inner();
        let operation = req.get_operation()?;
        let user_ids: Vec<_> = req.get_user_ids().iter().map(|id| *id as UserId).collect();
        let schema_ids: Vec<_> = req.schema_ids.clone();
        match operation {
            Operation::GrantPrivilege(grant_privilege) => {
                self.metadata_manager
                    .catalog_controller
                    .grant_default_privileges(
                        user_ids,
                        req.database_id,
                        schema_ids,
                        UserId::from(req.granted_by),
                        grant_privilege.actions().collect(),
                        grant_privilege.get_object_type()?,
                        grant_privilege
                            .grantees
                            .iter()
                            .map(|id| *id as UserId)
                            .collect(),
                        grant_privilege.with_grant_option,
                    )
                    .await?
            }
            Operation::RevokePrivilege(revoke_privilege) => {
                self.metadata_manager
                    .catalog_controller
                    .revoke_default_privileges(
                        user_ids,
                        req.database_id,
                        schema_ids,
                        revoke_privilege.actions().collect(),
                        revoke_privilege.get_object_type()?,
                        revoke_privilege
                            .grantees
                            .iter()
                            .map(|id| *id as UserId)
                            .collect(),
                        revoke_privilege.revoke_grant_option,
                    )
                    .await?
            }
        }

        Ok(Response::new(AlterDefaultPrivilegeResponse {
            status: None,
        }))
    }
}
