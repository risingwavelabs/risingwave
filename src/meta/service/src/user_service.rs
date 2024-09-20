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

use itertools::Itertools;
use risingwave_meta::manager::MetadataManager;
use risingwave_meta_model_v2::UserId;
use risingwave_pb::user::grant_privilege::Object;
use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_pb::user::user_service_server::UserService;
use risingwave_pb::user::{
    CreateUserRequest, CreateUserResponse, DropUserRequest, DropUserResponse, GrantPrivilege,
    GrantPrivilegeRequest, GrantPrivilegeResponse, RevokePrivilegeRequest, RevokePrivilegeResponse,
    UpdateUserRequest, UpdateUserResponse,
};
use tonic::{Request, Response, Status};

use crate::manager::MetaSrvEnv;
use crate::MetaResult;

pub struct UserServiceImpl {
    env: MetaSrvEnv,
    metadata_manager: MetadataManager,
}

impl UserServiceImpl {
    pub fn new(env: MetaSrvEnv, metadata_manager: MetadataManager) -> Self {
        Self {
            env,
            metadata_manager,
        }
    }

    /// Expands `GrantPrivilege` with object `GrantAllTables` or `GrantAllSources` to specific
    /// tables and sources, and set `with_grant_option` inside when grant privilege to a user.
    async fn expand_privilege(
        &self,
        privileges: &[GrantPrivilege],
        with_grant_option: Option<bool>,
    ) -> MetaResult<Vec<GrantPrivilege>> {
        let mut expanded_privileges = Vec::new();
        for privilege in privileges {
            if let Some(Object::AllTablesSchemaId(schema_id)) = &privilege.object {
                let tables = self
                    .metadata_manager
                    .catalog_controller
                    .list_readonly_table_ids(*schema_id as _)
                    .await?
                    .into_iter()
                    .map(|id| id as _)
                    .collect();
                for table_id in tables {
                    let mut privilege = privilege.clone();
                    privilege.object = Some(Object::TableId(table_id));
                    if let Some(true) = with_grant_option {
                        privilege
                            .action_with_opts
                            .iter_mut()
                            .for_each(|p| p.with_grant_option = true);
                    }
                    expanded_privileges.push(privilege);
                }
            } else if let Some(Object::AllDmlRelationsSchemaId(schema_id)) = &privilege.object {
                let tables = self
                    .metadata_manager
                    .catalog_controller
                    .list_dml_table_ids(*schema_id as _)
                    .await?
                    .into_iter()
                    .map(|id| id as _)
                    .collect();
                let views = self
                    .metadata_manager
                    .catalog_controller
                    .list_view_ids(*schema_id as _)
                    .await?
                    .into_iter()
                    .map(|id| id as _)
                    .collect();
                for table_id in tables {
                    let mut privilege = privilege.clone();
                    privilege.object = Some(Object::TableId(table_id));
                    if let Some(true) = with_grant_option {
                        privilege
                            .action_with_opts
                            .iter_mut()
                            .for_each(|p| p.with_grant_option = true);
                    }
                    expanded_privileges.push(privilege);
                }
                for view_id in views {
                    let mut privilege = privilege.clone();
                    privilege.object = Some(Object::ViewId(view_id));
                    if let Some(true) = with_grant_option {
                        privilege
                            .action_with_opts
                            .iter_mut()
                            .for_each(|p| p.with_grant_option = true);
                    }
                    expanded_privileges.push(privilege);
                }
            } else if let Some(Object::AllSourcesSchemaId(schema_id)) = &privilege.object {
                let sources = self
                    .metadata_manager
                    .catalog_controller
                    .list_source_ids(*schema_id as _)
                    .await?
                    .into_iter()
                    .map(|id| id as _)
                    .collect();
                for source_id in sources {
                    let mut privilege = privilege.clone();
                    privilege.object = Some(Object::SourceId(source_id));
                    if let Some(with_grant_option) = with_grant_option {
                        privilege.action_with_opts.iter_mut().for_each(|p| {
                            p.with_grant_option = with_grant_option;
                        });
                    }
                    expanded_privileges.push(privilege);
                }
            } else {
                let mut privilege = privilege.clone();
                if let Some(with_grant_option) = with_grant_option {
                    privilege.action_with_opts.iter_mut().for_each(|p| {
                        p.with_grant_option = with_grant_option;
                    });
                }
                expanded_privileges.push(privilege);
            }
        }

        Ok(expanded_privileges)
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
        let new_privileges = self
            .expand_privilege(req.get_privileges(), Some(req.with_grant_option))
            .await?;
        let user_ids: Vec<_> = req.get_user_ids().iter().map(|id| *id as UserId).collect();
        let version = self
            .metadata_manager
            .catalog_controller
            .grant_privilege(user_ids, &new_privileges, req.granted_by as _)
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
        let privileges = self.expand_privilege(req.get_privileges(), None).await?;
        let user_ids: Vec<_> = req.get_user_ids().iter().map(|id| *id as UserId).collect();
        let version = self
            .metadata_manager
            .catalog_controller
            .revoke_privilege(
                user_ids,
                &privileges,
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
