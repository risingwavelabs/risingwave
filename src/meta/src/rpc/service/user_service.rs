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

use itertools::Itertools;
use risingwave_pb::user::grant_privilege::Object;
use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_pb::user::user_service_server::UserService;
use risingwave_pb::user::{
    CreateUserRequest, CreateUserResponse, DropUserRequest, DropUserResponse, GrantPrivilege,
    GrantPrivilegeRequest, GrantPrivilegeResponse, RevokePrivilegeRequest, RevokePrivilegeResponse,
    UpdateUserRequest, UpdateUserResponse,
};
use tonic::{Request, Response, Status};

use crate::manager::{CatalogManagerRef, IdCategory, MetaSrvEnv};
use crate::storage::MetaStore;
use crate::MetaResult;

// TODO: Change user manager as a part of the catalog manager, to ensure that operations on Catalog
// and User are transactional.
pub struct UserServiceImpl<S: MetaStore> {
    env: MetaSrvEnv<S>,

    catalog_manager: CatalogManagerRef<S>,
}

impl<S> UserServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(env: MetaSrvEnv<S>, catalog_manager: CatalogManagerRef<S>) -> Self {
        Self {
            env,
            catalog_manager,
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
                let tables = self.catalog_manager.list_tables(*schema_id).await?;
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
            } else if let Some(Object::AllSourcesSchemaId(source_id)) = &privilege.object {
                let sources = self.catalog_manager.list_source_ids(*source_id).await;
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
impl<S: MetaStore> UserService for UserServiceImpl<S> {
    #[cfg_attr(coverage, no_coverage)]
    async fn create_user(
        &self,
        request: Request<CreateUserRequest>,
    ) -> Result<Response<CreateUserResponse>, Status> {
        let req = request.into_inner();
        let id = self
            .env
            .id_gen_manager()
            .generate::<{ IdCategory::User }>()
            .await? as u32;
        let mut user = req.get_user()?.clone();
        user.id = id;
        let version = self.catalog_manager.create_user(&user).await?;

        Ok(Response::new(CreateUserResponse {
            status: None,
            version,
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn drop_user(
        &self,
        request: Request<DropUserRequest>,
    ) -> Result<Response<DropUserResponse>, Status> {
        let req = request.into_inner();
        let version = self.catalog_manager.drop_user(req.user_id).await?;

        Ok(Response::new(DropUserResponse {
            status: None,
            version,
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn update_user(
        &self,
        request: Request<UpdateUserRequest>,
    ) -> Result<Response<UpdateUserResponse>, Status> {
        let req = request.into_inner();
        let update_fields = req
            .update_fields
            .iter()
            .map(|i| UpdateField::from_i32(*i).unwrap())
            .collect_vec();
        let user = req.get_user()?.clone();
        let version = self
            .catalog_manager
            .update_user(&user, &update_fields)
            .await?;

        Ok(Response::new(UpdateUserResponse {
            status: None,
            version,
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn grant_privilege(
        &self,
        request: Request<GrantPrivilegeRequest>,
    ) -> Result<Response<GrantPrivilegeResponse>, Status> {
        let req = request.into_inner();
        let new_privileges = self
            .expand_privilege(req.get_privileges(), Some(req.with_grant_option))
            .await?;
        let version = self
            .catalog_manager
            .grant_privilege(&req.user_ids, &new_privileges, req.granted_by)
            .await?;

        Ok(Response::new(GrantPrivilegeResponse {
            status: None,
            version,
        }))
    }

    #[cfg_attr(coverage, no_coverage)]
    async fn revoke_privilege(
        &self,
        request: Request<RevokePrivilegeRequest>,
    ) -> Result<Response<RevokePrivilegeResponse>, Status> {
        let req = request.into_inner();
        let privileges = self.expand_privilege(req.get_privileges(), None).await?;
        let revoke_grant_option = req.revoke_grant_option;
        let version = self
            .catalog_manager
            .revoke_privilege(
                &req.user_ids,
                &privileges,
                req.granted_by,
                req.revoke_by,
                revoke_grant_option,
                req.cascade,
            )
            .await?;

        Ok(Response::new(RevokePrivilegeResponse {
            status: None,
            version,
        }))
    }
}
