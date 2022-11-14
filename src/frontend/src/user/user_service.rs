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

use std::sync::Arc;

use parking_lot::lock_api::ArcRwLockReadGuard;
use parking_lot::{RawRwLock, RwLock};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_pb::user::{GrantPrivilege, UserInfo};
use risingwave_rpc_client::MetaClient;
use tokio::sync::watch::Receiver;

use crate::user::user_manager::UserInfoManager;
use crate::user::{UserId, UserInfoVersion};

pub type UserInfoReadGuard = ArcRwLockReadGuard<RawRwLock, UserInfoManager>;

#[derive(Clone)]
pub struct UserInfoReader(Arc<RwLock<UserInfoManager>>);
impl UserInfoReader {
    pub fn new(inner: Arc<RwLock<UserInfoManager>>) -> Self {
        UserInfoReader(inner)
    }

    pub fn read_guard(&self) -> UserInfoReadGuard {
        self.0.read_arc()
    }
}

#[async_trait::async_trait]
pub trait UserInfoWriter: Send + Sync {
    async fn create_user(&self, user_info: UserInfo) -> Result<()>;

    async fn drop_user(&self, id: UserId) -> Result<()>;

    async fn update_user(&self, user: UserInfo, update_fields: Vec<UpdateField>) -> Result<()>;

    async fn grant_privilege(
        &self,
        users: Vec<UserId>,
        privileges: Vec<GrantPrivilege>,
        with_grant_option: bool,
        grantor: UserId,
    ) -> Result<()>;

    async fn revoke_privilege(
        &self,
        users: Vec<UserId>,
        privileges: Vec<GrantPrivilege>,
        granted_by: Option<UserId>,
        revoke_by: UserId,
        revoke_grant_option: bool,
        cascade: bool,
    ) -> Result<()>;
}

#[derive(Clone)]
pub struct UserInfoWriterImpl {
    meta_client: MetaClient,
    user_updated_rx: Receiver<UserInfoVersion>,
}

#[async_trait::async_trait]
impl UserInfoWriter for UserInfoWriterImpl {
    async fn create_user(&self, user_info: UserInfo) -> Result<()> {
        let version = self.meta_client.create_user(user_info).await?;
        self.wait_version(version).await
    }

    async fn drop_user(&self, id: UserId) -> Result<()> {
        let version = self.meta_client.drop_user(id).await?;
        self.wait_version(version).await
    }

    async fn update_user(&self, user: UserInfo, update_fields: Vec<UpdateField>) -> Result<()> {
        let version = self.meta_client.update_user(user, update_fields).await?;
        self.wait_version(version).await
    }

    async fn grant_privilege(
        &self,
        users: Vec<UserId>,
        privileges: Vec<GrantPrivilege>,
        with_grant_option: bool,
        granted_by: UserId,
    ) -> Result<()> {
        let version = self
            .meta_client
            .grant_privilege(users, privileges, with_grant_option, granted_by)
            .await?;
        self.wait_version(version).await
    }

    async fn revoke_privilege(
        &self,
        users: Vec<UserId>,
        privileges: Vec<GrantPrivilege>,
        granted_by: Option<UserId>,
        revoke_by: UserId,
        revoke_grant_option: bool,
        cascade: bool,
    ) -> Result<()> {
        let version = self
            .meta_client
            .revoke_privilege(
                users,
                privileges,
                granted_by,
                revoke_by,
                revoke_grant_option,
                cascade,
            )
            .await?;
        self.wait_version(version).await
    }
}

impl UserInfoWriterImpl {
    pub fn new(meta_client: MetaClient, user_updated_rx: Receiver<UserInfoVersion>) -> Self {
        UserInfoWriterImpl {
            meta_client,
            user_updated_rx,
        }
    }

    async fn wait_version(&self, version: UserInfoVersion) -> Result<()> {
        let mut rx = self.user_updated_rx.clone();
        while *rx.borrow_and_update() < version {
            rx.changed()
                .await
                .map_err(|e| RwError::from(InternalError(e.to_string())))?;
        }
        Ok(())
    }
}
