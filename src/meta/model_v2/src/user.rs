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

use risingwave_pb::user::PbUserInfo;
use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue;

use crate::{AuthInfo, UserId};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "user")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub user_id: UserId,
    #[sea_orm(unique)]
    pub name: String,
    pub is_super: bool,
    pub can_create_db: bool,
    pub can_create_user: bool,
    pub can_login: bool,
    pub auth_info: Option<AuthInfo>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::object::Entity")]
    Object,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl From<PbUserInfo> for ActiveModel {
    fn from(user: PbUserInfo) -> Self {
        Self {
            user_id: ActiveValue::Set(user.id),
            name: ActiveValue::Set(user.name),
            is_super: ActiveValue::Set(user.is_super),
            can_create_db: ActiveValue::Set(user.can_create_db),
            can_create_user: ActiveValue::Set(user.can_create_user),
            can_login: ActiveValue::Set(user.can_login),
            auth_info: ActiveValue::Set(user.auth_info.map(AuthInfo)),
        }
    }
}

impl Into<PbUserInfo> for Model {
    fn into(self) -> PbUserInfo {
        PbUserInfo {
            id: self.user_id,
            name: self.name,
            is_super: self.is_super,
            can_create_db: self.can_create_db,
            can_create_user: self.can_create_user,
            can_login: self.can_login,
            auth_info: self.auth_info.map(|x| x.0),
            grant_privileges: vec![], // fill in later
        }
    }
}
