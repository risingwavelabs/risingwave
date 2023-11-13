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
use sea_orm::ActiveValue::Set;
use sea_orm::NotSet;

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
        let user_id = if user.id == 0 {
            NotSet
        } else {
            Set(user.id as _)
        };
        Self {
            user_id,
            name: Set(user.name),
            is_super: Set(user.is_super),
            can_create_db: Set(user.can_create_db),
            can_create_user: Set(user.can_create_user),
            can_login: Set(user.can_login),
            auth_info: Set(user.auth_info.map(AuthInfo)),
        }
    }
}

impl From<Model> for PbUserInfo {
    fn from(val: Model) -> Self {
        PbUserInfo {
            id: val.user_id as _,
            name: val.name,
            is_super: val.is_super,
            can_create_db: val.can_create_db,
            can_create_user: val.can_create_user,
            can_login: val.can_login,
            auth_info: val.auth_info.map(|x| x.into_inner()),
            grant_privileges: vec![], // fill in later
        }
    }
}
