// Copyright 2026 RisingWave Labs
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

use risingwave_pb::user::PbRoleMembership;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{RoleMembershipId, UserId};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "user_role_membership")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: RoleMembershipId,
    pub role_id: UserId,
    pub member_id: UserId,
    pub granted_by: UserId,
    pub admin_option: bool,
    pub inherit_option: bool,
    pub set_option: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::RoleId",
        to = "super::user::Column::UserId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Role,
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::MemberId",
        to = "super::user::Column::UserId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Member,
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::GrantedBy",
        to = "super::user::Column::UserId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Grantor,
}

impl ActiveModelBehavior for ActiveModel {}

impl From<Model> for PbRoleMembership {
    fn from(value: Model) -> Self {
        Self {
            role_id: value.role_id.as_raw_id(),
            member_id: value.member_id.as_raw_id(),
            granted_by: value.granted_by.as_raw_id(),
            admin_option: value.admin_option,
            inherit_option: value.inherit_option,
            set_option: value.set_option,
        }
    }
}
