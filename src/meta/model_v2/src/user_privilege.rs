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

use risingwave_pb::user::grant_privilege::PbAction;
use sea_orm::entity::prelude::*;

use crate::{ObjectId, PrivilegeId, UserId};

#[derive(Clone, Debug, Hash, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(None)")]
pub enum Action {
    #[sea_orm(string_value = "INSERT")]
    Insert,
    #[sea_orm(string_value = "SELECT")]
    Select,
    #[sea_orm(string_value = "UPDATE")]
    Update,
    #[sea_orm(string_value = "DELETE")]
    Delete,
    #[sea_orm(string_value = "USAGE")]
    Usage,
    #[sea_orm(string_value = "CREATE")]
    Create,
    #[sea_orm(string_value = "CONNECT")]
    Connect,
}

impl From<PbAction> for Action {
    fn from(action: PbAction) -> Self {
        match action {
            PbAction::Unspecified => unreachable!("unspecified action"),
            PbAction::Insert => Self::Insert,
            PbAction::Select => Self::Select,
            PbAction::Update => Self::Update,
            PbAction::Delete => Self::Delete,
            PbAction::Usage => Self::Usage,
            PbAction::Create => Self::Create,
            PbAction::Connect => Self::Connect,
        }
    }
}

impl From<Action> for PbAction {
    fn from(action: Action) -> Self {
        match action {
            Action::Insert => Self::Insert,
            Action::Select => Self::Select,
            Action::Update => Self::Update,
            Action::Delete => Self::Delete,
            Action::Usage => Self::Usage,
            Action::Create => Self::Create,
            Action::Connect => Self::Connect,
        }
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "user_privilege")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: PrivilegeId,
    pub dependent_id: Option<PrivilegeId>,
    pub user_id: UserId,
    pub oid: ObjectId,
    pub granted_by: UserId,
    pub action: Action,
    pub with_grant_option: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::Oid",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Object,
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::GrantedBy",
        to = "super::user::Column::UserId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    User2,
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::UserId",
        to = "super::user::Column::UserId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    User1,
    #[sea_orm(
        belongs_to = "Entity",
        from = "Column::DependentId",
        to = "Column::Id",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    SelfRef,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
