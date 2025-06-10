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

use sea_orm::entity::prelude::*;

use crate::object::ObjectType;
use crate::user_privilege::Action;
use crate::{DatabaseId, DefaultPrivilegeId, SchemaId, UserId};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "user_default_privilege")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: DefaultPrivilegeId,
    pub database_id: DatabaseId,
    pub schema_id: Option<SchemaId>,
    pub object_type: ObjectType,
    pub for_materialized_view: Option<bool>,
    pub user_id: UserId,
    pub grantee: UserId,
    pub granted_by: UserId,
    pub action: Action,
    pub with_grant_option: bool,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::database::Entity",
        from = "Column::DatabaseId",
        to = "super::database::Column::DatabaseId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Database,
    #[sea_orm(
        belongs_to = "super::schema::Entity",
        from = "Column::SchemaId",
        to = "super::schema::Column::SchemaId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Schema,
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::UserId",
        to = "super::user::Column::UserId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    User3,
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::Grantee",
        to = "super::user::Column::UserId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    User2,
    #[sea_orm(
        belongs_to = "super::user::Entity",
        from = "Column::GrantedBy",
        to = "super::user::Column::UserId",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    User1,
}

impl Related<super::database::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Database.def()
    }
}

impl Related<super::schema::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Schema.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
