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

use sea_orm::entity::prelude::*;

use crate::model_v2::I32Array;

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(None)")]
pub enum SinkType {
    #[sea_orm(string_value = "APPEND_ONLY")]
    AppendOnly,
    #[sea_orm(string_value = "FORCE_APPEND_ONLY")]
    ForceAppendOnly,
    #[sea_orm(string_value = "UPSERT")]
    Upsert,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "sink")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub sink_id: i32,
    pub name: String,
    pub schema_id: i32,
    pub database_id: i32,
    pub columns: Option<Json>,
    pub pk_column_ids: Option<Json>,
    pub distribution_key: Option<I32Array>,
    pub downstream_pk: Option<I32Array>,
    pub sink_type: SinkType,
    pub properties: Option<Json>,
    pub definition: String,
    pub connection_id: Option<i32>,
    pub db_name: String,
    pub sink_from_name: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::connection::Entity",
        from = "Column::ConnectionId",
        to = "super::connection::Column::ConnectionId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Connection,
    #[sea_orm(
        belongs_to = "super::database::Entity",
        from = "Column::DatabaseId",
        to = "super::database::Column::DatabaseId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Database,
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::SinkId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
    #[sea_orm(
        belongs_to = "super::schema::Entity",
        from = "Column::SchemaId",
        to = "super::schema::Column::SchemaId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Schema,
}

impl Related<super::connection::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Connection.def()
    }
}

impl Related<super::database::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Database.def()
    }
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl Related<super::schema::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Schema.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
