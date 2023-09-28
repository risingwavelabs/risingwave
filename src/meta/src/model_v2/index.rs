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

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "index")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub index_id: i32,
    pub name: String,
    pub schema_id: i32,
    pub database_id: i32,
    pub index_table_id: i32,
    pub primary_table_id: i32,
    pub index_items: Option<Json>,
    pub original_columns: Option<I32Array>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
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
        from = "Column::IndexId",
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
    #[sea_orm(
        belongs_to = "super::table::Entity",
        from = "Column::IndexTableId",
        to = "super::table::Column::TableId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Table2,
    #[sea_orm(
        belongs_to = "super::table::Entity",
        from = "Column::PrimaryTableId",
        to = "super::table::Column::TableId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Table1,
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
