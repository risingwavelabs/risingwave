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

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "database")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub database_id: i32,
    #[sea_orm(unique)]
    pub name: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::connection::Entity")]
    Connection,
    #[sea_orm(has_many = "super::function::Entity")]
    Function,
    #[sea_orm(has_many = "super::index::Entity")]
    Index,
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::DatabaseId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
    #[sea_orm(has_many = "super::schema::Entity")]
    Schema,
    #[sea_orm(has_many = "super::sink::Entity")]
    Sink,
    #[sea_orm(has_many = "super::source::Entity")]
    Source,
    #[sea_orm(has_many = "super::table::Entity")]
    Table,
    #[sea_orm(has_many = "super::view::Entity")]
    View,
}

impl Related<super::connection::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Connection.def()
    }
}

impl Related<super::function::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Function.def()
    }
}

impl Related<super::index::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Index.def()
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

impl Related<super::sink::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Sink.def()
    }
}

impl Related<super::source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Source.def()
    }
}

impl Related<super::table::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Table.def()
    }
}

impl Related<super::view::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::View.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
