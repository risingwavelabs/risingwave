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

use risingwave_pb::catalog::function::Kind;
use risingwave_pb::catalog::PbFunction;
use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue;

use crate::model_v2::{DataType, DataTypeArray};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(None)")]
pub enum FunctionKind {
    #[sea_orm(string_value = "Scalar")]
    Scalar,
    #[sea_orm(string_value = "Table")]
    Table,
    #[sea_orm(string_value = "Aggregate")]
    Aggregate,
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "function")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub function_id: i32,
    pub name: String,
    pub schema_id: i32,
    pub database_id: i32,
    pub arg_types: DataTypeArray,
    pub return_type: DataType,
    pub language: String,
    pub link: String,
    pub identifier: String,
    pub kind: FunctionKind,
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
        from = "Column::FunctionId",
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

impl From<Kind> for FunctionKind {
    fn from(kind: Kind) -> Self {
        match kind {
            Kind::Scalar(_) => Self::Scalar,
            Kind::Table(_) => Self::Table,
            Kind::Aggregate(_) => Self::Aggregate,
        }
    }
}

impl From<PbFunction> for ActiveModel {
    fn from(function: PbFunction) -> Self {
        Self {
            function_id: ActiveValue::Set(function.id as _),
            name: ActiveValue::Set(function.name),
            schema_id: ActiveValue::Set(function.schema_id as _),
            database_id: ActiveValue::Set(function.database_id as _),
            arg_types: ActiveValue::Set(DataTypeArray(function.arg_types)),
            return_type: ActiveValue::Set(DataType(function.return_type.unwrap())),
            language: ActiveValue::Set(function.language),
            link: ActiveValue::Set(function.link),
            identifier: ActiveValue::Set(function.identifier),
            kind: ActiveValue::Set(function.kind.unwrap().into()),
        }
    }
}
