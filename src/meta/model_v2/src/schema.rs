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

use risingwave_pb::catalog::PbSchema;
use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue::Set;

use crate::SchemaId;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "schema")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub schema_id: SchemaId,
    pub name: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::SchemaId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl From<PbSchema> for ActiveModel {
    fn from(schema: PbSchema) -> Self {
        Self {
            schema_id: Set(schema.id as _),
            name: Set(schema.name),
        }
    }
}
