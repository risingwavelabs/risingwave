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

use risingwave_pb::catalog::PbIndex;
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{ExprNodeArray, IndexColumnPropertiesArray, IndexId, TableId};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "index")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub index_id: IndexId,
    pub name: String,
    pub index_table_id: TableId,
    pub primary_table_id: TableId,
    pub index_items: ExprNodeArray,
    pub index_column_properties: Option<IndexColumnPropertiesArray>,
    pub index_columns_len: i32,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::IndexId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
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

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl From<PbIndex> for ActiveModel {
    fn from(pb_index: PbIndex) -> Self {
        Self {
            index_id: Set(pb_index.id as _),
            name: Set(pb_index.name),
            index_table_id: Set(pb_index.index_table_id as _),
            primary_table_id: Set(pb_index.primary_table_id as _),
            index_items: Set(pb_index.index_item.into()),
            index_columns_len: Set(pb_index.index_columns_len as _),
            index_column_properties: Set(Some(pb_index.index_column_properties.into())),
        }
    }
}
