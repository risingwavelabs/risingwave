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

use risingwave_pb::catalog::source::OptionalAssociatedTableId;
use risingwave_pb::catalog::PbSource;
use sea_orm::entity::prelude::*;
use sea_orm::ActiveValue::Set;

use crate::{
    ColumnCatalogArray, ConnectionId, I32Array, Property, SourceId, StreamSourceInfo, TableId,
    WatermarkDescArray,
};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "source")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_id: SourceId,
    pub name: String,
    pub row_id_index: Option<i32>,
    pub columns: ColumnCatalogArray,
    pub pk_column_ids: I32Array,
    pub properties: Property,
    pub definition: String,
    pub source_info: Option<StreamSourceInfo>,
    pub watermark_descs: WatermarkDescArray,
    pub optional_associated_table_id: Option<TableId>,
    pub connection_id: Option<ConnectionId>,
    pub version: i64,
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
        belongs_to = "super::object::Entity",
        from = "Column::SourceId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
    #[sea_orm(has_many = "super::table::Entity")]
    Table,
}

impl Related<super::connection::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Connection.def()
    }
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl Related<super::table::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Table.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl From<PbSource> for ActiveModel {
    fn from(source: PbSource) -> Self {
        let optional_associated_table_id = source.optional_associated_table_id.map(|x| match x {
            OptionalAssociatedTableId::AssociatedTableId(id) => id as TableId,
        });
        Self {
            source_id: Set(source.id as _),
            name: Set(source.name),
            row_id_index: Set(source.row_id_index.map(|x| x as _)),
            columns: Set(ColumnCatalogArray(source.columns)),
            pk_column_ids: Set(I32Array(source.pk_column_ids)),
            properties: Set(Property(source.properties)),
            definition: Set(source.definition),
            source_info: Set(source.info.map(StreamSourceInfo)),
            watermark_descs: Set(WatermarkDescArray(source.watermark_descs)),
            optional_associated_table_id: Set(optional_associated_table_id),
            connection_id: Set(source.connection_id.map(|id| id as _)),
            version: Set(source.version as _),
        }
    }
}
