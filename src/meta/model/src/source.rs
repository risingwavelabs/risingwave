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

use risingwave_pb::catalog::PbSource;
use risingwave_pb::catalog::source::OptionalAssociatedTableId;
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    CdcEtlSourceInfo, ColumnCatalogArray, ConnectionId, I32Array, Property, SecretRef, SourceId,
    StreamSourceInfo, TableId, WatermarkDescArray,
};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "source")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub source_id: SourceId,
    pub name: String,
    pub row_id_index: Option<i32>,
    pub columns: ColumnCatalogArray,
    pub pk_column_ids: I32Array,
    pub with_properties: Property,
    pub definition: String,
    pub source_info: Option<StreamSourceInfo>,
    pub watermark_descs: WatermarkDescArray,
    pub optional_associated_table_id: Option<TableId>,
    pub connection_id: Option<ConnectionId>,
    pub version: i64,
    // `secret_ref` stores the mapping info mapping from property name to secret id and type.
    pub secret_ref: Option<SecretRef>,
    pub rate_limit: Option<i32>,
    pub cdc_etl_info: Option<CdcEtlSourceInfo>,
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
            columns: Set(ColumnCatalogArray::from(source.columns)),
            pk_column_ids: Set(I32Array(source.pk_column_ids)),
            with_properties: Set(Property(source.with_properties)),
            definition: Set(source.definition),
            source_info: Set(source.info.as_ref().map(StreamSourceInfo::from)),
            watermark_descs: Set(WatermarkDescArray::from(source.watermark_descs)),
            optional_associated_table_id: Set(optional_associated_table_id),
            connection_id: Set(source.connection_id.map(|id| id as _)),
            version: Set(source.version as _),
            secret_ref: Set(Some(SecretRef::from(source.secret_refs))),
            rate_limit: Set(source.rate_limit.map(|id| id as _)),
            cdc_etl_info: Set(source.cdc_etl_info.as_ref().map(CdcEtlSourceInfo::from)),
        }
    }
}

impl Model {
    pub fn is_shared(&self) -> bool {
        self.source_info
            .as_ref()
            .is_some_and(|s| s.to_protobuf().is_shared())
    }
}
