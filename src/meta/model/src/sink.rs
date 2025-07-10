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

use risingwave_pb::catalog::{PbSink, PbSinkType};
use sea_orm::ActiveValue::Set;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    ColumnCatalogArray, ColumnOrderArray, ConnectionId, I32Array, Property, SecretRef,
    SinkFormatDesc, SinkId, TableId,
};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum SinkType {
    #[sea_orm(string_value = "APPEND_ONLY")]
    AppendOnly,
    #[sea_orm(string_value = "FORCE_APPEND_ONLY")]
    ForceAppendOnly,
    #[sea_orm(string_value = "UPSERT")]
    Upsert,
}

impl From<SinkType> for PbSinkType {
    fn from(sink_type: SinkType) -> Self {
        match sink_type {
            SinkType::AppendOnly => Self::AppendOnly,
            SinkType::ForceAppendOnly => Self::ForceAppendOnly,
            SinkType::Upsert => Self::Upsert,
        }
    }
}

impl From<PbSinkType> for SinkType {
    fn from(sink_type: PbSinkType) -> Self {
        match sink_type {
            PbSinkType::AppendOnly => Self::AppendOnly,
            PbSinkType::ForceAppendOnly => Self::ForceAppendOnly,
            PbSinkType::Upsert => Self::Upsert,
            PbSinkType::Unspecified => unreachable!("Unspecified sink type"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "sink")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub sink_id: SinkId,
    pub name: String,
    pub columns: ColumnCatalogArray,
    pub plan_pk: ColumnOrderArray,
    pub distribution_key: I32Array,
    pub downstream_pk: I32Array,
    pub sink_type: SinkType,
    pub properties: Property,
    pub definition: String,
    pub connection_id: Option<ConnectionId>,
    pub db_name: String,
    pub sink_from_name: String,
    pub sink_format_desc: Option<SinkFormatDesc>,
    pub target_table: Option<TableId>,
    // `secret_ref` stores the mapping info mapping from property name to secret id and type.
    pub secret_ref: Option<SecretRef>,
    pub original_target_columns: Option<ColumnCatalogArray>,
    pub auto_refresh_schema_from_table: Option<TableId>,
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
        from = "Column::SinkId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
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

impl ActiveModelBehavior for ActiveModel {}

impl From<PbSink> for ActiveModel {
    fn from(pb_sink: PbSink) -> Self {
        let sink_type = pb_sink.sink_type();

        Self {
            sink_id: Set(pb_sink.id as _),
            name: Set(pb_sink.name),
            columns: Set(pb_sink.columns.into()),
            plan_pk: Set(pb_sink.plan_pk.into()),
            distribution_key: Set(pb_sink.distribution_key.into()),
            downstream_pk: Set(pb_sink.downstream_pk.into()),
            sink_type: Set(sink_type.into()),
            properties: Set(pb_sink.properties.into()),
            definition: Set(pb_sink.definition),
            connection_id: Set(pb_sink.connection_id.map(|x| x as _)),
            db_name: Set(pb_sink.db_name),
            sink_from_name: Set(pb_sink.sink_from_name),
            sink_format_desc: Set(pb_sink.format_desc.as_ref().map(|x| x.into())),
            target_table: Set(pb_sink.target_table.map(|x| x as _)),
            secret_ref: Set(Some(SecretRef::from(pb_sink.secret_refs))),
            original_target_columns: Set(Some(pb_sink.original_target_columns.into())),
            auto_refresh_schema_from_table: Set(pb_sink
                .auto_refresh_schema_from_table
                .map(|id| id as _)),
        }
    }
}
