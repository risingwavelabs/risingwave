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

use risingwave_pb::catalog::PbSinkType;
use sea_orm::entity::prelude::*;

use crate::{
    ColumnCatalogArray, ColumnOrderArray, ConnectionId, I32Array, JobStatus, Property,
    SinkFormatDesc, SinkId,
};

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

impl From<SinkType> for PbSinkType {
    fn from(sink_type: SinkType) -> Self {
        match sink_type {
            SinkType::AppendOnly => Self::AppendOnly,
            SinkType::ForceAppendOnly => Self::ForceAppendOnly,
            SinkType::Upsert => Self::Upsert,
        }
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
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
    pub job_status: JobStatus,
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
