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

use risingwave_pb::catalog::table::PbTableType;
use risingwave_pb::catalog::PbHandleConflictBehavior;
use sea_orm::entity::prelude::*;

use crate::{
    Cardinality, ColumnCatalogArray, ColumnOrderArray, CreateType, FragmentId, I32Array, JobStatus,
    Property, SourceId, TableId, TableVersion,
};

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(None)")]
pub enum TableType {
    #[sea_orm(string_value = "TABLE")]
    Table,
    #[sea_orm(string_value = "MATERIALIZED_VIEW")]
    MaterializedView,
    #[sea_orm(string_value = "INDEX")]
    Index,
    #[sea_orm(string_value = "INTERNAL")]
    Internal,
}

impl From<TableType> for PbTableType {
    fn from(table_type: TableType) -> Self {
        match table_type {
            TableType::Table => Self::Table,
            TableType::MaterializedView => Self::MaterializedView,
            TableType::Index => Self::Index,
            TableType::Internal => Self::Internal,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum)]
#[sea_orm(rs_type = "String", db_type = "String(None)")]
pub enum HandleConflictBehavior {
    #[sea_orm(string_value = "OVERWRITE")]
    Overwrite,
    #[sea_orm(string_value = "IGNORE")]
    Ignore,
    #[sea_orm(string_value = "NO_CHECK")]
    NoCheck,
}

impl From<HandleConflictBehavior> for PbHandleConflictBehavior {
    fn from(handle_conflict_behavior: HandleConflictBehavior) -> Self {
        match handle_conflict_behavior {
            HandleConflictBehavior::Overwrite => Self::Overwrite,
            HandleConflictBehavior::Ignore => Self::Ignore,
            HandleConflictBehavior::NoCheck => Self::NoCheck,
        }
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "table")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub table_id: TableId,
    pub name: String,
    pub optional_associated_source_id: Option<SourceId>,
    pub table_type: TableType,
    pub columns: ColumnCatalogArray,
    pub pk: ColumnOrderArray,
    pub distribution_key: I32Array,
    pub stream_key: I32Array,
    pub append_only: bool,
    pub properties: Property,
    pub fragment_id: FragmentId,
    pub vnode_col_index: Option<i32>,
    pub row_id_index: Option<i32>,
    pub value_indices: I32Array,
    pub definition: String,
    pub handle_pk_conflict_behavior: HandleConflictBehavior,
    pub read_prefix_len_hint: i32,
    pub watermark_indices: I32Array,
    pub dist_key_in_pk: I32Array,
    pub dml_fragment_id: Option<FragmentId>,
    pub cardinality: Option<Cardinality>,
    pub cleaned_by_watermark: bool,
    pub job_status: JobStatus,
    pub create_type: CreateType,
    pub description: Option<String>,
    pub version: TableVersion,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::fragment::Entity",
        from = "Column::DmlFragmentId",
        to = "super::fragment::Column::FragmentId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Fragment2,
    #[sea_orm(
        belongs_to = "super::fragment::Entity",
        from = "Column::FragmentId",
        to = "super::fragment::Column::FragmentId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Fragment1,
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::TableId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object,
    #[sea_orm(
        belongs_to = "super::source::Entity",
        from = "Column::OptionalAssociatedSourceId",
        to = "super::source::Column::SourceId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Source,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object.def()
    }
}

impl Related<super::source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Source.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
