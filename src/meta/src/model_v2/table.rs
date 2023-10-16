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

use crate::model_v2::{I32Array, Property, SourceId, TableId};

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

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "table")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub table_id: TableId,
    pub name: String,
    pub optional_associated_source_id: Option<SourceId>,
    pub table_type: TableType,
    pub columns: Json,
    pub pk: Json,
    pub distribution_key: I32Array,
    pub append_only: bool,
    pub properties: Property,
    pub fragment_id: i32,
    pub vnode_col_index: I32Array,
    pub value_indices: I32Array,
    pub definition: String,
    pub handle_pk_conflict_behavior: i32,
    pub read_prefix_len_hint: i32,
    pub watermark_indices: I32Array,
    pub dist_key_in_pk: I32Array,
    pub dml_fragment_id: Option<i32>,
    pub cardinality: Option<I32Array>,
    pub cleaned_by_watermark: bool,
    pub version: Json,
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
