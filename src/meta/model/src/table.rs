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

use risingwave_common::catalog::OBJECT_ID_PLACEHOLDER;
use risingwave_common::hash::VnodeCountCompat;
use risingwave_pb::catalog::table::{OptionalAssociatedSourceId, PbEngine, PbTableType};
use risingwave_pb::catalog::{PbHandleConflictBehavior, PbTable};
use sea_orm::ActiveValue::Set;
use sea_orm::NotSet;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{
    Cardinality, ColumnCatalogArray, ColumnOrderArray, FragmentId, I32Array, ObjectId, SourceId,
    TableId, TableVersion, WebhookSourceInfo,
};

#[derive(
    Clone, Debug, PartialEq, Hash, Copy, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize,
)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
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

impl From<PbTableType> for TableType {
    fn from(table_type: PbTableType) -> Self {
        match table_type {
            PbTableType::Table => Self::Table,
            PbTableType::MaterializedView => Self::MaterializedView,
            PbTableType::Index => Self::Index,
            PbTableType::Internal => Self::Internal,
            PbTableType::Unspecified => unreachable!("Unspecified table type"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum HandleConflictBehavior {
    #[sea_orm(string_value = "OVERWRITE")]
    Overwrite,
    #[sea_orm(string_value = "IGNORE")]
    Ignore,
    #[sea_orm(string_value = "NO_CHECK")]
    NoCheck,
    #[sea_orm(string_value = "DO_UPDATE_IF_NOT_NULL")]
    DoUpdateIfNotNull,
}

impl From<HandleConflictBehavior> for PbHandleConflictBehavior {
    fn from(handle_conflict_behavior: HandleConflictBehavior) -> Self {
        match handle_conflict_behavior {
            HandleConflictBehavior::Overwrite => Self::Overwrite,
            HandleConflictBehavior::Ignore => Self::Ignore,
            HandleConflictBehavior::NoCheck => Self::NoCheck,
            HandleConflictBehavior::DoUpdateIfNotNull => Self::DoUpdateIfNotNull,
        }
    }
}

impl From<PbHandleConflictBehavior> for HandleConflictBehavior {
    fn from(handle_conflict_behavior: PbHandleConflictBehavior) -> Self {
        match handle_conflict_behavior {
            PbHandleConflictBehavior::Overwrite => Self::Overwrite,
            PbHandleConflictBehavior::Ignore => Self::Ignore,
            PbHandleConflictBehavior::NoCheck => Self::NoCheck,
            PbHandleConflictBehavior::DoUpdateIfNotNull => Self::DoUpdateIfNotNull,
            PbHandleConflictBehavior::Unspecified => {
                unreachable!("Unspecified handle conflict behavior")
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, EnumIter, DeriveActiveEnum, Serialize, Deserialize)]
#[sea_orm(rs_type = "String", db_type = "string(None)")]
pub enum Engine {
    #[sea_orm(string_value = "HUMMOCK")]
    Hummock,
    #[sea_orm(string_value = "ICEBERG")]
    Iceberg,
}

impl From<Engine> for PbEngine {
    fn from(engine: Engine) -> Self {
        match engine {
            Engine::Hummock => Self::Hummock,
            Engine::Iceberg => Self::Iceberg,
        }
    }
}

impl From<PbEngine> for Engine {
    fn from(engine: PbEngine) -> Self {
        match engine {
            PbEngine::Hummock => Self::Hummock,
            PbEngine::Iceberg => Self::Iceberg,
            PbEngine::Unspecified => Self::Hummock,
        }
    }
}

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq, Serialize, Deserialize)]
#[sea_orm(table_name = "table")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub table_id: TableId,
    pub name: String,
    pub optional_associated_source_id: Option<SourceId>,
    pub table_type: TableType,
    pub belongs_to_job_id: Option<ObjectId>,
    pub columns: ColumnCatalogArray,
    pub pk: ColumnOrderArray,
    pub distribution_key: I32Array,
    pub stream_key: I32Array,
    pub append_only: bool,
    pub fragment_id: Option<FragmentId>,
    pub vnode_col_index: Option<i32>,
    pub row_id_index: Option<i32>,
    pub value_indices: I32Array,
    pub definition: String,
    pub handle_pk_conflict_behavior: HandleConflictBehavior,
    pub version_column_index: Option<i32>,
    pub read_prefix_len_hint: i32,
    pub watermark_indices: I32Array,
    pub dist_key_in_pk: I32Array,
    pub dml_fragment_id: Option<FragmentId>,
    pub cardinality: Option<Cardinality>,
    pub cleaned_by_watermark: bool,
    pub description: Option<String>,
    pub version: Option<TableVersion>,
    pub retention_seconds: Option<i32>,
    pub incoming_sinks: I32Array,
    pub cdc_table_id: Option<String>,
    pub vnode_count: i32,
    pub webhook_info: Option<WebhookSourceInfo>,
    pub engine: Option<Engine>,
    pub clean_watermark_index_in_pk: Option<i32>,
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
        from = "Column::BelongsToJobId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object2,
    #[sea_orm(
        belongs_to = "super::object::Entity",
        from = "Column::TableId",
        to = "super::object::Column::Oid",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    Object1,
    #[sea_orm(
        belongs_to = "super::source::Entity",
        from = "Column::OptionalAssociatedSourceId",
        to = "super::source::Column::SourceId",
        on_update = "NoAction",
        on_delete = "NoAction"
    )]
    Source,

    // To join object_dependency on the used_by column
    #[sea_orm(
        belongs_to = "super::object_dependency::Entity",
        from = "Column::TableId",
        to = "super::object_dependency::Column::UsedBy",
        on_update = "NoAction",
        on_delete = "Cascade"
    )]
    ObjectDependency,
}

impl Related<super::object::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Object1.def()
    }
}

impl Related<super::source::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Source.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl From<PbTable> for ActiveModel {
    fn from(pb_table: PbTable) -> Self {
        let table_type = pb_table.table_type();
        let handle_pk_conflict_behavior = pb_table.handle_pk_conflict_behavior();

        // `PbTable` here should be sourced from the wire, not from persistence.
        // A placeholder `maybe_vnode_count` field should be treated as `NotSet`, instead of calling
        // the compatibility code.
        let vnode_count = pb_table
            .vnode_count_inner()
            .value_opt()
            .map(|v| v as _)
            .map_or(NotSet, Set);
        let fragment_id = if pb_table.fragment_id == OBJECT_ID_PLACEHOLDER {
            NotSet
        } else {
            Set(Some(pb_table.fragment_id as FragmentId))
        };
        let dml_fragment_id = pb_table
            .dml_fragment_id
            .map(|x| Set(Some(x as FragmentId)))
            .unwrap_or_default();
        let optional_associated_source_id =
            if let Some(OptionalAssociatedSourceId::AssociatedSourceId(src_id)) =
                pb_table.optional_associated_source_id
            {
                Set(Some(src_id as SourceId))
            } else {
                NotSet
            };

        Self {
            table_id: Set(pb_table.id as _),
            name: Set(pb_table.name),
            optional_associated_source_id,
            table_type: Set(table_type.into()),
            belongs_to_job_id: Set(pb_table.job_id.map(|x| x as _)),
            columns: Set(pb_table.columns.into()),
            pk: Set(pb_table.pk.into()),
            distribution_key: Set(pb_table.distribution_key.into()),
            stream_key: Set(pb_table.stream_key.into()),
            append_only: Set(pb_table.append_only),
            fragment_id,
            vnode_col_index: Set(pb_table.vnode_col_index.map(|x| x as i32)),
            row_id_index: Set(pb_table.row_id_index.map(|x| x as i32)),
            value_indices: Set(pb_table.value_indices.into()),
            definition: Set(pb_table.definition),
            handle_pk_conflict_behavior: Set(handle_pk_conflict_behavior.into()),
            version_column_index: Set(pb_table.version_column_index.map(|x| x as i32)),
            read_prefix_len_hint: Set(pb_table.read_prefix_len_hint as _),
            watermark_indices: Set(pb_table.watermark_indices.into()),
            dist_key_in_pk: Set(pb_table.dist_key_in_pk.into()),
            dml_fragment_id,
            cardinality: Set(pb_table.cardinality.as_ref().map(|x| x.into())),
            cleaned_by_watermark: Set(pb_table.cleaned_by_watermark),
            description: Set(pb_table.description),
            version: Set(pb_table.version.as_ref().map(|v| v.into())),
            retention_seconds: Set(pb_table.retention_seconds.map(|i| i as _)),
            incoming_sinks: Set(pb_table.incoming_sinks.into()),
            cdc_table_id: Set(pb_table.cdc_table_id),
            vnode_count,
            webhook_info: Set(pb_table.webhook_info.as_ref().map(WebhookSourceInfo::from)),
            engine: Set(pb_table
                .engine
                .map(|engine| Engine::from(PbEngine::try_from(engine).expect("Invalid engine")))),
            clean_watermark_index_in_pk: Set(pb_table.clean_watermark_index_in_pk),
        }
    }
}
