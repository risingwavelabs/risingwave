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

#![expect(dead_code, reason = "WIP")]

use anyhow::anyhow;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model_v2::{
    connection, database, index, object, schema, sink, source, table, view,
};
use risingwave_pb::catalog::connection::PbInfo as PbConnectionInfo;
use risingwave_pb::catalog::source::PbOptionalAssociatedTableId;
use risingwave_pb::catalog::table::{PbOptionalAssociatedSourceId, PbTableType};
use risingwave_pb::catalog::{
    PbConnection, PbCreateType, PbDatabase, PbHandleConflictBehavior, PbIndex, PbSchema, PbSink,
    PbSinkType, PbSource, PbStreamJobStatus, PbTable, PbView,
};
use sea_orm::{DatabaseConnection, ModelTrait};

use crate::MetaError;

pub mod catalog;
pub mod cluster;
pub mod fragment;
pub mod rename;
pub mod system_param;
pub mod user;
pub mod utils;

// todo: refine the error transform.
impl From<sea_orm::DbErr> for MetaError {
    fn from(err: sea_orm::DbErr) -> Self {
        if let Some(err) = err.sql_err() {
            return anyhow!(err).into();
        }
        anyhow!(err).into()
    }
}

#[derive(Clone)]
pub struct SqlMetaStore {
    pub conn: DatabaseConnection,
}

impl SqlMetaStore {
    pub fn new(conn: DatabaseConnection) -> Self {
        Self { conn }
    }

    #[cfg(any(test, feature = "test"))]
    #[cfg(not(madsim))]
    pub async fn for_test() -> Self {
        use risingwave_meta_model_migration::{Migrator, MigratorTrait};
        let conn = sea_orm::Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&conn, None).await.unwrap();
        Self { conn }
    }
}

pub struct ObjectModel<M: ModelTrait>(M, object::Model);

impl From<ObjectModel<database::Model>> for PbDatabase {
    fn from(value: ObjectModel<database::Model>) -> Self {
        Self {
            id: value.0.database_id as _,
            name: value.0.name,
            owner: value.1.owner_id as _,
        }
    }
}

impl From<ObjectModel<schema::Model>> for PbSchema {
    fn from(value: ObjectModel<schema::Model>) -> Self {
        Self {
            id: value.0.schema_id as _,
            name: value.0.name,
            database_id: value.1.database_id.unwrap() as _,
            owner: value.1.owner_id as _,
        }
    }
}

impl From<ObjectModel<table::Model>> for PbTable {
    fn from(value: ObjectModel<table::Model>) -> Self {
        Self {
            id: value.0.table_id as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            columns: value.0.columns.0,
            pk: value.0.pk.0,
            dependent_relations: vec![], // todo: deprecate it.
            table_type: PbTableType::from(value.0.table_type) as _,
            distribution_key: value.0.distribution_key.0,
            stream_key: value.0.stream_key.0,
            append_only: value.0.append_only,
            owner: value.1.owner_id as _,
            properties: value.0.properties.0,
            fragment_id: value.0.fragment_id as u32,
            vnode_col_index: value.0.vnode_col_index.map(|index| index as _),
            row_id_index: value.0.row_id_index.map(|index| index as _),
            value_indices: value.0.value_indices.0,
            definition: value.0.definition,
            handle_pk_conflict_behavior: PbHandleConflictBehavior::from(
                value.0.handle_pk_conflict_behavior,
            ) as _,
            read_prefix_len_hint: value.0.read_prefix_len_hint as _,
            watermark_indices: value.0.watermark_indices.0,
            dist_key_in_pk: value.0.dist_key_in_pk.0,
            dml_fragment_id: value.0.dml_fragment_id.map(|id| id as u32),
            cardinality: value.0.cardinality.map(|cardinality| cardinality.0),
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(value.1.initialized_at.timestamp_millis() as _).0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(value.1.created_at.timestamp_millis() as _).0,
            ),
            cleaned_by_watermark: value.0.cleaned_by_watermark,
            stream_job_status: PbStreamJobStatus::from(value.0.job_status) as _,
            create_type: PbCreateType::from(value.0.create_type) as _,
            version: Some(value.0.version.0),
            optional_associated_source_id: value
                .0
                .optional_associated_source_id
                .map(|id| PbOptionalAssociatedSourceId::AssociatedSourceId(id as _)),
            description: None,
        }
    }
}

impl From<ObjectModel<source::Model>> for PbSource {
    fn from(value: ObjectModel<source::Model>) -> Self {
        Self {
            id: value.0.source_id as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            row_id_index: value.0.row_id_index.map(|id| id as _),
            columns: value.0.columns.0,
            pk_column_ids: value.0.pk_column_ids.0,
            properties: value.0.properties.0,
            owner: value.1.owner_id as _,
            info: value.0.source_info.map(|info| info.0),
            watermark_descs: value.0.watermark_descs.0,
            definition: value.0.definition,
            connection_id: value.0.connection_id.map(|id| id as _),
            // todo: using the timestamp from the database directly.
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(value.1.initialized_at.timestamp_millis() as _).0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(value.1.created_at.timestamp_millis() as _).0,
            ),
            version: value.0.version as _,
            optional_associated_table_id: value
                .0
                .optional_associated_table_id
                .map(|id| PbOptionalAssociatedTableId::AssociatedTableId(id as _)),
        }
    }
}

impl From<ObjectModel<sink::Model>> for PbSink {
    fn from(value: ObjectModel<sink::Model>) -> Self {
        Self {
            id: value.0.sink_id as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            columns: value.0.columns.0,
            plan_pk: value.0.plan_pk.0,
            dependent_relations: vec![], // todo: deprecate it.
            distribution_key: value.0.distribution_key.0,
            downstream_pk: value.0.downstream_pk.0,
            sink_type: PbSinkType::from(value.0.sink_type) as _,
            owner: value.1.owner_id as _,
            properties: value.0.properties.0,
            definition: value.0.definition,
            connection_id: value.0.connection_id.map(|id| id as _),
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(value.1.initialized_at.timestamp_millis() as _).0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(value.1.created_at.timestamp_millis() as _).0,
            ),
            db_name: value.0.db_name,
            sink_from_name: value.0.sink_from_name,
            stream_job_status: PbStreamJobStatus::from(value.0.job_status) as _,
            format_desc: value.0.sink_format_desc.map(|desc| desc.0),
            sink_into_name: None,
        }
    }
}

impl From<ObjectModel<index::Model>> for PbIndex {
    fn from(value: ObjectModel<index::Model>) -> Self {
        Self {
            id: value.0.index_id as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            owner: value.1.owner_id as _,
            index_table_id: value.0.index_table_id as _,
            primary_table_id: value.0.primary_table_id as _,
            index_item: value.0.index_items.0,
            original_columns: value.0.original_columns.0,
            initialized_at_epoch: Some(
                Epoch::from_unix_millis(value.1.initialized_at.timestamp_millis() as _).0,
            ),
            created_at_epoch: Some(
                Epoch::from_unix_millis(value.1.created_at.timestamp_millis() as _).0,
            ),
            stream_job_status: PbStreamJobStatus::from(value.0.job_status) as _,
        }
    }
}

impl From<ObjectModel<view::Model>> for PbView {
    fn from(value: ObjectModel<view::Model>) -> Self {
        Self {
            id: value.0.view_id as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            owner: value.1.owner_id as _,
            properties: value.0.properties.0,
            sql: value.0.definition,
            dependent_relations: vec![], // todo: deprecate it.
            columns: value.0.columns.0,
        }
    }
}

impl From<ObjectModel<connection::Model>> for PbConnection {
    fn from(value: ObjectModel<connection::Model>) -> Self {
        Self {
            id: value.1.oid as _,
            schema_id: value.1.schema_id.unwrap() as _,
            database_id: value.1.database_id.unwrap() as _,
            name: value.0.name,
            owner: value.1.owner_id as _,
            info: Some(PbConnectionInfo::PrivateLinkService(value.0.info.0)),
        }
    }
}
