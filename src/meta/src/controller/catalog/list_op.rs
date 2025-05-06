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

use super::*;

impl CatalogController {
    pub async fn list_time_travel_table_ids(&self) -> MetaResult<Vec<TableId>> {
        self.inner.read().await.list_time_travel_table_ids().await
    }

    pub async fn list_stream_job_desc_for_telemetry(
        &self,
    ) -> MetaResult<Vec<MetaTelemetryJobDesc>> {
        let inner = self.inner.read().await;
        let info: Vec<(TableId, Option<Property>)> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .column(source::Column::WithProperties)
            .join(JoinType::LeftJoin, table::Relation::Source.def())
            .filter(
                table::Column::TableType
                    .eq(TableType::Table)
                    .or(table::Column::TableType.eq(TableType::MaterializedView)),
            )
            .into_tuple()
            .all(&inner.db)
            .await?;

        Ok(info
            .into_iter()
            .map(|(table_id, properties)| {
                let connector_info = if let Some(inner_props) = properties {
                    inner_props
                        .inner_ref()
                        .get(UPSTREAM_SOURCE_KEY)
                        .map(|v| v.to_lowercase())
                } else {
                    None
                };
                MetaTelemetryJobDesc {
                    table_id,
                    connector: connector_info,
                    optimization: vec![],
                }
            })
            .collect())
    }

    pub async fn list_background_creating_mviews(
        &self,
        include_initial: bool,
    ) -> MetaResult<Vec<table::Model>> {
        let inner = self.inner.read().await;
        let status_cond = if include_initial {
            streaming_job::Column::JobStatus.is_in([JobStatus::Initial, JobStatus::Creating])
        } else {
            streaming_job::Column::JobStatus.eq(JobStatus::Creating)
        };
        let tables = Table::find()
            .join(JoinType::LeftJoin, table::Relation::Object1.def())
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(
                table::Column::TableType
                    .eq(TableType::MaterializedView)
                    .and(
                        streaming_job::Column::CreateType
                            .eq(CreateType::Background)
                            .and(status_cond),
                    ),
            )
            .all(&inner.db)
            .await?;
        Ok(tables)
    }

    pub async fn list_databases(&self) -> MetaResult<Vec<PbDatabase>> {
        let inner = self.inner.read().await;
        inner.list_databases().await
    }

    pub async fn list_all_object_dependencies(&self) -> MetaResult<Vec<PbObjectDependencies>> {
        self.list_object_dependencies(true).await
    }

    pub async fn list_created_object_dependencies(&self) -> MetaResult<Vec<PbObjectDependencies>> {
        self.list_object_dependencies(false).await
    }

    pub async fn list_schemas(&self) -> MetaResult<Vec<PbSchema>> {
        let inner = self.inner.read().await;
        inner.list_schemas().await
    }

    pub async fn list_all_state_tables(&self) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        inner.list_all_state_tables().await
    }

    pub async fn list_readonly_table_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;
        let table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .filter(
                object::Column::SchemaId
                    .eq(schema_id)
                    .and(table::Column::TableType.ne(TableType::Table)),
            )
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(table_ids)
    }

    pub async fn list_dml_table_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;
        let table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .filter(
                object::Column::SchemaId
                    .eq(schema_id)
                    .and(table::Column::TableType.eq(TableType::Table)),
            )
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(table_ids)
    }

    pub async fn list_view_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<ViewId>> {
        let inner = self.inner.read().await;
        let view_ids: Vec<ViewId> = View::find()
            .select_only()
            .column(view::Column::ViewId)
            .join(JoinType::InnerJoin, view::Relation::Object.def())
            .filter(object::Column::SchemaId.eq(schema_id))
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(view_ids)
    }

    pub async fn list_tables_by_type(&self, table_type: TableType) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::TableType.eq(table_type))
            .all(&inner.db)
            .await?;
        Ok(table_objs
            .into_iter()
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into())
            .collect())
    }

    pub async fn list_sources(&self) -> MetaResult<Vec<PbSource>> {
        let inner = self.inner.read().await;
        inner.list_sources().await
    }

    // Return a hashmap to distinguish whether each source is shared or not.
    pub async fn list_source_id_with_shared_types(&self) -> MetaResult<HashMap<SourceId, bool>> {
        let inner = self.inner.read().await;
        let source_ids: Vec<(SourceId, Option<StreamSourceInfo>)> = Source::find()
            .select_only()
            .columns([source::Column::SourceId, source::Column::SourceInfo])
            .into_tuple()
            .all(&inner.db)
            .await?;

        Ok(source_ids
            .into_iter()
            .map(|(source_id, info)| {
                (
                    source_id,
                    info.map(|info| info.to_protobuf().cdc_source_job)
                        .unwrap_or(false),
                )
            })
            .collect())
    }

    pub async fn list_connections(&self) -> MetaResult<Vec<PbConnection>> {
        let inner = self.inner.read().await;
        let conn_objs = Connection::find()
            .find_also_related(Object)
            .all(&inner.db)
            .await?;
        Ok(conn_objs
            .into_iter()
            .map(|(conn, obj)| ObjectModel(conn, obj.unwrap()).into())
            .collect())
    }

    pub async fn list_source_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<SourceId>> {
        let inner = self.inner.read().await;
        let source_ids: Vec<SourceId> = Source::find()
            .select_only()
            .column(source::Column::SourceId)
            .join(JoinType::InnerJoin, source::Relation::Object.def())
            .filter(object::Column::SchemaId.eq(schema_id))
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(source_ids)
    }

    pub async fn list_indexes(&self) -> MetaResult<Vec<PbIndex>> {
        let inner = self.inner.read().await;
        inner.list_indexes().await
    }

    pub async fn list_sinks(&self) -> MetaResult<Vec<PbSink>> {
        let inner = self.inner.read().await;
        inner.list_sinks().await
    }

    pub async fn list_subscriptions(&self) -> MetaResult<Vec<PbSubscription>> {
        let inner = self.inner.read().await;
        inner.list_subscriptions().await
    }

    pub async fn list_views(&self) -> MetaResult<Vec<PbView>> {
        let inner = self.inner.read().await;
        inner.list_views().await
    }

    pub async fn list_users(&self) -> MetaResult<Vec<PbUserInfo>> {
        let inner = self.inner.read().await;
        inner.list_users().await
    }
}
