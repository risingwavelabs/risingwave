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

use std::collections::BTreeMap;

use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::hash::ActorAlignmentId;
use risingwave_meta_model::table::RefreshState;
use risingwave_pb::plan_common::ExprContext;

use super::*;
use crate::controller::fragment::InflightFragmentInfo;
use crate::controller::scale::resolve_streaming_job_definition;
use crate::controller::utils::{
    StreamingJobExtraInfo, get_database_resource_group, get_existing_job_resource_group,
    get_streaming_job_extra_info as fetch_streaming_job_extra_info, get_table_columns,
};
use crate::model::{StreamActor, StreamContext, StreamJobFragments, TableParallelism};

impl CatalogController {
    pub async fn get_secret_by_id(&self, secret_id: SecretId) -> MetaResult<PbSecret> {
        let inner = self.inner.read().await;
        let (secret, obj) = Secret::find_by_id(secret_id)
            .find_also_related(Object)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("secret", secret_id))?;
        Ok(ObjectModel(secret, obj.unwrap()).into())
    }

    pub async fn get_object_database_id(&self, object_id: ObjectId) -> MetaResult<DatabaseId> {
        let inner = self.inner.read().await;
        let (database_id,): (Option<DatabaseId>,) = Object::find_by_id(object_id)
            .select_only()
            .select_column(object::Column::DatabaseId)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("object", object_id))?;
        Ok(database_id.ok_or_else(|| anyhow!("object has no database id: {object_id}"))?)
    }

    pub async fn get_connection_by_id(
        &self,
        connection_id: ConnectionId,
    ) -> MetaResult<PbConnection> {
        let inner = self.inner.read().await;
        let (conn, obj) = Connection::find_by_id(connection_id)
            .find_also_related(Object)
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("connection", connection_id))?;

        Ok(ObjectModel(conn, obj.unwrap()).into())
    }

    pub async fn get_table_catalog_by_name(
        &self,
        database_id: DatabaseId,
        schema_id: SchemaId,
        name: &str,
    ) -> MetaResult<Option<PbTable>> {
        let inner = self.inner.read().await;
        let table_obj = Table::find()
            .find_also_related(Object)
            .filter(
                table::Column::Name
                    .eq(name)
                    .and(object::Column::DatabaseId.eq(database_id))
                    .and(object::Column::SchemaId.eq(schema_id)),
            )
            .one(&inner.db)
            .await?;
        Ok(table_obj.map(|(table, obj)| ObjectModel(table, obj.unwrap()).into()))
    }

    pub async fn get_table_by_name(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> MetaResult<Option<PbTable>> {
        let inner = self.inner.read().await;
        let table_obj = Table::find()
            .find_also_related(Object)
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .filter(
                table::Column::Name
                    .eq(table_name)
                    .and(database::Column::Name.eq(database_name)),
            )
            .one(&inner.db)
            .await?;
        Ok(table_obj.map(|(table, obj)| ObjectModel(table, obj.unwrap()).into()))
    }

    pub async fn get_table_associated_source_id(
        &self,
        table_id: TableId,
    ) -> MetaResult<Option<SourceId>> {
        let inner = self.inner.read().await;
        Table::find_by_id(table_id)
            .select_only()
            .select_column(table::Column::OptionalAssociatedSourceId)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("table", table_id))
    }

    pub async fn get_table_by_associate_source_id(
        &self,
        associated_source_id: SourceId,
    ) -> MetaResult<PbTable> {
        let inner = self.inner.read().await;
        Table::find()
            .find_also_related(Object)
            .filter(table::Column::OptionalAssociatedSourceId.eq(associated_source_id))
            .one(&inner.db)
            .await?
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into())
            .ok_or_else(|| {
                MetaError::catalog_id_not_found("table associated source", associated_source_id)
            })
    }

    pub async fn get_table_by_id(&self, table_id: TableId) -> MetaResult<PbTable> {
        let inner = self.inner.read().await;
        let table_obj = Table::find_by_id(table_id)
            .find_also_related(Object)
            .one(&inner.db)
            .await?;
        if let Some((table, obj)) = table_obj {
            Ok(ObjectModel(table, obj.unwrap()).into())
        } else {
            Err(MetaError::catalog_id_not_found("table", table_id))
        }
    }

    pub async fn get_user_created_table_by_ids(
        &self,
        table_ids: Vec<TableId>,
    ) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(
                table::Column::TableId
                    .is_in(table_ids.clone())
                    .and(table::Column::TableType.eq(TableType::Table)),
            )
            .all(&inner.db)
            .await?;
        let tables = table_objs
            .into_iter()
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into())
            .collect();
        Ok(tables)
    }

    pub async fn get_table_by_ids(
        &self,
        table_ids: Vec<TableId>,
        include_dropped_table: bool,
    ) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::TableId.is_in(table_ids.clone()))
            .all(&inner.db)
            .await?;
        let tables = table_objs
            .into_iter()
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into());
        let tables = if include_dropped_table {
            tables
                .chain(inner.dropped_tables.iter().filter_map(|(id, t)| {
                    if table_ids.contains(id) {
                        Some(t.clone())
                    } else {
                        None
                    }
                }))
                .collect()
        } else {
            tables.collect()
        };
        Ok(tables)
    }

    pub async fn get_table_columns(&self, id: TableId) -> MetaResult<Vec<ColumnCatalog>> {
        let inner = self.inner.read().await;
        Ok(get_table_columns(&inner.db, id)
            .await?
            .to_protobuf()
            .into_iter()
            .map(|col| col.into())
            .collect())
    }

    pub async fn get_table_incoming_sinks(&self, table_id: TableId) -> MetaResult<Vec<PbSink>> {
        let inner = self.inner.read().await;
        let sink_objs = Sink::find()
            .find_also_related(Object)
            .filter(sink::Column::TargetTable.eq(table_id))
            .all(&inner.db)
            .await?;
        Ok(sink_objs
            .into_iter()
            .map(|(sink, obj)| ObjectModel(sink, obj.unwrap()).into())
            .collect())
    }

    /// Get the refresh state of a table
    pub async fn get_table_refresh_state(
        &self,
        table_id: TableId,
    ) -> MetaResult<Option<RefreshState>> {
        let inner = self.inner.read().await;
        let (refresh_state,): (Option<RefreshState>,) = Table::find_by_id(table_id)
            .select_only()
            .select_column(table::Column::RefreshState)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("table", table_id))?;

        // Default to IDLE if not set (for backward compatibility)
        Ok(Some(refresh_state.unwrap_or(RefreshState::Idle)))
    }

    pub async fn get_sink_by_ids(&self, sink_ids: Vec<SinkId>) -> MetaResult<Vec<PbSink>> {
        let inner = self.inner.read().await;
        let sink_objs = Sink::find()
            .find_also_related(Object)
            .filter(sink::Column::SinkId.is_in(sink_ids))
            .all(&inner.db)
            .await?;
        Ok(sink_objs
            .into_iter()
            .map(|(sink, obj)| ObjectModel(sink, obj.unwrap()).into())
            .collect())
    }

    pub async fn get_sink_auto_refresh_schema_from(
        &self,
        table_id: TableId,
    ) -> MetaResult<Vec<PbSink>> {
        let inner = self.inner.read().await;
        let sink_objs = Sink::find()
            .find_also_related(Object)
            .filter(sink::Column::AutoRefreshSchemaFromTable.eq(table_id))
            .all(&inner.db)
            .await?;
        Ok(sink_objs
            .into_iter()
            .map(|(sink, obj)| ObjectModel(sink, obj.unwrap()).into())
            .collect())
    }

    pub async fn get_sink_state_table_ids(&self, sink_id: SinkId) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;
        let tables: Vec<I32Array> = Fragment::find()
            .select_only()
            .column(fragment::Column::StateTableIds)
            .filter(fragment::Column::JobId.eq(sink_id))
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(tables
            .into_iter()
            .flat_map(|ids| ids.into_inner().into_iter())
            .collect())
    }

    pub async fn get_subscription_by_id(
        &self,
        subscription_id: SubscriptionId,
    ) -> MetaResult<PbSubscription> {
        let inner = self.inner.read().await;
        let subscription_objs = Subscription::find()
            .find_also_related(Object)
            .filter(subscription::Column::SubscriptionId.eq(subscription_id))
            .all(&inner.db)
            .await?;
        let subscription: PbSubscription = subscription_objs
            .into_iter()
            .map(|(subscription, obj)| ObjectModel(subscription, obj.unwrap()).into())
            .find_or_first(|_| true)
            .ok_or_else(|| anyhow!("cannot find subscription with id {}", subscription_id))?;

        Ok(subscription)
    }

    pub async fn get_mv_depended_subscriptions(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<HashMap<DatabaseId, HashMap<TableId, HashMap<SubscriptionId, u64>>>> {
        let inner = self.inner.read().await;
        let select = Subscription::find()
            .select_only()
            .select_column(subscription::Column::SubscriptionId)
            .select_column(subscription::Column::DependentTableId)
            .select_column(subscription::Column::RetentionSeconds)
            .select_column(object::Column::DatabaseId)
            .join(JoinType::InnerJoin, subscription::Relation::Object.def());
        let select = if let Some(database_id) = database_id {
            select.filter(object::Column::DatabaseId.eq(database_id))
        } else {
            select
        };
        let subscription_objs: Vec<(SubscriptionId, ObjectId, i64, DatabaseId)> =
            select.into_tuple().all(&inner.db).await?;
        let mut map: HashMap<_, HashMap<_, HashMap<_, _>>> = HashMap::new();
        // Write object at the same time we write subscription, so we must be able to get obj
        for (subscription_id, dependent_table_id, retention_seconds, database_id) in
            subscription_objs
        {
            map.entry(database_id)
                .or_default()
                .entry(dependent_table_id)
                .or_default()
                .insert(subscription_id, retention_seconds as _);
        }
        Ok(map)
    }

    pub async fn get_all_table_options(&self) -> MetaResult<HashMap<TableId, TableOption>> {
        let inner = self.inner.read().await;
        let table_options: Vec<(TableId, Option<i32>)> = Table::find()
            .select_only()
            .columns([table::Column::TableId, table::Column::RetentionSeconds])
            .into_tuple::<(TableId, Option<i32>)>()
            .all(&inner.db)
            .await?;

        Ok(table_options
            .into_iter()
            .map(|(id, retention_seconds)| {
                (
                    id,
                    TableOption {
                        retention_seconds: retention_seconds.map(|i| i.try_into().unwrap()),
                    },
                )
            })
            .collect())
    }

    pub async fn get_all_streaming_parallelisms(
        &self,
    ) -> MetaResult<HashMap<ObjectId, StreamingParallelism>> {
        let inner = self.inner.read().await;

        let job_parallelisms = StreamingJob::find()
            .select_only()
            .columns([
                streaming_job::Column::JobId,
                streaming_job::Column::Parallelism,
            ])
            .into_tuple::<(ObjectId, StreamingParallelism)>()
            .all(&inner.db)
            .await?;

        Ok(job_parallelisms
            .into_iter()
            .collect::<HashMap<ObjectId, StreamingParallelism>>())
    }

    pub async fn get_table_name_type_mapping(
        &self,
    ) -> MetaResult<HashMap<TableId, (String, String)>> {
        let inner = self.inner.read().await;
        let table_name_types: Vec<(TableId, String, TableType)> = Table::find()
            .select_only()
            .columns([
                table::Column::TableId,
                table::Column::Name,
                table::Column::TableType,
            ])
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(table_name_types
            .into_iter()
            .map(|(id, name, table_type)| {
                (
                    id,
                    (name, PbTableType::from(table_type).as_str_name().to_owned()),
                )
            })
            .collect())
    }

    pub async fn get_table_by_cdc_table_id(
        &self,
        cdc_table_id: &String,
    ) -> MetaResult<Vec<PbTable>> {
        let inner = self.inner.read().await;
        let table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::CdcTableId.eq(cdc_table_id))
            .all(&inner.db)
            .await?;
        Ok(table_objs
            .into_iter()
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into())
            .collect())
    }

    pub async fn get_created_table_ids(&self) -> MetaResult<Vec<TableId>> {
        let inner = self.inner.read().await;

        // created table ids.
        let mut table_ids: Vec<TableId> = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .into_tuple()
            .all(&inner.db)
            .await?;

        // internal table ids.
        let internal_table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .filter(table::Column::BelongsToJobId.is_in(table_ids.clone()))
            .into_tuple()
            .all(&inner.db)
            .await?;
        table_ids.extend(internal_table_ids);

        Ok(table_ids)
    }

    /// Returns column ids of versioned tables.
    /// Being versioned implies using `ColumnAwareSerde`.
    pub async fn get_versioned_table_schemas(&self) -> MetaResult<HashMap<TableId, Vec<i32>>> {
        let res = self
            .list_all_state_tables()
            .await?
            .into_iter()
            .filter_map(|t| {
                if t.version.is_some() {
                    let ret = (
                        t.id.try_into().unwrap(),
                        t.columns
                            .iter()
                            .map(|c| c.column_desc.as_ref().unwrap().column_id)
                            .collect_vec(),
                    );
                    return Some(ret);
                }
                None
            })
            .collect();
        Ok(res)
    }

    pub async fn get_existing_job_resource_group(
        &self,
        streaming_job_id: ObjectId,
    ) -> MetaResult<String> {
        let inner = self.inner.read().await;
        get_existing_job_resource_group(&inner.db, streaming_job_id).await
    }

    pub async fn get_database_resource_group(&self, database_id: ObjectId) -> MetaResult<String> {
        let inner = self.inner.read().await;
        get_database_resource_group(&inner.db, database_id).await
    }

    pub async fn get_existing_job_resource_groups(
        &self,
        streaming_job_ids: Vec<ObjectId>,
    ) -> MetaResult<HashMap<ObjectId, String>> {
        let inner = self.inner.read().await;
        let mut resource_groups = HashMap::new();
        for job_id in streaming_job_ids {
            let resource_group = get_existing_job_resource_group(&inner.db, job_id).await?;
            resource_groups.insert(job_id, resource_group);
        }

        Ok(resource_groups)
    }

    pub async fn get_existing_job_database_resource_group(
        &self,
        streaming_job_id: ObjectId,
    ) -> MetaResult<String> {
        let inner = self.inner.read().await;
        let database_id: ObjectId = StreamingJob::find_by_id(streaming_job_id)
            .select_only()
            .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
            .column(object::Column::DatabaseId)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", streaming_job_id))?;

        get_database_resource_group(&inner.db, database_id).await
    }

    pub async fn get_job_streaming_parallelisms(
        &self,
        streaming_job_id: ObjectId,
    ) -> MetaResult<StreamingParallelism> {
        let inner = self.inner.read().await;

        let job_parallelism: StreamingParallelism = StreamingJob::find_by_id(streaming_job_id)
            .select_only()
            .column(streaming_job::Column::Parallelism)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", streaming_job_id))?;

        Ok(job_parallelism)
    }

    pub async fn get_fragment_streaming_job_id(
        &self,
        fragment_id: FragmentId,
    ) -> MetaResult<ObjectId> {
        let inner = self.inner.read().await;
        let job_id: ObjectId = Fragment::find_by_id(fragment_id)
            .select_only()
            .column(fragment::Column::JobId)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("fragment", fragment_id))?;
        Ok(job_id)
    }

    pub async fn list_streaming_job_with_database(
        &self,
    ) -> MetaResult<HashMap<DatabaseId, Vec<ObjectId>>> {
        let inner = self.inner.read().await;
        let database_objects: Vec<(DatabaseId, ObjectId)> = StreamingJob::find()
            .select_only()
            .column(object::Column::DatabaseId)
            .column(streaming_job::Column::JobId)
            .join(JoinType::LeftJoin, streaming_job::Relation::Object.def())
            .into_tuple()
            .all(&inner.db)
            .await?;

        Ok(database_objects.into_iter().into_group_map())
    }

    // Output: Vec<(table id, db name, schema name, table name, resource group)>
    pub async fn list_table_objects(
        &self,
    ) -> MetaResult<Vec<(TableId, String, String, String, String)>> {
        let inner = self.inner.read().await;
        Ok(Object::find()
            .select_only()
            .join(JoinType::InnerJoin, object::Relation::Table.def())
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .join(JoinType::InnerJoin, object::Relation::Schema2.def())
            .column(object::Column::Oid)
            .column(database::Column::Name)
            .column(schema::Column::Name)
            .column(table::Column::Name)
            .column(database::Column::ResourceGroup)
            .into_tuple()
            .all(&inner.db)
            .await?)
    }

    // Output: Vec<(source id, db name, schema name, source name, resource group)>
    pub async fn list_source_objects(
        &self,
    ) -> MetaResult<Vec<(TableId, String, String, String, String)>> {
        let inner = self.inner.read().await;
        Ok(Object::find()
            .select_only()
            .join(JoinType::InnerJoin, object::Relation::Source.def())
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .join(JoinType::InnerJoin, object::Relation::Schema2.def())
            .column(object::Column::Oid)
            .column(database::Column::Name)
            .column(schema::Column::Name)
            .column(source::Column::Name)
            .column(database::Column::ResourceGroup)
            .into_tuple()
            .all(&inner.db)
            .await?)
    }

    // Output: Vec<(sink id, db name, schema name, sink name, resource group)>
    pub async fn list_sink_objects(
        &self,
    ) -> MetaResult<Vec<(TableId, String, String, String, String)>> {
        let inner = self.inner.read().await;
        Ok(Object::find()
            .select_only()
            .join(JoinType::InnerJoin, object::Relation::Sink.def())
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .join(JoinType::InnerJoin, object::Relation::Schema2.def())
            .column(object::Column::Oid)
            .column(database::Column::Name)
            .column(schema::Column::Name)
            .column(sink::Column::Name)
            .column(database::Column::ResourceGroup)
            .into_tuple()
            .all(&inner.db)
            .await?)
    }

    pub async fn get_streaming_job_status(
        &self,
        streaming_job_id: ObjectId,
    ) -> MetaResult<JobStatus> {
        let inner = self.inner.read().await;
        let status = StreamingJob::find_by_id(streaming_job_id)
            .select_only()
            .column(streaming_job::Column::JobStatus)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", streaming_job_id))?;
        Ok(status)
    }

    pub async fn get_streaming_job_extra_info(
        &self,
        job_ids: Vec<ObjectId>,
    ) -> MetaResult<HashMap<ObjectId, StreamingJobExtraInfo>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let result = fetch_streaming_job_extra_info(&txn, job_ids).await?;
        Ok(result)
    }

    pub async fn restore_background_streaming_job(
        &self,
        job_infos: HashMap<ObjectId, HashMap<FragmentId, InflightFragmentInfo>>,
    ) -> MetaResult<HashMap<risingwave_common::catalog::TableId, (String, StreamJobFragments)>>
    {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let job_ids = job_infos.keys().cloned().collect::<HashSet<_>>();

        let streaming_jobs: Vec<_> = StreamingJob::find()
            .filter(streaming_job::Column::JobId.is_in(job_ids.clone()))
            .all(&txn)
            .await?;

        let definitions = resolve_streaming_job_definition(&txn, &job_ids).await?;

        let mut result = HashMap::new();

        for streaming_job::Model {
            job_id,
            job_status: _,
            create_type: _,
            timezone,
            parallelism,
            max_parallelism,
            specific_resource_group: _,
            ..
        } in streaming_jobs
        {
            let fragment_infos = job_infos.get(&job_id).unwrap();

            let definition = definitions.get(&job_id).cloned().unwrap_or_default();

            let mut fragments = BTreeMap::new();
            let mut actor_locations = BTreeMap::new();

            for (
                fragment_id,
                InflightFragmentInfo {
                    fragment_id: _,
                    distribution_type,
                    fragment_type_mask,
                    vnode_count,
                    nodes,
                    actors,
                    state_table_ids,
                },
            ) in fragment_infos
            {
                let stream_actors = actors
                    .iter()
                    .map(|(actor_id, actor_info)| StreamActor {
                        actor_id: *actor_id as _,
                        fragment_id: *fragment_id as _,
                        vnode_bitmap: actor_info.vnode_bitmap.clone(),
                        mview_definition: definition.clone(),
                        expr_context: Some(ExprContext::default()),
                    })
                    .collect_vec();

                for (idx, (actor_id, actor_info)) in actors
                    .iter()
                    .sorted_by(|(a, _), (b, _)| a.cmp(b))
                    .enumerate()
                {
                    actor_locations.insert(
                        *actor_id,
                        ActorAlignmentId::new(actor_info.worker_id as u32, idx),
                    );
                }

                let fragment = crate::model::Fragment {
                    fragment_id: *fragment_id as _,
                    fragment_type_mask: *fragment_type_mask,
                    distribution_type: (*distribution_type).into(),
                    actors: stream_actors,
                    state_table_ids: state_table_ids.iter().map(|id| id.table_id).collect(),
                    maybe_vnode_count: Some(*vnode_count as _),
                    nodes: nodes.clone(),
                };

                fragments.insert(*fragment_id as u32, fragment);
            }

            let ctx = StreamContext {
                timezone: timezone.clone(),
            };

            let table_parallelism: TableParallelism = parallelism.into();

            let job_id = risingwave_common::catalog::TableId::new(job_id as _);
            let stream_job_fragments = StreamJobFragments::new(
                job_id,
                fragments,
                &actor_locations,
                ctx,
                table_parallelism,
                max_parallelism as _,
            );

            result.insert(job_id, (definition, stream_job_fragments));
        }

        Ok(result)
    }
}
