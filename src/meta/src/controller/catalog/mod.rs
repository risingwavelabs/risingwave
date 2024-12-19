// Copyright 2024 RisingWave Labs
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

mod alter_op;
mod create_op;
mod get_op;
mod list_op;
mod test;
mod util;

use std::collections::{BTreeSet, HashMap, HashSet};
use std::iter;
use std::mem::take;
use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::{TableOption, DEFAULT_SCHEMA_NAME, SYSTEM_SCHEMAS};
use risingwave_common::secret::LocalSecretManager;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont_mut;
use risingwave_common::{bail, current_cluster_version};
use risingwave_connector::source::cdc::build_cdc_table_id;
use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::*;
use risingwave_meta_model::table::TableType;
use risingwave_meta_model::{
    actor, connection, database, fragment, function, index, object, object_dependency, schema,
    secret, sink, source, streaming_job, subscription, table, user_privilege, view, ActorId,
    ActorUpstreamActors, ColumnCatalogArray, ConnectionId, CreateType, DatabaseId, FragmentId,
    FunctionId, I32Array, IndexId, JobStatus, ObjectId, Property, SchemaId, SecretId, SinkId,
    SourceId, StreamNode, StreamSourceInfo, StreamingParallelism, SubscriptionId, TableId, UserId,
    ViewId,
};
use risingwave_pb::catalog::connection::Info as ConnectionInfo;
use risingwave_pb::catalog::subscription::SubscriptionState;
use risingwave_pb::catalog::table::PbTableType;
use risingwave_pb::catalog::{
    PbComment, PbConnection, PbDatabase, PbFunction, PbIndex, PbSchema, PbSecret, PbSink, PbSource,
    PbStreamJobStatus, PbSubscription, PbTable, PbView,
};
use risingwave_pb::meta::cancel_creating_jobs_request::PbCreatingJobInfo;
use risingwave_pb::meta::list_object_dependencies_response::PbObjectDependencies;
use risingwave_pb::meta::relation::PbRelationInfo;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Info, Operation as NotificationOperation, Operation,
};
use risingwave_pb::meta::{PbFragmentWorkerSlotMapping, PbRelation, PbRelationGroup};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::FragmentTypeFlag;
use risingwave_pb::telemetry::PbTelemetryEventStage;
use risingwave_pb::user::PbUserInfo;
use sea_orm::sea_query::{Expr, Query, SimpleExpr};
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, JoinType, PaginatorTrait, QueryFilter, QuerySelect, RelationTrait,
    SelectColumns, TransactionTrait, Value,
};
use tokio::sync::oneshot::Sender;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::info;

use super::utils::{
    check_subscription_name_duplicate, get_fragment_ids_by_jobs, get_internal_tables_by_id,
    rename_relation, rename_relation_refer,
};
use crate::controller::catalog::util::update_internal_tables;
use crate::controller::utils::{
    build_relation_group_for_delete, check_connection_name_duplicate,
    check_database_name_duplicate, check_function_signature_duplicate,
    check_relation_name_duplicate, check_schema_name_duplicate, check_secret_name_duplicate,
    ensure_object_id, ensure_object_not_refer, ensure_schema_empty, ensure_user_id,
    extract_external_table_name_from_definition, get_referring_objects,
    get_referring_objects_cascade, get_user_privilege, list_user_info_by_ids,
    resolve_source_register_info_for_jobs, PartialObject,
};
use crate::controller::ObjectModel;
use crate::manager::{
    get_referred_connection_ids_from_source, get_referred_secret_ids_from_source, MetaSrvEnv,
    NotificationVersion, IGNORED_NOTIFICATION_VERSION,
};
use crate::rpc::ddl_controller::DropMode;
use crate::telemetry::{report_event, MetaTelemetryJobDesc};
use crate::{MetaError, MetaResult};

pub type Catalog = (
    Vec<PbDatabase>,
    Vec<PbSchema>,
    Vec<PbTable>,
    Vec<PbSource>,
    Vec<PbSink>,
    Vec<PbSubscription>,
    Vec<PbIndex>,
    Vec<PbView>,
    Vec<PbFunction>,
    Vec<PbConnection>,
    Vec<PbSecret>,
);

pub type CatalogControllerRef = Arc<CatalogController>;

/// `CatalogController` is the controller for catalog related operations, including database, schema, table, view, etc.
pub struct CatalogController {
    pub(crate) env: MetaSrvEnv,
    pub(crate) inner: RwLock<CatalogControllerInner>,
}

#[derive(Clone, Default)]
pub struct ReleaseContext {
    pub(crate) database_id: DatabaseId,
    pub(crate) streaming_job_ids: Vec<ObjectId>,
    /// Dropped state table list, need to unregister from hummock.
    pub(crate) state_table_ids: Vec<TableId>,
    /// Dropped source list, need to unregister from source manager.
    pub(crate) source_ids: Vec<SourceId>,
    /// Dropped connection list, need to delete from vpc endpoints.
    #[allow(dead_code)]
    pub(crate) connections: Vec<ConnectionId>,

    /// Dropped fragments that are fetching data from the target source.
    pub(crate) source_fragments: HashMap<SourceId, BTreeSet<FragmentId>>,
    /// Dropped actors.
    pub(crate) removed_actors: HashSet<ActorId>,

    pub(crate) removed_fragments: HashSet<FragmentId>,
}

impl CatalogController {
    pub async fn new(env: MetaSrvEnv) -> MetaResult<Self> {
        let meta_store = env.meta_store();
        let catalog_controller = Self {
            env,
            inner: RwLock::new(CatalogControllerInner {
                db: meta_store.conn,
                creating_table_finish_notifier: HashMap::new(),
            }),
        };

        catalog_controller.init().await?;
        Ok(catalog_controller)
    }

    /// Used in `NotificationService::subscribe`.
    /// Need to pay attention to the order of acquiring locks to prevent deadlock problems.
    pub async fn get_inner_read_guard(&self) -> RwLockReadGuard<'_, CatalogControllerInner> {
        self.inner.read().await
    }

    pub async fn get_inner_write_guard(&self) -> RwLockWriteGuard<'_, CatalogControllerInner> {
        self.inner.write().await
    }
}

pub struct CatalogControllerInner {
    pub(crate) db: DatabaseConnection,
    /// Registered finish notifiers for creating tables.
    ///
    /// `DdlController` will update this map, and pass the `tx` side to `CatalogController`.
    /// On notifying, we can remove the entry from this map.
    pub creating_table_finish_notifier:
        HashMap<ObjectId, Vec<Sender<MetaResult<NotificationVersion>>>>,
}

impl CatalogController {
    pub(crate) async fn notify_frontend(
        &self,
        operation: NotificationOperation,
        info: NotificationInfo,
    ) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend(operation, info)
            .await
    }

    pub(crate) async fn notify_frontend_relation_info(
        &self,
        operation: NotificationOperation,
        relation_info: PbRelationInfo,
    ) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend_relation_info(operation, relation_info)
            .await
    }

    pub(crate) async fn current_notification_version(&self) -> NotificationVersion {
        self.env.notification_manager().current_version().await
    }
}

impl CatalogController {
    pub async fn drop_database(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<(ReleaseContext, NotificationVersion)> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        ensure_object_id(ObjectType::Database, database_id, &txn).await?;

        let streaming_jobs: Vec<ObjectId> = StreamingJob::find()
            .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(object::Column::DatabaseId.eq(Some(database_id)))
            .into_tuple()
            .all(&txn)
            .await?;

        let (source_fragments, removed_actors, removed_fragments) =
            resolve_source_register_info_for_jobs(&txn, streaming_jobs.clone()).await?;

        let state_table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .filter(
                table::Column::BelongsToJobId
                    .is_in(streaming_jobs.clone())
                    .or(table::Column::TableId.is_in(streaming_jobs.clone())),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let source_ids: Vec<SourceId> = Object::find()
            .select_only()
            .column(object::Column::Oid)
            .filter(
                object::Column::DatabaseId
                    .eq(Some(database_id))
                    .and(object::Column::ObjType.eq(ObjectType::Source)),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let connections = Connection::find()
            .inner_join(Object)
            .filter(object::Column::DatabaseId.eq(Some(database_id)))
            .all(&txn)
            .await?
            .into_iter()
            .map(|conn| conn.connection_id)
            .collect_vec();

        // Find affect users with privileges on the database and the objects in the database.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .join(JoinType::InnerJoin, user_privilege::Relation::Object.def())
            .filter(
                object::Column::DatabaseId
                    .eq(database_id)
                    .or(user_privilege::Column::Oid.eq(database_id)),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let fragment_mappings = get_fragment_ids_by_jobs(&txn, streaming_jobs.clone())
            .await?
            .into_iter()
            .map(|fragment_id| PbFragmentWorkerSlotMapping {
                fragment_id: fragment_id as _,
                mapping: None,
            })
            .collect();

        // The schema and objects in the database will be delete cascade.
        let res = Object::delete_by_id(database_id).exec(&txn).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("database", database_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        self.notify_users_update(user_infos).await;
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Database(PbDatabase {
                    id: database_id as _,
                    ..Default::default()
                }),
            )
            .await;

        self.notify_fragment_mapping(NotificationOperation::Delete, fragment_mappings)
            .await;
        Ok((
            ReleaseContext {
                database_id,
                streaming_job_ids: streaming_jobs,
                state_table_ids,
                source_ids,
                connections,
                source_fragments,
                removed_actors,
                removed_fragments,
            },
            version,
        ))
    }

    pub async fn drop_schema(
        &self,
        schema_id: SchemaId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let schema_obj = Object::find_by_id(schema_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("schema", schema_id))?;
        // TODO: support drop schema cascade.
        if drop_mode == DropMode::Restrict {
            ensure_schema_empty(schema_id, &txn).await?;
        } else {
            return Err(MetaError::permission_denied(
                "drop schema cascade is not supported yet".to_owned(),
            ));
        }

        // Find affect users with privileges on the schema and the objects in the schema.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .join(JoinType::InnerJoin, user_privilege::Relation::Object.def())
            .filter(
                object::Column::SchemaId
                    .eq(schema_id)
                    .or(user_privilege::Column::Oid.eq(schema_id)),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let res = Object::delete(object::ActiveModel {
            oid: Set(schema_id),
            ..Default::default()
        })
        .exec(&txn)
        .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("schema", schema_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        self.notify_users_update(user_infos).await;
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Schema(PbSchema {
                    id: schema_id as _,
                    database_id: schema_obj.database_id.unwrap() as _,
                    ..Default::default()
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn finish_create_subscription_catalog(&self, subscription_id: u32) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let job_id = subscription_id as i32;

        // update `created_at` as now() and `created_at_cluster_version` as current cluster version.
        let res = Object::update_many()
            .col_expr(object::Column::CreatedAt, Expr::current_timestamp().into())
            .col_expr(
                object::Column::CreatedAtClusterVersion,
                current_cluster_version().into(),
            )
            .filter(object::Column::Oid.eq(job_id))
            .exec(&txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("subscription", job_id));
        }

        // mark the target subscription as `Create`.
        let job = subscription::ActiveModel {
            subscription_id: Set(job_id),
            subscription_state: Set(SubscriptionState::Created.into()),
            ..Default::default()
        };
        job.update(&txn).await?;
        txn.commit().await?;

        Ok(())
    }

    pub async fn notify_create_subscription(
        &self,
        subscription_id: u32,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let job_id = subscription_id as i32;
        let (subscription, obj) = Subscription::find_by_id(job_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("subscription", job_id))?;
        txn.commit().await?;

        let version = self
            .notify_frontend(
                Operation::Add,
                Info::RelationGroup(PbRelationGroup {
                    relations: vec![PbRelation {
                        relation_info: PbRelationInfo::Subscription(
                            ObjectModel(subscription, obj.unwrap()).into(),
                        )
                        .into(),
                    }],
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn clean_dirty_subscription(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let filter_condition = object::Column::ObjType.eq(ObjectType::Subscription).and(
            object::Column::Oid.not_in_subquery(
                Query::select()
                    .column(subscription::Column::SubscriptionId)
                    .from(Subscription)
                    .and_where(
                        subscription::Column::SubscriptionState
                            .eq(SubscriptionState::Created as i32),
                    )
                    .take(),
            ),
        );

        let filter_condition = if let Some(database_id) = database_id {
            filter_condition.and(object::Column::DatabaseId.eq(database_id))
        } else {
            filter_condition
        };

        Object::delete_many()
            .filter(filter_condition)
            .exec(&txn)
            .await?;
        txn.commit().await?;
        Ok(())
    }

    pub async fn has_any_streaming_jobs(&self) -> MetaResult<bool> {
        let inner = self.inner.read().await;
        let count = streaming_job::Entity::find().count(&inner.db).await?;
        Ok(count > 0)
    }

    /// `clean_dirty_creating_jobs` cleans up creating jobs that are creating in Foreground mode or in Initial status.
    pub async fn clean_dirty_creating_jobs(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<Vec<SourceId>> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let filter_condition = streaming_job::Column::JobStatus.eq(JobStatus::Initial).or(
            streaming_job::Column::JobStatus
                .eq(JobStatus::Creating)
                .and(streaming_job::Column::CreateType.eq(CreateType::Foreground)),
        );

        let filter_condition = if let Some(database_id) = database_id {
            filter_condition.and(object::Column::DatabaseId.eq(database_id))
        } else {
            filter_condition
        };

        let dirty_job_objs: Vec<PartialObject> = streaming_job::Entity::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .columns([
                object::Column::Oid,
                object::Column::ObjType,
                object::Column::SchemaId,
                object::Column::DatabaseId,
            ])
            .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
            .filter(filter_condition)
            .into_partial_model()
            .all(&txn)
            .await?;

        let changed = Self::clean_dirty_sink_downstreams(&txn).await?;

        if dirty_job_objs.is_empty() {
            if changed {
                txn.commit().await?;
            }

            return Ok(vec![]);
        }

        self.log_cleaned_dirty_jobs(&dirty_job_objs, &txn).await?;

        let dirty_job_ids = dirty_job_objs.iter().map(|obj| obj.oid).collect::<Vec<_>>();

        // Filter out dummy objs for replacement.
        // todo: we'd better introduce a new dummy object type for replacement.
        let all_dirty_table_ids = dirty_job_objs
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Table)
            .map(|obj| obj.oid)
            .collect_vec();
        let dirty_table_type_map: HashMap<ObjectId, TableType> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .column(table::Column::TableType)
            .filter(table::Column::TableId.is_in(all_dirty_table_ids))
            .into_tuple::<(ObjectId, TableType)>()
            .all(&txn)
            .await?
            .into_iter()
            .collect();

        // Only notify delete for failed materialized views.
        let dirty_mview_objs = dirty_job_objs
            .into_iter()
            .filter(|obj| {
                matches!(
                    dirty_table_type_map.get(&obj.oid),
                    Some(TableType::MaterializedView)
                )
            })
            .collect_vec();

        let associated_source_ids: Vec<SourceId> = Table::find()
            .select_only()
            .column(table::Column::OptionalAssociatedSourceId)
            .filter(
                table::Column::TableId
                    .is_in(dirty_job_ids.clone())
                    .and(table::Column::OptionalAssociatedSourceId.is_not_null()),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        let dirty_state_table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .filter(table::Column::BelongsToJobId.is_in(dirty_job_ids.clone()))
            .into_tuple()
            .all(&txn)
            .await?;

        let dirty_mview_internal_table_objs = Object::find()
            .select_only()
            .columns([
                object::Column::Oid,
                object::Column::ObjType,
                object::Column::SchemaId,
                object::Column::DatabaseId,
            ])
            .join(JoinType::InnerJoin, object::Relation::Table.def())
            .filter(table::Column::BelongsToJobId.is_in(dirty_mview_objs.iter().map(|obj| obj.oid)))
            .into_partial_model()
            .all(&txn)
            .await?;

        let to_delete_objs: HashSet<ObjectId> = dirty_job_ids
            .clone()
            .into_iter()
            .chain(dirty_state_table_ids.into_iter())
            .chain(associated_source_ids.clone().into_iter())
            .collect();

        let res = Object::delete_many()
            .filter(object::Column::Oid.is_in(to_delete_objs))
            .exec(&txn)
            .await?;
        assert!(res.rows_affected > 0);

        txn.commit().await?;

        let relation_group = build_relation_group_for_delete(
            dirty_mview_objs
                .into_iter()
                .chain(dirty_mview_internal_table_objs.into_iter())
                .collect_vec(),
        );

        let _version = self
            .notify_frontend(NotificationOperation::Delete, relation_group)
            .await;

        Ok(associated_source_ids)
    }

    async fn log_cleaned_dirty_jobs(
        &self,
        dirty_objs: &[PartialObject],
        txn: &DatabaseTransaction,
    ) -> MetaResult<()> {
        // Record cleaned streaming jobs in event logs.
        let mut dirty_table_ids = vec![];
        let mut dirty_source_ids = vec![];
        let mut dirty_sink_ids = vec![];
        for dirty_job_obj in dirty_objs {
            let job_id = dirty_job_obj.oid;
            let job_type = dirty_job_obj.obj_type;
            match job_type {
                ObjectType::Table | ObjectType::Index => dirty_table_ids.push(job_id),
                ObjectType::Source => dirty_source_ids.push(job_id),
                ObjectType::Sink => dirty_sink_ids.push(job_id),
                _ => unreachable!("unexpected streaming job type"),
            }
        }

        let mut event_logs = vec![];
        if !dirty_table_ids.is_empty() {
            let table_info: Vec<(TableId, String, String)> = Table::find()
                .select_only()
                .columns([
                    table::Column::TableId,
                    table::Column::Name,
                    table::Column::Definition,
                ])
                .filter(table::Column::TableId.is_in(dirty_table_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (table_id, name, definition) in table_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: table_id as _,
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        if !dirty_source_ids.is_empty() {
            let source_info: Vec<(SourceId, String, String)> = Source::find()
                .select_only()
                .columns([
                    source::Column::SourceId,
                    source::Column::Name,
                    source::Column::Definition,
                ])
                .filter(source::Column::SourceId.is_in(dirty_source_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (source_id, name, definition) in source_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: source_id as _,
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        if !dirty_sink_ids.is_empty() {
            let sink_info: Vec<(SinkId, String, String)> = Sink::find()
                .select_only()
                .columns([
                    sink::Column::SinkId,
                    sink::Column::Name,
                    sink::Column::Definition,
                ])
                .filter(sink::Column::SinkId.is_in(dirty_sink_ids))
                .into_tuple()
                .all(txn)
                .await?;
            for (sink_id, name, definition) in sink_info {
                let event = risingwave_pb::meta::event_log::EventDirtyStreamJobClear {
                    id: sink_id as _,
                    name,
                    definition,
                    error: "clear during recovery".to_owned(),
                };
                event_logs.push(risingwave_pb::meta::event_log::Event::DirtyStreamJobClear(
                    event,
                ));
            }
        }
        self.env.event_log_manager_ref().add_event_logs(event_logs);
        Ok(())
    }

    async fn clean_dirty_sink_downstreams(txn: &DatabaseTransaction) -> MetaResult<bool> {
        // clean incoming sink from (table)
        // clean upstream fragment ids from (fragment)
        // clean stream node from (fragment)
        // clean upstream actor ids from (actor)
        let all_fragment_ids: Vec<FragmentId> = Fragment::find()
            .select_only()
            .columns(vec![fragment::Column::FragmentId])
            .into_tuple()
            .all(txn)
            .await?;

        let all_fragment_ids: HashSet<_> = all_fragment_ids.into_iter().collect();

        let table_sink_ids: Vec<ObjectId> = Sink::find()
            .select_only()
            .column(sink::Column::SinkId)
            .filter(sink::Column::TargetTable.is_not_null())
            .into_tuple()
            .all(txn)
            .await?;

        let all_table_with_incoming_sinks: Vec<(ObjectId, I32Array)> = Table::find()
            .select_only()
            .columns(vec![table::Column::TableId, table::Column::IncomingSinks])
            .into_tuple()
            .all(txn)
            .await?;

        let table_incoming_sinks_to_update = all_table_with_incoming_sinks
            .into_iter()
            .filter(|(_, incoming_sinks)| {
                let inner_ref = incoming_sinks.inner_ref();
                !inner_ref.is_empty()
                    && inner_ref
                        .iter()
                        .any(|sink_id| !table_sink_ids.contains(sink_id))
            })
            .collect_vec();

        let new_table_incoming_sinks = table_incoming_sinks_to_update
            .into_iter()
            .map(|(table_id, incoming_sinks)| {
                let new_incoming_sinks = incoming_sinks
                    .into_inner()
                    .extract_if(|id| table_sink_ids.contains(id))
                    .collect_vec();
                (table_id, I32Array::from(new_incoming_sinks))
            })
            .collect_vec();

        // no need to update, returning
        if new_table_incoming_sinks.is_empty() {
            return Ok(false);
        }

        for (table_id, new_incoming_sinks) in new_table_incoming_sinks {
            tracing::info!("cleaning dirty table sink downstream table {}", table_id);
            Table::update_many()
                .col_expr(table::Column::IncomingSinks, new_incoming_sinks.into())
                .filter(table::Column::TableId.eq(table_id))
                .exec(txn)
                .await?;

            let fragments: Vec<(FragmentId, I32Array, StreamNode, i32)> = Fragment::find()
                .select_only()
                .columns(vec![
                    fragment::Column::FragmentId,
                    fragment::Column::UpstreamFragmentId,
                    fragment::Column::StreamNode,
                    fragment::Column::FragmentTypeMask,
                ])
                .filter(fragment::Column::JobId.eq(table_id))
                .into_tuple()
                .all(txn)
                .await?;

            for (fragment_id, upstream_fragment_ids, stream_node, fragment_mask) in fragments {
                let mut upstream_fragment_ids = upstream_fragment_ids.into_inner();

                let dirty_upstream_fragment_ids = upstream_fragment_ids
                    .extract_if(|id| !all_fragment_ids.contains(id))
                    .collect_vec();

                if !dirty_upstream_fragment_ids.is_empty() {
                    // dirty downstream should be materialize fragment of table
                    assert!(fragment_mask & FragmentTypeFlag::Mview as i32 > 0);

                    tracing::info!(
                        "cleaning dirty table sink fragment {:?} from downstream fragment {}",
                        dirty_upstream_fragment_ids,
                        fragment_id
                    );

                    let mut pb_stream_node = stream_node.to_protobuf();

                    visit_stream_node_cont_mut(&mut pb_stream_node, |node| {
                        if let Some(NodeBody::Union(_)) = node.node_body {
                            node.input.retain_mut(|input| {
                                if let Some(NodeBody::Merge(merge_node)) = &mut input.node_body
                                    && all_fragment_ids
                                        .contains(&(merge_node.upstream_fragment_id as i32))
                                {
                                    true
                                } else {
                                    false
                                }
                            });
                        }
                        true
                    });

                    Fragment::update_many()
                        .col_expr(
                            fragment::Column::UpstreamFragmentId,
                            I32Array::from(upstream_fragment_ids).into(),
                        )
                        .col_expr(
                            fragment::Column::StreamNode,
                            StreamNode::from(&pb_stream_node).into(),
                        )
                        .filter(fragment::Column::FragmentId.eq(fragment_id))
                        .exec(txn)
                        .await?;

                    let actors: Vec<(ActorId, ActorUpstreamActors)> = Actor::find()
                        .select_only()
                        .columns(vec![
                            actor::Column::ActorId,
                            actor::Column::UpstreamActorIds,
                        ])
                        .filter(actor::Column::FragmentId.eq(fragment_id))
                        .into_tuple()
                        .all(txn)
                        .await?;

                    for (actor_id, upstream_actor_ids) in actors {
                        let mut upstream_actor_ids = upstream_actor_ids.into_inner();

                        let dirty_actor_upstreams = upstream_actor_ids
                            .extract_if(|id, _| !all_fragment_ids.contains(id))
                            .map(|(id, _)| id)
                            .collect_vec();

                        if !dirty_actor_upstreams.is_empty() {
                            tracing::debug!(
                                "cleaning dirty table sink fragment {:?} from downstream fragment {} actor {}",
                                dirty_actor_upstreams,
                                fragment_id,
                                actor_id,
                            );

                            Actor::update_many()
                                .col_expr(
                                    actor::Column::UpstreamActorIds,
                                    ActorUpstreamActors::from(upstream_actor_ids).into(),
                                )
                                .filter(actor::Column::ActorId.eq(actor_id))
                                .exec(txn)
                                .await?;
                        }
                    }
                }
            }
        }

        Ok(true)
    }

    pub async fn drop_function(&self, function_id: FunctionId) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let function_obj = Object::find_by_id(function_id)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("function", function_id))?;
        ensure_object_not_refer(ObjectType::Function, function_id, &txn).await?;

        // Find affect users with privileges on the function.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.eq(function_id))
            .into_tuple()
            .all(&txn)
            .await?;

        let res = Object::delete_by_id(function_id).exec(&txn).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("function", function_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        self.notify_users_update(user_infos).await;
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Function(PbFunction {
                    id: function_id as _,
                    schema_id: function_obj.schema_id.unwrap() as _,
                    database_id: function_obj.database_id.unwrap() as _,
                    ..Default::default()
                }),
            )
            .await;
        Ok(version)
    }

    pub async fn drop_secret(&self, secret_id: SecretId) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let (secret, secret_obj) = Secret::find_by_id(secret_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("secret", secret_id))?;
        ensure_object_not_refer(ObjectType::Secret, secret_id, &txn).await?;

        // Find affect users with privileges on the secret.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.eq(secret_id))
            .into_tuple()
            .all(&txn)
            .await?;

        let res = Object::delete_by_id(secret_id).exec(&txn).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("secret", secret_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        let pb_secret: PbSecret = ObjectModel(secret, secret_obj.unwrap()).into();

        self.notify_users_update(user_infos).await;

        LocalSecretManager::global().remove_secret(pb_secret.id);
        self.env
            .notification_manager()
            .notify_compute_without_version(Operation::Delete, Info::Secret(pb_secret.clone()));
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Secret(pb_secret),
            )
            .await;
        Ok(version)
    }

    pub async fn drop_connection(
        &self,
        connection_id: ConnectionId,
    ) -> MetaResult<(NotificationVersion, PbConnection)> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let (conn, conn_obj) = Connection::find_by_id(connection_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("connection", connection_id))?;
        ensure_object_not_refer(ObjectType::Connection, connection_id, &txn).await?;

        // Find affect users with privileges on the connection.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.eq(connection_id))
            .into_tuple()
            .all(&txn)
            .await?;

        let res = Object::delete_by_id(connection_id).exec(&txn).await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found("connection", connection_id));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        let pb_connection: PbConnection = ObjectModel(conn, conn_obj.unwrap()).into();

        self.notify_users_update(user_infos).await;
        let version = self
            .notify_frontend(
                NotificationOperation::Delete,
                NotificationInfo::Connection(pb_connection.clone()),
            )
            .await;
        Ok((version, pb_connection))
    }

    pub async fn comment_on(&self, comment: PbComment) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        ensure_object_id(ObjectType::Database, comment.database_id as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, comment.schema_id as _, &txn).await?;
        let table_obj = Object::find_by_id(comment.table_id as ObjectId)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("table", comment.table_id))?;

        let table = if let Some(col_idx) = comment.column_index {
            let columns: ColumnCatalogArray = Table::find_by_id(comment.table_id as TableId)
                .select_only()
                .column(table::Column::Columns)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", comment.table_id))?;
            let mut pb_columns = columns.to_protobuf();

            let column = pb_columns
                .get_mut(col_idx as usize)
                .ok_or_else(|| MetaError::catalog_id_not_found("column", col_idx))?;
            let column_desc = column.column_desc.as_mut().ok_or_else(|| {
                anyhow!(
                    "column desc at index {} for table id {} not found",
                    col_idx,
                    comment.table_id
                )
            })?;
            column_desc.description = comment.description;
            table::ActiveModel {
                table_id: Set(comment.table_id as _),
                columns: Set(pb_columns.into()),
                ..Default::default()
            }
            .update(&txn)
            .await?
        } else {
            table::ActiveModel {
                table_id: Set(comment.table_id as _),
                description: Set(comment.description),
                ..Default::default()
            }
            .update(&txn)
            .await?
        };
        txn.commit().await?;

        let version = self
            .notify_frontend_relation_info(
                NotificationOperation::Update,
                PbRelationInfo::Table(ObjectModel(table, table_obj).into()),
            )
            .await;

        Ok(version)
    }

    pub async fn drop_relation(
        &self,
        object_type: ObjectType,
        object_id: ObjectId,
        drop_mode: DropMode,
    ) -> MetaResult<(ReleaseContext, NotificationVersion)> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let obj: PartialObject = Object::find_by_id(object_id)
            .into_partial_model()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(object_type.as_str(), object_id))?;
        assert_eq!(obj.obj_type, object_type);
        let database_id = obj
            .database_id
            .ok_or_else(|| anyhow!("dropped object should have database_id"))?;

        let mut to_drop_objects = match drop_mode {
            DropMode::Cascade => get_referring_objects_cascade(object_id, &txn).await?,
            DropMode::Restrict => {
                ensure_object_not_refer(object_type, object_id, &txn).await?;
                if obj.obj_type == ObjectType::Table {
                    let indexes = get_referring_objects(object_id, &txn).await?;
                    assert!(
                        indexes.iter().all(|obj| obj.obj_type == ObjectType::Index),
                        "only index could be dropped in restrict mode"
                    );
                    indexes
                } else {
                    vec![]
                }
            }
        };
        assert!(
            to_drop_objects.iter().all(|obj| matches!(
                obj.obj_type,
                ObjectType::Table
                    | ObjectType::Index
                    | ObjectType::Sink
                    | ObjectType::View
                    | ObjectType::Subscription
            )),
            "only these objects will depends on others"
        );
        to_drop_objects.push(obj);

        // Special handling for 'sink into table'.
        if object_type != ObjectType::Sink {
            // When dropping a table downstream, all incoming sinks of the table should be dropped as well.
            if object_type == ObjectType::Table {
                let table = Table::find_by_id(object_id)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", object_id))?;

                let incoming_sinks = table.incoming_sinks.into_inner();

                if !incoming_sinks.is_empty() {
                    let objs: Vec<PartialObject> = Object::find()
                        .filter(object::Column::Oid.is_in(incoming_sinks))
                        .into_partial_model()
                        .all(&txn)
                        .await?;

                    to_drop_objects.extend(objs);
                }
            }

            let to_drop_object_ids: HashSet<_> =
                to_drop_objects.iter().map(|obj| obj.oid).collect();

            // When there is a table sink in the dependency chain of drop cascade, an error message needs to be returned currently to manually drop the sink.
            for obj in &to_drop_objects {
                if obj.obj_type == ObjectType::Sink {
                    let sink = Sink::find_by_id(obj.oid)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found("sink", obj.oid))?;

                    // Since dropping the sink into the table requires the frontend to handle some of the logic (regenerating the plan), it’s not compatible with the current cascade dropping.
                    if let Some(target_table) = sink.target_table
                        && !to_drop_object_ids.contains(&target_table)
                    {
                        bail!(
                            "Found sink into table with sink id {} in dependency, please drop them manually",
                            obj.oid,
                        );
                    }
                }
            }
        }

        let to_drop_table_ids = to_drop_objects
            .iter()
            .filter(|obj| obj.obj_type == ObjectType::Table || obj.obj_type == ObjectType::Index)
            .map(|obj| obj.oid);
        let mut to_drop_streaming_jobs = to_drop_objects
            .iter()
            .filter(|obj| {
                obj.obj_type == ObjectType::Table
                    || obj.obj_type == ObjectType::Sink
                    || obj.obj_type == ObjectType::Index
            })
            .map(|obj| obj.oid)
            .collect_vec();

        // source streaming job.
        if object_type == ObjectType::Source {
            let source_info: Option<StreamSourceInfo> = Source::find_by_id(object_id)
                .select_only()
                .column(source::Column::SourceInfo)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("source", object_id))?;
            if let Some(source_info) = source_info
                && source_info.to_protobuf().is_shared()
            {
                to_drop_streaming_jobs.push(object_id);
            }
        }

        let creating = StreamingJob::find()
            .filter(
                streaming_job::Column::JobStatus
                    .ne(JobStatus::Created)
                    .and(streaming_job::Column::JobId.is_in(to_drop_streaming_jobs.clone())),
            )
            .count(&txn)
            .await?;
        if creating != 0 {
            return Err(MetaError::permission_denied(format!(
                "can not drop {creating} creating streaming job, please cancel them firstly"
            )));
        }

        let mut to_drop_state_table_ids = to_drop_table_ids.clone().collect_vec();

        // Add associated sources.
        let mut to_drop_source_ids: Vec<SourceId> = Table::find()
            .select_only()
            .column(table::Column::OptionalAssociatedSourceId)
            .filter(
                table::Column::TableId
                    .is_in(to_drop_table_ids)
                    .and(table::Column::OptionalAssociatedSourceId.is_not_null()),
            )
            .into_tuple()
            .all(&txn)
            .await?;
        let to_drop_source_objs: Vec<PartialObject> = Object::find()
            .filter(object::Column::Oid.is_in(to_drop_source_ids.clone()))
            .into_partial_model()
            .all(&txn)
            .await?;
        to_drop_objects.extend(to_drop_source_objs);
        if object_type == ObjectType::Source {
            to_drop_source_ids.push(object_id);
        }

        if !to_drop_streaming_jobs.is_empty() {
            let to_drop_internal_table_objs: Vec<PartialObject> = Object::find()
                .select_only()
                .columns([
                    object::Column::Oid,
                    object::Column::ObjType,
                    object::Column::SchemaId,
                    object::Column::DatabaseId,
                ])
                .join(JoinType::InnerJoin, object::Relation::Table.def())
                .filter(table::Column::BelongsToJobId.is_in(to_drop_streaming_jobs.clone()))
                .into_partial_model()
                .all(&txn)
                .await?;

            to_drop_state_table_ids.extend(to_drop_internal_table_objs.iter().map(|obj| obj.oid));
            to_drop_objects.extend(to_drop_internal_table_objs);
        }

        let to_drop_objects = to_drop_objects;

        to_drop_objects.iter().for_each(|obj| {
            if let Some(obj_database_id) = obj.database_id {
                assert_eq!(
                    database_id, obj_database_id,
                    "dropped objects not in the same database: {:?}",
                    obj
                );
            }
        });

        let (source_fragments, removed_actors, removed_fragments) =
            resolve_source_register_info_for_jobs(&txn, to_drop_streaming_jobs.clone()).await?;

        let fragment_ids = get_fragment_ids_by_jobs(&txn, to_drop_streaming_jobs.clone()).await?;

        // Find affect users with privileges on all this objects.
        let to_update_user_ids: Vec<UserId> = UserPrivilege::find()
            .select_only()
            .distinct()
            .column(user_privilege::Column::UserId)
            .filter(user_privilege::Column::Oid.is_in(to_drop_objects.iter().map(|obj| obj.oid)))
            .into_tuple()
            .all(&txn)
            .await?;

        // delete all in to_drop_objects.
        let res = Object::delete_many()
            .filter(object::Column::Oid.is_in(to_drop_objects.iter().map(|obj| obj.oid)))
            .exec(&txn)
            .await?;
        if res.rows_affected == 0 {
            return Err(MetaError::catalog_id_not_found(
                object_type.as_str(),
                object_id,
            ));
        }
        let user_infos = list_user_info_by_ids(to_update_user_ids, &txn).await?;

        txn.commit().await?;

        // notify about them.
        self.notify_users_update(user_infos).await;
        let relation_group = build_relation_group_for_delete(to_drop_objects);

        let version = self
            .notify_frontend(NotificationOperation::Delete, relation_group)
            .await;

        let fragment_mappings = fragment_ids
            .into_iter()
            .map(|fragment_id| PbFragmentWorkerSlotMapping {
                fragment_id: fragment_id as _,
                mapping: None,
            })
            .collect();

        self.notify_fragment_mapping(NotificationOperation::Delete, fragment_mappings)
            .await;

        Ok((
            ReleaseContext {
                database_id,
                streaming_job_ids: to_drop_streaming_jobs,
                state_table_ids: to_drop_state_table_ids,
                source_ids: to_drop_source_ids,
                connections: vec![],
                source_fragments,
                removed_actors,
                removed_fragments,
            },
            version,
        ))
    }

    pub async fn find_creating_streaming_job_ids(
        &self,
        infos: Vec<PbCreatingJobInfo>,
    ) -> MetaResult<Vec<ObjectId>> {
        let inner = self.inner.read().await;

        type JobKey = (DatabaseId, SchemaId, String);

        // Index table is already included if we still assign the same name for index table as the index.
        let creating_tables: Vec<(ObjectId, String, DatabaseId, SchemaId)> = Table::find()
            .select_only()
            .columns([table::Column::TableId, table::Column::Name])
            .columns([object::Column::DatabaseId, object::Column::SchemaId])
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
            .into_tuple()
            .all(&inner.db)
            .await?;
        let creating_sinks: Vec<(ObjectId, String, DatabaseId, SchemaId)> = Sink::find()
            .select_only()
            .columns([sink::Column::SinkId, sink::Column::Name])
            .columns([object::Column::DatabaseId, object::Column::SchemaId])
            .join(JoinType::InnerJoin, sink::Relation::Object.def())
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
            .into_tuple()
            .all(&inner.db)
            .await?;
        let creating_subscriptions: Vec<(ObjectId, String, DatabaseId, SchemaId)> =
            Subscription::find()
                .select_only()
                .columns([
                    subscription::Column::SubscriptionId,
                    subscription::Column::Name,
                ])
                .columns([object::Column::DatabaseId, object::Column::SchemaId])
                .join(JoinType::InnerJoin, subscription::Relation::Object.def())
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(streaming_job::Column::JobStatus.eq(JobStatus::Creating))
                .into_tuple()
                .all(&inner.db)
                .await?;

        let mut job_mapping: HashMap<JobKey, ObjectId> = creating_tables
            .into_iter()
            .chain(creating_sinks.into_iter())
            .chain(creating_subscriptions.into_iter())
            .map(|(id, name, database_id, schema_id)| ((database_id, schema_id, name), id))
            .collect();

        Ok(infos
            .into_iter()
            .flat_map(|info| {
                job_mapping.remove(&(
                    info.database_id as _,
                    info.schema_id as _,
                    info.name.clone(),
                ))
            })
            .collect())
    }
}

/// `CatalogStats` is a struct to store the statistics of all catalogs.
pub struct CatalogStats {
    pub table_num: u64,
    pub mview_num: u64,
    pub index_num: u64,
    pub source_num: u64,
    pub sink_num: u64,
    pub function_num: u64,
    pub streaming_job_num: u64,
    pub actor_num: u64,
}

impl CatalogControllerInner {
    pub async fn snapshot(&self) -> MetaResult<(Catalog, Vec<PbUserInfo>)> {
        let databases = self.list_databases().await?;
        let schemas = self.list_schemas().await?;
        let tables = self.list_tables().await?;
        let sources = self.list_sources().await?;
        let sinks = self.list_sinks().await?;
        let subscriptions = self.list_subscriptions().await?;
        let indexes = self.list_indexes().await?;
        let views = self.list_views().await?;
        let functions = self.list_functions().await?;
        let connections = self.list_connections().await?;
        let secrets = self.list_secrets().await?;

        let users = self.list_users().await?;

        Ok((
            (
                databases,
                schemas,
                tables,
                sources,
                sinks,
                subscriptions,
                indexes,
                views,
                functions,
                connections,
                secrets,
            ),
            users,
        ))
    }

    pub async fn stats(&self) -> MetaResult<CatalogStats> {
        let mut table_num_map: HashMap<_, _> = Table::find()
            .select_only()
            .column(table::Column::TableType)
            .column_as(table::Column::TableId.count(), "num")
            .group_by(table::Column::TableType)
            .having(table::Column::TableType.ne(TableType::Internal))
            .into_tuple::<(TableType, i64)>()
            .all(&self.db)
            .await?
            .into_iter()
            .map(|(table_type, num)| (table_type, num as u64))
            .collect();

        let source_num = Source::find().count(&self.db).await?;
        let sink_num = Sink::find().count(&self.db).await?;
        let function_num = Function::find().count(&self.db).await?;
        let streaming_job_num = StreamingJob::find().count(&self.db).await?;
        let actor_num = Actor::find().count(&self.db).await?;

        Ok(CatalogStats {
            table_num: table_num_map.remove(&TableType::Table).unwrap_or(0),
            mview_num: table_num_map
                .remove(&TableType::MaterializedView)
                .unwrap_or(0),
            index_num: table_num_map.remove(&TableType::Index).unwrap_or(0),
            source_num,
            sink_num,
            function_num,
            streaming_job_num,
            actor_num,
        })
    }

    async fn list_databases(&self) -> MetaResult<Vec<PbDatabase>> {
        let db_objs = Database::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;
        Ok(db_objs
            .into_iter()
            .map(|(db, obj)| ObjectModel(db, obj.unwrap()).into())
            .collect())
    }

    async fn list_schemas(&self) -> MetaResult<Vec<PbSchema>> {
        let schema_objs = Schema::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(schema_objs
            .into_iter()
            .map(|(schema, obj)| ObjectModel(schema, obj.unwrap()).into())
            .collect())
    }

    async fn list_users(&self) -> MetaResult<Vec<PbUserInfo>> {
        let mut user_infos: Vec<PbUserInfo> = User::find()
            .all(&self.db)
            .await?
            .into_iter()
            .map(Into::into)
            .collect();

        for user_info in &mut user_infos {
            user_info.grant_privileges = get_user_privilege(user_info.id as _, &self.db).await?;
        }
        Ok(user_infos)
    }

    /// `list_all_tables` return all tables and internal tables.
    pub async fn list_all_state_tables(&self) -> MetaResult<Vec<PbTable>> {
        let table_objs = Table::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(table_objs
            .into_iter()
            .map(|(table, obj)| ObjectModel(table, obj.unwrap()).into())
            .collect())
    }

    /// `list_tables` return all `CREATED` tables, `CREATING` materialized views and internal tables that belong to them.
    async fn list_tables(&self) -> MetaResult<Vec<PbTable>> {
        let table_objs = Table::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(
                streaming_job::Column::JobStatus
                    .eq(JobStatus::Created)
                    .or(table::Column::TableType.eq(TableType::MaterializedView)),
            )
            .all(&self.db)
            .await?;

        let created_streaming_job_ids: Vec<ObjectId> = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .into_tuple()
            .all(&self.db)
            .await?;

        let job_ids: HashSet<ObjectId> = table_objs
            .iter()
            .map(|(t, _)| t.table_id)
            .chain(created_streaming_job_ids.iter().cloned())
            .collect();

        let internal_table_objs = Table::find()
            .find_also_related(Object)
            .filter(
                table::Column::TableType
                    .eq(TableType::Internal)
                    .and(table::Column::BelongsToJobId.is_in(job_ids)),
            )
            .all(&self.db)
            .await?;

        Ok(table_objs
            .into_iter()
            .chain(internal_table_objs.into_iter())
            .map(|(table, obj)| {
                // Correctly set the stream job status for creating materialized views and internal tables.
                let is_created = created_streaming_job_ids.contains(&table.table_id)
                    || (table.table_type == TableType::Internal
                        && created_streaming_job_ids.contains(&table.belongs_to_job_id.unwrap()));
                let mut pb_table: PbTable = ObjectModel(table, obj.unwrap()).into();
                pb_table.stream_job_status = if is_created {
                    PbStreamJobStatus::Created.into()
                } else {
                    PbStreamJobStatus::Creating.into()
                };
                pb_table
            })
            .collect())
    }

    /// `list_sources` return all sources and `CREATED` ones if contains any streaming jobs.
    async fn list_sources(&self) -> MetaResult<Vec<PbSource>> {
        let mut source_objs = Source::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(
                streaming_job::Column::JobStatus
                    .is_null()
                    .or(streaming_job::Column::JobStatus.eq(JobStatus::Created)),
            )
            .all(&self.db)
            .await?;

        // filter out inner connector sources that are still under creating.
        let created_table_ids: HashSet<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .join(JoinType::InnerJoin, table::Relation::Object1.def())
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(
                table::Column::OptionalAssociatedSourceId
                    .is_not_null()
                    .and(streaming_job::Column::JobStatus.eq(JobStatus::Created)),
            )
            .into_tuple()
            .all(&self.db)
            .await?
            .into_iter()
            .collect();
        source_objs.retain_mut(|(source, _)| {
            source.optional_associated_table_id.is_none()
                || created_table_ids.contains(&source.optional_associated_table_id.unwrap())
        });

        Ok(source_objs
            .into_iter()
            .map(|(source, obj)| ObjectModel(source, obj.unwrap()).into())
            .collect())
    }

    /// `list_sinks` return all `CREATED` sinks.
    async fn list_sinks(&self) -> MetaResult<Vec<PbSink>> {
        let sink_objs = Sink::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .all(&self.db)
            .await?;

        Ok(sink_objs
            .into_iter()
            .map(|(sink, obj)| ObjectModel(sink, obj.unwrap()).into())
            .collect())
    }

    /// `list_subscriptions` return all `CREATED` subscriptions.
    async fn list_subscriptions(&self) -> MetaResult<Vec<PbSubscription>> {
        let subscription_objs = Subscription::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(subscription_objs
            .into_iter()
            .map(|(subscription, obj)| ObjectModel(subscription, obj.unwrap()).into())
            .collect())
    }

    async fn list_views(&self) -> MetaResult<Vec<PbView>> {
        let view_objs = View::find().find_also_related(Object).all(&self.db).await?;

        Ok(view_objs
            .into_iter()
            .map(|(view, obj)| ObjectModel(view, obj.unwrap()).into())
            .collect())
    }

    /// `list_indexes` return all `CREATED` indexes.
    async fn list_indexes(&self) -> MetaResult<Vec<PbIndex>> {
        let index_objs = Index::find()
            .find_also_related(Object)
            .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .all(&self.db)
            .await?;

        Ok(index_objs
            .into_iter()
            .map(|(index, obj)| ObjectModel(index, obj.unwrap()).into())
            .collect())
    }

    async fn list_connections(&self) -> MetaResult<Vec<PbConnection>> {
        let conn_objs = Connection::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(conn_objs
            .into_iter()
            .map(|(conn, obj)| ObjectModel(conn, obj.unwrap()).into())
            .collect())
    }

    pub async fn list_secrets(&self) -> MetaResult<Vec<PbSecret>> {
        let secret_objs = Secret::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;
        Ok(secret_objs
            .into_iter()
            .map(|(secret, obj)| ObjectModel(secret, obj.unwrap()).into())
            .collect())
    }

    async fn list_functions(&self) -> MetaResult<Vec<PbFunction>> {
        let func_objs = Function::find()
            .find_also_related(Object)
            .all(&self.db)
            .await?;

        Ok(func_objs
            .into_iter()
            .map(|(func, obj)| ObjectModel(func, obj.unwrap()).into())
            .collect())
    }

    pub(crate) fn register_finish_notifier(
        &mut self,
        id: i32,
        sender: Sender<MetaResult<NotificationVersion>>,
    ) {
        self.creating_table_finish_notifier
            .entry(id)
            .or_default()
            .push(sender);
    }

    pub(crate) async fn streaming_job_is_finished(&mut self, id: i32) -> MetaResult<bool> {
        let status = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobStatus)
            .filter(streaming_job::Column::JobId.eq(id))
            .into_tuple::<JobStatus>()
            .one(&self.db)
            .await?;

        status
            .map(|status| status == JobStatus::Created)
            .ok_or_else(|| {
                MetaError::catalog_id_not_found("streaming job", "may have been cancelled/dropped")
            })
    }

    pub(crate) fn notify_finish_failed(&mut self, err: &MetaError) {
        for tx in take(&mut self.creating_table_finish_notifier)
            .into_values()
            .flatten()
        {
            let _ = tx.send(Err(err.clone()));
        }
    }

    pub async fn list_time_travel_table_ids(&self) -> MetaResult<Vec<TableId>> {
        let table_ids: Vec<TableId> = Table::find()
            .select_only()
            .filter(table::Column::TableType.is_in(vec![
                TableType::Table,
                TableType::MaterializedView,
                TableType::Index,
            ]))
            .column(table::Column::TableId)
            .into_tuple()
            .all(&self.db)
            .await?;
        Ok(table_ids)
    }
}
