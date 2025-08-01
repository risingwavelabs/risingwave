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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;

use anyhow::anyhow;
use indexmap::IndexMap;
use itertools::Itertools;
use risingwave_common::catalog::{FragmentTypeFlag, FragmentTypeMask};
use risingwave_common::config::DefaultParallelism;
use risingwave_common::hash::VnodeCountCompat;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::stream_graph_visitor::{
    visit_stream_node_body, visit_stream_node_mut,
};
use risingwave_common::{bail, current_cluster_version};
use risingwave_connector::allow_alter_on_fly_fields::check_sink_allow_alter_on_fly_fields;
use risingwave_connector::error::ConnectorError;
use risingwave_connector::sink::file_sink::fs::FsSink;
use risingwave_connector::sink::{CONNECTOR_TYPE_KEY, SinkError};
use risingwave_connector::source::{ConnectorProperties, SplitImpl};
use risingwave_connector::{WithOptionsSecResolved, WithPropertiesExt, match_sink_name_str};
use risingwave_meta_model::actor::ActorStatus;
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::{StreamingJob as StreamingJobModel, *};
use risingwave_meta_model::table::TableType;
use risingwave_meta_model::user_privilege::Action;
use risingwave_meta_model::*;
use risingwave_pb::catalog::source::PbOptionalAssociatedTableId;
use risingwave_pb::catalog::table::PbOptionalAssociatedSourceId;
use risingwave_pb::catalog::{PbCreateType, PbSink, PbTable};
use risingwave_pb::meta::list_rate_limits_response::RateLimitInfo;
use risingwave_pb::meta::object::PbObjectInfo;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Info, Operation as NotificationOperation, Operation,
};
use risingwave_pb::meta::table_fragments::PbActorStatus;
use risingwave_pb::meta::{PbObject, PbObjectGroup};
use risingwave_pb::plan_common::PbColumnCatalog;
use risingwave_pb::secret::PbSecretRef;
use risingwave_pb::source::{PbConnectorSplit, PbConnectorSplits};
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::{PbSinkLogStoreType, PbStreamNode};
use risingwave_pb::user::PbUserInfo;
use risingwave_sqlparser::ast::{SqlOption, Statement};
use risingwave_sqlparser::parser::{Parser, ParserError};
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::{BinOper, Expr, Query, SimpleExpr};
use sea_orm::{
    ActiveEnum, ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, IntoActiveModel,
    IntoSimpleExpr, JoinType, ModelTrait, NotSet, PaginatorTrait, QueryFilter, QuerySelect,
    RelationTrait, TransactionTrait,
};
use thiserror_ext::AsReport;

use super::rename::IndexItemRewriter;
use crate::barrier::{ReplaceStreamJobPlan, Reschedule};
use crate::controller::ObjectModel;
use crate::controller::catalog::{CatalogController, DropTableConnectorContext};
use crate::controller::utils::{
    PartialObject, build_object_group_for_delete, check_relation_name_duplicate,
    check_sink_into_table_cycle, ensure_object_id, ensure_user_id, get_fragment_actor_ids,
    get_internal_tables_by_id, get_table_columns, grant_default_privileges_automatically,
    insert_fragment_relations, list_user_info_by_ids,
};
use crate::error::MetaErrorInner;
use crate::manager::{NotificationVersion, StreamingJob, StreamingJobType};
use crate::model::{
    FragmentDownstreamRelation, FragmentReplaceUpstream, StreamActor, StreamContext,
    StreamJobFragments, StreamJobFragmentsToCreate, TableParallelism,
};
use crate::stream::{JobReschedulePostUpdates, SplitAssignment};
use crate::{MetaError, MetaResult};

impl CatalogController {
    pub async fn create_streaming_job_obj(
        txn: &DatabaseTransaction,
        obj_type: ObjectType,
        owner_id: UserId,
        database_id: Option<DatabaseId>,
        schema_id: Option<SchemaId>,
        create_type: PbCreateType,
        timezone: Option<String>,
        streaming_parallelism: StreamingParallelism,
        max_parallelism: usize,
        specific_resource_group: Option<String>, // todo: can we move it to StreamContext?
    ) -> MetaResult<ObjectId> {
        let obj = Self::create_object(txn, obj_type, owner_id, database_id, schema_id).await?;
        let job = streaming_job::ActiveModel {
            job_id: Set(obj.oid),
            job_status: Set(JobStatus::Initial),
            create_type: Set(create_type.into()),
            timezone: Set(timezone),
            parallelism: Set(streaming_parallelism),
            max_parallelism: Set(max_parallelism as _),
            specific_resource_group: Set(specific_resource_group),
        };
        job.insert(txn).await?;

        Ok(obj.oid)
    }

    /// Create catalogs for the streaming job, then notify frontend about them if the job is a
    /// materialized view.
    ///
    /// Some of the fields in the given streaming job are placeholders, which will
    /// be updated later in `prepare_streaming_job` and notify again in `finish_streaming_job`.
    #[await_tree::instrument]
    pub async fn create_job_catalog(
        &self,
        streaming_job: &mut StreamingJob,
        ctx: &StreamContext,
        parallelism: &Option<Parallelism>,
        max_parallelism: usize,
        mut dependencies: HashSet<ObjectId>,
        specific_resource_group: Option<String>,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let create_type = streaming_job.create_type();

        let streaming_parallelism = match (parallelism, self.env.opts.default_parallelism) {
            (None, DefaultParallelism::Full) => StreamingParallelism::Adaptive,
            (None, DefaultParallelism::Default(n)) => StreamingParallelism::Fixed(n.get()),
            (Some(n), _) => StreamingParallelism::Fixed(n.parallelism as _),
        };

        ensure_user_id(streaming_job.owner() as _, &txn).await?;
        ensure_object_id(ObjectType::Database, streaming_job.database_id() as _, &txn).await?;
        ensure_object_id(ObjectType::Schema, streaming_job.schema_id() as _, &txn).await?;
        check_relation_name_duplicate(
            &streaming_job.name(),
            streaming_job.database_id() as _,
            streaming_job.schema_id() as _,
            &txn,
        )
        .await?;

        // check if any dependency is in altering status.
        if !dependencies.is_empty() {
            let altering_cnt = ObjectDependency::find()
                .join(
                    JoinType::InnerJoin,
                    object_dependency::Relation::Object1.def(),
                )
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(
                    object_dependency::Column::Oid
                        .is_in(dependencies.clone())
                        .and(object::Column::ObjType.eq(ObjectType::Table))
                        .and(streaming_job::Column::JobStatus.ne(JobStatus::Created))
                        .and(
                            // It means the referring table is just dummy for altering.
                            object::Column::Oid.not_in_subquery(
                                Query::select()
                                    .column(table::Column::TableId)
                                    .from(Table)
                                    .to_owned(),
                            ),
                        ),
                )
                .count(&txn)
                .await?;
            if altering_cnt != 0 {
                return Err(MetaError::permission_denied(
                    "some dependent relations are being altered",
                ));
            }
        }

        match streaming_job {
            StreamingJob::MaterializedView(table) => {
                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Table,
                    table.owner as _,
                    Some(table.database_id as _),
                    Some(table.schema_id as _),
                    create_type,
                    ctx.timezone.clone(),
                    streaming_parallelism,
                    max_parallelism,
                    specific_resource_group,
                )
                .await?;
                table.id = job_id as _;
                let table_model: table::ActiveModel = table.clone().into();
                Table::insert(table_model).exec(&txn).await?;
            }
            StreamingJob::Sink(sink, _) => {
                if let Some(target_table_id) = sink.target_table
                    && check_sink_into_table_cycle(
                        target_table_id as ObjectId,
                        dependencies.iter().cloned().collect(),
                        &txn,
                    )
                    .await?
                {
                    bail!("Creating such a sink will result in circular dependency.");
                }

                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Sink,
                    sink.owner as _,
                    Some(sink.database_id as _),
                    Some(sink.schema_id as _),
                    create_type,
                    ctx.timezone.clone(),
                    streaming_parallelism,
                    max_parallelism,
                    specific_resource_group,
                )
                .await?;
                sink.id = job_id as _;
                let sink_model: sink::ActiveModel = sink.clone().into();
                Sink::insert(sink_model).exec(&txn).await?;
            }
            StreamingJob::Table(src, table, _) => {
                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Table,
                    table.owner as _,
                    Some(table.database_id as _),
                    Some(table.schema_id as _),
                    create_type,
                    ctx.timezone.clone(),
                    streaming_parallelism,
                    max_parallelism,
                    specific_resource_group,
                )
                .await?;
                table.id = job_id as _;
                if let Some(src) = src {
                    let src_obj = Self::create_object(
                        &txn,
                        ObjectType::Source,
                        src.owner as _,
                        Some(src.database_id as _),
                        Some(src.schema_id as _),
                    )
                    .await?;
                    src.id = src_obj.oid as _;
                    src.optional_associated_table_id =
                        Some(PbOptionalAssociatedTableId::AssociatedTableId(job_id as _));
                    table.optional_associated_source_id = Some(
                        PbOptionalAssociatedSourceId::AssociatedSourceId(src_obj.oid as _),
                    );
                    let source: source::ActiveModel = src.clone().into();
                    Source::insert(source).exec(&txn).await?;
                }
                let table_model: table::ActiveModel = table.clone().into();
                Table::insert(table_model).exec(&txn).await?;
            }
            StreamingJob::Index(index, table) => {
                ensure_object_id(ObjectType::Table, index.primary_table_id as _, &txn).await?;
                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Index,
                    index.owner as _,
                    Some(index.database_id as _),
                    Some(index.schema_id as _),
                    create_type,
                    ctx.timezone.clone(),
                    streaming_parallelism,
                    max_parallelism,
                    specific_resource_group,
                )
                .await?;
                // to be compatible with old implementation.
                index.id = job_id as _;
                index.index_table_id = job_id as _;
                table.id = job_id as _;

                ObjectDependency::insert(object_dependency::ActiveModel {
                    oid: Set(index.primary_table_id as _),
                    used_by: Set(table.id as _),
                    ..Default::default()
                })
                .exec(&txn)
                .await?;

                let table_model: table::ActiveModel = table.clone().into();
                Table::insert(table_model).exec(&txn).await?;
                let index_model: index::ActiveModel = index.clone().into();
                Index::insert(index_model).exec(&txn).await?;
            }
            StreamingJob::Source(src) => {
                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Source,
                    src.owner as _,
                    Some(src.database_id as _),
                    Some(src.schema_id as _),
                    create_type,
                    ctx.timezone.clone(),
                    streaming_parallelism,
                    max_parallelism,
                    specific_resource_group,
                )
                .await?;
                src.id = job_id as _;
                let source_model: source::ActiveModel = src.clone().into();
                Source::insert(source_model).exec(&txn).await?;
            }
        }

        // collect dependent secrets.
        dependencies.extend(
            streaming_job
                .dependent_secret_ids()?
                .into_iter()
                .map(|secret_id| secret_id as ObjectId),
        );
        // collect dependent connection
        dependencies.extend(
            streaming_job
                .dependent_connection_ids()?
                .into_iter()
                .map(|conn_id| conn_id as ObjectId),
        );

        // record object dependency.
        if !dependencies.is_empty() {
            ObjectDependency::insert_many(dependencies.into_iter().map(|oid| {
                object_dependency::ActiveModel {
                    oid: Set(oid),
                    used_by: Set(streaming_job.id() as _),
                    ..Default::default()
                }
            }))
            .exec(&txn)
            .await?;
        }

        txn.commit().await?;

        Ok(())
    }

    /// Create catalogs for internal tables, then notify frontend about them if the job is a
    /// materialized view.
    ///
    /// Some of the fields in the given "incomplete" internal tables are placeholders, which will
    /// be updated later in `prepare_streaming_job` and notify again in `finish_streaming_job`.
    ///
    /// Returns a mapping from the temporary table id to the actual global table id.
    pub async fn create_internal_table_catalog(
        &self,
        job: &StreamingJob,
        mut incomplete_internal_tables: Vec<PbTable>,
    ) -> MetaResult<HashMap<u32, u32>> {
        let job_id = job.id() as ObjectId;
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let mut table_id_map = HashMap::new();
        for table in &mut incomplete_internal_tables {
            let table_id = Self::create_object(
                &txn,
                ObjectType::Table,
                table.owner as _,
                Some(table.database_id as _),
                Some(table.schema_id as _),
            )
            .await?
            .oid;
            table_id_map.insert(table.id, table_id as u32);
            table.id = table_id as _;
            table.job_id = Some(job_id as _);

            let table_model = table::ActiveModel {
                table_id: Set(table_id as _),
                belongs_to_job_id: Set(Some(job_id)),
                fragment_id: NotSet,
                ..table.clone().into()
            };
            Table::insert(table_model).exec(&txn).await?;
        }
        txn.commit().await?;

        Ok(table_id_map)
    }

    pub async fn prepare_stream_job_fragments(
        &self,
        stream_job_fragments: &StreamJobFragmentsToCreate,
        streaming_job: &StreamingJob,
        for_replace: bool,
    ) -> MetaResult<()> {
        let need_notify = streaming_job.should_notify_creating();
        let (sink, table) = match streaming_job {
            StreamingJob::Sink(sink, _) => (Some(sink), None),
            StreamingJob::Table(_, table, _) => (None, Some(table)),
            StreamingJob::Index(_, _)
            | StreamingJob::Source(_)
            | StreamingJob::MaterializedView(_) => (None, None),
        };
        self.prepare_streaming_job(
            stream_job_fragments.stream_job_id().table_id as _,
            || stream_job_fragments.fragments.values(),
            &stream_job_fragments.actor_status,
            &stream_job_fragments.actor_splits,
            &stream_job_fragments.downstreams,
            need_notify,
            streaming_job.definition(),
            for_replace,
            sink,
            table,
        )
        .await
    }

    // TODO: In this function, we also update the `Table` model in the meta store.
    // Given that we've ensured the tables inside `TableFragments` are complete, shall we consider
    // making them the source of truth and performing a full replacement for those in the meta store?
    /// Insert fragments and actors to meta store. Used both for creating new jobs and replacing jobs.
    #[expect(clippy::too_many_arguments)]
    #[await_tree::instrument("prepare_streaming_job_for_{}", if for_replace { "replace" } else { "create" }
    )]
    pub async fn prepare_streaming_job<'a, I: Iterator<Item = &'a crate::model::Fragment> + 'a>(
        &self,
        job_id: ObjectId,
        get_fragments: impl Fn() -> I + 'a,
        actor_status: &BTreeMap<crate::model::ActorId, PbActorStatus>,
        actor_splits: &HashMap<crate::model::ActorId, Vec<SplitImpl>>,
        downstreams: &FragmentDownstreamRelation,
        need_notify: bool,
        definition: String,
        for_replace: bool,
        sink: Option<&PbSink>,
        table: Option<&PbTable>,
    ) -> MetaResult<()> {
        let fragment_actors = Self::extract_fragment_and_actors_from_fragments(
            job_id,
            get_fragments(),
            actor_status,
            actor_splits,
        )?;
        let inner = self.inner.write().await;

        let mut objects_to_notify = if need_notify { Some(vec![]) } else { None };
        let txn = inner.db.begin().await?;

        // Add fragments.
        let (fragments, actors): (Vec<_>, Vec<_>) = fragment_actors.into_iter().unzip();
        for fragment in fragments {
            let fragment_id = fragment.fragment_id;
            let state_table_ids = fragment.state_table_ids.inner_ref().clone();

            let fragment = fragment.into_active_model();
            Fragment::insert(fragment).exec(&txn).await?;

            // Fields including `fragment_id` and `vnode_count` were placeholder values before.
            // After table fragments are created, update them for all tables.
            if !for_replace {
                let all_tables = StreamJobFragments::collect_tables(get_fragments());
                for state_table_id in state_table_ids {
                    // Table's vnode count is not always the fragment's vnode count, so we have to
                    // look up the table from `TableFragments`.
                    // See `ActorGraphBuilder::new`.
                    let table = all_tables
                        .get(&(state_table_id as u32))
                        .unwrap_or_else(|| panic!("table {} not found", state_table_id));
                    assert_eq!(table.id, state_table_id as u32);
                    assert_eq!(table.fragment_id, fragment_id as u32);
                    let vnode_count = table.vnode_count();

                    table::ActiveModel {
                        table_id: Set(state_table_id as _),
                        fragment_id: Set(Some(fragment_id)),
                        vnode_count: Set(vnode_count as _),
                        ..Default::default()
                    }
                    .update(&txn)
                    .await?;

                    if let Some(objects) = &mut objects_to_notify {
                        let mut table = table.clone();
                        // In production, definition was replaced but still needed for notification.
                        if cfg!(not(debug_assertions)) && table.id == job_id as u32 {
                            table.definition = definition.clone();
                        }
                        objects.push(PbObject {
                            object_info: Some(PbObjectInfo::Table(table)),
                        });
                    }
                }
            }
        }

        if let Some(objects) = &mut objects_to_notify
            && let Some(sink) = sink
        {
            objects.push(PbObject {
                object_info: Some(PbObjectInfo::Sink(sink.clone())),
            })
        }

        insert_fragment_relations(&txn, downstreams).await?;

        // Add actors and actor dispatchers.
        for actors in actors {
            for actor in actors {
                let actor = actor.into_active_model();
                Actor::insert(actor).exec(&txn).await?;
            }
        }

        if !for_replace {
            // Update dml fragment id.
            if let Some(table) = table {
                Table::update(table::ActiveModel {
                    table_id: Set(table.id as _),
                    dml_fragment_id: Set(table.dml_fragment_id.map(|id| id as _)),
                    ..Default::default()
                })
                .exec(&txn)
                .await?;
            }
        }

        txn.commit().await?;

        if let Some(objects) = objects_to_notify
            && !objects.is_empty()
        {
            self.notify_frontend(Operation::Add, Info::ObjectGroup(PbObjectGroup { objects }))
                .await;
        }

        Ok(())
    }

    /// `try_abort_creating_streaming_job` is used to abort the job that is under initial status or in `FOREGROUND` mode.
    /// It returns (true, _) if the job is not found or aborted.
    /// It returns (_, Some(`database_id`)) is the `database_id` of the `job_id` exists
    #[await_tree::instrument]
    pub async fn try_abort_creating_streaming_job(
        &self,
        job_id: ObjectId,
        is_cancelled: bool,
    ) -> MetaResult<(bool, Option<DatabaseId>)> {
        let mut inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let obj = Object::find_by_id(job_id).one(&txn).await?;
        let Some(obj) = obj else {
            tracing::warn!(
                id = job_id,
                "streaming job not found when aborting creating, might be cleaned by recovery"
            );
            return Ok((true, None));
        };
        let database_id = obj
            .database_id
            .ok_or_else(|| anyhow!("obj has no database id: {:?}", obj))?;
        let streaming_job = streaming_job::Entity::find_by_id(job_id).one(&txn).await?;

        if !is_cancelled && let Some(streaming_job) = &streaming_job {
            assert_ne!(streaming_job.job_status, JobStatus::Created);
            if streaming_job.create_type == CreateType::Background
                && streaming_job.job_status == JobStatus::Creating
            {
                // If the job is created in background and still in creating status, we should not abort it and let recovery handle it.
                tracing::warn!(
                    id = job_id,
                    "streaming job is created in background and still in creating status"
                );
                return Ok((false, Some(database_id)));
            }
        }

        let internal_table_ids = get_internal_tables_by_id(job_id, &txn).await?;

        // Get the notification info if the job is a materialized view or created in the background.
        let mut objs = vec![];
        let table_obj = Table::find_by_id(job_id).one(&txn).await?;
        let need_notify = if let Some(table) = &table_obj {
            // If the job is a materialized view, we need to notify the frontend.
            table.table_type == TableType::MaterializedView
        } else {
            streaming_job.is_some_and(|job| job.create_type == CreateType::Background)
        };
        if need_notify {
            let obj: Option<PartialObject> = Object::find_by_id(job_id)
                .select_only()
                .columns([
                    object::Column::Oid,
                    object::Column::ObjType,
                    object::Column::SchemaId,
                    object::Column::DatabaseId,
                ])
                .into_partial_model()
                .one(&txn)
                .await?;
            let obj =
                obj.ok_or_else(|| MetaError::catalog_id_not_found("streaming job", job_id))?;
            objs.push(obj);
            let internal_table_objs: Vec<PartialObject> = Object::find()
                .select_only()
                .columns([
                    object::Column::Oid,
                    object::Column::ObjType,
                    object::Column::SchemaId,
                    object::Column::DatabaseId,
                ])
                .join(JoinType::InnerJoin, object::Relation::Table.def())
                .filter(table::Column::BelongsToJobId.eq(job_id))
                .into_partial_model()
                .all(&txn)
                .await?;
            objs.extend(internal_table_objs);
        }

        // Check if the job is creating sink into table.
        if table_obj.is_none()
            && let Some(Some(target_table_id)) = Sink::find_by_id(job_id)
                .select_only()
                .column(sink::Column::TargetTable)
                .into_tuple::<Option<TableId>>()
                .one(&txn)
                .await?
        {
            let tmp_id: Option<ObjectId> = ObjectDependency::find()
                .select_only()
                .column(object_dependency::Column::UsedBy)
                .join(
                    JoinType::InnerJoin,
                    object_dependency::Relation::Object1.def(),
                )
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(
                    object_dependency::Column::Oid
                        .eq(target_table_id)
                        .and(object::Column::ObjType.eq(ObjectType::Table))
                        .and(streaming_job::Column::JobStatus.ne(JobStatus::Created)),
                )
                .into_tuple()
                .one(&txn)
                .await?;
            if let Some(tmp_id) = tmp_id {
                tracing::warn!(
                    id = tmp_id,
                    "aborting temp streaming job for sink into table"
                );
                Object::delete_by_id(tmp_id).exec(&txn).await?;
            }
        }

        Object::delete_by_id(job_id).exec(&txn).await?;
        if !internal_table_ids.is_empty() {
            Object::delete_many()
                .filter(object::Column::Oid.is_in(internal_table_ids))
                .exec(&txn)
                .await?;
        }
        if let Some(t) = &table_obj
            && let Some(source_id) = t.optional_associated_source_id
        {
            Object::delete_by_id(source_id).exec(&txn).await?;
        }

        let err = if is_cancelled {
            MetaError::cancelled(format!("streaming job {job_id} is cancelled"))
        } else {
            MetaError::catalog_id_not_found("stream job", format!("streaming job {job_id} failed"))
        };
        let abort_reason = format!("streaming job aborted {}", err.as_report());
        for tx in inner
            .creating_table_finish_notifier
            .get_mut(&database_id)
            .map(|creating_tables| creating_tables.remove(&job_id).into_iter())
            .into_iter()
            .flatten()
            .flatten()
        {
            let _ = tx.send(Err(abort_reason.clone()));
        }
        txn.commit().await?;

        if !objs.is_empty() {
            // We also have notified the frontend about these objects,
            // so we need to notify the frontend to delete them here.
            self.notify_frontend(Operation::Delete, build_object_group_for_delete(objs))
                .await;
        }
        Ok((true, Some(database_id)))
    }

    #[await_tree::instrument]
    pub async fn post_collect_job_fragments(
        &self,
        job_id: ObjectId,
        actor_ids: Vec<crate::model::ActorId>,
        upstream_fragment_new_downstreams: &FragmentDownstreamRelation,
        split_assignment: &SplitAssignment,
    ) -> MetaResult<()> {
        self.post_collect_job_fragments_inner(
            job_id,
            actor_ids,
            upstream_fragment_new_downstreams,
            split_assignment,
        )
        .await
    }

    pub async fn post_collect_job_fragments_inner(
        &self,
        job_id: ObjectId,
        actor_ids: Vec<crate::model::ActorId>,
        upstream_fragment_new_downstreams: &FragmentDownstreamRelation,
        split_assignment: &SplitAssignment,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        Actor::update_many()
            .col_expr(
                actor::Column::Status,
                SimpleExpr::from(ActorStatus::Running.into_value()),
            )
            .filter(
                actor::Column::ActorId
                    .is_in(actor_ids.into_iter().map(|id| id as ActorId).collect_vec()),
            )
            .exec(&txn)
            .await?;

        for splits in split_assignment.values() {
            for (actor_id, splits) in splits {
                let splits = splits.iter().map(PbConnectorSplit::from).collect_vec();
                let connector_splits = &PbConnectorSplits { splits };
                actor::ActiveModel {
                    actor_id: Set(*actor_id as _),
                    splits: Set(Some(connector_splits.into())),
                    ..Default::default()
                }
                .update(&txn)
                .await?;
            }
        }

        insert_fragment_relations(&txn, upstream_fragment_new_downstreams).await?;

        // Mark job as CREATING.
        streaming_job::ActiveModel {
            job_id: Set(job_id),
            job_status: Set(JobStatus::Creating),
            ..Default::default()
        }
        .update(&txn)
        .await?;

        txn.commit().await?;

        Ok(())
    }

    pub async fn create_job_catalog_for_replace(
        &self,
        streaming_job: &StreamingJob,
        ctx: Option<&StreamContext>,
        specified_parallelism: Option<&NonZeroUsize>,
        expected_original_max_parallelism: Option<usize>,
    ) -> MetaResult<ObjectId> {
        let id = streaming_job.id();
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        // 1. check version.
        streaming_job.verify_version_for_replace(&txn).await?;
        // 2. check concurrent replace.
        let referring_cnt = ObjectDependency::find()
            .join(
                JoinType::InnerJoin,
                object_dependency::Relation::Object1.def(),
            )
            .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
            .filter(
                object_dependency::Column::Oid
                    .eq(id as ObjectId)
                    .and(object::Column::ObjType.eq(ObjectType::Table))
                    .and(streaming_job::Column::JobStatus.ne(JobStatus::Created)),
            )
            .count(&txn)
            .await?;
        if referring_cnt != 0 {
            return Err(MetaError::permission_denied(
                "job is being altered or referenced by some creating jobs",
            ));
        }

        // 3. check parallelism.
        let (original_max_parallelism, original_timezone): (i32, Option<String>) =
            StreamingJobModel::find_by_id(id as ObjectId)
                .select_only()
                .column(streaming_job::Column::MaxParallelism)
                .column(streaming_job::Column::Timezone)
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found(streaming_job.job_type_str(), id))?;

        if let Some(max_parallelism) = expected_original_max_parallelism
            && original_max_parallelism != max_parallelism as i32
        {
            // We already override the max parallelism in `StreamFragmentGraph` before entering this function.
            // This should not happen in normal cases.
            bail!(
                "cannot use a different max parallelism \
                 when replacing streaming job, \
                 original: {}, new: {}",
                original_max_parallelism,
                max_parallelism
            );
        }

        let parallelism = match specified_parallelism {
            None => StreamingParallelism::Adaptive,
            Some(n) => StreamingParallelism::Fixed(n.get() as _),
        };
        let timezone = ctx
            .map(|ctx| ctx.timezone.clone())
            .unwrap_or(original_timezone);

        // 4. create streaming object for new replace table.
        let new_obj_id = Self::create_streaming_job_obj(
            &txn,
            streaming_job.object_type(),
            streaming_job.owner() as _,
            Some(streaming_job.database_id() as _),
            Some(streaming_job.schema_id() as _),
            streaming_job.create_type(),
            timezone,
            parallelism,
            original_max_parallelism as _,
            None,
        )
        .await?;

        // 5. record dependency for new replace table.
        ObjectDependency::insert(object_dependency::ActiveModel {
            oid: Set(id as _),
            used_by: Set(new_obj_id as _),
            ..Default::default()
        })
        .exec(&txn)
        .await?;

        txn.commit().await?;

        Ok(new_obj_id)
    }

    /// `finish_streaming_job` marks job related objects as `Created` and notify frontend.
    pub async fn finish_streaming_job(
        &self,
        job_id: ObjectId,
        replace_stream_job_info: Option<ReplaceStreamJobPlan>,
    ) -> MetaResult<()> {
        let mut inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let job_type = Object::find_by_id(job_id)
            .select_only()
            .column(object::Column::ObjType)
            .into_tuple()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", job_id))?;

        let create_type: CreateType = StreamingJobModel::find_by_id(job_id)
            .select_only()
            .column(streaming_job::Column::CreateType)
            .into_tuple()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("streaming job", job_id))?;

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
            return Err(MetaError::catalog_id_not_found("streaming job", job_id));
        }

        // mark the target stream job as `Created`.
        let job = streaming_job::ActiveModel {
            job_id: Set(job_id),
            job_status: Set(JobStatus::Created),
            ..Default::default()
        };
        job.update(&txn).await?;

        // notify frontend: job, internal tables.
        let internal_table_objs = Table::find()
            .find_also_related(Object)
            .filter(table::Column::BelongsToJobId.eq(job_id))
            .all(&txn)
            .await?;
        let mut objects = internal_table_objs
            .iter()
            .map(|(table, obj)| PbObject {
                object_info: Some(PbObjectInfo::Table(
                    ObjectModel(table.clone(), obj.clone().unwrap()).into(),
                )),
            })
            .collect_vec();
        let mut notification_op = if create_type == CreateType::Background {
            NotificationOperation::Update
        } else {
            NotificationOperation::Add
        };
        let mut updated_user_info = vec![];

        match job_type {
            ObjectType::Table => {
                let (table, obj) = Table::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", job_id))?;
                if table.table_type == TableType::MaterializedView {
                    notification_op = NotificationOperation::Update;
                }

                if let Some(source_id) = table.optional_associated_source_id {
                    let (src, obj) = Source::find_by_id(source_id)
                        .find_also_related(Object)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found("source", source_id))?;
                    objects.push(PbObject {
                        object_info: Some(PbObjectInfo::Source(
                            ObjectModel(src, obj.unwrap()).into(),
                        )),
                    });
                }
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Table(ObjectModel(table, obj.unwrap()).into())),
                });
            }
            ObjectType::Sink => {
                let (sink, obj) = Sink::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", job_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Sink(ObjectModel(sink, obj.unwrap()).into())),
                });
            }
            ObjectType::Index => {
                let (index, obj) = Index::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("index", job_id))?;
                {
                    let (table, obj) = Table::find_by_id(index.index_table_id)
                        .find_also_related(Object)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| {
                            MetaError::catalog_id_not_found("table", index.index_table_id)
                        })?;
                    objects.push(PbObject {
                        object_info: Some(PbObjectInfo::Table(
                            ObjectModel(table, obj.unwrap()).into(),
                        )),
                    });
                }

                // If the index is created on a table with privileges, we should also
                // grant the privileges for the index and its state tables.
                let primary_table_privileges = UserPrivilege::find()
                    .filter(
                        user_privilege::Column::Oid
                            .eq(index.primary_table_id)
                            .and(user_privilege::Column::Action.eq(Action::Select)),
                    )
                    .all(&txn)
                    .await?;
                if !primary_table_privileges.is_empty() {
                    let index_state_table_ids: Vec<TableId> = Table::find()
                        .select_only()
                        .column(table::Column::TableId)
                        .filter(
                            table::Column::BelongsToJobId
                                .eq(job_id)
                                .or(table::Column::TableId.eq(index.index_table_id)),
                        )
                        .into_tuple()
                        .all(&txn)
                        .await?;
                    let mut new_privileges = vec![];
                    for privilege in &primary_table_privileges {
                        for state_table_id in &index_state_table_ids {
                            new_privileges.push(user_privilege::ActiveModel {
                                id: Default::default(),
                                oid: Set(*state_table_id),
                                user_id: Set(privilege.user_id),
                                action: Set(Action::Select),
                                dependent_id: Set(privilege.dependent_id),
                                granted_by: Set(privilege.granted_by),
                                with_grant_option: Set(privilege.with_grant_option),
                            });
                        }
                    }
                    UserPrivilege::insert_many(new_privileges)
                        .exec(&txn)
                        .await?;

                    updated_user_info = list_user_info_by_ids(
                        primary_table_privileges.into_iter().map(|p| p.user_id),
                        &txn,
                    )
                    .await?;
                }

                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Index(ObjectModel(index, obj.unwrap()).into())),
                });
            }
            ObjectType::Source => {
                let (source, obj) = Source::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("source", job_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Source(
                        ObjectModel(source, obj.unwrap()).into(),
                    )),
                });
            }
            _ => unreachable!("invalid job type: {:?}", job_type),
        }

        let replace_table_mapping_update = match replace_stream_job_info {
            Some(ReplaceStreamJobPlan {
                streaming_job,
                replace_upstream,
                tmp_id,
                ..
            }) => {
                let incoming_sink_id = job_id;

                let (relations, _) = Self::finish_replace_streaming_job_inner(
                    tmp_id as ObjectId,
                    replace_upstream,
                    SinkIntoTableContext {
                        creating_sink_id: Some(incoming_sink_id as _),
                        dropping_sink_id: None,
                        updated_sink_catalogs: vec![],
                    },
                    &txn,
                    streaming_job,
                    None, // will not drop table connector when creating a streaming job
                    None, // no auto schema refresh sinks when create table
                )
                .await?;

                Some(relations)
            }
            None => None,
        };

        if job_type != ObjectType::Index {
            updated_user_info = grant_default_privileges_automatically(&txn, job_id).await?;
        }
        txn.commit().await?;

        let mut version = self
            .notify_frontend(
                notification_op,
                NotificationInfo::ObjectGroup(PbObjectGroup { objects }),
            )
            .await;

        // notify users about the default privileges
        if !updated_user_info.is_empty() {
            version = self.notify_users_update(updated_user_info).await;
        }

        if let Some(objects) = replace_table_mapping_update {
            version = self
                .notify_frontend(
                    NotificationOperation::Update,
                    NotificationInfo::ObjectGroup(PbObjectGroup { objects }),
                )
                .await;
        }
        inner
            .creating_table_finish_notifier
            .values_mut()
            .for_each(|creating_tables| {
                if let Some(txs) = creating_tables.remove(&job_id) {
                    for tx in txs {
                        let _ = tx.send(Ok(version));
                    }
                }
            });

        Ok(())
    }

    pub async fn finish_replace_streaming_job(
        &self,
        tmp_id: ObjectId,
        streaming_job: StreamingJob,
        replace_upstream: FragmentReplaceUpstream,
        sink_into_table_context: SinkIntoTableContext,
        drop_table_connector_ctx: Option<&DropTableConnectorContext>,
        auto_refresh_schema_sinks: Option<Vec<FinishAutoRefreshSchemaSinkContext>>,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let (objects, delete_notification_objs) = Self::finish_replace_streaming_job_inner(
            tmp_id,
            replace_upstream,
            sink_into_table_context,
            &txn,
            streaming_job,
            drop_table_connector_ctx,
            auto_refresh_schema_sinks,
        )
        .await?;

        txn.commit().await?;

        let mut version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup { objects }),
            )
            .await;

        if let Some((user_infos, to_drop_objects)) = delete_notification_objs {
            self.notify_users_update(user_infos).await;
            version = self
                .notify_frontend(
                    NotificationOperation::Delete,
                    build_object_group_for_delete(to_drop_objects),
                )
                .await;
        }

        Ok(version)
    }

    pub async fn finish_replace_streaming_job_inner(
        tmp_id: ObjectId,
        replace_upstream: FragmentReplaceUpstream,
        SinkIntoTableContext {
            creating_sink_id,
            dropping_sink_id,
            updated_sink_catalogs,
        }: SinkIntoTableContext,
        txn: &DatabaseTransaction,
        streaming_job: StreamingJob,
        drop_table_connector_ctx: Option<&DropTableConnectorContext>,
        auto_refresh_schema_sinks: Option<Vec<FinishAutoRefreshSchemaSinkContext>>,
    ) -> MetaResult<(Vec<PbObject>, Option<(Vec<PbUserInfo>, Vec<PartialObject>)>)> {
        let original_job_id = streaming_job.id() as ObjectId;
        let job_type = streaming_job.job_type();

        let mut index_item_rewriter = None;

        // Update catalog
        match streaming_job {
            StreamingJob::Table(_source, table, _table_job_type) => {
                // The source catalog should remain unchanged

                let original_column_catalogs = get_table_columns(txn, original_job_id).await?;

                index_item_rewriter = Some({
                    let original_columns = original_column_catalogs
                        .to_protobuf()
                        .into_iter()
                        .map(|c| c.column_desc.unwrap())
                        .collect_vec();
                    let new_columns = table
                        .columns
                        .iter()
                        .map(|c| c.column_desc.clone().unwrap())
                        .collect_vec();

                    IndexItemRewriter {
                        original_columns,
                        new_columns,
                    }
                });

                // For sinks created in earlier versions, we need to set the original_target_columns.
                for sink_id in updated_sink_catalogs {
                    sink::ActiveModel {
                        sink_id: Set(sink_id as _),
                        original_target_columns: Set(Some(original_column_catalogs.clone())),
                        ..Default::default()
                    }
                    .update(txn)
                    .await?;
                }
                // Update the table catalog with the new one. (column catalog is also updated here)
                let mut table = table::ActiveModel::from(table);
                let mut incoming_sinks = table.incoming_sinks.as_ref().inner_ref().clone();
                if let Some(sink_id) = creating_sink_id {
                    debug_assert!(!incoming_sinks.contains(&{ sink_id }));
                    incoming_sinks.push(sink_id as _);
                }
                if let Some(drop_table_connector_ctx) = drop_table_connector_ctx
                    && drop_table_connector_ctx.to_change_streaming_job_id == original_job_id
                {
                    // drop table connector, the rest logic is in `drop_table_associated_source`
                    table.optional_associated_source_id = Set(None);
                }

                if let Some(sink_id) = dropping_sink_id {
                    let drained = incoming_sinks
                        .extract_if(.., |id| *id == sink_id)
                        .collect_vec();
                    debug_assert_eq!(drained, vec![sink_id]);
                }

                table.incoming_sinks = Set(incoming_sinks.into());
                table.update(txn).await?;
            }
            StreamingJob::Source(source) => {
                // Update the source catalog with the new one.
                let source = source::ActiveModel::from(source);
                source.update(txn).await?;
            }
            StreamingJob::MaterializedView(table) => {
                // Update the table catalog with the new one.
                let table = table::ActiveModel::from(table);
                table.update(txn).await?;
            }
            _ => unreachable!(
                "invalid streaming job type: {:?}",
                streaming_job.job_type_str()
            ),
        }

        async fn finish_fragments(
            txn: &DatabaseTransaction,
            tmp_id: ObjectId,
            original_job_id: ObjectId,
            replace_upstream: FragmentReplaceUpstream,
        ) -> MetaResult<()> {
            // 0. update internal tables
            // Fields including `fragment_id` were placeholder values before.
            // After table fragments are created, update them for all internal tables.
            let fragment_info: Vec<(FragmentId, I32Array)> = Fragment::find()
                .select_only()
                .columns([
                    fragment::Column::FragmentId,
                    fragment::Column::StateTableIds,
                ])
                .filter(fragment::Column::JobId.eq(tmp_id))
                .into_tuple()
                .all(txn)
                .await?;
            for (fragment_id, state_table_ids) in fragment_info {
                for state_table_id in state_table_ids.into_inner() {
                    table::ActiveModel {
                        table_id: Set(state_table_id as _),
                        fragment_id: Set(Some(fragment_id)),
                        // No need to update `vnode_count` because it must remain the same.
                        ..Default::default()
                    }
                    .update(txn)
                    .await?;
                }
            }

            // 1. replace old fragments/actors with new ones.
            Fragment::delete_many()
                .filter(fragment::Column::JobId.eq(original_job_id))
                .exec(txn)
                .await?;
            Fragment::update_many()
                .col_expr(fragment::Column::JobId, SimpleExpr::from(original_job_id))
                .filter(fragment::Column::JobId.eq(tmp_id))
                .exec(txn)
                .await?;

            // 2. update merges.
            // update downstream fragment's Merge node, and upstream_fragment_id
            for (fragment_id, fragment_replace_map) in replace_upstream {
                let (fragment_id, mut stream_node) =
                    Fragment::find_by_id(fragment_id as FragmentId)
                        .select_only()
                        .columns([fragment::Column::FragmentId, fragment::Column::StreamNode])
                        .into_tuple::<(FragmentId, StreamNode)>()
                        .one(txn)
                        .await?
                        .map(|(id, node)| (id, node.to_protobuf()))
                        .ok_or_else(|| MetaError::catalog_id_not_found("fragment", fragment_id))?;

                visit_stream_node_mut(&mut stream_node, |body| {
                    if let PbNodeBody::Merge(m) = body
                        && let Some(new_fragment_id) =
                            fragment_replace_map.get(&m.upstream_fragment_id)
                    {
                        m.upstream_fragment_id = *new_fragment_id;
                    }
                });
                fragment::ActiveModel {
                    fragment_id: Set(fragment_id),
                    stream_node: Set(StreamNode::from(&stream_node)),
                    ..Default::default()
                }
                .update(txn)
                .await?;
            }

            // 3. remove dummy object.
            Object::delete_by_id(tmp_id).exec(txn).await?;

            Ok(())
        }

        finish_fragments(txn, tmp_id, original_job_id, replace_upstream).await?;

        // 4. update catalogs and notify.
        let mut objects = vec![];
        match job_type {
            StreamingJobType::Table(_) | StreamingJobType::MaterializedView => {
                let (table, table_obj) = Table::find_by_id(original_job_id)
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("object", original_job_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Table(
                        ObjectModel(table, table_obj.unwrap()).into(),
                    )),
                })
            }
            StreamingJobType::Source => {
                let (source, source_obj) = Source::find_by_id(original_job_id)
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("object", original_job_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Source(
                        ObjectModel(source, source_obj.unwrap()).into(),
                    )),
                })
            }
            _ => unreachable!("invalid streaming job type for replace: {:?}", job_type),
        }

        if let Some(expr_rewriter) = index_item_rewriter {
            let index_items: Vec<(IndexId, ExprNodeArray)> = Index::find()
                .select_only()
                .columns([index::Column::IndexId, index::Column::IndexItems])
                .filter(index::Column::PrimaryTableId.eq(original_job_id))
                .into_tuple()
                .all(txn)
                .await?;
            for (index_id, nodes) in index_items {
                let mut pb_nodes = nodes.to_protobuf();
                pb_nodes
                    .iter_mut()
                    .for_each(|x| expr_rewriter.rewrite_expr(x));
                let index = index::ActiveModel {
                    index_id: Set(index_id),
                    index_items: Set(pb_nodes.into()),
                    ..Default::default()
                }
                .update(txn)
                .await?;
                let index_obj = index
                    .find_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("object", index.index_id))?;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Index(ObjectModel(index, index_obj).into())),
                });
            }
        }

        if let Some(sinks) = auto_refresh_schema_sinks {
            for finish_sink_context in sinks {
                finish_fragments(
                    txn,
                    finish_sink_context.tmp_sink_id,
                    finish_sink_context.original_sink_id,
                    Default::default(),
                )
                .await?;
                let (mut sink, sink_obj) = Sink::find_by_id(finish_sink_context.original_sink_id)
                    .find_also_related(Object)
                    .one(txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", original_job_id))?;
                let columns = ColumnCatalogArray::from(finish_sink_context.columns);
                Sink::update(sink::ActiveModel {
                    sink_id: Set(finish_sink_context.original_sink_id),
                    columns: Set(columns.clone()),
                    ..Default::default()
                })
                .exec(txn)
                .await?;
                sink.columns = columns;
                objects.push(PbObject {
                    object_info: Some(PbObjectInfo::Sink(
                        ObjectModel(sink, sink_obj.unwrap()).into(),
                    )),
                });
                if let Some((log_store_table_id, new_log_store_table_columns)) =
                    finish_sink_context.new_log_store_table
                {
                    let new_log_store_table_columns: ColumnCatalogArray =
                        new_log_store_table_columns.into();
                    let (mut table, table_obj) = Table::find_by_id(log_store_table_id)
                        .find_also_related(Object)
                        .one(txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found("table", original_job_id))?;
                    Table::update(table::ActiveModel {
                        table_id: Set(log_store_table_id),
                        columns: Set(new_log_store_table_columns.clone()),
                        ..Default::default()
                    })
                    .exec(txn)
                    .await?;
                    table.columns = new_log_store_table_columns;
                    objects.push(PbObject {
                        object_info: Some(PbObjectInfo::Table(
                            ObjectModel(table, table_obj.unwrap()).into(),
                        )),
                    });
                }
            }
        }

        let mut notification_objs: Option<(Vec<PbUserInfo>, Vec<PartialObject>)> = None;
        if let Some(drop_table_connector_ctx) = drop_table_connector_ctx {
            notification_objs =
                Some(Self::drop_table_associated_source(txn, drop_table_connector_ctx).await?);
        }

        Ok((objects, notification_objs))
    }

    /// Abort the replacing streaming job by deleting the temporary job object.
    pub async fn try_abort_replacing_streaming_job(
        &self,
        tmp_job_id: ObjectId,
        tmp_sink_ids: Option<Vec<ObjectId>>,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        Object::delete_by_id(tmp_job_id).exec(&txn).await?;
        if let Some(tmp_sink_ids) = tmp_sink_ids {
            for tmp_sink_id in tmp_sink_ids {
                Object::delete_by_id(tmp_sink_id).exec(&txn).await?;
            }
        }
        txn.commit().await?;
        Ok(())
    }

    // edit the `rate_limit` of the `Source` node in given `source_id`'s fragments
    // return the actor_ids to be applied
    pub async fn update_source_rate_limit_by_source_id(
        &self,
        source_id: SourceId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        {
            let active_source = source::ActiveModel {
                source_id: Set(source_id),
                rate_limit: Set(rate_limit.map(|v| v as i32)),
                ..Default::default()
            };
            active_source.update(&txn).await?;
        }

        let (source, obj) = Source::find_by_id(source_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| {
                MetaError::catalog_id_not_found(ObjectType::Source.as_str(), source_id)
            })?;

        let is_fs_source = source.with_properties.inner_ref().is_new_fs_connector();
        let streaming_job_ids: Vec<ObjectId> =
            if let Some(table_id) = source.optional_associated_table_id {
                vec![table_id]
            } else if let Some(source_info) = &source.source_info
                && source_info.to_protobuf().is_shared()
            {
                vec![source_id]
            } else {
                ObjectDependency::find()
                    .select_only()
                    .column(object_dependency::Column::UsedBy)
                    .filter(object_dependency::Column::Oid.eq(source_id))
                    .into_tuple()
                    .all(&txn)
                    .await?
            };

        if streaming_job_ids.is_empty() {
            return Err(MetaError::invalid_parameter(format!(
                "source id {source_id} not used by any streaming job"
            )));
        }

        let fragments: Vec<(FragmentId, i32, StreamNode)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::StreamNode,
            ])
            .filter(fragment::Column::JobId.is_in(streaming_job_ids))
            .into_tuple()
            .all(&txn)
            .await?;
        let mut fragments = fragments
            .into_iter()
            .map(|(id, mask, stream_node)| {
                (
                    id,
                    FragmentTypeMask::from(mask as u32),
                    stream_node.to_protobuf(),
                )
            })
            .collect_vec();

        fragments.retain_mut(|(_, fragment_type_mask, stream_node)| {
            let mut found = false;
            if fragment_type_mask.contains(FragmentTypeFlag::Source) {
                visit_stream_node_mut(stream_node, |node| {
                    if let PbNodeBody::Source(node) = node
                        && let Some(node_inner) = &mut node.source_inner
                        && node_inner.source_id == source_id as u32
                    {
                        node_inner.rate_limit = rate_limit;
                        found = true;
                    }
                });
            }
            if is_fs_source {
                // in older versions, there's no fragment type flag for `FsFetch` node,
                // so we just scan all fragments for StreamFsFetch node if using fs connector
                visit_stream_node_mut(stream_node, |node| {
                    if let PbNodeBody::StreamFsFetch(node) = node {
                        fragment_type_mask.add(FragmentTypeFlag::FsFetch);
                        if let Some(node_inner) = &mut node.node_inner
                            && node_inner.source_id == source_id as u32
                        {
                            node_inner.rate_limit = rate_limit;
                            found = true;
                        }
                    }
                });
            }
            found
        });

        assert!(
            !fragments.is_empty(),
            "source id should be used by at least one fragment"
        );
        let fragment_ids = fragments.iter().map(|(id, _, _)| *id).collect_vec();
        for (id, fragment_type_mask, stream_node) in fragments {
            fragment::ActiveModel {
                fragment_id: Set(id),
                fragment_type_mask: Set(fragment_type_mask.into()),
                stream_node: Set(StreamNode::from(&stream_node)),
                ..Default::default()
            }
            .update(&txn)
            .await?;
        }
        let fragment_actors = get_fragment_actor_ids(&txn, fragment_ids).await?;

        txn.commit().await?;

        let relation_info = PbObjectInfo::Source(ObjectModel(source, obj.unwrap()).into());
        let _version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: vec![PbObject {
                        object_info: Some(relation_info),
                    }],
                }),
            )
            .await;

        Ok(fragment_actors)
    }

    // edit the content of fragments in given `table_id`
    // return the actor_ids to be applied
    pub async fn mutate_fragments_by_job_id(
        &self,
        job_id: ObjectId,
        // returns true if the mutation is applied
        mut fragments_mutation_fn: impl FnMut(FragmentTypeMask, &mut PbStreamNode) -> MetaResult<bool>,
        // error message when no relevant fragments is found
        err_msg: &'static str,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragments: Vec<(FragmentId, i32, StreamNode)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::StreamNode,
            ])
            .filter(fragment::Column::JobId.eq(job_id))
            .into_tuple()
            .all(&txn)
            .await?;
        let mut fragments = fragments
            .into_iter()
            .map(|(id, mask, stream_node)| {
                (id, FragmentTypeMask::from(mask), stream_node.to_protobuf())
            })
            .collect_vec();

        let fragments = fragments
            .iter_mut()
            .map(|(_, fragment_type_mask, stream_node)| {
                fragments_mutation_fn(*fragment_type_mask, stream_node)
            })
            .collect::<MetaResult<Vec<bool>>>()?
            .into_iter()
            .zip_eq_debug(std::mem::take(&mut fragments))
            .filter_map(|(keep, fragment)| if keep { Some(fragment) } else { None })
            .collect::<Vec<_>>();

        if fragments.is_empty() {
            return Err(MetaError::invalid_parameter(format!(
                "job id {job_id}: {}",
                err_msg
            )));
        }

        let fragment_ids = fragments.iter().map(|(id, _, _)| *id).collect_vec();
        for (id, _, stream_node) in fragments {
            fragment::ActiveModel {
                fragment_id: Set(id),
                stream_node: Set(StreamNode::from(&stream_node)),
                ..Default::default()
            }
            .update(&txn)
            .await?;
        }
        let fragment_actors = get_fragment_actor_ids(&txn, fragment_ids).await?;

        txn.commit().await?;

        Ok(fragment_actors)
    }

    async fn mutate_fragment_by_fragment_id(
        &self,
        fragment_id: FragmentId,
        mut fragment_mutation_fn: impl FnMut(FragmentTypeMask, &mut PbStreamNode) -> bool,
        err_msg: &'static str,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let (fragment_type_mask, stream_node): (i32, StreamNode) =
            Fragment::find_by_id(fragment_id)
                .select_only()
                .columns([
                    fragment::Column::FragmentTypeMask,
                    fragment::Column::StreamNode,
                ])
                .into_tuple()
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("fragment", fragment_id))?;
        let mut pb_stream_node = stream_node.to_protobuf();
        let fragment_type_mask = FragmentTypeMask::from(fragment_type_mask);

        if !fragment_mutation_fn(fragment_type_mask, &mut pb_stream_node) {
            return Err(MetaError::invalid_parameter(format!(
                "fragment id {fragment_id}: {}",
                err_msg
            )));
        }

        fragment::ActiveModel {
            fragment_id: Set(fragment_id),
            stream_node: Set(stream_node),
            ..Default::default()
        }
        .update(&txn)
        .await?;

        let fragment_actors = get_fragment_actor_ids(&txn, vec![fragment_id]).await?;

        txn.commit().await?;

        Ok(fragment_actors)
    }

    // edit the `rate_limit` of the `Chain` node in given `table_id`'s fragments
    // return the actor_ids to be applied
    pub async fn update_backfill_rate_limit_by_job_id(
        &self,
        job_id: ObjectId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let update_backfill_rate_limit =
            |fragment_type_mask: FragmentTypeMask, stream_node: &mut PbStreamNode| {
                let mut found = false;
                if fragment_type_mask
                    .contains_any(FragmentTypeFlag::backfill_rate_limit_fragments())
                {
                    visit_stream_node_mut(stream_node, |node| match node {
                        PbNodeBody::StreamCdcScan(node) => {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                        PbNodeBody::StreamScan(node) => {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                        PbNodeBody::SourceBackfill(node) => {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                        PbNodeBody::Sink(node) => {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                        _ => {}
                    });
                }
                Ok(found)
            };

        self.mutate_fragments_by_job_id(
            job_id,
            update_backfill_rate_limit,
            "stream scan node or source node not found",
        )
        .await
    }

    // edit the `rate_limit` of the `Sink` node in given `table_id`'s fragments
    // return the actor_ids to be applied
    pub async fn update_sink_rate_limit_by_job_id(
        &self,
        job_id: ObjectId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let update_sink_rate_limit =
            |fragment_type_mask: FragmentTypeMask, stream_node: &mut PbStreamNode| {
                let mut found = Ok(false);
                if fragment_type_mask.contains_any(FragmentTypeFlag::sink_rate_limit_fragments()) {
                    visit_stream_node_mut(stream_node, |node| {
                        if let PbNodeBody::Sink(node) = node {
                            if node.log_store_type != PbSinkLogStoreType::KvLogStore as i32 {
                                found = Err(MetaError::invalid_parameter(
                                    "sink rate limit is only supported for kv log store, please SET sink_decouple = TRUE before CREATE SINK",
                                ));
                                return;
                            }
                            node.rate_limit = rate_limit;
                            found = Ok(true);
                        }
                    });
                }
                found
            };

        self.mutate_fragments_by_job_id(job_id, update_sink_rate_limit, "sink node not found")
            .await
    }

    pub async fn update_dml_rate_limit_by_job_id(
        &self,
        job_id: ObjectId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let update_dml_rate_limit =
            |fragment_type_mask: FragmentTypeMask, stream_node: &mut PbStreamNode| {
                let mut found = false;
                if fragment_type_mask.contains_any(FragmentTypeFlag::dml_rate_limit_fragments()) {
                    visit_stream_node_mut(stream_node, |node| {
                        if let PbNodeBody::Dml(node) = node {
                            node.rate_limit = rate_limit;
                            found = true;
                        }
                    });
                }
                Ok(found)
            };

        self.mutate_fragments_by_job_id(job_id, update_dml_rate_limit, "dml node not found")
            .await
    }

    pub async fn update_source_props_by_source_id(
        &self,
        source_id: SourceId,
        alter_props: BTreeMap<String, String>,
        alter_secret_refs: BTreeMap<String, PbSecretRef>,
    ) -> MetaResult<WithOptionsSecResolved> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let (source, _obj) = Source::find_by_id(source_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| {
                MetaError::catalog_id_not_found(ObjectType::Source.as_str(), source_id)
            })?;
        let connector = source.with_properties.0.get_connector().unwrap();

        // Use check_source_allow_alter_on_fly_fields to validate allowed properties
        let prop_keys: Vec<String> = alter_props
            .keys()
            .chain(alter_secret_refs.keys())
            .cloned()
            .collect();
        risingwave_connector::allow_alter_on_fly_fields::check_source_allow_alter_on_fly_fields(
            &connector, &prop_keys,
        )?;

        let mut options_with_secret = WithOptionsSecResolved::new(
            source.with_properties.0.clone(),
            source
                .secret_ref
                .map(|secret_ref| secret_ref.to_protobuf())
                .unwrap_or_default(),
        );
        let (to_add_secret_dep, to_remove_secret_dep) =
            options_with_secret.handle_update(alter_props, alter_secret_refs)?;

        tracing::info!(
            "applying new properties to source: source_id={}, options_with_secret={:?}",
            source_id,
            options_with_secret
        );
        // check if the alter-ed props are valid for each Connector
        let _ = ConnectorProperties::extract(options_with_secret.clone(), true)?;
        // todo: validate via source manager

        let mut associate_table_id = None;

        // can be source_id or table_id
        // if updating an associated source, the preferred_id is the table_id
        // otherwise, it is the source_id
        let mut preferred_id: i32 = source_id;
        let rewrite_sql = {
            let definition = source.definition.clone();

            let [mut stmt]: [_; 1] = Parser::parse_sql(&definition)
                .map_err(|e| {
                    MetaError::from(MetaErrorInner::Connector(ConnectorError::from(
                        anyhow!(e).context("Failed to parse source definition SQL"),
                    )))
                })?
                .try_into()
                .unwrap();

            /// Formats SQL options with secret values properly resolved
            ///
            /// This function processes configuration options that may contain sensitive data:
            /// - Plaintext options are directly converted to `SqlOption`
            /// - Secret options are retrieved from the database and formatted as "SECRET {name}"
            ///   without exposing the actual secret value
            ///
            /// # Arguments
            /// * `txn` - Database transaction for retrieving secrets
            /// * `options_with_secret` - Container of options with both plaintext and secret values
            ///
            /// # Returns
            /// * `MetaResult<Vec<SqlOption>>` - List of formatted SQL options or error
            async fn format_with_option_secret_resolved(
                txn: &DatabaseTransaction,
                options_with_secret: &WithOptionsSecResolved,
            ) -> MetaResult<Vec<SqlOption>> {
                let mut options = Vec::new();
                for (k, v) in options_with_secret.as_plaintext() {
                    let sql_option = SqlOption::try_from((k, &format!("'{}'", v)))
                        .map_err(|e| MetaError::invalid_parameter(e.to_report_string()))?;
                    options.push(sql_option);
                }
                for (k, v) in options_with_secret.as_secret() {
                    if let Some(secret_model) =
                        Secret::find_by_id(v.secret_id as i32).one(txn).await?
                    {
                        let sql_option =
                            SqlOption::try_from((k, &format!("SECRET {}", secret_model.name)))
                                .map_err(|e| MetaError::invalid_parameter(e.to_report_string()))?;
                        options.push(sql_option);
                    } else {
                        return Err(MetaError::catalog_id_not_found("secret", v.secret_id));
                    }
                }
                Ok(options)
            }

            match &mut stmt {
                Statement::CreateSource { stmt } => {
                    stmt.with_properties.0 =
                        format_with_option_secret_resolved(&txn, &options_with_secret).await?;
                }
                Statement::CreateTable { with_options, .. } => {
                    *with_options =
                        format_with_option_secret_resolved(&txn, &options_with_secret).await?;
                    associate_table_id = source.optional_associated_table_id;
                    preferred_id = associate_table_id.unwrap();
                }
                _ => unreachable!(),
            }

            stmt.to_string()
        };

        {
            // update secret dependencies
            if !to_add_secret_dep.is_empty() {
                ObjectDependency::insert_many(to_add_secret_dep.into_iter().map(|secret_id| {
                    object_dependency::ActiveModel {
                        oid: Set(secret_id as _),
                        used_by: Set(preferred_id as _),
                        ..Default::default()
                    }
                }))
                .exec(&txn)
                .await?;
            }
            if !to_remove_secret_dep.is_empty() {
                // todo: fix the filter logic
                let _ = ObjectDependency::delete_many()
                    .filter(
                        object_dependency::Column::Oid
                            .is_in(to_remove_secret_dep)
                            .and(
                                object_dependency::Column::UsedBy.eq::<ObjectId>(preferred_id as _),
                            ),
                    )
                    .exec(&txn)
                    .await?;
            }
        }

        let active_source_model = source::ActiveModel {
            source_id: Set(source_id),
            definition: Set(rewrite_sql.clone()),
            with_properties: Set(options_with_secret.as_plaintext().clone().into()),
            secret_ref: Set((!options_with_secret.as_secret().is_empty())
                .then(|| SecretRef::from(options_with_secret.as_secret().clone()))),
            ..Default::default()
        };
        active_source_model.update(&txn).await?;

        if let Some(associate_table_id) = associate_table_id {
            // update the associated table statement accordly
            let active_table_model = table::ActiveModel {
                table_id: Set(associate_table_id),
                definition: Set(rewrite_sql),
                ..Default::default()
            };
            active_table_model.update(&txn).await?;
        }

        // update fragments
        update_connector_props_fragments(
            &txn,
            if let Some(associate_table_id) = associate_table_id {
                // if updating table with connector, the fragment_id is table id
                associate_table_id
            } else {
                source_id
            },
            FragmentTypeFlag::Source,
            |node, found| {
                if let PbNodeBody::Source(node) = node
                    && let Some(source_inner) = &mut node.source_inner
                {
                    source_inner.with_properties = options_with_secret.as_plaintext().clone();
                    source_inner.secret_refs = options_with_secret.as_secret().clone();
                    *found = true;
                }
            },
        )
        .await?;

        let mut to_update_objs = Vec::with_capacity(2);
        let (source, obj) = Source::find_by_id(source_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| {
                MetaError::catalog_id_not_found(ObjectType::Source.as_str(), source_id)
            })?;
        to_update_objs.push(PbObject {
            object_info: Some(PbObjectInfo::Source(
                ObjectModel(source, obj.unwrap()).into(),
            )),
        });

        if let Some(associate_table_id) = associate_table_id {
            let (table, obj) = Table::find_by_id(associate_table_id)
                .find_also_related(Object)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", associate_table_id))?;
            to_update_objs.push(PbObject {
                object_info: Some(PbObjectInfo::Table(ObjectModel(table, obj.unwrap()).into())),
            });
        }

        txn.commit().await?;

        self.notify_frontend(
            NotificationOperation::Update,
            NotificationInfo::ObjectGroup(PbObjectGroup {
                objects: to_update_objs,
            }),
        )
        .await;

        Ok(options_with_secret)
    }

    pub async fn update_sink_props_by_sink_id(
        &self,
        sink_id: SinkId,
        props: BTreeMap<String, String>,
    ) -> MetaResult<HashMap<String, String>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let (sink, _obj) = Sink::find_by_id(sink_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(ObjectType::Sink.as_str(), sink_id))?;

        // Validate that props can be altered
        match sink.properties.inner_ref().get(CONNECTOR_TYPE_KEY) {
            Some(connector) => {
                let connector_type = connector.to_lowercase();
                let field_names: Vec<String> = props.keys().cloned().collect();
                check_sink_allow_alter_on_fly_fields(&connector_type, &field_names)
                    .map_err(|e| SinkError::Config(anyhow!(e)))?;

                match_sink_name_str!(
                    connector_type.as_str(),
                    SinkType,
                    {
                        let mut new_props = sink.properties.0.clone();
                        new_props.extend(props.clone());
                        SinkType::validate_alter_config(&new_props)
                    },
                    |sink: &str| Err(SinkError::Config(anyhow!("unsupported sink type {}", sink)))
                )?
            }
            None => {
                return Err(
                    SinkError::Config(anyhow!("connector not specified when alter sink")).into(),
                );
            }
        };
        let definition = sink.definition.clone();
        let [mut stmt]: [_; 1] = Parser::parse_sql(&definition)
            .map_err(|e| SinkError::Config(anyhow!(e)))?
            .try_into()
            .unwrap();
        if let Statement::CreateSink { stmt } = &mut stmt {
            let mut new_sql_options = stmt
                .with_properties
                .0
                .iter()
                .map(|sql_option| (&sql_option.name, sql_option))
                .collect::<IndexMap<_, _>>();
            let add_sql_options = props
                .iter()
                .map(|(k, v)| SqlOption::try_from((k, v)))
                .collect::<Result<Vec<SqlOption>, ParserError>>()
                .map_err(|e| SinkError::Config(anyhow!(e)))?;
            new_sql_options.extend(
                add_sql_options
                    .iter()
                    .map(|sql_option| (&sql_option.name, sql_option)),
            );
            stmt.with_properties.0 = new_sql_options.into_values().cloned().collect();
        } else {
            panic!("sink definition is not a create sink statement")
        }
        let mut new_config = sink.properties.clone().into_inner();
        new_config.extend(props);

        let active_sink = sink::ActiveModel {
            sink_id: Set(sink_id),
            properties: Set(risingwave_meta_model::Property(new_config.clone())),
            definition: Set(stmt.to_string()),
            ..Default::default()
        };
        active_sink.update(&txn).await?;

        let fragments: Vec<(FragmentId, i32, StreamNode)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::StreamNode,
            ])
            .filter(fragment::Column::JobId.eq(sink_id))
            .into_tuple()
            .all(&txn)
            .await?;
        let fragments = fragments
            .into_iter()
            .filter(|(_, fragment_type_mask, _)| {
                FragmentTypeMask::from(*fragment_type_mask).contains(FragmentTypeFlag::Sink)
            })
            .filter_map(|(id, _, stream_node)| {
                let mut stream_node = stream_node.to_protobuf();
                let mut found = false;
                visit_stream_node_mut(&mut stream_node, |node| {
                    if let PbNodeBody::Sink(node) = node
                        && let Some(sink_desc) = &mut node.sink_desc
                        && sink_desc.id == sink_id as u32
                    {
                        sink_desc.properties = new_config.clone();
                        found = true;
                    }
                });
                if found { Some((id, stream_node)) } else { None }
            })
            .collect_vec();
        assert!(
            !fragments.is_empty(),
            "sink id should be used by at least one fragment"
        );
        for (id, stream_node) in fragments {
            fragment::ActiveModel {
                fragment_id: Set(id),
                stream_node: Set(StreamNode::from(&stream_node)),
                ..Default::default()
            }
            .update(&txn)
            .await?;
        }

        let (sink, obj) = Sink::find_by_id(sink_id)
            .find_also_related(Object)
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(ObjectType::Sink.as_str(), sink_id))?;

        txn.commit().await?;

        let relation_info = PbObjectInfo::Sink(ObjectModel(sink, obj.unwrap()).into());
        let _version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::ObjectGroup(PbObjectGroup {
                    objects: vec![PbObject {
                        object_info: Some(relation_info),
                    }],
                }),
            )
            .await;

        Ok(new_config.into_iter().collect())
    }

    pub async fn update_fragment_rate_limit_by_fragment_id(
        &self,
        fragment_id: FragmentId,
        rate_limit: Option<u32>,
    ) -> MetaResult<HashMap<FragmentId, Vec<ActorId>>> {
        let update_rate_limit = |fragment_type_mask: FragmentTypeMask,
                                 stream_node: &mut PbStreamNode| {
            let mut found = false;
            if fragment_type_mask.contains_any(
                FragmentTypeFlag::dml_rate_limit_fragments()
                    .chain(FragmentTypeFlag::sink_rate_limit_fragments())
                    .chain(FragmentTypeFlag::backfill_rate_limit_fragments())
                    .chain(FragmentTypeFlag::source_rate_limit_fragments()),
            ) {
                visit_stream_node_mut(stream_node, |node| {
                    if let PbNodeBody::Dml(node) = node {
                        node.rate_limit = rate_limit;
                        found = true;
                    }
                    if let PbNodeBody::Sink(node) = node {
                        node.rate_limit = rate_limit;
                        found = true;
                    }
                    if let PbNodeBody::StreamCdcScan(node) = node {
                        node.rate_limit = rate_limit;
                        found = true;
                    }
                    if let PbNodeBody::StreamScan(node) = node {
                        node.rate_limit = rate_limit;
                        found = true;
                    }
                    if let PbNodeBody::SourceBackfill(node) = node {
                        node.rate_limit = rate_limit;
                        found = true;
                    }
                });
            }
            found
        };
        self.mutate_fragment_by_fragment_id(fragment_id, update_rate_limit, "fragment not found")
            .await
    }

    pub async fn post_apply_reschedules(
        &self,
        reschedules: HashMap<FragmentId, Reschedule>,
        post_updates: &JobReschedulePostUpdates,
    ) -> MetaResult<()> {
        let new_created_actors: HashSet<_> = reschedules
            .values()
            .flat_map(|reschedule| {
                reschedule
                    .added_actors
                    .values()
                    .flatten()
                    .map(|actor_id| *actor_id as ActorId)
            })
            .collect();

        let inner = self.inner.write().await;

        let txn = inner.db.begin().await?;

        for Reschedule {
            removed_actors,
            vnode_bitmap_updates,
            actor_splits,
            newly_created_actors,
            ..
        } in reschedules.into_values()
        {
            // drop removed actors
            Actor::delete_many()
                .filter(
                    actor::Column::ActorId
                        .is_in(removed_actors.iter().map(|id| *id as ActorId).collect_vec()),
                )
                .exec(&txn)
                .await?;

            // add new actors
            for (
                (
                    StreamActor {
                        actor_id,
                        fragment_id,
                        vnode_bitmap,
                        expr_context,
                        ..
                    },
                    _,
                ),
                worker_id,
            ) in newly_created_actors.into_values()
            {
                let splits = actor_splits
                    .get(&actor_id)
                    .map(|splits| splits.iter().map(PbConnectorSplit::from).collect_vec());

                Actor::insert(actor::ActiveModel {
                    actor_id: Set(actor_id as _),
                    fragment_id: Set(fragment_id as _),
                    status: Set(ActorStatus::Running),
                    splits: Set(splits.map(|splits| (&PbConnectorSplits { splits }).into())),
                    worker_id: Set(worker_id),
                    upstream_actor_ids: Set(Default::default()),
                    vnode_bitmap: Set(vnode_bitmap
                        .as_ref()
                        .map(|bitmap| (&bitmap.to_protobuf()).into())),
                    expr_context: Set(expr_context.as_ref().unwrap().into()),
                })
                .exec(&txn)
                .await?;
            }

            // actor update
            for (actor_id, bitmap) in vnode_bitmap_updates {
                let actor = Actor::find_by_id(actor_id as ActorId)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("actor", actor_id))?;

                let mut actor = actor.into_active_model();
                actor.vnode_bitmap = Set(Some((&bitmap.to_protobuf()).into()));
                actor.update(&txn).await?;
            }

            // Update actor_splits for existing actors
            for (actor_id, splits) in actor_splits {
                if new_created_actors.contains(&(actor_id as ActorId)) {
                    continue;
                }

                let actor = Actor::find_by_id(actor_id as ActorId)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("actor", actor_id))?;

                let mut actor = actor.into_active_model();
                let splits = splits.iter().map(PbConnectorSplit::from).collect_vec();
                actor.splits = Set(Some((&PbConnectorSplits { splits }).into()));
                actor.update(&txn).await?;
            }
        }

        let JobReschedulePostUpdates {
            parallelism_updates,
            resource_group_updates,
        } = post_updates;

        for (table_id, parallelism) in parallelism_updates {
            let mut streaming_job = StreamingJobModel::find_by_id(table_id.table_id() as ObjectId)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", table_id))?
                .into_active_model();

            streaming_job.parallelism = Set(match parallelism {
                TableParallelism::Adaptive => StreamingParallelism::Adaptive,
                TableParallelism::Fixed(n) => StreamingParallelism::Fixed(*n as _),
                TableParallelism::Custom => StreamingParallelism::Custom,
            });

            if let Some(resource_group) =
                resource_group_updates.get(&(table_id.table_id() as ObjectId))
            {
                streaming_job.specific_resource_group = Set(resource_group.to_owned());
            }

            streaming_job.update(&txn).await?;
        }

        txn.commit().await?;

        Ok(())
    }

    /// Note: `FsFetch` created in old versions are not included.
    /// Since this is only used for debugging, it should be fine.
    pub async fn list_rate_limits(&self) -> MetaResult<Vec<RateLimitInfo>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragments: Vec<(FragmentId, ObjectId, i32, StreamNode)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::JobId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::StreamNode,
            ])
            .filter(fragment_type_mask_intersects(FragmentTypeFlag::raw_flag(
                FragmentTypeFlag::rate_limit_fragments(),
            ) as _))
            .into_tuple()
            .all(&txn)
            .await?;

        let mut rate_limits = Vec::new();
        for (fragment_id, job_id, fragment_type_mask, stream_node) in fragments {
            let stream_node = stream_node.to_protobuf();
            visit_stream_node_body(&stream_node, |node| {
                let mut rate_limit = None;
                let mut node_name = None;

                match node {
                    // source rate limit
                    PbNodeBody::Source(node) => {
                        if let Some(node_inner) = &node.source_inner {
                            rate_limit = node_inner.rate_limit;
                            node_name = Some("SOURCE");
                        }
                    }
                    PbNodeBody::StreamFsFetch(node) => {
                        if let Some(node_inner) = &node.node_inner {
                            rate_limit = node_inner.rate_limit;
                            node_name = Some("FS_FETCH");
                        }
                    }
                    // backfill rate limit
                    PbNodeBody::SourceBackfill(node) => {
                        rate_limit = node.rate_limit;
                        node_name = Some("SOURCE_BACKFILL");
                    }
                    PbNodeBody::StreamScan(node) => {
                        rate_limit = node.rate_limit;
                        node_name = Some("STREAM_SCAN");
                    }
                    PbNodeBody::StreamCdcScan(node) => {
                        rate_limit = node.rate_limit;
                        node_name = Some("STREAM_CDC_SCAN");
                    }
                    PbNodeBody::Sink(node) => {
                        rate_limit = node.rate_limit;
                        node_name = Some("SINK");
                    }
                    _ => {}
                }

                if let Some(rate_limit) = rate_limit {
                    rate_limits.push(RateLimitInfo {
                        fragment_id: fragment_id as u32,
                        job_id: job_id as u32,
                        fragment_type_mask: fragment_type_mask as u32,
                        rate_limit,
                        node_name: node_name.unwrap().to_owned(),
                    });
                }
            });
        }

        Ok(rate_limits)
    }
}

fn bitflag_intersects(column: SimpleExpr, value: i32) -> SimpleExpr {
    column
        .binary(BinOper::Custom("&"), value)
        .binary(BinOper::NotEqual, 0)
}

fn fragment_type_mask_intersects(value: i32) -> SimpleExpr {
    bitflag_intersects(fragment::Column::FragmentTypeMask.into_simple_expr(), value)
}

pub struct SinkIntoTableContext {
    /// For creating sink into table, this is `Some`, otherwise `None`.
    pub creating_sink_id: Option<SinkId>,
    /// For dropping sink into table, this is `Some`, otherwise `None`.
    pub dropping_sink_id: Option<SinkId>,
    /// For alter table (e.g., add column), this is the list of existing sink ids
    /// otherwise empty.
    pub updated_sink_catalogs: Vec<SinkId>,
}

pub struct FinishAutoRefreshSchemaSinkContext {
    pub tmp_sink_id: ObjectId,
    pub original_sink_id: ObjectId,
    pub columns: Vec<PbColumnCatalog>,
    pub new_log_store_table: Option<(ObjectId, Vec<PbColumnCatalog>)>,
}

async fn update_connector_props_fragments<F>(
    txn: &DatabaseTransaction,
    job_id: i32,
    expect_flag: FragmentTypeFlag,
    mut alter_stream_node_fn: F,
) -> MetaResult<()>
where
    F: FnMut(&mut PbNodeBody, &mut bool),
{
    let fragments: Vec<(FragmentId, i32, StreamNode)> = Fragment::find()
        .select_only()
        .columns([
            fragment::Column::FragmentId,
            fragment::Column::FragmentTypeMask,
            fragment::Column::StreamNode,
        ])
        .filter(fragment::Column::JobId.eq(job_id))
        .into_tuple()
        .all(txn)
        .await?;
    let fragments = fragments
        .into_iter()
        .filter(|(_, fragment_type_mask, _)| *fragment_type_mask & expect_flag as i32 != 0)
        .filter_map(|(id, _, stream_node)| {
            let mut stream_node = stream_node.to_protobuf();
            let mut found = false;
            visit_stream_node_mut(&mut stream_node, |node| {
                alter_stream_node_fn(node, &mut found);
            });
            if found { Some((id, stream_node)) } else { None }
        })
        .collect_vec();
    assert!(
        !fragments.is_empty(),
        "job {} (type: {:?}) should be used by at least one fragment",
        job_id,
        expect_flag
    );

    for (id, stream_node) in fragments {
        fragment::ActiveModel {
            fragment_id: Set(id),
            stream_node: Set(StreamNode::from(&stream_node)),
            ..Default::default()
        }
        .update(txn)
        .await?;
    }

    Ok(())
}
