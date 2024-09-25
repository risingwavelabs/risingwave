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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::num::NonZeroUsize;

use itertools::Itertools;
use risingwave_common::buffer::Bitmap;
use risingwave_common::hash::{ActorMapping, ParallelUnitId, ParallelUnitMapping};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::stream_graph_visitor::visit_stream_node;
use risingwave_common::{bail, current_cluster_version};
use risingwave_meta_model_v2::actor::ActorStatus;
use risingwave_meta_model_v2::actor_dispatcher::DispatcherType;
use risingwave_meta_model_v2::object::ObjectType;
use risingwave_meta_model_v2::prelude::{
    Actor, ActorDispatcher, Fragment, Index, Object, ObjectDependency, Sink, Source,
    StreamingJob as StreamingJobModel, Table,
};
use risingwave_meta_model_v2::{
    actor, actor_dispatcher, fragment, index, object, object_dependency, sink, source,
    streaming_job, table, ActorId, ActorUpstreamActors, CreateType, DatabaseId, ExprNodeArray,
    FragmentId, I32Array, IndexId, JobStatus, ObjectId, SchemaId, SourceId, StreamNode,
    StreamingParallelism, TableId, TableVersion, UserId,
};
use risingwave_pb::catalog::source::PbOptionalAssociatedTableId;
use risingwave_pb::catalog::table::{PbOptionalAssociatedSourceId, PbTableVersion};
use risingwave_pb::catalog::{PbCreateType, PbTable};
use risingwave_pb::meta::relation::PbRelationInfo;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation, Operation,
};
use risingwave_pb::meta::table_fragments::PbActorStatus;
use risingwave_pb::meta::{
    FragmentWorkerSlotMapping, PbFragmentWorkerSlotMapping, PbRelation, PbRelationGroup,
    PbTableFragments, Relation,
};
use risingwave_pb::source::{PbConnectorSplit, PbConnectorSplits};
use risingwave_pb::stream_plan::stream_fragment_graph::Parallelism;
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::update_mutation::{MergeUpdate, PbMergeUpdate};
use risingwave_pb::stream_plan::{
    PbDispatcher, PbDispatcherType, PbFragmentTypeFlag, PbStreamActor,
};
use sea_orm::sea_query::{Expr, Query, SimpleExpr};
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveEnum, ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, IntoActiveModel,
    JoinType, ModelTrait, NotSet, PaginatorTrait, QueryFilter, QuerySelect, RelationTrait,
    TransactionTrait,
};

use crate::barrier::Reschedule;
use crate::controller::catalog::CatalogController;
use crate::controller::rename::ReplaceTableExprRewriter;
use crate::controller::utils::{
    check_relation_name_duplicate, check_sink_into_table_cycle, ensure_object_id, ensure_user_id,
    get_fragment_actor_ids, get_fragment_mappings, get_parallel_unit_to_worker_map,
};
use crate::controller::ObjectModel;
use crate::manager::{NotificationVersion, SinkId, StreamingJob};
use crate::model::{StreamContext, TableParallelism};
use crate::stream::SplitAssignment;
use crate::{MetaError, MetaResult};

impl CatalogController {
    pub async fn create_streaming_job_obj(
        txn: &DatabaseTransaction,
        obj_type: ObjectType,
        owner_id: UserId,
        database_id: Option<DatabaseId>,
        schema_id: Option<SchemaId>,
        create_type: PbCreateType,
        ctx: &StreamContext,
        streaming_parallelism: StreamingParallelism,
    ) -> MetaResult<ObjectId> {
        let obj = Self::create_object(txn, obj_type, owner_id, database_id, schema_id).await?;
        let job = streaming_job::ActiveModel {
            job_id: Set(obj.oid),
            job_status: Set(JobStatus::Initial),
            create_type: Set(create_type.into()),
            timezone: Set(ctx.timezone.clone()),
            parallelism: Set(streaming_parallelism),
        };
        job.insert(txn).await?;

        Ok(obj.oid)
    }

    pub async fn create_job_catalog(
        &self,
        streaming_job: &mut StreamingJob,
        ctx: &StreamContext,
        parallelism: &Option<Parallelism>,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let create_type = streaming_job.create_type();

        let streaming_parallelism = match parallelism {
            None => StreamingParallelism::Adaptive,
            Some(n) => StreamingParallelism::Fixed(n.parallelism as _),
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

        // check if any dependent relation is in altering status.
        let dependent_relations = streaming_job.dependent_relations();
        if !dependent_relations.is_empty() {
            let altering_cnt = ObjectDependency::find()
                .join(
                    JoinType::InnerJoin,
                    object_dependency::Relation::Object1.def(),
                )
                .join(JoinType::InnerJoin, object::Relation::StreamingJob.def())
                .filter(
                    object_dependency::Column::Oid
                        .is_in(dependent_relations.iter().map(|id| *id as ObjectId))
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
                    ctx,
                    streaming_parallelism,
                )
                .await?;
                table.id = job_id as _;
                let table: table::ActiveModel = table.clone().into();
                Table::insert(table).exec(&txn).await?;
            }
            StreamingJob::Sink(sink, _) => {
                if let Some(target_table_id) = sink.target_table {
                    if check_sink_into_table_cycle(
                        target_table_id as ObjectId,
                        sink.dependent_relations
                            .iter()
                            .map(|id| *id as ObjectId)
                            .collect(),
                        &txn,
                    )
                    .await?
                    {
                        bail!("Creating such a sink will result in circular dependency.");
                    }
                }

                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Sink,
                    sink.owner as _,
                    Some(sink.database_id as _),
                    Some(sink.schema_id as _),
                    create_type,
                    ctx,
                    streaming_parallelism,
                )
                .await?;
                sink.id = job_id as _;
                let sink: sink::ActiveModel = sink.clone().into();
                Sink::insert(sink).exec(&txn).await?;
            }
            StreamingJob::Table(src, table, _) => {
                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Table,
                    table.owner as _,
                    Some(table.database_id as _),
                    Some(table.schema_id as _),
                    create_type,
                    ctx,
                    streaming_parallelism,
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
                let table: table::ActiveModel = table.clone().into();
                Table::insert(table).exec(&txn).await?;
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
                    ctx,
                    streaming_parallelism,
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

                let table: table::ActiveModel = table.clone().into();
                Table::insert(table).exec(&txn).await?;
                let index: index::ActiveModel = index.clone().into();
                Index::insert(index).exec(&txn).await?;
            }
            StreamingJob::Source(src) => {
                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Source,
                    src.owner as _,
                    Some(src.database_id as _),
                    Some(src.schema_id as _),
                    create_type,
                    ctx,
                    streaming_parallelism,
                )
                .await?;
                src.id = job_id as _;
                let source: source::ActiveModel = src.clone().into();
                Source::insert(source).exec(&txn).await?;
            }
        }

        // record object dependency.
        if !dependent_relations.is_empty() {
            ObjectDependency::insert_many(dependent_relations.into_iter().map(|id| {
                object_dependency::ActiveModel {
                    oid: Set(id as _),
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

    pub async fn create_internal_table_catalog(
        &self,
        job_id: ObjectId,
        internal_tables: Vec<PbTable>,
    ) -> MetaResult<HashMap<u32, u32>> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let mut table_id_map = HashMap::new();
        for table in internal_tables {
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
            let mut table: table::ActiveModel = table.into();
            table.table_id = Set(table_id as _);
            table.belongs_to_job_id = Set(Some(job_id as _));
            table.fragment_id = NotSet;
            Table::insert(table).exec(&txn).await?;
        }
        txn.commit().await?;

        Ok(table_id_map)
    }

    pub async fn prepare_streaming_job(
        &self,
        table_fragment: PbTableFragments,
        streaming_job: &StreamingJob,
        for_replace: bool,
    ) -> MetaResult<()> {
        let fragment_actors =
            Self::extract_fragment_and_actors_from_table_fragments(table_fragment)?;
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        // Add fragments.
        let (fragments, actor_with_dispatchers): (Vec<_>, Vec<_>) = fragment_actors
            .into_iter()
            .map(|(fragment, actors, actor_dispatchers)| (fragment, (actors, actor_dispatchers)))
            .unzip();
        for fragment in fragments {
            let fragment_id = fragment.fragment_id;
            let state_table_ids = fragment.state_table_ids.inner_ref().clone();
            let fragment = fragment.into_active_model();
            Fragment::insert(fragment).exec(&txn).await?;

            // Update fragment id for all state tables.
            if !for_replace {
                for state_table_id in state_table_ids {
                    table::ActiveModel {
                        table_id: Set(state_table_id as _),
                        fragment_id: Set(Some(fragment_id)),
                        ..Default::default()
                    }
                    .update(&txn)
                    .await?;
                }
            }
        }

        // Add actors and actor dispatchers.
        for (actors, actor_dispatchers) in actor_with_dispatchers {
            for actor in actors {
                let actor = actor.into_active_model();
                Actor::insert(actor).exec(&txn).await?;
            }
            for (_, actor_dispatchers) in actor_dispatchers {
                for actor_dispatcher in actor_dispatchers {
                    let mut actor_dispatcher = actor_dispatcher.into_active_model();
                    actor_dispatcher.id = NotSet;
                    ActorDispatcher::insert(actor_dispatcher).exec(&txn).await?;
                }
            }
        }

        if !for_replace {
            // // Update dml fragment id.
            if let StreamingJob::Table(_, table, ..) = streaming_job {
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

        Ok(())
    }

    /// `try_abort_creating_streaming_job` is used to abort the job that is under initial status or in `FOREGROUND` mode.
    /// It returns true if the job is not found or aborted.
    pub async fn try_abort_creating_streaming_job(
        &self,
        job_id: ObjectId,
        is_cancelled: bool,
    ) -> MetaResult<(bool, Vec<TableId>)> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let cnt = Object::find_by_id(job_id).count(&txn).await?;
        if cnt == 0 {
            tracing::warn!(
                id = job_id,
                "streaming job not found when aborting creating, might be cleaned by recovery"
            );
            return Ok((true, Vec::new()));
        }

        if !is_cancelled {
            let streaming_job = streaming_job::Entity::find_by_id(job_id).one(&txn).await?;
            if let Some(streaming_job) = streaming_job {
                assert_ne!(streaming_job.job_status, JobStatus::Created);
                if streaming_job.create_type == CreateType::Background
                    && streaming_job.job_status == JobStatus::Creating
                {
                    // If the job is created in background and still in creating status, we should not abort it and let recovery to handle it.
                    tracing::warn!(
                        id = job_id,
                        "streaming job is created in background and still in creating status"
                    );
                    return Ok((false, Vec::new()));
                }
            }
        }

        let internal_table_ids: Vec<TableId> = Table::find()
            .select_only()
            .column(table::Column::TableId)
            .filter(table::Column::BelongsToJobId.eq(job_id))
            .into_tuple()
            .all(&txn)
            .await?;

        let mv_table_id: Option<TableId> = Table::find_by_id(job_id)
            .select_only()
            .column(table::Column::TableId)
            .into_tuple()
            .one(&txn)
            .await?;

        let associated_source_id: Option<SourceId> = Table::find_by_id(job_id)
            .select_only()
            .column(table::Column::OptionalAssociatedSourceId)
            .filter(table::Column::OptionalAssociatedSourceId.is_not_null())
            .into_tuple()
            .one(&txn)
            .await?;

        Object::delete_by_id(job_id).exec(&txn).await?;
        if !internal_table_ids.is_empty() {
            Object::delete_many()
                .filter(object::Column::Oid.is_in(internal_table_ids.iter().cloned()))
                .exec(&txn)
                .await?;
        }
        if let Some(source_id) = associated_source_id {
            Object::delete_by_id(source_id).exec(&txn).await?;
        }
        txn.commit().await?;

        let mut state_table_ids = internal_table_ids;

        state_table_ids.extend(mv_table_id.into_iter());

        Ok((true, state_table_ids))
    }

    pub async fn post_collect_table_fragments(
        &self,
        job_id: ObjectId,
        actor_ids: Vec<crate::model::ActorId>,
        new_actor_dispatchers: HashMap<crate::model::ActorId, Vec<PbDispatcher>>,
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

        let mut actor_dispatchers = vec![];
        for (actor_id, dispatchers) in new_actor_dispatchers {
            for dispatcher in dispatchers {
                let mut actor_dispatcher =
                    actor_dispatcher::Model::from((actor_id, dispatcher)).into_active_model();
                actor_dispatcher.id = NotSet;
                actor_dispatchers.push(actor_dispatcher);
            }
        }

        if !actor_dispatchers.is_empty() {
            ActorDispatcher::insert_many(actor_dispatchers)
                .exec(&txn)
                .await?;
        }

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
        ctx: &StreamContext,
        version: &PbTableVersion,
        specified_parallelism: &Option<NonZeroUsize>,
    ) -> MetaResult<ObjectId> {
        let id = streaming_job.id();
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        // 1. check version.
        let original_version: Option<TableVersion> = Table::find_by_id(id as TableId)
            .select_only()
            .column(table::Column::Version)
            .into_tuple()
            .one(&txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found(ObjectType::Table.as_str(), id))?;
        let original_version = original_version.expect("version for table should exist");
        if version.version != original_version.to_protobuf().version + 1 {
            return Err(MetaError::permission_denied("table version is stale"));
        }

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
                "table is being altered or referenced by some creating jobs",
            ));
        }

        let parallelism = match specified_parallelism {
            None => StreamingParallelism::Adaptive,
            Some(n) => StreamingParallelism::Fixed(n.get() as _),
        };

        // 3. create streaming object for new replace table.
        let obj_id = Self::create_streaming_job_obj(
            &txn,
            ObjectType::Table,
            streaming_job.owner() as _,
            Some(streaming_job.database_id() as _),
            Some(streaming_job.schema_id() as _),
            PbCreateType::Foreground,
            ctx,
            parallelism,
        )
        .await?;

        // 4. record dependency for new replace table.
        ObjectDependency::insert(object_dependency::ActiveModel {
            oid: Set(id as _),
            used_by: Set(obj_id as _),
            ..Default::default()
        })
        .exec(&txn)
        .await?;

        txn.commit().await?;

        Ok(obj_id)
    }

    /// `finish_streaming_job` marks job related objects as `Created` and notify frontend.
    pub async fn finish_streaming_job(
        &self,
        job_id: ObjectId,
        replace_table_job_info: Option<(crate::manager::StreamingJob, Vec<MergeUpdate>, u32)>,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let job_type = Object::find_by_id(job_id)
            .select_only()
            .column(object::Column::ObjType)
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
        let mut relations = internal_table_objs
            .iter()
            .map(|(table, obj)| PbRelation {
                relation_info: Some(PbRelationInfo::Table(
                    ObjectModel(table.clone(), obj.clone().unwrap()).into(),
                )),
            })
            .collect_vec();

        match job_type {
            ObjectType::Table => {
                let (table, obj) = Table::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("table", job_id))?;
                if let Some(source_id) = table.optional_associated_source_id {
                    let (src, obj) = Source::find_by_id(source_id)
                        .find_also_related(Object)
                        .one(&txn)
                        .await?
                        .ok_or_else(|| MetaError::catalog_id_not_found("source", source_id))?;
                    relations.push(PbRelation {
                        relation_info: Some(PbRelationInfo::Source(
                            ObjectModel(src, obj.unwrap()).into(),
                        )),
                    });
                }
                relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Table(
                        ObjectModel(table, obj.unwrap()).into(),
                    )),
                });
            }
            ObjectType::Sink => {
                let (sink, obj) = Sink::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("sink", job_id))?;
                relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Sink(
                        ObjectModel(sink, obj.unwrap()).into(),
                    )),
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
                    relations.push(PbRelation {
                        relation_info: Some(PbRelationInfo::Table(
                            ObjectModel(table, obj.unwrap()).into(),
                        )),
                    });
                }
                relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Index(
                        ObjectModel(index, obj.unwrap()).into(),
                    )),
                });
            }
            ObjectType::Source => {
                let (source, obj) = Source::find_by_id(job_id)
                    .find_also_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("source", job_id))?;
                relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Source(
                        ObjectModel(source, obj.unwrap()).into(),
                    )),
                });
            }
            _ => unreachable!("invalid job type: {:?}", job_type),
        }

        let fragment_mapping = get_fragment_mappings(&txn, job_id).await?;

        let replace_table_mapping_update = match replace_table_job_info {
            Some((streaming_job, merge_updates, dummy_id)) => {
                let incoming_sink_id = job_id;

                let (relations, fragment_mapping) = Self::finish_replace_streaming_job_inner(
                    dummy_id as ObjectId,
                    merge_updates,
                    None,
                    Some(incoming_sink_id as _),
                    None,
                    &txn,
                    streaming_job,
                )
                .await?;

                Some((relations, fragment_mapping))
            }
            None => None,
        };

        txn.commit().await?;

        self.notify_fragment_mapping(NotificationOperation::Add, fragment_mapping)
            .await;

        let mut version = self
            .notify_frontend(
                NotificationOperation::Add,
                NotificationInfo::RelationGroup(PbRelationGroup { relations }),
            )
            .await;

        if let Some((relations, fragment_mapping)) = replace_table_mapping_update {
            self.notify_fragment_mapping(NotificationOperation::Add, fragment_mapping)
                .await;
            version = self
                .notify_frontend(
                    NotificationOperation::Update,
                    NotificationInfo::RelationGroup(PbRelationGroup { relations }),
                )
                .await;
        }

        Ok(version)
    }

    pub async fn finish_replace_streaming_job(
        &self,
        dummy_id: ObjectId,
        streaming_job: StreamingJob,
        merge_updates: Vec<PbMergeUpdate>,
        table_col_index_mapping: Option<ColIndexMapping>,
        creating_sink_id: Option<SinkId>,
        dropping_sink_id: Option<SinkId>,
    ) -> MetaResult<NotificationVersion> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let (relations, fragment_mapping) = Self::finish_replace_streaming_job_inner(
            dummy_id,
            merge_updates,
            table_col_index_mapping,
            creating_sink_id,
            dropping_sink_id,
            &txn,
            streaming_job,
        )
        .await?;

        txn.commit().await?;

        // FIXME: Do not notify frontend currently, because frontend nodes might refer to old table
        // catalog and need to access the old fragment. Let frontend nodes delete the old fragment
        // when they receive table catalog change.
        // self.notify_fragment_mapping(NotificationOperation::Delete, old_fragment_mappings)
        //     .await;
        self.notify_fragment_mapping(NotificationOperation::Add, fragment_mapping)
            .await;
        let version = self
            .notify_frontend(
                NotificationOperation::Update,
                NotificationInfo::RelationGroup(PbRelationGroup { relations }),
            )
            .await;

        Ok(version)
    }

    pub async fn finish_replace_streaming_job_inner(
        dummy_id: ObjectId,
        merge_updates: Vec<PbMergeUpdate>,
        table_col_index_mapping: Option<ColIndexMapping>,
        creating_sink_id: Option<SinkId>,
        dropping_sink_id: Option<SinkId>,
        txn: &DatabaseTransaction,
        streaming_job: StreamingJob,
    ) -> MetaResult<(Vec<Relation>, Vec<PbFragmentWorkerSlotMapping>)> {
        // Question: The source catalog should be remain unchanged?
        let StreamingJob::Table(_, table, ..) = streaming_job else {
            unreachable!("unexpected job: {streaming_job:?}")
        };

        let job_id = table.id as ObjectId;

        let mut table = table::ActiveModel::from(table);
        let mut incoming_sinks = table.incoming_sinks.as_ref().inner_ref().clone();
        if let Some(sink_id) = creating_sink_id {
            debug_assert!(!incoming_sinks.contains(&(sink_id as i32)));
            incoming_sinks.push(sink_id as _);
        }

        if let Some(sink_id) = dropping_sink_id {
            let drained = incoming_sinks
                .extract_if(|id| *id == sink_id as i32)
                .collect_vec();
            debug_assert_eq!(drained, vec![sink_id as i32]);
        }

        table.incoming_sinks = Set(incoming_sinks.into());
        let table = table.update(txn).await?;

        // Update state table fragment id.
        let fragment_table_ids: Vec<(FragmentId, I32Array)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::StateTableIds,
            ])
            .filter(fragment::Column::JobId.eq(dummy_id))
            .into_tuple()
            .all(txn)
            .await?;
        for (fragment_id, state_table_ids) in fragment_table_ids {
            for state_table_id in state_table_ids.into_inner() {
                table::ActiveModel {
                    table_id: Set(state_table_id as _),
                    fragment_id: Set(Some(fragment_id)),
                    ..Default::default()
                }
                .update(txn)
                .await?;
            }
        }

        // let old_fragment_mappings = get_fragment_mappings(&txn, job_id).await?;
        // 1. replace old fragments/actors with new ones.
        Fragment::delete_many()
            .filter(fragment::Column::JobId.eq(job_id))
            .exec(txn)
            .await?;
        Fragment::update_many()
            .col_expr(fragment::Column::JobId, SimpleExpr::from(job_id))
            .filter(fragment::Column::JobId.eq(dummy_id))
            .exec(txn)
            .await?;

        // 2. update merges.
        let fragment_replace_map: HashMap<_, _> = merge_updates
            .iter()
            .map(|update| {
                (
                    update.upstream_fragment_id,
                    (
                        update.new_upstream_fragment_id.unwrap(),
                        update.added_upstream_actor_id.clone(),
                    ),
                )
            })
            .collect();

        // TODO: remove cache upstream fragment/actor ids and derive them from `actor_dispatcher` table.
        let mut to_update_fragment_ids = HashSet::new();
        for merge_update in merge_updates {
            assert!(merge_update.removed_upstream_actor_id.is_empty());
            assert!(merge_update.new_upstream_fragment_id.is_some());
            let (actor_id, fragment_id, mut upstream_actors) =
                Actor::find_by_id(merge_update.actor_id as ActorId)
                    .select_only()
                    .columns([
                        actor::Column::ActorId,
                        actor::Column::FragmentId,
                        actor::Column::UpstreamActorIds,
                    ])
                    .into_tuple::<(ActorId, FragmentId, ActorUpstreamActors)>()
                    .one(txn)
                    .await?
                    .ok_or_else(|| {
                        MetaError::catalog_id_not_found("actor", merge_update.actor_id)
                    })?;

            assert!(upstream_actors
                .0
                .remove(&(merge_update.upstream_fragment_id as FragmentId))
                .is_some());
            upstream_actors.0.insert(
                merge_update.new_upstream_fragment_id.unwrap() as _,
                merge_update
                    .added_upstream_actor_id
                    .iter()
                    .map(|id| *id as _)
                    .collect(),
            );
            actor::ActiveModel {
                actor_id: Set(actor_id),
                upstream_actor_ids: Set(upstream_actors),
                ..Default::default()
            }
            .update(txn)
            .await?;

            to_update_fragment_ids.insert(fragment_id);
        }
        for fragment_id in to_update_fragment_ids {
            let (fragment_id, mut stream_node, mut upstream_fragment_id) =
                Fragment::find_by_id(fragment_id)
                    .select_only()
                    .columns([
                        fragment::Column::FragmentId,
                        fragment::Column::StreamNode,
                        fragment::Column::UpstreamFragmentId,
                    ])
                    .into_tuple::<(FragmentId, StreamNode, I32Array)>()
                    .one(txn)
                    .await?
                    .map(|(id, node, upstream)| (id, node.to_protobuf(), upstream))
                    .ok_or_else(|| MetaError::catalog_id_not_found("fragment", fragment_id))?;
            visit_stream_node(&mut stream_node, |body| {
                if let PbNodeBody::Merge(m) = body
                    && let Some((new_fragment_id, new_actor_ids)) =
                        fragment_replace_map.get(&m.upstream_fragment_id)
                {
                    m.upstream_fragment_id = *new_fragment_id;
                    m.upstream_actor_id.clone_from(new_actor_ids);
                }
            });
            for fragment_id in &mut upstream_fragment_id.0 {
                if let Some((new_fragment_id, _)) = fragment_replace_map.get(&(*fragment_id as _)) {
                    *fragment_id = *new_fragment_id as _;
                }
            }
            fragment::ActiveModel {
                fragment_id: Set(fragment_id),
                stream_node: Set(StreamNode::from(&stream_node)),
                upstream_fragment_id: Set(upstream_fragment_id),
                ..Default::default()
            }
            .update(txn)
            .await?;
        }

        // 3. remove dummy object.
        Object::delete_by_id(dummy_id).exec(txn).await?;

        // 4. update catalogs and notify.
        let mut relations = vec![];
        let table_obj = table
            .find_related(Object)
            .one(txn)
            .await?
            .ok_or_else(|| MetaError::catalog_id_not_found("object", table.table_id))?;
        relations.push(PbRelation {
            relation_info: Some(PbRelationInfo::Table(ObjectModel(table, table_obj).into())),
        });
        if let Some(table_col_index_mapping) = table_col_index_mapping {
            let expr_rewriter = ReplaceTableExprRewriter {
                table_col_index_mapping,
            };

            let index_items: Vec<(IndexId, ExprNodeArray)> = Index::find()
                .select_only()
                .columns([index::Column::IndexId, index::Column::IndexItems])
                .filter(index::Column::PrimaryTableId.eq(job_id))
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
                relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Index(
                        ObjectModel(index, index_obj).into(),
                    )),
                });
            }
        }

        let fragment_mapping: Vec<_> = get_fragment_mappings(txn, job_id as _).await?;

        Ok((relations, fragment_mapping))
    }

    /// `try_abort_replacing_streaming_job` is used to abort the replacing streaming job, the input `job_id` is the dummy job id.
    pub async fn try_abort_replacing_streaming_job(&self, job_id: ObjectId) -> MetaResult<()> {
        let inner = self.inner.write().await;
        Object::delete_by_id(job_id).exec(&inner.db).await?;
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

        let source = Source::find_by_id(source_id)
            .one(&txn)
            .await?
            .ok_or_else(|| {
                MetaError::catalog_id_not_found(ObjectType::Source.as_str(), source_id)
            })?;
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
            .map(|(id, mask, stream_node)| (id, mask, stream_node.to_protobuf()))
            .collect_vec();

        // TODO: limit source backfill?
        fragments.retain_mut(|(_, fragment_type_mask, stream_node)| {
            let mut found = false;
            if *fragment_type_mask & PbFragmentTypeFlag::Source as i32 != 0 {
                visit_stream_node(stream_node, |node| {
                    if let PbNodeBody::Source(node) = node {
                        if let Some(node_inner) = &mut node.source_inner
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

    // edit the `rate_limit` of the `Chain` node in given `table_id`'s fragments
    // return the actor_ids to be applied
    pub async fn update_mv_rate_limit_by_job_id(
        &self,
        job_id: ObjectId,
        rate_limit: Option<u32>,
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
            .map(|(id, mask, stream_node)| (id, mask, stream_node.to_protobuf()))
            .collect_vec();

        fragments.retain_mut(|(_, fragment_type_mask, stream_node)| {
            let mut found = false;
            if (*fragment_type_mask & PbFragmentTypeFlag::StreamScan as i32 != 0)
                || (*fragment_type_mask & PbFragmentTypeFlag::Source as i32 != 0)
            {
                visit_stream_node(stream_node, |node| match node {
                    PbNodeBody::StreamScan(node) => {
                        node.rate_limit = rate_limit;
                        found = true;
                    }
                    PbNodeBody::Source(node) => {
                        if let Some(inner) = node.source_inner.as_mut() {
                            inner.rate_limit = rate_limit;
                            found = true;
                        }
                    }
                    _ => {}
                });
            }
            found
        });

        if fragments.is_empty() {
            return Err(MetaError::invalid_parameter(format!(
                "stream scan node or source node not found in job id {job_id}"
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

    pub async fn post_apply_reschedules(
        &self,
        reschedules: HashMap<FragmentId, Reschedule>,
        table_parallelism_assignment: HashMap<
            risingwave_common::catalog::TableId,
            TableParallelism,
        >,
    ) -> MetaResult<()> {
        fn update_actors(
            actors: &mut Vec<ActorId>,
            to_remove: &HashSet<ActorId>,
            to_create: &Vec<ActorId>,
        ) {
            let actor_id_set: HashSet<_> = actors.iter().copied().collect();
            for actor_id in to_create {
                debug_assert!(!actor_id_set.contains(actor_id));
            }
            for actor_id in to_remove {
                debug_assert!(actor_id_set.contains(actor_id));
            }

            actors.retain(|actor_id| !to_remove.contains(actor_id));
            actors.extend_from_slice(to_create);
        }

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

        let parallel_unit_to_worker = get_parallel_unit_to_worker_map(&txn).await?;

        let mut fragment_mapping_to_notify = vec![];

        // for assert only
        let mut assert_dispatcher_update_checker = HashSet::new();

        for (
            fragment_id,
            Reschedule {
                added_actors,
                removed_actors,
                vnode_bitmap_updates,
                actor_splits,
                injectable: _,
                newly_created_actors,
                upstream_fragment_dispatcher_ids,
                upstream_dispatcher_mapping,
                downstream_fragment_ids,
            },
        ) in reschedules
        {
            // drop removed actors
            Actor::delete_many()
                .filter(
                    actor::Column::ActorId
                        .is_in(removed_actors.iter().map(|id| *id as ActorId).collect_vec()),
                )
                .exec(&txn)
                .await?;

            for (
                PbStreamActor {
                    actor_id,
                    fragment_id,
                    mut nodes,
                    dispatcher,
                    upstream_actor_id,
                    vnode_bitmap,
                    expr_context,
                    ..
                },
                // actor_status
                PbActorStatus {
                    parallel_unit,
                    state: _,
                },
            ) in newly_created_actors
            {
                let mut actor_upstreams = BTreeMap::<FragmentId, BTreeSet<ActorId>>::new();
                let mut new_actor_dispatchers = vec![];

                if let Some(nodes) = &mut nodes {
                    visit_stream_node(nodes, |node| {
                        if let PbNodeBody::Merge(node) = node {
                            actor_upstreams
                                .entry(node.upstream_fragment_id as FragmentId)
                                .or_default()
                                .extend(node.upstream_actor_id.iter().map(|id| *id as ActorId));
                        }
                    });
                }

                let actor_upstreams: BTreeMap<FragmentId, Vec<ActorId>> = actor_upstreams
                    .into_iter()
                    .map(|(k, v)| (k, v.into_iter().collect()))
                    .collect();

                debug_assert_eq!(
                    actor_upstreams
                        .values()
                        .flatten()
                        .cloned()
                        .sorted()
                        .collect_vec(),
                    upstream_actor_id
                        .iter()
                        .map(|actor_id| *actor_id as i32)
                        .sorted()
                        .collect_vec()
                );

                let actor_upstreams = ActorUpstreamActors(actor_upstreams);
                let parallel_unit = parallel_unit.unwrap();

                let splits = actor_splits
                    .get(&actor_id)
                    .map(|splits| splits.iter().map(PbConnectorSplit::from).collect_vec());

                Actor::insert(actor::ActiveModel {
                    actor_id: Set(actor_id as _),
                    fragment_id: Set(fragment_id as _),
                    status: Set(ActorStatus::Running),
                    splits: Set(splits.map(|splits| (&PbConnectorSplits { splits }).into())),
                    parallel_unit_id: Set(parallel_unit.id as _),
                    worker_id: Set(parallel_unit.worker_node_id as _),
                    upstream_actor_ids: Set(actor_upstreams),
                    vnode_bitmap: Set(vnode_bitmap.as_ref().map(|bitmap| bitmap.into())),
                    expr_context: Set(expr_context.as_ref().unwrap().into()),
                })
                .exec(&txn)
                .await?;

                for PbDispatcher {
                    r#type: dispatcher_type,
                    dist_key_indices,
                    output_indices,
                    hash_mapping,
                    dispatcher_id,
                    downstream_actor_id,
                } in dispatcher
                {
                    new_actor_dispatchers.push(actor_dispatcher::ActiveModel {
                        id: Default::default(),
                        actor_id: Set(actor_id as _),
                        dispatcher_type: Set(PbDispatcherType::try_from(dispatcher_type)
                            .unwrap()
                            .into()),
                        dist_key_indices: Set(dist_key_indices.into()),
                        output_indices: Set(output_indices.into()),
                        hash_mapping: Set(hash_mapping.as_ref().map(|mapping| mapping.into())),
                        dispatcher_id: Set(dispatcher_id as _),
                        downstream_actor_ids: Set(downstream_actor_id.into()),
                    })
                }
                if !new_actor_dispatchers.is_empty() {
                    ActorDispatcher::insert_many(new_actor_dispatchers)
                        .exec(&txn)
                        .await?;
                }
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

            // fragment update
            let fragment = Fragment::find_by_id(fragment_id)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("fragment", fragment_id))?;

            let fragment_actors = fragment.find_related(Actor).all(&txn).await?;

            let mut actor_to_parallel_unit = HashMap::with_capacity(fragment_actors.len());
            let mut actor_to_vnode_bitmap = HashMap::with_capacity(fragment_actors.len());
            for actor in &fragment_actors {
                actor_to_parallel_unit.insert(actor.actor_id as u32, actor.parallel_unit_id as _);
                if let Some(vnode_bitmap) = &actor.vnode_bitmap {
                    let bitmap = Bitmap::from(&vnode_bitmap.to_protobuf());
                    actor_to_vnode_bitmap.insert(actor.actor_id as u32, bitmap);
                }
            }

            let vnode_mapping = if actor_to_vnode_bitmap.is_empty() {
                let parallel_unit = *actor_to_parallel_unit.values().exactly_one().unwrap();
                ParallelUnitMapping::new_single(parallel_unit as ParallelUnitId)
            } else {
                // Generate the parallel unit mapping from the fragment's actor bitmaps.
                assert_eq!(actor_to_vnode_bitmap.len(), actor_to_parallel_unit.len());
                ActorMapping::from_bitmaps(&actor_to_vnode_bitmap)
                    .to_parallel_unit(&actor_to_parallel_unit)
            }
            .to_protobuf();

            let mut fragment = fragment.into_active_model();
            fragment.vnode_mapping = Set((&vnode_mapping).into());
            fragment.update(&txn).await?;

            let worker_slot_mapping = ParallelUnitMapping::from_protobuf(&vnode_mapping)
                .to_worker_slot(&parallel_unit_to_worker)?
                .to_protobuf();

            fragment_mapping_to_notify.push(FragmentWorkerSlotMapping {
                fragment_id: fragment_id as u32,
                mapping: Some(worker_slot_mapping),
            });

            // for downstream and upstream
            let removed_actor_ids: HashSet<_> = removed_actors
                .iter()
                .map(|actor_id| *actor_id as ActorId)
                .collect();

            let added_actor_ids = added_actors
                .values()
                .flatten()
                .map(|actor_id| *actor_id as ActorId)
                .collect_vec();

            // first step, upstream fragment
            for (upstream_fragment_id, dispatcher_id) in upstream_fragment_dispatcher_ids {
                let upstream_fragment = Fragment::find_by_id(upstream_fragment_id as FragmentId)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("fragment", fragment_id))?;

                let all_dispatchers = actor_dispatcher::Entity::find()
                    .join(JoinType::InnerJoin, actor_dispatcher::Relation::Actor.def())
                    .filter(actor::Column::FragmentId.eq(upstream_fragment.fragment_id))
                    .filter(actor_dispatcher::Column::DispatcherId.eq(dispatcher_id as i32))
                    .all(&txn)
                    .await?;

                for dispatcher in all_dispatchers {
                    debug_assert!(assert_dispatcher_update_checker.insert(dispatcher.id));
                    if new_created_actors.contains(&dispatcher.actor_id) {
                        continue;
                    }

                    let mut dispatcher = dispatcher.into_active_model();

                    // Only hash dispatcher needs mapping
                    if dispatcher.dispatcher_type.as_ref() == &DispatcherType::Hash {
                        dispatcher.hash_mapping =
                            Set(upstream_dispatcher_mapping.as_ref().map(|m| {
                                risingwave_meta_model_v2::ActorMapping::from(&m.to_protobuf())
                            }));
                    }

                    let mut new_downstream_actor_ids =
                        dispatcher.downstream_actor_ids.as_ref().inner_ref().clone();

                    update_actors(
                        new_downstream_actor_ids.as_mut(),
                        &removed_actor_ids,
                        &added_actor_ids,
                    );

                    dispatcher.downstream_actor_ids = Set(new_downstream_actor_ids.into());
                    dispatcher.update(&txn).await?;
                }
            }

            // second step, downstream fragment
            for downstream_fragment_id in downstream_fragment_ids {
                let actors = Actor::find()
                    .filter(actor::Column::FragmentId.eq(downstream_fragment_id as FragmentId))
                    .all(&txn)
                    .await?;

                for actor in actors {
                    if new_created_actors.contains(&actor.actor_id) {
                        continue;
                    }

                    let mut actor = actor.into_active_model();

                    let mut new_upstream_actor_ids =
                        actor.upstream_actor_ids.as_ref().inner_ref().clone();

                    update_actors(
                        new_upstream_actor_ids.get_mut(&fragment_id).unwrap(),
                        &removed_actor_ids,
                        &added_actor_ids,
                    );

                    actor.upstream_actor_ids = Set(new_upstream_actor_ids.into());

                    actor.update(&txn).await?;
                }
            }
        }

        for (table_id, parallelism) in table_parallelism_assignment {
            let mut streaming_job = StreamingJobModel::find_by_id(table_id.table_id() as ObjectId)
                .one(&txn)
                .await?
                .ok_or_else(|| MetaError::catalog_id_not_found("table", table_id))?
                .into_active_model();

            streaming_job.parallelism = Set(match parallelism {
                TableParallelism::Adaptive => StreamingParallelism::Adaptive,
                TableParallelism::Fixed(n) => StreamingParallelism::Fixed(n as _),
                TableParallelism::Custom => StreamingParallelism::Custom,
            });

            streaming_job.update(&txn).await?;
        }

        txn.commit().await?;
        self.notify_fragment_mapping(Operation::Update, fragment_mapping_to_notify)
            .await;

        Ok(())
    }
}
