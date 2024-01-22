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

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::stream_graph_visitor::visit_stream_node;
use risingwave_meta_model_v2::actor::ActorStatus;
use risingwave_meta_model_v2::object::ObjectType;
use risingwave_meta_model_v2::prelude::{
    Actor, ActorDispatcher, Fragment, Index, Object, ObjectDependency, Source, Table,
};
use risingwave_meta_model_v2::{
    actor, actor_dispatcher, fragment, index, object_dependency, sink, source, streaming_job,
    table, ActorId, ActorUpstreamActors, CreateType, DatabaseId, ExprNodeArray, FragmentId,
    I32Array, IndexId, JobStatus, ObjectId, SchemaId, SourceId, StreamNode, TableId, TableVersion,
    UserId, subscription,
};
use risingwave_pb::catalog::source::PbOptionalAssociatedTableId;
use risingwave_pb::catalog::table::{PbOptionalAssociatedSourceId, PbTableVersion};
use risingwave_pb::catalog::{PbCreateType, PbTable};
use risingwave_pb::meta::relation::PbRelationInfo;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use risingwave_pb::meta::{PbRelation, PbRelationGroup, PbTableFragments};
use risingwave_pb::source::{PbConnectorSplit, PbConnectorSplits};
use risingwave_pb::stream_plan::stream_node::PbNodeBody;
use risingwave_pb::stream_plan::update_mutation::PbMergeUpdate;
use risingwave_pb::stream_plan::{PbDispatcher, PbFragmentTypeFlag};
use sea_orm::sea_query::SimpleExpr;
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveEnum, ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, IntoActiveModel,
    ModelTrait, NotSet, QueryFilter, QuerySelect, TransactionTrait,
};

use crate::controller::catalog::CatalogController;
use crate::controller::rename::ReplaceTableExprRewriter;
use crate::controller::utils::{
    check_relation_name_duplicate, ensure_object_id, ensure_user_id, get_fragment_actor_ids,
    get_fragment_mappings,
};
use crate::controller::ObjectModel;
use crate::manager::{NotificationVersion, SinkId, StreamingJob};
use crate::model::StreamContext;
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
    ) -> MetaResult<ObjectId> {
        let obj = Self::create_object(txn, obj_type, owner_id, database_id, schema_id).await?;
        let job = streaming_job::ActiveModel {
            job_id: Set(obj.oid),
            job_status: Set(JobStatus::Initial),
            create_type: Set(create_type.into()),
            timezone: Set(ctx.timezone.clone()),
        };
        job.insert(txn).await?;

        Ok(obj.oid)
    }

    pub async fn create_job_catalog(
        &self,
        streaming_job: &mut StreamingJob,
        ctx: &StreamContext,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let create_type = streaming_job.create_type();

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
                )
                .await?;
                table.id = job_id as _;
                let table: table::ActiveModel = table.clone().into();
                table.insert(&txn).await?;
            }
            StreamingJob::Sink(sink, _) => {
                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Sink,
                    sink.owner as _,
                    Some(sink.database_id as _),
                    Some(sink.schema_id as _),
                    create_type,
                    ctx,
                )
                .await?;
                sink.id = job_id as _;
                let sink: sink::ActiveModel = sink.clone().into();
                sink.insert(&txn).await?;
            }
            StreamingJob::Subscription(subscription) => {
                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Sink,
                    subscription.owner as _,
                    Some(subscription.database_id as _),
                    Some(subscription.schema_id as _),
                    create_type,
                    ctx,
                )
                .await?;
            subscription.id = job_id as _;
                let subscription: subscription::ActiveModel = subscription.clone().into();
                subscription.insert(&txn).await?;
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
                    source.insert(&txn).await?;
                }
                let table: table::ActiveModel = table.clone().into();
                table.insert(&txn).await?;
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
                )
                .await?;
                // to be compatible with old implementation.
                index.id = job_id as _;
                index.index_table_id = job_id as _;
                table.id = job_id as _;

                object_dependency::ActiveModel {
                    oid: Set(index.primary_table_id as _),
                    used_by: Set(table.id as _),
                    ..Default::default()
                }
                .insert(&txn)
                .await?;

                let table: table::ActiveModel = table.clone().into();
                table.insert(&txn).await?;
                let index: index::ActiveModel = index.clone().into();
                index.insert(&txn).await?;
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
                )
                .await?;
                src.id = job_id as _;
                let source: source::ActiveModel = src.clone().into();
                source.insert(&txn).await?;
            }
        }

        // record object dependency.
        let dependent_relations = streaming_job.dependent_relations();
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
            table.insert(&txn).await?;
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

        // Add fragments, actors and actor dispatchers.
        for (fragment, actors, actor_dispatchers) in fragment_actors {
            let fragment = fragment.into_active_model();
            let fragment = fragment.insert(&txn).await?;
            for actor in actors {
                let actor = actor.into_active_model();
                actor.insert(&txn).await?;
            }
            for (_, actor_dispatchers) in actor_dispatchers {
                for actor_dispatcher in actor_dispatchers {
                    let mut actor_dispatcher = actor_dispatcher.into_active_model();
                    actor_dispatcher.id = NotSet;
                    actor_dispatcher.insert(&txn).await?;
                }
            }
            // Update fragment id for all state tables.
            if !for_replace {
                for state_table_id in fragment.state_table_ids.into_inner() {
                    table::ActiveModel {
                        table_id: Set(state_table_id as _),
                        fragment_id: Set(Some(fragment.fragment_id as _)),
                        ..Default::default()
                    }
                    .update(&txn)
                    .await?;
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
    pub async fn try_abort_creating_streaming_job(&self, job_id: ObjectId) -> MetaResult<bool> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        let streaming_job = streaming_job::Entity::find_by_id(job_id).one(&txn).await?;
        let Some(streaming_job) = streaming_job else {
            tracing::warn!(
                id = job_id,
                "streaming job not found when aborting creating, might be cleaned by recovery"
            );
            return Ok(true);
        };

        assert_ne!(streaming_job.job_status, JobStatus::Created);
        if streaming_job.create_type == CreateType::Background
            && streaming_job.job_status == JobStatus::Creating
        {
            // If the job is created in background and still in creating status, we should not abort it and let recovery to handle it.
            tracing::warn!(
                id = job_id,
                "streaming job is created in background and still in creating status"
            );
            return Ok(false);
        }

        Object::delete_by_id(job_id).exec(&txn).await?;
        txn.commit().await?;

        Ok(true)
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
                let connector_splits = PbConnectorSplits { splits };
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
        if version.version != original_version.inner_ref().version + 1 {
            return Err(MetaError::permission_denied("table version is stale"));
        }

        // 2. create streaming object for new replace table.
        let obj_id = Self::create_streaming_job_obj(
            &txn,
            ObjectType::Table,
            streaming_job.owner() as _,
            Some(streaming_job.database_id() as _),
            Some(streaming_job.schema_id() as _),
            PbCreateType::Foreground,
            ctx,
        )
        .await?;

        // 3. record dependency for new replace table.
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

    pub async fn finish_replace_streaming_job(
        &self,
        dummy_id: ObjectId,
        streaming_job: StreamingJob,
        merge_updates: Vec<PbMergeUpdate>,
        table_col_index_mapping: Option<ColIndexMapping>,
        _creating_sink_id: Option<SinkId>,
        _dropping_sink_id: Option<SinkId>,
    ) -> MetaResult<NotificationVersion> {
        // Question: The source catalog should be remain unchanged?
        let StreamingJob::Table(_, table, ..) = streaming_job else {
            unreachable!("unexpected job: {streaming_job:?}")
        };

        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        let job_id = table.id as ObjectId;

        let table = table::ActiveModel::from(table).update(&txn).await?;

        // 1. replace old fragments/actors with new ones.
        Fragment::delete_many()
            .filter(fragment::Column::JobId.eq(job_id))
            .exec(&txn)
            .await?;
        Fragment::update_many()
            .col_expr(fragment::Column::JobId, SimpleExpr::from(job_id))
            .filter(fragment::Column::JobId.eq(dummy_id))
            .exec(&txn)
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
                    .one(&txn)
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
            .update(&txn)
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
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("fragment", fragment_id))?;
            visit_stream_node(&mut stream_node.0, |body| {
                if let PbNodeBody::Merge(m) = body
                    && let Some((new_fragment_id, new_actor_ids)) =
                        fragment_replace_map.get(&m.upstream_fragment_id)
                {
                    m.upstream_fragment_id = *new_fragment_id;
                    m.upstream_actor_id = new_actor_ids.clone();
                }
            });
            for fragment_id in &mut upstream_fragment_id.0 {
                if let Some((new_fragment_id, _)) = fragment_replace_map.get(&(*fragment_id as _)) {
                    *fragment_id = *new_fragment_id as _;
                }
            }
            fragment::ActiveModel {
                fragment_id: Set(fragment_id),
                stream_node: Set(stream_node),
                upstream_fragment_id: Set(upstream_fragment_id),
                ..Default::default()
            }
            .update(&txn)
            .await?;
        }

        // 3. remove dummy object.
        Object::delete_by_id(dummy_id).exec(&txn).await?;

        // 4. update catalogs and notify.
        let mut relations = vec![];
        let table_obj = table
            .find_related(Object)
            .one(&txn)
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
                .all(&txn)
                .await?;
            for (index_id, mut nodes) in index_items {
                nodes
                    .0
                    .iter_mut()
                    .for_each(|x| expr_rewriter.rewrite_expr(x));
                let index = index::ActiveModel {
                    index_id: Set(index_id),
                    index_items: Set(nodes),
                    ..Default::default()
                }
                .update(&txn)
                .await?;
                let index_obj = index
                    .find_related(Object)
                    .one(&txn)
                    .await?
                    .ok_or_else(|| MetaError::catalog_id_not_found("object", index.index_id))?;
                relations.push(PbRelation {
                    relation_info: Some(PbRelationInfo::Index(
                        ObjectModel(index, index_obj).into(),
                    )),
                });
            }
        }
        let fragment_mapping = get_fragment_mappings(&txn, job_id).await?;

        txn.commit().await?;

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
                && source_info.inner_ref().cdc_source_job
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

        let mut fragments: Vec<(FragmentId, i32, StreamNode)> = Fragment::find()
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

        fragments.retain_mut(|(_, fragment_type_mask, stream_node)| {
            let mut found = false;
            if *fragment_type_mask & PbFragmentTypeFlag::Source as i32 != 0 {
                visit_stream_node(&mut stream_node.0, |node| {
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
                stream_node: Set(stream_node),
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

        let mut fragments: Vec<(FragmentId, i32, StreamNode)> = Fragment::find()
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

        fragments.retain_mut(|(_, fragment_type_mask, stream_node)| {
            let mut found = false;
            if *fragment_type_mask & PbFragmentTypeFlag::StreamScan as i32 != 0 {
                visit_stream_node(&mut stream_node.0, |node| {
                    if let PbNodeBody::StreamScan(node) = node {
                        node.rate_limit = rate_limit;
                        found = true;
                    }
                });
            }
            found
        });

        if fragments.is_empty() {
            return Err(MetaError::invalid_parameter(format!(
                "stream scan node not found in job id {job_id}"
            )));
        }
        let fragment_ids = fragments.iter().map(|(id, _, _)| *id).collect_vec();
        for (id, _, stream_node) in fragments {
            fragment::ActiveModel {
                fragment_id: Set(id),
                stream_node: Set(stream_node),
                ..Default::default()
            }
            .update(&txn)
            .await?;
        }
        let fragment_actors = get_fragment_actor_ids(&txn, fragment_ids).await?;

        txn.commit().await?;

        Ok(fragment_actors)
    }
}
