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

use std::collections::HashMap;

use itertools::Itertools;
use risingwave_meta_model_v2::actor::ActorStatus;
use risingwave_meta_model_v2::object::ObjectType;
use risingwave_meta_model_v2::prelude::{Actor, ActorDispatcher, Object, ObjectDependency, Table};
use risingwave_meta_model_v2::{
    actor, actor_dispatcher, index, object_dependency, sink, source, streaming_job, table, ActorId,
    CreateType, DatabaseId, JobStatus, ObjectId, SchemaId, UserId,
};
use risingwave_pb::catalog::source::PbOptionalAssociatedTableId;
use risingwave_pb::catalog::table::PbOptionalAssociatedSourceId;
use risingwave_pb::catalog::{PbCreateType, PbTable};
use risingwave_pb::meta::PbTableFragments;
use risingwave_pb::source::{PbConnectorSplit, PbConnectorSplits};
use risingwave_pb::stream_plan::Dispatcher;
use sea_orm::sea_query::SimpleExpr;
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveEnum, ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, IntoActiveModel,
    NotSet, QueryFilter, TransactionTrait,
};

use crate::controller::catalog::CatalogController;
use crate::controller::utils::{check_relation_name_duplicate, ensure_object_id, ensure_user_id};
use crate::manager::StreamingJob;
use crate::model::StreamContext;
use crate::stream::SplitAssignment;
use crate::MetaResult;

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
            table.insert(&txn).await?;
        }
        txn.commit().await?;

        Ok(table_id_map)
    }

    pub async fn prepare_streaming_job(
        &self,
        table_fragment: PbTableFragments,
        streaming_job: &StreamingJob,
    ) -> MetaResult<()> {
        let fragment_actors =
            Self::extract_fragment_and_actors_from_table_fragments(table_fragment)?;
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;

        // Add fragments, actors and actor dispatchers.
        for (fragment, actors, actor_dispatchers) in fragment_actors {
            let fragment = fragment.into_active_model();
            fragment.insert(&txn).await?;
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
        }

        // Update fragment id and dml fragment id.
        match streaming_job {
            StreamingJob::MaterializedView(table)
            | StreamingJob::Index(_, table)
            | StreamingJob::Table(_, table, ..) => {
                Table::update(table::ActiveModel {
                    table_id: Set(table.id as _),
                    fragment_id: Set(Some(table.fragment_id as _)),
                    ..Default::default()
                })
                .exec(&txn)
                .await?;
            }
            _ => {}
        }
        if let StreamingJob::Table(_, table, ..) = streaming_job {
            Table::update(table::ActiveModel {
                table_id: Set(table.id as _),
                dml_fragment_id: Set(table.dml_fragment_id.map(|id| id as _)),
                ..Default::default()
            })
            .exec(&txn)
            .await?;
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
        new_actor_dispatchers: HashMap<crate::model::ActorId, Vec<Dispatcher>>,
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
}
