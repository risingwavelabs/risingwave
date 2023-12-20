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

use std::collections::HashMap;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_meta_model_v2::actor::ActorStatus;
use risingwave_meta_model_v2::object::ObjectType;
use risingwave_meta_model_v2::prelude::{Actor, ActorDispatcher, ObjectDependency, Table};
use risingwave_meta_model_v2::{
    actor, actor_dispatcher, fragment, index, object_dependency, sink, source, streaming_job,
    table, ActorId, DatabaseId, JobStatus, ObjectId, SchemaId, UserId,
};
use risingwave_pb::catalog::source::PbOptionalAssociatedTableId;
use risingwave_pb::catalog::table::PbOptionalAssociatedSourceId;
use risingwave_pb::catalog::{PbCreateType, PbTable};
use risingwave_pb::meta::subscribe_response::Operation as NotificationOperation;
use risingwave_pb::source::{PbConnectorSplit, PbConnectorSplits};
use risingwave_pb::stream_plan::Dispatcher;
use sea_orm::sea_query::SimpleExpr;
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ActiveEnum, ActiveModelTrait, ColumnTrait, DatabaseTransaction, EntityTrait, IntoActiveModel,
    QueryFilter, TransactionTrait,
};

use crate::controller::catalog::CatalogController;
use crate::controller::utils::{
    check_relation_name_duplicate, ensure_object_id, ensure_user_id, get_fragment_mappings,
};
use crate::manager::StreamingJob;
use crate::model::StreamEnvironment;
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
        env: &StreamEnvironment,
    ) -> MetaResult<ObjectId> {
        let obj = Self::create_object(txn, obj_type, owner_id, database_id, schema_id).await?;
        let job = streaming_job::ActiveModel {
            job_id: Set(obj.oid),
            job_status: Set(JobStatus::Creating),
            create_type: Set(create_type.into()),
            timezone: Set(env.timezone.clone()),
        };
        job.insert(txn).await?;

        Ok(obj.oid)
    }

    pub async fn create_job_catalog(
        &self,
        streaming_job: &mut StreamingJob,
        create_type: PbCreateType,
        env: &StreamEnvironment,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
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
                    env,
                )
                .await?;
                table.id = job_id as _;
                let table: table::ActiveModel = table.clone().into();
                table.insert(&txn).await?;
            }
            StreamingJob::Sink(sink) => {
                let job_id = Self::create_streaming_job_obj(
                    &txn,
                    ObjectType::Sink,
                    sink.owner as _,
                    Some(sink.database_id as _),
                    Some(sink.schema_id as _),
                    create_type,
                    env,
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
                    env,
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
                    env,
                )
                .await?;
                // to be compatible with old implementation.
                index.id = job_id as _;
                index.index_table_id = job_id as _;
                table.id = job_id as _;
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
                    env,
                )
                .await?;
                src.id = job_id as _;
                let source: source::ActiveModel = src.clone().into();
                source.insert(&txn).await?;
            }
        }

        // record object dependency.
        let dependent_relations = streaming_job.dependent_relations();
        ObjectDependency::insert_many(dependent_relations.into_iter().map(|id| {
            object_dependency::ActiveModel {
                oid: Set(id as _),
                used_by: Set(streaming_job.id() as _),
                ..Default::default()
            }
        }))
        .exec(&txn)
        .await?;

        txn.commit().await?;

        Ok(())
    }

    pub async fn create_internal_table_catalog(
        &self,
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
            table.insert(&txn).await?;
        }
        txn.commit().await?;

        Ok(table_id_map)
    }

    #[allow(clippy::type_complexity)]
    pub async fn create_fragment_actors(
        &self,
        fragment_actors: Vec<(
            fragment::Model,
            Vec<actor::Model>,
            HashMap<ActorId, Vec<actor_dispatcher::Model>>,
        )>,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        for (fragment, actors, actor_dispatchers) in fragment_actors {
            // TODO: check if into_active_models works or not.
            let fragment = fragment.into_active_model();
            fragment.insert(&txn).await?;
            for actor in actors {
                let actor = actor.into_active_model();
                actor.insert(&txn).await?;
            }
            for (_, actor_dispatchers) in actor_dispatchers {
                for actor_dispatcher in actor_dispatchers {
                    let actor_dispatcher = actor_dispatcher.into_active_model();
                    actor_dispatcher.insert(&txn).await?;
                }
            }
        }
        txn.commit().await?;

        Ok(())
    }

    pub async fn update_fragment_id_in_table(
        &self,
        streaming_job: &StreamingJob,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        match streaming_job {
            StreamingJob::MaterializedView(table)
            | StreamingJob::Index(_, table)
            | StreamingJob::Table(_, table, ..) => {
                Table::update(table::ActiveModel {
                    table_id: Set(table.id as _),
                    fragment_id: Set(table.fragment_id as _),
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

    pub async fn post_collect_table_fragments(
        &self,
        table_id: &TableId,
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

        for (_, splits) in split_assignment {
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
                actor_dispatchers.push(
                    actor_dispatcher::Model::from((actor_id as u32, dispatcher))
                        .into_active_model(),
                );
            }
        }
        ActorDispatcher::insert_many(actor_dispatchers)
            .exec(&txn)
            .await?;

        let fragment_mapping = get_fragment_mappings(&txn, table_id.table_id as _).await?;
        txn.commit().await?;

        self.notify_fragment_mapping(NotificationOperation::Add, fragment_mapping)
            .await;

        Ok(())
    }
}
