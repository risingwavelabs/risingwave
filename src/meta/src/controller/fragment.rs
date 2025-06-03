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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::{Context, anyhow};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::hash::{VnodeCount, VnodeCountCompat, WorkerSlotId};
use risingwave_common::util::stream_graph_visitor::{visit_stream_node, visit_stream_node_mut};
use risingwave_connector::source::SplitImpl;
use risingwave_meta_model::actor::ActorStatus;
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::{
    Actor, Fragment as FragmentModel, FragmentRelation, Sink, StreamingJob,
};
use risingwave_meta_model::{
    ActorId, ConnectorSplits, DatabaseId, DispatcherType, ExprContext, FragmentId, I32Array,
    JobStatus, ObjectId, SchemaId, SinkId, SourceId, StreamNode, StreamingParallelism, TableId,
    VnodeBitmap, WorkerId, actor, database, fragment, fragment_relation, object, sink, source,
    streaming_job, table,
};
use risingwave_meta_model_migration::{Alias, SelectStatement};
use risingwave_pb::common::PbActorLocation;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use risingwave_pb::meta::table_fragments::actor_status::PbActorState;
use risingwave_pb::meta::table_fragments::fragment::{
    FragmentDistributionType, PbFragmentDistributionType,
};
use risingwave_pb::meta::table_fragments::{PbActorStatus, PbState};
use risingwave_pb::meta::{FragmentWorkerSlotMapping, PbFragmentWorkerSlotMapping};
use risingwave_pb::source::{ConnectorSplit, PbConnectorSplits};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    PbDispatchOutputMapping, PbDispatcherType, PbFragmentTypeFlag, PbStreamContext, PbStreamNode,
    PbStreamScanType, StreamScanType,
};
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, DbErr, EntityTrait, FromQueryResult, JoinType, ModelTrait, PaginatorTrait,
    QueryFilter, QuerySelect, RelationTrait, SelectGetableTuple, Selector, TransactionTrait, Value,
};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::barrier::SnapshotBackfillInfo;
use crate::controller::catalog::{CatalogController, CatalogControllerInner};
use crate::controller::scale::resolve_streaming_job_definition;
use crate::controller::utils::{
    FragmentDesc, PartialActorLocation, PartialFragmentStateTables, get_fragment_actor_dispatchers,
    get_fragment_mappings, rebuild_fragment_mapping_from_actors,
    resolve_no_shuffle_actor_dispatcher,
};
use crate::manager::LocalNotification;
use crate::model::{
    DownstreamFragmentRelation, Fragment, FragmentActorDispatchers, FragmentDownstreamRelation,
    StreamActor, StreamContext, StreamJobFragments, TableParallelism,
};
use crate::stream::{SplitAssignment, build_actor_split_impls};
use crate::{MetaError, MetaResult, model};

/// Some information of running (inflight) actors.
#[derive(Clone, Debug)]
pub struct InflightActorInfo {
    pub worker_id: WorkerId,
    pub vnode_bitmap: Option<Bitmap>,
}

#[derive(Clone, Debug)]
pub struct InflightFragmentInfo {
    pub fragment_id: crate::model::FragmentId,
    pub distribution_type: DistributionType,
    pub nodes: PbStreamNode,
    pub actors: HashMap<crate::model::ActorId, InflightActorInfo>,
    pub state_table_ids: HashSet<risingwave_common::catalog::TableId>,
}

#[derive(Clone, Debug)]
pub struct FragmentParallelismInfo {
    pub distribution_type: FragmentDistributionType,
    pub actor_count: usize,
    pub vnode_count: usize,
}

#[derive(Clone, Debug, FromQueryResult, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")] // for dashboard
pub struct StreamingJobInfo {
    pub job_id: ObjectId,
    pub obj_type: ObjectType,
    pub name: String,
    pub job_status: JobStatus,
    pub parallelism: StreamingParallelism,
    pub max_parallelism: i32,
    pub resource_group: String,
}

impl CatalogControllerInner {
    /// List all fragment vnode mapping info for all CREATED streaming jobs.
    pub async fn all_running_fragment_mappings(
        &self,
    ) -> MetaResult<impl Iterator<Item = FragmentWorkerSlotMapping> + '_> {
        let txn = self.db.begin().await?;

        let job_ids: Vec<ObjectId> = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .filter(streaming_job::Column::JobStatus.eq(JobStatus::Created))
            .into_tuple()
            .all(&txn)
            .await?;

        let mut result = vec![];
        for job_id in job_ids {
            let mappings = get_fragment_mappings(&txn, job_id).await?;

            result.extend(mappings.into_iter());
        }

        Ok(result.into_iter())
    }
}

impl CatalogController {
    pub(crate) async fn notify_fragment_mapping(
        &self,
        operation: NotificationOperation,
        fragment_mappings: Vec<PbFragmentWorkerSlotMapping>,
    ) {
        let fragment_ids = fragment_mappings
            .iter()
            .map(|mapping| mapping.fragment_id)
            .collect_vec();
        // notify all fragment mappings to frontend.
        for fragment_mapping in fragment_mappings {
            self.env
                .notification_manager()
                .notify_frontend(
                    operation,
                    NotificationInfo::StreamingWorkerSlotMapping(fragment_mapping),
                )
                .await;
        }

        // update serving vnode mappings.
        match operation {
            NotificationOperation::Add | NotificationOperation::Update => {
                self.env
                    .notification_manager()
                    .notify_local_subscribers(LocalNotification::FragmentMappingsUpsert(
                        fragment_ids,
                    ))
                    .await;
            }
            NotificationOperation::Delete => {
                self.env
                    .notification_manager()
                    .notify_local_subscribers(LocalNotification::FragmentMappingsDelete(
                        fragment_ids,
                    ))
                    .await;
            }
            op => {
                tracing::warn!("unexpected fragment mapping op: {}", op.as_str_name());
            }
        }
    }

    pub fn extract_fragment_and_actors_from_fragments(
        stream_job_fragments: &StreamJobFragments,
    ) -> MetaResult<Vec<(fragment::Model, Vec<actor::Model>)>> {
        stream_job_fragments
            .fragments
            .values()
            .map(|fragment| {
                Self::extract_fragment_and_actors_for_new_job(
                    stream_job_fragments.stream_job_id.table_id as _,
                    fragment,
                    &stream_job_fragments.actor_status,
                    &stream_job_fragments.actor_splits,
                )
            })
            .try_collect()
    }

    fn extract_fragment_and_actors_for_new_job(
        job_id: ObjectId,
        fragment: &Fragment,
        actor_status: &BTreeMap<crate::model::ActorId, PbActorStatus>,
        actor_splits: &HashMap<crate::model::ActorId, Vec<SplitImpl>>,
    ) -> MetaResult<(fragment::Model, Vec<actor::Model>)> {
        let vnode_count = fragment.vnode_count();
        let Fragment {
            fragment_id: pb_fragment_id,
            fragment_type_mask: pb_fragment_type_mask,
            distribution_type: pb_distribution_type,
            actors: pb_actors,
            state_table_ids: pb_state_table_ids,
            nodes,
            ..
        } = fragment;

        let state_table_ids = pb_state_table_ids.clone().into();

        assert!(!pb_actors.is_empty());

        let stream_node = {
            let mut stream_node = nodes.clone();
            visit_stream_node_mut(&mut stream_node, |body| {
                #[expect(deprecated)]
                if let NodeBody::Merge(m) = body {
                    m.upstream_actor_id = vec![];
                }
            });

            stream_node
        };

        let mut actors = vec![];

        for actor in pb_actors {
            let StreamActor {
                actor_id,
                fragment_id,
                vnode_bitmap,
                mview_definition: _,
                expr_context: pb_expr_context,
                ..
            } = actor;

            let splits = actor_splits.get(actor_id).map(|splits| {
                ConnectorSplits::from(&PbConnectorSplits {
                    splits: splits.iter().map(ConnectorSplit::from).collect(),
                })
            });
            let status = actor_status.get(actor_id).cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "actor {} in fragment {} has no actor_status",
                    actor_id,
                    fragment_id
                )
            })?;

            let worker_id = status.worker_id() as _;

            let pb_expr_context = pb_expr_context
                .as_ref()
                .expect("no expression context found");

            #[expect(deprecated)]
            actors.push(actor::Model {
                actor_id: *actor_id as _,
                fragment_id: *fragment_id as _,
                status: status.get_state().unwrap().into(),
                splits,
                worker_id,
                upstream_actor_ids: Default::default(),
                vnode_bitmap: vnode_bitmap
                    .as_ref()
                    .map(|bitmap| VnodeBitmap::from(&bitmap.to_protobuf())),
                expr_context: ExprContext::from(pb_expr_context),
            });
        }

        let stream_node = StreamNode::from(&stream_node);

        let distribution_type = PbFragmentDistributionType::try_from(*pb_distribution_type)
            .unwrap()
            .into();

        #[expect(deprecated)]
        let fragment = fragment::Model {
            fragment_id: *pb_fragment_id as _,
            job_id,
            fragment_type_mask: *pb_fragment_type_mask as _,
            distribution_type,
            stream_node,
            state_table_ids,
            upstream_fragment_id: Default::default(),
            vnode_count: vnode_count as _,
        };

        Ok((fragment, actors))
    }

    pub fn compose_table_fragments(
        table_id: u32,
        state: PbState,
        ctx: Option<PbStreamContext>,
        fragments: Vec<(fragment::Model, Vec<actor::Model>)>,
        parallelism: StreamingParallelism,
        max_parallelism: usize,
        job_definition: Option<String>,
    ) -> MetaResult<StreamJobFragments> {
        let mut pb_fragments = BTreeMap::new();
        let mut pb_actor_splits = HashMap::new();
        let mut pb_actor_status = BTreeMap::new();

        for (fragment, actors) in fragments {
            let (fragment, fragment_actor_status, fragment_actor_splits) =
                Self::compose_fragment(fragment, actors, job_definition.clone())?;

            pb_fragments.insert(fragment.fragment_id, fragment);

            pb_actor_splits.extend(build_actor_split_impls(&fragment_actor_splits));
            pb_actor_status.extend(fragment_actor_status.into_iter());
        }

        let table_fragments = StreamJobFragments {
            stream_job_id: table_id.into(),
            state: state as _,
            fragments: pb_fragments,
            actor_status: pb_actor_status,
            actor_splits: pb_actor_splits,
            ctx: ctx
                .as_ref()
                .map(StreamContext::from_protobuf)
                .unwrap_or_default(),
            assigned_parallelism: match parallelism {
                StreamingParallelism::Custom => TableParallelism::Custom,
                StreamingParallelism::Adaptive => TableParallelism::Adaptive,
                StreamingParallelism::Fixed(n) => TableParallelism::Fixed(n as _),
            },
            max_parallelism,
        };

        Ok(table_fragments)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn compose_fragment(
        fragment: fragment::Model,
        actors: Vec<actor::Model>,
        job_definition: Option<String>,
    ) -> MetaResult<(
        Fragment,
        HashMap<crate::model::ActorId, PbActorStatus>,
        HashMap<crate::model::ActorId, PbConnectorSplits>,
    )> {
        let fragment::Model {
            fragment_id,
            fragment_type_mask,
            distribution_type,
            stream_node,
            state_table_ids,
            vnode_count,
            ..
        } = fragment;

        let stream_node = stream_node.to_protobuf();
        let mut upstream_fragments = HashSet::new();
        visit_stream_node(&stream_node, |body| {
            if let NodeBody::Merge(m) = body {
                assert!(
                    upstream_fragments.insert(m.upstream_fragment_id),
                    "non-duplicate upstream fragment"
                );
            }
        });

        let mut pb_actors = vec![];

        let mut pb_actor_status = HashMap::new();
        let mut pb_actor_splits = HashMap::new();

        for actor in actors {
            if actor.fragment_id != fragment_id {
                bail!(
                    "fragment id {} from actor {} is different from fragment {}",
                    actor.fragment_id,
                    actor.actor_id,
                    fragment_id
                )
            }

            let actor::Model {
                actor_id,
                fragment_id,
                status,
                worker_id,
                splits,
                vnode_bitmap,
                expr_context,
                ..
            } = actor;

            let vnode_bitmap =
                vnode_bitmap.map(|vnode_bitmap| Bitmap::from(vnode_bitmap.to_protobuf()));
            let pb_expr_context = Some(expr_context.to_protobuf());

            pb_actor_status.insert(
                actor_id as _,
                PbActorStatus {
                    location: PbActorLocation::from_worker(worker_id as u32),
                    state: PbActorState::from(status) as _,
                },
            );

            if let Some(splits) = splits {
                pb_actor_splits.insert(actor_id as _, splits.to_protobuf());
            }

            pb_actors.push(StreamActor {
                actor_id: actor_id as _,
                fragment_id: fragment_id as _,
                vnode_bitmap,
                mview_definition: job_definition.clone().unwrap_or("".to_owned()),
                expr_context: pb_expr_context,
            })
        }

        let pb_state_table_ids = state_table_ids.into_u32_array();
        let pb_distribution_type = PbFragmentDistributionType::from(distribution_type) as _;
        let pb_fragment = Fragment {
            fragment_id: fragment_id as _,
            fragment_type_mask: fragment_type_mask as _,
            distribution_type: pb_distribution_type,
            actors: pb_actors,
            state_table_ids: pb_state_table_ids,
            maybe_vnode_count: VnodeCount::set(vnode_count).to_protobuf(),
            nodes: stream_node,
        };

        Ok((pb_fragment, pb_actor_status, pb_actor_splits))
    }

    pub async fn running_fragment_parallelisms(
        &self,
        id_filter: Option<HashSet<FragmentId>>,
    ) -> MetaResult<HashMap<FragmentId, FragmentParallelismInfo>> {
        let inner = self.inner.read().await;

        let query_alias = Alias::new("fragment_actor_count");
        let count_alias = Alias::new("count");

        let mut query = SelectStatement::new()
            .column(actor::Column::FragmentId)
            .expr_as(actor::Column::ActorId.count(), count_alias.clone())
            .from(Actor)
            .group_by_col(actor::Column::FragmentId)
            .to_owned();

        if let Some(id_filter) = id_filter {
            query.cond_having(actor::Column::FragmentId.is_in(id_filter));
        }

        let outer = SelectStatement::new()
            .column((FragmentModel, fragment::Column::FragmentId))
            .column(count_alias)
            .column(fragment::Column::DistributionType)
            .column(fragment::Column::VnodeCount)
            .from_subquery(query.to_owned(), query_alias.clone())
            .inner_join(
                FragmentModel,
                Expr::col((query_alias, actor::Column::FragmentId))
                    .equals((FragmentModel, fragment::Column::FragmentId)),
            )
            .to_owned();

        let fragment_parallelisms: Vec<(FragmentId, i64, DistributionType, i32)> =
            Selector::<SelectGetableTuple<(FragmentId, i64, DistributionType, i32)>>::into_tuple(
                outer.to_owned(),
            )
            .all(&inner.db)
            .await?;

        Ok(fragment_parallelisms
            .into_iter()
            .map(|(fragment_id, count, distribution_type, vnode_count)| {
                (
                    fragment_id,
                    FragmentParallelismInfo {
                        distribution_type: distribution_type.into(),
                        actor_count: count as usize,
                        vnode_count: vnode_count as usize,
                    },
                )
            })
            .collect())
    }

    pub async fn fragment_job_mapping(&self) -> MetaResult<HashMap<FragmentId, ObjectId>> {
        let inner = self.inner.read().await;
        let fragment_jobs: Vec<(FragmentId, ObjectId)> = FragmentModel::find()
            .select_only()
            .columns([fragment::Column::FragmentId, fragment::Column::JobId])
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(fragment_jobs.into_iter().collect())
    }

    pub async fn get_fragment_job_id(
        &self,
        fragment_ids: Vec<FragmentId>,
    ) -> MetaResult<Vec<ObjectId>> {
        let inner = self.inner.read().await;

        let object_ids: Vec<ObjectId> = FragmentModel::find()
            .select_only()
            .column(fragment::Column::JobId)
            .filter(fragment::Column::FragmentId.is_in(fragment_ids))
            .into_tuple()
            .all(&inner.db)
            .await?;

        Ok(object_ids)
    }

    pub async fn get_fragment_desc_by_id(
        &self,
        fragment_id: FragmentId,
    ) -> MetaResult<Option<(FragmentDesc, Vec<FragmentId>)>> {
        let inner = self.inner.read().await;

        // Get the fragment description
        let fragment_opt = FragmentModel::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::JobId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::DistributionType,
                fragment::Column::StateTableIds,
                fragment::Column::VnodeCount,
                fragment::Column::StreamNode,
            ])
            .column_as(Expr::col(actor::Column::ActorId).count(), "parallelism")
            .join(JoinType::LeftJoin, fragment::Relation::Actor.def())
            .filter(fragment::Column::FragmentId.eq(fragment_id))
            .group_by(fragment::Column::FragmentId)
            .into_model::<FragmentDesc>()
            .one(&inner.db)
            .await?;

        let Some(fragment) = fragment_opt else {
            return Ok(None); // Fragment not found
        };

        // Get upstream fragments
        let upstreams: Vec<FragmentId> = FragmentRelation::find()
            .select_only()
            .column(fragment_relation::Column::SourceFragmentId)
            .filter(fragment_relation::Column::TargetFragmentId.eq(fragment_id))
            .into_tuple()
            .all(&inner.db)
            .await?;

        Ok(Some((fragment, upstreams)))
    }

    pub async fn list_fragment_database_ids(
        &self,
        select_fragment_ids: Option<Vec<FragmentId>>,
    ) -> MetaResult<Vec<(FragmentId, DatabaseId)>> {
        let inner = self.inner.read().await;
        let select = FragmentModel::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .column(object::Column::DatabaseId)
            .join(JoinType::InnerJoin, fragment::Relation::Object.def());
        let select = if let Some(select_fragment_ids) = select_fragment_ids {
            select.filter(fragment::Column::FragmentId.is_in(select_fragment_ids))
        } else {
            select
        };
        Ok(select.into_tuple().all(&inner.db).await?)
    }

    pub async fn get_job_fragments_by_id(
        &self,
        job_id: ObjectId,
    ) -> MetaResult<StreamJobFragments> {
        let inner = self.inner.read().await;
        let fragment_actors = FragmentModel::find()
            .find_with_related(Actor)
            .filter(fragment::Column::JobId.eq(job_id))
            .all(&inner.db)
            .await?;

        let job_info = StreamingJob::find_by_id(job_id)
            .one(&inner.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("job {} not found in database", job_id))?;

        let job_definition = resolve_streaming_job_definition(&inner.db, &HashSet::from([job_id]))
            .await?
            .remove(&job_id);

        Self::compose_table_fragments(
            job_id as _,
            job_info.job_status.into(),
            job_info.timezone.map(|tz| PbStreamContext { timezone: tz }),
            fragment_actors,
            job_info.parallelism.clone(),
            job_info.max_parallelism as _,
            job_definition,
        )
    }

    pub async fn get_fragment_actor_dispatchers(
        &self,
        fragment_ids: Vec<FragmentId>,
    ) -> MetaResult<FragmentActorDispatchers> {
        let inner = self.inner.read().await;
        get_fragment_actor_dispatchers(&inner.db, fragment_ids).await
    }

    pub async fn get_fragment_downstream_relations(
        &self,
        fragment_ids: Vec<FragmentId>,
    ) -> MetaResult<FragmentDownstreamRelation> {
        let inner = self.inner.read().await;
        let mut stream = FragmentRelation::find()
            .filter(fragment_relation::Column::SourceFragmentId.is_in(fragment_ids))
            .stream(&inner.db)
            .await?;
        let mut relations = FragmentDownstreamRelation::new();
        while let Some(relation) = stream.try_next().await? {
            relations
                .entry(relation.source_fragment_id as _)
                .or_default()
                .push(DownstreamFragmentRelation {
                    downstream_fragment_id: relation.target_fragment_id as _,
                    dispatcher_type: relation.dispatcher_type,
                    dist_key_indices: relation.dist_key_indices.into_u32_array(),
                    output_mapping: PbDispatchOutputMapping {
                        indices: relation.output_indices.into_u32_array(),
                        types: relation
                            .output_type_mapping
                            .unwrap_or_default()
                            .to_protobuf(),
                    },
                });
        }
        Ok(relations)
    }

    pub async fn get_job_fragment_backfill_scan_type(
        &self,
        job_id: ObjectId,
    ) -> MetaResult<HashMap<model::FragmentId, PbStreamScanType>> {
        let inner = self.inner.read().await;
        let fragments: Vec<_> = FragmentModel::find()
            .filter(fragment::Column::JobId.eq(job_id))
            .all(&inner.db)
            .await?;

        let mut result = HashMap::new();

        for fragment::Model {
            fragment_id,
            stream_node,
            ..
        } in fragments
        {
            let stream_node = stream_node.to_protobuf();
            visit_stream_node(&stream_node, |body| {
                if let NodeBody::StreamScan(node) = body {
                    match node.stream_scan_type() {
                        StreamScanType::Unspecified => {}
                        scan_type => {
                            result.insert(fragment_id as model::FragmentId, scan_type);
                        }
                    }
                }
            });
        }

        Ok(result)
    }

    pub async fn list_streaming_job_infos(&self) -> MetaResult<Vec<StreamingJobInfo>> {
        let inner = self.inner.read().await;
        let job_states = StreamingJob::find()
            .select_only()
            .column(streaming_job::Column::JobId)
            .join(JoinType::InnerJoin, streaming_job::Relation::Object.def())
            .join(JoinType::InnerJoin, object::Relation::Database2.def())
            .column(object::Column::ObjType)
            .join(JoinType::LeftJoin, table::Relation::Object1.def().rev())
            .join(JoinType::LeftJoin, source::Relation::Object.def().rev())
            .join(JoinType::LeftJoin, sink::Relation::Object.def().rev())
            .column_as(
                Expr::if_null(
                    Expr::col((table::Entity, table::Column::Name)),
                    Expr::if_null(
                        Expr::col((source::Entity, source::Column::Name)),
                        Expr::if_null(
                            Expr::col((sink::Entity, sink::Column::Name)),
                            Expr::val("<unknown>"),
                        ),
                    ),
                ),
                "name",
            )
            .columns([
                streaming_job::Column::JobStatus,
                streaming_job::Column::Parallelism,
                streaming_job::Column::MaxParallelism,
            ])
            .column_as(
                Expr::if_null(
                    Expr::col((
                        streaming_job::Entity,
                        streaming_job::Column::SpecificResourceGroup,
                    )),
                    Expr::col((database::Entity, database::Column::ResourceGroup)),
                ),
                "resource_group",
            )
            .into_model()
            .all(&inner.db)
            .await?;
        Ok(job_states)
    }

    pub async fn get_max_parallelism_by_id(&self, job_id: ObjectId) -> MetaResult<usize> {
        let inner = self.inner.read().await;
        let max_parallelism: i32 = StreamingJob::find_by_id(job_id)
            .select_only()
            .column(streaming_job::Column::MaxParallelism)
            .into_tuple()
            .one(&inner.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("job {} not found in database", job_id))?;
        Ok(max_parallelism as usize)
    }

    /// Get all actor ids in the target streaming jobs.
    pub async fn get_job_actor_mapping(
        &self,
        job_ids: Vec<ObjectId>,
    ) -> MetaResult<HashMap<ObjectId, Vec<ActorId>>> {
        let inner = self.inner.read().await;
        let job_actors: Vec<(ObjectId, ActorId)> = Actor::find()
            .select_only()
            .column(fragment::Column::JobId)
            .column(actor::Column::ActorId)
            .join(JoinType::InnerJoin, actor::Relation::Fragment.def())
            .filter(fragment::Column::JobId.is_in(job_ids))
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(job_actors.into_iter().into_group_map())
    }

    /// Try to get internal table ids of each streaming job, used by metrics collection.
    pub async fn get_job_internal_table_ids(&self) -> Option<Vec<(ObjectId, Vec<TableId>)>> {
        if let Ok(inner) = self.inner.try_read() {
            if let Ok(job_state_tables) = FragmentModel::find()
                .select_only()
                .columns([fragment::Column::JobId, fragment::Column::StateTableIds])
                .into_tuple::<(ObjectId, I32Array)>()
                .all(&inner.db)
                .await
            {
                let mut job_internal_table_ids = HashMap::new();
                for (job_id, state_table_ids) in job_state_tables {
                    job_internal_table_ids
                        .entry(job_id)
                        .or_insert_with(Vec::new)
                        .extend(state_table_ids.into_inner());
                }
                return Some(job_internal_table_ids.into_iter().collect());
            }
        }
        None
    }

    pub async fn has_any_running_jobs(&self) -> MetaResult<bool> {
        let inner = self.inner.read().await;
        let count = FragmentModel::find().count(&inner.db).await?;
        Ok(count > 0)
    }

    pub async fn worker_actor_count(&self) -> MetaResult<HashMap<WorkerId, usize>> {
        let inner = self.inner.read().await;
        let actor_cnt: Vec<(WorkerId, i64)> = Actor::find()
            .select_only()
            .column(actor::Column::WorkerId)
            .column_as(actor::Column::ActorId.count(), "count")
            .group_by(actor::Column::WorkerId)
            .into_tuple()
            .all(&inner.db)
            .await?;

        Ok(actor_cnt
            .into_iter()
            .map(|(worker_id, count)| (worker_id, count as usize))
            .collect())
    }

    // TODO: This function is too heavy, we should avoid using it and implement others on demand.
    pub async fn table_fragments(&self) -> MetaResult<BTreeMap<ObjectId, StreamJobFragments>> {
        let inner = self.inner.read().await;
        let jobs = StreamingJob::find().all(&inner.db).await?;

        let mut job_definition = resolve_streaming_job_definition(
            &inner.db,
            &HashSet::from_iter(jobs.iter().map(|job| job.job_id)),
        )
        .await?;

        let mut table_fragments = BTreeMap::new();
        for job in jobs {
            let fragment_actors = FragmentModel::find()
                .find_with_related(Actor)
                .filter(fragment::Column::JobId.eq(job.job_id))
                .all(&inner.db)
                .await?;

            table_fragments.insert(
                job.job_id as ObjectId,
                Self::compose_table_fragments(
                    job.job_id as _,
                    job.job_status.into(),
                    job.timezone.map(|tz| PbStreamContext { timezone: tz }),
                    fragment_actors,
                    job.parallelism.clone(),
                    job.max_parallelism as _,
                    job_definition.remove(&job.job_id),
                )?,
            );
        }

        Ok(table_fragments)
    }

    pub async fn upstream_fragments(
        &self,
        fragment_ids: impl Iterator<Item = crate::model::FragmentId>,
    ) -> MetaResult<HashMap<crate::model::FragmentId, HashSet<crate::model::FragmentId>>> {
        let inner = self.inner.read().await;
        let mut stream = FragmentRelation::find()
            .select_only()
            .columns([
                fragment_relation::Column::SourceFragmentId,
                fragment_relation::Column::TargetFragmentId,
            ])
            .filter(
                fragment_relation::Column::TargetFragmentId
                    .is_in(fragment_ids.map(|id| id as FragmentId)),
            )
            .into_tuple::<(FragmentId, FragmentId)>()
            .stream(&inner.db)
            .await?;
        let mut upstream_fragments: HashMap<_, HashSet<_>> = HashMap::new();
        while let Some((upstream_fragment_id, downstream_fragment_id)) = stream.try_next().await? {
            upstream_fragments
                .entry(downstream_fragment_id as crate::model::FragmentId)
                .or_default()
                .insert(upstream_fragment_id as crate::model::FragmentId);
        }
        Ok(upstream_fragments)
    }

    pub async fn list_actor_locations(&self) -> MetaResult<Vec<PartialActorLocation>> {
        let inner = self.inner.read().await;
        let actor_locations: Vec<PartialActorLocation> =
            Actor::find().into_partial_model().all(&inner.db).await?;
        Ok(actor_locations)
    }

    pub async fn list_actor_info(
        &self,
    ) -> MetaResult<Vec<(ActorId, FragmentId, ObjectId, SchemaId, ObjectType)>> {
        let inner = self.inner.read().await;
        let actor_locations: Vec<(ActorId, FragmentId, ObjectId, SchemaId, ObjectType)> =
            Actor::find()
                .join(JoinType::LeftJoin, actor::Relation::Fragment.def())
                .join(JoinType::LeftJoin, fragment::Relation::Object.def())
                .select_only()
                .columns([actor::Column::ActorId, actor::Column::FragmentId])
                .column_as(object::Column::Oid, "job_id")
                .column_as(object::Column::SchemaId, "schema_id")
                .column_as(object::Column::ObjType, "type")
                .into_tuple()
                .all(&inner.db)
                .await?;
        Ok(actor_locations)
    }

    pub async fn list_source_actors(&self) -> MetaResult<Vec<(ActorId, FragmentId)>> {
        let inner = self.inner.read().await;

        let source_actors: Vec<(ActorId, FragmentId)> = Actor::find()
            .select_only()
            .filter(actor::Column::Splits.is_not_null())
            .columns([actor::Column::ActorId, actor::Column::FragmentId])
            .into_tuple()
            .all(&inner.db)
            .await?;

        Ok(source_actors)
    }

    pub async fn list_fragment_descs(&self) -> MetaResult<Vec<(FragmentDesc, Vec<FragmentId>)>> {
        let inner = self.inner.read().await;
        let mut result = Vec::new();
        let fragments = FragmentModel::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::JobId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::DistributionType,
                fragment::Column::StateTableIds,
                fragment::Column::VnodeCount,
                fragment::Column::StreamNode,
            ])
            .column_as(Expr::col(actor::Column::ActorId).count(), "parallelism")
            .join(JoinType::LeftJoin, fragment::Relation::Actor.def())
            .group_by(fragment::Column::FragmentId)
            .into_model::<FragmentDesc>()
            .all(&inner.db)
            .await?;
        for fragment in fragments {
            let upstreams: Vec<FragmentId> = FragmentRelation::find()
                .select_only()
                .column(fragment_relation::Column::SourceFragmentId)
                .filter(fragment_relation::Column::TargetFragmentId.eq(fragment.fragment_id))
                .into_tuple()
                .all(&inner.db)
                .await?;
            result.push((fragment, upstreams));
        }
        Ok(result)
    }

    pub async fn list_sink_actor_mapping(
        &self,
    ) -> MetaResult<HashMap<SinkId, (String, Vec<ActorId>)>> {
        let inner = self.inner.read().await;
        let sink_id_names: Vec<(SinkId, String)> = Sink::find()
            .select_only()
            .columns([sink::Column::SinkId, sink::Column::Name])
            .into_tuple()
            .all(&inner.db)
            .await?;
        let (sink_ids, _): (Vec<_>, Vec<_>) = sink_id_names.iter().cloned().unzip();
        let sink_name_mapping: HashMap<SinkId, String> = sink_id_names.into_iter().collect();

        let actor_with_type: Vec<(ActorId, SinkId, i32)> = Actor::find()
            .select_only()
            .column(actor::Column::ActorId)
            .columns([fragment::Column::JobId, fragment::Column::FragmentTypeMask])
            .join(JoinType::InnerJoin, actor::Relation::Fragment.def())
            .filter(fragment::Column::JobId.is_in(sink_ids))
            .into_tuple()
            .all(&inner.db)
            .await?;

        let mut sink_actor_mapping = HashMap::new();
        for (actor_id, sink_id, type_mask) in actor_with_type {
            if type_mask & PbFragmentTypeFlag::Sink as i32 != 0 {
                sink_actor_mapping
                    .entry(sink_id)
                    .or_insert_with(|| (sink_name_mapping.get(&sink_id).unwrap().clone(), vec![]))
                    .1
                    .push(actor_id);
            }
        }

        Ok(sink_actor_mapping)
    }

    pub async fn list_fragment_state_tables(&self) -> MetaResult<Vec<PartialFragmentStateTables>> {
        let inner = self.inner.read().await;
        let fragment_state_tables: Vec<PartialFragmentStateTables> = FragmentModel::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::JobId,
                fragment::Column::StateTableIds,
            ])
            .into_partial_model()
            .all(&inner.db)
            .await?;
        Ok(fragment_state_tables)
    }

    /// Used in [`crate::barrier::GlobalBarrierManager`], load all running actor that need to be sent or
    /// collected
    pub async fn load_all_actors(
        &self,
        database_id: Option<DatabaseId>,
    ) -> MetaResult<HashMap<DatabaseId, HashMap<TableId, HashMap<FragmentId, InflightFragmentInfo>>>>
    {
        let inner = self.inner.read().await;
        let filter_condition = actor::Column::Status.eq(ActorStatus::Running);
        let filter_condition = if let Some(database_id) = database_id {
            filter_condition.and(object::Column::DatabaseId.eq(database_id))
        } else {
            filter_condition
        };
        #[expect(clippy::type_complexity)]
        let mut actor_info_stream: BoxStream<
            '_,
            Result<
                (
                    ActorId,
                    WorkerId,
                    Option<VnodeBitmap>,
                    FragmentId,
                    StreamNode,
                    I32Array,
                    DistributionType,
                    DatabaseId,
                    ObjectId,
                ),
                _,
            >,
        > = Actor::find()
            .select_only()
            .column(actor::Column::ActorId)
            .column(actor::Column::WorkerId)
            .column(actor::Column::VnodeBitmap)
            .column(fragment::Column::FragmentId)
            .column(fragment::Column::StreamNode)
            .column(fragment::Column::StateTableIds)
            .column(fragment::Column::DistributionType)
            .column(object::Column::DatabaseId)
            .column(object::Column::Oid)
            .join(JoinType::InnerJoin, actor::Relation::Fragment.def())
            .join(JoinType::InnerJoin, fragment::Relation::Object.def())
            .filter(filter_condition)
            .into_tuple()
            .stream(&inner.db)
            .await?;

        let mut database_fragment_infos: HashMap<_, HashMap<_, HashMap<_, InflightFragmentInfo>>> =
            HashMap::new();

        while let Some((
            actor_id,
            worker_id,
            vnode_bitmap,
            fragment_id,
            node,
            state_table_ids,
            distribution_type,
            database_id,
            job_id,
        )) = actor_info_stream.try_next().await?
        {
            let fragment_infos = database_fragment_infos
                .entry(database_id)
                .or_default()
                .entry(job_id)
                .or_default();
            let state_table_ids = state_table_ids.into_inner();
            let state_table_ids = state_table_ids
                .into_iter()
                .map(|table_id| risingwave_common::catalog::TableId::new(table_id as _))
                .collect();
            let actor_info = InflightActorInfo {
                worker_id,
                vnode_bitmap: vnode_bitmap.map(|bitmap| bitmap.to_protobuf().into()),
            };
            match fragment_infos.entry(fragment_id) {
                Entry::Occupied(mut entry) => {
                    let info: &mut InflightFragmentInfo = entry.get_mut();
                    assert_eq!(info.state_table_ids, state_table_ids);
                    assert!(info.actors.insert(actor_id as _, actor_info).is_none());
                }
                Entry::Vacant(entry) => {
                    entry.insert(InflightFragmentInfo {
                        fragment_id: fragment_id as _,
                        distribution_type,
                        nodes: node.to_protobuf(),
                        actors: HashMap::from_iter([(actor_id as _, actor_info)]),
                        state_table_ids,
                    });
                }
            }
        }

        debug!(?database_fragment_infos, "reload all actors");

        Ok(database_fragment_infos)
    }

    pub async fn migrate_actors(
        &self,
        plan: HashMap<WorkerSlotId, WorkerSlotId>,
    ) -> MetaResult<()> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let actors: Vec<(
            FragmentId,
            DistributionType,
            ActorId,
            Option<VnodeBitmap>,
            WorkerId,
            ActorStatus,
        )> = Actor::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::DistributionType,
            ])
            .columns([
                actor::Column::ActorId,
                actor::Column::VnodeBitmap,
                actor::Column::WorkerId,
                actor::Column::Status,
            ])
            .join(JoinType::InnerJoin, actor::Relation::Fragment.def())
            .into_tuple()
            .all(&txn)
            .await?;

        let mut actor_locations = HashMap::new();

        for (fragment_id, _, actor_id, _, worker_id, status) in &actors {
            if *status != ActorStatus::Running {
                tracing::warn!(
                    "skipping actor {} in fragment {} with status {:?}",
                    actor_id,
                    fragment_id,
                    status
                );
                continue;
            }

            actor_locations
                .entry(*worker_id)
                .or_insert(HashMap::new())
                .entry(*fragment_id)
                .or_insert(BTreeSet::new())
                .insert(*actor_id);
        }

        let expired_or_changed_workers: HashSet<_> =
            plan.keys().map(|k| k.worker_id() as WorkerId).collect();

        let mut actor_migration_plan = HashMap::new();
        for (worker, fragment) in actor_locations {
            if expired_or_changed_workers.contains(&worker) {
                for (fragment_id, actors) in fragment {
                    debug!(
                        "worker {} expired or changed, migrating fragment {}",
                        worker, fragment_id
                    );
                    let worker_slot_to_actor: HashMap<_, _> = actors
                        .iter()
                        .enumerate()
                        .map(|(idx, actor_id)| {
                            (WorkerSlotId::new(worker as _, idx as _), *actor_id)
                        })
                        .collect();

                    for (worker_slot, actor) in worker_slot_to_actor {
                        if let Some(target) = plan.get(&worker_slot) {
                            actor_migration_plan.insert(actor, target.worker_id() as WorkerId);
                        }
                    }
                }
            }
        }

        for (actor, worker) in actor_migration_plan {
            Actor::update_many()
                .col_expr(
                    actor::Column::WorkerId,
                    Expr::value(Value::Int(Some(worker))),
                )
                .filter(actor::Column::ActorId.eq(actor))
                .exec(&txn)
                .await?;
        }

        txn.commit().await?;

        self.notify_fragment_mapping(
            NotificationOperation::Update,
            rebuild_fragment_mapping_from_actors(actors),
        )
        .await;

        Ok(())
    }

    pub async fn all_inuse_worker_slots(&self) -> MetaResult<HashSet<WorkerSlotId>> {
        let inner = self.inner.read().await;

        let actors: Vec<(FragmentId, ActorId, WorkerId)> = Actor::find()
            .select_only()
            .columns([fragment::Column::FragmentId])
            .columns([actor::Column::ActorId, actor::Column::WorkerId])
            .join(JoinType::InnerJoin, actor::Relation::Fragment.def())
            .into_tuple()
            .all(&inner.db)
            .await?;

        let mut actor_locations = HashMap::new();

        for (fragment_id, _, worker_id) in actors {
            *actor_locations
                .entry(worker_id)
                .or_insert(HashMap::new())
                .entry(fragment_id)
                .or_insert(0_usize) += 1;
        }

        let mut result = HashSet::new();
        for (worker_id, mapping) in actor_locations {
            let max_fragment_len = mapping.values().max().unwrap();

            result
                .extend((0..*max_fragment_len).map(|idx| WorkerSlotId::new(worker_id as u32, idx)))
        }

        Ok(result)
    }

    pub async fn all_node_actors(
        &self,
        include_inactive: bool,
    ) -> MetaResult<HashMap<WorkerId, Vec<StreamActor>>> {
        let inner = self.inner.read().await;
        let fragment_actors = if include_inactive {
            FragmentModel::find()
                .find_with_related(Actor)
                .all(&inner.db)
                .await?
        } else {
            FragmentModel::find()
                .find_with_related(Actor)
                .filter(actor::Column::Status.eq(ActorStatus::Running))
                .all(&inner.db)
                .await?
        };

        let job_definitions = resolve_streaming_job_definition(
            &inner.db,
            &HashSet::from_iter(fragment_actors.iter().map(|(fragment, _)| fragment.job_id)),
        )
        .await?;

        let mut node_actors = HashMap::new();
        for (fragment, actors) in fragment_actors {
            let job_id = fragment.job_id;
            let (table_fragments, actor_status, _) = Self::compose_fragment(
                fragment,
                actors,
                job_definitions.get(&(job_id as _)).cloned(),
            )?;
            for actor in table_fragments.actors {
                let node_id = actor_status[&actor.actor_id].worker_id() as WorkerId;
                node_actors
                    .entry(node_id)
                    .or_insert_with(Vec::new)
                    .push(actor);
            }
        }

        Ok(node_actors)
    }

    pub async fn get_worker_actor_ids(
        &self,
        job_ids: Vec<ObjectId>,
    ) -> MetaResult<BTreeMap<WorkerId, Vec<ActorId>>> {
        let inner = self.inner.read().await;
        let actor_workers: Vec<(ActorId, WorkerId)> = Actor::find()
            .select_only()
            .columns([actor::Column::ActorId, actor::Column::WorkerId])
            .join(JoinType::InnerJoin, actor::Relation::Fragment.def())
            .filter(fragment::Column::JobId.is_in(job_ids))
            .into_tuple()
            .all(&inner.db)
            .await?;

        let mut worker_actors = BTreeMap::new();
        for (actor_id, worker_id) in actor_workers {
            worker_actors
                .entry(worker_id)
                .or_insert_with(Vec::new)
                .push(actor_id);
        }

        Ok(worker_actors)
    }

    pub async fn update_actor_splits(&self, split_assignment: &SplitAssignment) -> MetaResult<()> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;
        for assignments in split_assignment.values() {
            for (actor_id, splits) in assignments {
                let actor_splits = splits.iter().map(Into::into).collect_vec();
                Actor::update(actor::ActiveModel {
                    actor_id: Set(*actor_id as _),
                    splits: Set(Some(ConnectorSplits::from(&PbConnectorSplits {
                        splits: actor_splits,
                    }))),
                    ..Default::default()
                })
                .exec(&txn)
                .await
                .map_err(|err| {
                    if err == DbErr::RecordNotUpdated {
                        MetaError::catalog_id_not_found("actor_id", actor_id)
                    } else {
                        err.into()
                    }
                })?;
            }
        }
        txn.commit().await?;

        Ok(())
    }

    pub async fn fill_snapshot_backfill_epoch(
        &self,
        fragment_ids: impl Iterator<Item = FragmentId>,
        snapshot_backfill_info: Option<&SnapshotBackfillInfo>,
        cross_db_snapshot_backfill_info: &SnapshotBackfillInfo,
    ) -> MetaResult<()> {
        let inner = self.inner.write().await;
        let txn = inner.db.begin().await?;
        for fragment_id in fragment_ids {
            let fragment = FragmentModel::find_by_id(fragment_id)
                .one(&txn)
                .await?
                .context(format!("fragment {} not found", fragment_id))?;
            let mut node = fragment.stream_node.to_protobuf();
            if crate::stream::fill_snapshot_backfill_epoch(
                &mut node,
                snapshot_backfill_info,
                cross_db_snapshot_backfill_info,
            )? {
                let node = StreamNode::from(&node);
                FragmentModel::update(fragment::ActiveModel {
                    fragment_id: Set(fragment_id),
                    stream_node: Set(node),
                    ..Default::default()
                })
                .exec(&txn)
                .await?;
            }
        }
        txn.commit().await?;
        Ok(())
    }

    /// Get the actor ids of the fragment with `fragment_id` with `Running` status.
    pub async fn get_running_actors_of_fragment(
        &self,
        fragment_id: FragmentId,
    ) -> MetaResult<Vec<ActorId>> {
        let inner = self.inner.read().await;
        let actors: Vec<ActorId> = Actor::find()
            .select_only()
            .column(actor::Column::ActorId)
            .filter(actor::Column::FragmentId.eq(fragment_id))
            .filter(actor::Column::Status.eq(ActorStatus::Running))
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(actors)
    }

    /// Get the actor ids, and each actor's upstream source actor ids of the fragment with `fragment_id` with `Running` status.
    /// (`backfill_actor_id`, `upstream_source_actor_id`)
    pub async fn get_running_actors_for_source_backfill(
        &self,
        source_backfill_fragment_id: FragmentId,
        source_fragment_id: FragmentId,
    ) -> MetaResult<Vec<(ActorId, ActorId)>> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let fragment_relation: DispatcherType = FragmentRelation::find()
            .select_only()
            .column(fragment_relation::Column::DispatcherType)
            .filter(fragment_relation::Column::SourceFragmentId.eq(source_fragment_id))
            .filter(fragment_relation::Column::TargetFragmentId.eq(source_backfill_fragment_id))
            .into_tuple()
            .one(&txn)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "no fragment connection from source fragment {} to source backfill fragment {}",
                    source_fragment_id,
                    source_backfill_fragment_id
                )
            })?;

        if fragment_relation != DispatcherType::NoShuffle {
            return Err(anyhow!("expect NoShuffle but get {:?}", fragment_relation).into());
        }

        let load_fragment_distribution_type = |txn, fragment_id: FragmentId| async move {
            let result: MetaResult<DistributionType> = try {
                FragmentModel::find_by_id(fragment_id)
                    .select_only()
                    .column(fragment::Column::DistributionType)
                    .into_tuple()
                    .one(txn)
                    .await?
                    .ok_or_else(|| anyhow!("failed to find fragment: {}", fragment_id))?
            };
            result
        };

        let source_backfill_distribution_type =
            load_fragment_distribution_type(&txn, source_backfill_fragment_id).await?;
        let source_distribution_type =
            load_fragment_distribution_type(&txn, source_fragment_id).await?;

        let load_fragment_actor_distribution = |txn, fragment_id: FragmentId| async move {
            Actor::find()
                .select_only()
                .column(actor::Column::ActorId)
                .column(actor::Column::VnodeBitmap)
                .filter(actor::Column::FragmentId.eq(fragment_id))
                .into_tuple()
                .stream(txn)
                .await?
                .map(|result| {
                    result.map(|(actor_id, vnode): (ActorId, Option<VnodeBitmap>)| {
                        (
                            actor_id as _,
                            vnode.map(|bitmap| Bitmap::from(bitmap.to_protobuf())),
                        )
                    })
                })
                .try_collect()
                .await
        };

        let source_backfill_actors: HashMap<crate::model::ActorId, Option<Bitmap>> =
            load_fragment_actor_distribution(&txn, source_backfill_fragment_id).await?;

        let source_actors = load_fragment_actor_distribution(&txn, source_fragment_id).await?;

        Ok(resolve_no_shuffle_actor_dispatcher(
            source_distribution_type,
            &source_actors,
            source_backfill_distribution_type,
            &source_backfill_actors,
        )
        .into_iter()
        .map(|(source_actor, source_backfill_actor)| {
            (source_backfill_actor as _, source_actor as _)
        })
        .collect())
    }

    pub async fn get_actors_by_job_ids(&self, job_ids: Vec<ObjectId>) -> MetaResult<Vec<ActorId>> {
        let inner = self.inner.read().await;
        let actors: Vec<ActorId> = Actor::find()
            .select_only()
            .column(actor::Column::ActorId)
            .join(JoinType::InnerJoin, actor::Relation::Fragment.def())
            .filter(fragment::Column::JobId.is_in(job_ids))
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(actors)
    }

    /// Get and filter the "**root**" fragments of the specified jobs.
    /// The root fragment is the bottom-most fragment of its fragment graph, and can be a `MView` or a `Source`.
    ///
    /// Root fragment connects to downstream jobs.
    ///
    /// ## What can be the root fragment
    /// - For sink, it should have one `Sink` fragment.
    /// - For MV, it should have one `MView` fragment.
    /// - For table, it should have one `MView` fragment and one or two `Source` fragments. `MView` should be the root.
    /// - For source, it should have one `Source` fragment.
    ///
    /// In other words, it's the `MView` or `Sink` fragment if it exists, otherwise it's the `Source` fragment.
    pub async fn get_root_fragments(
        &self,
        job_ids: Vec<ObjectId>,
    ) -> MetaResult<(HashMap<ObjectId, Fragment>, Vec<(ActorId, WorkerId)>)> {
        let inner = self.inner.read().await;

        let job_definitions = resolve_streaming_job_definition(
            &inner.db,
            &HashSet::from_iter(job_ids.iter().copied()),
        )
        .await?;

        let all_fragments = FragmentModel::find()
            .filter(fragment::Column::JobId.is_in(job_ids))
            .all(&inner.db)
            .await?;
        // job_id -> fragment
        let mut root_fragments = HashMap::<ObjectId, fragment::Model>::new();
        for fragment in all_fragments {
            if (fragment.fragment_type_mask & PbFragmentTypeFlag::Mview as i32) != 0
                || (fragment.fragment_type_mask & PbFragmentTypeFlag::Sink as i32) != 0
            {
                _ = root_fragments.insert(fragment.job_id, fragment);
            } else if fragment.fragment_type_mask & PbFragmentTypeFlag::Source as i32 != 0 {
                // look for Source fragment only if there's no MView fragment
                // (notice try_insert here vs insert above)
                _ = root_fragments.try_insert(fragment.job_id, fragment);
            }
        }

        let mut root_fragments_pb = HashMap::new();
        for (_, fragment) in root_fragments {
            let actors = fragment.find_related(Actor).all(&inner.db).await?;

            let job_id = fragment.job_id;
            root_fragments_pb.insert(
                fragment.job_id,
                Self::compose_fragment(
                    fragment,
                    actors,
                    job_definitions.get(&(job_id as _)).cloned(),
                )?
                .0,
            );
        }

        let actors: Vec<(ActorId, WorkerId)> = Actor::find()
            .select_only()
            .columns([actor::Column::ActorId, actor::Column::WorkerId])
            .into_tuple()
            .all(&inner.db)
            .await?;

        Ok((root_fragments_pb, actors))
    }

    pub async fn get_root_fragment(
        &self,
        job_id: ObjectId,
    ) -> MetaResult<(Fragment, Vec<(ActorId, WorkerId)>)> {
        let (mut root_fragments, actors) = self.get_root_fragments(vec![job_id]).await?;
        let root_fragment = root_fragments
            .remove(&job_id)
            .context(format!("root fragment for job {} not found", job_id))?;
        Ok((root_fragment, actors))
    }

    /// Get the downstream fragments connected to the specified job.
    pub async fn get_downstream_fragments(
        &self,
        job_id: ObjectId,
    ) -> MetaResult<(Vec<(PbDispatcherType, Fragment)>, Vec<(ActorId, WorkerId)>)> {
        let (root_fragment, actors) = self.get_root_fragment(job_id).await?;

        let inner = self.inner.read().await;
        let downstream_fragment_relations: Vec<fragment_relation::Model> = FragmentRelation::find()
            .filter(
                fragment_relation::Column::SourceFragmentId
                    .eq(root_fragment.fragment_id as FragmentId),
            )
            .all(&inner.db)
            .await?;
        let job_definition = resolve_streaming_job_definition(&inner.db, &HashSet::from([job_id]))
            .await?
            .remove(&job_id);

        let mut downstream_fragments = vec![];
        for fragment_relation::Model {
            target_fragment_id: fragment_id,
            dispatcher_type,
            ..
        } in downstream_fragment_relations
        {
            let mut fragment_actors = FragmentModel::find_by_id(fragment_id)
                .find_with_related(Actor)
                .all(&inner.db)
                .await?;
            if fragment_actors.is_empty() {
                bail!("No fragment found for fragment id {}", fragment_id);
            }
            assert_eq!(fragment_actors.len(), 1);
            let (fragment, actors) = fragment_actors.pop().unwrap();
            let dispatch_type = PbDispatcherType::from(dispatcher_type);
            let fragment = Self::compose_fragment(fragment, actors, job_definition.clone())?.0;
            downstream_fragments.push((dispatch_type, fragment));
        }

        Ok((downstream_fragments, actors))
    }

    pub async fn load_source_fragment_ids(
        &self,
    ) -> MetaResult<HashMap<SourceId, BTreeSet<FragmentId>>> {
        let inner = self.inner.read().await;
        let mut fragments: Vec<(FragmentId, i32, StreamNode)> = FragmentModel::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::StreamNode,
            ])
            .into_tuple()
            .all(&inner.db)
            .await?;
        fragments.retain(|(_, mask, _)| *mask & PbFragmentTypeFlag::Source as i32 != 0);

        let mut source_fragment_ids = HashMap::new();
        for (fragment_id, _, stream_node) in fragments {
            if let Some(source_id) = stream_node.to_protobuf().find_stream_source() {
                source_fragment_ids
                    .entry(source_id as SourceId)
                    .or_insert_with(BTreeSet::new)
                    .insert(fragment_id);
            }
        }
        Ok(source_fragment_ids)
    }

    pub async fn load_backfill_fragment_ids(
        &self,
    ) -> MetaResult<HashMap<SourceId, BTreeSet<(FragmentId, u32)>>> {
        let inner = self.inner.read().await;
        let mut fragments: Vec<(FragmentId, i32, StreamNode)> = FragmentModel::find()
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::StreamNode,
            ])
            .into_tuple()
            .all(&inner.db)
            .await?;
        fragments.retain(|(_, mask, _)| *mask & PbFragmentTypeFlag::SourceScan as i32 != 0);

        let mut source_fragment_ids = HashMap::new();
        for (fragment_id, _, stream_node) in fragments {
            if let Some((source_id, upstream_source_fragment_id)) =
                stream_node.to_protobuf().find_source_backfill()
            {
                source_fragment_ids
                    .entry(source_id as SourceId)
                    .or_insert_with(BTreeSet::new)
                    .insert((fragment_id, upstream_source_fragment_id));
            }
        }
        Ok(source_fragment_ids)
    }

    pub async fn load_actor_splits(&self) -> MetaResult<HashMap<ActorId, ConnectorSplits>> {
        let inner = self.inner.read().await;
        let splits: Vec<(ActorId, ConnectorSplits)> = Actor::find()
            .select_only()
            .columns([actor::Column::ActorId, actor::Column::Splits])
            .filter(actor::Column::Splits.is_not_null())
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(splits.into_iter().collect())
    }

    /// Get the actor count of `Materialize` or `Sink` fragment of the specified table.
    pub async fn get_actual_job_fragment_parallelism(
        &self,
        job_id: ObjectId,
    ) -> MetaResult<Option<usize>> {
        let inner = self.inner.read().await;
        let mut fragments: Vec<(FragmentId, i32, i64)> = FragmentModel::find()
            .join(JoinType::InnerJoin, fragment::Relation::Actor.def())
            .select_only()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::FragmentTypeMask,
            ])
            .column_as(actor::Column::ActorId.count(), "count")
            .filter(fragment::Column::JobId.eq(job_id))
            .group_by(fragment::Column::FragmentId)
            .into_tuple()
            .all(&inner.db)
            .await?;

        fragments.retain(|(_, mask, _)| {
            *mask & PbFragmentTypeFlag::Mview as i32 != 0
                || *mask & PbFragmentTypeFlag::Sink as i32 != 0
        });

        Ok(fragments
            .into_iter()
            .at_most_one()
            .ok()
            .flatten()
            .map(|(_, _, count)| count as usize))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};

    use itertools::Itertools;
    use risingwave_common::hash::{ActorMapping, VirtualNode, VnodeCount};
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_common::util::stream_graph_visitor::visit_stream_node;
    use risingwave_meta_model::actor::ActorStatus;
    use risingwave_meta_model::fragment::DistributionType;
    use risingwave_meta_model::{
        ActorId, ConnectorSplits, ExprContext, FragmentId, I32Array, ObjectId, StreamNode, TableId,
        VnodeBitmap, actor, fragment,
    };
    use risingwave_pb::common::PbActorLocation;
    use risingwave_pb::meta::table_fragments::PbActorStatus;
    use risingwave_pb::meta::table_fragments::actor_status::PbActorState;
    use risingwave_pb::meta::table_fragments::fragment::PbFragmentDistributionType;
    use risingwave_pb::plan_common::PbExprContext;
    use risingwave_pb::source::{PbConnectorSplit, PbConnectorSplits};
    use risingwave_pb::stream_plan::stream_node::PbNodeBody;
    use risingwave_pb::stream_plan::{MergeNode, PbFragmentTypeFlag, PbStreamNode, PbUnionNode};

    use crate::MetaResult;
    use crate::controller::catalog::CatalogController;
    use crate::model::{Fragment, StreamActor};

    type ActorUpstreams = BTreeMap<crate::model::FragmentId, HashSet<crate::model::ActorId>>;

    type FragmentActorUpstreams = HashMap<crate::model::ActorId, ActorUpstreams>;

    const TEST_FRAGMENT_ID: FragmentId = 1;

    const TEST_UPSTREAM_FRAGMENT_ID: FragmentId = 2;

    const TEST_JOB_ID: ObjectId = 1;

    const TEST_STATE_TABLE_ID: TableId = 1000;

    fn generate_upstream_actor_ids_for_actor(actor_id: u32) -> ActorUpstreams {
        let mut upstream_actor_ids = BTreeMap::new();
        upstream_actor_ids.insert(
            TEST_UPSTREAM_FRAGMENT_ID as crate::model::FragmentId,
            HashSet::from_iter([(actor_id + 100)]),
        );
        upstream_actor_ids.insert(
            (TEST_UPSTREAM_FRAGMENT_ID + 1) as _,
            HashSet::from_iter([(actor_id + 200)]),
        );
        upstream_actor_ids
    }

    fn generate_merger_stream_node(actor_upstream_actor_ids: &ActorUpstreams) -> PbStreamNode {
        let mut input = vec![];
        for upstream_fragment_id in actor_upstream_actor_ids.keys() {
            input.push(PbStreamNode {
                node_body: Some(PbNodeBody::Merge(Box::new(MergeNode {
                    upstream_fragment_id: *upstream_fragment_id as _,
                    ..Default::default()
                }))),
                ..Default::default()
            });
        }

        PbStreamNode {
            input,
            node_body: Some(PbNodeBody::Union(PbUnionNode {})),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_extract_fragment() -> MetaResult<()> {
        let actor_count = 3u32;
        let upstream_actor_ids: FragmentActorUpstreams = (0..actor_count)
            .map(|actor_id| {
                (
                    actor_id as _,
                    generate_upstream_actor_ids_for_actor(actor_id),
                )
            })
            .collect();

        let actor_bitmaps = ActorMapping::new_uniform(
            (0..actor_count).map(|i| i as _),
            VirtualNode::COUNT_FOR_TEST,
        )
        .to_bitmaps();

        let stream_node = generate_merger_stream_node(upstream_actor_ids.values().next().unwrap());

        let pb_actors = (0..actor_count)
            .map(|actor_id| StreamActor {
                actor_id: actor_id as _,
                fragment_id: TEST_FRAGMENT_ID as _,
                vnode_bitmap: actor_bitmaps.get(&actor_id).cloned(),
                mview_definition: "".to_owned(),
                expr_context: Some(PbExprContext {
                    time_zone: String::from("America/New_York"),
                    strict_mode: false,
                }),
            })
            .collect_vec();

        let pb_fragment = Fragment {
            fragment_id: TEST_FRAGMENT_ID as _,
            fragment_type_mask: PbFragmentTypeFlag::Source as _,
            distribution_type: PbFragmentDistributionType::Hash as _,
            actors: pb_actors.clone(),
            state_table_ids: vec![TEST_STATE_TABLE_ID as _],
            maybe_vnode_count: VnodeCount::for_test().to_protobuf(),
            nodes: stream_node.clone(),
        };

        let pb_actor_status = (0..actor_count)
            .map(|actor_id| {
                (
                    actor_id,
                    PbActorStatus {
                        location: PbActorLocation::from_worker(0),
                        state: PbActorState::Running as _,
                    },
                )
            })
            .collect();

        let pb_actor_splits = Default::default();

        let (fragment, actors) = CatalogController::extract_fragment_and_actors_for_new_job(
            TEST_JOB_ID,
            &pb_fragment,
            &pb_actor_status,
            &pb_actor_splits,
        )?;

        check_fragment(fragment, pb_fragment);
        check_actors(
            actors,
            &upstream_actor_ids,
            pb_actors,
            Default::default(),
            &stream_node,
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_compose_fragment() -> MetaResult<()> {
        let actor_count = 3u32;

        let upstream_actor_ids: FragmentActorUpstreams = (0..actor_count)
            .map(|actor_id| {
                (
                    actor_id as _,
                    generate_upstream_actor_ids_for_actor(actor_id),
                )
            })
            .collect();

        let mut actor_bitmaps = ActorMapping::new_uniform(
            (0..actor_count).map(|i| i as _),
            VirtualNode::COUNT_FOR_TEST,
        )
        .to_bitmaps();

        let actors = (0..actor_count)
            .map(|actor_id| {
                let actor_splits = Some(ConnectorSplits::from(&PbConnectorSplits {
                    splits: vec![PbConnectorSplit {
                        split_type: "dummy".to_owned(),
                        ..Default::default()
                    }],
                }));

                #[expect(deprecated)]
                actor::Model {
                    actor_id: actor_id as ActorId,
                    fragment_id: TEST_FRAGMENT_ID,
                    status: ActorStatus::Running,
                    splits: actor_splits,
                    worker_id: 0,
                    upstream_actor_ids: Default::default(),
                    vnode_bitmap: actor_bitmaps
                        .remove(&actor_id)
                        .map(|bitmap| bitmap.to_protobuf())
                        .as_ref()
                        .map(VnodeBitmap::from),
                    expr_context: ExprContext::from(&PbExprContext {
                        time_zone: String::from("America/New_York"),
                        strict_mode: false,
                    }),
                }
            })
            .collect_vec();

        let stream_node = {
            let template_actor = actors.first().cloned().unwrap();

            let template_upstream_actor_ids = upstream_actor_ids
                .get(&(template_actor.actor_id as _))
                .unwrap();

            generate_merger_stream_node(template_upstream_actor_ids)
        };

        #[expect(deprecated)]
        let fragment = fragment::Model {
            fragment_id: TEST_FRAGMENT_ID,
            job_id: TEST_JOB_ID,
            fragment_type_mask: 0,
            distribution_type: DistributionType::Hash,
            stream_node: StreamNode::from(&stream_node),
            state_table_ids: I32Array(vec![TEST_STATE_TABLE_ID]),
            upstream_fragment_id: Default::default(),
            vnode_count: VirtualNode::COUNT_FOR_TEST as _,
        };

        let (pb_fragment, pb_actor_status, pb_actor_splits) =
            CatalogController::compose_fragment(fragment.clone(), actors.clone(), None).unwrap();

        assert_eq!(pb_actor_status.len(), actor_count as usize);
        assert_eq!(pb_actor_splits.len(), actor_count as usize);

        let pb_actors = pb_fragment.actors.clone();

        check_fragment(fragment, pb_fragment);
        check_actors(
            actors,
            &upstream_actor_ids,
            pb_actors,
            pb_actor_splits,
            &stream_node,
        );

        Ok(())
    }

    fn check_actors(
        actors: Vec<actor::Model>,
        actor_upstreams: &FragmentActorUpstreams,
        pb_actors: Vec<StreamActor>,
        pb_actor_splits: HashMap<u32, PbConnectorSplits>,
        stream_node: &PbStreamNode,
    ) {
        for (
            actor::Model {
                actor_id,
                fragment_id,
                status,
                splits,
                worker_id: _,
                vnode_bitmap,
                expr_context,
                ..
            },
            StreamActor {
                actor_id: pb_actor_id,
                fragment_id: pb_fragment_id,
                vnode_bitmap: pb_vnode_bitmap,
                mview_definition,
                expr_context: pb_expr_context,
                ..
            },
        ) in actors.into_iter().zip_eq_debug(pb_actors.into_iter())
        {
            assert_eq!(actor_id, pb_actor_id as ActorId);
            assert_eq!(fragment_id, pb_fragment_id as FragmentId);

            assert_eq!(
                vnode_bitmap.map(|bitmap| bitmap.to_protobuf().into()),
                pb_vnode_bitmap,
            );

            assert_eq!(mview_definition, "");

            visit_stream_node(stream_node, |body| {
                if let PbNodeBody::Merge(m) = body {
                    assert!(
                        actor_upstreams
                            .get(&(actor_id as _))
                            .unwrap()
                            .contains_key(&m.upstream_fragment_id)
                    );
                }
            });

            assert_eq!(status, ActorStatus::Running);

            assert_eq!(
                splits,
                pb_actor_splits.get(&pb_actor_id).map(ConnectorSplits::from)
            );

            assert_eq!(Some(expr_context.to_protobuf()), pb_expr_context);
        }
    }

    fn check_fragment(fragment: fragment::Model, pb_fragment: Fragment) {
        let Fragment {
            fragment_id,
            fragment_type_mask,
            distribution_type: pb_distribution_type,
            actors: _,
            state_table_ids: pb_state_table_ids,
            maybe_vnode_count: _,
            nodes,
        } = pb_fragment;

        assert_eq!(fragment_id, TEST_FRAGMENT_ID as u32);
        assert_eq!(fragment_type_mask, fragment.fragment_type_mask as u32);
        assert_eq!(
            pb_distribution_type,
            PbFragmentDistributionType::from(fragment.distribution_type)
        );

        assert_eq!(
            pb_state_table_ids,
            fragment.state_table_ids.into_u32_array()
        );
        assert_eq!(fragment.stream_node.to_protobuf(), nodes);
    }
}
