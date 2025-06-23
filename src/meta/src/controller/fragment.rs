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
use std::sync::Arc;

use anyhow::{Context, anyhow};
use futures::TryStreamExt;
use itertools::Itertools;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{FragmentTypeFlag, FragmentTypeMask};
use risingwave_common::hash::{VnodeCount, VnodeCountCompat};
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::util::stream_graph_visitor::{
    visit_stream_node_body, visit_stream_node_mut,
};
use risingwave_common::{bail, catalog};
use risingwave_connector::source::SplitImpl;
use risingwave_meta_model::actor::{ActorModel, ActorStatus};
use risingwave_meta_model::fragment::DistributionType;
use risingwave_meta_model::object::ObjectType;
use risingwave_meta_model::prelude::{
    Fragment as FragmentModel, FragmentRelation, Sink, SourceSplits, StreamingJob,
};
use risingwave_meta_model::{
    ActorId, ConnectorSplits, DatabaseId, DispatcherType, ExprContext, FragmentId, I32Array,
    JobStatus, ObjectId, SchemaId, SinkId, SourceId, StreamNode, StreamingParallelism, TableId,
    VnodeBitmap, WorkerId, database, fragment, fragment_relation, object, sink, source,
    source_splits, streaming_job, table,
};
use risingwave_meta_model_migration::{ExprTrait, OnConflict, SimpleExpr};
use risingwave_pb::catalog::PbTable;
use risingwave_pb::common::PbActorLocation;
use risingwave_pb::meta::subscribe_response::{
    Info as NotificationInfo, Operation as NotificationOperation,
};
use risingwave_pb::meta::table_fragments::actor_status::PbActorState;
use risingwave_pb::meta::table_fragments::fragment::{
    FragmentDistributionType, PbFragmentDistributionType,
};
use risingwave_pb::meta::table_fragments::{PbActorStatus, PbState};
use risingwave_pb::meta::{FragmentDistribution, PbFragmentWorkerSlotMapping};
use risingwave_pb::plan_common::PbExprContext;
use risingwave_pb::source::{ConnectorSplit, PbConnectorSplits};
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    PbDispatchOutputMapping, PbDispatcherType, PbStreamContext, PbStreamNode, PbStreamScanType,
    StreamScanType,
};
use sea_orm::ActiveValue::Set;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, FromQueryResult, JoinType, PaginatorTrait,
    QueryFilter, QuerySelect, RelationTrait, TransactionTrait,
};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::barrier::{SharedActorInfos, SharedFragmentInfo, SnapshotBackfillInfo};
use crate::controller::catalog::CatalogController;
use crate::controller::scale::{load_fragment_info, resolve_streaming_job_definition};
use crate::controller::utils::{
    FragmentDesc, PartialActorLocation, PartialFragmentStateTables, compose_dispatchers,
    get_sink_fragment_by_ids, has_table_been_migrated, rebuild_fragment_mapping,
    resolve_no_shuffle_actor_dispatcher,
};
use crate::manager::{ActiveStreamingWorkerNodes, LocalNotification, NotificationManager};
use crate::model::{
    DownstreamFragmentRelation, Fragment, FragmentActorDispatchers, FragmentDownstreamRelation,
    StreamActor, StreamContext, StreamJobFragments, TableParallelism,
};
use crate::rpc::ddl_controller::build_upstream_sink_info;
use crate::stream::{SourceManagerRef, SplitAssignment, UpstreamSinkInfo, build_actor_split_impls};
use crate::{MetaError, MetaResult};

/// Some information of running (inflight) actors.
#[derive(Clone, Debug)]
pub struct InflightActorInfo {
    pub worker_id: WorkerId,
    pub vnode_bitmap: Option<Bitmap>,
    pub splits: Vec<SplitImpl>,
}

#[derive(Clone, Debug)]
pub struct InflightFragmentInfo {
    pub fragment_id: crate::model::FragmentId,
    pub job_id: ObjectId,
    pub distribution_type: DistributionType,
    pub fragment_type_mask: FragmentTypeMask,
    pub vnode_count: usize,
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

#[easy_ext::ext(FragmentTypeMaskExt)]
pub impl FragmentTypeMask {
    fn intersects(flag: FragmentTypeFlag) -> SimpleExpr {
        Expr::col(fragment::Column::FragmentTypeMask)
            .bit_and(Expr::value(flag as i32))
            .ne(0)
    }

    fn intersects_any(flags: impl IntoIterator<Item = FragmentTypeFlag>) -> SimpleExpr {
        Expr::col(fragment::Column::FragmentTypeMask)
            .bit_and(Expr::value(FragmentTypeFlag::raw_flag(flags) as i32))
            .ne(0)
    }

    fn disjoint(flag: FragmentTypeFlag) -> SimpleExpr {
        Expr::col(fragment::Column::FragmentTypeMask)
            .bit_and(Expr::value(flag as i32))
            .eq(0)
    }
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
    pub database_id: DatabaseId,
    pub schema_id: SchemaId,
}

impl NotificationManager {
    pub(crate) fn notify_fragment_mapping(
        &self,
        operation: NotificationOperation,
        fragment_mappings: Vec<PbFragmentWorkerSlotMapping>,
    ) {
        let fragment_ids = fragment_mappings
            .iter()
            .map(|mapping| mapping.fragment_id)
            .collect_vec();
        if fragment_ids.is_empty() {
            return;
        }
        // notify all fragment mappings to frontend.
        for fragment_mapping in fragment_mappings {
            self.notify_frontend_without_version(
                operation,
                NotificationInfo::StreamingWorkerSlotMapping(fragment_mapping),
            );
        }

        // update serving vnode mappings.
        match operation {
            NotificationOperation::Add | NotificationOperation::Update => {
                self.notify_local_subscribers(LocalNotification::FragmentMappingsUpsert(
                    fragment_ids,
                ));
            }
            NotificationOperation::Delete => {
                self.notify_local_subscribers(LocalNotification::FragmentMappingsDelete(
                    fragment_ids,
                ));
            }
            op => {
                tracing::warn!("unexpected fragment mapping op: {}", op.as_str_name());
            }
        }
    }
}

impl CatalogController {
    pub fn extract_fragment_and_actors_from_fragments(
        job_id: ObjectId,
        fragments: impl Iterator<Item = &Fragment>,
        actor_status: &BTreeMap<crate::model::ActorId, PbActorStatus>,
        actor_splits: &HashMap<crate::model::ActorId, Vec<SplitImpl>>,
    ) -> MetaResult<Vec<(fragment::Model, Vec<ActorModel>)>> {
        fragments
            .map(|fragment| {
                Self::extract_fragment_and_actors_for_new_job(
                    job_id,
                    fragment,
                    actor_status,
                    actor_splits,
                )
            })
            .try_collect()
    }

    pub fn extract_fragment_and_actors_for_new_job(
        job_id: ObjectId,
        fragment: &Fragment,
        actor_status: &BTreeMap<crate::model::ActorId, PbActorStatus>,
        actor_splits: &HashMap<crate::model::ActorId, Vec<SplitImpl>>,
    ) -> MetaResult<(fragment::Model, Vec<ActorModel>)> {
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

            actors.push(ActorModel {
                actor_id: *actor_id as _,
                fragment_id: *fragment_id as _,
                status: status.get_state().unwrap().into(),
                splits,
                worker_id,
                vnode_bitmap: vnode_bitmap
                    .as_ref()
                    .map(|bitmap| VnodeBitmap::from(&bitmap.to_protobuf())),
                expr_context_2: ExprContext::from(pb_expr_context),
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
            fragment_type_mask: (*pb_fragment_type_mask).into(),
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
        fragments: Vec<(fragment::Model, Vec<ActorModel>)>,
        parallelism: StreamingParallelism,
        max_parallelism: usize,
        job_definition: Option<String>,
    ) -> MetaResult<StreamJobFragments> {
        let mut pb_fragments = BTreeMap::new();
        let mut pb_actor_status = BTreeMap::new();

        for (fragment, actors) in fragments {
            let (fragment, fragment_actor_status, _) =
                Self::compose_fragment(fragment, actors, job_definition.clone())?;

            pb_fragments.insert(fragment.fragment_id, fragment);
            pb_actor_status.extend(fragment_actor_status.into_iter());
        }

        let table_fragments = StreamJobFragments {
            stream_job_id: table_id.into(),
            state: state as _,
            fragments: pb_fragments,
            actor_status: pb_actor_status,
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
        actors: Vec<ActorModel>,
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
        visit_stream_node_body(&stream_node, |body| {
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

            let ActorModel {
                actor_id,
                fragment_id,
                status,
                worker_id,
                splits,
                vnode_bitmap,
                expr_context_2,
                ..
            } = actor;

            let vnode_bitmap =
                vnode_bitmap.map(|vnode_bitmap| Bitmap::from(vnode_bitmap.to_protobuf()));
            let pb_expr_context = Some(expr_context_2.to_protobuf());

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
            fragment_type_mask: fragment_type_mask.into(),
            distribution_type: pb_distribution_type,
            actors: pb_actors,
            state_table_ids: pb_state_table_ids,
            maybe_vnode_count: VnodeCount::set(vnode_count).to_protobuf(),
            nodes: stream_node,
        };

        Ok((pb_fragment, pb_actor_status, pb_actor_splits))
    }

    pub fn running_fragment_parallelisms(
        &self,
        id_filter: Option<HashSet<FragmentId>>,
    ) -> MetaResult<HashMap<FragmentId, FragmentParallelismInfo>> {
        let info = self.env.shared_actor_infos().read_guard();

        let mut result = HashMap::new();
        for (fragment_id, fragment) in info.iter_over_fragments() {
            if let Some(id_filter) = &id_filter
                && !id_filter.contains(&(*fragment_id as _))
            {
                continue; // Skip fragments not in the filter
            }

            result.insert(
                *fragment_id as _,
                FragmentParallelismInfo {
                    distribution_type: fragment.distribution_type.into(),
                    actor_count: fragment.actors.len() as _,
                    vnode_count: fragment.vnode_count,
                },
            );
        }

        Ok(result)
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

        let fragment_model_opt = FragmentModel::find_by_id(fragment_id)
            .one(&inner.db)
            .await?;

        // Use the cache-based result from here on
        let fragment_opt = fragment_model_opt.map(|fragment| {
            let info = self.env.shared_actor_infos().read_guard();

            let SharedFragmentInfo { actors, .. } =
                info.get_fragment(fragment.fragment_id as _).unwrap();

            FragmentDesc {
                fragment_id: fragment.fragment_id,
                job_id: fragment.job_id,
                fragment_type_mask: fragment.fragment_type_mask,
                distribution_type: fragment.distribution_type,
                state_table_ids: fragment.state_table_ids.clone(),
                vnode_count: fragment.vnode_count,
                stream_node: fragment.stream_node.clone(),
                parallelism: actors.len() as _,
            }
        });

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

    pub async fn get_fragment_ids_by_job_ids(
        &self,
        txn: &impl ConnectionTrait,
        job_id: &[ObjectId],
    ) -> MetaResult<Vec<FragmentId>> {
        let fragment_ids: Vec<FragmentId> = FragmentModel::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .filter(fragment::Column::JobId.is_in(job_id.to_vec()))
            .into_tuple()
            .all(txn)
            .await?;

        Ok(fragment_ids)
    }

    pub async fn get_job_sink_fragments_by_id(
        &self,
        job_id: ObjectId,
    ) -> MetaResult<Vec<fragment::Model>> {
        let inner = self.inner.read().await;

        let fragments: Vec<_> = FragmentModel::find()
            .columns([
                fragment::Column::FragmentId,
                fragment::Column::FragmentTypeMask,
            ])
            .filter(fragment::Column::JobId.eq(job_id))
            .all(&inner.db)
            .await?;

        let fragments = fragments
            .into_iter()
            .filter(|fragment| {
                FragmentTypeMask::from(fragment.fragment_type_mask).contains(FragmentTypeFlag::Sink)
            })
            .collect();

        Ok(fragments)
    }

    pub async fn get_job_info_by_id(&self, job_id: ObjectId) -> MetaResult<streaming_job::Model> {
        let inner = self.inner.read().await;

        let job_info = StreamingJob::find_by_id(job_id)
            .one(&inner.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("job {} not found in database", job_id))?;

        Ok(job_info)
    }

    pub async fn list_job_state_table_ids_by_id(&self, job_id: ObjectId) -> MetaResult<Vec<u32>> {
        let inner = self.inner.read().await;

        let state_table_ids: Vec<I32Array> = FragmentModel::find()
            .select_only()
            .column(fragment::Column::StateTableIds)
            .filter(fragment::Column::JobId.eq(job_id))
            .into_tuple()
            .all(&inner.db)
            .await?;

        let state_table_ids = state_table_ids
            .into_iter()
            .flat_map(|arr| arr.into_inner().into_iter().map(|id| id as u32))
            .collect_vec();

        Ok(state_table_ids)
    }

    // todo, unpatched
    pub async fn get_job_fragments_by_id(
        &self,
        job_id: ObjectId,
    ) -> MetaResult<StreamJobFragments> {
        let inner = self.inner.read().await;

        // Load fragments matching the job from the database
        let fragments: Vec<_> = FragmentModel::find()
            .filter(fragment::Column::JobId.eq(job_id))
            .all(&inner.db)
            .await?;

        let job_info = StreamingJob::find_by_id(job_id)
            .one(&inner.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("job {} not found in database", job_id))?;

        // Build (FragmentModel, Vec<ActorModel>) from the in-memory cache
        let fragment_actors_from_cache: Vec<(_, Vec<ActorModel>)> = {
            let info = self.env.shared_actor_infos().read_guard();

            let x = info.iter_over_fragments().collect_vec();

            println!("all fragments {:?}", x);
            fragments
                .into_iter()
                .map(|fm| {
                    let fragment = info.get_fragment(fm.fragment_id as _).unwrap();

                    let actors = fragment
                        .actors
                        .iter()
                        .map(|(actor_id, actor_info)| ActorModel {
                            actor_id: *actor_id as _,
                            fragment_id: fm.fragment_id,
                            status: ActorStatus::Running, // Placeholder, actual status should be fetched from DB if needed
                            worker_id: actor_info.worker_id as _,
                            splits: None, // Placeholder, actual splits should be fetched from DB if needed
                            vnode_bitmap: actor_info.vnode_bitmap.as_ref().map(|bitmap| {
                                VnodeBitmap::from(&bitmap.to_protobuf())
                            }),
                            expr_context_2: (&PbExprContext {
                                time_zone: job_info.timezone.clone().unwrap_or("".to_owned()),
                                strict_mode: false,
                            }).into(),
                        })
                        .collect();
                    (fm, actors)
                })
                .collect()
        };

        // Use cache-based result from here on
        let fragment_actors = fragment_actors_from_cache;

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

        self.get_fragment_actor_dispatchers_txn(&inner.db, fragment_ids)
            .await
    }

    pub async fn get_fragment_actor_dispatchers_txn(
        &self,
        c: &impl ConnectionTrait,
        fragment_ids: Vec<FragmentId>,
    ) -> MetaResult<FragmentActorDispatchers> {
        println!("enter get_fragment_actor_dispatchers");

        println!("read lock acquired");
        let fragment_relations = FragmentRelation::find()
            .filter(fragment_relation::Column::SourceFragmentId.is_in(fragment_ids))
            .all(c)
            .await?;

        println!("all relations");

        type FragmentActorInfo = (
            DistributionType,
            Arc<HashMap<crate::model::ActorId, Option<Bitmap>>>,
        );

        println!("xxx");
        let shared_info = self.env.shared_actor_infos();
        let mut fragment_actor_cache: HashMap<FragmentId, FragmentActorInfo> = HashMap::new();
        println!("yyyy");
        let get_fragment_actors = |fragment_id: FragmentId| async move {
            let result: MetaResult<FragmentActorInfo> = try {
                println!("try get");
                let read_guard = shared_info.read_guard();
                println!("try get read lock");

                let fragment = read_guard.get_fragment(fragment_id as _).unwrap();

                (
                    fragment.distribution_type,
                    Arc::new(
                        fragment
                            .actors
                            .iter()
                            .map(|(actor_id, actor_info)| {
                                (
                                    *actor_id,
                                    actor_info
                                        .vnode_bitmap
                                        .as_ref()
                                        .map(|bitmap| Bitmap::from(bitmap.to_protobuf())),
                                )
                            })
                            .collect(),
                    ),
                )
            };
            result
        };
        println!("zzz");

        println!("before iterating over fragment relations");
        let mut actor_dispatchers_map: HashMap<_, HashMap<_, Vec<_>>> = HashMap::new();
        for fragment_relation::Model {
            source_fragment_id,
            target_fragment_id,
            dispatcher_type,
            dist_key_indices,
            output_indices,
            output_type_mapping,
        } in fragment_relations
        {
            let (source_fragment_distribution, source_fragment_actors) = {
                let (distribution, actors) = {
                    match fragment_actor_cache.entry(source_fragment_id) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => {
                            entry.insert(get_fragment_actors(source_fragment_id).await?)
                        }
                    }
                };
                (*distribution, actors.clone())
            };
            let (target_fragment_distribution, target_fragment_actors) = {
                let (distribution, actors) = {
                    match fragment_actor_cache.entry(target_fragment_id) {
                        Entry::Occupied(entry) => entry.into_mut(),
                        Entry::Vacant(entry) => {
                            entry.insert(get_fragment_actors(target_fragment_id).await?)
                        }
                    }
                };
                (*distribution, actors.clone())
            };
            let output_mapping = PbDispatchOutputMapping {
                indices: output_indices.into_u32_array(),
                types: output_type_mapping.unwrap_or_default().to_protobuf(),
            };
            let dispatchers = compose_dispatchers(
                source_fragment_distribution,
                &source_fragment_actors,
                target_fragment_id as _,
                target_fragment_distribution,
                &target_fragment_actors,
                dispatcher_type,
                dist_key_indices.into_u32_array(),
                output_mapping,
            );
            let actor_dispatchers_map = actor_dispatchers_map
                .entry(source_fragment_id as _)
                .or_default();
            for (actor_id, dispatchers) in dispatchers {
                actor_dispatchers_map
                    .entry(actor_id as _)
                    .or_default()
                    .push(dispatchers);
            }
        }
        Ok(actor_dispatchers_map)
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
    ) -> MetaResult<HashMap<crate::model::FragmentId, PbStreamScanType>> {
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
            visit_stream_node_body(&stream_node, |body| {
                if let NodeBody::StreamScan(node) = body {
                    match node.stream_scan_type() {
                        StreamScanType::Unspecified => {}
                        scan_type => {
                            result.insert(fragment_id as crate::model::FragmentId, scan_type);
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
            .column(object::Column::DatabaseId)
            .column(object::Column::SchemaId)
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

    // /// Get all actor ids in the target streaming jobs.
    // pub async fn get_job_actor_mapping(
    //     &self,
    //     job_ids: Vec<ObjectId>,
    // ) -> MetaResult<HashMap<ObjectId, Vec<ActorId>>> {
    //     let inner = self.inner.read().await;
    //
    //     let fragment_job_ids: Vec<(FragmentId, ObjectId)> = FragmentModel::find()
    //         .select_only()
    //         .columns([fragment::Column::FragmentId, fragment::Column::JobId])
    //         .filter(fragment::Column::JobId.is_in(job_ids.clone()))
    //         .into_tuple()
    //         .all(&inner.db)
    //         .await?;
    //
    //     let job_actors: HashSet<(_, _)> = fragment_job_ids
    //         .into_iter()
    //         .flat_map(|(fragment_id, job_id)| {
    //             let actors = inner
    //                 .actors
    //                 .actors_by_fragment_id
    //                 .get(&fragment_id)
    //                 .cloned()
    //                 .unwrap_or_default();
    //
    //             actors.into_iter().map(move |actor_id| (job_id, actor_id))
    //         })
    //         .collect();
    //
    //     {
    //         let job_actors_from_db: Vec<(ObjectId, ActorId)> = Actor::find()
    //             .select_only()
    //             .column(fragment::Column::JobId)
    //             .column(actor::Column::ActorId)
    //             .join(JoinType::InnerJoin, actor::Relation::Fragment.def())
    //             .filter(fragment::Column::JobId.is_in(job_ids))
    //             .into_tuple()
    //             .all(&inner.db)
    //             .await?;
    //
    //         let job_actors_from_db = job_actors_from_db
    //             .into_iter()
    //             .map(|(job_id, actor_id)| (job_id as ObjectId, actor_id as ActorId))
    //             .collect::<HashSet<_>>();
    //
    //         debug_assert_eq!(job_actors, job_actors_from_db);
    //     }
    //
    //     Ok(job_actors.into_iter().into_group_map())
    // }

    /// Try to get internal table ids of each streaming job, used by metrics collection.
    pub async fn get_job_internal_table_ids(&self) -> Option<Vec<(ObjectId, Vec<TableId>)>> {
        if let Ok(inner) = self.inner.try_read()
            && let Ok(job_state_tables) = FragmentModel::find()
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
        None
    }

    pub async fn has_any_running_jobs(&self) -> MetaResult<bool> {
        let inner = self.inner.read().await;
        let count = FragmentModel::find().count(&inner.db).await?;
        Ok(count > 0)
    }

    pub fn worker_actor_count(&self) -> MetaResult<HashMap<WorkerId, usize>> {
        let read_guard = self.env.shared_actor_infos().read_guard();
        let actor_cnt: HashMap<WorkerId, _> = read_guard
            .iter_over_fragments()
            .flat_map(|(_, fragment)| {
                fragment
                    .actors
                    .iter()
                    .map(|(actor_id, actor)| (actor.worker_id, *actor_id))
            })
            .into_group_map()
            .into_iter()
            .map(|(k, v)| (k, v.len()))
            .collect();

        Ok(actor_cnt)
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

        println!("calling table_fragments");
        let mut table_fragments = BTreeMap::new();
        for job in jobs {
            println!("\tcalling table_fragments for job {}", job.job_id);
            let fragments = FragmentModel::find()
                .filter(fragment::Column::JobId.eq(job.job_id))
                .all(&inner.db)
                .await?;

            let fragment_actors = {
                let guard = self.env.shared_actor_infos().read_guard();

                fragments
                    .into_iter()
                    .map(|fragment| {
                        println!(
                            "\t\tcalling table_fragments for job {} fragment {}",
                            job.job_id, fragment.fragment_id
                        );

                        let fragment_info = guard.get_fragment(fragment.fragment_id as _).unwrap();

                        for (actor_id, actor) in &fragment_info.actors {
                            println!("\t\t\tactor id {} worker id {}", actor_id, actor.worker_id);
                        }

                        let actors = fragment_info
                            .actors
                            .iter()
                            .map(|(actor_id, actor_info)| ActorModel {
                                actor_id: *actor_id as _,
                                fragment_id: fragment.fragment_id as _,
                                status: ActorStatus::Running,
                                splits: None, // Placeholder, actual expr_context should be fetched from DB if needed
                                worker_id: actor_info.worker_id,
                                vnode_bitmap: actor_info.vnode_bitmap.as_ref().map(|bitmap| {
                                    VnodeBitmap::from(&bitmap.to_protobuf())
                                }),
                                expr_context_2: (&PbExprContext {
                                    time_zone: job.timezone.clone().unwrap_or("".to_owned()),
                                    strict_mode: false,
                                }).into()
                            })
                            .collect();

                        (fragment, actors)
                    })
                    .collect()
            };

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

    /// Returns pairs of (job id, actor ids), where actors belong to CDC table backfill fragment of the job.
    pub fn cdc_table_backfill_actor_ids(&self) -> MetaResult<HashMap<u32, HashSet<u32>>> {
        let guard = self.env.shared_actor_info.read_guard();
        let job_id_actor_ids = guard
            .iter_over_fragments()
            .filter(|(_, fragment)| {
                fragment
                    .fragment_type_mask
                    .contains(FragmentTypeFlag::StreamCdcScan)
            })
            .map(|(_, fragment)| {
                let job_id = fragment.job_id as u32;
                let actor_ids: HashSet<u32> = fragment.actors.keys().copied().collect();
                (job_id, actor_ids)
            })
            .into_group_map()
            .into_iter()
            .map(|(job_id, actor_ids_vec)| {
                let actor_ids = actor_ids_vec.into_iter().flatten().collect();
                (job_id, actor_ids)
            })
            .collect();

        Ok(job_id_actor_ids)
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

    pub fn list_actor_locations(&self) -> MetaResult<Vec<PartialActorLocation>> {
        let info = self.env.shared_actor_infos().read_guard();

        let actor_locations = info
            .iter_over_fragments()
            .flat_map(|(fragment_id, fragment)| {
                fragment
                    .actors
                    .iter()
                    .map(|(actor_id, actor)| PartialActorLocation {
                        actor_id: *actor_id as _,
                        fragment_id: *fragment_id as _,
                        worker_id: actor.worker_id,
                        status: ActorStatus::Running,
                    })
            })
            .collect_vec();

        Ok(actor_locations)
    }

    pub async fn list_actor_info(
        &self,
    ) -> MetaResult<Vec<(ActorId, FragmentId, ObjectId, SchemaId, ObjectType)>> {
        let inner = self.inner.read().await;

        let fragment_objects: Vec<(FragmentId, ObjectId, SchemaId, ObjectType, DatabaseId)> =
            FragmentModel::find()
                .select_only()
                .join(JoinType::LeftJoin, fragment::Relation::Object.def())
                .column_as(object::Column::Oid, "job_id")
                .column_as(object::Column::SchemaId, "schema_id")
                .column_as(object::Column::ObjType, "type")
                .column_as(object::Column::DatabaseId, "database_id")
                .into_tuple()
                .all(&inner.db)
                .await?;

        let actor_infos = {
            let info = self.env.shared_actor_infos().read_guard();

            fragment_objects
                .into_iter()
                .flat_map(
                    |(fragment_id, object_id, schema_id, object_type, database_id)| {
                        let SharedFragmentInfo {
                            fragment_id,
                            actors,
                            ..
                        } = info.get_fragment(*&(fragment_id as _)).unwrap();
                        actors.keys().map(move |actor_id| {
                            (
                                *actor_id as _,
                                *fragment_id as _,
                                object_id,
                                schema_id,
                                object_type,
                            )
                        })
                    },
                )
                .collect_vec()
        };

        Ok(actor_infos)
    }

    pub fn get_worker_slot_mappings(&self) -> Vec<PbFragmentWorkerSlotMapping> {
        let guard = self.env.shared_actor_info.read_guard();
        guard
            .iter_over_fragments()
            .map(|(_, fragment)| rebuild_fragment_mapping(fragment))
            .collect_vec()
    }

    pub async fn list_fragment_descs(
        &self,
        is_creating: bool,
    ) -> MetaResult<Vec<(FragmentDistribution, Vec<FragmentId>)>> {
        let inner = self.inner.read().await;

        let txn = inner.db.begin().await?;

        let fragments: Vec<_> = if is_creating {
            FragmentModel::find()
                .join(JoinType::LeftJoin, fragment::Relation::Object.def())
                .join(JoinType::LeftJoin, object::Relation::StreamingJob.def())
                .filter(
                    streaming_job::Column::JobStatus
                        .eq(JobStatus::Initial)
                        .or(streaming_job::Column::JobStatus.eq(JobStatus::Creating)),
                )
                .all(&txn)
                .await?
        } else {
            FragmentModel::find().all(&txn).await?
        };

        let fragment_ids = fragments.iter().map(|f| f.fragment_id).collect_vec();

        let upstreams: Vec<(FragmentId, FragmentId)> = FragmentRelation::find()
            .select_only()
            .columns([
                fragment_relation::Column::TargetFragmentId,
                fragment_relation::Column::SourceFragmentId,
            ])
            .filter(fragment_relation::Column::TargetFragmentId.is_in(fragment_ids))
            .into_tuple()
            .all(&txn)
            .await?;

        let guard = self.env.shared_actor_info.read_guard();

        let mut result = Vec::new();

        let mut all_upstreams = upstreams.into_iter().into_group_map();

        for fragment_desc in fragments {
            let parallelism = guard
                .get_fragment(fragment_desc.fragment_id as _)
                .unwrap()
                .actors
                .len();

            let upstreams = all_upstreams
                .remove(&fragment_desc.fragment_id)
                .unwrap_or_default();

            let fragment = FragmentDistribution {
                fragment_id: fragment_desc.fragment_id as _,
                table_id: fragment_desc.job_id as _,
                distribution_type: PbFragmentDistributionType::from(fragment_desc.distribution_type)
                    as _,
                state_table_ids: fragment_desc.state_table_ids.into_u32_array(),
                upstream_fragment_ids: upstreams.iter().map(|id| *id as _).collect(),
                fragment_type_mask: fragment_desc.fragment_type_mask as _,
                parallelism: parallelism as _,
                vnode_count: fragment_desc.vnode_count as _,
                node: Some(fragment_desc.stream_node.to_protobuf()),
            };

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

        let actor_with_type: Vec<(ActorId, ObjectId)> = {
            let info = self.env.shared_actor_infos().read_guard();

            info.iter_over_fragments()
                .filter(|(_, fragment)| {
                    sink_ids.contains(&fragment.job_id)
                        && fragment.fragment_type_mask.contains(FragmentTypeFlag::Sink)
                })
                .flat_map(|(_, fragment)| {
                    fragment
                        .actors
                        .keys()
                        .map(move |actor_id| (*actor_id as _, fragment.job_id as _))
                })
                .collect()
        };

        let mut sink_actor_mapping = HashMap::new();
        for (actor_id, sink_id) in actor_with_type {
            sink_actor_mapping
                .entry(sink_id)
                .or_insert_with(|| (sink_name_mapping.get(&sink_id).unwrap().clone(), vec![]))
                .1
                .push(actor_id);
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
    pub async fn load_all_actors_dynamic(
        &self,
        database_id: Option<DatabaseId>,
        worker_nodes: &ActiveStreamingWorkerNodes,
        source_manager_ref: SourceManagerRef,
    ) -> MetaResult<HashMap<DatabaseId, HashMap<TableId, HashMap<FragmentId, InflightFragmentInfo>>>>
    {
        let adaptive_parallelism_strategy = {
            let system_params_reader = self.env.system_params_reader().await;
            system_params_reader.adaptive_parallelism_strategy()
        };

        let id_gen = self.env.id_gen_manager();

        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        println!("111");
        let database_fragment_infos = load_fragment_info(
            &txn,
            id_gen,
            database_id,
            worker_nodes,
            adaptive_parallelism_strategy,
        )
        .await?;

        debug!(?database_fragment_infos, "reload all actors");

        Ok(database_fragment_infos)
    }

    // pub async fn migrate_actors(
    //     &self,
    //     plan: HashMap<WorkerSlotId, WorkerSlotId>,
    // ) -> MetaResult<()> {
    //     let inner = self.inner.write().await;
    //     let txn = inner.db.begin().await?;
    //
    //     let actors: Vec<(
    //         FragmentId,
    //         DistributionType,
    //         ActorId,
    //         Option<VnodeBitmap>,
    //         WorkerId,
    //         ActorStatus,
    //     )> = Actor::find()
    //         .select_only()
    //         .columns([
    //             fragment::Column::FragmentId,
    //             fragment::Column::DistributionType,
    //         ])
    //         .columns([
    //             actor::Column::ActorId,
    //             actor::Column::VnodeBitmap,
    //             actor::Column::WorkerId,
    //             actor::Column::Status,
    //         ])
    //         .join(JoinType::InnerJoin, actor::Relation::Fragment.def())
    //         .into_tuple()
    //         .all(&txn)
    //         .await?;
    //
    //     let mut actor_locations = HashMap::new();
    //
    //     for (fragment_id, _, actor_id, _, worker_id, status) in &actors {
    //         if *status != ActorStatus::Running {
    //             tracing::warn!(
    //                 "skipping actor {} in fragment {} with status {:?}",
    //                 actor_id,
    //                 fragment_id,
    //                 status
    //             );
    //             continue;
    //         }
    //
    //         actor_locations
    //             .entry(*worker_id)
    //             .or_insert(HashMap::new())
    //             .entry(*fragment_id)
    //             .or_insert(BTreeSet::new())
    //             .insert(*actor_id);
    //     }
    //
    //     let expired_or_changed_workers: HashSet<_> =
    //         plan.keys().map(|k| k.worker_id() as WorkerId).collect();
    //
    //     let mut actor_migration_plan = HashMap::new();
    //     for (worker, fragment) in actor_locations {
    //         if expired_or_changed_workers.contains(&worker) {
    //             for (fragment_id, actors) in fragment {
    //                 debug!(
    //                     "worker {} expired or changed, migrating fragment {}",
    //                     worker, fragment_id
    //                 );
    //                 let worker_slot_to_actor: HashMap<_, _> = actors
    //                     .iter()
    //                     .enumerate()
    //                     .map(|(idx, actor_id)| {
    //                         (WorkerSlotId::new(worker as _, idx as _), *actor_id)
    //                     })
    //                     .collect();
    //
    //                 for (worker_slot, actor) in worker_slot_to_actor {
    //                     if let Some(target) = plan.get(&worker_slot) {
    //                         actor_migration_plan.insert(actor, target.worker_id() as WorkerId);
    //                     }
    //                 }
    //             }
    //         }
    //     }
    //
    //     for (actor, worker) in &actor_migration_plan {
    //         Actor::update_many()
    //             .col_expr(
    //                 actor::Column::WorkerId,
    //                 Expr::value(Value::Int(Some(*worker))),
    //             )
    //             .filter(actor::Column::ActorId.eq(*actor))
    //             .exec(&txn)
    //             .await?;
    //     }
    //
    //     txn.commit().await?;
    //
    //     Ok(())
    // }

    // pub async fn all_inuse_worker_slots(&self) -> MetaResult<HashSet<WorkerSlotId>> {
    //     let inner = self.inner.read().await;
    //
    //     // Build the fragment–actor–worker triples entirely from the in-memory cache
    //     let actors_from_cache: Vec<(FragmentId, ActorId, WorkerId)> = inner
    //         .actors
    //         .actors_by_fragment_id
    //         .iter()
    //         .flat_map(|(fragment_id, actor_ids)| {
    //             actor_ids.iter().map(|actor_id| {
    //                 let worker_id = inner.actors.models[actor_id].worker_id;
    //                 (*fragment_id, *actor_id, worker_id)
    //             })
    //         })
    //         .collect();
    //
    //     {
    //         // Execute original DB query for comparison
    //         let actors_from_db: Vec<(FragmentId, ActorId, WorkerId)> = Actor::find()
    //             .select_only()
    //             .columns([fragment::Column::FragmentId])
    //             .columns([actor::Column::ActorId, actor::Column::WorkerId])
    //             .join(JoinType::InnerJoin, actor::Relation::Fragment.def())
    //             .into_tuple()
    //             .all(&inner.db)
    //             .await?;
    //
    //         // Compare as sets to ignore ordering differences
    //         let set_db: HashSet<(FragmentId, ActorId, WorkerId)> =
    //             actors_from_db.into_iter().collect();
    //         let set_cache: HashSet<(FragmentId, ActorId, WorkerId)> =
    //             actors_from_cache.iter().copied().collect();
    //
    //         debug_assert_eq!(
    //             set_db, set_cache,
    //             "Fragment–Actor–Worker triples mismatch between DB and cache"
    //         );
    //     }
    //
    //     // Use the cache-based result from here on
    //     let actors = actors_from_cache;
    //     let mut actor_locations = HashMap::new();
    //
    //     for (fragment_id, _, worker_id) in actors {
    //         *actor_locations
    //             .entry(worker_id)
    //             .or_insert(HashMap::new())
    //             .entry(fragment_id)
    //             .or_insert(0_usize) += 1;
    //     }
    //
    //     let mut result = HashSet::new();
    //     for (worker_id, mapping) in actor_locations {
    //         let max_fragment_len = mapping.values().max().unwrap();
    //
    //         result
    //             .extend((0..*max_fragment_len).map(|idx| WorkerSlotId::new(worker_id as u32, idx)))
    //     }
    //
    //     Ok(result)
    // }

    // pub async fn all_node_actors(
    //     &self,
    //     include_inactive: bool,
    // ) -> MetaResult<HashMap<WorkerId, Vec<StreamActor>>> {
    //     let inner = self.inner.read().await;
    //     // Build fragment–actor mapping entirely from the in-memory cache
    //     let fragment_models: Vec<_> = FragmentModel::find().all(&inner.db).await?;
    //     let fragment_actors_from_cache: Vec<(_, Vec<actor::Model>)> = fragment_models
    //         .into_iter()
    //         .map(|fragment| {
    //             // collect actor IDs for this fragment
    //             let actor_ids = inner
    //                 .actors
    //                 .actors_by_fragment_id
    //                 .get(&fragment.fragment_id)
    //                 .cloned()
    //                 .unwrap_or_default();
    //             // filter by status if needed, then collect models
    //             let models = actor_ids
    //                 .into_iter()
    //                 .filter_map(|actor_id| inner.actors.models.get(&actor_id))
    //                 .filter(|m| include_inactive || m.status == ActorStatus::Running)
    //                 .cloned()
    //                 .collect();
    //             (fragment, models)
    //         })
    //         .collect();
    //
    //     {
    //         // Execute the original DB query for comparison
    //         let fragment_actors_from_db: Vec<(_, Vec<actor::Model>)> = if include_inactive {
    //             FragmentModel::find()
    //                 .find_with_related(Actor)
    //                 .all(&inner.db)
    //                 .await?
    //         } else {
    //             FragmentModel::find()
    //                 .find_with_related(Actor)
    //                 .filter(actor::Column::Status.eq(ActorStatus::Running))
    //                 .all(&inner.db)
    //                 .await?
    //         };
    //
    //         // Build maps from fragment_id → set of actor_ids
    //         let map_db: HashMap<FragmentId, HashSet<ActorId>> = fragment_actors_from_db
    //             .into_iter()
    //             .map(|(fm, acts)| {
    //                 let set = acts.into_iter().map(|a| a.actor_id).collect();
    //                 (fm.fragment_id, set)
    //             })
    //             .collect();
    //         let map_cache: HashMap<FragmentId, HashSet<ActorId>> = fragment_actors_from_cache
    //             .iter()
    //             .map(|(fm, acts)| {
    //                 let set = acts.iter().map(|a| a.actor_id).collect();
    //                 (fm.fragment_id, set)
    //             })
    //             .collect();
    //
    //         debug_assert_eq!(
    //             map_db, map_cache,
    //             "Fragment–actor mapping mismatch between DB and cache"
    //         );
    //     }
    //
    //     // Use the cache-based result from here on
    //     let fragment_actors = fragment_actors_from_cache;
    //
    //     let job_definitions = resolve_streaming_job_definition(
    //         &inner.db,
    //         &HashSet::from_iter(fragment_actors.iter().map(|(fragment, _)| fragment.job_id)),
    //     )
    //     .await?;
    //
    //     let mut node_actors = HashMap::new();
    //     for (fragment, actors) in fragment_actors {
    //         let job_id = fragment.job_id;
    //         let (table_fragments, actor_status, _) = Self::compose_fragment(
    //             fragment,
    //             actors,
    //             job_definitions.get(&(job_id as _)).cloned(),
    //         )?;
    //         for actor in table_fragments.actors {
    //             let node_id = actor_status[&actor.actor_id].worker_id() as WorkerId;
    //             node_actors
    //                 .entry(node_id)
    //                 .or_insert_with(Vec::new)
    //                 .push(actor);
    //         }
    //     }
    //
    //     Ok(node_actors)
    // }

    // pub async fn get_worker_actor_ids(
    //     &self,
    //     job_ids: Vec<ObjectId>,
    // ) -> MetaResult<BTreeMap<WorkerId, Vec<ActorId>>> {
    //     let inner = self.inner.read().await;
    //
    //     // 1. Load fragment IDs for the given jobs from the database
    //     let fragment_ids: Vec<FragmentId> = FragmentModel::find()
    //         .select_only()
    //         .column(fragment::Column::FragmentId)
    //         .filter(fragment::Column::JobId.is_in(job_ids.clone()))
    //         .into_tuple()
    //         .all(&inner.db)
    //         .await?;
    //
    //     // 2. Build (ActorId, WorkerId) pairs entirely from in-memory cache
    //     let actor_workers_from_cache: Vec<(ActorId, WorkerId)> = fragment_ids
    //         .iter()
    //         .flat_map(|&fid| {
    //             let actors = inner
    //                 .actors
    //                 .actors_by_fragment_id
    //                 .get(&fid)
    //                 .cloned()
    //                 .unwrap_or_default();
    //
    //             actors.into_iter().map(|actor_id| {
    //                 let m = &inner.actors.models[&actor_id];
    //                 (actor_id, m.worker_id)
    //             })
    //         })
    //         .collect();
    //
    //     {
    //         // Execute the original DB query for comparison
    //         let actor_workers_from_db: Vec<(ActorId, WorkerId)> = Actor::find()
    //             .select_only()
    //             .columns([actor::Column::ActorId, actor::Column::WorkerId])
    //             .join(JoinType::InnerJoin, actor::Relation::Fragment.def())
    //             .filter(fragment::Column::JobId.is_in(job_ids))
    //             .into_tuple()
    //             .all(&inner.db)
    //             .await?;
    //
    //         // Compare as sets to ignore ordering
    //         let set_db: HashSet<(ActorId, WorkerId)> = actor_workers_from_db.into_iter().collect();
    //         let set_cache: HashSet<(ActorId, WorkerId)> =
    //             actor_workers_from_cache.iter().copied().collect();
    //
    //         debug_assert_eq!(
    //             set_db, set_cache,
    //             "Actor–Worker pairs mismatch between DB and cache"
    //         );
    //     }
    //
    //     // 3. Use the cache-based result from here on
    //     let actor_workers = actor_workers_from_cache;
    //
    //     let mut worker_actors = BTreeMap::new();
    //     for (actor_id, worker_id) in actor_workers {
    //         worker_actors
    //             .entry(worker_id)
    //             .or_insert_with(Vec::new)
    //             .push(actor_id);
    //     }
    //
    //     Ok(worker_actors)
    // }

    pub async fn update_actor_splits(&self, _split_assignment: &SplitAssignment) -> MetaResult<()> {
        // qq: why read() here?
        let _inner = self.inner.write().await;
        // let txn = inner.db.begin().await?;
        // for assignments in split_assignment.values() {
        //     for (actor_id, splits) in assignments {
        //         let actor_splits = splits.iter().map(Into::into).collect_vec();
        //         Actor::update(actor::ActiveModel {
        //             actor_id: Set(*actor_id as _),
        //             splits: Set(Some(ConnectorSplits::from(&PbConnectorSplits {
        //                 splits: actor_splits,
        //             }))),
        //             ..Default::default()
        //         })
        //         .exec(&txn)
        //         .await
        //         .map_err(|err| {
        //             if err == DbErr::RecordNotUpdated {
        //                 MetaError::catalog_id_not_found("actor_id", actor_id)
        //             } else {
        //                 err.into()
        //             }
        //         })?;
        //     }
        // }
        // txn.commit().await?;

        // for assignments in split_assignment.values() {
        //     for (&actor_id, splits) in assignments {
        //         inner.actors.mutate_actor(actor_id as ActorId, |actor| {
        //             actor.splits = Some(ConnectorSplits::from(&PbConnectorSplits {
        //                 splits: splits.iter().map(Into::into).collect_vec(),
        //             }));
        //         });
        //     }
        // }

        Ok(())
    }

    #[await_tree::instrument]
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
    pub fn get_running_actors_of_fragment(
        &self,
        fragment_id: FragmentId,
    ) -> MetaResult<Vec<ActorId>> {
        let info = self.env.shared_actor_infos().read_guard();

        let actors = info
            .get_fragment(fragment_id as _)
            .map(|SharedFragmentInfo { actors, .. }| actors.keys().map(|id| *id as _).collect_vec())
            .unwrap_or_default();

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

        let load_fragment_actor_distribution =
            |actor_info: &SharedActorInfos,
             fragment_id: FragmentId|
             -> HashMap<crate::model::ActorId, Option<Bitmap>> {
                let guard = actor_info.read_guard();

                guard
                    .get_fragment(fragment_id as _)
                    .map(|fragment| {
                        fragment
                            .actors
                            .iter()
                            .map(|(actor_id, actor)| {
                                (
                                    *actor_id as _,
                                    actor
                                        .vnode_bitmap
                                        .as_ref()
                                        .map(|bitmap| Bitmap::from(bitmap.to_protobuf())),
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            };

        let source_backfill_actors: HashMap<crate::model::ActorId, Option<Bitmap>> =
            load_fragment_actor_distribution(
                self.env.shared_actor_infos(),
                source_backfill_fragment_id,
            );

        let source_actors =
            load_fragment_actor_distribution(self.env.shared_actor_infos(), source_fragment_id);

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
    ) -> MetaResult<(
        HashMap<ObjectId, (SharedFragmentInfo, PbStreamNode)>,
        Vec<(ActorId, WorkerId)>,
    )> {
        let inner = self.inner.read().await;

        let all_fragments = FragmentModel::find()
            .filter(fragment::Column::JobId.is_in(job_ids))
            .all(&inner.db)
            .await?;
        // job_id -> fragment
        let mut root_fragments = HashMap::<ObjectId, fragment::Model>::new();
        for fragment in all_fragments {
            let mask = FragmentTypeMask::from(fragment.fragment_type_mask);
            if mask.contains_any([FragmentTypeFlag::Mview, FragmentTypeFlag::Sink]) {
                _ = root_fragments.insert(fragment.job_id, fragment);
            } else if mask.contains(FragmentTypeFlag::Source) {
                // look for Source fragment only if there's no MView fragment
                // (notice try_insert here vs insert above)
                _ = root_fragments.try_insert(fragment.job_id, fragment);
            }
        }

        let mut root_fragments_pb = HashMap::new();

        let info = self.env.shared_actor_infos().read_guard();

        let root_fragment_to_jobs: HashMap<_, _> = root_fragments
            .iter()
            .map(|(k, v)| (v.fragment_id as u32, *k))
            .collect();

        // for (fragment_id, fragment_info) in info
        //     .values()
        //     .flatten()
        //     .filter(|&(fragment_id, _)| root_fragment_to_jobs.contains_key(fragment_id))
        // {

        // }

        for fragment in root_fragment_to_jobs.keys() {
            let fragment_info = info.get_fragment(*fragment).unwrap();

            let job_id = root_fragment_to_jobs[&fragment_info.fragment_id];
            let fragment = root_fragments
                .get(&job_id)
                .context(format!("root fragment for job {} not found", job_id))?;

            root_fragments_pb.insert(
                job_id,
                (fragment_info.clone(), fragment.stream_node.to_protobuf()),
            );
        }

        let mut all_actor_locations = vec![];

        for (_, SharedFragmentInfo { actors, .. }) in info.iter_over_fragments() {
            for (actor_id, actor_info) in actors {
                all_actor_locations.push((*actor_id as ActorId, actor_info.worker_id));
            }
        }

        println!("actor locations should be optimized");

        Ok((root_fragments_pb, all_actor_locations))
    }

    pub async fn get_root_fragment(
        &self,
        job_id: ObjectId,
    ) -> MetaResult<(SharedFragmentInfo, HashMap<u32, WorkerId>)> {
        let (mut root_fragments, actors) = self.get_root_fragments(vec![job_id]).await?;
        let (root_fragment, _) = root_fragments
            .remove(&job_id)
            .context(format!("root fragment for job {} not found", job_id))?;

        Ok((
            root_fragment,
            actors.into_iter().map(|(k, v)| (k as _, v)).collect(),
        ))
    }

    /// Get the downstream fragments connected to the specified job.
    pub async fn get_downstream_fragments(
        &self,
        job_id: ObjectId,
    ) -> MetaResult<(
        Vec<(
            stream_plan::DispatcherType,
            SharedFragmentInfo,
            PbStreamNode,
        )>,
        HashMap<u32, WorkerId>,
    )> {
        let (root_fragment, actor_locations) = self.get_root_fragment(job_id).await?;

        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;
        let downstream_fragment_relations: Vec<fragment_relation::Model> = FragmentRelation::find()
            .filter(
                fragment_relation::Column::SourceFragmentId
                    .eq(root_fragment.fragment_id as FragmentId),
            )
            .all(&txn)
            .await?;

        // todo, optimize
        let downstream_fragment_ids = downstream_fragment_relations
            .iter()
            .map(|model| model.target_fragment_id as FragmentId)
            .collect::<HashSet<_>>();

        let downstream_fragment_nodes: Vec<(FragmentId, StreamNode)> = FragmentModel::find()
            .select_only()
            .columns([fragment::Column::FragmentId, fragment::Column::StreamNode])
            .filter(fragment::Column::FragmentId.is_in(downstream_fragment_ids))
            .into_tuple()
            .all(&txn)
            .await?;

        let downstream_fragment_nodes: HashMap<_, _> = downstream_fragment_nodes
            .into_iter()
            .map(|(id, node)| (id as u32, node))
            .collect();

        let mut downstream_fragments = vec![];

        let info = self.env.shared_actor_infos().read_guard();

        let fragment_map: HashMap<_, _> = downstream_fragment_relations
            .iter()
            .map(|model| (model.target_fragment_id as u32, model.dispatcher_type))
            .collect();

        for fragment_id in fragment_map.keys() {
            let fragment_info @ SharedFragmentInfo { actors, .. } =
                info.get_fragment(*fragment_id).unwrap();

            let dispatcher_type = fragment_map[fragment_id];

            if actors.is_empty() {
                bail!("No fragment found for fragment id {}", fragment_id);
            }

            let dispatch_type = PbDispatcherType::from(dispatcher_type);

            let nodes = downstream_fragment_nodes
                .get(fragment_id)
                .context(format!(
                    "downstream fragment node for id {} not found",
                    fragment_id
                ))?
                .to_protobuf();

            downstream_fragments.push((dispatch_type, fragment_info.clone(), nodes));
        }
        Ok((downstream_fragments, actor_locations))
    }

    pub async fn load_source_fragment_ids(
        &self,
    ) -> MetaResult<HashMap<SourceId, BTreeSet<FragmentId>>> {
        let inner = self.inner.read().await;
        let fragments: Vec<(FragmentId, StreamNode)> = FragmentModel::find()
            .select_only()
            .columns([fragment::Column::FragmentId, fragment::Column::StreamNode])
            .filter(FragmentTypeMask::intersects(FragmentTypeFlag::Source))
            .into_tuple()
            .all(&inner.db)
            .await?;

        let mut source_fragment_ids = HashMap::new();
        for (fragment_id, stream_node) in fragments {
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
        let fragments: Vec<(FragmentId, StreamNode)> = FragmentModel::find()
            .select_only()
            .columns([fragment::Column::FragmentId, fragment::Column::StreamNode])
            .filter(FragmentTypeMask::intersects(FragmentTypeFlag::SourceScan))
            .into_tuple()
            .all(&inner.db)
            .await?;

        let mut source_fragment_ids = HashMap::new();
        for (fragment_id, stream_node) in fragments {
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

    pub async fn get_all_upstream_sink_infos(
        &self,
        target_table: &PbTable,
        target_fragment_id: FragmentId,
    ) -> MetaResult<Vec<UpstreamSinkInfo>> {
        let incoming_sinks = self.get_table_incoming_sinks(target_table.id as _).await?;

        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let sink_ids = incoming_sinks.iter().map(|s| s.id as SinkId).collect_vec();
        let sink_fragment_ids = get_sink_fragment_by_ids(&txn, sink_ids).await?;

        let mut upstream_sink_infos = Vec::with_capacity(incoming_sinks.len());
        for pb_sink in &incoming_sinks {
            let sink_fragment_id =
                sink_fragment_ids
                    .get(&(pb_sink.id as _))
                    .cloned()
                    .ok_or(anyhow::anyhow!(
                        "sink fragment not found for sink id {}",
                        pb_sink.id
                    ))?;
            let upstream_info = build_upstream_sink_info(
                pb_sink,
                sink_fragment_id,
                target_table,
                target_fragment_id,
            )?;
            upstream_sink_infos.push(upstream_info);
        }

        Ok(upstream_sink_infos)
    }

    pub async fn get_mview_fragment_by_id(&self, table_id: TableId) -> MetaResult<FragmentId> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let mview_fragment: Vec<FragmentId> = FragmentModel::find()
            .select_only()
            .column(fragment::Column::FragmentId)
            .filter(
                fragment::Column::JobId
                    .eq(table_id)
                    .and(FragmentTypeMask::intersects(FragmentTypeFlag::Mview)),
            )
            .into_tuple()
            .all(&txn)
            .await?;

        if mview_fragment.len() != 1 {
            return Err(anyhow::anyhow!(
                "expected exactly one mview fragment for table {}, found {}",
                table_id,
                mview_fragment.len()
            )
            .into());
        }

        Ok(mview_fragment.into_iter().next().unwrap())
    }

    pub async fn has_table_been_migrated(&self, table_id: TableId) -> MetaResult<bool> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;
        has_table_been_migrated(&txn, table_id).await
    }

    pub async fn update_source_splits(
        &self,
        source_splits: &HashMap<SourceId, Vec<SplitImpl>>,
    ) -> MetaResult<()> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;

        let models: Vec<source_splits::ActiveModel> = source_splits
            .iter()
            .map(|(source_id, splits)| source_splits::ActiveModel {
                source_id: Set(*source_id as _),
                splits: Set(Some(ConnectorSplits::from(&PbConnectorSplits {
                    splits: splits.iter().map(Into::into).collect_vec(),
                }))),
            })
            .collect();

        SourceSplits::insert_many(models)
            .on_conflict(
                OnConflict::column(source_splits::Column::SourceId)
                    .update_column(source_splits::Column::Splits)
                    .to_owned(),
            )
            .exec(&txn)
            .await?;

        txn.commit().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};

    use itertools::Itertools;
    use risingwave_common::catalog::{FragmentTypeFlag, FragmentTypeMask};
    use risingwave_common::hash::{ActorMapping, VirtualNode, VnodeCount};
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_common::util::stream_graph_visitor::visit_stream_node_body;
    use risingwave_meta_model::actor::{ActorModel, ActorStatus};
    use risingwave_meta_model::fragment::DistributionType;
    use risingwave_meta_model::{
        ActorId, ConnectorSplits, ExprContext, FragmentId, I32Array, ObjectId, StreamNode, TableId,
        VnodeBitmap, fragment,
    };
    use risingwave_pb::common::PbActorLocation;
    use risingwave_pb::meta::table_fragments::PbActorStatus;
    use risingwave_pb::meta::table_fragments::actor_status::PbActorState;
    use risingwave_pb::meta::table_fragments::fragment::PbFragmentDistributionType;
    use risingwave_pb::plan_common::PbExprContext;
    use risingwave_pb::source::{PbConnectorSplit, PbConnectorSplits};
    use risingwave_pb::stream_plan::stream_node::PbNodeBody;
    use risingwave_pb::stream_plan::{MergeNode, PbStreamNode, PbUnionNode};

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
            fragment_type_mask: FragmentTypeMask::from(FragmentTypeFlag::Source as u32),
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

                ActorModel {
                    actor_id: actor_id as ActorId,
                    fragment_id: TEST_FRAGMENT_ID,
                    status: ActorStatus::Running,
                    splits: actor_splits,
                    worker_id: 0,
                    vnode_bitmap: actor_bitmaps
                        .remove(&actor_id)
                        .map(|bitmap| bitmap.to_protobuf())
                        .as_ref()
                        .map(VnodeBitmap::from),
                    expr_context_2: ExprContext::from(&PbExprContext {
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
        actors: Vec<ActorModel>,
        actor_upstreams: &FragmentActorUpstreams,
        pb_actors: Vec<StreamActor>,
        pb_actor_splits: HashMap<u32, PbConnectorSplits>,
        stream_node: &PbStreamNode,
    ) {
        for (
            ActorModel {
                actor_id,
                fragment_id,
                status,
                splits,
                worker_id: _,
                vnode_bitmap,
                expr_context_2,
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

            visit_stream_node_body(stream_node, |body| {
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

            assert_eq!(Some(expr_context_2.to_protobuf()), pb_expr_context);
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
        assert_eq!(fragment_type_mask, fragment.fragment_type_mask.into());
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
