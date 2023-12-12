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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::mem::swap;

use anyhow::Context;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::util::stream_graph_visitor::visit_stream_node;
use risingwave_meta_model_v2::actor::ActorStatus;
use risingwave_meta_model_v2::prelude::{Actor, ActorDispatcher, Fragment, StreamingJob};
use risingwave_meta_model_v2::{
    actor, actor_dispatcher, fragment, ActorId, ConnectorSplits, ExprContext, FragmentId,
    FragmentVnodeMapping, I32Array, ObjectId, StreamNode, TableId, VnodeBitmap, WorkerId,
};
use risingwave_pb::common::PbParallelUnit;
use risingwave_pb::ddl_service::PbTableJobType;
use risingwave_pb::meta::table_fragments::actor_status::PbActorState;
use risingwave_pb::meta::table_fragments::fragment::PbFragmentDistributionType;
use risingwave_pb::meta::table_fragments::{PbActorStatus, PbFragment, PbState};
use risingwave_pb::meta::{FragmentParallelUnitMapping, PbTableFragments};
use risingwave_pb::source::PbConnectorSplits;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    PbDispatchStrategy, PbFragmentTypeFlag, PbStreamActor, PbStreamContext,
};
use sea_orm::sea_query::{Expr, Value};
use sea_orm::ActiveValue::Set;
use sea_orm::{
    ColumnTrait, EntityTrait, JoinType, ModelTrait, PaginatorTrait, QueryFilter, QuerySelect,
    RelationTrait, TransactionTrait,
};

use crate::controller::catalog::{CatalogController, CatalogControllerInner};
use crate::controller::utils::{get_actor_dispatchers, get_parallel_unit_mapping};
use crate::manager::ActorInfos;
use crate::stream::SplitAssignment;
use crate::MetaResult;

impl CatalogControllerInner {
    /// List all fragment vnode mapping info
    pub async fn all_running_fragment_mappings(
        &self,
    ) -> MetaResult<impl Iterator<Item = FragmentParallelUnitMapping> + '_> {
        let fragment_mappings: Vec<(FragmentId, FragmentVnodeMapping)> = Fragment::find()
            .select_only()
            .columns([fragment::Column::FragmentId, fragment::Column::VnodeMapping])
            .into_tuple()
            .all(&self.db)
            .await?;
        Ok(fragment_mappings.into_iter().map(|(fragment_id, mapping)| {
            FragmentParallelUnitMapping {
                fragment_id: fragment_id as _,
                mapping: Some(mapping.into_inner()),
            }
        }))
    }
}

impl CatalogController {
    #[allow(clippy::type_complexity)]
    pub fn extract_fragment_and_actors_from_table_fragments(
        PbTableFragments {
            table_id,
            fragments,
            actor_status,
            actor_splits,
            ..
        }: PbTableFragments,
    ) -> MetaResult<
        Vec<(
            fragment::Model,
            Vec<actor::Model>,
            HashMap<ActorId, Vec<actor_dispatcher::Model>>,
        )>,
    > {
        let mut result = vec![];

        let fragments: BTreeMap<_, _> = fragments.into_iter().collect();

        for (_, fragment) in fragments {
            let (fragment, actors, dispatchers) = Self::extract_fragment_and_actors(
                table_id as _,
                fragment,
                &actor_status,
                &actor_splits,
            )?;

            result.push((fragment, actors, dispatchers));
        }

        Ok(result)
    }

    #[allow(clippy::type_complexity)]
    pub fn extract_fragment_and_actors(
        job_id: ObjectId,
        pb_fragment: PbFragment,
        pb_actor_status: &HashMap<u32, PbActorStatus>,
        pb_actor_splits: &HashMap<u32, PbConnectorSplits>,
    ) -> MetaResult<(
        fragment::Model,
        Vec<actor::Model>,
        HashMap<ActorId, Vec<actor_dispatcher::Model>>,
    )> {
        let PbFragment {
            fragment_id: pb_fragment_id,
            fragment_type_mask: pb_fragment_type_mask,
            distribution_type: pb_distribution_type,
            actors: pb_actors,
            vnode_mapping: pb_vnode_mapping,
            state_table_ids: pb_state_table_ids,
            upstream_fragment_ids: pb_upstream_fragment_ids,
        } = pb_fragment;

        let state_table_ids = pb_state_table_ids.into();

        assert!(!pb_actors.is_empty());

        let stream_node = {
            let actor_template = pb_actors.first().cloned().unwrap();
            let mut stream_node = actor_template.nodes.unwrap();
            visit_stream_node(&mut stream_node, |body| {
                if let NodeBody::Merge(m) = body {
                    m.upstream_actor_id = vec![];
                }
            });

            stream_node
        };

        let mut actors = vec![];
        let mut actor_dispatchers = HashMap::new();

        for mut actor in pb_actors {
            let mut upstream_actors = BTreeMap::new();

            let node = actor.nodes.as_mut().context("nodes is empty")?;

            visit_stream_node(node, |body| {
                if let NodeBody::Merge(m) = body {
                    let mut upstream_actor_ids = vec![];
                    swap(&mut m.upstream_actor_id, &mut upstream_actor_ids);
                    assert!(
                        upstream_actors
                            .insert(m.upstream_fragment_id, upstream_actor_ids)
                            .is_none(),
                        "There should only be one link between two fragments"
                    );
                }
            });

            let PbStreamActor {
                actor_id,
                fragment_id,
                nodes: _,
                dispatcher: pb_dispatcher,
                upstream_actor_id: pb_upstream_actor_id,
                vnode_bitmap: pb_vnode_bitmap,
                mview_definition: _,
                expr_context: pb_expr_context,
            } = actor;

            let splits = pb_actor_splits.get(&actor_id).cloned().map(ConnectorSplits);
            let status = pb_actor_status.get(&actor_id).cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "actor {} in fragment {} has no actor_status",
                    actor_id,
                    fragment_id
                )
            })?;

            let parallel_unit_id = status
                .parallel_unit
                .as_ref()
                .map(|parallel_unit| parallel_unit.id)
                .expect("no parallel unit id found in actor_status")
                as _;

            assert_eq!(
                pb_upstream_actor_id
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>(),
                upstream_actors
                    .values()
                    .flatten()
                    .cloned()
                    .collect::<BTreeSet<_>>()
            );
            let pb_expr_context = pb_expr_context.expect("no expression context found");

            actors.push(actor::Model {
                actor_id: actor_id as _,
                fragment_id: fragment_id as _,
                status: status.get_state().unwrap().into(),
                splits,
                parallel_unit_id,
                upstream_actor_ids: upstream_actors.into(),
                vnode_bitmap: pb_vnode_bitmap.map(VnodeBitmap),
                expr_context: ExprContext(pb_expr_context),
            });
            actor_dispatchers.insert(
                actor_id as ActorId,
                pb_dispatcher
                    .into_iter()
                    .map(|dispatcher| (actor_id, dispatcher).into())
                    .collect(),
            );
        }

        let upstream_fragment_id = pb_upstream_fragment_ids.into();

        let vnode_mapping = pb_vnode_mapping.map(FragmentVnodeMapping).unwrap();

        let stream_node = StreamNode(stream_node);

        let distribution_type = PbFragmentDistributionType::try_from(pb_distribution_type)
            .unwrap()
            .into();

        let fragment = fragment::Model {
            fragment_id: pb_fragment_id as _,
            job_id,
            fragment_type_mask: pb_fragment_type_mask as _,
            distribution_type,
            stream_node,
            vnode_mapping,
            state_table_ids,
            upstream_fragment_id,
        };

        Ok((fragment, actors, actor_dispatchers))
    }

    #[allow(clippy::type_complexity)]
    pub fn compose_table_fragments(
        table_id: u32,
        state: PbState,
        ctx: Option<PbStreamContext>,
        fragments: Vec<(
            fragment::Model,
            Vec<actor::Model>,
            HashMap<ActorId, Vec<actor_dispatcher::Model>>,
        )>,
        parallel_units_map: HashMap<u32, PbParallelUnit>,
    ) -> MetaResult<PbTableFragments> {
        let mut pb_fragments = HashMap::new();
        let mut pb_actor_splits = HashMap::new();
        let mut pb_actor_status = HashMap::new();

        for (fragment, actors, actor_dispatcher) in fragments {
            let (fragment, fragment_actor_status, fragment_actor_splits) =
                Self::compose_fragment(fragment, actors, actor_dispatcher, &parallel_units_map)?;

            pb_fragments.insert(fragment.fragment_id, fragment);

            pb_actor_splits.extend(fragment_actor_splits.into_iter());
            pb_actor_status.extend(fragment_actor_status.into_iter());
        }

        let table_fragments = PbTableFragments {
            table_id,
            state: state as _,
            fragments: pb_fragments,
            actor_status: pb_actor_status,
            actor_splits: pb_actor_splits,
            ctx,
        };

        Ok(table_fragments)
    }

    #[allow(clippy::type_complexity)]
    pub(crate) fn compose_fragment(
        fragment: fragment::Model,
        actors: Vec<actor::Model>,
        mut actor_dispatcher: HashMap<ActorId, Vec<actor_dispatcher::Model>>,
        parallel_units_map: &HashMap<u32, PbParallelUnit>,
    ) -> MetaResult<(
        PbFragment,
        HashMap<u32, PbActorStatus>,
        HashMap<u32, PbConnectorSplits>,
    )> {
        let fragment::Model {
            fragment_id,
            job_id: _,
            fragment_type_mask,
            distribution_type,
            stream_node,
            vnode_mapping,
            state_table_ids,
            upstream_fragment_id,
        } = fragment;

        let stream_node_template = stream_node.into_inner();

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
                parallel_unit_id,
                splits,
                upstream_actor_ids,
                vnode_bitmap,
                expr_context,
            } = actor;

            let upstream_fragment_actors = upstream_actor_ids.into_inner();

            let pb_nodes = {
                let mut nodes = stream_node_template.clone();

                visit_stream_node(&mut nodes, |body| {
                    if let NodeBody::Merge(m) = body
                        && let Some(upstream_actor_ids) =
                            upstream_fragment_actors.get(&(m.upstream_fragment_id as _))
                    {
                        m.upstream_actor_id =
                            upstream_actor_ids.iter().map(|id| *id as _).collect();
                    }
                });

                Some(nodes)
            };

            let pb_vnode_bitmap = vnode_bitmap.map(|vnode_bitmap| vnode_bitmap.into_inner());
            let pb_expr_context = Some(expr_context.into_inner());

            let pb_upstream_actor_id = upstream_fragment_actors
                .values()
                .flatten()
                .map(|&id| id as _)
                .collect();

            let pb_dispatcher = actor_dispatcher
                .remove(&actor_id)
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect();

            pb_actor_status.insert(
                actor_id as _,
                PbActorStatus {
                    parallel_unit: Some(
                        parallel_units_map
                            .get(&(parallel_unit_id as _))
                            .unwrap()
                            .clone(),
                    ),
                    state: PbActorState::from(status) as _,
                },
            );

            if let Some(splits) = splits {
                pb_actor_splits.insert(actor_id as _, splits.into_inner());
            }

            pb_actors.push(PbStreamActor {
                actor_id: actor_id as _,
                fragment_id: fragment_id as _,
                nodes: pb_nodes,
                dispatcher: pb_dispatcher,
                upstream_actor_id: pb_upstream_actor_id,
                vnode_bitmap: pb_vnode_bitmap,
                mview_definition: "".to_string(),
                expr_context: pb_expr_context,
            })
        }

        let pb_upstream_fragment_ids = upstream_fragment_id.into_u32_array();
        let pb_vnode_mapping = vnode_mapping.into_inner();
        let pb_state_table_ids = state_table_ids.into_u32_array();
        let pb_distribution_type = PbFragmentDistributionType::from(distribution_type) as _;
        let pb_fragment = PbFragment {
            fragment_id: fragment_id as _,
            fragment_type_mask: fragment_type_mask as _,
            distribution_type: pb_distribution_type,
            actors: pb_actors,
            vnode_mapping: Some(pb_vnode_mapping),
            state_table_ids: pb_state_table_ids,
            upstream_fragment_ids: pb_upstream_fragment_ids,
        };

        Ok((pb_fragment, pb_actor_status, pb_actor_splits))
    }

    pub async fn running_fragment_parallelisms(
        &self,
        id_filter: Option<HashSet<FragmentId>>,
    ) -> MetaResult<HashMap<FragmentId, usize>> {
        let inner = self.inner.read().await;
        let mut select = Actor::find()
            .select_only()
            .column(actor::Column::FragmentId)
            .column_as(actor::Column::ActorId.count(), "count")
            .group_by(actor::Column::FragmentId);
        if let Some(id_filter) = id_filter {
            select = select.having(actor::Column::FragmentId.is_in(id_filter));
        }
        let fragment_parallelisms: Vec<(FragmentId, i64)> =
            select.into_tuple().all(&inner.db).await?;
        Ok(fragment_parallelisms
            .into_iter()
            .map(|(fragment_id, count)| (fragment_id, count as usize))
            .collect())
    }

    pub async fn fragment_job_mapping(&self) -> MetaResult<HashMap<FragmentId, ObjectId>> {
        let inner = self.inner.read().await;
        let fragment_jobs: Vec<(FragmentId, ObjectId)> = Fragment::find()
            .select_only()
            .columns([fragment::Column::FragmentId, fragment::Column::JobId])
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(fragment_jobs.into_iter().collect())
    }

    /// Gets the counts for each upstream relation that each stream job
    /// indicated by `table_ids` depends on.
    /// For example in the following query:
    /// ```sql
    /// CREATE MATERIALIZED VIEW m1 AS
    ///   SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t2.b = t3.b
    /// ```
    ///
    /// We have t1 occurring once, and t2 occurring once.
    pub async fn get_upstream_job_counts(
        &self,
        job_ids: Vec<ObjectId>,
    ) -> MetaResult<HashMap<ObjectId, HashMap<ObjectId, usize>>> {
        let inner = self.inner.read().await;
        let upstream_fragments: Vec<(ObjectId, i32, I32Array)> = Fragment::find()
            .select_only()
            .columns([
                fragment::Column::JobId,
                fragment::Column::FragmentTypeMask,
                fragment::Column::UpstreamFragmentId,
            ])
            .filter(fragment::Column::JobId.is_in(job_ids))
            .into_tuple()
            .all(&inner.db)
            .await?;

        // filter out stream scan node.
        let upstream_fragments = upstream_fragments
            .into_iter()
            .filter(|(_, mask, _)| (*mask & PbFragmentTypeFlag::StreamScan as i32) != 0)
            .map(|(obj, _, upstream_fragments)| (obj, upstream_fragments.into_inner()))
            .collect_vec();

        // count by fragment id.
        let upstream_fragment_counts = upstream_fragments
            .iter()
            .flat_map(|(_, upstream_fragments)| upstream_fragments.iter().cloned())
            .counts();

        // get fragment id to job id mapping.
        let fragment_job_ids: Vec<(FragmentId, ObjectId)> = Fragment::find()
            .select_only()
            .columns([fragment::Column::FragmentId, fragment::Column::JobId])
            .filter(
                fragment::Column::FragmentId
                    .is_in(upstream_fragment_counts.keys().cloned().collect_vec()),
            )
            .into_tuple()
            .all(&inner.db)
            .await?;
        let fragment_job_mapping: HashMap<FragmentId, ObjectId> =
            fragment_job_ids.into_iter().collect();

        // get upstream job counts.
        let upstream_job_counts = upstream_fragments
            .into_iter()
            .map(|(job_id, upstream_fragments)| {
                let upstream_job_counts = upstream_fragments
                    .into_iter()
                    .map(|upstream_fragment_id| {
                        let upstream_job_id =
                            fragment_job_mapping.get(&upstream_fragment_id).unwrap();
                        (
                            *upstream_job_id,
                            *upstream_fragment_counts.get(&upstream_fragment_id).unwrap(),
                        )
                    })
                    .collect();
                (job_id, upstream_job_counts)
            })
            .collect();
        Ok(upstream_job_counts)
    }

    pub async fn get_job_fragments_by_id(&self, job_id: ObjectId) -> MetaResult<PbTableFragments> {
        let inner = self.inner.read().await;
        let fragment_actors = Fragment::find()
            .find_with_related(Actor)
            .filter(fragment::Column::JobId.eq(job_id))
            .all(&inner.db)
            .await?;
        let mut actor_dispatchers = get_actor_dispatchers(
            &inner.db,
            fragment_actors
                .iter()
                .flat_map(|(_, actors)| actors.iter().map(|actor| actor.actor_id))
                .collect(),
        )
        .await?;
        let job_info = StreamingJob::find_by_id(job_id)
            .one(&inner.db)
            .await?
            .ok_or_else(|| anyhow::anyhow!("job {} not found in database", job_id))?;

        let mut fragment_info = vec![];
        for (fragment, actors) in fragment_actors {
            let mut dispatcher_info = HashMap::new();
            for actor in &actors {
                if let Some(dispatchers) = actor_dispatchers.remove(&actor.actor_id) {
                    dispatcher_info.insert(actor.actor_id, dispatchers);
                }
            }
            fragment_info.push((fragment, actors, dispatcher_info));
        }

        let parallel_units_map = get_parallel_unit_mapping(&inner.db).await?;
        Self::compose_table_fragments(
            job_id as _,
            job_info.job_status.into(),
            job_info.timezone.map(|tz| PbStreamContext { timezone: tz }),
            fragment_info,
            parallel_units_map,
        )
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
            if let Ok(job_state_tables) = Fragment::find()
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
        let count = Fragment::find().count(&inner.db).await?;
        Ok(count > 0)
    }

    pub fn table_fragments(&self) -> MetaResult<BTreeMap<ObjectId, PbTableFragments>> {
        unimplemented!(
            "This function is too heavy, we should avoid using it and implement others on demand."
        )
    }

    /// Check if the fragment type mask is injectable.
    fn is_injectable(fragment_type_mask: u32) -> bool {
        (fragment_type_mask
            & (PbFragmentTypeFlag::Source as u32
                | PbFragmentTypeFlag::Now as u32
                | PbFragmentTypeFlag::Values as u32
                | PbFragmentTypeFlag::BarrierRecv as u32))
            != 0
    }

    /// Used in [`crate::barrier::GlobalBarrierManager`], load all actor that need to be sent or
    /// collected
    pub async fn load_all_actor(
        &self,
        parallel_units_map: &HashMap<u32, PbParallelUnit>,
        check_state: impl Fn(PbActorState, ObjectId, ActorId) -> bool,
    ) -> MetaResult<ActorInfos> {
        let inner = self.inner.read().await;
        let actor_info: Vec<(ActorId, ActorStatus, i32, ObjectId, i32)> = Actor::find()
            .select_only()
            .column(actor::Column::ActorId)
            .column(actor::Column::Status)
            .column(actor::Column::ParallelUnitId)
            .column(fragment::Column::JobId)
            .column(fragment::Column::FragmentTypeMask)
            .join(JoinType::InnerJoin, actor::Relation::Fragment.def())
            .into_tuple()
            .all(&inner.db)
            .await?;

        let mut actor_maps = HashMap::new();
        let mut barrier_inject_actor_maps = HashMap::new();

        for (actor_id, status, parallel_unit_id, job_id, type_mask) in actor_info {
            let status = PbActorState::from(status);
            let worker_id = parallel_units_map
                .get(&(parallel_unit_id as _))
                .unwrap()
                .worker_node_id;
            if check_state(status, job_id, actor_id) {
                actor_maps
                    .entry(worker_id)
                    .or_insert_with(Vec::new)
                    .push(actor_id as _);
                if Self::is_injectable(type_mask as _) {
                    barrier_inject_actor_maps
                        .entry(worker_id)
                        .or_insert_with(Vec::new)
                        .push(actor_id as _);
                }
            }
        }

        Ok(ActorInfos {
            actor_maps,
            barrier_inject_actor_maps,
        })
    }

    pub async fn migrate_actors(&self, plan: HashMap<u32, PbParallelUnit>) -> MetaResult<()> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;
        for (from_pu_id, to_pu_id) in plan {
            Actor::update_many()
                .col_expr(
                    actor::Column::ParallelUnitId,
                    Expr::value(Value::Int(Some(to_pu_id.id as _))),
                )
                .filter(actor::Column::ParallelUnitId.eq(from_pu_id as i32))
                .exec(&txn)
                .await?;
        }
        txn.commit().await?;

        Ok(())
    }

    pub async fn all_inuse_parallel_units(&self) -> MetaResult<Vec<i32>> {
        let inner = self.inner.read().await;
        let parallel_units: Vec<i32> = Actor::find()
            .select_only()
            .column(actor::Column::ParallelUnitId)
            .distinct()
            .into_tuple()
            .all(&inner.db)
            .await?;
        Ok(parallel_units)
    }

    pub async fn all_node_actors(
        &self,
        include_inactive: bool,
    ) -> MetaResult<HashMap<WorkerId, Vec<PbStreamActor>>> {
        let inner = self.inner.read().await;
        let parallel_units_map = get_parallel_unit_mapping(&inner.db).await?;
        let fragment_actors = if include_inactive {
            Fragment::find()
                .find_with_related(Actor)
                .all(&inner.db)
                .await?
        } else {
            Fragment::find()
                .find_with_related(Actor)
                .filter(actor::Column::Status.eq(ActorStatus::Running))
                .all(&inner.db)
                .await?
        };

        let mut actor_dispatchers = get_actor_dispatchers(
            &inner.db,
            fragment_actors
                .iter()
                .flat_map(|(_, actors)| actors.iter().map(|actor| actor.actor_id))
                .collect(),
        )
        .await?;

        let mut node_actors = HashMap::new();
        for (fragment, actors) in fragment_actors {
            let mut dispatcher_info = HashMap::new();
            for actor in &actors {
                if let Some(dispatchers) = actor_dispatchers.remove(&actor.actor_id) {
                    dispatcher_info.insert(actor.actor_id, dispatchers);
                }
            }

            let (table_fragments, actor_status, _) =
                Self::compose_fragment(fragment, actors, dispatcher_info, &parallel_units_map)?;
            for actor in table_fragments.actors {
                let node_id = actor_status[&actor.actor_id]
                    .get_parallel_unit()
                    .unwrap()
                    .worker_node_id as WorkerId;
                node_actors
                    .entry(node_id)
                    .or_insert_with(Vec::new)
                    .push(actor);
            }
        }

        Ok(node_actors)
    }

    pub async fn update_actor_splits(&self, split_assignment: &SplitAssignment) -> MetaResult<()> {
        let inner = self.inner.read().await;
        let txn = inner.db.begin().await?;
        for assignments in split_assignment.values() {
            for (actor_id, splits) in assignments {
                Actor::update(actor::ActiveModel {
                    actor_id: Set(*actor_id as _),
                    splits: Set(Some(ConnectorSplits(PbConnectorSplits {
                        splits: splits.iter().map(Into::into).collect(),
                    }))),
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
    pub async fn get_running_actors_by_fragment(
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

    /// Get and filter the upstream `Materialize` or `Source` fragments of the specified relations.
    pub async fn get_upstream_root_fragments(
        &self,
        upstream_job_ids: Vec<ObjectId>,
        job_type: Option<PbTableJobType>,
    ) -> MetaResult<HashMap<ObjectId, PbFragment>> {
        let inner = self.inner.read().await;

        let mut fragments = Fragment::find()
            .filter(fragment::Column::JobId.is_in(upstream_job_ids))
            .all(&inner.db)
            .await?;
        fragments.retain(|f| match job_type {
            Some(PbTableJobType::SharedCdcSource) => {
                f.fragment_type_mask & PbFragmentTypeFlag::Source as i32 != 0
            }
            // MV on MV, and other kinds of table job
            None | Some(PbTableJobType::General) | Some(PbTableJobType::Unspecified) => {
                f.fragment_type_mask & PbFragmentTypeFlag::Mview as i32 != 0
            }
        });

        let parallel_units_map = get_parallel_unit_mapping(&inner.db).await?;
        let mut root_fragments = HashMap::new();
        for fragment in fragments {
            let actors = fragment.find_related(Actor).all(&inner.db).await?;
            let actor_dispatchers = get_actor_dispatchers(
                &inner.db,
                actors.iter().map(|actor| actor.actor_id).collect(),
            )
            .await?;

            root_fragments.insert(
                fragment.job_id,
                Self::compose_fragment(fragment, actors, actor_dispatchers, &parallel_units_map)?.0,
            );
        }

        Ok(root_fragments)
    }

    /// Get the downstream `Chain` fragments of the specified table.
    pub async fn get_downstream_chain_fragments(
        &self,
        job_id: ObjectId,
    ) -> MetaResult<Vec<(PbDispatchStrategy, PbFragment)>> {
        let mview_fragment = self.get_mview_fragment(job_id).await?;
        let downstream_dispatches: HashMap<_, _> = mview_fragment.actors[0]
            .dispatcher
            .iter()
            .map(|d| {
                let fragment_id = d.dispatcher_id as FragmentId;
                let strategy = PbDispatchStrategy {
                    r#type: d.r#type,
                    dist_key_indices: d.dist_key_indices.clone(),
                    output_indices: d.output_indices.clone(),
                };
                (fragment_id, strategy)
            })
            .collect();

        let inner = self.inner.read().await;
        let parallel_units_map = get_parallel_unit_mapping(&inner.db).await?;
        let mut chain_fragments = vec![];
        for (fragment_id, dispatch_strategy) in downstream_dispatches {
            let mut fragment_actors = Fragment::find_by_id(fragment_id)
                .find_with_related(Actor)
                .all(&inner.db)
                .await?;
            if fragment_actors.is_empty() {
                bail!("No fragment found for fragment id {}", fragment_id);
            }
            assert_eq!(fragment_actors.len(), 1);
            let (fragment, actors) = fragment_actors.pop().unwrap();
            let actor_dispatchers = get_actor_dispatchers(
                &inner.db,
                actors.iter().map(|actor| actor.actor_id).collect(),
            )
            .await?;
            let fragment =
                Self::compose_fragment(fragment, actors, actor_dispatchers, &parallel_units_map)?.0;
            chain_fragments.push((dispatch_strategy, fragment));
        }

        Ok(chain_fragments)
    }

    /// Get the `Materialize` fragment of the specified table.
    pub async fn get_mview_fragment(&self, job_id: ObjectId) -> MetaResult<PbFragment> {
        let inner = self.inner.read().await;
        let mut fragments = Fragment::find()
            .filter(fragment::Column::JobId.eq(job_id))
            .all(&inner.db)
            .await?;
        fragments.retain(|f| f.fragment_type_mask & PbFragmentTypeFlag::Mview as i32 != 0);
        if fragments.is_empty() {
            bail!("No mview fragment found for job {}", job_id);
        }
        assert_eq!(fragments.len(), 1);

        let parallel_units_map = get_parallel_unit_mapping(&inner.db).await?;
        let fragment = fragments.pop().unwrap();
        let actor_with_dispatchers = fragment
            .find_related(Actor)
            .find_with_related(ActorDispatcher)
            .all(&inner.db)
            .await?;
        let mut actors = vec![];
        let mut actor_dispatchers = HashMap::new();
        for (actor, dispatchers) in actor_with_dispatchers {
            actor_dispatchers.insert(actor.actor_id, dispatchers);
            actors.push(actor);
        }

        Ok(Self::compose_fragment(fragment, actors, actor_dispatchers, &parallel_units_map)?.0)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::default::Default;

    use itertools::Itertools;
    use risingwave_common::hash::{ParallelUnitId, ParallelUnitMapping};
    use risingwave_common::util::iter_util::ZipEqDebug;
    use risingwave_common::util::stream_graph_visitor::visit_stream_node;
    use risingwave_meta_model_v2::actor::ActorStatus;
    use risingwave_meta_model_v2::fragment::DistributionType;
    use risingwave_meta_model_v2::{
        actor, actor_dispatcher, fragment, ActorId, ActorUpstreamActors, ConnectorSplits,
        ExprContext, FragmentId, FragmentVnodeMapping, I32Array, ObjectId, StreamNode, TableId,
        VnodeBitmap,
    };
    use risingwave_pb::common::ParallelUnit;
    use risingwave_pb::meta::table_fragments::actor_status::PbActorState;
    use risingwave_pb::meta::table_fragments::fragment::PbFragmentDistributionType;
    use risingwave_pb::meta::table_fragments::{PbActorStatus, PbFragment};
    use risingwave_pb::plan_common::PbExprContext;
    use risingwave_pb::source::{PbConnectorSplit, PbConnectorSplits};
    use risingwave_pb::stream_plan::stream_node::{NodeBody, PbNodeBody};
    use risingwave_pb::stream_plan::{
        Dispatcher, MergeNode, PbDispatcher, PbDispatcherType, PbFragmentTypeFlag, PbStreamActor,
        PbStreamNode, PbUnionNode,
    };

    use crate::controller::catalog::CatalogController;
    use crate::MetaResult;

    const TEST_FRAGMENT_ID: FragmentId = 1;

    const TEST_UPSTREAM_FRAGMENT_ID: FragmentId = 2;

    const TEST_JOB_ID: ObjectId = 1;

    const TEST_STATE_TABLE_ID: TableId = 1000;

    fn generate_parallel_units(count: u32) -> Vec<ParallelUnit> {
        (0..count)
            .map(|parallel_unit_id| ParallelUnit {
                id: parallel_unit_id,
                ..Default::default()
            })
            .collect_vec()
    }

    fn generate_dispatchers_for_actor(actor_id: u32) -> Vec<Dispatcher> {
        vec![PbDispatcher {
            r#type: PbDispatcherType::Hash as _,
            dispatcher_id: actor_id as u64,
            downstream_actor_id: vec![actor_id],
            ..Default::default()
        }]
    }

    fn generate_upstream_actor_ids_for_actor(actor_id: u32) -> BTreeMap<FragmentId, Vec<ActorId>> {
        let mut upstream_actor_ids = BTreeMap::new();
        upstream_actor_ids.insert(TEST_UPSTREAM_FRAGMENT_ID, vec![(actor_id + 100) as ActorId]);
        upstream_actor_ids.insert(
            TEST_UPSTREAM_FRAGMENT_ID + 1,
            vec![(actor_id + 200) as ActorId],
        );
        upstream_actor_ids
    }

    fn generate_merger_stream_node(
        actor_upstream_actor_ids: &BTreeMap<FragmentId, Vec<ActorId>>,
    ) -> PbStreamNode {
        let mut input = vec![];
        for (upstream_fragment_id, upstream_actor_ids) in actor_upstream_actor_ids {
            input.push(PbStreamNode {
                node_body: Some(PbNodeBody::Merge(MergeNode {
                    upstream_actor_id: upstream_actor_ids.iter().map(|id| *id as _).collect(),
                    upstream_fragment_id: *upstream_fragment_id as _,
                    ..Default::default()
                })),
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
        let parallel_units = generate_parallel_units(actor_count);
        let parallel_unit_mapping = ParallelUnitMapping::build(&parallel_units);
        let actor_vnode_bitmaps = parallel_unit_mapping.to_bitmaps();

        let upstream_actor_ids: HashMap<ActorId, BTreeMap<FragmentId, Vec<ActorId>>> = (0
            ..actor_count)
            .map(|actor_id| {
                (
                    actor_id as _,
                    generate_upstream_actor_ids_for_actor(actor_id),
                )
            })
            .collect();

        let pb_actors = (0..actor_count)
            .map(|actor_id| {
                let actor_upstream_actor_ids =
                    upstream_actor_ids.get(&(actor_id as _)).cloned().unwrap();
                let stream_node = generate_merger_stream_node(&actor_upstream_actor_ids);

                PbStreamActor {
                    actor_id: actor_id as _,
                    fragment_id: TEST_FRAGMENT_ID as _,
                    nodes: Some(stream_node),
                    dispatcher: generate_dispatchers_for_actor(actor_id),
                    upstream_actor_id: actor_upstream_actor_ids
                        .values()
                        .flatten()
                        .map(|id| *id as _)
                        .collect(),
                    vnode_bitmap: actor_vnode_bitmaps
                        .get(&actor_id)
                        .cloned()
                        .map(|bitmap| bitmap.to_protobuf()),
                    mview_definition: "".to_string(),
                    expr_context: Some(PbExprContext {
                        time_zone: String::from("America/New_York"),
                    }),
                }
            })
            .collect_vec();

        let pb_fragment = PbFragment {
            fragment_id: TEST_FRAGMENT_ID as _,
            fragment_type_mask: PbFragmentTypeFlag::Source as _,
            distribution_type: PbFragmentDistributionType::Hash as _,
            actors: pb_actors.clone(),
            vnode_mapping: Some(parallel_unit_mapping.to_protobuf()),
            state_table_ids: vec![TEST_STATE_TABLE_ID as _],
            upstream_fragment_ids: upstream_actor_ids
                .values()
                .flat_map(|m| m.keys().map(|x| *x as _))
                .collect(),
        };

        let pb_actor_status = (0..actor_count)
            .map(|actor_id| {
                (
                    actor_id,
                    PbActorStatus {
                        parallel_unit: Some(parallel_units[actor_id as usize].clone()),
                        state: PbActorState::Running as _,
                    },
                )
            })
            .collect();

        let pb_actor_splits = Default::default();

        let (fragment, actors, actor_dispatchers) = CatalogController::extract_fragment_and_actors(
            TEST_JOB_ID,
            pb_fragment.clone(),
            &pb_actor_status,
            &pb_actor_splits,
        )?;

        check_fragment_template(fragment.clone(), pb_actors.clone(), &upstream_actor_ids);
        check_fragment(fragment, pb_fragment);
        check_actors(actors, actor_dispatchers, pb_actors, pb_actor_splits);

        Ok(())
    }

    fn check_fragment_template(
        fragment: fragment::Model,
        actors: Vec<PbStreamActor>,
        upstream_actor_ids: &HashMap<ActorId, BTreeMap<FragmentId, Vec<ActorId>>>,
    ) {
        let stream_node_template = fragment.stream_node.clone();

        for PbStreamActor {
            nodes, actor_id, ..
        } in actors
        {
            let mut template_node = stream_node_template.clone().into_inner();
            let nodes = nodes.unwrap();
            let actor_upstream_actor_ids =
                upstream_actor_ids.get(&(actor_id as _)).cloned().unwrap();
            visit_stream_node(&mut template_node, |body| {
                if let NodeBody::Merge(m) = body {
                    m.upstream_actor_id = actor_upstream_actor_ids
                        .get(&(m.upstream_fragment_id as _))
                        .map(|actors| actors.iter().map(|id| *id as _).collect())
                        .unwrap();
                }
            });

            assert_eq!(nodes, template_node);
        }
    }

    #[tokio::test]
    async fn test_compose_fragment() -> MetaResult<()> {
        let actor_count = 3u32;
        let parallel_units = generate_parallel_units(actor_count);
        let parallel_unit_mapping = ParallelUnitMapping::build(&parallel_units);
        let mut actor_vnode_bitmaps = parallel_unit_mapping.to_bitmaps();

        let upstream_actor_ids: HashMap<ActorId, BTreeMap<FragmentId, Vec<ActorId>>> = (0
            ..actor_count)
            .map(|actor_id| {
                (
                    actor_id as _,
                    generate_upstream_actor_ids_for_actor(actor_id),
                )
            })
            .collect();

        let parallel_units_map = parallel_units
            .iter()
            .map(|parallel_unit| (parallel_unit.id, parallel_unit.clone()))
            .collect();

        let actors = (0..actor_count)
            .map(|actor_id| {
                let parallel_unit_id = actor_id as ParallelUnitId;

                let vnode_bitmap = actor_vnode_bitmaps
                    .remove(&parallel_unit_id)
                    .map(|m| VnodeBitmap(m.to_protobuf()));

                let actor_splits = Some(ConnectorSplits(PbConnectorSplits {
                    splits: vec![PbConnectorSplit {
                        split_type: "dummy".to_string(),
                        ..Default::default()
                    }],
                }));

                let actor_upstream_actor_ids =
                    upstream_actor_ids.get(&(actor_id as _)).cloned().unwrap();

                actor::Model {
                    actor_id: actor_id as ActorId,
                    fragment_id: TEST_FRAGMENT_ID,
                    status: ActorStatus::Running,
                    splits: actor_splits,
                    parallel_unit_id: parallel_unit_id as i32,
                    upstream_actor_ids: ActorUpstreamActors(actor_upstream_actor_ids),
                    vnode_bitmap,
                    expr_context: ExprContext(PbExprContext {
                        time_zone: String::from("America/New_York"),
                    }),
                }
            })
            .collect_vec();

        let actor_dispatchers: HashMap<_, _> = (0..actor_count)
            .map(|actor_id| {
                let dispatchers = generate_dispatchers_for_actor(actor_id);
                (
                    actor_id as ActorId,
                    dispatchers
                        .into_iter()
                        .map(|dispatcher| (actor_id, dispatcher).into())
                        .collect_vec(),
                )
            })
            .collect();

        let stream_node = {
            let template_actor = actors.first().cloned().unwrap();

            let template_upstream_actor_ids = template_actor
                .upstream_actor_ids
                .into_inner()
                .into_keys()
                .map(|k| (k, vec![]))
                .collect();

            generate_merger_stream_node(&template_upstream_actor_ids)
        };

        let fragment = fragment::Model {
            fragment_id: TEST_FRAGMENT_ID,
            job_id: TEST_JOB_ID,
            fragment_type_mask: 0,
            distribution_type: DistributionType::Hash,
            stream_node: StreamNode(stream_node),
            vnode_mapping: FragmentVnodeMapping(parallel_unit_mapping.to_protobuf()),
            state_table_ids: I32Array(vec![TEST_STATE_TABLE_ID]),
            upstream_fragment_id: I32Array::default(),
        };

        let (pb_fragment, pb_actor_status, pb_actor_splits) = CatalogController::compose_fragment(
            fragment.clone(),
            actors.clone(),
            actor_dispatchers.clone(),
            &parallel_units_map,
        )
        .unwrap();

        assert_eq!(pb_actor_status.len(), actor_count as usize);
        assert_eq!(pb_actor_splits.len(), actor_count as usize);

        for (actor_id, actor_status) in &pb_actor_status {
            let parallel_unit_id = parallel_units[*actor_id as usize].id;
            assert_eq!(
                parallel_unit_id,
                actor_status.parallel_unit.clone().unwrap().id
            );
        }

        let pb_actors = pb_fragment.actors.clone();

        check_fragment_template(fragment.clone(), pb_actors.clone(), &upstream_actor_ids);
        check_fragment(fragment, pb_fragment);
        check_actors(actors, actor_dispatchers, pb_actors, pb_actor_splits);

        Ok(())
    }

    fn check_actors(
        actors: Vec<actor::Model>,
        mut actor_dispatchers: HashMap<ActorId, Vec<actor_dispatcher::Model>>,
        pb_actors: Vec<PbStreamActor>,
        pb_actor_splits: HashMap<u32, PbConnectorSplits>,
    ) {
        for (
            actor::Model {
                actor_id,
                fragment_id,
                status,
                splits,
                parallel_unit_id,
                upstream_actor_ids,
                vnode_bitmap,
                expr_context,
            },
            PbStreamActor {
                actor_id: pb_actor_id,
                fragment_id: pb_fragment_id,
                nodes: pb_nodes,
                dispatcher: pb_dispatcher,
                upstream_actor_id: pb_upstream_actor_id,
                vnode_bitmap: pb_vnode_bitmap,
                mview_definition,
                expr_context: pb_expr_context,
            },
        ) in actors.into_iter().zip_eq_debug(pb_actors.into_iter())
        {
            assert_eq!(actor_id, pb_actor_id as ActorId);
            assert_eq!(fragment_id, pb_fragment_id as FragmentId);
            assert_eq!(parallel_unit_id, pb_actor_id as i32);
            let upstream_actor_ids = upstream_actor_ids.into_inner();

            assert_eq!(
                upstream_actor_ids
                    .values()
                    .flatten()
                    .map(|id| *id as u32)
                    .collect_vec(),
                pb_upstream_actor_id
            );

            let actor_dispatcher: Vec<PbDispatcher> = actor_dispatchers
                .remove(&actor_id)
                .unwrap()
                .into_iter()
                .map(Into::into)
                .collect();

            assert_eq!(actor_dispatcher, pb_dispatcher);
            assert_eq!(
                vnode_bitmap,
                pb_vnode_bitmap
                    .as_ref()
                    .map(|bitmap| VnodeBitmap(bitmap.clone()))
            );

            assert_eq!(mview_definition, "");

            let mut pb_nodes = pb_nodes.unwrap();

            visit_stream_node(&mut pb_nodes, |body| {
                if let PbNodeBody::Merge(m) = body {
                    let upstream_actor_ids = upstream_actor_ids
                        .get(&(m.upstream_fragment_id as _))
                        .map(|actors| actors.iter().map(|id| *id as u32).collect_vec())
                        .unwrap();
                    assert_eq!(upstream_actor_ids, m.upstream_actor_id);
                }
            });

            assert_eq!(status, ActorStatus::Running);

            assert_eq!(
                splits,
                pb_actor_splits
                    .get(&pb_actor_id)
                    .cloned()
                    .map(ConnectorSplits)
            );

            assert_eq!(Some(expr_context.into_inner()), pb_expr_context);
        }
    }

    fn check_fragment(fragment: fragment::Model, pb_fragment: PbFragment) {
        let PbFragment {
            fragment_id,
            fragment_type_mask,
            distribution_type: pb_distribution_type,
            actors: _,
            vnode_mapping: pb_vnode_mapping,
            state_table_ids: pb_state_table_ids,
            upstream_fragment_ids: pb_upstream_fragment_ids,
        } = pb_fragment;

        assert_eq!(fragment_id, TEST_FRAGMENT_ID as u32);
        assert_eq!(fragment_type_mask, fragment.fragment_type_mask as u32);
        assert_eq!(
            pb_distribution_type,
            PbFragmentDistributionType::from(fragment.distribution_type) as i32
        );
        assert_eq!(
            pb_vnode_mapping.map(FragmentVnodeMapping).unwrap(),
            fragment.vnode_mapping
        );

        assert_eq!(
            pb_upstream_fragment_ids,
            fragment.upstream_fragment_id.into_u32_array()
        );

        assert_eq!(
            pb_state_table_ids,
            fragment.state_table_ids.into_u32_array()
        );
    }
}
