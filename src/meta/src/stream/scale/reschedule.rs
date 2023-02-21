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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::iter::repeat;

use anyhow::anyhow;
use futures::future::BoxFuture;
use itertools::Itertools;
use num_integer::Integer;
use risingwave_common::bail;
use risingwave_common::buffer::{Bitmap, BitmapBuilder};
use risingwave_common::hash::{ActorMapping, ParallelUnitId, VirtualNode};
use risingwave_common::util::iter_util::{ZipEqDebug, ZipEqFast};
use risingwave_pb::common::{worker_node, ActorInfo, ParallelUnit, WorkerNode, WorkerType};
use risingwave_pb::meta::table_fragments::actor_status::ActorState;
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::table_fragments::{self, ActorStatus, Fragment};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{DispatcherType, FragmentTypeFlag, StreamActor, StreamNode};
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, UpdateActorsRequest,
};
use uuid::Uuid;

use crate::barrier::{Command, Reschedule};
use crate::manager::{IdCategory, WorkerId};
use crate::model::{ActorId, DispatcherId, FragmentId, TableFragments};
use crate::storage::MetaStore;
use crate::stream::scale::utils::{
    appoint_actors_to_vacant_interval_sets, divide_vnode_into_occupied_and_vacant,
};
use crate::stream::GlobalStreamManager;
use crate::MetaResult;

#[derive(Debug)]
pub struct ParallelUnitReschedule {
    pub added_parallel_units: Vec<ParallelUnitId>,
    pub removed_parallel_units: Vec<ParallelUnitId>,
}

pub(crate) struct RescheduleContext {
    /// Index used to map `ParallelUnitId` to `WorkerId`
    parallel_unit_id_to_worker_id: BTreeMap<ParallelUnitId, WorkerId>,
    /// Meta information for all Actors
    actor_map: HashMap<ActorId, StreamActor>,
    /// Status of all Actors, used to find the location of the `Actor`
    actor_status: BTreeMap<ActorId, ActorStatus>,
    /// Meta information of all `Fragment`, used to find the `Fragment`'s `Actor`
    fragment_map: HashMap<FragmentId, Fragment>,
    /// Indexes for all `Worker`s
    worker_nodes: HashMap<WorkerId, WorkerNode>,
    /// Index of all `Actor` upstreams, specific to `Dispatcher`
    upstream_dispatchers: HashMap<ActorId, Vec<(FragmentId, DispatcherId, DispatcherType)>>,
    /// Fragments with stream source
    stream_source_fragment_ids: HashSet<FragmentId>,
    /// Target fragments in NoShuffle relation
    no_shuffle_target_fragment_ids: HashSet<FragmentId>,
    /// Source fragments in NoShuffle relation
    no_shuffle_source_fragment_ids: HashSet<FragmentId>,
    // index for dispatcher type from upstream fragment to downstream fragment
    fragment_dispatcher_map: HashMap<FragmentId, HashMap<FragmentId, DispatcherType>>,
}

impl RescheduleContext {
    fn actor_id_to_parallel_unit(&self, actor_id: &ActorId) -> MetaResult<&ParallelUnit> {
        self.actor_status
            .get(actor_id)
            .and_then(|actor_status| actor_status.parallel_unit.as_ref())
            .ok_or_else(|| anyhow!("could not found ParallelUnit for {}", actor_id).into())
    }

    fn parallel_unit_id_to_worker(
        &self,
        parallel_unit_id: &ParallelUnitId,
    ) -> MetaResult<&WorkerNode> {
        self.parallel_unit_id_to_worker_id
            .get(parallel_unit_id)
            .and_then(|worker_id| self.worker_nodes.get(worker_id))
            .ok_or_else(|| {
                anyhow!(
                    "could not found Worker for ParallelUint {}",
                    parallel_unit_id
                )
                .into()
            })
    }
}

/// This function provides a simple balancing method, which aims to enable parallel units to
/// leverage locality of vnodes which are assigned to them to reduce storage block cache size by not
/// storing irrelevant data in their block cache, while keeping affinity to avoid too much
/// operator cache refilling.
/// The specific process is as follows
///
/// 1. Calculate the number of target actors, and calculate the average value and the remainder, and
/// use the average value as expected.
///
/// 2. Filter out the actor to be removed and the actor to be retained.
///
/// 3. Divide virtual nodes into occupied and vacant.
///
/// 4. Assign vacant vnodes to actors.
///
/// This can handle scale in, scale out, migration, and simultaneous scaling with as much affinity
/// as possible.
///
/// Note that this function can only rebalance actors whose `vnode_bitmap` is not `None`, in other
/// words, for `Fragment` of `FragmentDistributionType::Single`, using this function will cause
/// assert to fail and should be skipped from the upper level.
///
/// The return value is the bitmap distribution after scaling, which covers all virtual node indexes
pub(crate) fn rebalance_actor_vnode(
    actors: &[StreamActor],
    actors_to_remove: &BTreeSet<ActorId>,
    actors_to_create: &BTreeSet<ActorId>,
) -> HashMap<ActorId, Bitmap> {
    assert!(actors.len() >= actors_to_remove.len());

    let target_actor_count = actors.len() - actors_to_remove.len() + actors_to_create.len();
    assert!(target_actor_count > 0);

    let (expected, remain) = VirtualNode::COUNT.div_rem(&target_actor_count);

    tracing::debug!(
        "expected {}, remain {}, prev actors {}, target actors {}",
        expected,
        remain,
        actors.len(),
        target_actor_count,
    );

    let (removed, rest): (Vec<_>, Vec<_>) = actors
        .iter()
        .filter_map(|actor| {
            actor
                .vnode_bitmap
                .as_ref()
                .map(|buffer| (actor.actor_id as ActorId, Bitmap::from(buffer)))
        })
        .partition(|(actor_id, _)| actors_to_remove.contains(actor_id));

    // Arbitrary vnodes can be assigned to flexible actors.
    let mut flexible_actors = actors_to_create.iter().cloned().collect_vec();

    if actors_to_remove.len() == actors_to_create.len() {
        let mut result = HashMap::with_capacity(target_actor_count);
        result.extend(rest.into_iter());
        result.extend(
            flexible_actors
                .into_iter()
                .zip_eq_fast(removed.into_iter().map(|(_, bitmap)| bitmap)),
        );
        return result;
    }

    // We will allocate `expected + 1` first and prune excess ones later (View comments
    // in function `prune_excess_remainder_segments` for details).
    let quota = expected + if remain > 0 { 1 } else { 0 };
    // `unallocated_remain` is the result of subtracting the number of actors which now have
    // `expected + 1` vnodes from `remain`.
    let (fixed_segments, unallocated_remain, uncovered_segments) =
        divide_vnode_into_occupied_and_vacant(&rest, quota, expected, remain, &mut flexible_actors);

    // Fill vnodes which are not occupied, i.e., assigning them to actors.
    let builders = appoint_actors_to_vacant_interval_sets(
        fixed_segments,
        uncovered_segments,
        quota,
        expected,
        remain,
        unallocated_remain,
        target_actor_count,
        &mut flexible_actors,
    );

    // Build bitmap from vnode set for each actor.
    let mut result = HashMap::with_capacity(target_actor_count);
    for (actor_id, intervals) in builders {
        let mut bitmap_builder = BitmapBuilder::zeroed(VirtualNode::COUNT);
        for interval in intervals {
            for vnode in interval {
                bitmap_builder.set(vnode, true);
            }
        }
        result.insert(actor_id, bitmap_builder.finish());
    }
    // Some actors can have empty bitmaps when `target_actor_count > VirtualNode::COUNT`.
    for actor_id in flexible_actors {
        result.insert(actor_id, BitmapBuilder::zeroed(VirtualNode::COUNT).finish());
    }

    result
}

impl<S> GlobalStreamManager<S>
where
    S: MetaStore,
{
    /// Build the context for rescheduling and do some validation for the request.
    async fn build_reschedule_context(
        &self,
        reschedule: &mut HashMap<FragmentId, ParallelUnitReschedule>,
    ) -> MetaResult<RescheduleContext> {
        // Index worker node, used to create actor
        let worker_nodes: HashMap<WorkerId, WorkerNode> = self
            .cluster_manager
            .list_worker_node(WorkerType::ComputeNode, Some(worker_node::State::Running))
            .await
            .into_iter()
            .map(|worker_node| (worker_node.id, worker_node))
            .collect();

        if worker_nodes.is_empty() {
            bail!("no available compute node in the cluster");
        }

        // Associating ParallelUnit with Worker
        let parallel_unit_id_to_worker_id: BTreeMap<_, _> = self
            .cluster_manager
            .list_active_parallel_units()
            .await
            .into_iter()
            .map(|parallel_unit| {
                (
                    parallel_unit.id as ParallelUnitId,
                    parallel_unit.worker_node_id as WorkerId,
                )
            })
            .collect();

        // Index for StreamActor
        let mut actor_map = HashMap::new();
        // Index for Fragment
        let mut fragment_map = HashMap::new();
        // Index for actor status, including actor's parallel unit
        let mut actor_status = BTreeMap::new();
        let mut fragment_state = HashMap::new();
        for table_fragments in self.fragment_manager.list_table_fragments().await? {
            fragment_state.extend(
                table_fragments
                    .fragment_ids()
                    .map(|f| (f, table_fragments.state())),
            );
            fragment_map.extend(table_fragments.fragments.clone());
            actor_map.extend(table_fragments.actor_map());
            actor_status.extend(table_fragments.actor_status.clone());
        }

        // NoShuffle relation index
        let mut no_shuffle_source_fragment_ids = HashSet::new();
        let mut no_shuffle_target_fragment_ids = HashSet::new();

        let mut fragment_dispatcher_map = HashMap::new();

        for actor in actor_map.values() {
            for dispatcher in &actor.dispatcher {
                for downstream_actor_id in &dispatcher.downstream_actor_id {
                    if let Some(downstream_actor) = actor_map.get(downstream_actor_id) {
                        fragment_dispatcher_map
                            .entry(actor.fragment_id)
                            .or_insert(HashMap::new())
                            .insert(downstream_actor.fragment_id, dispatcher.r#type());

                        // Checking for no shuffle dispatchers
                        if dispatcher.r#type() == DispatcherType::NoShuffle {
                            no_shuffle_source_fragment_ids.insert(actor.fragment_id);
                            no_shuffle_target_fragment_ids.insert(downstream_actor.fragment_id);
                        }
                    }
                }
            }
        }

        // Then, we collect all available upstreams
        let mut upstream_dispatchers: HashMap<
            ActorId,
            Vec<(FragmentId, DispatcherId, DispatcherType)>,
        > = HashMap::new();
        for stream_actor in actor_map.values() {
            for dispatcher in &stream_actor.dispatcher {
                for downstream_actor_id in &dispatcher.downstream_actor_id {
                    upstream_dispatchers
                        .entry(*downstream_actor_id as ActorId)
                        .or_default()
                        .push((
                            stream_actor.fragment_id as FragmentId,
                            dispatcher.dispatcher_id as DispatcherId,
                            dispatcher.r#type(),
                        ));
                }
            }
        }

        let mut stream_source_fragment_ids = HashSet::new();
        let mut no_shuffle_reschedule = HashMap::new();
        for (
            fragment_id,
            ParallelUnitReschedule {
                added_parallel_units,
                removed_parallel_units,
            },
        ) in reschedule.iter()
        {
            let fragment = fragment_map
                .get(fragment_id)
                .ok_or_else(|| anyhow!("fragment {fragment_id} does not exist"))?;

            // Check if the reschedule is supported.
            match fragment_state[fragment_id] {
                table_fragments::State::Unspecified => unreachable!(),
                state @ table_fragments::State::Initial
                | state @ table_fragments::State::Creating => {
                    bail!(
                        "the materialized view of fragment {fragment_id} is in state {}",
                        state.as_str_name()
                    )
                }
                table_fragments::State::Created => {}
            }

            if no_shuffle_target_fragment_ids.contains(fragment_id) {
                bail!("rescheduling NoShuffle downstream fragment (maybe Chain fragment) is forbidden, please use NoShuffle upstream fragment (like Materialized fragment) to scale");
            }

            // For the relation of NoShuffle (e.g. Materialize and Chain), we need a special
            // treatment because the upstream and downstream of NoShuffle are always 1-1
            // correspondence, so we need to clone the reschedule plan to the downstream of all
            // cascading relations.
            //
            // Delta join will introduce a `NoShuffle` edge between index chain node and lookup node
            // (index_mv --NoShuffle--> index_chain --NoShuffle--> lookup) which will break current
            // `NoShuffle` scaling assumption. Currently we detect this case and forbid it to scale.
            if no_shuffle_source_fragment_ids.contains(fragment_id) {
                let mut queue: VecDeque<_> = fragment_dispatcher_map
                    .get(fragment_id)
                    .unwrap()
                    .keys()
                    .cloned()
                    .collect();

                while let Some(downstream_id) = queue.pop_front() {
                    if !no_shuffle_target_fragment_ids.contains(&downstream_id) {
                        continue;
                    }

                    if let Some(downstream_fragments) = fragment_dispatcher_map.get(&downstream_id)
                    {
                        // If `NoShuffle` used by other fragment type rather than `ChainNode`, bail.
                        for downstream_fragment_id in downstream_fragments.keys() {
                            let downstream_fragment = fragment_map
                                .get(downstream_fragment_id)
                                .ok_or_else(|| anyhow!("fragment {fragment_id} does not exist"))?;
                            if (downstream_fragment.get_fragment_type_mask()
                                & (FragmentTypeFlag::ChainNode as u32
                                    | FragmentTypeFlag::Mview as u32))
                                == 0
                            {
                                bail!("Rescheduling NoShuffle edge only supports ChainNode and Mview. Other usage for e.g. delta join is forbidden currently.");
                            }
                        }

                        queue.extend(downstream_fragments.keys().cloned());
                    }

                    no_shuffle_reschedule.insert(
                        downstream_id,
                        ParallelUnitReschedule {
                            added_parallel_units: added_parallel_units.clone(),
                            removed_parallel_units: removed_parallel_units.clone(),
                        },
                    );
                }
            }

            if (fragment.get_fragment_type_mask() & FragmentTypeFlag::Source as u32) != 0 {
                let stream_node = fragment.actors.first().unwrap().get_nodes().unwrap();
                let source_node =
                    TableFragments::find_source_node_with_stream_source(stream_node).unwrap();
                if source_node.source_inner.is_some() {
                    stream_source_fragment_ids.insert(*fragment_id);
                }
            }

            // Check if the reschedule plan is valid.
            let current_parallel_units = fragment
                .actors
                .iter()
                .map(|a| {
                    actor_status
                        .get(&a.actor_id)
                        .unwrap()
                        .get_parallel_unit()
                        .unwrap()
                        .id
                })
                .collect::<HashSet<_>>();
            for removed in removed_parallel_units {
                if !current_parallel_units.contains(removed) {
                    bail!(
                        "no actor on the parallel unit {} of fragment {}",
                        removed,
                        fragment_id
                    );
                }
            }
            for added in added_parallel_units {
                if !parallel_unit_id_to_worker_id.contains_key(added) {
                    bail!("parallel unit {} not available", added);
                }
                if current_parallel_units.contains(added) && !removed_parallel_units.contains(added)
                {
                    bail!(
                        "parallel unit {} of fragment {} is already in use",
                        added,
                        fragment_id
                    );
                }
            }

            match fragment.distribution_type() {
                FragmentDistributionType::Hash => {}
                FragmentDistributionType::Single => {
                    if added_parallel_units.len() != removed_parallel_units.len() {
                        bail!("single distribution fragment only support migration");
                    }
                }
                FragmentDistributionType::Unspecified => unreachable!(),
            }
        }

        if !no_shuffle_reschedule.is_empty() {
            tracing::info!(
                "reschedule plan rewritten with NoShuffle reschedule {:?}",
                no_shuffle_reschedule
            );
        }

        // Modifications for NoShuffle downstream.
        reschedule.extend(no_shuffle_reschedule.into_iter());

        Ok(RescheduleContext {
            parallel_unit_id_to_worker_id,
            actor_map,
            actor_status,
            fragment_map,
            worker_nodes,
            upstream_dispatchers,
            stream_source_fragment_ids,
            no_shuffle_target_fragment_ids,
            no_shuffle_source_fragment_ids,
            fragment_dispatcher_map,
        })
    }

    pub async fn reschedule_actors(
        &self,
        reschedules: HashMap<FragmentId, ParallelUnitReschedule>,
    ) -> MetaResult<()> {
        let mut revert_funcs = vec![];
        if let Err(e) = self
            .reschedule_actors_impl(&mut revert_funcs, reschedules)
            .await
        {
            for revert_func in revert_funcs.into_iter().rev() {
                revert_func.await;
            }
            return Err(e);
        }
        Ok(())
    }

    async fn reschedule_actors_impl(
        &self,
        revert_funcs: &mut Vec<BoxFuture<'_, ()>>,
        mut reschedules: HashMap<FragmentId, ParallelUnitReschedule>,
    ) -> MetaResult<()> {
        let ctx = self.build_reschedule_context(&mut reschedules).await?;
        // Index of actors to create/remove
        // Fragment Id => ( Actor Id => Parallel Unit Id )

        let (fragment_actors_to_remove, fragment_actors_to_create) =
            self.arrange_reschedules(&reschedules, &ctx).await?;

        let mut fragment_actor_bitmap = HashMap::new();
        for fragment_id in reschedules.keys() {
            if ctx.no_shuffle_target_fragment_ids.contains(fragment_id) {
                // skipping chain fragment, we need to clone the upstream materialize fragment's
                // mapping later
                continue;
            }

            let actors_to_create = fragment_actors_to_create
                .get(fragment_id)
                .map(|map| map.keys().cloned().collect())
                .unwrap_or_default();

            let actors_to_remove = fragment_actors_to_remove
                .get(fragment_id)
                .map(|map| map.keys().cloned().collect())
                .unwrap_or_default();

            let fragment = ctx.fragment_map.get(fragment_id).unwrap();

            match fragment.distribution_type() {
                FragmentDistributionType::Single => {
                    // Skip rebalance action for single distribution (always None)
                    fragment_actor_bitmap
                        .insert(fragment.fragment_id as FragmentId, Default::default());
                }
                FragmentDistributionType::Hash => {
                    let actor_vnode = rebalance_actor_vnode(
                        &fragment.actors,
                        &actors_to_remove,
                        &actors_to_create,
                    );

                    fragment_actor_bitmap.insert(fragment.fragment_id as FragmentId, actor_vnode);
                }

                FragmentDistributionType::Unspecified => unreachable!(),
            }
        }

        // Index for fragment -> { actor -> parallel_unit } after reschedule.
        // Since we need to organize the upstream and downstream relationships of NoShuffle,
        // we need to organize the actor distribution after a scaling.
        let mut fragment_actors_after_reschedule = HashMap::with_capacity(reschedules.len());
        for fragment_id in reschedules.keys() {
            let fragment = ctx.fragment_map.get(fragment_id).unwrap();
            let mut new_actor_ids = BTreeMap::new();
            for actor in &fragment.actors {
                if let Some(actors_to_remove) = fragment_actors_to_remove.get(fragment_id) {
                    if actors_to_remove.contains_key(&actor.actor_id) {
                        continue;
                    }
                }
                let parallel_unit_id = ctx.actor_id_to_parallel_unit(&actor.actor_id)?.id;
                new_actor_ids.insert(
                    actor.actor_id as ActorId,
                    parallel_unit_id as ParallelUnitId,
                );
            }

            if let Some(actors_to_create) = fragment_actors_to_create.get(fragment_id) {
                for (actor_id, parallel_unit_id) in actors_to_create {
                    new_actor_ids.insert(*actor_id, *parallel_unit_id as ParallelUnitId);
                }
            }

            if new_actor_ids.is_empty() {
                bail!(
                    "should be at least one actor in fragment {} after rescheduling",
                    fragment_id
                );
            }

            fragment_actors_after_reschedule.insert(*fragment_id, new_actor_ids);
        }

        let fragment_actors_after_reschedule = fragment_actors_after_reschedule;

        // In order to maintain consistency with the original structure, the upstream and downstream
        // actors of NoShuffle need to be in the same parallel unit and hold the same virtual nodes,
        // so for the actors after the upstream rebalancing, we need to find the parallel
        // unit corresponding to each actor, and find the downstream actor corresponding to
        // the parallel unit, and then copy the Bitmap to the corresponding actor. At the
        // same time, we need to sort out the relationship between upstream and downstream
        // actors
        fn arrange_no_shuffle_relation(
            ctx: &RescheduleContext,
            fragment_id: &FragmentId,
            upstream_fragment_id: &FragmentId,
            fragment_actors_after_reschedule: &HashMap<
                FragmentId,
                BTreeMap<ActorId, ParallelUnitId>,
            >,
            fragment_updated_bitmap: &mut HashMap<FragmentId, HashMap<ActorId, Bitmap>>,
            no_shuffle_upstream_actor_map: &mut HashMap<ActorId, ActorId>,
            no_shuffle_downstream_actors_map: &mut HashMap<ActorId, HashMap<FragmentId, ActorId>>,
        ) {
            if !ctx.no_shuffle_target_fragment_ids.contains(fragment_id) {
                return;
            }

            if fragment_updated_bitmap.contains_key(fragment_id) {
                // Diamond dependency
                return;
            }

            let fragment = ctx.fragment_map.get(fragment_id).unwrap();

            // If the upstream is a Singleton Fragment, there will be no Bitmap changes
            let mut upstream_fragment_bitmap = fragment_updated_bitmap
                .get(upstream_fragment_id)
                .cloned()
                .unwrap_or_default();

            let upstream_fragment_actor_map = fragment_actors_after_reschedule
                .get(upstream_fragment_id)
                .cloned()
                .unwrap();

            let mut parallel_unit_id_to_actor_id = HashMap::new();
            for (actor_id, parallel_unit_id) in
                fragment_actors_after_reschedule.get(fragment_id).unwrap()
            {
                parallel_unit_id_to_actor_id.insert(*parallel_unit_id, *actor_id);
            }

            let mut fragment_bitmap = HashMap::new();
            for (upstream_actor_id, parallel_unit_id) in upstream_fragment_actor_map {
                let actor_id = parallel_unit_id_to_actor_id.get(&parallel_unit_id).unwrap();

                if let Some(bitmap) = upstream_fragment_bitmap.remove(&upstream_actor_id) {
                    // Copy the bitmap
                    fragment_bitmap.insert(*actor_id, bitmap);
                }

                no_shuffle_upstream_actor_map.insert(*actor_id as ActorId, upstream_actor_id);
                no_shuffle_downstream_actors_map
                    .entry(upstream_actor_id)
                    .or_default()
                    .insert(*fragment_id, *actor_id);
            }

            match fragment.distribution_type() {
                FragmentDistributionType::Hash => {}
                FragmentDistributionType::Single => {
                    // single distribution should update nothing
                    assert!(fragment_bitmap.is_empty());
                }
                FragmentDistributionType::Unspecified => unreachable!(),
            }

            fragment_updated_bitmap
                .try_insert(*fragment_id, fragment_bitmap)
                .unwrap();

            if let Some(downstream_fragments) = ctx.fragment_dispatcher_map.get(fragment_id) {
                for downstream_fragment_id in downstream_fragments.keys() {
                    arrange_no_shuffle_relation(
                        ctx,
                        downstream_fragment_id,
                        fragment_id,
                        fragment_actors_after_reschedule,
                        fragment_updated_bitmap,
                        no_shuffle_upstream_actor_map,
                        no_shuffle_downstream_actors_map,
                    );
                }
            }
        }

        let mut no_shuffle_upstream_actor_map = HashMap::new();
        let mut no_shuffle_downstream_actors_map = HashMap::new();
        // For all roots in the upstream and downstream dependency trees of NoShuffle, recursively
        // find all correspondences
        for fragment_id in reschedules.keys() {
            if ctx.no_shuffle_source_fragment_ids.contains(fragment_id)
                && !ctx.no_shuffle_target_fragment_ids.contains(fragment_id)
            {
                if let Some(downstream_fragments) = ctx.fragment_dispatcher_map.get(fragment_id) {
                    for downstream_fragment_id in downstream_fragments.keys() {
                        arrange_no_shuffle_relation(
                            &ctx,
                            downstream_fragment_id,
                            fragment_id,
                            &fragment_actors_after_reschedule,
                            &mut fragment_actor_bitmap,
                            &mut no_shuffle_upstream_actor_map,
                            &mut no_shuffle_downstream_actors_map,
                        );
                    }
                }
            }
        }

        let mut new_created_actors = HashMap::new();
        for fragment_id in reschedules.keys() {
            let actors_to_create = fragment_actors_to_create
                .get(fragment_id)
                .cloned()
                .unwrap_or_default();

            let fragment = ctx.fragment_map.get(fragment_id).unwrap();

            assert!(!fragment.actors.is_empty());

            for (actor_to_create, sample_actor) in actors_to_create
                .iter()
                .zip_eq_debug(repeat(fragment.actors.first().unwrap()).take(actors_to_create.len()))
            {
                let new_actor_id = actor_to_create.0;
                let mut new_actor = sample_actor.clone();

                // This should be assigned before the `modify_actor_upstream_and_downstream` call,
                // because we need to use the new actor id to find the upstream and
                // downstream in the NoShuffle relationship
                new_actor.actor_id = *new_actor_id;

                Self::modify_actor_upstream_and_downstream(
                    &ctx,
                    &fragment_actors_to_remove,
                    &fragment_actors_to_create,
                    &fragment_actor_bitmap,
                    &no_shuffle_upstream_actor_map,
                    &no_shuffle_downstream_actors_map,
                    &mut new_actor,
                )?;

                if let Some(bitmap) = fragment_actor_bitmap
                    .get(fragment_id)
                    .and_then(|actor_bitmaps| actor_bitmaps.get(new_actor_id))
                {
                    new_actor.vnode_bitmap = Some(bitmap.to_protobuf());
                }

                new_created_actors.insert(*new_actor_id, new_actor);
            }
        }

        // After modification, for newly created actors, both upstream and downstream actor ids
        // have been modified
        let mut actor_infos_to_broadcast = BTreeMap::new();
        let mut node_actors_to_create: HashMap<WorkerId, Vec<_>> = HashMap::new();
        let mut broadcast_worker_ids = HashSet::new();

        for actors_to_create in fragment_actors_to_create.values() {
            for (new_actor_id, new_parallel_unit_id) in actors_to_create {
                let new_actor = new_created_actors.get(new_actor_id).unwrap();
                for upstream_actor_id in &new_actor.upstream_actor_id {
                    if new_created_actors.contains_key(upstream_actor_id) {
                        continue;
                    }

                    let upstream_worker_id = ctx
                        .actor_id_to_parallel_unit(upstream_actor_id)?
                        .worker_node_id;
                    let upstream_worker = ctx.worker_nodes.get(&upstream_worker_id).unwrap();

                    // Force broadcast upstream actor info, because the actor information of the new
                    // node may not have been synchronized yet
                    actor_infos_to_broadcast.insert(
                        *upstream_actor_id,
                        ActorInfo {
                            actor_id: *upstream_actor_id,
                            host: upstream_worker.host.clone(),
                        },
                    );

                    broadcast_worker_ids.insert(upstream_worker_id);
                }

                for dispatcher in &new_actor.dispatcher {
                    for downstream_actor_id in &dispatcher.downstream_actor_id {
                        if new_created_actors.contains_key(downstream_actor_id) {
                            continue;
                        }

                        let downstream_worker_id = ctx
                            .actor_id_to_parallel_unit(downstream_actor_id)?
                            .worker_node_id;
                        let downstream_worker =
                            ctx.worker_nodes.get(&downstream_worker_id).unwrap();

                        actor_infos_to_broadcast.insert(
                            *downstream_actor_id,
                            ActorInfo {
                                actor_id: *downstream_actor_id,
                                host: downstream_worker.host.clone(),
                            },
                        );

                        broadcast_worker_ids.insert(downstream_worker_id);
                    }
                }

                let worker = ctx.parallel_unit_id_to_worker(new_parallel_unit_id)?;

                node_actors_to_create
                    .entry(worker.id)
                    .or_default()
                    .push(new_actor.clone());

                broadcast_worker_ids.insert(worker.id);

                actor_infos_to_broadcast.insert(
                    *new_actor_id,
                    ActorInfo {
                        actor_id: *new_actor_id,
                        host: worker.host.clone(),
                    },
                );
            }
        }

        self.create_actors_on_compute_node(
            &ctx.worker_nodes,
            actor_infos_to_broadcast,
            node_actors_to_create,
            broadcast_worker_ids,
        )
        .await?;

        // For stream source fragments, we need to reallocate the splits.
        // Because we are in the Pause state, so it's no problem to reallocate
        let mut fragment_stream_source_actor_splits = HashMap::new();
        for fragment_id in reschedules.keys() {
            let actors_after_reschedule =
                fragment_actors_after_reschedule.get(fragment_id).unwrap();

            if ctx.stream_source_fragment_ids.contains(fragment_id) {
                let actor_ids = actors_after_reschedule.keys().cloned();
                let actor_splits = self
                    .source_manager
                    .reallocate_splits(fragment_id, actor_ids)
                    .await?;

                fragment_stream_source_actor_splits.insert(*fragment_id, actor_splits);
            }
        }

        // Generate fragment reschedule plan
        let mut reschedule_fragment: HashMap<FragmentId, Reschedule> =
            HashMap::with_capacity(reschedules.len());

        for (fragment_id, _) in reschedules {
            let actors_to_create = fragment_actors_to_create
                .get(&fragment_id)
                .cloned()
                .unwrap_or_default()
                .into_keys()
                .collect();
            let actors_to_remove = fragment_actors_to_remove
                .get(&fragment_id)
                .cloned()
                .unwrap_or_default()
                .into_keys()
                .collect();

            let actors_after_reschedule =
                fragment_actors_after_reschedule.get(&fragment_id).unwrap();

            let parallel_unit_to_actor_after_reschedule: BTreeMap<_, _> = actors_after_reschedule
                .iter()
                .map(|(actor_id, parallel_unit_id)| {
                    (*parallel_unit_id as ParallelUnitId, *actor_id as ActorId)
                })
                .collect();

            assert!(!parallel_unit_to_actor_after_reschedule.is_empty());

            let fragment = ctx.fragment_map.get(&fragment_id).unwrap();

            let in_degree_types: HashSet<_> = fragment
                .upstream_fragment_ids
                .iter()
                .flat_map(|upstream_fragment_id| {
                    ctx.fragment_dispatcher_map
                        .get(upstream_fragment_id)
                        .and_then(|dispatcher_map| {
                            dispatcher_map.get(&fragment.fragment_id).cloned()
                        })
                })
                .collect();

            let upstream_dispatcher_mapping = match fragment.distribution_type() {
                FragmentDistributionType::Hash => {
                    if !in_degree_types.contains(&DispatcherType::Hash) {
                        None
                    } else if parallel_unit_to_actor_after_reschedule.len() == 1 {
                        let actor_id = parallel_unit_to_actor_after_reschedule
                            .into_values()
                            .next()
                            .unwrap();
                        Some(ActorMapping::new_single(actor_id))
                    } else {
                        // Changes of the bitmap must occur in the case of HashDistribution
                        Some(ActorMapping::from_bitmaps(
                            &fragment_actor_bitmap[&fragment_id],
                        ))
                    }
                }

                FragmentDistributionType::Single => {
                    assert!(fragment_actor_bitmap.get(&fragment_id).unwrap().is_empty());
                    None
                }
                FragmentDistributionType::Unspecified => unreachable!(),
            };

            let mut upstream_fragment_dispatcher_set = BTreeSet::new();

            for actor in &fragment.actors {
                if let Some(upstream_actor_tuples) = ctx.upstream_dispatchers.get(&actor.actor_id) {
                    for (upstream_fragment_id, upstream_dispatcher_id, upstream_dispatcher_type) in
                        upstream_actor_tuples
                    {
                        match upstream_dispatcher_type {
                            DispatcherType::Unspecified => unreachable!(),
                            DispatcherType::NoShuffle => {}
                            _ => {
                                upstream_fragment_dispatcher_set
                                    .insert((*upstream_fragment_id, *upstream_dispatcher_id));
                            }
                        }
                    }
                }
            }

            let downstream_fragment_ids =
                if let Some(downstream_fragments) = ctx.fragment_dispatcher_map.get(&fragment_id) {
                    // Skip NoShuffle fragments' downstream
                    if ctx
                        .no_shuffle_source_fragment_ids
                        .contains(&fragment.fragment_id)
                    {
                        vec![]
                    } else {
                        downstream_fragments.keys().copied().collect_vec()
                    }
                } else {
                    vec![]
                };

            let vnode_bitmap_updates = match fragment.distribution_type() {
                FragmentDistributionType::Hash => {
                    let mut vnode_bitmap_updates =
                        fragment_actor_bitmap.remove(&fragment_id).unwrap();

                    // We need to keep the bitmaps from changed actors only,
                    // otherwise the barrier will become very large with many actors
                    for actor_id in actors_after_reschedule.keys() {
                        assert!(vnode_bitmap_updates.contains_key(actor_id));

                        // retain actor
                        if let Some(actor) = ctx.actor_map.get(actor_id) {
                            let bitmap = vnode_bitmap_updates.get(actor_id).unwrap();

                            if let Some(buffer) = actor.vnode_bitmap.as_ref() {
                                let prev_bitmap = Bitmap::from(buffer);

                                if prev_bitmap.eq(bitmap) {
                                    vnode_bitmap_updates.remove(actor_id);
                                }
                            }
                        }
                    }

                    vnode_bitmap_updates
                }
                FragmentDistributionType::Single => HashMap::new(),
                FragmentDistributionType::Unspecified => unreachable!(),
            };

            let upstream_fragment_dispatcher_ids =
                upstream_fragment_dispatcher_set.into_iter().collect_vec();

            let actor_splits = fragment_stream_source_actor_splits
                .get(&fragment_id)
                .cloned()
                .unwrap_or_default();

            reschedule_fragment.insert(
                fragment_id,
                Reschedule {
                    added_actors: actors_to_create,
                    removed_actors: actors_to_remove,
                    vnode_bitmap_updates,
                    upstream_fragment_dispatcher_ids,
                    upstream_dispatcher_mapping,
                    downstream_fragment_ids,
                    actor_splits,
                },
            );
        }

        let mut fragment_created_actors = HashMap::new();
        for (fragment_id, actors_to_create) in &fragment_actors_to_create {
            let mut created_actors = HashMap::new();
            for (actor_id, parallel_unit_id) in actors_to_create {
                let actor = new_created_actors.get(actor_id).cloned().unwrap();
                let worker_id = ctx
                    .parallel_unit_id_to_worker_id
                    .get(parallel_unit_id)
                    .unwrap();
                created_actors.insert(
                    *actor_id,
                    (
                        actor,
                        ActorStatus {
                            parallel_unit: Some(ParallelUnit {
                                id: *parallel_unit_id,
                                worker_node_id: *worker_id,
                            }),
                            state: ActorState::Inactive as i32,
                        },
                    ),
                );
            }

            fragment_created_actors.insert(*fragment_id, created_actors);
        }

        let applied_reschedules = self
            .fragment_manager
            .pre_apply_reschedules(fragment_created_actors)
            .await;

        let fragment_manager_ref = self.fragment_manager.clone();

        revert_funcs.push(Box::pin(async move {
            fragment_manager_ref
                .cancel_apply_reschedules(applied_reschedules)
                .await;
        }));

        let _source_pause_guard = self.source_manager.paused.lock().await;

        tracing::trace!("reschedule plan: {:#?}", reschedule_fragment);

        self.barrier_scheduler
            .run_command_with_paused(Command::RescheduleFragment(reschedule_fragment))
            .await?;

        Ok(())
    }

    async fn create_actors_on_compute_node(
        &self,
        worker_nodes: &HashMap<WorkerId, WorkerNode>,
        actor_infos_to_broadcast: BTreeMap<u32, ActorInfo>,
        node_actors_to_create: HashMap<WorkerId, Vec<StreamActor>>,
        broadcast_worker_ids: HashSet<u32>,
    ) -> MetaResult<()> {
        for worker_id in &broadcast_worker_ids {
            let node = worker_nodes.get(worker_id).unwrap();
            let client = self.env.stream_client_pool().get(node).await?;

            let actor_infos_to_broadcast = actor_infos_to_broadcast.values().cloned().collect();

            client
                .to_owned()
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_infos_to_broadcast,
                })
                .await?;
        }

        for (node_id, stream_actors) in &node_actors_to_create {
            let node = worker_nodes.get(node_id).unwrap();
            let client = self.env.stream_client_pool().get(node).await?;
            let request_id = Uuid::new_v4().to_string();
            let request = UpdateActorsRequest {
                request_id,
                actors: stream_actors.clone(),
            };

            client.to_owned().update_actors(request).await?;
        }

        for (node_id, stream_actors) in node_actors_to_create {
            let node = worker_nodes.get(&node_id).unwrap();
            let client = self.env.stream_client_pool().get(node).await?;
            let request_id = Uuid::new_v4().to_string();

            client
                .to_owned()
                .build_actors(BuildActorsRequest {
                    request_id,
                    actor_id: stream_actors
                        .iter()
                        .map(|stream_actor| stream_actor.actor_id)
                        .collect(),
                })
                .await?;
        }

        Ok(())
    }

    async fn arrange_reschedules(
        &self,
        reschedule: &HashMap<FragmentId, ParallelUnitReschedule>,
        ctx: &RescheduleContext,
    ) -> MetaResult<(
        HashMap<FragmentId, BTreeMap<ActorId, ParallelUnitId>>,
        HashMap<FragmentId, BTreeMap<ActorId, ParallelUnitId>>,
    )> {
        let mut fragment_actors_to_remove = HashMap::with_capacity(reschedule.len());
        let mut fragment_actors_to_create = HashMap::with_capacity(reschedule.len());

        for (
            fragment_id,
            ParallelUnitReschedule {
                added_parallel_units,
                removed_parallel_units,
            },
        ) in reschedule
        {
            let fragment = ctx.fragment_map.get(fragment_id).unwrap();

            // Actor Id => Parallel Unit Id
            let mut actors_to_remove = BTreeMap::new();
            let mut actors_to_create = BTreeMap::new();

            let parallel_unit_to_actor: HashMap<_, _> = fragment
                .actors
                .iter()
                .map(|actor| {
                    ctx.actor_id_to_parallel_unit(&actor.actor_id)
                        .map(|parallel_unit| {
                            (
                                parallel_unit.id as ParallelUnitId,
                                actor.actor_id as ActorId,
                            )
                        })
                })
                .try_collect()?;

            for removed_parallel_unit_id in removed_parallel_units {
                if let Some(removed_actor_id) = parallel_unit_to_actor.get(removed_parallel_unit_id)
                {
                    actors_to_remove.insert(*removed_actor_id, *removed_parallel_unit_id);
                }
            }

            for created_parallel_unit_id in added_parallel_units {
                let id = self
                    .env
                    .id_gen_manager()
                    .generate::<{ IdCategory::Actor }>()
                    .await? as ActorId;

                actors_to_create.insert(id, *created_parallel_unit_id);
            }

            if !actors_to_remove.is_empty() {
                fragment_actors_to_remove.insert(*fragment_id as FragmentId, actors_to_remove);
            }

            if !actors_to_create.is_empty() {
                fragment_actors_to_create.insert(*fragment_id as FragmentId, actors_to_create);
            }
        }

        Ok((fragment_actors_to_remove, fragment_actors_to_create))
    }

    /// Modifies the upstream and downstream actors of the new created actor according to the
    /// overall changes, and is used to handle cascading updates
    fn modify_actor_upstream_and_downstream(
        ctx: &RescheduleContext,
        fragment_actors_to_remove: &HashMap<FragmentId, BTreeMap<ActorId, ParallelUnitId>>,
        fragment_actors_to_create: &HashMap<FragmentId, BTreeMap<ActorId, ParallelUnitId>>,
        fragment_actor_bitmap: &HashMap<FragmentId, HashMap<ActorId, Bitmap>>,
        no_shuffle_upstream_actor_map: &HashMap<ActorId, ActorId>,
        no_shuffle_downstream_actors_map: &HashMap<ActorId, HashMap<FragmentId, ActorId>>,
        new_actor: &mut StreamActor,
    ) -> MetaResult<()> {
        let fragment = &ctx.fragment_map.get(&new_actor.fragment_id).unwrap();
        let mut applied_upstream_fragment_actor_ids = HashMap::new();

        for upstream_fragment_id in &fragment.upstream_fragment_ids {
            let upstream_dispatch_type = &ctx
                .fragment_dispatcher_map
                .get(upstream_fragment_id)
                .and_then(|map| map.get(&fragment.fragment_id))
                .unwrap();

            match upstream_dispatch_type {
                DispatcherType::Unspecified => unreachable!(),
                DispatcherType::Hash | DispatcherType::Broadcast | DispatcherType::Simple => {
                    let upstream_fragment = &ctx.fragment_map.get(upstream_fragment_id).unwrap();
                    let mut upstream_actor_ids = upstream_fragment
                        .actors
                        .iter()
                        .map(|actor| actor.actor_id as ActorId)
                        .collect_vec();

                    if let Some(upstream_actors_to_remove) =
                        fragment_actors_to_remove.get(upstream_fragment_id)
                    {
                        upstream_actor_ids.drain_filter(|actor_id| {
                            upstream_actors_to_remove.contains_key(actor_id)
                        });
                    }

                    if let Some(upstream_actors_to_create) =
                        fragment_actors_to_create.get(upstream_fragment_id)
                    {
                        upstream_actor_ids.extend(upstream_actors_to_create.keys().cloned());
                    }

                    applied_upstream_fragment_actor_ids.insert(
                        *upstream_fragment_id as FragmentId,
                        upstream_actor_ids.clone(),
                    );
                }
                DispatcherType::NoShuffle => {
                    let no_shuffle_upstream_actor_id = no_shuffle_upstream_actor_map
                        .get(&new_actor.actor_id)
                        .cloned()
                        .unwrap();

                    applied_upstream_fragment_actor_ids.insert(
                        *upstream_fragment_id as FragmentId,
                        vec![no_shuffle_upstream_actor_id as ActorId],
                    );
                }
            }
        }

        new_actor.upstream_actor_id = applied_upstream_fragment_actor_ids
            .values()
            .flatten()
            .cloned()
            .collect_vec();

        fn replace_merge_node_upstream(
            stream_node: &mut StreamNode,
            applied_upstream_fragment_actor_ids: &HashMap<FragmentId, Vec<ActorId>>,
        ) {
            if let Some(NodeBody::Merge(s)) = stream_node.node_body.as_mut() {
                s.upstream_actor_id = applied_upstream_fragment_actor_ids
                    .get(&s.upstream_fragment_id)
                    .cloned()
                    .unwrap();
            }

            for child in &mut stream_node.input {
                replace_merge_node_upstream(child, applied_upstream_fragment_actor_ids);
            }
        }

        if let Some(node) = new_actor.nodes.as_mut() {
            replace_merge_node_upstream(node, &applied_upstream_fragment_actor_ids);
        }

        // Update downstream actor ids
        for dispatcher in &mut new_actor.dispatcher {
            let downstream_fragment_id = dispatcher
                .downstream_actor_id
                .iter()
                .filter_map(|actor_id| ctx.actor_map.get(actor_id).map(|actor| actor.fragment_id))
                .dedup()
                .exactly_one()
                .unwrap() as FragmentId;

            let downstream_fragment_actors_to_remove =
                fragment_actors_to_remove.get(&downstream_fragment_id);
            let downstream_fragment_actors_to_create =
                fragment_actors_to_create.get(&downstream_fragment_id);

            match dispatcher.r#type() {
                d @ (DispatcherType::Hash | DispatcherType::Simple | DispatcherType::Broadcast) => {
                    if let Some(downstream_actors_to_remove) = downstream_fragment_actors_to_remove
                    {
                        dispatcher
                            .downstream_actor_id
                            .drain_filter(|id| downstream_actors_to_remove.contains_key(id));
                    }

                    if let Some(downstream_actors_to_create) = downstream_fragment_actors_to_create
                    {
                        dispatcher
                            .downstream_actor_id
                            .extend(downstream_actors_to_create.keys().cloned())
                    }

                    // There should be still exactly one downstream actor
                    if d == DispatcherType::Simple {
                        assert_eq!(dispatcher.downstream_actor_id.len(), 1);
                    }
                }
                DispatcherType::NoShuffle => {
                    assert_eq!(dispatcher.downstream_actor_id.len(), 1);
                    let downstream_actor_id = no_shuffle_downstream_actors_map
                        .get(&new_actor.actor_id)
                        .and_then(|map| map.get(&downstream_fragment_id))
                        .unwrap();
                    dispatcher.downstream_actor_id = vec![*downstream_actor_id as ActorId];
                }
                DispatcherType::Unspecified => unreachable!(),
            }

            if let Some(mapping) = dispatcher.hash_mapping.as_mut() {
                if let Some(downstream_updated_bitmap) =
                    fragment_actor_bitmap.get(&downstream_fragment_id)
                {
                    // If downstream scale in/out
                    *mapping = ActorMapping::from_bitmaps(downstream_updated_bitmap).to_protobuf();
                }
            }
        }

        Ok(())
    }
}
