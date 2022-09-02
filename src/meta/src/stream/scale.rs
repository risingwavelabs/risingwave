// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::{anyhow, Context};
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::types::{ParallelUnitId, VIRTUAL_NODE_COUNT};
use risingwave_pb::common::{ActorInfo, ParallelUnit, ParallelUnitMapping, WorkerNode, WorkerType};
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::table_fragments::{ActorState, ActorStatus, Fragment};
use risingwave_pb::stream_plan::barrier::Mutation;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    ActorMapping, DispatcherType, FragmentType, PauseMutation, ResumeMutation, StreamActor,
    StreamNode,
};
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, HangingChannel, UpdateActorsRequest,
};
use uuid::Uuid;

use crate::barrier::{Command, Reschedule};
use crate::manager::{IdCategory, WorkerId};
use crate::model::{ActorId, DispatcherId, FragmentId};
use crate::storage::MetaStore;
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
    /// The downstream list of `Fragment`, which will be embedded in `FragmentManager` in the
    /// future
    downstream_fragment_id_map: HashMap<FragmentId, HashSet<FragmentId>>,
    /// Indexes for all `Worker`s
    worker_nodes: HashMap<WorkerId, WorkerNode>,
    /// Index of all `Actor` upstreams, specific to `Dispatcher`
    upstream_dispatchers: HashMap<ActorId, Vec<(FragmentId, ActorId, DispatcherId)>>,
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

impl<S> GlobalStreamManager<S>
where
    S: MetaStore,
{
    async fn build_reschedule_context(
        &self,
        reschedule: &HashMap<FragmentId, ParallelUnitReschedule>,
    ) -> MetaResult<RescheduleContext> {
        // Index worker node, used to create actor
        let worker_nodes: HashMap<WorkerId, WorkerNode> = self
            .cluster_manager
            .list_worker_node(
                WorkerType::ComputeNode,
                Some(risingwave_pb::common::worker_node::State::Running),
            )
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
            .list_parallel_units()
            .await
            .into_iter()
            .map(|parallel_unit| {
                (
                    parallel_unit.id as ParallelUnitId,
                    parallel_unit.worker_node_id as WorkerId,
                )
            })
            .collect();

        let mut chain_actor_ids = HashSet::new();
        let mut actor_map = HashMap::new();
        let mut fragment_map = HashMap::new();
        let mut actor_status = BTreeMap::new();
        for table_fragments in self.fragment_manager.list_table_fragments().await? {
            fragment_map.extend(table_fragments.fragments.clone());
            actor_map.extend(table_fragments.actor_map());
            chain_actor_ids.extend(table_fragments.chain_actor_ids());
            actor_status.extend(table_fragments.actor_status.clone());
        }

        // Index the downstream fragment, will skip the chain of mv on mv
        let mut downstream_fragment_id_map = HashMap::new();
        for actor in actor_map.values() {
            for dispatcher in &actor.dispatcher {
                for downstream_actor_id in &dispatcher.downstream_actor_id {
                    if chain_actor_ids.contains(downstream_actor_id) {
                        continue;
                    }

                    if let Some(downstream_actor) = actor_map.get(downstream_actor_id) {
                        downstream_fragment_id_map
                            .entry(actor.fragment_id as FragmentId)
                            .or_insert_with(HashSet::new)
                            .insert(downstream_actor.fragment_id as FragmentId);
                    }
                }
            }
        }

        // then, we collect all available upstream
        let mut upstream_dispatchers: HashMap<ActorId, Vec<(FragmentId, ActorId, DispatcherId)>> =
            HashMap::new();
        for (actor_id, stream_actor) in &actor_map {
            for dispatcher in &stream_actor.dispatcher {
                for downstream_actor_id in &dispatcher.downstream_actor_id {
                    upstream_dispatchers
                        .entry(*downstream_actor_id as ActorId)
                        .or_insert(vec![])
                        .push((
                            stream_actor.fragment_id as FragmentId,
                            *actor_id as ActorId,
                            dispatcher.dispatcher_id as DispatcherId,
                        ));
                }
            }
        }

        for (
            fragment_id,
            ParallelUnitReschedule {
                added_parallel_units,
                removed_parallel_units,
            },
        ) in reschedule
        {
            let fragment = fragment_map
                .get(fragment_id)
                .context("fragment id does not exist")?;

            match fragment.get_fragment_type()? {
                FragmentType::Source => bail!("scaling SourceNode is not supported"),
                FragmentType::Sink if downstream_fragment_id_map.get(fragment_id).is_some() => {
                    bail!("scaling of SinkNode with downstream is not supported")
                }
                _ => {}
            }

            for parallel_unit_id in added_parallel_units
                .iter()
                .chain(removed_parallel_units.iter())
            {
                if !parallel_unit_id_to_worker_id.contains_key(parallel_unit_id) {
                    bail!("ParallelUnit {} not found", parallel_unit_id);
                }
            }
        }

        Ok(RescheduleContext {
            parallel_unit_id_to_worker_id,
            actor_map,
            actor_status,
            fragment_map,
            downstream_fragment_id_map,
            worker_nodes,
            upstream_dispatchers,
        })
    }

    pub async fn reschedule_actors(
        &self,
        reschedule: HashMap<FragmentId, ParallelUnitReschedule>,
    ) -> MetaResult<()> {
        let ctx = self.build_reschedule_context(&reschedule).await?;

        // Index of actors to create/remove
        let mut fragment_actors_to_remove = HashMap::with_capacity(reschedule.len());
        let mut fragment_actors_to_create = HashMap::with_capacity(reschedule.len());

        for (
            fragment_id,
            ParallelUnitReschedule {
                added_parallel_units,
                removed_parallel_units,
            },
        ) in &reschedule
        {
            let fragment = ctx.fragment_map.get(fragment_id).unwrap();
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
                    .id_gen_manager
                    .generate::<{ IdCategory::Actor }>()
                    .await? as ActorId;

                actors_to_create.insert(id, *created_parallel_unit_id);
            }

            fragment_actors_to_remove.insert(*fragment_id as FragmentId, actors_to_remove);
            fragment_actors_to_create.insert(*fragment_id as FragmentId, actors_to_create);
        }

        let fragment_actors_to_remove = fragment_actors_to_remove;
        let fragment_actors_to_create = fragment_actors_to_create;

        // Note: we must create hanging channels at first
        let mut node_hanging_channels: HashMap<WorkerId, Vec<HangingChannel>> = HashMap::new();
        let mut new_created_actors = HashMap::new();
        for fragment_id in reschedule.keys() {
            let actors_to_create = fragment_actors_to_create
                .get(fragment_id)
                .cloned()
                .unwrap_or_default();
            let actors_to_remove = fragment_actors_to_remove
                .get(fragment_id)
                .cloned()
                .unwrap_or_default();

            if actors_to_remove.len() != actors_to_create.len() {
                bail!("TODO: we only support migration for now")
            }

            for (actor_to_remove, actor_to_create) in
                actors_to_remove.iter().zip_eq(actors_to_create.iter())
            {
                // use old actor as simple actor
                let mut new_actor = ctx.actor_map.get(actor_to_remove.0).cloned().unwrap();

                let new_parallel_unit_id = actor_to_create.1;
                let new_actor_id = actor_to_create.0;

                let worker = ctx.parallel_unit_id_to_worker(new_parallel_unit_id)?;
                for upstream_actor_id in &new_actor.upstream_actor_id {
                    let upstream_worker_id = ctx
                        .actor_id_to_parallel_unit(upstream_actor_id)?
                        .worker_node_id;
                    node_hanging_channels
                        .entry(upstream_worker_id)
                        .or_default()
                        .push(HangingChannel {
                            upstream: Some(ActorInfo {
                                actor_id: *upstream_actor_id,
                                host: None,
                            }),
                            downstream: Some(ActorInfo {
                                actor_id: *new_actor_id,
                                host: worker.host.clone(),
                            }),
                        });
                }

                Self::modify_actor_upstream_and_downstream(
                    &ctx.fragment_map,
                    &ctx.actor_map,
                    &fragment_actors_to_remove,
                    &fragment_actors_to_create,
                    &mut new_actor,
                )?;

                new_actor.actor_id = *new_actor_id;
                new_created_actors.insert(*new_actor_id, new_actor);
            }
        }

        // todo: update actor vnode mapping when scale in/out

        // After modification, for newly created `Actor` s, both upstream and downstream actor ids
        // have been modified
        let mut actor_infos_to_broadcast = BTreeMap::new();
        let mut node_actors_to_create: HashMap<WorkerId, Vec<_>> = HashMap::new();
        let mut broadcast_worker_ids = HashSet::new();
        for actors_to_create in fragment_actors_to_create.values() {
            for (new_actor_id, new_parallel_unit_id) in actors_to_create {
                let new_actor = new_created_actors.get(new_actor_id).unwrap();
                for upstream_actor_id in &new_actor.upstream_actor_id {
                    if new_created_actors.contains_key(upstream_actor_id) {
                        // We will only create a hanging channel from the existing actor to the
                        // newly created actor
                        continue;
                    }

                    let upstream_worker_id = ctx
                        .actor_id_to_parallel_unit(upstream_actor_id)?
                        .worker_node_id;
                    let upstream_worker = ctx.worker_nodes.get(&upstream_worker_id).unwrap();

                    // force broadcast upstream actor info
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

        for worker_id in &broadcast_worker_ids {
            let node = ctx.worker_nodes.get(worker_id).unwrap();
            let client = self.client_pool.get(node).await?;

            client
                .to_owned()
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_infos_to_broadcast.values().cloned().collect(),
                })
                .await?;
        }

        for (node_id, stream_actors) in &node_actors_to_create {
            let node = ctx.worker_nodes.get(node_id).unwrap();
            let client = self.client_pool.get(node).await?;
            let request_id = Uuid::new_v4().to_string();
            let request = UpdateActorsRequest {
                request_id,
                actors: stream_actors.clone(),
                hanging_channels: node_hanging_channels.remove(node_id).unwrap_or_default(),
            };

            client.to_owned().update_actors(request).await?;
        }

        for (node_id, hanging_channels) in node_hanging_channels {
            let node = ctx.worker_nodes.get(&node_id).unwrap();

            let client = self.client_pool.get(node).await?;
            let request_id = Uuid::new_v4().to_string();

            client
                .to_owned()
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: vec![],
                    hanging_channels,
                })
                .await?;
        }

        for (node_id, stream_actors) in node_actors_to_create {
            let node = ctx.worker_nodes.get(&node_id).unwrap();
            let client = self.client_pool.get(node).await?;
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

        // Index for fragment -> { parallel_unit -> actor } after reschedule
        let mut fragment_actors_after_reschedule = HashMap::with_capacity(reschedule.len());
        for fragment_id in reschedule.keys() {
            let fragment = ctx
                .fragment_map
                .get(fragment_id)
                .context("could not find Fragment")?;
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

            fragment_actors_after_reschedule.insert(*fragment_id, new_actor_ids);
        }
        let fragment_actors_after_reschedule = fragment_actors_after_reschedule;

        // Generate fragment reschedule plan
        let mut reschedule_fragment: HashMap<FragmentId, Reschedule> =
            HashMap::with_capacity(reschedule.len());
        for (
            fragment_id,
            ParallelUnitReschedule {
                added_parallel_units,
                removed_parallel_units,
            },
        ) in reschedule
        {
            assert_eq!(added_parallel_units.len(), removed_parallel_units.len());

            let replace_parallel_unit_map: BTreeMap<_, _> = removed_parallel_units
                .clone()
                .into_iter()
                .zip_eq(added_parallel_units.clone().into_iter())
                .collect();

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

            let parallel_unit_to_actor_after_reschedule: BTreeMap<_, _> =
                fragment_actors_after_reschedule
                    .get(&fragment_id)
                    .unwrap()
                    .iter()
                    .map(|(actor_id, parallel_unit_id)| {
                        (*parallel_unit_id as ParallelUnitId, *actor_id as ActorId)
                    })
                    .collect();

            let fragment = ctx.fragment_map.get(&fragment_id).unwrap();

            assert!(
                !parallel_unit_to_actor_after_reschedule.is_empty(),
                "hash dispatcher should have at least one downstream actor"
            );

            let upstream_dispatcher_mapping = match fragment.distribution_type() {
                FragmentDistributionType::Hash => {
                    if parallel_unit_to_actor_after_reschedule.len() == 1 {
                        Some(ActorMapping {
                            original_indices: vec![VIRTUAL_NODE_COUNT as u64 - 1],
                            data: vec![
                                *parallel_unit_to_actor_after_reschedule
                                    .first_key_value()
                                    .unwrap()
                                    .1,
                            ],
                        })
                    } else {
                        let downstream_vnode_mapping = fragment.vnode_mapping.clone().unwrap();
                        let ParallelUnitMapping {
                            original_indices,
                            data,
                            ..
                        } = downstream_vnode_mapping;

                        let data = data
                            .iter()
                            .map(|parallel_unit_id| {
                                if let Some(new_parallel_unit_id) =
                                    replace_parallel_unit_map.get(parallel_unit_id)
                                {
                                    parallel_unit_to_actor_after_reschedule[new_parallel_unit_id]
                                } else {
                                    parallel_unit_to_actor_after_reschedule[parallel_unit_id]
                                }
                            })
                            .collect_vec();

                        Some(ActorMapping {
                            original_indices: original_indices.clone(),
                            data,
                        })
                    }
                }
                FragmentDistributionType::Single => None,
                FragmentDistributionType::Unspecified => unreachable!(),
            };

            let mut upstream_fragment_dispatcher_set = BTreeSet::new();

            for actor in &fragment.actors {
                if let Some(upstream_actor_tuples) = ctx.upstream_dispatchers.get(&actor.actor_id) {
                    for (upstream_fragment_id, _, upstream_dispatcher_id) in upstream_actor_tuples {
                        upstream_fragment_dispatcher_set
                            .insert((*upstream_fragment_id, *upstream_dispatcher_id));
                    }
                }
            }

            let downstream_fragment_id = if let Some(downstream_fragment_ids) =
                ctx.downstream_fragment_id_map.get(&fragment_id)
            {
                let downstream_fragment_id = downstream_fragment_ids.iter().exactly_one().unwrap();
                Some(*downstream_fragment_id as FragmentId)
            } else {
                None
            };

            let upstream_fragment_dispatcher_ids =
                upstream_fragment_dispatcher_set.into_iter().collect_vec();
            reschedule_fragment.insert(
                fragment_id,
                Reschedule {
                    added_actors: actors_to_create,
                    removed_actors: actors_to_remove,
                    added_parallel_units,
                    removed_parallel_units,
                    vnode_bitmap_updates: Default::default(),
                    upstream_fragment_dispatcher_ids,
                    upstream_dispatcher_mapping,
                    downstream_fragment_id,
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

        self.fragment_manager
            .pre_apply_reschedules(fragment_created_actors)
            .await?;

        self.barrier_manager
            .run_multiple_commands(vec![
                Command::Plain(Some(Mutation::Pause(PauseMutation {}))),
                Command::RescheduleFragment(reschedule_fragment),
                Command::Plain(Some(Mutation::Resume(ResumeMutation {}))),
            ])
            .await
    }

    // Modifies the upstream and downstream actors of the new created actor according to the overall
    // changes, and is used to handle cascading updates
    fn modify_actor_upstream_and_downstream(
        fragment_map: &HashMap<FragmentId, Fragment>,
        actor_map: &HashMap<ActorId, StreamActor>,
        fragment_actors_to_remove: &HashMap<FragmentId, BTreeMap<ActorId, ParallelUnitId>>,
        fragment_actors_to_create: &HashMap<FragmentId, BTreeMap<ActorId, ParallelUnitId>>,
        new_actor: &mut StreamActor,
    ) -> MetaResult<()> {
        let upstream_fragment_ids: HashSet<_> = new_actor
            .upstream_actor_id
            .iter()
            .filter_map(|actor_id| actor_map.get(actor_id).map(|actor| actor.fragment_id))
            .collect();

        for upstream_fragment_id in &upstream_fragment_ids {
            let upstream_fragment = fragment_map.get(upstream_fragment_id).unwrap();

            match upstream_fragment.distribution_type() {
                FragmentDistributionType::Hash => {}
                FragmentDistributionType::Single => {
                    let upstream_actors_to_create =
                        fragment_actors_to_create.get(upstream_fragment_id);
                    let upstream_actors_to_remove =
                        fragment_actors_to_remove.get(upstream_fragment_id);

                    match (upstream_actors_to_create, upstream_actors_to_remove) {
                        (Some(create), Some(remove)) if create.len() == remove.len() => {}
                        (None, None) => {}
                        _ => bail!("single distribution only support migration"),
                    }
                }
                _ => unreachable!(),
            }
        }

        let mut upstream_actors_to_remove = HashSet::new();
        for upstream_fragment_id in &upstream_fragment_ids {
            if let Some(actors_to_remove) = fragment_actors_to_remove.get(upstream_fragment_id) {
                upstream_actors_to_remove.extend(actors_to_remove.keys().cloned());
            }
        }

        // update upstream actor ids
        new_actor
            .upstream_actor_id
            .drain_filter(|actor_id| upstream_actors_to_remove.contains(actor_id));

        for upstream_fragment_id in &upstream_fragment_ids {
            if let Some(upstream_actors_to_create) =
                fragment_actors_to_create.get(upstream_fragment_id)
            {
                new_actor
                    .upstream_actor_id
                    .extend(upstream_actors_to_create.keys().cloned())
            }
        }

        fn replace_merge_node_upstream(
            stream_node: &mut StreamNode,
            fragment_actors_to_remove: &HashMap<FragmentId, BTreeMap<ActorId, ParallelUnitId>>,
            fragment_actors_to_create: &HashMap<FragmentId, BTreeMap<ActorId, ParallelUnitId>>,
        ) {
            if let Some(NodeBody::Merge(s)) = stream_node.node_body.as_mut() {
                if let Some(upstream_actors_to_remove) =
                    fragment_actors_to_remove.get(&s.upstream_fragment_id)
                {
                    s.upstream_actor_id
                        .drain_filter(|actor_id| upstream_actors_to_remove.contains_key(actor_id));
                }

                if let Some(upstream_actors_to_create) =
                    fragment_actors_to_create.get(&s.upstream_fragment_id)
                {
                    s.upstream_actor_id
                        .extend(upstream_actors_to_create.keys().cloned());
                }
            }

            for child in &mut stream_node.input {
                replace_merge_node_upstream(
                    child,
                    fragment_actors_to_remove,
                    fragment_actors_to_create,
                );
            }
        }

        if let Some(node) = new_actor.nodes.as_mut() {
            replace_merge_node_upstream(node, fragment_actors_to_remove, fragment_actors_to_create);
        }

        // update downstream actor ids
        for dispatcher in &mut new_actor.dispatcher {
            let downstream_fragment_id = dispatcher
                .downstream_actor_id
                .iter()
                .filter_map(|actor_id| actor_map.get(actor_id).map(|actor| actor.fragment_id))
                .dedup()
                .exactly_one()
                .unwrap() as FragmentId;

            let downstream_fragment_actors_to_remove =
                fragment_actors_to_remove.get(&downstream_fragment_id);
            let downstream_fragment_actors_to_create =
                fragment_actors_to_create.get(&downstream_fragment_id);

            match dispatcher.r#type() {
                DispatcherType::Hash => {
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
                }
                _ => unimplemented!(),
            }

            if let Some(mapping) = dispatcher.hash_mapping.as_mut() {
                // todo we only support migration
                match (
                    downstream_fragment_actors_to_remove,
                    downstream_fragment_actors_to_create,
                ) {
                    (Some(to_remove), Some(to_create)) if to_remove.len() == to_create.len() => {
                        let map: HashMap<_, _> = to_remove
                            .keys()
                            .cloned()
                            .zip_eq(to_create.keys().cloned())
                            .collect();

                        for actor_id in &mut mapping.data {
                            if let Some(new_actor_id) = map.get(actor_id) {
                                *actor_id = *new_actor_id;
                            }
                        }
                    }
                    (None, None) => {
                        // do nothing
                    }
                    _ => unimplemented!(),
                }
            }
        }

        Ok(())
    }
}
