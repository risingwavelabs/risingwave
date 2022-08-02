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

use risingwave_pb::stream_plan::update_mutation::DispatcherUpdate;
use itertools::Itertools;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::collections::BTreeSet;
use uuid::Uuid;
use async_recursion::async_recursion;
use risingwave_common::bail;
use risingwave_common::catalog::TableId;
use crate::storage::MetaStore;
use crate::stream::GlobalStreamManager;
use risingwave_common::error::Result;
use risingwave_common::types::{ParallelUnitId, VIRTUAL_NODE_COUNT};
use risingwave_pb::common::{ActorInfo, WorkerNode, WorkerType};
use risingwave_pb::stream_plan::{DispatcherType, StreamActor, UpdateMutation};
use risingwave_pb::stream_plan::barrier::Mutation;
use risingwave_pb::stream_plan::update_mutation::MergeUpdate;
use risingwave_pb::stream_service::{BroadcastActorInfoTableRequest, BuildActorsRequest, HangingChannel, UpdateActorsRequest};
use crate::barrier::Command;
use crate::cluster::WorkerId;
use crate::manager::IdCategory;
use crate::model::{ActorId, DispatcherId, FragmentId, TableFragments};

pub struct Reschedule {
    pub added_parallel_units: Vec<ParallelUnitId>,
    pub removed_parallel_units: Vec<ParallelUnitId>,
}

impl<S> GlobalStreamManager<S> where S: MetaStore {
    pub async fn reschedule_actors(&self, reschedule: HashMap<FragmentId, Reschedule>) -> Result<()> {

        // first, we collect necessary infos
        let table_ids = self.fragment_manager.find_table_ids_for_fragment_ids(reschedule.keys().cloned().collect()).await.values().cloned().collect();
        let mut table_fragments_map = HashMap::new();
        self.resolv_dependent_table_fragments(table_ids, &mut table_fragments_map, &mut HashSet::new()).await?;

        let mut chain_actor_ids = HashSet::new();
        let mut actor_map = HashMap::new();
        let mut actor_status = BTreeMap::new();
        let mut parallel_unit_id_to_actor_id = HashMap::new();
        for (_, table_fragments) in &table_fragments_map {
            chain_actor_ids.extend(table_fragments.chain_actor_ids());
            let table_actor_status = table_fragments.actor_status.clone();
            for (fragment_id, fragment) in &table_fragments.fragments {
                for actor in &fragment.actors {
                    actor_map.insert(actor.actor_id, actor.clone());

                    if let Some(parallel_unit) = table_actor_status.get(&actor.actor_id).and_then(|status| status.parallel_unit.as_ref()) {
                        parallel_unit_id_to_actor_id.insert((*fragment_id, parallel_unit.id as ParallelUnitId), actor.actor_id);
                    }
                }
            }
            actor_status.extend(table_actor_status);
        }


        // then, we collect all available upstream and downstream
        let mut downstream_actors = HashMap::new();
        let mut upstream_actors = HashMap::new();
        for (actor_id, stream_actor) in &actor_map {
            for dispatcher in &stream_actor.dispatcher {
                for downstream_actor_id in &dispatcher.downstream_actor_id {
                    downstream_actors
                        .entry(*actor_id as ActorId)
                        .or_insert(vec![])
                        .push(*downstream_actor_id as ActorId);
                    upstream_actors
                        .entry(*downstream_actor_id as ActorId)
                        .or_insert(vec![])
                        .push((*actor_id as ActorId, dispatcher.dispatcher_id as DispatcherId));
                }
            }
        }

        //
        // // if add.len == removed.len, enter migration logic
        // let mut actor_migration_plan = HashMap::new();

        let mut to_remove = HashMap::new();
        let mut to_add = HashMap::new();

        for (fragment_id, Reschedule { added_parallel_units, removed_parallel_units }) in &reschedule {
            if removed_parallel_units.len() != added_parallel_units.len() {
                bail!("only migration is supported now");
            }

            for (removed_parallel_unit_id, added_parallel_unit_id) in removed_parallel_units.iter().zip_eq(added_parallel_units) {
                let fragment_parallel_unit_id = (*fragment_id, *removed_parallel_unit_id);
                // if let Some(actor_id) = parallel_unit_id_to_actor_id.get(&fragment_parallel_unit_id) {
                //     actor_migration_plan.insert(*actor_id as ActorId, (removed_parallel_unit_id, added_parallel_unit_id));
                // } else {
                //     bail!("could not find actor id for parallel_unit")
                // }
            }
        }



        // generate new actor ids
        let mut recreated_actor_ids = HashMap::new();
        for (actor_id, _) in &actor_migration_plan {
            let id = self
                .id_gen_manager
                .generate::<{ IdCategory::Actor }>()
                .await? as ActorId;

            recreated_actor_ids.insert(*actor_id, id);
        }

        // dump actor info, and modify actor upstream/downstream
        let mut recreated_actors = HashMap::new();
        for (actor_id, _) in &actor_migration_plan {
            let new_actor_id = recreated_actor_ids.get(actor_id).unwrap();

            let old_actor = actor_map.get(actor_id).unwrap();
            let mut new_actor = old_actor.clone();

            if chain_actor_ids.contains(actor_id) {
                todo!()
                // let upstream_actor_ids = upstream_actors.get(actor_id).unwrap();
                // assert_eq!(upstream_actor_ids.len(), 1);
                // let (upstream_actor_id, _) = upstream_actor_ids.iter().next().unwrap();
                // if actor_ids.contains(upstream_actor_id) {
                //     new_actor.same_worker_node_as_upstream = false;
                // }
            }

            for upstream_actor_id in &mut new_actor.upstream_actor_id {
                if let Some(new_actor_id) = recreated_actor_ids.get(upstream_actor_id) {
                    *upstream_actor_id = *new_actor_id as u32;
                }
            }

            for dispatcher in &mut new_actor.dispatcher {
                for downstream_actor_id in &mut dispatcher.downstream_actor_id {
                    if let Some(new_actor_id) = recreated_actor_ids.get(downstream_actor_id) {
                        *downstream_actor_id = *new_actor_id as u32;
                    }
                }
            }

            new_actor.actor_id = *new_actor_id;
            recreated_actors.insert(*actor_id as ActorId, new_actor);
        }

        let mut node_hanging_channels: HashMap<WorkerId, Vec<HangingChannel>> = HashMap::new();

        for (actor_id, &(from, to)) in &actor_migration_plan {
            // let worker_id = actor_id_to_target_id.get(actor_id).unwrap();
            // let worker = worker_nodes.get(worker_id).unwrap();
            // if let Some(upstream_actor_ids) = upstream_actors.get(actor_id) {
            //     for (upstream_actor_id, _upstream_dispatcher_id) in upstream_actor_ids {
            //         if actor_ids.contains(upstream_actor_id) {
            //             continue;
            //         }
            //
            //         let new_actor_id = recreated_actor_ids.get(actor_id).unwrap();
            //
            //         let upstream_worker_id = actor_id_to_worker_id.get(upstream_actor_id).unwrap();
            //
            //         // note: Before PR #4045, we need to remove the local-to-local hanging_channels
            //         // if worker_id == upstream_worker_id {
            //         //     continue;
            //         // }
            //
            //         node_hanging_channels
            //             .entry(*upstream_worker_id)
            //             .or_default()
            //             .push(HangingChannel {
            //                 upstream: Some(ActorInfo {
            //                     actor_id: *upstream_actor_id,
            //                     host: None,
            //                 }),
            //                 downstream: Some(ActorInfo {
            //                     actor_id: *new_actor_id,
            //                     host: worker.host.clone(),
            //                 }),
            //             })
            //     }
            // }
        }



        todo!()
    }

    #[async_recursion]
    async fn resolv_dependent_table_fragments(&self, table_ids: HashSet<TableId>, table_fragments_map: &mut HashMap<TableId, TableFragments>, cache: &mut HashSet<TableId>) -> Result<()> {
        for table_id in table_ids {
            if cache.contains(&table_id) {
                continue;
            }

            let fragments = self
                .fragment_manager
                .select_table_fragments_by_table_id(&table_id)
                .await?;

            if !fragments.chain_actor_ids().is_empty() {
                let dependent_table_ids = fragments.dependent_table_ids();
                self.resolv_dependent_table_fragments(
                    dependent_table_ids,
                    table_fragments_map,
                    cache,
                )
                    .await?;
            }

            table_fragments_map.insert(table_id, fragments);
            cache.insert(table_id);
        }

        Ok(())
    }


    #[allow(clippy::too_many_arguments)]
    #[async_recursion]
    async fn resolve_migrate_dependent_actors2(
        &self,
        table_ids: HashSet<TableId>,
        actor_map: &mut HashMap<ActorId, StreamActor>,
        actor_id_to_worker_id: &mut HashMap<ActorId, WorkerId>,
        chain_actor_ids: &mut HashSet<ActorId>,
        table_fragments: &mut HashMap<TableId, TableFragments>,
        cache: &mut HashSet<TableId>,
    ) -> Result<()> {
        for table_id in table_ids {
            if cache.contains(&table_id) {
                continue;
            }

            let fragments = self
                .fragment_manager
                .select_table_fragments_by_table_id(&table_id)
                .await?;
            actor_id_to_worker_id.extend(fragments.actor_to_node());

            let table_actor_map = fragments.actor_map();

            for (actor_id, actor) in table_actor_map {
                actor_map.insert(actor_id, actor.clone());
            }

            let table_chain_actor_ids = fragments.chain_actor_ids();
            if !table_chain_actor_ids.is_empty() {
                chain_actor_ids.extend(table_chain_actor_ids);
                let dependent_table_ids = fragments.dependent_table_ids();
                self.resolve_migrate_dependent_actors(
                    dependent_table_ids,
                    actor_map,
                    actor_id_to_worker_id,
                    chain_actor_ids,
                    table_fragments,
                    cache,
                )
                    .await?;
            }

            table_fragments.insert(table_id, fragments);
            cache.insert(table_id);
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[async_recursion]
    async fn resolve_migrate_dependent_actors(
        &self,
        table_ids: HashSet<TableId>,
        actor_map: &mut HashMap<ActorId, StreamActor>,
        actor_id_to_worker_id: &mut HashMap<ActorId, WorkerId>,
        chain_actor_ids: &mut HashSet<ActorId>,
        table_fragments: &mut HashMap<TableId, TableFragments>,
        cache: &mut HashSet<TableId>,
    ) -> Result<()> {
        for table_id in table_ids {
            if cache.contains(&table_id) {
                continue;
            }

            let fragments = self
                .fragment_manager
                .select_table_fragments_by_table_id(&table_id)
                .await?;
            actor_id_to_worker_id.extend(fragments.actor_to_node());

            let table_actor_map = fragments.actor_map();

            for (actor_id, actor) in table_actor_map {
                actor_map.insert(actor_id, actor.clone());
            }

            let table_chain_actor_ids = fragments.chain_actor_ids();
            if !table_chain_actor_ids.is_empty() {
                chain_actor_ids.extend(table_chain_actor_ids);
                let dependent_table_ids = fragments.dependent_table_ids();
                self.resolve_migrate_dependent_actors(
                    dependent_table_ids,
                    actor_map,
                    actor_id_to_worker_id,
                    chain_actor_ids,
                    table_fragments,
                    cache,
                )
                    .await?;
            }

            table_fragments.insert(table_id, fragments);
            cache.insert(table_id);
        }

        Ok(())
    }

    pub async fn migrate_actors(
        &self,
        actors: HashMap<TableId, HashMap<ActorId, WorkerId>>,
    ) -> Result<HashMap<ActorId, ActorId>> {
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

        let mut chain_actor_ids = HashSet::new();
        let mut actor_id_to_worker_id = HashMap::new();
        let mut actor_map = HashMap::new();
        let mut table_fragments = HashMap::new();
        let mut _cache = HashSet::new();
        self.resolve_migrate_dependent_actors(
            actors.keys().cloned().collect(),
            &mut actor_map,
            &mut actor_id_to_worker_id,
            &mut chain_actor_ids,
            &mut table_fragments,
            &mut _cache,
        )
            .await?;

        let mut actor_id_to_target_id = HashMap::new();
        for map in actors.values() {
            for actor_id in map.keys() {
                if !actor_map.contains_key(actor_id) {
                    bail!("actor {} not found", actor_id);
                }
            }

            for (&actor_id, &worker_id) in map {
                if !worker_nodes.contains_key(&worker_id) {
                    bail!("worker {} not found", worker_id);
                }

                actor_id_to_target_id.insert(actor_id, worker_id);
            }
        }

        let mut downstream_actors = HashMap::new();
        let mut upstream_actors = HashMap::new();
        for (actor_id, stream_actor) in &actor_map {
            for dispatcher in &stream_actor.dispatcher {
                for downstream_actor_id in &dispatcher.downstream_actor_id {
                    downstream_actors
                        .entry(*actor_id as ActorId)
                        .or_insert(vec![])
                        .push(*downstream_actor_id as ActorId);
                    upstream_actors
                        .entry(*downstream_actor_id as ActorId)
                        .or_insert(vec![])
                        .push((*actor_id, dispatcher.dispatcher_id));
                }
            }
        }

        let actor_ids: BTreeSet<ActorId> = actors
            .values()
            .flat_map(|value| value.keys().into_iter().cloned())
            .collect();

        let mut recreated_actor_ids = HashMap::new();
        let mut recreated_actors = HashMap::new();

        for actor_id in &actor_ids {
            let id = self
                .id_gen_manager
                .generate::<{ IdCategory::Actor }>()
                .await? as ActorId;
            recreated_actor_ids.insert(*actor_id, id);

            let old_actor = actor_map.get(actor_id).unwrap();
            let mut new_actor = old_actor.clone();

            if chain_actor_ids.contains(actor_id) {
                let upstream_actor_ids = upstream_actors.get(actor_id).unwrap();
                assert_eq!(upstream_actor_ids.len(), 1);
                let (upstream_actor_id, _) = upstream_actor_ids.iter().next().unwrap();
                if actor_ids.contains(upstream_actor_id) {
                    new_actor.same_worker_node_as_upstream = false;
                }
            }

            for upstream_actor_id in &mut new_actor.upstream_actor_id {
                if let Some(new_actor_id) = recreated_actor_ids.get(upstream_actor_id) {
                    *upstream_actor_id = *new_actor_id as u32;
                }
            }

            for dispatcher in &mut new_actor.dispatcher {
                for downstream_actor_id in &mut dispatcher.downstream_actor_id {
                    if let Some(new_actor_id) = recreated_actor_ids.get(downstream_actor_id) {
                        *downstream_actor_id = *new_actor_id as u32;
                    }
                }
            }

            new_actor.actor_id = id;
            recreated_actors.insert(*actor_id as ActorId, new_actor);
        }

        let mut node_hanging_channels: HashMap<WorkerId, Vec<HangingChannel>> = HashMap::new();

        for actor_id in &actor_ids {
            let worker_id = actor_id_to_target_id.get(actor_id).unwrap();
            let worker = worker_nodes.get(worker_id).unwrap();
            if let Some(upstream_actor_ids) = upstream_actors.get(actor_id) {
                for (upstream_actor_id, _upstream_dispatcher_id) in upstream_actor_ids {
                    if actor_ids.contains(upstream_actor_id) {
                        continue;
                    }

                    let new_actor_id = recreated_actor_ids.get(actor_id).unwrap();

                    let upstream_worker_id = actor_id_to_worker_id.get(upstream_actor_id).unwrap();

                    // note: Before PR #4045, we need to remove the local-to-local hanging_channels
                    // if worker_id == upstream_worker_id {
                    //     continue;
                    // }

                    node_hanging_channels
                        .entry(*upstream_worker_id)
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
                        })
                }
            }
        }

        let mut actor_infos_to_broadcast = vec![];
        let mut node_actors: HashMap<WorkerId, Vec<_>> = HashMap::new();
        for (actor_id, &worker_id) in &actor_id_to_target_id {
            let new_actor = recreated_actors.get(actor_id).unwrap();
            node_actors
                .entry(worker_id)
                .or_default()
                .push(new_actor.clone());

            let worker = worker_nodes.get(&worker_id).unwrap();
            actor_infos_to_broadcast.push(ActorInfo {
                actor_id: new_actor.actor_id,
                host: worker.host.clone(),
            })
        }

        let mut broadcast_node_ids = HashSet::new();
        for actor_id in &actor_ids {
            if let Some(upstream_actor_ids) = upstream_actors.get(actor_id) {
                for (upstream_actor_id, _) in upstream_actor_ids {
                    let node_id = actor_id_to_worker_id.get(upstream_actor_id).unwrap();
                    broadcast_node_ids.insert(node_id);
                }
            }

            if let Some(downstream_actor_ids) = downstream_actors.get(actor_id) {
                for downstream_actor_id in downstream_actor_ids {
                    let node_id = actor_id_to_worker_id.get(downstream_actor_id).unwrap();
                    broadcast_node_ids.insert(node_id);
                }
            }
        }

        for node_id in broadcast_node_ids {
            let node = worker_nodes.get(node_id).unwrap();
            let client = self.client_pool.get(node).await?;

            client
                .to_owned()
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_infos_to_broadcast.clone(),
                })
                .await?;
        }

        for (node_id, stream_actors) in &node_actors {
            let node = worker_nodes.get(node_id).unwrap();
            let client = self.client_pool.get(node).await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "update actors");
            let request = UpdateActorsRequest {
                request_id,
                actors: stream_actors.clone(),
                hanging_channels: node_hanging_channels.remove(node_id).unwrap_or_default(),
            };

            client.to_owned().update_actors(request).await?;
        }

        // Build remaining hanging channels on compute nodes.
        for (node_id, hanging_channels) in node_hanging_channels {
            let node = worker_nodes.get(&node_id).unwrap();

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

        // In the second stage, each [`WorkerNode`] builds local actors and connect them with
        // channels.
        for (node_id, stream_actors) in node_actors {
            let node = worker_nodes.get(&node_id).unwrap();

            let client = self.client_pool.get(node).await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "build actors");
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

        let mut actor_dispatcher_update = HashMap::new();
        let mut actor_merge_update = HashMap::new();
        for actor_id in &actor_ids {
            if let Some(upstream_actor_ids) = upstream_actors.get(actor_id) {
                for (upstream_actor_id, upstream_dispatcher_id) in upstream_actor_ids {
                    if actor_ids.contains(upstream_actor_id) {
                        continue;
                    }

                    let new_actor_id = recreated_actor_ids.get(actor_id).unwrap();

                    let upstream_actor = actor_map.get(upstream_actor_id).unwrap();

                    let dispatcher = upstream_actor
                        .dispatcher
                        .iter()
                        .find(|&dispatcher| dispatcher.dispatcher_id == *upstream_dispatcher_id)
                        .unwrap();

                    let mut new_hash_mapping = dispatcher.hash_mapping.clone();

                    if dispatcher.get_type().unwrap() == DispatcherType::Hash {
                        if let Some(actor_mapping) = new_hash_mapping.as_mut() {
                            for mapping_actor_id in &mut actor_mapping.data {
                                if let Some(new_actor_id) =
                                recreated_actor_ids.get(mapping_actor_id)
                                {
                                    *mapping_actor_id = *new_actor_id;
                                }
                            }
                        }
                    }

                    let dispatcher_update = actor_dispatcher_update
                        .entry(*upstream_actor_id)
                        .or_insert(DispatcherUpdate {
                            dispatcher_id: dispatcher.dispatcher_id,
                            hash_mapping: new_hash_mapping,
                            added_downstream_actor_id: vec![],
                            removed_downstream_actor_id: vec![],
                        });

                    dispatcher_update
                        .added_downstream_actor_id
                        .push(*new_actor_id);
                    dispatcher_update
                        .removed_downstream_actor_id
                        .push(*actor_id);
                }
            }

            if let Some(downstream_actor_ids) = downstream_actors.get(actor_id) {
                for downstream_actor_id in downstream_actor_ids {
                    if actor_ids.contains(downstream_actor_id) {
                        continue;
                    }

                    let new_actor_id = recreated_actor_ids.get(actor_id).unwrap();

                    let merger_update =
                        actor_merge_update
                            .entry(*downstream_actor_id)
                            .or_insert(MergeUpdate {
                                added_upstream_actor_id: vec![],
                                removed_upstream_actor_id: vec![],
                            });

                    merger_update.added_upstream_actor_id.push(*new_actor_id);
                    merger_update.removed_upstream_actor_id.push(*actor_id);
                }
            }
        }

        self.barrier_manager
            .run_command(Command::Plain(Some(Mutation::Update(UpdateMutation {
                actor_dispatcher_update,
                actor_merge_update,
                dropped_actors: actor_ids.iter().cloned().collect(),
            }))))
            .await?;

        let table_fragments = table_fragments.into_values().collect_vec();

        let (new_fragments, migrate_map) = self
            .fragment_manager
            .recreate_actors(
                &actor_id_to_target_id,
                &recreated_actor_ids,
                &recreated_actors,
                &worker_nodes,
                table_fragments,
            )
            .await?;

        self.barrier_manager
            .catalog_manager
            .update_table_mapping(&new_fragments, &migrate_map)
            .await?;

        // update hash mapping
        for fragments in new_fragments {
            for (fragment_id, fragment) in fragments.fragments {
                let mapping = fragment.vnode_mapping.as_ref().unwrap();
                let vnode_mapping = risingwave_common::util::compress::decompress_data(
                    &mapping.original_indices,
                    &mapping.data,
                );
                assert_eq!(vnode_mapping.len(), VIRTUAL_NODE_COUNT);
                self.barrier_manager
                    .env
                    .hash_mapping_manager()
                    .set_fragment_hash_mapping(fragment_id, vnode_mapping);
            }
        }

        Ok(recreated_actor_ids)
    }
}