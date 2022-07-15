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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::stream::CreateMaterializedViewContext;
use futures::future::try_join_all;
use risingwave_pb::common::worker_node::State::Running;
use itertools::Itertools;
use tokio::sync::Mutex;
use risingwave_common::bail;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_common::error::RwError;
use risingwave_common::types::{ParallelUnitId, VIRTUAL_NODE_COUNT};
use risingwave_pb::catalog::{Sink};
use risingwave_pb::common::{ActorInfo, ParallelUnitMapping, WorkerType};
use risingwave_pb::meta::table_fragments::{ActorState, ActorStatus};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{ActorMapping, Dispatcher, DispatcherType, StreamNode};
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, HangingChannel, UpdateActorsRequest,
};
use risingwave_rpc_client::StreamClientPoolRef;
use uuid::Uuid;
use risingwave_pb::stream_service::{
    CreateSinkRequest as ComputeNodeCreateSinkRequest,
    DropSinkRequest as ComputeNodeDropSinkRequest,
};

use risingwave_rpc_client::StreamClient;
use super::ScheduledLocations;
use crate::barrier::{BarrierManagerRef, Command};
use crate::cluster::{ClusterManagerRef, WorkerId};
use crate::manager::{MetaSrvEnv, SinkId};
use crate::model::{ActorId, TableFragments};
use crate::storage::MetaStore;
use crate::stream::{fetch_source_fragments, FragmentManagerRef, Scheduler};

pub type SinkManagerRef<S> = Arc<SinkManager<S>>;

#[allow(dead_code)]
pub struct SinkManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    cluster_manager: ClusterManagerRef<S>,
    /// Manages definition and status of fragments and actors
    fragment_manager: FragmentManagerRef<S>,

    /// Broadcasts and collect barriers
    barrier_manager: BarrierManagerRef<S>,

    /// Schedules streaming actors into compute nodes
    scheduler: Scheduler<S>,

    /// Client Pool to stream service on compute nodes
    client_pool: StreamClientPoolRef,

    core: Arc<Mutex<SinkManagerCore<S>>>,
}

pub struct SinkManagerCore<S: MetaStore> {
    pub fragment_manager: FragmentManagerRef<S>,
    pub managed_sinks: HashMap<SinkId, String>,
}

impl<S> SinkManagerCore<S>
    where
        S: MetaStore,
{
    fn new(
        fragment_manager: FragmentManagerRef<S>,
        managed_sinks: HashMap<SinkId, String>,
    ) -> Self {
        Self {
            fragment_manager,
            managed_sinks,
        }
    }
}

impl<S> SinkManager<S>
    where
        S: MetaStore,
{
    pub async fn new(
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        barrier_manager: BarrierManagerRef<S>,
    ) -> Result<Self> {
        let managed_sinks = HashMap::new();
        let core = Arc::new(Mutex::new(SinkManagerCore::new(
            fragment_manager.clone(),
            managed_sinks,
        )));

        Ok(Self {
            env: env.clone(),
            cluster_manager: cluster_manager.clone(),
            fragment_manager,
            barrier_manager,
            scheduler: Scheduler::new(cluster_manager, env.hash_mapping_manager_ref()),
            client_pool: env.stream_client_pool_ref(),
            core,
        })
    }

    async fn resolve_chain_node(
        &self,
        table_fragments: &mut TableFragments,
        dependent_table_ids: &HashSet<TableId>,
        dispatchers: &mut HashMap<ActorId, Vec<Dispatcher>>,
        upstream_node_actors: &mut HashMap<WorkerId, HashSet<ActorId>>,
        locations: &ScheduledLocations,
    ) -> Result<()> {
        // The closure environment. Used to simulate recursive closure.
        struct Env<'a> {
            /// Records what's the correspoding actor of each parallel unit of one table.
            upstream_parallel_unit_info: &'a HashMap<TableId, BTreeMap<ParallelUnitId, ActorId>>,
            /// Records what's the actors on each worker of one table.
            tables_node_actors: &'a HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>>,
            /// Schedule information of all actors.
            locations: &'a ScheduledLocations,
            /// New dispatchers for this mview.
            dispatchers: &'a mut HashMap<ActorId, Vec<Dispatcher>>,
            /// Upstream Materialize actor ids grouped by worker id.
            upstream_node_actors: &'a mut HashMap<WorkerId, HashSet<ActorId>>,
        }

        impl Env<'_> {
            fn resolve_chain_node_inner(
                &mut self,
                stream_node: &mut StreamNode,
                actor_id: ActorId,
                same_worker_node_as_upstream: bool,
            ) -> Result<()> {
                let Some(NodeBody::Chain(ref mut chain)) = stream_node.node_body else {
                    // If node is not chain node, recursively deal with input nodes
                    for input in &mut stream_node.input {
                        self.resolve_chain_node_inner(input, actor_id, same_worker_node_as_upstream)?;
                    }
                    return Ok(());
                };
                // If node is chain node, we insert upstream ids into chain's input (merge)

                // get upstream table id
                let table_id = TableId::new(chain.table_id);

                let upstream_actor_id = {
                    // 1. use table id to get upstream parallel_unit -> actor_id mapping
                    let upstream_parallel_actor_mapping =
                        &self.upstream_parallel_unit_info[&table_id];
                    // 2. use our actor id to get parallel unit id of the chain actor
                    let parallel_unit_id = self.locations.actor_locations[&actor_id].id;
                    // 3. and use chain actor's parallel unit id to get the corresponding upstream
                    // actor id
                    upstream_parallel_actor_mapping[&parallel_unit_id]
                };

                // The current implementation already ensures chain and upstream are on the same
                // worker node. So we do a sanity check here, in case that the logic get changed but
                // `same_worker_node` constraint is not satisfied.
                if same_worker_node_as_upstream {
                    // Parallel unit id is a globally unique id across all worker nodes. It can be
                    // seen as something like CPU core id. Therefore, we verify that actor's unit id
                    // == upstream's unit id.

                    let actor_parallel_unit_id =
                        self.locations.actor_locations.get(&actor_id).unwrap().id;

                    assert_eq!(
                        *self
                            .upstream_parallel_unit_info
                            .get(&table_id)
                            .unwrap()
                            .get(&actor_parallel_unit_id)
                            .unwrap(),
                        upstream_actor_id
                    );
                }

                // fill upstream node-actor info for later use
                let upstream_table_node_actors = self.tables_node_actors.get(&table_id).unwrap();

                let chain_upstream_node_actors = upstream_table_node_actors
                    .iter()
                    .flat_map(|(node_id, actor_ids)| {
                        actor_ids.iter().map(|actor_id| (*node_id, *actor_id))
                    })
                    .filter(|(_, actor_id)| upstream_actor_id == *actor_id)
                    .into_group_map();
                for (node_id, actor_ids) in chain_upstream_node_actors {
                    self.upstream_node_actors
                        .entry(node_id)
                        .or_default()
                        .extend(actor_ids.iter());
                }

                // deal with merge and batch query node, setting upstream infos.
                let batch_stream_node = &stream_node.input[1];
                assert!(
                    matches!(batch_stream_node.node_body, Some(NodeBody::BatchPlan(_))),
                    "chain's input[1] should always be batch query"
                );

                let merge_stream_node = &mut stream_node.input[0];
                let Some(NodeBody::Merge(ref mut merge)) = merge_stream_node.node_body else {
                    unreachable!("chain's input[0] should always be merge");
                };
                merge.upstream_actor_id.push(upstream_actor_id);

                // finally, we should also build dispatcher infos here.
                //
                // Note: currently we ensure that the downstream chain operator has the same
                // parallel unit and distribution as the upstream mview, so we can simply use
                // `NoShuffle` dispatcher here.
                // TODO: support different parallel unit and distribution for new MV.
                self.dispatchers
                    .entry(upstream_actor_id)
                    .or_default()
                    .push(Dispatcher {
                        r#type: DispatcherType::NoShuffle as _,
                        // Use chain actor id as dispatcher id to avoid collision in this
                        // Dispatch executor.
                        dispatcher_id: actor_id as _,
                        downstream_actor_id: vec![actor_id],
                        ..Default::default()
                    });

                Ok(())
            }
        }

        let upstream_parallel_unit_info = &self
            .fragment_manager
            .get_sink_parallel_unit_ids(dependent_table_ids)
            .await?;

        let tables_node_actors = &self
            .fragment_manager
            .get_tables_node_actors(dependent_table_ids)
            .await?;

        let mut env = Env {
            upstream_parallel_unit_info,
            tables_node_actors,
            locations,
            dispatchers,
            upstream_node_actors,
        };

        for fragment in table_fragments.fragments.values_mut() {
            for actor in &mut fragment.actors {
                if let Some(ref mut stream_node) = actor.nodes {
                    env.resolve_chain_node_inner(
                        stream_node,
                        actor.actor_id,
                        actor.same_worker_node_as_upstream,
                    )?;
                }
            }
        }
        Ok(())
    }


    async fn all_stream_clients(&self) -> Result<impl Iterator<Item=StreamClient>> {
        let all_compute_nodes = self
            .cluster_manager
            .list_worker_node(WorkerType::ComputeNode, Some(Running))
            .await;

        let all_stream_clients = try_join_all(
            all_compute_nodes
                .iter()
                .map(|worker| self.env.stream_client_pool().get(worker)),
        )
            .await?
            .into_iter();

        Ok(all_stream_clients)
    }

    pub async fn creat_sink_2(&self,
                              mut table_fragments: TableFragments,
                              CreateMaterializedViewContext {
                                  dispatchers,
                                  upstream_node_actors,
                                  table_sink_map,
                                  dependent_table_ids,
                                  table_properties: _,
                                  ..
                              }: &mut CreateMaterializedViewContext, ) -> Result<()> {
        // This scope guard does clean up jobs ASYNCHRONOUSLY before Err returns.
        // It MUST be cleared before Ok returns.
        let mut revert_funcs = scopeguard::guard(
            vec![],
            |revert_funcs: Vec<futures::future::BoxFuture<()>>| {
                tokio::spawn(async move {
                    for revert_func in revert_funcs.into_iter().rev() {
                        revert_func.await;
                    }
                });
            },
        );

        // Schedule actors to parallel units. `locations` will record the parallel unit that an
        // actor is scheduled to, and the worker node this parallel unit is on.
        let locations = {
            // List all running worker nodes.
            let workers = self
                .cluster_manager
                .list_worker_node(
                    WorkerType::ComputeNode,
                    Some(risingwave_pb::common::worker_node::State::Running),
                )
                .await;
            if workers.is_empty() {
                bail!("no available compute node in the cluster");
            }

            // Create empty locations.
            let mut locations = ScheduledLocations::with_workers(workers);

            // Schedule each fragment(actors) to nodes, recorded in `locations`.
            // Vnode mapping in fragment will be filled in as well.
            let topological_order = table_fragments.generate_topological_order();
            for fragment_id in topological_order {
                let fragment = table_fragments.fragments.get_mut(&fragment_id).unwrap();
                self.scheduler.schedule(fragment, &mut locations).await?;
            }

            locations
        };

        // Resolve chain node infos, including:
        // 1. insert upstream actor id in merge node
        // 2. insert parallel unit id in batch query node
        self.resolve_chain_node(
            &mut table_fragments,
            dependent_table_ids,
            dispatchers,
            upstream_node_actors,
            &locations,
        )
            .await?;

        let dispatchers = &*dispatchers;

        // Record vnode to parallel unit mapping for actors.
        let actor_to_vnode_mapping = {
            let mut mapping = HashMap::new();
            for fragment in table_fragments.fragments.values() {
                for actor in &fragment.actors {
                    mapping
                        .try_insert(actor.actor_id, fragment.vnode_mapping.clone())
                        .unwrap();
                }
            }
            mapping
        };

        // Fill hash dispatcher's mapping with scheduled locations.
        for fragment in table_fragments.fragments.values_mut() {
            // Filter out hash dispatchers in this fragment.
            let dispatchers = fragment
                .actors
                .iter_mut()
                .flat_map(|actor| actor.dispatcher.iter_mut())
                .filter(|d| d.get_type().unwrap() == DispatcherType::Hash);

            for dispatcher in dispatchers {
                match dispatcher.downstream_actor_id.as_slice() {
                    [] => panic!("hash dispatcher should have at least one downstream actor"),

                    // There exists some unoptimized situation where a hash dispatcher has ONLY ONE
                    // downstream actor, which makes it behave like a simple dispatcher. As a
                    // workaround, we specially compute the consistent hash mapping here.
                    // This arm could be removed after the optimizer has been fully implemented.
                    &[single_downstream_actor] => {
                        dispatcher.hash_mapping = Some(ActorMapping {
                            original_indices: vec![VIRTUAL_NODE_COUNT as u64 - 1],
                            data: vec![single_downstream_actor],
                        });
                    }

                    // For normal cases, we can simply transform the mapping from downstream actors
                    // to current hash dispatchers.
                    downstream_actors @ &[first_downstream_actor, ..] => {
                        // All actors in the downstream fragment should have the same parallel unit
                        // mapping, find it with the first downstream actor.
                        let downstream_vnode_mapping = actor_to_vnode_mapping
                            .get(&first_downstream_actor)
                            .unwrap()
                            .as_ref()
                            .unwrap_or_else(|| {
                                panic!("no vnode mapping for actor {}", &first_downstream_actor);
                            });

                        // Mapping from the parallel unit to downstream actors.
                        let parallel_unit_actor_map = downstream_actors
                            .iter()
                            .map(|actor_id| {
                                (
                                    locations.actor_locations.get(actor_id).unwrap().id,
                                    *actor_id,
                                )
                            })
                            .collect::<HashMap<_, _>>();

                        // Trasform the mapping of parallel unit to the mapping of actor.
                        let ParallelUnitMapping {
                            original_indices,
                            data,
                            ..
                        } = downstream_vnode_mapping;
                        let data = data
                            .iter()
                            .map(|parallel_unit_id| parallel_unit_actor_map[parallel_unit_id])
                            .collect_vec();
                        dispatcher.hash_mapping = Some(ActorMapping {
                            original_indices: original_indices.clone(),
                            data,
                        });
                    }
                }
            }
        }

        // Mark the actors to be built as `State::Building`.
        let actor_info = locations
            .actor_locations
            .iter()
            .map(|(&actor_id, parallel_unit)| {
                (
                    actor_id,
                    ActorStatus {
                        parallel_unit: Some(parallel_unit.clone()),
                        state: ActorState::Inactive as i32,
                    },
                )
            })
            .collect();
        table_fragments.set_actor_status(actor_info);
        let actor_map = table_fragments.actor_map();

        // Actors on each stream node will need to know where their upstream lies. `actor_info`
        // includes such information. It contains:
        // 1. actors in the current create-materialized-view request.
        // 2. all upstream actors.
        let actor_infos_to_broadcast = {
            let current = locations.actor_infos();
            let upstream = upstream_node_actors
                .iter()
                .flat_map(|(node_id, upstreams)| {
                    upstreams.iter().map(|up_id| ActorInfo {
                        actor_id: *up_id,
                        host: locations
                            .worker_locations
                            .get(node_id)
                            .unwrap()
                            .host
                            .clone(),
                    })
                });
            current.chain(upstream).collect_vec()
        };

        let actor_host_infos = locations.actor_info_map();
        let node_actors = locations.worker_actors();

        // Hanging channels for each worker node.
        let mut node_hanging_channels = {
            // upstream_actor_id -> Vec<downstream_actor_info>
            let up_id_to_down_info = dispatchers
                .iter()
                .map(|(&up_id, dispatchers)| {
                    let down_infos = dispatchers
                        .iter()
                        .flat_map(|d| d.downstream_actor_id.iter())
                        .map(|down_id| actor_host_infos[down_id].clone())
                        .collect_vec();
                    (up_id, down_infos)
                })
                .collect::<HashMap<_, _>>();

            upstream_node_actors
                .iter()
                .map(|(node_id, up_ids)| {
                    (
                        *node_id,
                        up_ids
                            .iter()
                            .flat_map(|up_id| {
                                up_id_to_down_info[up_id]
                                    .iter()
                                    .map(|down_info| HangingChannel {
                                        upstream: Some(ActorInfo {
                                            actor_id: *up_id,
                                            host: None,
                                        }),
                                        downstream: Some(down_info.clone()),
                                    })
                            })
                            .collect_vec(),
                    )
                })
                .collect::<HashMap<_, _>>()
        };

        // We send RPC request in two stages.
        // The first stage does 2 things: broadcast actor info, and send local actor ids to
        // different WorkerNodes. Such that each WorkerNode knows the overall actor
        // allocation, but not actually builds it. We initialize all channels in this stage.
        for (node_id, actors) in &node_actors {
            let node = locations.worker_locations.get(node_id).unwrap();

            let client = self.client_pool.get(node).await?;

            client
                .to_owned()
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_infos_to_broadcast.clone(),
                })
                .await?;

            let stream_actors = actors
                .iter()
                .map(|actor_id| actor_map.get(actor_id).cloned().unwrap())
                .collect::<Vec<_>>();

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "update actors");
            client
                .to_owned()
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: stream_actors.clone(),
                    hanging_channels: node_hanging_channels.remove(node_id).unwrap_or_default(),
                })
                .await?;
        }

        // Build remaining hanging channels on compute nodes.
        for (node_id, hanging_channels) in node_hanging_channels {
            let node = locations.worker_locations.get(&node_id).unwrap();

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

        // // Register to compaction group beforehand.
        // let registered_table_ids = self
        //     .compaction_group_manager
        //     .register_table_fragments(&table_fragments, table_properties)
        //     .await?;
        // let compaction_group_manager_ref = self.compaction_group_manager.clone();
        // revert_funcs.push(Box::pin(async move {
        //     if let Err(e) = compaction_group_manager_ref.unregister_table_ids(&registered_table_ids).await {
        //         tracing::warn!("Failed to unregister_table_ids {:#?}.\nThey will be cleaned up on node restart.\n{:#?}", registered_table_ids, e);
        //     }
        // }));

        // In the second stage, each [`WorkerNode`] builds local actors and connect them with
        // channels.
        for (node_id, actors) in node_actors {
            let node = locations.worker_locations.get(&node_id).unwrap();

            let client = self.client_pool.get(node).await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "build actors");
            client
                .to_owned()
                .build_actors(BuildActorsRequest {
                    request_id,
                    actor_id: actors,
                })
                .await?;
        }

        // Extract the fragments that include source operators.
        let _source_fragments = {
            let mut source_fragments = HashMap::new();
            fetch_source_fragments(&mut source_fragments, &table_fragments);
            source_fragments
        };

        // Add table fragments to meta store with state: `State::Creating`.
        self.fragment_manager
            .start_create_table_fragments(table_fragments.clone())
            .await?;

        let table_id = table_fragments.table_id();

        if let Err(err) = self
            .barrier_manager
            .run_command(Command::CreateMaterializedView {
                table_fragments,
                table_sink_map: table_sink_map.clone(),
                dispatchers: dispatchers.clone(),
                //source_state: init_split_assignment.clone(),
                source_state: Default::default(),
            })
            .await
        {
            self.fragment_manager
                .cancel_create_table_fragments(&table_id)
                .await?;
            return Err(err);
        }

        revert_funcs.clear();
        Ok(())
    }

    /// Broadcast the create sink request to all compute nodes.
    pub async fn create_sink(&self, sink: &Sink, _table_fragments: TableFragments) -> Result<()> {
        // This scope guard does clean up jobs ASYNCHRONOUSLY before Err returns.
        // It MUST be cleared before Ok returns.
        let mut revert_funcs = scopeguard::guard(
            vec![],
            |revert_funcs: Vec<futures::future::BoxFuture<()>>| {
                tokio::spawn(async move {
                    for revert_func in revert_funcs {
                        revert_func.await;
                    }
                });
            },
        );

        let futures = self
            .all_stream_clients()
            .await?
            .into_iter()
            .map(|mut client| {
                let request = ComputeNodeCreateSinkRequest {
                    sink: Some(sink.clone()),
                };
                async move { client.create_sink(request).await.map_err(RwError::from) }
            });

        // ignore response body, always none
        let _ = try_join_all(futures).await?;

        let core = self.core.lock().await;
        if core.managed_sinks.contains_key(&sink.get_id()) {
            log::warn!("sink {} already registered", sink.get_id());
            revert_funcs.clear();
            return Ok(());
        }


        revert_funcs.clear();
        Ok(())
    }

    pub async fn drop_sink(&self, sink_id: SinkId) -> Result<()> {
        let futures = self
            .all_stream_clients()
            .await?
            .into_iter()
            .map(|mut client| {
                let request = ComputeNodeDropSinkRequest { sink_id };
                async move { client.drop_sink(request).await.map_err(RwError::from) }
            });
        let _responses: Vec<_> = try_join_all(futures).await?;

        Ok(())
    }
}
