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
use std::time::Instant;

use itertools::Itertools;
use log::{debug, info};
use risingwave_common::catalog::TableId;
use risingwave_common::error::{internal_error, Result, ToRwResult};
use risingwave_common::hash::VIRTUAL_NODE_COUNT;
use risingwave_common::util::compress::compress_data;
use risingwave_pb::catalog::Source;
use risingwave_pb::common::{ActorInfo, ParallelUnitMapping, WorkerType};
use risingwave_pb::meta::table_fragments::{ActorState, ActorStatus};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{ActorMapping, DispatcherType, StreamNode, StreamSourceState};
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, HangingChannel, UpdateActorsRequest,
};
use risingwave_rpc_client::StreamClientPoolRef;
use uuid::Uuid;

use super::ScheduledLocations;
use crate::barrier::{BarrierManagerRef, Command};
use crate::cluster::{ClusterManagerRef, ParallelUnitId, WorkerId};
use crate::manager::{HashMappingManagerRef, MetaSrvEnv};
use crate::model::{ActorId, DispatcherId, TableFragments};
use crate::storage::MetaStore;
use crate::stream::{FragmentManagerRef, Scheduler, SourceManagerRef};

pub type GlobalStreamManagerRef<S> = Arc<GlobalStreamManager<S>>;

/// [`CreateMaterializedViewContext`] carries one-time infos.
#[derive(Default)]
pub struct CreateMaterializedViewContext {
    /// New dispatches to add from upstream actors to downstream actors.
    pub dispatches: HashMap<(ActorId, DispatcherId), Vec<ActorId>>,
    /// Upstream mview actor ids grouped by node id.
    pub upstream_node_actors: HashMap<WorkerId, Vec<ActorId>>,
    /// Upstream mview actor ids grouped by table id.
    pub table_sink_map: HashMap<TableId, Vec<ActorId>>,
    /// Dependent table ids
    pub dependent_table_ids: HashSet<TableId>,
    /// Temporary source info used during `create_materialized_source`
    pub affiliated_source: Option<Source>,
    /// Table id offset get from meta id generator. Used to calculate global unique table id.
    pub table_id_offset: u32,
}

/// `GlobalStreamManager` manages all the streams in the system.
pub struct GlobalStreamManager<S: MetaStore> {
    /// Manages definition and status of fragments and actors
    fragment_manager: FragmentManagerRef<S>,

    /// Broadcasts and collect barriers
    barrier_manager: BarrierManagerRef<S>,

    /// Maintains information of the cluster
    cluster_manager: ClusterManagerRef<S>,

    /// Maintains streaming sources from external system like kafka
    source_manager: SourceManagerRef<S>,

    /// Maintains vnode mapping of all fragments and state tables.
    hash_mapping_manager: HashMappingManagerRef,

    /// Schedules streaming actors into compute nodes
    scheduler: Scheduler<S>,

    /// Client Pool to stream service on compute nodes
    client_pool: StreamClientPoolRef,
}

impl<S> GlobalStreamManager<S>
where
    S: MetaStore,
{
    pub async fn new(
        env: MetaSrvEnv<S>,
        fragment_manager: FragmentManagerRef<S>,
        barrier_manager: BarrierManagerRef<S>,
        cluster_manager: ClusterManagerRef<S>,
        source_manager: SourceManagerRef<S>,
    ) -> Result<Self> {
        Ok(Self {
            scheduler: Scheduler::new(cluster_manager.clone(), env.hash_mapping_manager_ref()),
            fragment_manager,
            barrier_manager,
            cluster_manager,
            source_manager,
            hash_mapping_manager: env.hash_mapping_manager_ref(),
            client_pool: env.stream_client_pool_ref(),
        })
    }

    async fn resolve_chain_node(
        &self,
        table_fragments: &mut TableFragments,
        dependent_table_ids: &HashSet<TableId>,
        dispatches: &mut HashMap<(ActorId, DispatcherId), Vec<ActorId>>,
        upstream_node_actors: &mut HashMap<WorkerId, Vec<ActorId>>,
        locations: &ScheduledLocations,
    ) -> Result<()> {
        // The closure environment. Used to simulate recursive closure.
        struct Env<'a> {
            hash_mapping_manager: &'a HashMappingManagerRef,
            /// Records what's the correspoding actor of each parallel unit of one table.
            upstream_parallel_unit_info: &'a HashMap<TableId, BTreeMap<ParallelUnitId, ActorId>>,
            /// Records what's the actors on each worker of one table.
            tables_node_actors: &'a HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>>,
            /// Schedule information of all actors.
            locations: &'a ScheduledLocations,

            dispatches: &'a mut HashMap<(ActorId, DispatcherId), Vec<ActorId>>,
            upstream_node_actors: &'a mut HashMap<WorkerId, Vec<ActorId>>,
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
                let table_id = TableId::from(&chain.table_ref_id);
                let hash_mapping = self
                    .hash_mapping_manager
                    .get_table_hash_mapping(&table_id.table_id)
                    .unwrap_or_else(|| {
                        panic!("table {} should have a vnode mapping", table_id.table_id)
                    });

                let (upstream_actor_id, parallel_unit_id) = {
                    // 1. use table id to get upstream parallel_unit -> actor_id mapping
                    let upstream_parallel_actor_mapping =
                        self.upstream_parallel_unit_info.get(&table_id).unwrap();
                    // 2. use our actor id to get parallel unit id of the chain actor
                    let parallel_unit_id =
                        self.locations.actor_locations.get(&actor_id).unwrap().id;
                    // 3. and use chain actor's parallel unit id to get the corresponding upstream
                    // actor id
                    (
                        *upstream_parallel_actor_mapping
                            .get(&parallel_unit_id)
                            .unwrap(),
                        parallel_unit_id,
                    )
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
                let merge_stream_node = &mut stream_node.input[0];
                if let Some(NodeBody::Merge(ref mut merge)) = merge_stream_node.node_body {
                    merge.upstream_actor_id.push(upstream_actor_id);
                } else {
                    unreachable!("chain's input[0] should always be merge");
                }
                let batch_stream_node = &mut stream_node.input[1];
                if let Some(NodeBody::BatchPlan(ref mut batch_query)) = batch_stream_node.node_body
                {
                    let (original_indices, data) = compress_data(&hash_mapping);
                    batch_query.hash_mapping = Some(ParallelUnitMapping {
                        table_id: Default::default(),
                        original_indices,
                        data,
                    });
                    batch_query.parallel_unit_id = parallel_unit_id;
                } else {
                    unreachable!("chain's input[1] should always be batch query");
                }

                // finally, we should also build dispatcher infos here.
                self.dispatches
                    .entry((upstream_actor_id, stream_node.operator_id))
                    .or_default()
                    .push(actor_id);

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
            hash_mapping_manager: &self.hash_mapping_manager,
            upstream_parallel_unit_info,
            tables_node_actors,
            locations,

            dispatches,
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

    /// Create materialized view, it works as follows:
    /// 1. schedule the actors to nodes in the cluster.
    /// 2. broadcast the actor info table.
    /// (optional) get the split information of the `StreamSource` via source manager and patch
    /// actors .
    /// 3. notify related nodes to update and build the actors.
    /// 4. store related meta data.
    ///
    /// Note the `table_fragments` is required to be sorted in topology order. (Downstream first,
    /// then upstream.)
    pub async fn create_materialized_view(
        &self,
        mut table_fragments: TableFragments,
        CreateMaterializedViewContext {
            mut dispatches,
            mut upstream_node_actors,
            table_sink_map,
            dependent_table_ids,
            affiliated_source,
            table_id_offset: _,
        }: CreateMaterializedViewContext,
    ) -> Result<()> {
        let nodes = self
            .cluster_manager
            .list_worker_node(
                WorkerType::ComputeNode,
                Some(risingwave_pb::common::worker_node::State::Running),
            )
            .await;
        if nodes.is_empty() {
            return Err(internal_error("no available compute node in the cluster"));
        }

        let mut locations = ScheduledLocations::new();
        locations.node_locations = nodes.into_iter().map(|node| (node.id, node)).collect();

        let topological_order = table_fragments.generate_topological_order();

        // Schedule each fragment(actors) to nodes. Vnode mapping in fragment will be filled in
        // as well.
        for fragment_id in topological_order {
            let fragment = table_fragments.fragments.get_mut(&fragment_id).unwrap();
            self.scheduler.schedule(fragment, &mut locations).await?;
        }

        // resolve chain node infos, including:
        // 1. insert upstream actor id in merge node
        // 2. insert parallel unit id in batch query node
        self.resolve_chain_node(
            &mut table_fragments,
            &dependent_table_ids,
            &mut dispatches,
            &mut upstream_node_actors,
            &locations,
        )
        .await?;

        // Verify whether all same_as_upstream constraints are satisfied.
        //
        // Currently, the scheduler (when there's no scale-in or scale-out) will always schedule
        // chain node on the same node as upstreams. However, this constraint will easily be broken
        // if parallel units are not aligned between upstream nodes.

        // Record actor -> fragment mapping for finding out downstream fragments.
        let mut actor_to_vnode_mapping = HashMap::new();
        for fragment in table_fragments.fragments.values() {
            for actor in &fragment.actors {
                actor_to_vnode_mapping.insert(actor.actor_id, fragment.vnode_mapping.clone());
            }
        }

        // Fill hash dispatcher's mapping with scheduled locations.
        table_fragments
            .fragments
            .iter_mut()
            .for_each(|(_, fragment)| {
                fragment.actors.iter_mut().for_each(|actor| {
                    actor.dispatcher.iter_mut().for_each(|dispatcher| {
                        if dispatcher.get_type().unwrap() == DispatcherType::Hash {
                            let downstream_actor_id =
                                dispatcher.downstream_actor_id.first().unwrap_or_else(|| {
                                    panic!(
                                        "hash dispatcher should have at least one downstream actor"
                                    );
                                });
                            let hash_mapping = actor_to_vnode_mapping
                                .get(downstream_actor_id)
                                .unwrap()
                                .as_ref()
                                .unwrap_or_else(|| {
                                    panic!(
                                        "actor {} should have a vnode mapping",
                                        downstream_actor_id
                                    );
                                });

                            let downstream_actors = &dispatcher.downstream_actor_id;

                            // `self.hash_parallel_count` as the number of its downstream actors.
                            // However, since the frontend optimizer is still WIP, there exists some
                            // unoptimized situation where a hash dispatcher has ONLY ONE downstream
                            // actor, which makes it behave like a simple dispatcher. As a
                            // workaround, we specially compute the consistent hash mapping here.
                            // The `if` branch could be removed after the optimizer has been fully
                            // implemented.
                            if downstream_actors.len() == 1 {
                                dispatcher.hash_mapping = Some(ActorMapping {
                                    original_indices: vec![VIRTUAL_NODE_COUNT as u64 - 1],
                                    data: vec![downstream_actors[0]],
                                });
                            } else {
                                // extract "parallel unit -> downstream actor" mapping from
                                // locations.
                                let parallel_unit_actor_map = downstream_actors
                                    .iter()
                                    .map(|actor_id| {
                                        (
                                            locations.actor_locations.get(actor_id).unwrap().id,
                                            *actor_id,
                                        )
                                    })
                                    .collect::<HashMap<_, _>>();
                                let ParallelUnitMapping {
                                    original_indices,
                                    data,
                                    ..
                                } = hash_mapping;
                                let data = data
                                    .iter()
                                    .map(|parallel_unit_id| {
                                        parallel_unit_actor_map[parallel_unit_id]
                                    })
                                    .collect_vec();
                                dispatcher.hash_mapping = Some(ActorMapping {
                                    original_indices: original_indices.clone(),
                                    data,
                                });
                            };
                        }
                    });
                })
            });

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
        let mut actor_map = table_fragments.actor_map();

        let mut source_actors_group_by_fragment = HashMap::new();
        for fragment in table_fragments.fragments() {
            let mut source_actors = HashMap::new();
            for actor in &fragment.actors {
                if let Some(source_id) =
                    TableFragments::fetch_stream_source_id(actor.nodes.as_ref().unwrap())
                {
                    source_actors
                        .entry(source_id)
                        .or_insert(vec![])
                        .push(actor.actor_id as ActorId)
                }
            }

            for (source_id, actors) in source_actors {
                source_actors_group_by_fragment
                    .entry(source_id)
                    .or_insert(vec![])
                    .push(actors)
            }
        }

        let split_assignment = self
            .source_manager
            .schedule_split_for_actors(source_actors_group_by_fragment, affiliated_source)
            .await?;

        // patch source actors with splits
        for (actor_id, actor) in &mut actor_map {
            if let Some(splits) = split_assignment.get(actor_id) {
                let mut node = actor.nodes.as_mut().unwrap();
                while !node.input.is_empty() {
                    node = node.input.first_mut().unwrap();
                }

                if let Some(NodeBody::Source(s)) = node.node_body.as_mut() {
                    log::debug!(
                        "patching source node #{} with splits {:?}",
                        actor_id,
                        splits
                    );

                    if !splits.is_empty() {
                        s.stream_source_state = Some(StreamSourceState {
                            split_type: splits.first().unwrap().get_type(),
                            stream_source_splits: splits
                                .iter()
                                .map(|split| split.to_json_bytes().to_vec())
                                .collect(),
                        });
                    }
                }
            }
        }

        // Actors on each stream node will need to know where their upstream lies. `actor_info`
        // includes such information. It contains: 1. actors in the current create
        // materialized view request. 2. all upstream actors.
        let mut actor_infos_to_broadcast = locations.actor_infos();
        actor_infos_to_broadcast.extend(upstream_node_actors.iter().flat_map(
            |(node_id, upstreams)| {
                upstreams.iter().map(|up_id| ActorInfo {
                    actor_id: *up_id,
                    host: locations.node_locations.get(node_id).unwrap().host.clone(),
                })
            },
        ));

        let actor_host_infos = locations.actor_info_map();

        let node_actors = locations.node_actors();

        let dispatches = dispatches
            .iter()
            .map(|(up_id, down_ids)| {
                (
                    *up_id,
                    down_ids
                        .iter()
                        .map(|down_id| {
                            actor_host_infos
                                .get(down_id)
                                .expect("downstream actor info not exist")
                                .clone()
                        })
                        .collect_vec(),
                )
            })
            .collect::<HashMap<_, _>>();

        let up_id_to_down_info = dispatches
            .iter()
            .map(|((up_id, _dispatcher_id), down_info)| (*up_id, down_info.clone()))
            .collect::<HashMap<_, _>>();

        let mut node_hanging_channels = upstream_node_actors
            .iter()
            .map(|(node_id, up_ids)| {
                (
                    *node_id,
                    up_ids
                        .iter()
                        .flat_map(|up_id| {
                            up_id_to_down_info
                                .get(up_id)
                                .expect("expected dispatches info")
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
            .collect::<HashMap<_, _>>();

        // We send RPC request in two stages.
        // The first stage does 2 things: broadcast actor info, and send local actor ids to
        // different WorkerNodes. Such that each WorkerNode knows the overall actor
        // allocation, but not actually builds it. We initialize all channels in this stage.
        for (node_id, actors) in &node_actors {
            let node = locations.node_locations.get(node_id).unwrap();

            let client = self.client_pool.get(node).await?;

            client
                .to_owned()
                .broadcast_actor_info_table(BroadcastActorInfoTableRequest {
                    info: actor_infos_to_broadcast.clone(),
                })
                .await
                .to_rw_result_with(|| format!("failed to connect to {}", node_id))?;

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
                .await
                .to_rw_result_with(|| format!("failed to connect to {}", node_id))?;
        }

        for (node_id, hanging_channels) in node_hanging_channels {
            let node = locations.node_locations.get(&node_id).unwrap();

            let client = self.client_pool.get(node).await?;
            let request_id = Uuid::new_v4().to_string();

            client
                .to_owned()
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: vec![],
                    hanging_channels,
                })
                .await
                .to_rw_result_with(|| format!("failed to connect to {}", node_id))?;
        }

        // In the second stage, each [`WorkerNode`] builds local actors and connect them with
        // channels.
        for (node_id, actors) in node_actors {
            let node = locations.node_locations.get(&node_id).unwrap();

            let client = self.client_pool.get(node).await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "build actors");
            client
                .to_owned()
                .build_actors(BuildActorsRequest {
                    request_id,
                    actor_id: actors,
                })
                .await
                .to_rw_result_with(|| format!("failed to connect to {}", node_id))?;
        }

        // Add table fragments to meta store with state: `State::Creating`.
        self.fragment_manager
            .start_create_table_fragments(table_fragments.clone())
            .await?;
        let table_id = table_fragments.table_id();
        if let Err(err) = self
            .barrier_manager
            .run_command(Command::CreateMaterializedView {
                table_fragments,
                table_sink_map,
                dispatches,
            })
            .await
        {
            self.fragment_manager
                .cancel_create_table_fragments(&table_id)
                .await?;
            return Err(err);
        }

        Ok(())
    }

    /// Dropping materialized view is done by barrier manager. Check
    /// [`Command::DropMaterializedView`] for details.
    pub async fn drop_materialized_view(&self, table_id: &TableId) -> Result<()> {
        self.barrier_manager
            .run_command(Command::DropMaterializedView(*table_id))
            .await?;

        Ok(())
    }

    /// Flush means waiting for the next barrier to collect.
    pub async fn flush(&self) -> Result<()> {
        let start = Instant::now();

        debug!("start barrier flush");
        self.barrier_manager
            .wait_for_next_barrier_to_collect()
            .await?;

        let elapsed = Instant::now().duration_since(start);
        info!("barrier flushed in {:?}", elapsed);

        Ok(())
    }

    // fn
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::thread::sleep;
    use std::time::Duration;

    use risingwave_common::catalog::TableId;
    use risingwave_common::error::tonic_err;
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
    use risingwave_pb::meta::table_fragments::Fragment;
    use risingwave_pb::plan_common::TableRefId;
    use risingwave_pb::stream_plan::*;
    use risingwave_pb::stream_service::stream_service_server::{
        StreamService, StreamServiceServer,
    };
    use risingwave_pb::stream_service::{
        BroadcastActorInfoTableResponse, BuildActorsResponse, DropActorsRequest,
        DropActorsResponse, InjectBarrierRequest, InjectBarrierResponse, UpdateActorsResponse, *,
    };
    use tokio::sync::oneshot::Sender;
    use tokio::task::JoinHandle;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::barrier::GlobalBarrierManager;
    use crate::cluster::ClusterManager;
    use crate::hummock::HummockManager;
    use crate::manager::{CatalogManager, MetaSrvEnv};
    use crate::model::ActorId;
    use crate::rpc::metrics::MetaMetrics;
    use crate::storage::MemStore;
    use crate::stream::{FragmentManager, SourceManager};

    struct FakeFragmentState {
        actor_streams: Mutex<HashMap<ActorId, StreamActor>>,
        actor_ids: Mutex<HashSet<ActorId>>,
        actor_infos: Mutex<HashMap<ActorId, HostAddress>>,
    }

    struct FakeStreamService {
        inner: Arc<FakeFragmentState>,
    }

    #[async_trait::async_trait]
    impl StreamService for FakeStreamService {
        async fn update_actors(
            &self,
            request: Request<UpdateActorsRequest>,
        ) -> std::result::Result<Response<UpdateActorsResponse>, Status> {
            let req = request.into_inner();
            let mut guard = self.inner.actor_streams.lock().unwrap();
            for actor in req.get_actors() {
                guard.insert(actor.get_actor_id(), actor.clone());
            }

            Ok(Response::new(UpdateActorsResponse { status: None }))
        }

        async fn build_actors(
            &self,
            request: Request<BuildActorsRequest>,
        ) -> std::result::Result<Response<BuildActorsResponse>, Status> {
            let req = request.into_inner();
            let mut guard = self.inner.actor_ids.lock().unwrap();
            for id in req.get_actor_id() {
                guard.insert(*id);
            }

            Ok(Response::new(BuildActorsResponse {
                request_id: "".to_string(),
                status: None,
            }))
        }

        async fn broadcast_actor_info_table(
            &self,
            request: Request<BroadcastActorInfoTableRequest>,
        ) -> std::result::Result<Response<BroadcastActorInfoTableResponse>, Status> {
            let req = request.into_inner();
            let mut guard = self.inner.actor_infos.lock().unwrap();
            for info in req.get_info() {
                guard.insert(
                    info.get_actor_id(),
                    info.get_host().map_err(tonic_err)?.clone(),
                );
            }

            Ok(Response::new(BroadcastActorInfoTableResponse {
                status: None,
            }))
        }

        async fn drop_actors(
            &self,
            _request: Request<DropActorsRequest>,
        ) -> std::result::Result<Response<DropActorsResponse>, Status> {
            panic!("not implemented")
        }

        async fn inject_barrier(
            &self,
            _request: Request<InjectBarrierRequest>,
        ) -> std::result::Result<Response<InjectBarrierResponse>, Status> {
            Ok(Response::new(InjectBarrierResponse::default()))
        }

        async fn create_source(
            &self,
            _request: Request<CreateSourceRequest>,
        ) -> std::result::Result<Response<CreateSourceResponse>, Status> {
            unimplemented!()
        }

        async fn drop_source(
            &self,
            _request: Request<DropSourceRequest>,
        ) -> std::result::Result<Response<DropSourceResponse>, Status> {
            unimplemented!()
        }

        async fn force_stop_actors(
            &self,
            _request: Request<ForceStopActorsRequest>,
        ) -> std::result::Result<Response<ForceStopActorsResponse>, Status> {
            Ok(Response::new(ForceStopActorsResponse::default()))
        }

        async fn sync_sources(
            &self,
            _request: Request<SyncSourcesRequest>,
        ) -> std::result::Result<Response<SyncSourcesResponse>, Status> {
            Ok(Response::new(SyncSourcesResponse::default()))
        }
    }

    struct MockServices {
        global_stream_manager: GlobalStreamManager<MemStore>,
        fragment_manager: FragmentManagerRef<MemStore>,
        state: Arc<FakeFragmentState>,
        join_handles: Vec<JoinHandle<()>>,
        shutdown_txs: Vec<Sender<()>>,
    }

    impl MockServices {
        async fn start(host: &str, port: u16) -> Result<Self> {
            let addr = SocketAddr::new(host.parse().unwrap(), port);
            let state = Arc::new(FakeFragmentState {
                actor_streams: Mutex::new(HashMap::new()),
                actor_ids: Mutex::new(HashSet::new()),
                actor_infos: Mutex::new(HashMap::new()),
            });
            let fake_service = FakeStreamService {
                inner: state.clone(),
            };

            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
            let stream_srv = StreamServiceServer::new(fake_service);
            let join_handle = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(stream_srv)
                    .serve_with_shutdown(addr, async move { shutdown_rx.await.unwrap() })
                    .await
                    .unwrap();
            });
            sleep(Duration::from_secs(1));

            let env = MetaSrvEnv::for_test().await;
            let cluster_manager =
                Arc::new(ClusterManager::new(env.clone(), Duration::from_secs(3600)).await?);
            let host = HostAddress {
                host: host.to_string(),
                port: port as i32,
            };
            cluster_manager
                .add_worker_node(host.clone(), WorkerType::ComputeNode)
                .await?;
            cluster_manager.activate_worker_node(host).await?;

            let catalog_manager = Arc::new(CatalogManager::new(env.clone()).await?);
            let fragment_manager = Arc::new(FragmentManager::new(env.clone()).await?);
            let meta_metrics = Arc::new(MetaMetrics::new());
            let hummock_manager = Arc::new(
                HummockManager::new(env.clone(), cluster_manager.clone(), meta_metrics.clone())
                    .await?,
            );
            let barrier_manager = Arc::new(GlobalBarrierManager::new(
                env.clone(),
                cluster_manager.clone(),
                catalog_manager.clone(),
                fragment_manager.clone(),
                hummock_manager,
                meta_metrics.clone(),
            ));

            let source_manager = Arc::new(
                SourceManager::new(
                    env.clone(),
                    cluster_manager.clone(),
                    barrier_manager.clone(),
                    catalog_manager.clone(),
                )
                .await?,
            );

            let stream_manager = GlobalStreamManager::new(
                env.clone(),
                fragment_manager.clone(),
                barrier_manager.clone(),
                cluster_manager.clone(),
                source_manager.clone(),
            )
            .await?;

            let (join_handle_2, shutdown_tx_2) = GlobalBarrierManager::start(barrier_manager).await;

            Ok(Self {
                global_stream_manager: stream_manager,
                fragment_manager,
                state,
                join_handles: vec![join_handle_2, join_handle],
                shutdown_txs: vec![shutdown_tx_2, shutdown_tx],
            })
        }

        async fn stop(self) {
            for shutdown_tx in self.shutdown_txs {
                shutdown_tx.send(()).unwrap();
            }
            for join_handle in self.join_handles {
                join_handle.await.unwrap();
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_create_materialized_view() -> Result<()> {
        let services = MockServices::start("127.0.0.1", 12333).await?;

        let table_ref_id = TableRefId {
            schema_ref_id: None,
            table_id: 0,
        };
        let table_id = TableId::from(&Some(table_ref_id.clone()));

        let actors = (0..5)
            .map(|i| StreamActor {
                actor_id: i,
                // A dummy node to avoid panic.
                nodes: Some(risingwave_pb::stream_plan::StreamNode {
                    node_body: Some(
                        risingwave_pb::stream_plan::stream_node::NodeBody::Materialize(
                            risingwave_pb::stream_plan::MaterializeNode {
                                table_ref_id: Some(table_ref_id.clone()),
                                ..Default::default()
                            },
                        ),
                    ),
                    operator_id: 1,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let mut fragments = BTreeMap::default();
        fragments.insert(
            0,
            Fragment {
                fragment_id: 0,
                fragment_type: FragmentType::Sink as i32,
                distribution_type: FragmentDistributionType::Hash as i32,
                actors: actors.clone(),
                vnode_mapping: None,
            },
        );
        let table_fragments = TableFragments::new(table_id, fragments);

        let ctx = CreateMaterializedViewContext::default();

        services
            .global_stream_manager
            .create_materialized_view(table_fragments, ctx)
            .await?;

        for actor in actors {
            let mut scheduled_actor = services
                .state
                .actor_streams
                .lock()
                .unwrap()
                .get(&actor.get_actor_id())
                .cloned()
                .unwrap()
                .clone();
            assert!(!scheduled_actor.vnode_bitmap.is_empty());
            scheduled_actor.vnode_bitmap.clear();
            assert_eq!(scheduled_actor, actor);
            assert!(services
                .state
                .actor_ids
                .lock()
                .unwrap()
                .contains(&actor.get_actor_id()));
            assert_eq!(
                services
                    .state
                    .actor_infos
                    .lock()
                    .unwrap()
                    .get(&actor.get_actor_id())
                    .cloned()
                    .unwrap(),
                HostAddress {
                    host: "127.0.0.1".to_string(),
                    port: 12333,
                }
            );
        }

        let sink_actor_ids = services
            .fragment_manager
            .get_table_sink_actor_ids(&table_id)
            .await?;
        let actor_ids = services
            .fragment_manager
            .get_table_actor_ids(&table_id)
            .await?;
        assert_eq!(sink_actor_ids, (0..5).collect::<Vec<u32>>());
        assert_eq!(actor_ids, (0..5).collect::<Vec<u32>>());

        services.stop().await;
        Ok(())
    }
}
