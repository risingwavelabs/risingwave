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

use futures::future::BoxFuture;
use itertools::Itertools;
use risingwave_common::bail;
use risingwave_common::catalog::TableId;
use risingwave_common::types::VIRTUAL_NODE_COUNT;
use risingwave_pb::catalog::Table;
use risingwave_pb::common::{ActorInfo, Buffer, ParallelUnitMapping, WorkerType};
use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
use risingwave_pb::meta::table_fragments::{ActorState, ActorStatus};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{ActorMapping, Dispatcher, DispatcherType, StreamNode};
use risingwave_pb::stream_service::{
    BroadcastActorInfoTableRequest, BuildActorsRequest, HangingChannel, UpdateActorsRequest,
};
use risingwave_rpc_client::StreamClientPoolRef;
use uuid::Uuid;

use super::ScheduledLocations;
use crate::barrier::{BarrierScheduler, Command};
use crate::hummock::compaction_group::manager::CompactionGroupManagerRef;
use crate::manager::{
    ClusterManagerRef, DatabaseId, FragmentManagerRef, FragmentVNodeInfo, IdGeneratorManagerRef,
    MetaSrvEnv, SchemaId, WorkerId,
};
use crate::model::{ActorId, FragmentId, TableFragments};
use crate::storage::MetaStore;
use crate::stream::{fetch_source_fragments, Scheduler, SourceManagerRef};
use crate::MetaResult;

pub type GlobalStreamManagerRef<S> = Arc<GlobalStreamManager<S>>;

/// [`CreateMaterializedViewContext`] carries one-time infos.
#[derive(Default)]
pub struct CreateMaterializedViewContext {
    /// New dispatchers to add from upstream actors to downstream actors.
    pub dispatchers: HashMap<ActorId, Vec<Dispatcher>>,
    /// Upstream mview actor ids grouped by worker node id.
    pub upstream_worker_actors: HashMap<WorkerId, HashSet<ActorId>>,
    /// Upstream mview actor ids grouped by table id.
    pub table_sink_map: HashMap<TableId, Vec<ActorId>>,
    /// Dependent table ids
    pub dependent_table_ids: HashSet<TableId>,
    /// Table id offset get from meta id generator. Used to calculate global unique table id.
    pub table_id_offset: u32,
    /// Internal TableID to Table mapping
    pub internal_table_id_map: HashMap<u32, Option<Table>>,
    /// The upstream tables of all fragments containing chain nodes.
    /// These fragments need to be colocated with their upstream tables.
    ///
    /// They are scheduled in `resolve_chain_node`.
    pub chain_fragment_upstream_table_map: HashMap<FragmentId, TableId>,
    /// SchemaId of mview
    pub schema_id: SchemaId,
    /// DatabaseId of mview
    pub database_id: DatabaseId,
    /// Name of mview, for internal table name generation.
    pub mview_name: String,
    pub table_properties: HashMap<String, String>,
}

impl CreateMaterializedViewContext {
    pub fn internal_tables(&self) -> Vec<Table> {
        self.internal_table_id_map
            .values()
            .flatten()
            .cloned()
            .collect()
    }

    pub fn internal_table_ids(&self) -> Vec<u32> {
        self.internal_table_id_map.keys().cloned().collect_vec()
    }
}

/// `GlobalStreamManager` manages all the streams in the system.
pub struct GlobalStreamManager<S: MetaStore> {
    /// Manages definition and status of fragments and actors
    pub(super) fragment_manager: FragmentManagerRef<S>,

    /// Broadcasts and collect barriers
    pub(crate) barrier_scheduler: BarrierScheduler<S>,

    /// Maintains information of the cluster
    pub(crate) cluster_manager: ClusterManagerRef<S>,

    /// Maintains streaming sources from external system like kafka
    source_manager: SourceManagerRef<S>,

    /// Schedules streaming actors into compute nodes
    scheduler: Scheduler<S>,

    /// Client Pool to stream service on compute nodes
    pub(crate) client_pool: StreamClientPoolRef,

    /// id generator manager.
    pub(crate) id_gen_manager: IdGeneratorManagerRef<S>,

    compaction_group_manager: CompactionGroupManagerRef<S>,
}

impl<S> GlobalStreamManager<S>
where
    S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        fragment_manager: FragmentManagerRef<S>,
        barrier_scheduler: BarrierScheduler<S>,
        cluster_manager: ClusterManagerRef<S>,
        source_manager: SourceManagerRef<S>,
        compaction_group_manager: CompactionGroupManagerRef<S>,
    ) -> MetaResult<Self> {
        Ok(Self {
            scheduler: Scheduler::new(cluster_manager.clone()),
            fragment_manager,
            barrier_scheduler,
            cluster_manager,
            source_manager,
            client_pool: env.stream_client_pool_ref(),
            compaction_group_manager,
            id_gen_manager: env.id_gen_manager_ref(),
        })
    }

    async fn resolve_chain_node(
        &self,
        table_fragments: &mut TableFragments,
        dependent_table_ids: &HashSet<TableId>,
        dispatchers: &mut HashMap<ActorId, Vec<Dispatcher>>,
        upstream_worker_actors: &mut HashMap<WorkerId, HashSet<ActorId>>,
        locations: &mut ScheduledLocations,
        chain_fragment_upstream_table_map: &HashMap<FragmentId, TableId>,
    ) -> MetaResult<()> {
        // The closure environment. Used to simulate recursive closure.
        struct Env<'a> {
            /// Records what's the corresponding parallel unit of each actor and mview vnode
            /// mapping info of one table.
            upstream_fragment_vnode_info: &'a HashMap<TableId, FragmentVNodeInfo>,
            /// Records each upstream mview actor's vnode bitmap info.
            upstream_vnode_bitmap_info: &'a mut HashMap<TableId, Vec<(ActorId, Option<Buffer>)>>,
            /// Records what's the actors on each worker of one table.
            tables_worker_actors: &'a HashMap<TableId, BTreeMap<WorkerId, Vec<ActorId>>>,
            /// Schedule information of all actors.
            locations: &'a mut ScheduledLocations,
            /// New dispatchers for this mview.
            dispatchers: &'a mut HashMap<ActorId, Vec<Dispatcher>>,
            /// New vnode bitmaps for chain actors.
            actor_vnode_bitmaps: &'a mut HashMap<ActorId, Option<Buffer>>,
            /// Upstream Materialize actor ids grouped by worker id.
            upstream_worker_actors: &'a mut HashMap<WorkerId, HashSet<ActorId>>,
        }

        impl Env<'_> {
            fn resolve_chain_node_inner(
                &mut self,
                stream_node: &mut StreamNode,
                actor_id: ActorId,
                upstream_actor_idx: usize,
                _same_worker_node_as_upstream: bool,
                is_singleton: bool,
            ) -> MetaResult<()> {
                let Some(NodeBody::Chain(ref mut chain)) = stream_node.node_body else {
                    // If node is not chain node, recursively deal with input nodes
                    for input in &mut stream_node.input {
                        self.resolve_chain_node_inner(input, actor_id, upstream_actor_idx, _same_worker_node_as_upstream, is_singleton)?;
                    }
                    return Ok(());
                };

                // get upstream table id
                let table_id = TableId::new(chain.table_id);
                // 1. use table id to get upstream vnode mapping info: [(actor_id,
                // option(vnode_bitmap))]
                let upstream_vnode_mapping_info = &self.upstream_vnode_bitmap_info[&table_id];

                let (upstream_actor_id, upstream_vnode_bitmap) = {
                    if is_singleton {
                        // The upstream fragment should also be singleton.
                        upstream_vnode_mapping_info.iter().exactly_one().unwrap()
                    } else {
                        // Assign a upstream actor id to this chain node.
                        &upstream_vnode_mapping_info[upstream_actor_idx]
                    }
                };

                // Here we force schedule the chain node to the same parallel unit as its upstream,
                // so `same_worker_node_as_upstream` is not used here. If we want to
                // support different parallel units, we need to keep the vnode bitmap and assign a
                // new parallel unit with some other strategies.
                let upstream_parallel_unit = self
                    .upstream_fragment_vnode_info
                    .get(&table_id)
                    .unwrap()
                    .actor_parallel_unit_maps
                    .get(upstream_actor_id)
                    .unwrap()
                    .clone();
                self.locations
                    .actor_locations
                    .insert(actor_id, upstream_parallel_unit);
                self.actor_vnode_bitmaps
                    .insert(actor_id, upstream_vnode_bitmap.clone());

                // fill upstream node-actor info for later use
                let upstream_table_worker_actors =
                    self.tables_worker_actors.get(&table_id).unwrap();

                let chain_upstream_worker_actors = upstream_table_worker_actors
                    .iter()
                    .flat_map(|(worker_id, actor_ids)| {
                        actor_ids.iter().map(|actor_id| (*worker_id, *actor_id))
                    })
                    .filter(|(_, actor_id)| upstream_actor_id == actor_id)
                    .into_group_map();
                for (worker_id, actor_ids) in chain_upstream_worker_actors {
                    self.upstream_worker_actors
                        .entry(worker_id)
                        .or_default()
                        .extend(actor_ids);
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
                merge.upstream_actor_id.push(*upstream_actor_id);

                // finally, we should also build dispatcher infos here.
                //
                // Note: currently we ensure that the downstream chain operator has the same
                // parallel unit and distribution as the upstream mview, so we can simply use
                // `NoShuffle` dispatcher here.
                self.dispatchers
                    .entry(*upstream_actor_id)
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

        let upstream_fragment_vnode_info = &self
            .fragment_manager
            .get_sink_fragment_vnode_info(dependent_table_ids)
            .await?;

        let upstream_vnode_bitmap_info = &mut self
            .fragment_manager
            .get_sink_vnode_bitmap_info(dependent_table_ids)
            .await?;

        let tables_worker_actors = &self
            .fragment_manager
            .get_tables_worker_actors(dependent_table_ids)
            .await?;

        let mut env = Env {
            upstream_fragment_vnode_info,
            upstream_vnode_bitmap_info,
            tables_worker_actors,
            locations,
            dispatchers,
            actor_vnode_bitmaps: &mut Default::default(),
            upstream_worker_actors,
        };

        for fragment in table_fragments.fragments.values_mut() {
            if !chain_fragment_upstream_table_map.contains_key(&fragment.fragment_id) {
                continue;
            }

            let is_singleton =
                fragment.get_distribution_type()? == FragmentDistributionType::Single;

            for (idx, actor) in &mut fragment.actors.iter_mut().enumerate() {
                let stream_node = actor.nodes.as_mut().unwrap();
                env.resolve_chain_node_inner(
                    stream_node,
                    actor.actor_id,
                    idx,
                    actor.same_worker_node_as_upstream,
                    is_singleton,
                )?;
                // setup actor vnode bitmap.
                actor.vnode_bitmap = env.actor_vnode_bitmaps.remove(&actor.actor_id).unwrap();
            }
            // setup fragment vnode mapping.
            let upstream_table_id = chain_fragment_upstream_table_map
                .get(&fragment.fragment_id)
                .unwrap();
            fragment.vnode_mapping = env
                .upstream_fragment_vnode_info
                .get(upstream_table_id)
                .unwrap()
                .vnode_mapping
                .clone();
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
        table_fragments: &mut TableFragments,
        context: &mut CreateMaterializedViewContext,
    ) -> MetaResult<()> {
        let mut revert_funcs = vec![];
        if let Err(e) = self
            .create_materialized_view_impl(&mut revert_funcs, table_fragments, context)
            .await
        {
            for revert_func in revert_funcs.into_iter().rev() {
                revert_func.await;
            }
            return Err(e);
        }
        Ok(())
    }

    async fn create_materialized_view_impl(
        &self,
        revert_funcs: &mut Vec<BoxFuture<'_, ()>>,
        table_fragments: &mut TableFragments,
        CreateMaterializedViewContext {
            dispatchers,
            upstream_worker_actors,
            table_sink_map,
            dependent_table_ids,
            table_properties,
            chain_fragment_upstream_table_map,
            ..
        }: &mut CreateMaterializedViewContext,
    ) -> MetaResult<()> {
        // Schedule actors to parallel units. `locations` will record the parallel unit that an
        // actor is scheduled to, and the worker node this parallel unit is on.
        let mut locations = {
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

            // Schedule each fragment(actors) to nodes except chain, recorded in `locations`.
            // Vnode mapping in fragment will be filled in as well.
            let topological_order = table_fragments.generate_topological_order();
            for fragment_id in topological_order {
                let fragment = table_fragments.fragments.get_mut(&fragment_id).unwrap();
                if !chain_fragment_upstream_table_map.contains_key(&fragment_id) {
                    self.scheduler.schedule(fragment, &mut locations).await?;
                }
            }

            locations
        };

        // Resolve chain node infos, including:
        // 1. insert upstream actor id in merge node
        // 2. insert parallel unit id in batch query node
        self.resolve_chain_node(
            table_fragments,
            dependent_table_ids,
            dispatchers,
            upstream_worker_actors,
            &mut locations,
            chain_fragment_upstream_table_map,
        )
        .await?;

        let dispatchers = &*dispatchers;
        let upstream_worker_actors = &*upstream_worker_actors;

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

                        // Transform the mapping of parallel unit to the mapping of actor.
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

        let table_fragments = table_fragments;
        let actor_map = table_fragments.actor_map();

        // Actors on each stream node will need to know where their upstream lies. `actor_info`
        // includes such information. It contains:
        // 1. actors in the current create-materialized-view request.
        // 2. all upstream actors.
        let actor_infos_to_broadcast = {
            let current = locations.actor_infos();
            let upstream = upstream_worker_actors
                .iter()
                .flat_map(|(worker_id, upstreams)| {
                    upstreams.iter().map(|up_id| ActorInfo {
                        actor_id: *up_id,
                        host: locations
                            .worker_locations
                            .get(worker_id)
                            .unwrap()
                            .host
                            .clone(),
                    })
                });
            current.chain(upstream).collect_vec()
        };

        let actor_host_infos = locations.actor_info_map();
        let worker_actors = locations.worker_actors();

        // Hanging channels for each worker node.
        let mut hanging_channels = {
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

            upstream_worker_actors
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
        for (worker_id, actors) in &worker_actors {
            let worker_node = locations.worker_locations.get(worker_id).unwrap();
            let client = self.client_pool.get(worker_node).await?;

            client
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
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: stream_actors.clone(),
                    hanging_channels: hanging_channels.remove(worker_id).unwrap_or_default(),
                })
                .await?;
        }

        // Build remaining hanging channels on compute nodes.
        for (worker_id, hanging_channels) in hanging_channels {
            let worker_node = locations.worker_locations.get(&worker_id).unwrap();
            let client = self.client_pool.get(worker_node).await?;

            let request_id = Uuid::new_v4().to_string();

            client
                .update_actors(UpdateActorsRequest {
                    request_id,
                    actors: vec![],
                    hanging_channels,
                })
                .await?;
        }

        // Register to compaction group beforehand.
        let compaction_group_manager_ref = self.compaction_group_manager.clone();
        let registered_table_ids = compaction_group_manager_ref
            .register_table_fragments(table_fragments, table_properties)
            .await?;
        revert_funcs.push(Box::pin(async move {
            if let Err(e) = compaction_group_manager_ref.unregister_table_ids(&registered_table_ids).await {
                tracing::warn!("Failed to unregister_table_ids {:#?}.\nThey will be cleaned up on node restart.\n{:#?}", registered_table_ids, e);
            }
        }));

        // In the second stage, each [`WorkerNode`] builds local actors and connect them with
        // channels.
        for (worker_id, actors) in worker_actors {
            let worker_node = locations.worker_locations.get(&worker_id).unwrap();
            let client = self.client_pool.get(worker_node).await?;

            let request_id = Uuid::new_v4().to_string();
            tracing::debug!(request_id = request_id.as_str(), actors = ?actors, "build actors");
            client
                .build_actors(BuildActorsRequest {
                    request_id,
                    actor_id: actors,
                })
                .await?;
        }

        // Extract the fragments that include source operators.
        let source_fragments = {
            let mut source_fragments = HashMap::new();
            fetch_source_fragments(&mut source_fragments, table_fragments);
            source_fragments
        };

        // Add table fragments to meta store with state: `State::Creating`.
        self.fragment_manager
            .start_create_table_fragments(table_fragments.clone())
            .await?;

        let table_id = table_fragments.table_id();
        let init_split_assignment = self
            .source_manager
            .pre_allocate_splits(&table_id, source_fragments.clone())
            .await?;

        if let Err(err) = self
            .barrier_scheduler
            .run_command(Command::CreateMaterializedView {
                table_fragments: table_fragments.clone(),
                table_sink_map: table_sink_map.clone(),
                dispatchers: dispatchers.clone(),
                source_state: init_split_assignment.clone(),
            })
            .await
        {
            self.fragment_manager
                .cancel_create_table_fragments(&table_id)
                .await?;
            return Err(err);
        }

        self.source_manager
            .patch_update(Some(source_fragments), Some(init_split_assignment))
            .await?;
        Ok(())
    }

    /// Dropping materialized view is done by barrier manager. Check
    /// [`Command::DropMaterializedView`] for details.
    pub async fn drop_materialized_view(&self, table_id: &TableId) -> MetaResult<()> {
        let table_fragments = self
            .fragment_manager
            .select_table_fragments_by_table_id(table_id)
            .await?;

        // Extract the fragments that include source operators.
        let source_fragments = {
            let mut source_fragments = HashMap::new();
            fetch_source_fragments(&mut source_fragments, &table_fragments);
            source_fragments
        };

        self.barrier_scheduler
            .run_command(Command::DropMaterializedView(*table_id))
            .await?;

        let mut actor_ids = HashSet::new();
        for fragment_ids in source_fragments.values() {
            for fragment_id in fragment_ids {
                if let Some(fragment) = table_fragments.fragments.get(fragment_id) {
                    for actor in &fragment.actors {
                        actor_ids.insert(actor.actor_id);
                    }
                }
            }
        }
        self.source_manager
            .drop_update(Some(source_fragments), Some(actor_ids))
            .await?;

        // Unregister from compaction group afterwards.
        if let Err(e) = self
            .compaction_group_manager
            .unregister_table_fragments(&table_fragments)
            .await
        {
            tracing::warn!(
                "Failed to unregister table {}. It will be unregistered eventually.\n{:#?}",
                table_id,
                e
            );
        }

        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use risingwave_common::catalog::TableId;
    use risingwave_pb::common::{HostAddress, WorkerType};
    use risingwave_pb::meta::table_fragments::fragment::FragmentDistributionType;
    use risingwave_pb::meta::table_fragments::Fragment;
    use risingwave_pb::stream_plan::*;
    use risingwave_pb::stream_service::stream_service_server::{
        StreamService, StreamServiceServer,
    };
    use risingwave_pb::stream_service::{
        BroadcastActorInfoTableResponse, BuildActorsResponse, DropActorsRequest,
        DropActorsResponse, InjectBarrierRequest, InjectBarrierResponse, UpdateActorsResponse, *,
    };
    use tokio::sync::oneshot::Sender;
    #[cfg(feature = "failpoints")]
    use tokio::sync::Notify;
    use tokio::task::JoinHandle;
    use tokio::time::sleep;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::barrier::GlobalBarrierManager;
    use crate::hummock::compaction_group::manager::CompactionGroupManager;
    use crate::hummock::{CompactorManager, HummockManager};
    use crate::manager::{CatalogManager, ClusterManager, FragmentManager, MetaSrvEnv};
    use crate::model::ActorId;
    use crate::rpc::metrics::MetaMetrics;
    use crate::storage::MemStore;
    use crate::stream::SourceManager;
    use crate::MetaOpts;

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
                guard.insert(info.get_actor_id(), info.get_host()?.clone());
            }

            Ok(Response::new(BroadcastActorInfoTableResponse {
                status: None,
            }))
        }

        async fn drop_actors(
            &self,
            _request: Request<DropActorsRequest>,
        ) -> std::result::Result<Response<DropActorsResponse>, Status> {
            Ok(Response::new(DropActorsResponse::default()))
        }

        async fn force_stop_actors(
            &self,
            _request: Request<ForceStopActorsRequest>,
        ) -> std::result::Result<Response<ForceStopActorsResponse>, Status> {
            Ok(Response::new(ForceStopActorsResponse::default()))
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

        async fn sync_sources(
            &self,
            _request: Request<SyncSourcesRequest>,
        ) -> std::result::Result<Response<SyncSourcesResponse>, Status> {
            Ok(Response::new(SyncSourcesResponse::default()))
        }

        async fn drop_source(
            &self,
            _request: Request<DropSourceRequest>,
        ) -> std::result::Result<Response<DropSourceResponse>, Status> {
            unimplemented!()
        }

        async fn barrier_complete(
            &self,
            _request: Request<BarrierCompleteRequest>,
        ) -> std::result::Result<Response<BarrierCompleteResponse>, Status> {
            Ok(Response::new(BarrierCompleteResponse {
                checkpoint: true,
                ..Default::default()
            }))
        }
    }

    struct MockServices {
        global_stream_manager: GlobalStreamManager<MemStore>,
        fragment_manager: FragmentManagerRef<MemStore>,
        state: Arc<FakeFragmentState>,
        join_handle_shutdown_txs: Vec<(JoinHandle<()>, Sender<()>)>,
    }

    impl MockServices {
        async fn start(host: &str, port: u16) -> MetaResult<Self> {
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

            sleep(Duration::from_secs(1)).await;

            let env = MetaSrvEnv::for_test_opts(Arc::new(MetaOpts::test(true))).await;
            let meta_metrics = Arc::new(MetaMetrics::new());
            let cluster_manager = Arc::new(
                ClusterManager::new(env.clone(), Duration::from_secs(3600), meta_metrics.clone())
                    .await?,
            );
            let host = HostAddress {
                host: host.to_string(),
                port: port as i32,
            };
            let fake_parallelism = 4;
            cluster_manager
                .add_worker_node(WorkerType::ComputeNode, host.clone(), fake_parallelism)
                .await?;
            cluster_manager.activate_worker_node(host).await?;

            let catalog_manager = Arc::new(CatalogManager::new(env.clone()).await?);
            let fragment_manager = Arc::new(FragmentManager::new(env.clone()).await?);
            let compaction_group_manager =
                Arc::new(CompactionGroupManager::new(env.clone()).await.unwrap());

            // TODO: what should we choose the task heartbeat interval to be? Anyway, we don't run a
            // heartbeat thread here, so it doesn't matter.
            let compactor_manager = Arc::new(
                CompactorManager::new_with_meta(env.clone(), 1)
                    .await
                    .unwrap(),
            );

            let hummock_manager = Arc::new(
                HummockManager::new(
                    env.clone(),
                    cluster_manager.clone(),
                    meta_metrics.clone(),
                    compaction_group_manager.clone(),
                    compactor_manager.clone(),
                )
                .await?,
            );

            let (barrier_scheduler, scheduled_barriers) =
                BarrierScheduler::new_pair(hummock_manager.clone());

            let compaction_group_manager =
                Arc::new(CompactionGroupManager::new(env.clone()).await?);

            let source_manager = Arc::new(
                SourceManager::new(
                    env.clone(),
                    cluster_manager.clone(),
                    barrier_scheduler.clone(),
                    catalog_manager.clone(),
                    fragment_manager.clone(),
                    compaction_group_manager.clone(),
                )
                .await?,
            );

            let barrier_manager = Arc::new(GlobalBarrierManager::new(
                scheduled_barriers,
                env.clone(),
                cluster_manager.clone(),
                catalog_manager.clone(),
                fragment_manager.clone(),
                hummock_manager,
                source_manager.clone(),
                meta_metrics.clone(),
            ));

            let stream_manager = GlobalStreamManager::new(
                env.clone(),
                fragment_manager.clone(),
                barrier_scheduler.clone(),
                cluster_manager.clone(),
                source_manager.clone(),
                compaction_group_manager.clone(),
            )?;

            let (join_handle_2, shutdown_tx_2) = GlobalBarrierManager::start(barrier_manager).await;

            Ok(Self {
                global_stream_manager: stream_manager,
                fragment_manager,
                state,
                join_handle_shutdown_txs: vec![
                    (join_handle_2, shutdown_tx_2),
                    (join_handle, shutdown_tx),
                ],
            })
        }

        async fn stop(self) {
            for (join_handle, shutdown_tx) in self.join_handle_shutdown_txs {
                shutdown_tx.send(()).unwrap();
                join_handle.await.unwrap();
            }
        }
    }

    fn make_mview_stream_actors(table_id: &TableId, count: usize) -> Vec<StreamActor> {
        (0..count)
            .map(|i| StreamActor {
                actor_id: i as u32,
                // A dummy node to avoid panic.
                nodes: Some(StreamNode {
                    node_body: Some(NodeBody::Materialize(MaterializeNode {
                        table_id: table_id.table_id(),
                        ..Default::default()
                    })),
                    operator_id: 1,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .collect_vec()
    }

    #[tokio::test]
    async fn test_create_materialized_view() -> MetaResult<()> {
        let services = MockServices::start("127.0.0.1", 12333).await?;

        let table_id = TableId::new(0);
        let actors = make_mview_stream_actors(&table_id, 5);

        let mut fragments = BTreeMap::default();
        fragments.insert(
            0,
            Fragment {
                fragment_id: 0,
                fragment_type: FragmentType::Sink as i32,
                distribution_type: FragmentDistributionType::Hash as i32,
                actors: actors.clone(),
                ..Default::default()
            },
        );
        let mut table_fragments = TableFragments::new(table_id, fragments);
        let mut ctx = CreateMaterializedViewContext::default();

        services
            .global_stream_manager
            .create_materialized_view(&mut table_fragments, &mut ctx)
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
            scheduled_actor.vnode_bitmap.take().unwrap();
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

    #[tokio::test]
    async fn test_drop_materialized_view() -> MetaResult<()> {
        let services = MockServices::start("127.0.0.1", 12334).await?;

        let table_id = TableId::new(0);
        let actors = make_mview_stream_actors(&table_id, 5);

        let mut fragments = BTreeMap::default();
        fragments.insert(
            0,
            Fragment {
                fragment_id: 0,
                fragment_type: FragmentType::Sink as i32,
                distribution_type: FragmentDistributionType::Hash as i32,
                actors: actors.clone(),
                ..Default::default()
            },
        );
        let mut table_fragments = TableFragments::new(table_id, fragments);
        let mut ctx = CreateMaterializedViewContext::default();

        services
            .global_stream_manager
            .create_materialized_view(&mut table_fragments, &mut ctx)
            .await?;

        for actor in actors {
            let mut scheduled_actor = services
                .state
                .actor_streams
                .lock()
                .unwrap()
                .get(&actor.get_actor_id())
                .cloned()
                .unwrap();
            scheduled_actor.vnode_bitmap.take().unwrap();
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
                    port: 12334,
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

        // test drop materialized_view
        // the table_fragments will be deleted when barrier_manager run_command DropMaterializedView
        // via drop_table_fragments
        services
            .global_stream_manager
            .drop_materialized_view(&table_fragments.table_id())
            .await?;

        // test get table_fragment;
        let select_err_1 = services
            .global_stream_manager
            .fragment_manager
            .select_table_fragments_by_table_id(&table_fragments.table_id())
            .await
            .unwrap_err();

        // TODO: check memory and metastore consistent
        assert_eq!(select_err_1.to_string(), "table_fragment not exist: id=0");

        services.stop().await;
        Ok(())
    }

    #[tokio::test]
    #[cfg(all(test, feature = "failpoints"))]
    async fn test_failpoints_drop_mv_recovery() {
        let inject_barrier_err = "inject_barrier_err";
        let inject_barrier_err_success = "inject_barrier_err_success";
        let services = MockServices::start("127.0.0.1", 12335).await.unwrap();

        let table_id = TableId::new(0);
        let actors = make_mview_stream_actors(&table_id, 5);

        let mut fragments = BTreeMap::default();
        fragments.insert(
            0,
            Fragment {
                fragment_id: 0,
                fragment_type: FragmentType::Sink as i32,
                distribution_type: FragmentDistributionType::Hash as i32,
                actors: actors.clone(),
                ..Default::default()
            },
        );

        let mut table_fragments = TableFragments::new(table_id, fragments);
        let mut ctx = CreateMaterializedViewContext::default();

        services
            .global_stream_manager
            .create_materialized_view(&mut table_fragments, &mut ctx)
            .await
            .unwrap();

        for actor in actors {
            let mut scheduled_actor = services
                .state
                .actor_streams
                .lock()
                .unwrap()
                .get(&actor.get_actor_id())
                .cloned()
                .unwrap();
            scheduled_actor.vnode_bitmap.take().unwrap();
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
                    port: 12335,
                }
            );
        }

        let sink_actor_ids = services
            .fragment_manager
            .get_table_sink_actor_ids(&table_id)
            .await
            .unwrap();
        let actor_ids = services
            .fragment_manager
            .get_table_actor_ids(&table_id)
            .await
            .unwrap();
        assert_eq!(sink_actor_ids, (0..5).collect::<Vec<u32>>());
        assert_eq!(actor_ids, (0..5).collect::<Vec<u32>>());
        let notify = Arc::new(Notify::new());
        let notify1 = notify.clone();

        // test recovery.
        fail::cfg(inject_barrier_err, "return").unwrap();
        tokio::spawn(async move {
            fail::cfg_callback(inject_barrier_err_success, move || {
                fail::remove(inject_barrier_err);
                fail::remove(inject_barrier_err_success);
                notify.notify_one();
            })
            .unwrap();
        });
        notify1.notified().await;

        let table_fragments = services
            .global_stream_manager
            .fragment_manager
            .select_table_fragments_by_table_id(&table_id)
            .await
            .unwrap();
        assert_eq!(table_fragments.actor_ids(), (0..5).collect_vec());

        // test drop materialized_view
        services
            .global_stream_manager
            .drop_materialized_view(&table_fragments.table_id())
            .await
            .unwrap();

        // test get table_fragment;
        let select_err_1 = services
            .global_stream_manager
            .fragment_manager
            .select_table_fragments_by_table_id(&table_fragments.table_id())
            .await
            .unwrap_err();

        // TODO: check memory and metastore consistent
        assert_eq!(select_err_1.to_string(), "table_fragment not exist: id=0");

        services.stop().await;
    }
}
