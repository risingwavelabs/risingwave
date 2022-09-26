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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::future::try_join_all;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;
use risingwave_connector::source::SplitImpl;
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use risingwave_pb::stream_plan::add_mutation::Dispatchers;
use risingwave_pb::stream_plan::barrier::Mutation;
use risingwave_pb::stream_plan::update_mutation::*;
use risingwave_pb::stream_plan::{
    ActorMapping, AddMutation, Dispatcher, PauseMutation, ResumeMutation, StopMutation,
    UpdateMutation,
};
use risingwave_pb::stream_service::DropActorsRequest;
use risingwave_rpc_client::StreamClientPoolRef;
use uuid::Uuid;

use super::info::BarrierActorInfo;
use crate::barrier::CommandChanges;
use crate::manager::{FragmentManagerRef, WorkerId};
use crate::model::{ActorId, DispatcherId, FragmentId, TableFragments};
use crate::storage::MetaStore;
use crate::{MetaError, MetaResult};

/// [`Reschedule`] is for the [`Command::RescheduleFragment`], which is used for rescheduling actors
/// in some fragment, like scaling or migrating.
#[derive(Debug, Clone)]
pub struct Reschedule {
    /// Added actors in this fragment.
    pub added_actors: Vec<ActorId>,
    /// Removed actors in this fragment.
    pub removed_actors: Vec<ActorId>,

    /// Vnode bitmap updates for some actors in this fragment.
    pub vnode_bitmap_updates: HashMap<ActorId, Bitmap>,

    /// The upstream fragments of this fragment, and the dispatchers that should be updated.
    pub upstream_fragment_dispatcher_ids: Vec<(FragmentId, DispatcherId)>,
    /// New hash mapping of the upstream dispatcher to be updated.
    pub upstream_dispatcher_mapping: Option<ActorMapping>,

    /// The downstream fragments of this fragment.
    pub downstream_fragment_id: Option<FragmentId>,
}

/// [`Command`] is the action of [`crate::barrier::GlobalBarrierManager`]. For different commands,
/// we'll build different barriers to send, and may do different stuffs after the barrier is
/// collected.
#[derive(Debug, Clone)]
pub enum Command {
    /// `Plain` command generates a barrier with the mutation it carries.
    ///
    /// Barriers from all actors marked as `Created` state will be collected.
    /// After the barrier is collected, it does nothing.
    Plain(Option<Mutation>),

    /// `DropMaterializedView` command generates a `Stop` barrier by the given [`TableId`]. The
    /// catalog has ensured that this materialized view is safe to be dropped by reference counts
    /// before.
    ///
    /// Barriers from the actors to be dropped will STILL be collected.
    /// After the barrier is collected, it notifies the local stream manager of compute nodes to
    /// drop actors, and then delete the table fragments info from meta store.
    DropMaterializedView(TableId),

    /// `CreateMaterializedView` command generates a `Add` barrier by given info.
    ///
    /// Barriers from the actors to be created, which is marked as `Creating` at first, will STILL
    /// be collected since the barrier should be passthroughed.
    /// After the barrier is collected, these newly created actors will be marked as `Created`. And
    /// it adds the table fragments info to meta store. However, the creating progress will last
    /// for a while until the `finish` channel is signaled.
    CreateMaterializedView {
        table_fragments: TableFragments,
        table_sink_map: HashMap<TableId, Vec<ActorId>>,
        dispatchers: HashMap<ActorId, Vec<Dispatcher>>,
        source_state: HashMap<ActorId, Vec<SplitImpl>>,
    },

    /// `Reschedule` command generates a `Update` barrier by the [`Reschedule`] of each fragment.
    /// Mainly used for scaling and migration.
    ///
    /// Barriers from which actors should be collected, and the post behavior of this command are
    /// very similar to `Create` and `Drop` commands, for added and removed actors, respectively.
    RescheduleFragment(HashMap<FragmentId, Reschedule>),
}

impl Command {
    pub fn checkpoint() -> Self {
        Self::Plain(None)
    }

    pub fn pause() -> Self {
        Self::Plain(Some(Mutation::Pause(PauseMutation {})))
    }

    pub fn resume() -> Self {
        Self::Plain(Some(Mutation::Resume(ResumeMutation {})))
    }

    /// Changes to the actors to be sent or collected after this command is committed.
    pub fn changes(&self) -> CommandChanges {
        match self {
            Command::Plain(_) => CommandChanges::None,
            Command::CreateMaterializedView {
                table_fragments, ..
            } => CommandChanges::CreateTable(table_fragments.table_id()),
            Command::DropMaterializedView(table_id) => CommandChanges::DropTable(*table_id),
            Command::RescheduleFragment(reschedules) => {
                let to_add = reschedules
                    .values()
                    .flat_map(|r| r.added_actors.iter().copied())
                    .collect();
                let to_remove = reschedules
                    .values()
                    .flat_map(|r| r.removed_actors.iter().copied())
                    .collect();
                CommandChanges::Actor { to_add, to_remove }
            }
        }
    }

    /// If we need to send a barrier to modify actor configuration, we will pause the barrier
    /// injection. return true.
    pub fn should_pause_inject_barrier(&self) -> bool {
        // Note: the meaning for `Pause` is not pausing the periodic barrier injection, but for
        // pausing the sources on compute nodes. However, `Pause` is used for configuration change
        // like scaling and migration, which must pause the concurrent checkpoint to ensure the
        // previous checkpoint has been done.
        matches!(self, Self::Plain(Some(Mutation::Pause(_))))
    }
}

/// [`CommandContext`] is used for generating barrier and doing post stuffs according to the given
/// [`Command`].
pub struct CommandContext<S: MetaStore> {
    fragment_manager: FragmentManagerRef<S>,

    client_pool: StreamClientPoolRef,

    /// Resolved info in this barrier loop.
    // TODO: this could be stale when we are calling `post_collect`, check if it matters
    pub info: Arc<BarrierActorInfo>,

    pub prev_epoch: Epoch,
    pub curr_epoch: Epoch,

    pub command: Command,
}

impl<S: MetaStore> CommandContext<S> {
    pub fn new(
        fragment_manager: FragmentManagerRef<S>,
        client_pool: StreamClientPoolRef,
        info: BarrierActorInfo,
        prev_epoch: Epoch,
        curr_epoch: Epoch,
        command: Command,
    ) -> Self {
        Self {
            fragment_manager,
            client_pool,
            info: Arc::new(info),
            prev_epoch,
            curr_epoch,
            command,
        }
    }
}

impl<S> CommandContext<S>
where
    S: MetaStore,
{
    /// Generate a mutation for the given command.
    pub async fn to_mutation(&self) -> MetaResult<Option<Mutation>> {
        let mutation = match &self.command {
            Command::Plain(mutation) => mutation.clone(),

            Command::DropMaterializedView(table_id) => {
                let actors = self.fragment_manager.get_table_actor_ids(table_id).await?;
                Some(Mutation::Stop(StopMutation { actors }))
            }

            Command::CreateMaterializedView {
                dispatchers,
                source_state,
                ..
            } => {
                let actor_dispatchers = dispatchers
                    .iter()
                    .map(|(&actor_id, dispatchers)| {
                        (
                            actor_id,
                            Dispatchers {
                                dispatchers: dispatchers.clone(),
                            },
                        )
                    })
                    .collect();
                let actor_splits = source_state
                    .iter()
                    .filter(|(_, splits)| !splits.is_empty())
                    .map(|(actor_id, splits)| {
                        (
                            *actor_id,
                            ConnectorSplits {
                                splits: splits.iter().map(ConnectorSplit::from).collect(),
                            },
                        )
                    })
                    .collect();

                Some(Mutation::Add(AddMutation {
                    actor_dispatchers,
                    actor_splits,
                }))
            }

            Command::RescheduleFragment(reschedules) => {
                let mut dispatcher_update = HashMap::new();
                for (_fragment_id, reschedule) in reschedules.iter() {
                    for &(upstream_fragment_id, dispatcher_id) in
                        &reschedule.upstream_fragment_dispatcher_ids
                    {
                        // Find the actors of the upstream fragment.
                        let upstream_actor_ids = self
                            .fragment_manager
                            .get_running_actors_of_fragment(upstream_fragment_id)
                            .await?;

                        // Record updates for all actors.
                        for actor_id in upstream_actor_ids {
                            // Index with the dispatcher id to check duplicates.
                            dispatcher_update
                                .try_insert(
                                    (actor_id, dispatcher_id),
                                    DispatcherUpdate {
                                        actor_id,
                                        dispatcher_id,
                                        hash_mapping: reschedule
                                            .upstream_dispatcher_mapping
                                            .clone(),
                                        added_downstream_actor_id: reschedule.added_actors.clone(),
                                        removed_downstream_actor_id: reschedule
                                            .removed_actors
                                            .clone(),
                                    },
                                )
                                .unwrap();
                        }
                    }
                }
                let dispatcher_update = dispatcher_update.into_values().collect();

                let mut merge_update = HashMap::new();
                for (&fragment_id, reschedule) in reschedules.iter() {
                    if let Some(downstream_fragment_id) = reschedule.downstream_fragment_id {
                        // Find the actors of the downstream fragment.
                        let downstream_actor_ids = self
                            .fragment_manager
                            .get_running_actors_of_fragment(downstream_fragment_id)
                            .await?;

                        // Downstream removed actors should be skipped
                        // Newly created actors of the current fragment will not dispatch Update
                        // barriers to them
                        let downstream_removed_actors: HashSet<_> = reschedules
                            .get(&downstream_fragment_id)
                            .map(|downstream_reschedule| {
                                downstream_reschedule
                                    .removed_actors
                                    .iter()
                                    .copied()
                                    .collect()
                            })
                            .unwrap_or_default();

                        // Record updates for all actors.
                        for actor_id in downstream_actor_ids {
                            if downstream_removed_actors.contains(&actor_id) {
                                continue;
                            }

                            // Index with the fragment id to check duplicates.
                            merge_update
                                .try_insert(
                                    (actor_id, fragment_id),
                                    MergeUpdate {
                                        actor_id,
                                        upstream_fragment_id: fragment_id,
                                        added_upstream_actor_id: reschedule.added_actors.clone(),
                                        removed_upstream_actor_id: reschedule
                                            .removed_actors
                                            .clone(),
                                    },
                                )
                                .unwrap();
                        }
                    }
                }
                let merge_update = merge_update.into_values().collect();

                let mut actor_vnode_bitmap_update = HashMap::new();
                for (_fragment_id, reschedule) in reschedules.iter() {
                    // Record updates for all actors in this fragment.
                    for (&actor_id, bitmap) in &reschedule.vnode_bitmap_updates {
                        let bitmap = bitmap.to_protobuf();
                        actor_vnode_bitmap_update
                            .try_insert(actor_id, bitmap)
                            .unwrap();
                    }
                }

                let dropped_actors = reschedules
                    .values()
                    .flat_map(|r| r.removed_actors.iter().copied())
                    .collect();

                let mutation = Mutation::Update(UpdateMutation {
                    dispatcher_update,
                    merge_update,
                    actor_vnode_bitmap_update,
                    dropped_actors,
                });
                tracing::trace!("update mutation: {mutation:#?}");
                Some(mutation)
            }
        };

        Ok(mutation)
    }

    /// For `CreateMaterializedView`, returns the actors of the `Chain` nodes. For other commands,
    /// returns an empty set.
    pub fn actors_to_track(&self) -> HashSet<ActorId> {
        match &self.command {
            Command::CreateMaterializedView { dispatchers, .. } => dispatchers
                .values()
                .flatten()
                .flat_map(|dispatcher| dispatcher.downstream_actor_id.iter().copied())
                .collect(),

            _ => Default::default(),
        }
    }

    /// Do some stuffs after barriers are collected, for the given command.
    pub async fn post_collect(&self) -> MetaResult<()> {
        match &self.command {
            Command::Plain(_) => {}

            Command::DropMaterializedView(table_id) => {
                // Tell compute nodes to drop actors.
                let node_actors = self.fragment_manager.table_node_actors(table_id).await?;
                let futures = node_actors.iter().map(|(node_id, actors)| {
                    let node = self.info.node_map.get(node_id).unwrap();
                    let request_id = Uuid::new_v4().to_string();

                    async move {
                        let client = self.client_pool.get(node).await?;
                        let request = DropActorsRequest {
                            request_id,
                            actor_ids: actors.to_owned(),
                        };
                        client.drop_actors(request).await?;

                        Ok::<_, MetaError>(())
                    }
                });

                try_join_all(futures).await?;

                // Drop fragment info in meta store.
                self.fragment_manager.drop_table_fragments(table_id).await?;
            }

            Command::CreateMaterializedView {
                table_fragments,
                dispatchers,
                table_sink_map,
                source_state: _,
            } => {
                let mut dependent_table_actors = Vec::with_capacity(table_sink_map.len());
                for (table_id, actors) in table_sink_map {
                    let downstream_actors = dispatchers
                        .iter()
                        .filter(|(upstream_actor_id, _)| actors.contains(upstream_actor_id))
                        .map(|(&k, v)| (k, v.clone()))
                        .collect();
                    dependent_table_actors.push((*table_id, downstream_actors));
                }
                self.fragment_manager
                    .finish_create_table_fragments(
                        &table_fragments.table_id(),
                        dependent_table_actors,
                    )
                    .await?;
            }

            Command::RescheduleFragment(reschedules) => {
                let mut node_dropped_actors = HashMap::new();
                for table_fragments in self.fragment_manager.list_table_fragments().await? {
                    for fragment_id in table_fragments.fragments.keys() {
                        if let Some(reschedule) = reschedules.get(fragment_id) {
                            for actor_id in &reschedule.removed_actors {
                                let node_id = table_fragments
                                    .actor_status
                                    .get(actor_id)
                                    .unwrap()
                                    .parallel_unit
                                    .as_ref()
                                    .unwrap()
                                    .worker_node_id;
                                node_dropped_actors
                                    .entry(node_id as WorkerId)
                                    .or_insert(vec![])
                                    .push(*actor_id as ActorId);
                            }
                        }
                    }
                }

                let drop_actor_futures =
                    node_dropped_actors.into_iter().map(|(node_id, actors)| {
                        let node = self.info.node_map.get(&node_id).unwrap();
                        let request_id = Uuid::new_v4().to_string();

                        async move {
                            let client = self.client_pool.get(node).await?;
                            let request = DropActorsRequest {
                                request_id,
                                actor_ids: actors.to_owned(),
                            };
                            client.drop_actors(request).await?;

                            Ok::<_, MetaError>(())
                        }
                    });

                try_join_all(drop_actor_futures).await?;

                // Update fragment info after rescheduling in meta store.
                self.fragment_manager
                    .post_apply_reschedules(reschedules.clone())
                    .await?;
            }
        }

        Ok(())
    }
}
