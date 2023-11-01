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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::future::try_join_all;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::ActorMapping;
use risingwave_connector::source::SplitImpl;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::meta::PausedReason;
use risingwave_pb::source::{ConnectorSplit, ConnectorSplits};
use risingwave_pb::stream_plan::barrier::{BarrierKind, Mutation};
use risingwave_pb::stream_plan::update_mutation::*;
use risingwave_pb::stream_plan::{
    AddMutation, Dispatcher, Dispatchers, PauseMutation, ResumeMutation, SourceChangeSplitMutation,
    StopMutation, UpdateMutation,
};
use risingwave_pb::stream_service::{DropActorsRequest, WaitEpochCommitRequest};
use risingwave_rpc_client::StreamClientPoolRef;
use uuid::Uuid;

use super::info::BarrierActorInfo;
use super::trace::TracedEpoch;
use crate::barrier::CommandChanges;
use crate::hummock::HummockManagerRef;
use crate::manager::{CatalogManagerRef, FragmentManagerRef, WorkerId};
use crate::model::{ActorId, DispatcherId, FragmentId, TableFragments};
use crate::stream::{build_actor_connector_splits, SourceManagerRef, SplitAssignment};
use crate::MetaResult;

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
    ///
    /// This field exists only when there's upstream fragment and the current fragment is
    /// hash-sharded.
    pub upstream_dispatcher_mapping: Option<ActorMapping>,

    /// The downstream fragments of this fragment.
    pub downstream_fragment_ids: Vec<FragmentId>,

    /// Reassigned splits for source actors
    pub actor_splits: HashMap<ActorId, Vec<SplitImpl>>,
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

    /// `Pause` command generates a `Pause` barrier with the provided [`PausedReason`] **only if**
    /// the cluster is not already paused. Otherwise, a barrier with no mutation will be generated.
    Pause(PausedReason),

    /// `Resume` command generates a `Resume` barrier with the provided [`PausedReason`] **only
    /// if** the cluster is paused with the same reason. Otherwise, a barrier with no mutation
    /// will be generated.
    Resume(PausedReason),

    /// `DropStreamingJobs` command generates a `Stop` barrier by the given
    /// [`HashSet<TableId>`]. The catalog has ensured that these streaming jobs are safe to be
    /// dropped by reference counts before.
    ///
    /// Barriers from the actors to be dropped will STILL be collected.
    /// After the barrier is collected, it notifies the local stream manager of compute nodes to
    /// drop actors, and then delete the table fragments info from meta store.
    DropStreamingJobs(HashSet<TableId>),

    /// `CreateStreamingJob` command generates a `Add` barrier by given info.
    ///
    /// Barriers from the actors to be created, which is marked as `Inactive` at first, will STILL
    /// be collected since the barrier should be passthrough.
    ///
    /// After the barrier is collected, these newly created actors will be marked as `Running`. And
    /// it adds the table fragments info to meta store. However, the creating progress will **last
    /// for a while** until the `finish` channel is signaled, then the state of `TableFragments`
    /// will be set to `Created`.
    CreateStreamingJob {
        table_fragments: TableFragments,
        upstream_mview_actors: HashMap<TableId, Vec<ActorId>>,
        dispatchers: HashMap<ActorId, Vec<Dispatcher>>,
        init_split_assignment: SplitAssignment,
        definition: String,
    },
    /// `CancelStreamingJob` command generates a `Stop` barrier including the actors of the given
    /// table fragment.
    ///
    /// The collecting and cleaning part works exactly the same as `DropStreamingJobs` command.
    CancelStreamingJob(TableFragments),

    /// `Reschedule` command generates a `Update` barrier by the [`Reschedule`] of each fragment.
    /// Mainly used for scaling and migration.
    ///
    /// Barriers from which actors should be collected, and the post behavior of this command are
    /// very similar to `Create` and `Drop` commands, for added and removed actors, respectively.
    RescheduleFragment {
        reschedules: HashMap<FragmentId, Reschedule>,
    },

    /// `ReplaceTable` command generates a `Update` barrier with the given `merge_updates`. This is
    /// essentially switching the downstream of the old table fragments to the new ones, and
    /// dropping the old table fragments. Used for table schema change.
    ///
    /// This can be treated as a special case of `RescheduleFragment`, while the upstream fragment
    /// of the Merge executors are changed additionally.
    ReplaceTable {
        old_table_fragments: TableFragments,
        new_table_fragments: TableFragments,
        merge_updates: Vec<MergeUpdate>,
        dispatchers: HashMap<ActorId, Vec<Dispatcher>>,
        init_split_assignment: SplitAssignment,
    },

    /// `SourceSplitAssignment` generates Plain(Mutation::Splits) for pushing initialized splits or
    /// newly added splits.
    SourceSplitAssignment(SplitAssignment),
}

impl Command {
    pub fn barrier() -> Self {
        Self::Plain(None)
    }

    pub fn pause(reason: PausedReason) -> Self {
        Self::Pause(reason)
    }

    pub fn resume(reason: PausedReason) -> Self {
        Self::Resume(reason)
    }

    /// Changes to the actors to be sent or collected after this command is committed.
    pub fn changes(&self) -> CommandChanges {
        match self {
            Command::Plain(_) => CommandChanges::None,
            Command::Pause(_) => CommandChanges::None,
            Command::Resume(_) => CommandChanges::None,
            Command::CreateStreamingJob {
                table_fragments, ..
            } => CommandChanges::CreateTable(table_fragments.table_id()),
            Command::DropStreamingJobs(table_ids) => CommandChanges::DropTables(table_ids.clone()),
            Command::CancelStreamingJob(table_fragments) => {
                CommandChanges::DropTables(std::iter::once(table_fragments.table_id()).collect())
            }
            Command::RescheduleFragment { reschedules, .. } => {
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
            Command::ReplaceTable {
                old_table_fragments,
                new_table_fragments,
                ..
            } => {
                let to_add = new_table_fragments.actor_ids().into_iter().collect();
                let to_remove = old_table_fragments.actor_ids().into_iter().collect();
                CommandChanges::Actor { to_add, to_remove }
            }
            Command::SourceSplitAssignment(_) => CommandChanges::None,
        }
    }

    /// If we need to send a barrier to modify actor configuration, we will pause the barrier
    /// injection. return true.
    pub fn should_pause_inject_barrier(&self) -> bool {
        // Note: the meaning for `Pause` is not pausing the periodic barrier injection, but for
        // pausing the sources on compute nodes. However, when `Pause` is used for configuration
        // change like scaling and migration, it must pause the concurrent checkpoint to ensure the
        // previous checkpoint has been done.
        matches!(self, Self::Pause(PausedReason::ConfigChange))
    }

    pub fn need_checkpoint(&self) -> bool {
        // todo! Reviewing the flow of different command to reduce the amount of checkpoint
        !matches!(self, Command::Plain(None) | Command::Resume(_))
    }
}

/// [`CommandContext`] is used for generating barrier and doing post stuffs according to the given
/// [`Command`].
pub struct CommandContext {
    pub fragment_manager: FragmentManagerRef,
    catalog_manager: CatalogManagerRef,
    hummock_manager: HummockManagerRef,

    client_pool: StreamClientPoolRef,

    /// Resolved info in this barrier loop.
    // TODO: this could be stale when we are calling `post_collect`, check if it matters
    pub info: Arc<BarrierActorInfo>,

    pub prev_epoch: TracedEpoch,
    pub curr_epoch: TracedEpoch,

    pub current_paused_reason: Option<PausedReason>,

    pub command: Command,

    pub kind: BarrierKind,

    source_manager: SourceManagerRef,

    /// The tracing span of this command.
    ///
    /// Differs from [`TracedEpoch`], this span focuses on the lifetime of the corresponding
    /// barrier, including the process of waiting for the barrier to be sent, flowing through the
    /// stream graph on compute nodes, and finishing its `post_collect` stuffs.
    pub span: tracing::Span,
}

impl CommandContext {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        fragment_manager: FragmentManagerRef,
        catalog_manager: CatalogManagerRef,
        hummock_manager: HummockManagerRef,
        client_pool: StreamClientPoolRef,
        info: BarrierActorInfo,
        prev_epoch: TracedEpoch,
        curr_epoch: TracedEpoch,
        current_paused_reason: Option<PausedReason>,
        command: Command,
        kind: BarrierKind,
        source_manager: SourceManagerRef,
        span: tracing::Span,
    ) -> Self {
        Self {
            fragment_manager,
            catalog_manager,
            hummock_manager,
            client_pool,
            info: Arc::new(info),
            prev_epoch,
            curr_epoch,
            current_paused_reason,
            command,
            kind,
            source_manager,
            span,
        }
    }
}

impl CommandContext {
    /// Generate a mutation for the given command.
    pub async fn to_mutation(&self) -> MetaResult<Option<Mutation>> {
        let mutation = match &self.command {
            Command::Plain(mutation) => mutation.clone(),

            Command::Pause(_) => {
                // Only pause when the cluster is not already paused.
                if self.current_paused_reason.is_none() {
                    Some(Mutation::Pause(PauseMutation {}))
                } else {
                    None
                }
            }

            Command::Resume(reason) => {
                // Only resume when the cluster is paused with the same reason.
                if self.current_paused_reason == Some(*reason) {
                    Some(Mutation::Resume(ResumeMutation {}))
                } else {
                    None
                }
            }

            Command::SourceSplitAssignment(change) => {
                let mut diff = HashMap::new();

                for actor_splits in change.values() {
                    diff.extend(actor_splits.clone());
                }

                Some(Mutation::Splits(SourceChangeSplitMutation {
                    actor_splits: build_actor_connector_splits(&diff),
                }))
            }

            Command::DropStreamingJobs(table_ids) => {
                let actors = self.fragment_manager.get_table_actor_ids(table_ids).await?;
                Some(Mutation::Stop(StopMutation { actors }))
            }

            Command::CreateStreamingJob {
                table_fragments,
                dispatchers,
                init_split_assignment: split_assignment,
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
                let added_actors = table_fragments.actor_ids();
                let actor_splits = split_assignment
                    .values()
                    .flat_map(build_actor_connector_splits)
                    .collect();
                Some(Mutation::Add(AddMutation {
                    actor_dispatchers,
                    added_actors,
                    actor_splits,
                    // If the cluster is already paused, the new actors should be paused too.
                    pause: self.current_paused_reason.is_some(),
                }))
            }

            Command::CancelStreamingJob(table_fragments) => {
                let actors = table_fragments.actor_ids();
                Some(Mutation::Stop(StopMutation { actors }))
            }

            Command::ReplaceTable {
                old_table_fragments,
                merge_updates,
                dispatchers,
                init_split_assignment,
                ..
            } => {
                let dropped_actors = old_table_fragments.actor_ids();

                let actor_new_dispatchers = dispatchers
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

                let actor_splits = init_split_assignment
                    .values()
                    .flat_map(build_actor_connector_splits)
                    .collect();

                Some(Mutation::Update(UpdateMutation {
                    actor_new_dispatchers,
                    merge_update: merge_updates.clone(),
                    dropped_actors,
                    actor_splits,
                    ..Default::default()
                }))
            }

            Command::RescheduleFragment { reschedules, .. } => {
                let mut dispatcher_update = HashMap::new();
                for reschedule in reschedules.values() {
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
                                            .as_ref()
                                            .map(|m| m.to_protobuf()),
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
                for (&fragment_id, reschedule) in reschedules {
                    for &downstream_fragment_id in &reschedule.downstream_fragment_ids {
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
                                        new_upstream_fragment_id: None,
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
                for reschedule in reschedules.values() {
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

                let mut actor_splits = HashMap::new();

                for reschedule in reschedules.values() {
                    for (actor_id, splits) in &reschedule.actor_splits {
                        actor_splits.insert(
                            *actor_id as ActorId,
                            ConnectorSplits {
                                splits: splits.iter().map(ConnectorSplit::from).collect(),
                            },
                        );
                    }
                }

                // we don't create dispatchers in reschedule scenario
                let actor_new_dispatchers = HashMap::new();

                let mutation = Mutation::Update(UpdateMutation {
                    dispatcher_update,
                    merge_update,
                    actor_vnode_bitmap_update,
                    dropped_actors,
                    actor_splits,
                    actor_new_dispatchers,
                });
                tracing::debug!("update mutation: {mutation:#?}");
                Some(mutation)
            }
        };

        Ok(mutation)
    }

    /// Returns the paused reason after executing the current command.
    pub fn next_paused_reason(&self) -> Option<PausedReason> {
        match &self.command {
            Command::Pause(reason) => {
                // Only pause when the cluster is not already paused.
                if self.current_paused_reason.is_none() {
                    Some(*reason)
                } else {
                    self.current_paused_reason
                }
            }

            Command::Resume(reason) => {
                // Only resume when the cluster is paused with the same reason.
                if self.current_paused_reason == Some(*reason) {
                    None
                } else {
                    self.current_paused_reason
                }
            }

            _ => self.current_paused_reason,
        }
    }

    /// For `CreateStreamingJob`, returns the actors of the `Chain` nodes. For other commands,
    /// returns an empty set.
    pub fn actors_to_track(&self) -> HashSet<ActorId> {
        match &self.command {
            Command::CreateStreamingJob {
                dispatchers,
                table_fragments,
                ..
            } => dispatchers
                .values()
                .flatten()
                .flat_map(|dispatcher| dispatcher.downstream_actor_id.iter().copied())
                .chain(table_fragments.values_actor_ids())
                .collect(),
            _ => Default::default(),
        }
    }

    /// For `CancelStreamingJob`, returns the actors of the `Chain` nodes. For other commands,
    /// returns an empty set.
    pub fn actors_to_cancel(&self) -> HashSet<ActorId> {
        match &self.command {
            Command::CancelStreamingJob(table_fragments) => table_fragments.chain_actor_ids(),
            _ => Default::default(),
        }
    }

    /// For `CancelStreamingJob`, returns the table id of the target table.
    pub fn table_to_cancel(&self) -> Option<TableId> {
        match &self.command {
            Command::CancelStreamingJob(table_fragments) => Some(table_fragments.table_id()),
            _ => None,
        }
    }

    /// For `CreateStreamingJob`, returns the table id of the target table.
    pub fn table_to_create(&self) -> Option<TableId> {
        match &self.command {
            Command::CreateStreamingJob {
                table_fragments, ..
            } => Some(table_fragments.table_id()),
            _ => None,
        }
    }

    /// Clean up actors in CNs if needed, used by drop, cancel and reschedule commands.
    async fn clean_up(
        &self,
        actors_to_clean: impl IntoIterator<Item = (WorkerId, Vec<ActorId>)>,
    ) -> MetaResult<()> {
        let futures = actors_to_clean.into_iter().map(|(node_id, actors)| {
            let node = self.info.node_map.get(&node_id).unwrap();
            let request_id = Uuid::new_v4().to_string();

            async move {
                let client = self.client_pool.get(node).await?;
                let request = DropActorsRequest {
                    request_id,
                    actor_ids: actors.to_owned(),
                };
                client.drop_actors(request).await
            }
        });

        try_join_all(futures).await?;
        Ok(())
    }

    pub async fn wait_epoch_commit(&self, epoch: HummockEpoch) -> MetaResult<()> {
        let futures = self.info.node_map.values().map(|worker_node| async {
            let client = self.client_pool.get(worker_node).await?;
            let request = WaitEpochCommitRequest { epoch };
            client.wait_epoch_commit(request).await
        });

        try_join_all(futures).await?;

        Ok(())
    }

    /// Do some stuffs after barriers are collected and the new storage version is committed, for
    /// the given command.
    pub async fn post_collect(&self) -> MetaResult<()> {
        match &self.command {
            Command::Plain(_) => {}

            Command::Pause(reason) => {
                if let PausedReason::ConfigChange = reason {
                    // After the `Pause` barrier is collected and committed, we must ensure that the
                    // storage version with this epoch is synced to all compute nodes before the
                    // execution of the next command of `Update`, as some newly created operators
                    // may immediately initialize their states on that barrier.
                    self.wait_epoch_commit(self.prev_epoch.value().0).await?;
                }
            }

            Command::Resume(_) => {}

            Command::SourceSplitAssignment(split_assignment) => {
                self.fragment_manager
                    .update_actor_splits_by_split_assignment(split_assignment)
                    .await?;
                self.source_manager
                    .apply_source_change(None, Some(split_assignment.clone()), None)
                    .await;
            }

            Command::DropStreamingJobs(table_ids) => {
                // Tell compute nodes to drop actors.
                let node_actors = self.fragment_manager.table_node_actors(table_ids).await?;
                self.clean_up(node_actors).await?;
                // Drop fragment info in meta store.
                self.fragment_manager
                    .drop_table_fragments_vec(table_ids)
                    .await?;
            }

            Command::CancelStreamingJob(table_fragments) => {
                tracing::debug!(id = ?table_fragments.table_id(), "cancelling stream job");
                let node_actors = table_fragments.worker_actor_ids();
                self.clean_up(node_actors).await?;

                // NOTE(kwannoel): At this point, meta has already registered the table ids.
                // We should unregister them.
                // This is required for background ddl, for foreground ddl this is a no-op.
                // Foreground ddl is handled entirely by stream manager, so it will unregister
                // the table ids on failure.
                // On the other hand background ddl could be handled by barrier manager.
                // It won't clean the tables on failure,
                // since the failure could be recoverable.
                // As such it needs to be handled here.
                let table_id = table_fragments.table_id().table_id;
                let mut table_ids = table_fragments.internal_table_ids();
                table_ids.push(table_id);
                if let Err(e) = self.hummock_manager.unregister_table_ids(&table_ids).await {
                    tracing::warn!("Failed to unregister compaction group for {:#?}. They will be cleaned up on node restart. {:#?}", &table_ids, e);
                }

                // NOTE(kwannoel): At this point, catalog manager has persisted the tables already.
                // We need to cleanup the table state. So we can do it here.
                // The logic is the same as above, for hummock_manager.unregister_table_ids.
                if let Err(e) = self
                    .catalog_manager
                    .cancel_create_table_procedure(
                        table_fragments.table_id().table_id,
                        table_fragments.internal_table_ids(),
                    )
                    .await
                {
                    let table_id = table_fragments.table_id().table_id;
                    tracing::warn!(
                        table_id,
                        reason=?e,
                        "cancel_create_table_procedure failed for CancelStreamingJob",
                    );
                    // If failed, check that table is not in meta store.
                    // If any table is, just panic, let meta do bootstrap recovery.
                    // Otherwise our persisted state is dirty.
                    let mut table_ids = table_fragments.internal_table_ids();
                    table_ids.push(table_id);
                    self.catalog_manager.assert_tables_deleted(table_ids).await;
                }

                // We need to drop table fragments here,
                // since this is not done in stream manager (foreground ddl)
                // OR barrier manager (background ddl)
                self.fragment_manager
                    .drop_table_fragments_vec(&HashSet::from_iter(std::iter::once(
                        table_fragments.table_id(),
                    )))
                    .await?;
            }

            Command::CreateStreamingJob {
                table_fragments,
                dispatchers,
                upstream_mview_actors,
                init_split_assignment,
                ..
            } => {
                let mut dependent_table_actors = Vec::with_capacity(upstream_mview_actors.len());
                for (table_id, actors) in upstream_mview_actors {
                    let downstream_actors = dispatchers
                        .iter()
                        .filter(|(upstream_actor_id, _)| actors.contains(upstream_actor_id))
                        .map(|(&k, v)| (k, v.clone()))
                        .collect();
                    dependent_table_actors.push((*table_id, downstream_actors));
                }
                self.fragment_manager
                    .post_create_table_fragments(
                        &table_fragments.table_id(),
                        dependent_table_actors,
                        init_split_assignment.clone(),
                    )
                    .await?;

                // Extract the fragments that include source operators.
                let source_fragments = table_fragments.stream_source_fragments();

                self.source_manager
                    .apply_source_change(
                        Some(source_fragments),
                        Some(init_split_assignment.clone()),
                        None,
                    )
                    .await;
            }

            Command::RescheduleFragment { reschedules } => {
                let mut node_dropped_actors = HashMap::new();
                for table_fragments in self
                    .fragment_manager
                    .get_fragment_read_guard()
                    .await
                    .table_fragments()
                    .values()
                {
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
                self.clean_up(node_dropped_actors).await?;

                // Update fragment info after rescheduling in meta store.
                self.fragment_manager
                    .post_apply_reschedules(reschedules.clone())
                    .await?;

                let mut stream_source_actor_splits = HashMap::new();
                let mut stream_source_dropped_actors = HashSet::new();

                for (fragment_id, reschedule) in reschedules {
                    if !reschedule.actor_splits.is_empty() {
                        stream_source_actor_splits
                            .insert(*fragment_id as FragmentId, reschedule.actor_splits.clone());
                        stream_source_dropped_actors.extend(reschedule.removed_actors.clone());
                    }
                }

                if !stream_source_actor_splits.is_empty() {
                    self.source_manager
                        .apply_source_change(
                            None,
                            Some(stream_source_actor_splits),
                            Some(stream_source_dropped_actors),
                        )
                        .await;
                }
            }

            Command::ReplaceTable {
                old_table_fragments,
                new_table_fragments,
                merge_updates,
                dispatchers,
                ..
            } => {
                let table_ids = HashSet::from_iter(std::iter::once(old_table_fragments.table_id()));

                // Tell compute nodes to drop actors.
                let node_actors = self.fragment_manager.table_node_actors(&table_ids).await?;
                self.clean_up(node_actors).await?;

                // Drop fragment info in meta store.
                self.fragment_manager
                    .post_replace_table(
                        old_table_fragments,
                        new_table_fragments,
                        merge_updates,
                        dispatchers,
                    )
                    .await?;
            }
        }

        Ok(())
    }
}
