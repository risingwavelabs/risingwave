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

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;

use super::command::CommandContext;
use super::notifier::Notifier;
use crate::barrier::{
    Command, TableActorMap, TableDefinitionMap, TableFragmentMap, TableNotifierMap,
    TableUpstreamMvCountMap,
};
use crate::manager::{FragmentManager, FragmentManagerRef};
use crate::model::{ActorId, TableFragments};
use crate::MetaResult;

type ConsumedRows = u64;

#[derive(Clone, Copy, Debug)]
pub enum ChainState {
    Init,
    ConsumingUpstream(Epoch, ConsumedRows),
    Done(ConsumedRows),
}

/// Progress of all actors containing chain nodes while creating mview.
#[derive(Debug)]
struct Progress {
    states: HashMap<ActorId, ChainState>,

    done_count: usize,

    /// Upstream mv count.
    /// Keep track of how many times each upstream MV
    /// appears in this stream job.
    upstream_mv_count: HashMap<TableId, usize>,

    /// Upstream mvs total key count.
    upstream_total_key_count: u64,

    /// Consumed rows
    consumed_rows: u64,

    /// DDL definition
    definition: String,
}

impl Progress {
    /// Create a [`Progress`] for some creating mview, with all `actors` containing the chain nodes.
    fn new(
        actors: impl IntoIterator<Item = ActorId>,
        upstream_mv_count: HashMap<TableId, usize>,
        upstream_total_key_count: u64,
        definition: String,
    ) -> Self {
        let states = actors
            .into_iter()
            .map(|a| (a, ChainState::Init))
            .collect::<HashMap<_, _>>();
        assert!(!states.is_empty());

        Self {
            states,
            done_count: 0,
            upstream_mv_count,
            upstream_total_key_count,
            consumed_rows: 0,
            definition,
        }
    }

    /// Update the progress of `actor`.
    fn update(&mut self, actor: ActorId, new_state: ChainState, upstream_total_key_count: u64) {
        self.upstream_total_key_count = upstream_total_key_count;
        match self.states.remove(&actor).unwrap() {
            ChainState::Init => {}
            ChainState::ConsumingUpstream(_, old_consumed_rows) => {
                self.consumed_rows -= old_consumed_rows;
            }
            ChainState::Done(_) => panic!("should not report done multiple times"),
        };
        match &new_state {
            ChainState::Init => {}
            ChainState::ConsumingUpstream(_, new_consumed_rows) => {
                self.consumed_rows += new_consumed_rows;
            }
            ChainState::Done(new_consumed_rows) => {
                self.consumed_rows += new_consumed_rows;
                self.done_count += 1;
            }
        };
        self.states.insert(actor, new_state);
        self.calculate_progress();
    }

    /// Returns whether all chains are done.
    fn is_done(&self) -> bool {
        self.done_count == self.states.len()
    }

    /// Returns the ids of all actors containing the chain nodes for the mview tracked by this
    /// [`Progress`].
    fn actors(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.states.keys().cloned()
    }

    /// `progress` = `consumed_rows` / `upstream_total_key_count`
    fn calculate_progress(&self) -> f64 {
        if self.is_done() || self.states.is_empty() {
            return 1.0;
        }
        let mut upstream_total_key_count = self.upstream_total_key_count as f64;
        if upstream_total_key_count == 0.0 {
            upstream_total_key_count = 1.0
        }
        let mut progress = self.consumed_rows as f64 / upstream_total_key_count;
        if progress >= 1.0 {
            progress = 0.99;
        }
        progress
    }
}

/// There are 2 kinds of `TrackingJobs`:
/// 1. `New`. This refers to the "New" type of tracking job.
///    It is instantiated and managed by the stream manager.
///    On recovery, the stream manager will stop managing the job.
/// 2. `Recovered`. This refers to the "Recovered" type of tracking job.
///    On recovery, the barrier manager will recover and start managing the job.
pub enum TrackingJob {
    New(TrackingCommand),
    Recovered(RecoveredTrackingJob),
}

impl TrackingJob {
    fn fragment_manager(&self) -> &FragmentManager {
        match self {
            TrackingJob::New(command) => command.context.fragment_manager.as_ref(),
            TrackingJob::Recovered(recovered) => recovered.fragment_manager.as_ref(),
        }
    }

    /// Returns whether the `TrackingJob` requires a checkpoint to complete.
    pub(crate) fn is_checkpoint_required(&self) -> bool {
        match self {
            // Recovered tracking job is always a streaming job,
            // It requires a checkpoint to complete.
            TrackingJob::Recovered(_) => true,
            TrackingJob::New(command) => {
                command.context.kind.is_initial() || command.context.kind.is_checkpoint()
            }
        }
    }

    pub(crate) async fn pre_finish(&self) -> MetaResult<()> {
        let table_fragments = match &self {
            TrackingJob::New(command) => match &command.context.command {
                Command::CreateStreamingJob {
                    table_fragments, ..
                } => Some(table_fragments),
                _ => None,
            },
            TrackingJob::Recovered(recovered) => Some(&recovered.fragments),
        };
        // Update the state of the table fragments from `Creating` to `Created`, so that the
        // fragments can be scaled.
        if let Some(table_fragments) = table_fragments {
            self.fragment_manager()
                .mark_table_fragments_created(table_fragments.table_id())
                .await?;
        }
        Ok(())
    }

    pub(crate) fn notify_finished(self) {
        match self {
            TrackingJob::New(command) => {
                command
                    .notifiers
                    .into_iter()
                    .for_each(Notifier::notify_finished);
            }
            TrackingJob::Recovered(recovered) => {
                recovered.finished.notify_finished();
            }
        }
    }

    pub(crate) fn table_to_create(&self) -> Option<TableId> {
        match self {
            TrackingJob::New(command) => command.context.table_to_create(),
            TrackingJob::Recovered(recovered) => Some(recovered.fragments.table_id()),
        }
    }
}

pub struct RecoveredTrackingJob {
    pub fragments: TableFragments,
    pub finished: Notifier,
    pub fragment_manager: FragmentManagerRef,
}

/// The command tracking by the [`CreateMviewProgressTracker`].
pub(super) struct TrackingCommand {
    /// The context of the command.
    pub context: Arc<CommandContext>,

    /// Should be called when the command is finished.
    pub notifiers: Vec<Notifier>,
}

/// Track the progress of all creating mviews. When creation is done, `notify_finished` will be
/// called on registered notifiers.
///
/// Tracking is done as follows:
/// 1. We identify a `StreamJob` by its `TableId` of its `Materialized` table.
/// 2. For each stream job, there are several actors which run its tasks.
/// 3. With `progress_map` we can use the ID of the `StreamJob` to view its progress.
/// 4. With `actor_map` we can use an actor's `ActorId` to find the ID of the `StreamJob`.
pub(super) struct CreateMviewProgressTracker {
    /// Progress of the create-mview DDL indicated by the TableId.
    progress_map: HashMap<TableId, (Progress, TrackingJob)>,

    /// Find the epoch of the create-mview DDL by the actor containing the chain node.
    actor_map: HashMap<ActorId, TableId>,
}

impl CreateMviewProgressTracker {
    /// This step recovers state from the meta side:
    /// 1. `Tables`.
    /// 2. `TableFragments`.
    ///
    /// Other state are persisted by the `BackfillExecutor`, such as:
    /// 1. `CreateMviewProgress`.
    /// 2. `Backfill` position.
    pub fn recover(
        table_map: TableActorMap,
        mut upstream_mv_counts: TableUpstreamMvCountMap,
        mut definitions: TableDefinitionMap,
        version_stats: HummockVersionStats,
        mut finished_notifiers: TableNotifierMap,
        mut table_fragment_map: TableFragmentMap,
        fragment_manager: FragmentManagerRef,
    ) -> Self {
        let mut actor_map = HashMap::new();
        let mut progress_map = HashMap::new();
        let table_map: HashMap<_, Vec<ActorId>> = table_map.into();
        for (creating_table_id, actors) in table_map {
            // 1. Recover `ChainState` in the tracker.
            let mut states = HashMap::new();
            for actor in actors {
                actor_map.insert(actor, creating_table_id);
                states.insert(actor, ChainState::ConsumingUpstream(Epoch(0), 0));
            }
            let upstream_mv_count = upstream_mv_counts.remove(&creating_table_id).unwrap();
            let upstream_total_key_count = upstream_mv_count
                .iter()
                .map(|(upstream_mv, count)| {
                    *count as u64
                        * version_stats
                            .table_stats
                            .get(&upstream_mv.table_id)
                            .map_or(0, |stat| stat.total_key_count as u64)
                })
                .sum();
            let definition = definitions.remove(&creating_table_id).unwrap();
            let progress = Progress {
                states,
                done_count: 0, // Fill only after first barrier pass
                upstream_mv_count,
                upstream_total_key_count,
                consumed_rows: 0, // Fill only after first barrier pass
                definition,
            };
            let tracking_job = TrackingJob::Recovered(RecoveredTrackingJob {
                fragments: table_fragment_map.remove(&creating_table_id).unwrap(),
                finished: finished_notifiers.remove(&creating_table_id).unwrap(),
                fragment_manager: fragment_manager.clone(),
            });
            progress_map.insert(creating_table_id, (progress, tracking_job));
        }
        Self {
            progress_map,
            actor_map,
        }
    }

    pub fn new() -> Self {
        Self {
            progress_map: Default::default(),
            actor_map: Default::default(),
        }
    }

    pub fn gen_ddl_progress(&self) -> Vec<DdlProgress> {
        self.progress_map
            .iter()
            .map(|(table_id, (x, _))| DdlProgress {
                id: table_id.table_id as u64,
                statement: x.definition.clone(),
                progress: format!("{:.2}%", x.calculate_progress() * 100.0),
            })
            .collect()
    }

    /// Try to find the target create-streaming-job command from track.
    ///
    /// Return the target command as it should be cancelled based on the input actors.
    pub fn find_cancelled_command(
        &mut self,
        actors_to_cancel: HashSet<ActorId>,
    ) -> Option<TrackingJob> {
        let epochs = actors_to_cancel
            .into_iter()
            .map(|actor_id| self.actor_map.get(&actor_id))
            .collect_vec();
        assert!(epochs.iter().all_equal());
        // If the target command found in progress map, return and remove it. Note that the command
        // should have finished if not found.
        if let Some(Some(epoch)) = epochs.first() {
            Some(self.progress_map.remove(epoch).unwrap().1)
        } else {
            None
        }
    }

    /// Add a new create-mview DDL command to track.
    ///
    /// If the actors to track is empty, return the given command as it can be finished immediately.
    pub fn add(
        &mut self,
        command: TrackingCommand,
        version_stats: &HummockVersionStats,
    ) -> Option<TrackingJob> {
        let actors = command.context.actors_to_track();
        if actors.is_empty() {
            // The command can be finished immediately.
            return Some(TrackingJob::New(command));
        }

        let (creating_mv_id, upstream_mv_count, upstream_total_key_count, definition) =
            if let Command::CreateStreamingJob {
                table_fragments,
                dispatchers,
                upstream_mview_actors,
                definition,
                ..
            } = &command.context.command
            {
                // Keep track of how many times each upstream MV appears.
                let mut upstream_mv_count = HashMap::new();
                for (table_id, actors) in upstream_mview_actors {
                    assert!(!actors.is_empty());
                    let dispatch_count: usize = dispatchers
                        .iter()
                        .filter(|(upstream_actor_id, _)| actors.contains(upstream_actor_id))
                        .map(|(_, v)| v.len())
                        .sum();
                    upstream_mv_count.insert(*table_id, dispatch_count / actors.len());
                }

                let upstream_total_key_count: u64 = upstream_mv_count
                    .iter()
                    .map(|(upstream_mv, count)| {
                        *count as u64
                            * version_stats
                                .table_stats
                                .get(&upstream_mv.table_id)
                                .map_or(0, |stat| stat.total_key_count as u64)
                    })
                    .sum();
                (
                    table_fragments.table_id(),
                    upstream_mv_count,
                    upstream_total_key_count,
                    definition.to_string(),
                )
            } else {
                unreachable!("Must be CreateStreamingJob.");
            };

        for &actor in &actors {
            self.actor_map.insert(actor, creating_mv_id);
        }

        let progress = Progress::new(
            actors,
            upstream_mv_count,
            upstream_total_key_count,
            definition,
        );
        let old = self
            .progress_map
            .insert(creating_mv_id, (progress, TrackingJob::New(command)));
        assert!(old.is_none());
        None
    }

    /// Update the progress of `actor` according to the Pb struct.
    ///
    /// If all actors in this MV have finished, returns the command.
    pub fn update(
        &mut self,
        progress: &CreateMviewProgress,
        version_stats: &HummockVersionStats,
    ) -> Option<TrackingJob> {
        let actor = progress.chain_actor_id;
        let Some(table_id) = self.actor_map.get(&actor).copied() else {
            // On restart, backfill will ALWAYS notify CreateMviewProgressTracker,
            // even if backfill is finished on recovery.
            // This is because we don't know if only this actor is finished,
            // OR the entire stream job is finished.
            // For the first case, we must notify meta.
            // For the second case, we can still notify meta, but ignore it here.
            tracing::info!(
                "no tracked progress for actor {}, the stream job could already be finished",
                actor
            );
            return None;
        };

        let new_state = if progress.done {
            ChainState::Done(progress.consumed_rows)
        } else {
            ChainState::ConsumingUpstream(progress.consumed_epoch.into(), progress.consumed_rows)
        };

        match self.progress_map.entry(table_id) {
            Entry::Occupied(mut o) => {
                let progress = &mut o.get_mut().0;

                let upstream_total_key_count: u64 = progress
                    .upstream_mv_count
                    .iter()
                    .map(|(upstream_mv, count)| {
                        assert_ne!(*count, 0);
                        *count as u64
                            * version_stats
                                .table_stats
                                .get(&upstream_mv.table_id)
                                .map_or(0, |stat| stat.total_key_count as u64)
                    })
                    .sum();

                progress.update(actor, new_state, upstream_total_key_count);

                if progress.is_done() {
                    tracing::debug!(
                        "all actors done for creating mview with table_id {}!",
                        table_id
                    );

                    // Clean-up the mapping from actors to DDL epoch.
                    for actor in o.get().0.actors() {
                        self.actor_map.remove(&actor);
                    }
                    Some(o.remove().1)
                } else {
                    None
                }
            }
            Entry::Vacant(_) => {
                tracing::warn!(
                    "update the progress of an non-existent creating streaming job: {progress:?}, which could be cancelled"
                );
                None
            }
        }
    }
}
