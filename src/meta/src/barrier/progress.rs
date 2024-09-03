// Copyright 2024 RisingWave Labs
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
use std::collections::HashMap;
use std::mem::take;

use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model_v2::ObjectId;
use risingwave_pb::catalog::{CreateType, Table};
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;

use crate::barrier::{
    Command, CreateStreamingJobCommandInfo, CreateStreamingJobType, EpochNode, ReplaceTablePlan,
};
use crate::manager::{
    DdlType, MetadataManager, MetadataManagerV1, MetadataManagerV2, StreamingJob,
};
use crate::model::{ActorId, TableFragments};
use crate::MetaResult;

type ConsumedRows = u64;

#[derive(Clone, Copy, Debug)]
enum BackfillState {
    Init,
    ConsumingUpstream(#[allow(dead_code)] Epoch, ConsumedRows),
    Done(ConsumedRows),
}

/// Progress of all actors containing backfill executors while creating mview.
#[derive(Debug)]
pub(super) struct Progress {
    states: HashMap<ActorId, BackfillState>,

    done_count: usize,

    /// Upstream mv count.
    /// Keep track of how many times each upstream MV
    /// appears in this stream job.
    upstream_mv_count: HashMap<TableId, usize>,

    /// Total key count in the upstream materialized view
    /// TODO: implement this for source backfill
    upstream_total_key_count: u64,

    /// Consumed rows
    consumed_rows: u64,

    /// DDL definition
    definition: String,
}

impl Progress {
    /// Create a [`Progress`] for some creating mview, with all `actors` containing the backfill executors.
    fn new(
        actors: impl IntoIterator<Item = ActorId>,
        upstream_mv_count: HashMap<TableId, usize>,
        upstream_total_key_count: u64,
        definition: String,
    ) -> Self {
        let states = actors
            .into_iter()
            .map(|a| (a, BackfillState::Init))
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
    fn update(&mut self, actor: ActorId, new_state: BackfillState, upstream_total_key_count: u64) {
        self.upstream_total_key_count = upstream_total_key_count;
        let total_actors = self.states.len();
        tracing::debug!(?actor, states = ?self.states, "update progress for actor");
        match self.states.remove(&actor).unwrap() {
            BackfillState::Init => {}
            BackfillState::ConsumingUpstream(_, old_consumed_rows) => {
                self.consumed_rows -= old_consumed_rows;
            }
            BackfillState::Done(_) => panic!("should not report done multiple times"),
        };
        match &new_state {
            BackfillState::Init => {}
            BackfillState::ConsumingUpstream(_, new_consumed_rows) => {
                self.consumed_rows += new_consumed_rows;
            }
            BackfillState::Done(new_consumed_rows) => {
                tracing::debug!("actor {} done", actor);
                self.consumed_rows += new_consumed_rows;
                self.done_count += 1;
                tracing::debug!(
                    "{} actors out of {} complete",
                    self.done_count,
                    total_actors,
                );
            }
        };
        self.states.insert(actor, new_state);
        self.calculate_progress();
    }

    /// Returns whether all backfill executors are done.
    fn is_done(&self) -> bool {
        tracing::trace!(
            "Progress::is_done? {}, {}, {:?}",
            self.done_count,
            self.states.len(),
            self.states
        );
        self.done_count == self.states.len()
    }

    /// Returns the ids of all actors containing the backfill executors for the mview tracked by this
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
    RecoveredV1(RecoveredTrackingJobV1),
    RecoveredV2(RecoveredTrackingJobV2),
}

impl TrackingJob {
    /// Notify metadata manager that the job is finished.
    pub(crate) async fn finish(self, metadata_manager: &MetadataManager) -> MetaResult<()> {
        match self {
            TrackingJob::New(command) => {
                let CreateStreamingJobCommandInfo {
                    table_fragments,
                    streaming_job,
                    internal_tables,
                    ..
                } = &command.info;
                match metadata_manager {
                    MetadataManager::V1(mgr) => {
                        mgr.fragment_manager
                            .mark_table_fragments_created(table_fragments.table_id())
                            .await?;
                        mgr.catalog_manager
                            .finish_stream_job(streaming_job.clone(), internal_tables.clone())
                            .await?;
                        Ok(())
                    }
                    MetadataManager::V2(mgr) => {
                        mgr.catalog_controller
                            .finish_streaming_job(
                                streaming_job.id() as i32,
                                command.replace_table_info.clone(),
                            )
                            .await?;
                        Ok(())
                    }
                }
            }
            TrackingJob::RecoveredV1(recovered) => {
                let manager = &recovered.metadata_manager;
                manager
                    .fragment_manager
                    .mark_table_fragments_created(recovered.fragments.table_id())
                    .await?;
                manager
                    .catalog_manager
                    .finish_stream_job(
                        recovered.streaming_job.clone(),
                        recovered.internal_tables.clone(),
                    )
                    .await?;
                Ok(())
            }
            TrackingJob::RecoveredV2(recovered) => {
                recovered
                    .metadata_manager
                    .catalog_controller
                    .finish_streaming_job(recovered.id, None)
                    .await?;
                Ok(())
            }
        }
    }

    pub(crate) fn table_to_create(&self) -> TableId {
        match self {
            TrackingJob::New(command) => command.info.table_fragments.table_id(),
            TrackingJob::RecoveredV1(recovered) => recovered.fragments.table_id(),
            TrackingJob::RecoveredV2(recovered) => (recovered.id as u32).into(),
        }
    }
}

impl std::fmt::Debug for TrackingJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrackingJob::New(command) => write!(
                f,
                "TrackingJob::New({:?})",
                command.info.table_fragments.table_id()
            ),
            TrackingJob::RecoveredV1(recovered) => {
                write!(
                    f,
                    "TrackingJob::RecoveredV1({:?})",
                    recovered.fragments.table_id()
                )
            }
            TrackingJob::RecoveredV2(recovered) => {
                write!(f, "TrackingJob::RecoveredV2({:?})", recovered.id)
            }
        }
    }
}

pub struct RecoveredTrackingJobV1 {
    pub fragments: TableFragments,
    pub streaming_job: StreamingJob,
    pub internal_tables: Vec<Table>,
    pub metadata_manager: MetadataManagerV1,
}

pub struct RecoveredTrackingJobV2 {
    pub id: ObjectId,
    pub metadata_manager: MetadataManagerV2,
}

/// The command tracking by the [`CreateMviewProgressTracker`].
pub(super) struct TrackingCommand {
    pub info: CreateStreamingJobCommandInfo,
    pub replace_table_info: Option<ReplaceTablePlan>,
}

/// Tracking is done as follows:
/// 1. We identify a `StreamJob` by its `TableId` of its `Materialized` table.
/// 2. For each stream job, there are several actors which run its tasks.
/// 3. With `progress_map` we can use the ID of the `StreamJob` to view its progress.
/// 4. With `actor_map` we can use an actor's `ActorId` to find the ID of the `StreamJob`.
#[derive(Default, Debug)]
pub(super) struct CreateMviewProgressTracker {
    // TODO: add a specialized progress for source
    /// Progress of the create-mview DDL indicated by the `TableId`.
    progress_map: HashMap<TableId, (Progress, TrackingJob)>,

    /// Find the epoch of the create-mview DDL by the actor containing the MV/source backfill executors.
    actor_map: HashMap<ActorId, TableId>,

    /// Stash of finished jobs. They will be finally finished on checkpoint.
    pending_finished_jobs: Vec<TrackingJob>,
}

impl CreateMviewProgressTracker {
    /// This step recovers state from the meta side:
    /// 1. `Tables`.
    /// 2. `TableFragments`.
    ///
    /// Other state are persisted by the `BackfillExecutor`, such as:
    /// 1. `CreateMviewProgress`.
    /// 2. `Backfill` position.
    pub fn recover_v1(
        version_stats: HummockVersionStats,
        mviews: HashMap<
            TableId,
            (
                TableFragments,
                Table,      // mview table
                Vec<Table>, // internal tables
            ),
        >,
        metadata_manager: MetadataManagerV1,
    ) -> Self {
        let mut actor_map = HashMap::new();
        let mut progress_map = HashMap::new();
        for (creating_table_id, (table_fragments, mview, internal_tables)) in mviews {
            let actors = table_fragments.backfill_actor_ids();
            let mut states = HashMap::new();
            tracing::debug!(?actors, ?creating_table_id, "recover progress for actors");
            for actor in actors {
                actor_map.insert(actor, creating_table_id);
                states.insert(actor, BackfillState::ConsumingUpstream(Epoch(0), 0));
            }

            let progress = Self::recover_progress(
                states,
                table_fragments.dependent_table_ids(),
                mview.definition.clone(),
                &version_stats,
            );
            let tracking_job = TrackingJob::RecoveredV1(RecoveredTrackingJobV1 {
                fragments: table_fragments,
                metadata_manager: metadata_manager.clone(),
                internal_tables,
                streaming_job: StreamingJob::MaterializedView(mview),
            });
            progress_map.insert(creating_table_id, (progress, tracking_job));
        }
        Self {
            progress_map,
            actor_map,
            pending_finished_jobs: Vec::new(),
        }
    }

    pub fn recover_v2(
        mview_map: HashMap<TableId, (String, TableFragments)>,
        version_stats: HummockVersionStats,
        metadata_manager: MetadataManagerV2,
    ) -> Self {
        let mut actor_map = HashMap::new();
        let mut progress_map = HashMap::new();
        for (creating_table_id, (definition, table_fragments)) in mview_map {
            let mut states = HashMap::new();
            let actors = table_fragments.backfill_actor_ids();
            for actor in actors {
                actor_map.insert(actor, creating_table_id);
                states.insert(actor, BackfillState::ConsumingUpstream(Epoch(0), 0));
            }

            let progress = Self::recover_progress(
                states,
                table_fragments.dependent_table_ids(),
                definition,
                &version_stats,
            );
            let tracking_job = TrackingJob::RecoveredV2(RecoveredTrackingJobV2 {
                id: creating_table_id.table_id as i32,
                metadata_manager: metadata_manager.clone(),
            });
            progress_map.insert(creating_table_id, (progress, tracking_job));
        }
        Self {
            progress_map,
            actor_map,
            pending_finished_jobs: Vec::new(),
        }
    }

    fn recover_progress(
        states: HashMap<ActorId, BackfillState>,
        upstream_mv_count: HashMap<TableId, usize>,
        definition: String,
        version_stats: &HummockVersionStats,
    ) -> Progress {
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
        Progress {
            states,
            done_count: 0, // Fill only after first barrier pass
            upstream_mv_count,
            upstream_total_key_count,
            consumed_rows: 0, // Fill only after first barrier pass
            definition,
        }
    }

    pub fn gen_ddl_progress(&self) -> HashMap<u32, DdlProgress> {
        self.progress_map
            .iter()
            .map(|(table_id, (x, _))| {
                let table_id = table_id.table_id;
                let ddl_progress = DdlProgress {
                    id: table_id as u64,
                    statement: x.definition.clone(),
                    progress: format!("{:.2}%", x.calculate_progress() * 100.0),
                };
                (table_id, ddl_progress)
            })
            .collect()
    }

    /// Apply a collected epoch node command to the tracker
    /// Return the finished jobs when the barrier kind is `Checkpoint`
    pub(super) fn apply_collected_command(
        &mut self,
        epoch_node: &EpochNode,
        version_stats: &HummockVersionStats,
    ) -> Vec<TrackingJob> {
        let command_ctx = &epoch_node.command_ctx;
        let new_tracking_job_info =
            if let Command::CreateStreamingJob { info, job_type } = &command_ctx.command {
                match job_type {
                    CreateStreamingJobType::Normal => Some((info, None)),
                    CreateStreamingJobType::SinkIntoTable(replace_table) => {
                        Some((info, Some(replace_table)))
                    }
                    CreateStreamingJobType::SnapshotBackfill(_) => {
                        // The progress of SnapshotBackfill won't be tracked here
                        None
                    }
                }
            } else {
                None
            };
        assert!(epoch_node.state.node_to_collect.is_empty());
        self.update_tracking_jobs(
            new_tracking_job_info,
            epoch_node
                .state
                .resps
                .iter()
                .flat_map(|resp| resp.create_mview_progress.iter()),
            version_stats,
        );
        if let Some(table_id) = command_ctx.command.table_to_cancel() {
            // the cancelled command is possibly stashed in `finished_commands` and waiting
            // for checkpoint, we should also clear it.
            self.cancel_command(table_id);
        }
        if command_ctx.kind.is_checkpoint() {
            self.take_finished_jobs()
        } else {
            vec![]
        }
    }

    /// Stash a command to finish later.
    pub(super) fn stash_command_to_finish(&mut self, finished_job: TrackingJob) {
        self.pending_finished_jobs.push(finished_job);
    }

    /// Finish stashed jobs on checkpoint.
    pub(super) fn take_finished_jobs(&mut self) -> Vec<TrackingJob> {
        tracing::trace!(finished_jobs=?self.pending_finished_jobs, progress_map=?self.progress_map, "finishing jobs");
        take(&mut self.pending_finished_jobs)
    }

    pub(super) fn has_pending_finished_jobs(&self) -> bool {
        !self.pending_finished_jobs.is_empty()
    }

    pub(super) fn cancel_command(&mut self, id: TableId) {
        let _ = self.progress_map.remove(&id);
        self.pending_finished_jobs
            .retain(|x| x.table_to_create() != id);
        self.actor_map.retain(|_, table_id| *table_id != id);
    }

    /// Notify all tracked commands that error encountered and clear them.
    pub fn abort_all(&mut self) {
        self.actor_map.clear();
        self.pending_finished_jobs.clear();
        self.progress_map.clear();
    }

    /// Add a new create-mview DDL command to track.
    ///
    /// If the actors to track is empty, return the given command as it can be finished immediately.
    pub fn add(
        &mut self,
        info: &CreateStreamingJobCommandInfo,
        replace_table: Option<&ReplaceTablePlan>,
        version_stats: &HummockVersionStats,
    ) -> Option<TrackingJob> {
        tracing::trace!(?info, "add job to track");
        let (info, actors, replace_table_info) = {
            let CreateStreamingJobCommandInfo {
                table_fragments, ..
            } = info;
            let actors = table_fragments.tracking_progress_actor_ids();
            if actors.is_empty() {
                // The command can be finished immediately.
                return Some(TrackingJob::New(TrackingCommand {
                    info: info.clone(),
                    replace_table_info: replace_table.cloned(),
                }));
            }
            (info.clone(), actors, replace_table.cloned())
        };

        let CreateStreamingJobCommandInfo {
            table_fragments,
            upstream_root_actors,
            dispatchers,
            definition,
            ddl_type,
            create_type,
            ..
        } = &info;

        let creating_mv_id = table_fragments.table_id();

        let (upstream_mv_count, upstream_total_key_count, ddl_type, create_type) = {
            // Keep track of how many times each upstream MV appears.
            let mut upstream_mv_count = HashMap::new();
            for (table_id, actors) in upstream_root_actors {
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
                upstream_mv_count,
                upstream_total_key_count,
                ddl_type,
                create_type,
            )
        };

        for &actor in &actors {
            self.actor_map.insert(actor, creating_mv_id);
        }

        let progress = Progress::new(
            actors,
            upstream_mv_count,
            upstream_total_key_count,
            definition.clone(),
        );
        if *ddl_type == DdlType::Sink && *create_type == CreateType::Background {
            // We return the original tracking job immediately.
            // This is because sink can be decoupled with backfill progress.
            // We don't need to wait for sink to finish backfill.
            // This still contains the notifiers, so we can tell listeners
            // that the sink job has been created.
            Some(TrackingJob::New(TrackingCommand {
                info,
                replace_table_info,
            }))
        } else {
            let old = self.progress_map.insert(
                creating_mv_id,
                (
                    progress,
                    TrackingJob::New(TrackingCommand {
                        info,
                        replace_table_info,
                    }),
                ),
            );
            assert!(old.is_none());
            None
        }
    }

    /// Update the progress of `actor` according to the Pb struct.
    ///
    /// If all actors in this MV have finished, returns the command.
    pub fn update(
        &mut self,
        progress: &CreateMviewProgress,
        version_stats: &HummockVersionStats,
    ) -> Option<TrackingJob> {
        tracing::trace!(?progress, "update progress");
        let actor = progress.backfill_actor_id;
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
            BackfillState::Done(progress.consumed_rows)
        } else {
            BackfillState::ConsumingUpstream(progress.consumed_epoch.into(), progress.consumed_rows)
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

                tracing::debug!(?table_id, "updating progress for table");
                progress.update(actor, new_state, upstream_total_key_count);

                if progress.is_done() {
                    tracing::debug!(
                        "all actors done for creating mview with table_id {}!",
                        table_id
                    );

                    // Clean-up the mapping from actors to DDL table_id.
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
