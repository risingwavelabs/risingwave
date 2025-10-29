// Copyright 2025 RisingWave Labs
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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::mem::take;

use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model::{CreateType, ObjectId};
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::PbBarrierCompleteResponse;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;

use crate::MetaResult;
use crate::barrier::backfill_order_control::BackfillOrderState;
use crate::barrier::info::{BarrierInfo, InflightStreamingJobInfo};
use crate::barrier::{Command, CreateStreamingJobCommandInfo, CreateStreamingJobType};
use crate::manager::MetadataManager;
use crate::model::{ActorId, BackfillUpstreamType, FragmentId, StreamJobFragments};
use crate::stream::{SourceChange, SourceManagerRef};

type ConsumedRows = u64;
type BufferedRows = u64;

#[derive(Clone, Copy, Debug)]
enum BackfillState {
    Init,
    ConsumingUpstream(#[expect(dead_code)] Epoch, ConsumedRows, BufferedRows),
    Done(ConsumedRows, BufferedRows),
}

/// Represents the backfill nodes that need to be scheduled or cleaned up.
#[derive(Debug, Default)]
pub(super) struct PendingBackfillFragments {
    /// Fragment IDs that should start backfilling in the next checkpoint
    pub next_backfill_nodes: Vec<FragmentId>,
    /// State tables of locality provider fragments that should be truncated
    pub truncate_locality_provider_state_tables: Vec<TableId>,
}

/// Progress of all actors containing backfill executors while creating mview.
#[derive(Debug)]
pub(super) struct Progress {
    // `states` and `done_count` decides whether the progress is done. See `is_done`.
    states: HashMap<ActorId, BackfillState>,
    backfill_order_state: BackfillOrderState,
    done_count: usize,

    /// Tells whether the backfill is from source or mv.
    backfill_upstream_types: HashMap<ActorId, BackfillUpstreamType>,

    // The following row counts are used to calculate the progress. See `calculate_progress`.
    /// Upstream mv count.
    /// Keep track of how many times each upstream MV
    /// appears in this stream job.
    upstream_mv_count: HashMap<TableId, usize>,
    /// Total key count of all the upstream materialized views
    upstream_mvs_total_key_count: u64,
    mv_backfill_consumed_rows: u64,
    source_backfill_consumed_rows: u64,
    /// Buffered rows (for locality backfill) that are yet to be consumed
    /// This is used to calculate precise progress: consumed / (`upstream_total` + buffered)
    mv_backfill_buffered_rows: u64,

    /// DDL definition
    definition: String,
    /// Create type
    create_type: CreateType,
}

impl Progress {
    /// Create a [`Progress`] for some creating mview, with all `actors` containing the backfill executors.
    fn new(
        actors: impl IntoIterator<Item = (ActorId, BackfillUpstreamType)>,
        upstream_mv_count: HashMap<TableId, usize>,
        upstream_total_key_count: u64,
        definition: String,
        create_type: CreateType,
        backfill_order_state: BackfillOrderState,
    ) -> Self {
        let mut states = HashMap::new();
        let mut backfill_upstream_types = HashMap::new();
        for (actor, backfill_upstream_type) in actors {
            states.insert(actor, BackfillState::Init);
            backfill_upstream_types.insert(actor, backfill_upstream_type);
        }
        assert!(!states.is_empty());

        Self {
            states,
            backfill_upstream_types,
            done_count: 0,
            upstream_mv_count,
            upstream_mvs_total_key_count: upstream_total_key_count,
            mv_backfill_consumed_rows: 0,
            source_backfill_consumed_rows: 0,
            mv_backfill_buffered_rows: 0,
            definition,
            create_type,
            backfill_order_state,
        }
    }

    /// Update the progress of `actor`.
    /// Returns the backfill fragments that need to be scheduled or cleaned up.
    fn update(
        &mut self,
        actor: ActorId,
        new_state: BackfillState,
        upstream_total_key_count: u64,
    ) -> PendingBackfillFragments {
        let mut result = PendingBackfillFragments::default();
        self.upstream_mvs_total_key_count = upstream_total_key_count;
        let total_actors = self.states.len();
        let backfill_upstream_type = self.backfill_upstream_types.get(&actor).unwrap();

        let mut old_consumed_row = 0;
        let mut new_consumed_row = 0;
        let mut old_buffered_row = 0;
        let mut new_buffered_row = 0;
        match self.states.remove(&actor).unwrap() {
            BackfillState::Init => {}
            BackfillState::ConsumingUpstream(_, consumed_rows, buffered_rows) => {
                old_consumed_row = consumed_rows;
                old_buffered_row = buffered_rows;
            }
            BackfillState::Done(_, _) => panic!("should not report done multiple times"),
        };
        match &new_state {
            BackfillState::Init => {}
            BackfillState::ConsumingUpstream(_, consumed_rows, buffered_rows) => {
                new_consumed_row = *consumed_rows;
                new_buffered_row = *buffered_rows;
            }
            BackfillState::Done(consumed_rows, buffered_rows) => {
                tracing::debug!("actor {} done", actor);
                new_consumed_row = *consumed_rows;
                new_buffered_row = *buffered_rows;
                self.done_count += 1;
                let before_backfill_nodes = self
                    .backfill_order_state
                    .current_backfill_node_fragment_ids();
                result.next_backfill_nodes = self.backfill_order_state.finish_actor(actor);
                let after_backfill_nodes = self
                    .backfill_order_state
                    .current_backfill_node_fragment_ids();
                // last_backfill_nodes = before_backfill_nodes - after_backfill_nodes
                let last_backfill_nodes_iter = before_backfill_nodes
                    .into_iter()
                    .filter(|x| !after_backfill_nodes.contains(x));
                result.truncate_locality_provider_state_tables = last_backfill_nodes_iter
                    .filter_map(|fragment_id| {
                        self.backfill_order_state
                            .get_locality_fragment_state_table_mapping()
                            .get(&fragment_id)
                    })
                    .flatten()
                    .copied()
                    .collect();
                tracing::debug!(
                    "{} actors out of {} complete",
                    self.done_count,
                    total_actors,
                );
            }
        };
        debug_assert!(
            new_consumed_row >= old_consumed_row,
            "backfill progress should not go backward"
        );
        debug_assert!(
            new_buffered_row >= old_buffered_row,
            "backfill progress should not go backward"
        );
        match backfill_upstream_type {
            BackfillUpstreamType::MView => {
                self.mv_backfill_consumed_rows += new_consumed_row - old_consumed_row;
            }
            BackfillUpstreamType::Source => {
                self.source_backfill_consumed_rows += new_consumed_row - old_consumed_row;
            }
            BackfillUpstreamType::Values => {
                // do not consider progress for values
            }
            BackfillUpstreamType::LocalityProvider => {
                // Track LocalityProvider progress similar to MView
                // Update buffered rows for precise progress calculation
                self.mv_backfill_consumed_rows += new_consumed_row - old_consumed_row;
                self.mv_backfill_buffered_rows += new_buffered_row - old_buffered_row;
            }
        }
        self.states.insert(actor, new_state);
        result
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
    fn calculate_progress(&self) -> String {
        if self.is_done() || self.states.is_empty() {
            return "100%".to_owned();
        }
        let mut mv_count = 0;
        let mut source_count = 0;
        for backfill_upstream_type in self.backfill_upstream_types.values() {
            match backfill_upstream_type {
                BackfillUpstreamType::MView => mv_count += 1,
                BackfillUpstreamType::Source => source_count += 1,
                BackfillUpstreamType::Values => (),
                BackfillUpstreamType::LocalityProvider => mv_count += 1, /* Count LocalityProvider as an MView for progress */
            }
        }

        let mv_progress = (mv_count > 0).then_some({
            // Include buffered rows in total for precise progress calculation
            // Progress = consumed / (upstream_total + buffered)
            let total_rows_to_consume =
                self.upstream_mvs_total_key_count + self.mv_backfill_buffered_rows;
            if total_rows_to_consume == 0 {
                "99.99%".to_owned()
            } else {
                let mut progress =
                    self.mv_backfill_consumed_rows as f64 / (total_rows_to_consume as f64);
                if progress > 1.0 {
                    progress = 0.9999;
                }
                format!(
                    "{:.2}% ({}/{})",
                    progress * 100.0,
                    self.mv_backfill_consumed_rows,
                    total_rows_to_consume
                )
            }
        });
        let source_progress = (source_count > 0).then_some(format!(
            "{} rows consumed",
            self.source_backfill_consumed_rows
        ));
        match (mv_progress, source_progress) {
            (Some(mv_progress), Some(source_progress)) => {
                format!(
                    "MView Backfill: {}, Source Backfill: {}",
                    mv_progress, source_progress
                )
            }
            (Some(mv_progress), None) => mv_progress,
            (None, Some(source_progress)) => source_progress,
            (None, None) => "Unknown".to_owned(),
        }
    }
}

/// There are two kinds of `TrackingJobs`:
/// 1. if `is_recovered` is false, it is a "New" tracking job.
///    It is instantiated and managed by the stream manager.
///    On recovery, the stream manager will stop managing the job.
/// 2. if `is_recovered` is true, it is a "Recovered" tracking job.
///    On recovery, the barrier manager will recover and start managing the job.
pub struct TrackingJob {
    job_id: ObjectId,
    is_recovered: bool,
    source_change: Option<SourceChange>,
}

impl std::fmt::Display for TrackingJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}",
            self.job_id,
            if self.is_recovered { "<recovered>" } else { "" }
        )
    }
}

impl TrackingJob {
    /// Create a new tracking job.
    pub(crate) fn new(job_id: ObjectId, source_change: Option<SourceChange>) -> Self {
        Self {
            job_id,
            is_recovered: false,
            source_change,
        }
    }

    /// Create a recovered tracking job.
    pub(crate) fn recovered(job_id: ObjectId, source_change: Option<SourceChange>) -> Self {
        Self {
            job_id,
            is_recovered: true,
            source_change,
        }
    }

    /// Notify the metadata manager that the job is finished.
    pub(crate) async fn finish(
        self,
        metadata_manager: &MetadataManager,
        source_manager: &SourceManagerRef,
    ) -> MetaResult<()> {
        metadata_manager
            .catalog_controller
            .finish_streaming_job(self.job_id)
            .await?;
        if let Some(source_change) = self.source_change {
            source_manager.apply_source_change(source_change).await;
        }
        Ok(())
    }

    pub(crate) fn table_to_create(&self) -> TableId {
        (self.job_id as u32).into()
    }
}

impl std::fmt::Debug for TrackingJob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !self.is_recovered {
            write!(f, "TrackingJob::New({})", self.job_id)
        } else {
            write!(f, "TrackingJob::Recovered({})", self.job_id)
        }
    }
}

/// Information collected during barrier completion that needs to be committed.
#[derive(Debug, Default)]
pub(super) struct StagingCommitInfo {
    /// Finished jobs that should be committed
    pub finished_jobs: Vec<TrackingJob>,
    /// Table IDs whose locality provider state tables need to be truncated
    pub table_ids_to_truncate: Vec<TableId>,
}

pub(super) enum UpdateProgressResult {
    None,
    /// The finished job, along with its pending backfill fragments for cleanup.
    Finished(TrackingJob, PendingBackfillFragments),
    /// Backfill nodes have finished and new ones need to be scheduled.
    BackfillNodeFinished(PendingBackfillFragments),
}

/// Tracking is done as follows:
/// 1. We identify a `StreamJob` by its `TableId` of its `Materialized` table.
/// 2. For each stream job, there are several actors which run its tasks.
/// 3. With `progress_map` we can use the ID of the `StreamJob` to view its progress.
/// 4. With `actor_map` we can use an actor's `ActorId` to find the ID of the `StreamJob`.
#[derive(Default, Debug)]
pub(super) struct CreateMviewProgressTracker {
    /// Progress of the create-mview DDL indicated by the `TableId`.
    progress_map: HashMap<TableId, (Progress, TrackingJob)>,

    actor_map: HashMap<ActorId, TableId>,

    /// Stash of pending backfill nodes. They will start backfilling on checkpoint.
    pending_backfill_nodes: Vec<FragmentId>,

    /// Staging commit info that will be committed on checkpoint.
    staging_commit_info: StagingCommitInfo,
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
        jobs: impl IntoIterator<
            Item = (
                TableId,
                (String, &InflightStreamingJobInfo, BackfillOrderState),
            ),
        >,
        version_stats: &HummockVersionStats,
    ) -> Self {
        let mut actor_map = HashMap::new();
        let mut progress_map = HashMap::new();
        let mut finished_jobs = vec![];
        for (creating_table_id, (definition, job_info, backfill_order_state)) in jobs {
            let source_backfill_fragments = StreamJobFragments::source_backfill_fragments_impl(
                job_info
                    .fragment_infos
                    .iter()
                    .map(|(fragment_id, fragment)| (*fragment_id, &fragment.nodes)),
            );
            let source_change = if source_backfill_fragments.is_empty() {
                None
            } else {
                Some(SourceChange::CreateJobFinished {
                    finished_backfill_fragments: source_backfill_fragments,
                })
            };
            let tracking_job =
                TrackingJob::recovered(creating_table_id.table_id as _, source_change);
            let actors = job_info.tracking_progress_actor_ids();
            if actors.is_empty() {
                finished_jobs.push(tracking_job);
            } else {
                let mut states = HashMap::new();
                let mut backfill_upstream_types = HashMap::new();

                for (actor, backfill_upstream_type) in actors {
                    actor_map.insert(actor, creating_table_id);
                    states.insert(actor, BackfillState::ConsumingUpstream(Epoch(0), 0, 0));
                    backfill_upstream_types.insert(actor, backfill_upstream_type);
                }

                let progress = Self::recover_progress(
                    states,
                    backfill_upstream_types,
                    StreamJobFragments::upstream_table_counts_impl(
                        job_info
                            .fragment_infos
                            .values()
                            .map(|fragment| &fragment.nodes),
                    ),
                    definition,
                    version_stats,
                    backfill_order_state,
                );
                progress_map.insert(creating_table_id, (progress, tracking_job));
            }
        }
        Self {
            progress_map,
            actor_map,
            pending_backfill_nodes: Vec::new(),
            staging_commit_info: StagingCommitInfo {
                finished_jobs,
                table_ids_to_truncate: vec![],
            },
        }
    }

    /// ## How recovery works
    ///
    /// The progress (number of rows consumed) is persisted in state tables.
    /// During recovery, the backfill executor will restore the number of rows consumed,
    /// and then it will just report progress like newly created executors.
    fn recover_progress(
        states: HashMap<ActorId, BackfillState>,
        backfill_upstream_types: HashMap<ActorId, BackfillUpstreamType>,
        upstream_mv_count: HashMap<TableId, usize>,
        definition: String,
        version_stats: &HummockVersionStats,
        backfill_order_state: BackfillOrderState,
    ) -> Progress {
        let upstream_mvs_total_key_count =
            calculate_total_key_count(&upstream_mv_count, version_stats);
        Progress {
            states,
            backfill_order_state,
            backfill_upstream_types,
            done_count: 0, // Fill only after first barrier pass
            upstream_mv_count,
            upstream_mvs_total_key_count,
            mv_backfill_consumed_rows: 0, // Fill only after first barrier pass
            source_backfill_consumed_rows: 0, // Fill only after first barrier pass
            mv_backfill_buffered_rows: 0, // Fill only after first barrier pass
            definition,
            create_type: CreateType::Background,
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
                    create_type: x.create_type.as_str().to_owned(),
                    progress: x.calculate_progress(),
                };
                (table_id, ddl_progress)
            })
            .collect()
    }

    /// Update the progress of tracked jobs, and add a new job to track if `info` is `Some`.
    /// Return the table ids whose locality provider state tables need to be truncated.
    pub(super) fn update_tracking_jobs<'a>(
        &mut self,
        info: Option<&CreateStreamingJobCommandInfo>,
        create_mview_progress: impl IntoIterator<Item = &'a CreateMviewProgress>,
        version_stats: &HummockVersionStats,
    ) {
        let mut table_ids_to_truncate = vec![];
        // Save `finished_commands` for Create MVs.
        let finished_commands = {
            let mut commands = vec![];
            // Add the command to tracker.
            if let Some(create_job_info) = info
                && let Some(command) = self.add(create_job_info, version_stats)
            {
                // Those with no actors to track can be finished immediately.
                commands.push(command);
            }
            // Update the progress of all commands.
            for progress in create_mview_progress {
                // Those with actors complete can be finished immediately.
                match self.update(progress, version_stats) {
                    UpdateProgressResult::None => {
                        tracing::trace!(?progress, "update progress");
                    }
                    UpdateProgressResult::Finished(command, pending) => {
                        table_ids_to_truncate
                            .extend(pending.truncate_locality_provider_state_tables.clone());
                        self.queue_backfill(pending.next_backfill_nodes);
                        tracing::trace!(?progress, "finish progress");
                        commands.push(command);
                    }
                    UpdateProgressResult::BackfillNodeFinished(pending) => {
                        table_ids_to_truncate
                            .extend(pending.truncate_locality_provider_state_tables.clone());
                        tracing::trace!(
                            ?progress,
                            next_backfill_nodes = ?pending.next_backfill_nodes,
                            "start next backfill node"
                        );
                        self.queue_backfill(pending.next_backfill_nodes);
                    }
                }
            }
            commands
        };

        for command in finished_commands {
            self.staging_commit_info.finished_jobs.push(command);
        }
        self.staging_commit_info
            .table_ids_to_truncate
            .extend(table_ids_to_truncate);
    }

    /// Apply a collected epoch node command to the tracker
    /// Return the staging commit info when the barrier kind is `Checkpoint`.
    pub(super) fn apply_collected_command(
        &mut self,
        command: Option<&Command>,
        barrier_info: &BarrierInfo,
        resps: impl IntoIterator<Item = &PbBarrierCompleteResponse>,
        version_stats: &HummockVersionStats,
    ) -> Option<StagingCommitInfo> {
        let new_tracking_job_info =
            if let Some(Command::CreateStreamingJob { info, job_type, .. }) = command {
                match job_type {
                    CreateStreamingJobType::Normal | CreateStreamingJobType::SinkIntoTable(_) => {
                        Some(info)
                    }
                    CreateStreamingJobType::SnapshotBackfill(_) => {
                        // The progress of SnapshotBackfill won't be tracked here
                        None
                    }
                }
            } else {
                None
            };
        self.update_tracking_jobs(
            new_tracking_job_info,
            resps
                .into_iter()
                .flat_map(|resp| resp.create_mview_progress.iter()),
            version_stats,
        );
        for table_id in command.map(Command::jobs_to_drop).into_iter().flatten() {
            // the cancelled command is possibly stashed in `finished_commands` and waiting
            // for checkpoint, we should also clear it.
            self.cancel_command(table_id);
        }
        if barrier_info.kind.is_checkpoint() {
            Some(take(&mut self.staging_commit_info))
        } else {
            None
        }
    }

    fn queue_backfill(&mut self, backfill_nodes: impl IntoIterator<Item = FragmentId>) {
        self.pending_backfill_nodes.extend(backfill_nodes);
    }

    pub(super) fn take_pending_backfill_nodes(&mut self) -> Vec<FragmentId> {
        take(&mut self.pending_backfill_nodes)
    }

    pub(super) fn has_pending_finished_jobs(&self) -> bool {
        !self.staging_commit_info.finished_jobs.is_empty()
    }

    pub(super) fn cancel_command(&mut self, id: TableId) {
        let _ = self.progress_map.remove(&id);
        self.staging_commit_info
            .finished_jobs
            .retain(|x| x.table_to_create() != id);
        self.actor_map.retain(|_, table_id| *table_id != id);
    }

    /// Notify all tracked commands that error encountered and clear them.
    pub fn abort_all(&mut self) {
        self.actor_map.clear();
        self.staging_commit_info = StagingCommitInfo::default();
        self.progress_map.clear();
    }

    /// Add a new create-mview DDL command to track.
    ///
    /// If the actors to track are empty, return the given command as it can be finished immediately.
    pub fn add(
        &mut self,
        info: &CreateStreamingJobCommandInfo,
        version_stats: &HummockVersionStats,
    ) -> Option<TrackingJob> {
        tracing::trace!(?info, "add job to track");
        let (info, actors) = {
            let CreateStreamingJobCommandInfo {
                stream_job_fragments,
                ..
            } = info;
            let actors = stream_job_fragments.tracking_progress_actor_ids();
            if actors.is_empty() {
                // The command can be finished immediately.
                return Some(TrackingJob::new(
                    info.stream_job_fragments.stream_job_id.table_id as _,
                    Some(SourceChange::CreateJobFinished {
                        finished_backfill_fragments: stream_job_fragments
                            .source_backfill_fragments(),
                    }),
                ));
            }
            (info.clone(), actors)
        };

        let CreateStreamingJobCommandInfo {
            stream_job_fragments: table_fragments,
            definition,
            create_type,
            fragment_backfill_ordering,
            locality_fragment_state_table_mapping,
            ..
        } = info;

        let creating_job_id = table_fragments.stream_job_id();
        let upstream_mv_count = table_fragments.upstream_table_counts();
        let upstream_total_key_count: u64 =
            calculate_total_key_count(&upstream_mv_count, version_stats);

        for (actor, _backfill_upstream_type) in &actors {
            self.actor_map.insert(*actor, creating_job_id);
        }

        let backfill_order_state = BackfillOrderState::new(
            fragment_backfill_ordering,
            &table_fragments,
            locality_fragment_state_table_mapping,
        );
        let progress = Progress::new(
            actors,
            upstream_mv_count,
            upstream_total_key_count,
            definition,
            create_type.into(),
            backfill_order_state,
        );
        let old = self.progress_map.insert(
            creating_job_id,
            (
                progress,
                TrackingJob::new(
                    creating_job_id.table_id as _,
                    Some(SourceChange::CreateJobFinished {
                        finished_backfill_fragments: table_fragments.source_backfill_fragments(),
                    }),
                ),
            ),
        );
        assert!(old.is_none());
        None
    }

    /// Update the progress of `actor` according to the Pb struct.
    ///
    /// If all actors in this MV have finished, return the command.
    pub fn update(
        &mut self,
        progress: &CreateMviewProgress,
        version_stats: &HummockVersionStats,
    ) -> UpdateProgressResult {
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
            return UpdateProgressResult::None;
        };

        let new_state = if progress.done {
            BackfillState::Done(progress.consumed_rows, progress.buffered_rows)
        } else {
            BackfillState::ConsumingUpstream(
                progress.consumed_epoch.into(),
                progress.consumed_rows,
                progress.buffered_rows,
            )
        };

        match self.progress_map.entry(table_id) {
            Entry::Occupied(mut o) => {
                let progress_state = &mut o.get_mut().0;

                let upstream_total_key_count: u64 =
                    calculate_total_key_count(&progress_state.upstream_mv_count, version_stats);

                tracing::debug!(?table_id, "updating progress for table");
                let pending = progress_state.update(actor, new_state, upstream_total_key_count);

                if progress_state.is_done() {
                    tracing::debug!(
                        "all actors done for creating mview with table_id {}!",
                        table_id
                    );

                    // Clean-up the mapping from actors to DDL table_id.
                    for actor in o.get().0.actors() {
                        self.actor_map.remove(&actor);
                    }
                    assert!(pending.next_backfill_nodes.is_empty());
                    UpdateProgressResult::Finished(o.remove().1, pending)
                } else if !pending.next_backfill_nodes.is_empty()
                    || !pending.truncate_locality_provider_state_tables.is_empty()
                {
                    UpdateProgressResult::BackfillNodeFinished(pending)
                } else {
                    UpdateProgressResult::None
                }
            }
            Entry::Vacant(_) => {
                tracing::warn!(
                    "update the progress of an non-existent creating streaming job: {progress:?}, which could be cancelled"
                );
                UpdateProgressResult::None
            }
        }
    }
}

fn calculate_total_key_count(
    table_count: &HashMap<TableId, usize>,
    version_stats: &HummockVersionStats,
) -> u64 {
    table_count
        .iter()
        .map(|(table_id, count)| {
            assert_ne!(*count, 0);
            *count as u64
                * version_stats
                    .table_stats
                    .get(&table_id.table_id)
                    .map_or(0, |stat| stat.total_key_count as u64)
        })
        .sum()
}
