// Copyright 2026 RisingWave Labs
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

mod barrier_control;
mod status;

use std::cmp::max;
use std::collections::{HashMap, HashSet, VecDeque, hash_map};
use std::mem::take;
use std::ops::Bound::{Excluded, Unbounded};
use std::time::Duration;

use risingwave_common::catalog::TableId;
use risingwave_common::id::JobId;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_common::util::epoch::Epoch;
use risingwave_meta_model::WorkerId;
use risingwave_pb::ddl_service::PbBackfillType;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::id::{ActorId, FragmentId, PartialGraphId};
use risingwave_pb::stream_plan::StopMutation;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use status::CreatingStreamingJobStatus;
use tracing::{debug, info};

use self::barrier_control::CreatingStreamingJobBarrierStats;
use super::super::state::RenderResult;
use super::{
    IndependentCheckpointJob, IndependentCheckpointJobControl, IndependentCheckpointJobStatus,
    IndependentJobControl, IndependentJobInfo, InitialPartialGraphRequest, SnapshotPhaseControl,
    add_initial_partial_graph, build_initial_add_mutation,
    snapshot_backfill_max_pending_barrier_num,
};
use crate::MetaResult;
use crate::barrier::checkpoint::independent_job::creating_job::status::CreateMviewLogStoreProgressTracker;
use crate::barrier::command::{
    PostCollectCommand, TableLogEpochs, ThrottleConfigMap, UpstreamTableLogEpochs,
};
use crate::barrier::context::CreateIndependentStreamingJobCommandInfo;
use crate::barrier::edge_builder::FragmentEdgeBuildResult;
use crate::barrier::info::{BarrierInfo, InflightStreamingJobInfo};
use crate::barrier::notifier::NotifierStarter;
use crate::barrier::partial_graph::{
    CollectedBarrier, PartialGraphBarrierInfo, PartialGraphManager, PartialGraphRecoverer,
};
use crate::barrier::progress::{TrackingJob, collect_done_fragments};
use crate::barrier::{
    BackfillProgress, BarrierKind, Command, FragmentBackfillProgress, TracedEpoch,
};
use crate::controller::fragment::InflightFragmentInfo;
use crate::model::{FragmentDownstreamRelation, StreamActor, StreamJobActorsToCreate};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::source_manager::SplitAssignment;
use crate::stream::{ExtendedFragmentBackfillOrder, build_actor_connector_splits};

#[derive(Debug)]
pub(crate) struct CreatingJobInfo {
    pub fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
    pub upstream_fragment_downstreams: FragmentDownstreamRelation,
    pub downstreams: FragmentDownstreamRelation,
    pub snapshot_backfill_upstream_tables: HashSet<TableId>,
    pub stream_actors: HashMap<ActorId, StreamActor>,
}

#[derive(Debug)]
pub(crate) struct CreatingStreamingJobControl {
    control: IndependentJobControl,

    node_actors: HashMap<WorkerId, HashSet<ActorId>>,

    status: CreatingStreamingJobStatus,
    max_lagged_barrier_num: usize,
    max_pending_barrier_num: usize,

    upstream_lag: LabelGuardedIntGauge,
}

impl CreatingStreamingJobControl {
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new<'a>(
        entry: hash_map::VacantEntry<'a, JobId, IndependentCheckpointJobControl>,
        create_info: CreateIndependentStreamingJobCommandInfo,
        notifier: Option<&mut NotifierStarter>,
        snapshot_backfill_upstream_tables: HashSet<TableId>,
        snapshot_epoch: u64,
        since_timestamp_upstream_log_epochs: Option<(&TableLogEpochs, PartialGraphId, u64)>,
        version_stat: &HummockVersionStats,
        term_id: &str,
        partial_graph_manager: &mut PartialGraphManager,
        edges: &mut FragmentEdgeBuildResult,
        split_assignment: &SplitAssignment,
        actors: &RenderResult,
    ) -> MetaResult<&'a mut Self> {
        let info = create_info.info.clone();
        let job_id = info.stream_job_fragments.stream_job_id();
        let database_id = info.streaming_job.database_id();
        let is_since_timestamp = since_timestamp_upstream_log_epochs.is_some();
        debug!(
            %job_id,
            definition = info.definition,
            "new creating job"
        );
        let fragment_infos = info
            .stream_job_fragments
            .new_fragment_info(
                &actors.stream_actors,
                &actors.actor_location,
                split_assignment,
            )
            .collect();
        let snapshot_backfill_actors: HashSet<ActorId> =
            InflightStreamingJobInfo::snapshot_backfill_actor_ids(&fragment_infos).collect();
        let actors_to_create = Command::create_streaming_job_actors_to_create(
            &info,
            edges,
            &actors.stream_actors,
            &actors.actor_location,
        );

        let (snapshot, initial_barrier_info, log_store_barriers_to_inject) = if let Some((
            upstream_log_epochs,
            upstream_partial_graph_id,
            new_upstream_barrier_prev_epoch,
        )) =
            since_timestamp_upstream_log_epochs
        {
            let (initial_barrier, barriers_to_inject) =
                Self::resolve_since_timestamp_upstream_log_epochs(
                    upstream_log_epochs,
                    partial_graph_manager.pending_barrier_infos(upstream_partial_graph_id),
                    snapshot_epoch,
                    new_upstream_barrier_prev_epoch,
                )?;
            (None, initial_barrier, Some(barriers_to_inject))
        } else {
            let (snapshot, initial_barrier) = SnapshotPhaseControl::for_new_job(
                snapshot_epoch,
                &fragment_infos,
                &info,
                version_stat,
            );
            (Some(snapshot), initial_barrier, None)
        };

        let actor_splits = split_assignment
            .values()
            .flat_map(build_actor_connector_splits)
            .collect();

        assert!(
            info.cdc_table_snapshot_splits.is_none(),
            "should not have cdc backfill for snapshot backfill job"
        );

        let initial_mutation = build_initial_add_mutation(
            &fragment_infos,
            &info.fragment_backfill_ordering,
            actor_splits,
        );

        let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());
        let independent_job_info = IndependentJobInfo::from_fragment_infos(
            database_id,
            job_id,
            snapshot_epoch,
            snapshot_backfill_upstream_tables,
            fragment_infos.values(),
        );
        let partial_graph_id = independent_job_info.partial_graph_id;
        let max_lagged_barrier_num = partial_graph_manager
            .control_stream_manager()
            .env
            .opts
            .snapshot_backfill_finish_max_lagged_barriers;
        let opts = &partial_graph_manager.control_stream_manager().env.opts;
        let max_pending_barrier_num = snapshot_backfill_max_pending_barrier_num(opts);

        let mut job = Self {
            control: IndependentJobControl::new(independent_job_info),
            status: CreatingStreamingJobStatus::PlaceHolder, // filled in later code
            max_lagged_barrier_num,
            max_pending_barrier_num,
            upstream_lag: GLOBAL_META_METRICS
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&format!("{}", job_id)]),
            node_actors,
        };

        let mut graph_adder = partial_graph_manager.add_partial_graph(
            partial_graph_id,
            term_id,
            CreatingStreamingJobBarrierStats::new(job_id, snapshot_epoch),
        );
        if let Err(e) = add_initial_partial_graph(
            &mut graph_adder,
            partial_graph_id,
            InitialPartialGraphRequest {
                node_actors: &job.node_actors,
                state_table_ids: &job.control.info.state_table_ids,
                barrier_info: initial_barrier_info,
                actors_to_create,
                mutation: initial_mutation,
                notifier,
                create_info,
            },
        ) {
            graph_adder.failed();
            entry.insert(IndependentCheckpointJobControl::resetting(
                job.control.info.snapshot_backfill_upstream_tables,
            ));
            return Err(e);
        }
        graph_adder.added();
        let job_info = CreatingJobInfo {
            fragment_infos,
            upstream_fragment_downstreams: info.upstream_fragment_downstreams.clone(),
            downstreams: info.stream_job_fragments.downstreams,
            snapshot_backfill_upstream_tables: job
                .control
                .info
                .snapshot_backfill_upstream_tables
                .clone(),
            stream_actors: actors
                .stream_actors
                .values()
                .flatten()
                .map(|actor| (actor.actor_id, actor.clone()))
                .collect(),
        };
        if let Some(log_store_barriers_to_inject) = log_store_barriers_to_inject {
            let upstream_lag = log_store_barriers_to_inject
                .last()
                .map(|info| info.prev_epoch().saturating_sub(snapshot_epoch))
                .unwrap_or(0);
            job.status = CreatingStreamingJobStatus::ConsumingLogStore {
                tracking_job: TrackingJob::recovered(job_id, &job_info.fragment_infos),
                info: job_info,
                log_store_progress_tracker: CreateMviewLogStoreProgressTracker::new(
                    snapshot_backfill_actors.iter().cloned(),
                    upstream_lag,
                ),
                pending_barriers: log_store_barriers_to_inject.into(),
            };
        } else {
            job.status = CreatingStreamingJobStatus::ConsumingSnapshot {
                snapshot: snapshot.expect("snapshot phase should be initialized"),
                pending_upstream_barriers: vec![],
                snapshot_backfill_actors,
                info: job_info,
            };
        }
        let job_control = entry.insert(if is_since_timestamp {
            // Since-timestamp resolution waits for current completion work and requires the
            // resolved snapshot epoch to be older than the upstream committed epoch.
            IndependentCheckpointJobControl::creating_streaming_job(
                job_id,
                partial_graph_id,
                IndependentCheckpointJobStatus::Ready,
                job,
            )
        } else {
            IndependentCheckpointJobControl::creating_streaming_job(
                job_id,
                partial_graph_id,
                IndependentCheckpointJobStatus::Initial { snapshot_epoch },
                job,
            )
        });
        let Some(IndependentCheckpointJob::CreatingStreamingJob(job)) = job_control.running_mut()
        else {
            unreachable!()
        };
        Ok(job)
    }

    pub(super) fn gen_fragment_backfill_progress(&self) -> Vec<FragmentBackfillProgress> {
        match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { snapshot, info, .. } => snapshot
                .create_mview_tracker
                .collect_fragment_progress(&info.fragment_infos, true),
            CreatingStreamingJobStatus::ConsumingLogStore { info, .. } => {
                collect_done_fragments(self.control.info.job_id, &info.fragment_infos)
            }
            CreatingStreamingJobStatus::Finishing(_, _)
            | CreatingStreamingJobStatus::PlaceHolder => vec![],
        }
    }

    pub(super) fn resolve_upstream_log_epochs(
        snapshot_backfill_upstream_tables: &HashSet<TableId>,
        upstream_table_log_epochs: &UpstreamTableLogEpochs,
        exclusive_start_log_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
    ) -> MetaResult<Vec<BarrierInfo>> {
        let table_id = snapshot_backfill_upstream_tables
            .iter()
            .next()
            .expect("snapshot backfill job should have upstream");
        let epochs_iter = if let Some(epochs) = upstream_table_log_epochs.get(table_id) {
            let mut epochs_iter = epochs.iter();
            loop {
                let (_, checkpoint_epoch) =
                    epochs_iter.next().expect("not reach committed epoch yet");
                if *checkpoint_epoch < exclusive_start_log_epoch {
                    continue;
                }
                assert_eq!(*checkpoint_epoch, exclusive_start_log_epoch);
                break;
            }
            epochs_iter
        } else {
            // snapshot backfill job has been marked as creating, but upstream table has not committed a new epoch yet, so no table change log
            assert_eq!(
                upstream_barrier_info.prev_epoch(),
                exclusive_start_log_epoch
            );
            static EMPTY_VEC: Vec<(Vec<u64>, u64)> = Vec::new();
            EMPTY_VEC.iter()
        };

        let mut ret = vec![];
        let mut prev_epoch = exclusive_start_log_epoch;
        let mut pending_non_checkpoint_barriers = vec![];
        for (non_checkpoint_epochs, checkpoint_epoch) in epochs_iter {
            for (i, epoch) in non_checkpoint_epochs
                .iter()
                .chain([checkpoint_epoch])
                .enumerate()
            {
                assert!(*epoch > prev_epoch);
                pending_non_checkpoint_barriers.push(prev_epoch);
                ret.push(BarrierInfo {
                    prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
                    curr_epoch: TracedEpoch::new(Epoch(*epoch)),
                    kind: if i == 0 {
                        BarrierKind::Checkpoint(take(&mut pending_non_checkpoint_barriers))
                    } else {
                        BarrierKind::Barrier
                    },
                });
                prev_epoch = *epoch;
            }
        }
        ret.push(BarrierInfo {
            prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
            curr_epoch: TracedEpoch::new(Epoch(upstream_barrier_info.curr_epoch())),
            kind: BarrierKind::Checkpoint(pending_non_checkpoint_barriers),
        });
        Ok(ret)
    }

    /// Resolves the log-store barriers that must be injected before the create barrier.
    ///
    /// Example with pending upstream barriers:
    ///
    /// ```text
    /// snapshot epoch: 60
    /// changelog after snapshot: [61, 62, 63, 64] + 65
    /// pending upstream barriers: 66 -> 67 barrier, 67 -> 68 barrier,
    ///                            68 -> 69 barrier, 69 -> 70 barrier
    /// new create barrier: 70 -> 71
    ///
    /// injected: 60 -> 61 checkpoint, 61 -> 62 barrier, ..., 64 -> 65 barrier,
    ///           65 -> 66 checkpoint, 66 -> 67 barrier, 67 -> 68 barrier, ...,
    ///           69 -> 70 barrier
    /// current create barrier later injects: 70 -> 71 checkpoint
    /// ```
    ///
    /// Example without pending upstream barriers:
    ///
    /// ```text
    /// snapshot epoch: 60
    /// changelog after snapshot: [61, 62, 63, 64] + 65
    /// new create barrier: 66 -> 67
    ///
    /// injected: 60 -> 61 checkpoint, 61 -> 62 barrier, ..., 64 -> 65 barrier,
    ///           65 -> 66 checkpoint
    /// current create barrier later injects: 66 -> 67 checkpoint
    /// ```
    fn resolve_since_timestamp_upstream_log_epochs(
        upstream_log_epochs: &TableLogEpochs,
        pending_upstream_barriers: impl Iterator<Item = &BarrierInfo>,
        snapshot_epoch: u64,
        new_upstream_barrier_prev_epoch: u64,
    ) -> MetaResult<(BarrierInfo, Vec<BarrierInfo>)> {
        let mut initial_barrier = None;
        let mut barriers = vec![];
        fn emit_barrier(
            initial_barrier: &mut Option<BarrierInfo>,
            barriers: &mut Vec<BarrierInfo>,
            barrier: BarrierInfo,
        ) {
            if initial_barrier.is_none() {
                *initial_barrier = Some(barrier);
            } else {
                barriers.push(barrier);
            }
        }

        let mut prev_epoch = snapshot_epoch;
        let mut pending_non_checkpoint_barriers = vec![];
        for (non_checkpoint_epochs, checkpoint_epoch) in upstream_log_epochs {
            for (i, epoch) in non_checkpoint_epochs
                .iter()
                .chain([checkpoint_epoch])
                .enumerate()
            {
                assert!(
                    *epoch > prev_epoch,
                    "changelog epochs should be strictly increasing"
                );
                pending_non_checkpoint_barriers.push(prev_epoch);
                emit_barrier(
                    &mut initial_barrier,
                    &mut barriers,
                    BarrierInfo {
                        prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
                        curr_epoch: TracedEpoch::new(Epoch(*epoch)),
                        kind: if i == 0 {
                            BarrierKind::Checkpoint(take(&mut pending_non_checkpoint_barriers))
                        } else {
                            BarrierKind::Barrier
                        },
                    },
                );
                prev_epoch = *epoch;
            }
        }

        let mut pending_upstream_barriers = pending_upstream_barriers.peekable();
        pending_non_checkpoint_barriers.push(prev_epoch);
        if pending_upstream_barriers.peek().is_none() {
            assert!(
                new_upstream_barrier_prev_epoch > prev_epoch,
                "new upstream barrier prev epoch should be newer than the latest changelog epoch"
            );
            emit_barrier(
                &mut initial_barrier,
                &mut barriers,
                BarrierInfo {
                    prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
                    curr_epoch: TracedEpoch::new(Epoch(new_upstream_barrier_prev_epoch)),
                    kind: BarrierKind::Checkpoint(pending_non_checkpoint_barriers),
                },
            );
        } else {
            let first_pending_barrier = pending_upstream_barriers
                .peek()
                .expect("first pending upstream barrier should exist after peek");
            assert!(
                first_pending_barrier.prev_epoch() > prev_epoch,
                "first pending upstream barrier should be newer than the latest resolved changelog epoch"
            );
            emit_barrier(
                &mut initial_barrier,
                &mut barriers,
                BarrierInfo {
                    prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
                    curr_epoch: TracedEpoch::new(Epoch(first_pending_barrier.prev_epoch())),
                    kind: BarrierKind::Checkpoint(take(&mut pending_non_checkpoint_barriers)),
                },
            );
            prev_epoch = first_pending_barrier.prev_epoch();
            for pending_barrier in pending_upstream_barriers {
                assert_eq!(
                    pending_barrier.prev_epoch(),
                    prev_epoch,
                    "pending upstream barriers should continue from resolved changelog epochs"
                );
                pending_non_checkpoint_barriers.push(prev_epoch);
                emit_barrier(
                    &mut initial_barrier,
                    &mut barriers,
                    BarrierInfo {
                        prev_epoch: TracedEpoch::new(Epoch(prev_epoch)),
                        curr_epoch: TracedEpoch::new(Epoch(pending_barrier.curr_epoch())),
                        kind: if pending_barrier.kind.is_checkpoint() {
                            BarrierKind::Checkpoint(take(&mut pending_non_checkpoint_barriers))
                        } else {
                            BarrierKind::Barrier
                        },
                    },
                );
                prev_epoch = pending_barrier.curr_epoch();
            }
            assert_eq!(
                new_upstream_barrier_prev_epoch, prev_epoch,
                "new upstream barrier prev epoch should match the latest pending log-store epoch"
            );
        }
        let Some(initial_barrier) = initial_barrier else {
            return Err(anyhow::anyhow!(
                "missing lagging barriers for direct log-store start from snapshot epoch {}",
                snapshot_epoch
            )
            .into());
        };
        assert!(initial_barrier.kind.is_checkpoint());
        Ok((initial_barrier, barriers))
    }

    fn recover_consuming_snapshot(
        job_id: JobId,
        upstream_table_log_epochs: &UpstreamTableLogEpochs,
        snapshot_epoch: u64,
        committed_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
        info: CreatingJobInfo,
        backfill_order: &ExtendedFragmentBackfillOrder,
        version_stat: &HummockVersionStats,
    ) -> MetaResult<(CreatingStreamingJobStatus, BarrierInfo)> {
        let (snapshot, barrier_info) = SnapshotPhaseControl::for_recovery(
            job_id,
            snapshot_epoch,
            committed_epoch,
            &info.fragment_infos,
            backfill_order,
            version_stat,
        );
        Ok((
            CreatingStreamingJobStatus::ConsumingSnapshot {
                snapshot,
                pending_upstream_barriers: Self::resolve_upstream_log_epochs(
                    &info.snapshot_backfill_upstream_tables,
                    upstream_table_log_epochs,
                    snapshot_epoch,
                    upstream_barrier_info,
                )?,
                snapshot_backfill_actors: InflightStreamingJobInfo::snapshot_backfill_actor_ids(
                    &info.fragment_infos,
                )
                .collect(),
                info,
            },
            barrier_info,
        ))
    }

    fn recover_consuming_log_store(
        job_id: JobId,
        upstream_table_log_epochs: &UpstreamTableLogEpochs,
        committed_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
        info: CreatingJobInfo,
    ) -> MetaResult<(CreatingStreamingJobStatus, BarrierInfo)> {
        let mut pending_barriers: VecDeque<_> = Self::resolve_upstream_log_epochs(
            &info.snapshot_backfill_upstream_tables,
            upstream_table_log_epochs,
            committed_epoch,
            upstream_barrier_info,
        )?
        .into();
        let mut first_barrier = pending_barriers
            .pop_front()
            .expect("resolved upstream log epochs should not be empty");
        assert!(first_barrier.kind.is_checkpoint());
        first_barrier.kind = BarrierKind::Initial;

        Ok((
            CreatingStreamingJobStatus::ConsumingLogStore {
                tracking_job: TrackingJob::recovered(job_id, &info.fragment_infos),
                log_store_progress_tracker: CreateMviewLogStoreProgressTracker::new(
                    InflightStreamingJobInfo::snapshot_backfill_actor_ids(&info.fragment_infos),
                    pending_barriers
                        .back()
                        .map(|info| info.prev_epoch() - committed_epoch)
                        .unwrap_or(0),
                ),
                pending_barriers,
                info,
            },
            first_barrier,
        ))
    }

    pub(crate) fn recover(
        control: IndependentJobControl,
        upstream_table_log_epochs: &UpstreamTableLogEpochs,
        upstream_barrier_info: &BarrierInfo,
        fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
        backfill_order: ExtendedFragmentBackfillOrder,
        fragment_relations: &FragmentDownstreamRelation,
        version_stat: &HummockVersionStats,
        new_actors: StreamJobActorsToCreate,
        initial_mutation: Mutation,
        term_id: &str,
        partial_graph_recoverer: &mut PartialGraphRecoverer<'_>,
    ) -> MetaResult<Self> {
        let job_id = control.info.job_id;
        let partial_graph_id = control.info.partial_graph_id;
        let snapshot_epoch = control.info.snapshot_epoch;
        let committed_epoch = control
            .max_committed_epoch
            .expect("recovered independent job should have committed");
        info!(
            %job_id,
            "recovered creating snapshot backfill job"
        );

        let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());

        let mut upstream_fragment_downstreams: FragmentDownstreamRelation = Default::default();
        for (upstream_fragment_id, downstreams) in fragment_relations {
            if fragment_infos.contains_key(upstream_fragment_id) {
                continue;
            }
            for downstream in downstreams {
                if fragment_infos.contains_key(&downstream.downstream_fragment_id) {
                    upstream_fragment_downstreams
                        .entry(*upstream_fragment_id)
                        .or_default()
                        .push(downstream.clone());
                }
            }
        }
        let downstreams = fragment_infos
            .keys()
            .filter_map(|fragment_id| {
                fragment_relations
                    .get(fragment_id)
                    .map(|relation| (*fragment_id, relation.clone()))
            })
            .collect();

        let info = CreatingJobInfo {
            fragment_infos,
            upstream_fragment_downstreams,
            downstreams,
            snapshot_backfill_upstream_tables: control
                .info
                .snapshot_backfill_upstream_tables
                .clone(),
            stream_actors: new_actors
                .values()
                .flat_map(|fragments| {
                    fragments.values().flat_map(|(_, actors, _)| {
                        actors
                            .iter()
                            .map(|(actor, _, _)| (actor.actor_id, actor.clone()))
                    })
                })
                .collect(),
        };

        let (status, first_barrier_info) = if committed_epoch < snapshot_epoch {
            Self::recover_consuming_snapshot(
                job_id,
                upstream_table_log_epochs,
                snapshot_epoch,
                committed_epoch,
                upstream_barrier_info,
                info,
                &backfill_order,
                version_stat,
            )?
        } else {
            Self::recover_consuming_log_store(
                job_id,
                upstream_table_log_epochs,
                committed_epoch,
                upstream_barrier_info,
                info,
            )?
        };

        let max_lagged_barrier_num = partial_graph_recoverer
            .control_stream_manager()
            .env
            .opts
            .snapshot_backfill_finish_max_lagged_barriers;
        let opts = &partial_graph_recoverer.control_stream_manager().env.opts;
        let max_pending_barrier_num = snapshot_backfill_max_pending_barrier_num(opts);

        partial_graph_recoverer.recover_graph(
            partial_graph_id,
            term_id,
            initial_mutation,
            &first_barrier_info,
            &node_actors,
            control.info.state_table_ids.iter().copied(),
            new_actors,
            CreatingStreamingJobBarrierStats::new(job_id, snapshot_epoch),
        )?;

        Ok(Self {
            control,
            node_actors,
            status,
            max_lagged_barrier_num,
            max_pending_barrier_num,
            upstream_lag: GLOBAL_META_METRICS
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&format!("{}", job_id)]),
        })
    }

    pub(crate) fn gen_backfill_progress(&self) -> BackfillProgress {
        let progress = match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { snapshot, .. } => {
                if snapshot.create_mview_tracker.is_finished() {
                    "Snapshot finished".to_owned()
                } else {
                    let progress = snapshot.create_mview_tracker.gen_backfill_progress();
                    format!("Snapshot [{}]", progress)
                }
            }
            CreatingStreamingJobStatus::ConsumingLogStore {
                log_store_progress_tracker,
                ..
            } => {
                format!(
                    "LogStore [{}]",
                    log_store_progress_tracker.gen_backfill_progress()
                )
            }
            CreatingStreamingJobStatus::Finishing(finish_epoch, ..) => {
                let committed_epoch = self
                    .control
                    .max_committed_epoch
                    .expect("should have committed");
                let lag = Duration::from_millis(
                    Epoch(*finish_epoch).physical_time() - Epoch(committed_epoch).physical_time(),
                );
                format!("Finishing [epoch lag: {lag:?}]",)
            }
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        };
        BackfillProgress {
            progress,
            backfill_type: PbBackfillType::SnapshotBackfill,
        }
    }

    pub(super) fn pinned_upstream_tables(&self) -> &HashSet<TableId> {
        &self.control.info.snapshot_backfill_upstream_tables
    }

    fn inject_barrier(
        partial_graph_id: PartialGraphId,
        partial_graph_manager: &mut PartialGraphManager,
        node_actors: &HashMap<WorkerId, HashSet<ActorId>>,
        state_table_ids: &HashSet<TableId>,
        is_finishing: bool,
        barrier_info: BarrierInfo,
        new_actors: Option<StreamJobActorsToCreate>,
        mutation: Option<Mutation>,
        notifier: Option<&mut NotifierStarter>,
        first_create_info: Option<CreateIndependentStreamingJobCommandInfo>,
    ) -> MetaResult<()> {
        let (table_ids_to_sync, nodes_to_sync_table) = if !is_finishing {
            (Some(state_table_ids), Some(node_actors.keys().copied()))
        } else {
            (None, None)
        };
        partial_graph_manager.inject_barrier(
            partial_graph_id,
            mutation,
            node_actors,
            table_ids_to_sync.into_iter().flatten().copied(),
            nodes_to_sync_table.into_iter().flatten(),
            new_actors,
            PartialGraphBarrierInfo::new(
                first_create_info.map_or_else(
                    PostCollectCommand::barrier,
                    CreateIndependentStreamingJobCommandInfo::into_post_collect,
                ),
                barrier_info,
                notifier,
                state_table_ids.clone(),
            ),
        )?;
        Ok(())
    }

    pub(crate) fn start_consume_upstream(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        barrier_info: &BarrierInfo,
    ) -> MetaResult<CreatingJobInfo> {
        info!(
            job_id = %self.control.info.job_id,
            prev_epoch = barrier_info.prev_epoch(),
            "start consuming upstream"
        );
        let info = self.status.start_consume_upstream(barrier_info);
        Self::inject_barrier(
            self.control.info.partial_graph_id,
            partial_graph_manager,
            &self.node_actors,
            &self.control.info.state_table_ids,
            true,
            barrier_info.clone(),
            None,
            Some(Mutation::Stop(StopMutation {
                // stop all actors
                actors: info
                    .fragment_infos
                    .values()
                    .flat_map(|info| info.actors.keys().copied())
                    .collect(),
                dropped_sink_fragments: vec![], // not related to sink-into-table
            })),
            None, // no notifier when start consuming upstream
            None,
        )?;
        Ok(info)
    }

    pub(crate) fn on_new_upstream_barrier(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        barrier_info: &BarrierInfo,
        mutation: Option<(Mutation, Option<&mut NotifierStarter>)>,
    ) -> MetaResult<()> {
        let progress_epoch = if let Some(max_committed_epoch) = self.control.max_committed_epoch {
            max(max_committed_epoch, self.control.info.snapshot_epoch)
        } else {
            self.control.info.snapshot_epoch
        };
        self.upstream_lag.set(
            barrier_info
                .prev_epoch
                .value()
                .0
                .saturating_sub(progress_epoch) as _,
        );
        let (mut mutation, mut notifier) = match mutation {
            Some((mutation, notifier)) => (Some(mutation), notifier),
            None => (None, None),
        };
        for (barrier_to_inject, mutation) in self.status.on_new_upstream_epoch(
            partial_graph_manager,
            self.control.info.partial_graph_id,
            self.max_pending_barrier_num,
            barrier_info,
            mutation.take(),
        ) {
            Self::inject_barrier(
                self.control.info.partial_graph_id,
                partial_graph_manager,
                &self.node_actors,
                &self.control.info.state_table_ids,
                false,
                barrier_to_inject,
                None,
                mutation,
                notifier.take(),
                None,
            )?;
        }
        Ok(())
    }

    pub(crate) fn pre_apply_throttle(
        &mut self,
        config: &mut ThrottleConfigMap,
    ) -> Option<Mutation> {
        self.status.pre_apply_throttle(config)
    }

    /// Returns whether the next barrier should be forced to a checkpoint.
    pub(crate) fn collect(&mut self, collected_barrier: CollectedBarrier<'_>) -> bool {
        let pending_barrier_num = collected_barrier.pending_barrier_num;
        self.status.update_progress(
            collected_barrier
                .resps
                .values()
                .flat_map(|resp| &resp.create_mview_progress),
        );
        self.is_ready_to_merge() && pending_barrier_num <= self.max_lagged_barrier_num
    }

    fn is_ready_to_merge(&self) -> bool {
        if let CreatingStreamingJobStatus::ConsumingLogStore {
            log_store_progress_tracker,
            pending_barriers,
            ..
        } = &self.status
            && pending_barriers.is_empty()
            && log_store_progress_tracker.is_finished()
        {
            true
        } else {
            false
        }
    }

    pub(crate) fn should_merge_to_upstream(
        &self,
        partial_graph_manager: &PartialGraphManager,
    ) -> bool {
        if !self.is_ready_to_merge() {
            return false;
        }

        // A job that is ready to merge has finished initialization and is not resetting, so its
        // partial graph must be running.
        partial_graph_manager.pending_barrier_num(self.control.info.partial_graph_id)
            <= self.max_lagged_barrier_num
    }
}

impl CreatingStreamingJobControl {
    pub(crate) fn start_completing(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        min_upstream_inflight_epoch: Option<u64>,
    ) -> Option<(
        u64,
        HashMap<WorkerId, BarrierCompleteResponse>,
        PartialGraphBarrierInfo,
        bool,
    )> {
        let (finished_at_epoch, epoch_end_bound) = match &self.status {
            CreatingStreamingJobStatus::Finishing(finish_at_epoch, _) => {
                let epoch_end_bound = min_upstream_inflight_epoch
                    .map(|upstream_epoch| {
                        if upstream_epoch < *finish_at_epoch {
                            Excluded(upstream_epoch)
                        } else {
                            Unbounded
                        }
                    })
                    .unwrap_or(Unbounded);
                (Some(*finish_at_epoch), epoch_end_bound)
            }
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. } => (
                None,
                min_upstream_inflight_epoch
                    .map(Excluded)
                    .unwrap_or(Unbounded),
            ),
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        };
        partial_graph_manager
            .start_completing(
                self.control.info.partial_graph_id,
                epoch_end_bound,
                |non_checkpoint_epoch, _, _| {
                    if let Some(finish_at_epoch) = finished_at_epoch {
                        assert!(non_checkpoint_epoch.prev < finish_at_epoch);
                    }
                },
            )
            .map(|(epoch, resps, info)| {
                let is_finish_epoch = if let Some(finish_at_epoch) = finished_at_epoch {
                    assert!(!info.post_collect_command.should_checkpoint());
                    if epoch == finish_at_epoch {
                        // TODO: can early remove partial graph here
                        self.ack_completed(partial_graph_manager, epoch);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                (epoch, resps, info, is_finish_epoch)
            })
    }

    pub(super) fn ack_completed(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        completed_epoch: u64,
    ) {
        match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. }
            | CreatingStreamingJobStatus::Finishing(_, _) => {
                partial_graph_manager
                    .ack_completed(self.control.info.partial_graph_id, completed_epoch);
                self.control.ack_completed(completed_epoch);
            }
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }

    pub(crate) fn fragment_infos(&self) -> Option<&HashMap<FragmentId, InflightFragmentInfo>> {
        self.status.fragment_infos()
    }

    pub fn into_tracking_job(self) -> TrackingJob {
        match self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. }
            | CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!("expect finish")
            }
            CreatingStreamingJobStatus::Finishing(_, tracking_job) => tracking_job,
        }
    }

    /// Whether the job can be dropped by resetting its independent partial graph.
    ///
    /// A finishing job has already been merged into the database graph, so it must be handled by
    /// the database-graph drop command instead.
    pub(super) fn can_drop_independently(&self) -> bool {
        match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. } => true,
            CreatingStreamingJobStatus::Finishing(_, _) => false,
            CreatingStreamingJobStatus::PlaceHolder => {
                unreachable!()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manager::MetaOpts;

    #[test]
    fn test_snapshot_backfill_max_pending_barrier_num() {
        let mut opts = MetaOpts::test(false);
        opts.in_flight_barrier_nums = 10;

        opts.snapshot_backfill_barrier_amplification_factor = 0;
        assert_eq!(snapshot_backfill_max_pending_barrier_num(&opts), 10);

        opts.snapshot_backfill_barrier_amplification_factor = 1;
        assert_eq!(snapshot_backfill_max_pending_barrier_num(&opts), 10);

        opts.snapshot_backfill_barrier_amplification_factor = 10;
        assert_eq!(snapshot_backfill_max_pending_barrier_num(&opts), 100);

        opts.in_flight_barrier_nums = usize::MAX;
        assert_eq!(snapshot_backfill_max_pending_barrier_num(&opts), usize::MAX);
    }

    #[test]
    fn test_resolve_since_timestamp_upstream_log_epochs() {
        let upstream_log_epochs = vec![(vec![45, 50], 55)];

        let (initial_barrier, barriers) =
            CreatingStreamingJobControl::resolve_since_timestamp_upstream_log_epochs(
                &upstream_log_epochs,
                [].iter(),
                40,
                60,
            )
            .unwrap();

        assert_eq!(
            (initial_barrier.prev_epoch(), initial_barrier.curr_epoch()),
            (40, 45)
        );
        assert!(initial_barrier.kind.is_checkpoint());
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| (barrier.prev_epoch(), barrier.curr_epoch()))
                .collect::<Vec<_>>(),
            vec![(45, 50), (50, 55), (55, 60)]
        );
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| match &barrier.kind {
                    BarrierKind::Checkpoint(epochs) => Some(epochs.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![None, None, Some(vec![45, 50, 55])]
        );
    }

    #[test]
    fn test_resolve_since_timestamp_upstream_log_epochs_with_pending_barriers() {
        let upstream_log_epochs = vec![(vec![45, 50], 55)];
        let pending_upstream_barriers = [
            BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(60)),
                curr_epoch: TracedEpoch::new(Epoch(65)),
                kind: BarrierKind::Barrier,
            },
            BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(65)),
                curr_epoch: TracedEpoch::new(Epoch(70)),
                kind: BarrierKind::Checkpoint(vec![60, 65]),
            },
        ];

        let (initial_barrier, barriers) =
            CreatingStreamingJobControl::resolve_since_timestamp_upstream_log_epochs(
                &upstream_log_epochs,
                pending_upstream_barriers.iter(),
                40,
                70,
            )
            .unwrap();

        assert_eq!(
            (initial_barrier.prev_epoch(), initial_barrier.curr_epoch()),
            (40, 45)
        );
        assert!(initial_barrier.kind.is_checkpoint());
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| (barrier.prev_epoch(), barrier.curr_epoch()))
                .collect::<Vec<_>>(),
            vec![(45, 50), (50, 55), (55, 60), (60, 65), (65, 70)]
        );
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| match &barrier.kind {
                    BarrierKind::Checkpoint(epochs) => Some(epochs.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![None, None, Some(vec![45, 50, 55]), None, Some(vec![60, 65])]
        );
    }

    #[test]
    fn test_resolve_since_timestamp_upstream_log_epochs_with_gap_before_pending_barriers() {
        let upstream_log_epochs = vec![(vec![61, 62, 63, 64], 65)];
        let pending_upstream_barriers = [
            BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(66)),
                curr_epoch: TracedEpoch::new(Epoch(67)),
                kind: BarrierKind::Barrier,
            },
            BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(67)),
                curr_epoch: TracedEpoch::new(Epoch(68)),
                kind: BarrierKind::Barrier,
            },
            BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(68)),
                curr_epoch: TracedEpoch::new(Epoch(69)),
                kind: BarrierKind::Barrier,
            },
            BarrierInfo {
                prev_epoch: TracedEpoch::new(Epoch(69)),
                curr_epoch: TracedEpoch::new(Epoch(70)),
                kind: BarrierKind::Barrier,
            },
        ];

        let (initial_barrier, barriers) =
            CreatingStreamingJobControl::resolve_since_timestamp_upstream_log_epochs(
                &upstream_log_epochs,
                pending_upstream_barriers.iter(),
                60,
                70,
            )
            .unwrap();

        assert_eq!(
            (initial_barrier.prev_epoch(), initial_barrier.curr_epoch()),
            (60, 61)
        );
        assert!(initial_barrier.kind.is_checkpoint());
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| (barrier.prev_epoch(), barrier.curr_epoch()))
                .collect::<Vec<_>>(),
            vec![
                (61, 62),
                (62, 63),
                (63, 64),
                (64, 65),
                (65, 66),
                (66, 67),
                (67, 68),
                (68, 69),
                (69, 70)
            ]
        );
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| match &barrier.kind {
                    BarrierKind::Checkpoint(epochs) => Some(epochs.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![
                None,
                None,
                None,
                None,
                Some(vec![61, 62, 63, 64, 65]),
                None,
                None,
                None,
                None
            ]
        );
    }

    #[test]
    fn test_resolve_since_timestamp_upstream_log_epochs_without_pending_barriers() {
        let upstream_log_epochs = vec![(vec![61, 62, 63, 64], 65)];

        let (initial_barrier, barriers) =
            CreatingStreamingJobControl::resolve_since_timestamp_upstream_log_epochs(
                &upstream_log_epochs,
                [].iter(),
                60,
                66,
            )
            .unwrap();

        assert_eq!(
            (initial_barrier.prev_epoch(), initial_barrier.curr_epoch()),
            (60, 61)
        );
        assert!(initial_barrier.kind.is_checkpoint());
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| (barrier.prev_epoch(), barrier.curr_epoch()))
                .collect::<Vec<_>>(),
            vec![(61, 62), (62, 63), (63, 64), (64, 65), (65, 66)]
        );
        assert_eq!(
            barriers
                .iter()
                .map(|barrier| match &barrier.kind {
                    BarrierKind::Checkpoint(epochs) => Some(epochs.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![None, None, None, None, Some(vec![61, 62, 63, 64, 65])]
        );
    }
}
