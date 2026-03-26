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

//! Batch refresh job checkpoint control for snapshot-only materialized views.
//!
//! It lives permanently in `DatabaseCheckpointControl.independent_checkpoint_job_controls`
//! as an `IndependentCheckpointJobControl::BatchRefresh` variant for its entire lifetime.
//!
//! Lifecycle for first run (snapshot only):
//!   DDL → `ConsumingSnapshot` → snapshot done → `Stopping` → stop actors committed → `Resetting` → `Idle`

mod barrier_control;

use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::ops::Bound::Excluded;

use barrier_control::BatchRefreshBarrierStats;
use itertools::Itertools;
use risingwave_common::catalog::{DatabaseId, TableId};
use risingwave_common::id::JobId;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_meta_model::{DispatcherType, WorkerId};
use risingwave_pb::ddl_service::PbBackfillType;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::id::{ActorId, FragmentId, PartialGraphId};
use risingwave_pb::stream_plan::barrier::PbBarrierKind;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{AddMutation, StartFragmentBackfillMutation, StopMutation};
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::{debug, info};

use super::creating_job::CreatingJobInfo;
use super::state::RenderResult;
use crate::MetaResult;
use crate::barrier::backfill_order_control::get_nodes_with_backfill_dependencies;
use crate::barrier::command::PostCollectCommand;
use crate::barrier::context::CreateSnapshotBackfillJobCommandInfo;
use crate::barrier::edge_builder::FragmentEdgeBuildResult;
use crate::barrier::info::BarrierInfo;
use crate::barrier::notifier::Notifier;
use crate::barrier::partial_graph::{
    CollectedBarrier, PartialGraphBarrierInfo, PartialGraphManager,
};
use crate::barrier::progress::{CreateMviewProgressTracker, TrackingJob, collect_done_fragments};
use crate::barrier::rpc::to_partial_graph_id;
use crate::barrier::{
    BackfillOrderState, BackfillProgress, BarrierKind, Command, FragmentBackfillProgress,
    TracedEpoch,
};
use crate::controller::fragment::InflightFragmentInfo;
use crate::model::{FragmentDownstreamRelation, StreamJobActorsToCreate};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::source_manager::SplitAssignment;
use crate::stream::{ExtendedFragmentBackfillOrder, build_actor_connector_splits};

// ── Public types ──────────────────────────────────────────────────────────────

/// Information about a batch refresh job stored at creation time.
#[derive(Debug, Clone)]
pub(crate) struct BatchRefreshJobInfo {
    /// The upstream MV table IDs that this job subscribes to.
    pub upstream_table_ids: HashSet<TableId>,
}

// ── Status ────────────────────────────────────────────────────────────────────

#[derive(Debug)]
enum BatchRefreshJobStatus {
    /// The job is consuming upstream snapshot.
    ///
    /// Once snapshot consumption finishes, the final checkpoint + stop barriers are injected
    /// and `stop_at_epoch` is set. When that epoch is committed, the job transitions to idle.
    ConsumingSnapshot {
        prev_epoch_fake_physical_time: u64,
        version_stats: HummockVersionStats,
        create_mview_tracker: CreateMviewProgressTracker,
        snapshot_epoch: u64,
        info: CreatingJobInfo,
        pending_non_checkpoint_barriers: Vec<u64>,
        /// Set when the stop barrier has been injected. The value is the `prev_epoch`
        /// of the stop barrier. When this epoch is committed, the job transitions to idle.
        stop_at_epoch: Option<u64>,
    },
    /// The job is idle, waiting for the next trigger. No partial graph or actors.
    Idle { last_committed_epoch: u64 },
    /// The partial graph is being reset (only for drop).
    Resetting {
        notifiers: Vec<Notifier>,
    },
    /// Temporary variant for `mem::replace` transitions.
    PlaceHolder,
}

// ── Complete type ─────────────────────────────────────────────────────────────

// ── Main checkpoint control ───────────────────────────────────────────────────

/// Self-contained checkpoint control for a batch refresh MV.
///
/// Unlike `CreatingStreamingJobControl`, this struct handles the full lifecycle
/// (snapshot → idle → re-run → idle → ...). Both types are stored together in
/// `DatabaseCheckpointControl.independent_checkpoint_job_controls` as
/// `IndependentCheckpointJobControl` variants.
#[derive(Debug)]
pub(crate) struct BatchRefreshJobCheckpointControl {
    job_id: JobId,
    partial_graph_id: PartialGraphId,
    snapshot_backfill_upstream_tables: HashSet<TableId>,
    snapshot_epoch: u64,

    node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    state_table_ids: HashSet<TableId>,

    max_committed_epoch: Option<u64>,
    status: BatchRefreshJobStatus,
    /// Used exactly once to notify catalog/frontends that the initial create finished.
    tracking_job: Option<TrackingJob>,

    upstream_lag: LabelGuardedIntGauge,
}

// ── Status helper methods ─────────────────────────────────────────────────────

impl BatchRefreshJobStatus {
    fn new_fake_barrier(
        prev_epoch_fake_physical_time: &mut u64,
        pending_non_checkpoint_barriers: &mut Vec<u64>,
        kind: PbBarrierKind,
    ) -> BarrierInfo {
        let prev_epoch =
            TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
        *prev_epoch_fake_physical_time += 1;
        let curr_epoch =
            TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
        let kind = match kind {
            PbBarrierKind::Unspecified => unreachable!(),
            PbBarrierKind::Initial => {
                assert!(pending_non_checkpoint_barriers.is_empty());
                BarrierKind::Initial
            }
            PbBarrierKind::Barrier => {
                pending_non_checkpoint_barriers.push(prev_epoch.value().0);
                BarrierKind::Barrier
            }
            PbBarrierKind::Checkpoint => {
                pending_non_checkpoint_barriers.push(prev_epoch.value().0);
                BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers))
            }
        };
        BarrierInfo {
            prev_epoch,
            curr_epoch,
            kind,
        }
    }

    /// Applies progress updates and returns `true` if snapshot is finished.
    fn update_progress(
        &mut self,
        create_mview_progress: impl IntoIterator<
            Item = &risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress,
        >,
    ) -> bool {
        match self {
            BatchRefreshJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                version_stats,
                ..
            } => {
                for progress in create_mview_progress {
                    create_mview_tracker.apply_progress(progress, version_stats);
                }
                create_mview_tracker.is_finished()
            }
            _ => false,
        }
    }

    fn on_new_upstream_epoch(
        &mut self,
        barrier_info: &BarrierInfo,
        mutation: Option<Mutation>,
    ) -> Vec<(BarrierInfo, Option<Mutation>)> {
        match self {
            BatchRefreshJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time,
                pending_non_checkpoint_barriers,
                create_mview_tracker,
                snapshot_epoch,
                info,
                stop_at_epoch,
                ..
            } => {
                if stop_at_epoch.is_some() {
                    // Already injected stop barrier, no more barriers.
                    return vec![];
                }

                if create_mview_tracker.is_finished() {
                    // Snapshot finished — inject final checkpoint + stop barrier.
                    pending_non_checkpoint_barriers.push(*snapshot_epoch);
                    let prev_epoch = Epoch::from_physical_time(*prev_epoch_fake_physical_time);
                    let final_checkpoint = BarrierInfo {
                        curr_epoch: TracedEpoch::new(Epoch(*snapshot_epoch)),
                        prev_epoch: TracedEpoch::new(prev_epoch),
                        kind: BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers)),
                    };

                    *prev_epoch_fake_physical_time = max(
                        *prev_epoch_fake_physical_time,
                        Epoch(*snapshot_epoch).physical_time(),
                    );

                    let stop_barrier = Self::new_fake_barrier(
                        prev_epoch_fake_physical_time,
                        pending_non_checkpoint_barriers,
                        PbBarrierKind::Checkpoint,
                    );

                    *stop_at_epoch = Some(stop_barrier.prev_epoch());

                    let stop_actors: Vec<ActorId> = info
                        .fragment_infos
                        .values()
                        .flat_map(|f| f.actors.keys().copied())
                        .collect();

                    return vec![
                        (final_checkpoint, None),
                        (
                            stop_barrier,
                            Some(Mutation::Stop(StopMutation {
                                actors: stop_actors,
                                dropped_sink_fragments: vec![],
                            })),
                        ),
                    ];
                }

                // Still consuming snapshot — forward a fake barrier.
                let mutation = mutation.or_else(|| {
                    let pending_backfill_nodes = create_mview_tracker
                        .take_pending_backfill_nodes()
                        .collect_vec();
                    if pending_backfill_nodes.is_empty() {
                        None
                    } else {
                        Some(Mutation::StartFragmentBackfill(
                            StartFragmentBackfillMutation {
                                fragment_ids: pending_backfill_nodes,
                            },
                        ))
                    }
                });
                vec![(
                    Self::new_fake_barrier(
                        prev_epoch_fake_physical_time,
                        pending_non_checkpoint_barriers,
                        match barrier_info.kind {
                            BarrierKind::Barrier => PbBarrierKind::Barrier,
                            BarrierKind::Checkpoint(_) => PbBarrierKind::Checkpoint,
                            BarrierKind::Initial => {
                                unreachable!("upstream new epoch should not be initial")
                            }
                        },
                    ),
                    mutation,
                )]
            }
            BatchRefreshJobStatus::Idle { .. } | BatchRefreshJobStatus::Resetting { .. } => vec![],
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        }
    }

    fn fragment_infos(&self) -> Option<&HashMap<FragmentId, InflightFragmentInfo>> {
        match self {
            BatchRefreshJobStatus::ConsumingSnapshot { info, .. } => Some(&info.fragment_infos),
            BatchRefreshJobStatus::Idle { .. } | BatchRefreshJobStatus::Resetting { .. } => None,
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        }
    }

    fn creating_job_info(&self) -> Option<&CreatingJobInfo> {
        match self {
            BatchRefreshJobStatus::ConsumingSnapshot { info, .. } => Some(info),
            BatchRefreshJobStatus::Idle { .. } | BatchRefreshJobStatus::Resetting { .. } => None,
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        }
    }
}

// ── Construction ──────────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    /// Create from DDL command. Starts in `ConsumingSnapshot`.
    pub(super) fn new(
        create_info: CreateSnapshotBackfillJobCommandInfo,
        notifiers: Vec<Notifier>,
        snapshot_backfill_upstream_tables: HashSet<TableId>,
        snapshot_epoch: u64,
        version_stat: &HummockVersionStats,
        partial_graph_manager: &mut PartialGraphManager,
        edges: &mut FragmentEdgeBuildResult,
        split_assignment: &SplitAssignment,
        actors: &RenderResult,
    ) -> MetaResult<Self> {
        let info = create_info.info.clone();
        let job_id = info.stream_job_fragments.stream_job_id();
        let database_id = info.streaming_job.database_id();

        debug!(
            %job_id,
            definition = info.definition,
            "new batch refresh job"
        );

        let fragment_infos: HashMap<FragmentId, InflightFragmentInfo> = info
            .stream_job_fragments
            .new_fragment_info(
                &actors.stream_actors,
                &actors.actor_location,
                split_assignment,
            )
            .collect();
        let backfill_nodes_to_pause =
            get_nodes_with_backfill_dependencies(&info.fragment_backfill_ordering)
                .into_iter()
                .collect();
        let backfill_order_state = BackfillOrderState::new(
            &info.fragment_backfill_ordering,
            &fragment_infos,
            info.locality_fragment_state_table_mapping.clone(),
        );
        let create_mview_tracker = CreateMviewProgressTracker::recover(
            job_id,
            &fragment_infos,
            backfill_order_state,
            version_stat,
        );

        let actors_to_create = Command::create_streaming_job_actors_to_create(
            &info,
            edges,
            &actors.stream_actors,
            &actors.actor_location,
        );

        let mut prev_epoch_fake_physical_time = 0;
        let mut pending_non_checkpoint_barriers = vec![];

        let initial_barrier_info = BatchRefreshJobStatus::new_fake_barrier(
            &mut prev_epoch_fake_physical_time,
            &mut pending_non_checkpoint_barriers,
            PbBarrierKind::Checkpoint,
        );

        let added_actors: Vec<ActorId> = actors
            .stream_actors
            .values()
            .flatten()
            .map(|actor| actor.actor_id)
            .collect();
        let actor_splits = split_assignment
            .values()
            .flat_map(build_actor_connector_splits)
            .collect();

        let initial_mutation = Mutation::Add(AddMutation {
            actor_dispatchers: Default::default(),
            added_actors,
            actor_splits,
            pause: false,
            subscriptions_to_add: Default::default(),
            backfill_nodes_to_pause,
            actor_cdc_table_snapshot_splits: None,
            new_upstream_sinks: Default::default(),
        });

        let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());
        let state_table_ids =
            InflightFragmentInfo::existing_table_ids(fragment_infos.values()).collect();

        let partial_graph_id = to_partial_graph_id(database_id, Some(job_id));
        let tracking_job = TrackingJob::new(&info.stream_job_fragments);

        let job_info = CreatingJobInfo {
            fragment_infos,
            upstream_fragment_downstreams: Default::default(),
            downstreams: info.stream_job_fragments.downstreams,
            snapshot_backfill_upstream_tables: snapshot_backfill_upstream_tables.clone(),
            stream_actors: actors
                .stream_actors
                .values()
                .flatten()
                .map(|actor| (actor.actor_id, actor.clone()))
                .collect(),
        };

        let mut job = Self {
            partial_graph_id,
            job_id,
            snapshot_backfill_upstream_tables,
            snapshot_epoch,
            max_committed_epoch: None,
            status: BatchRefreshJobStatus::PlaceHolder,
            tracking_job: Some(tracking_job),
            upstream_lag: GLOBAL_META_METRICS
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&format!("{}", job_id)]),
            node_actors,
            state_table_ids,
        };

        let mut graph_adder = partial_graph_manager.add_partial_graph(
            partial_graph_id,
            BatchRefreshBarrierStats::new(job_id, snapshot_epoch),
        );

        if let Err(e) = Self::inject_barrier(
            partial_graph_id,
            graph_adder.manager(),
            &job.node_actors,
            &job.state_table_ids,
            initial_barrier_info,
            Some(actors_to_create),
            Some(initial_mutation),
            notifiers,
            Some(create_info),
        ) {
            graph_adder.failed();
            job.status = BatchRefreshJobStatus::Resetting {
                notifiers: vec![],
            };
            return Err(e);
        }

        graph_adder.added();
        assert!(pending_non_checkpoint_barriers.is_empty());
        job.status = BatchRefreshJobStatus::ConsumingSnapshot {
            prev_epoch_fake_physical_time,
            version_stats: version_stat.clone(),
            create_mview_tracker,
            snapshot_epoch,
            info: job_info,
            pending_non_checkpoint_barriers,
            stop_at_epoch: None,
        };

        Ok(job)
    }

    /// Recover from a persistent state during recovery.
    ///
    /// - If `committed_epoch >= snapshot_epoch` → Idle (snapshot completed before crash).
    /// - If `committed_epoch < snapshot_epoch` → `ConsumingSnapshot` (re-render actors, restart).
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn recover(
        database_id: DatabaseId,
        job_id: JobId,
        snapshot_backfill_upstream_tables: HashSet<TableId>,
        snapshot_epoch: u64,
        committed_epoch: u64,
        fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
        backfill_order: ExtendedFragmentBackfillOrder,
        fragment_relations: &FragmentDownstreamRelation,
        version_stat: &HummockVersionStats,
        new_actors: StreamJobActorsToCreate,
        initial_mutation: Mutation,
        batch_refresh_info: BatchRefreshJobInfo,
        partial_graph_recoverer: &mut crate::barrier::partial_graph::PartialGraphRecoverer<'_>,
    ) -> MetaResult<Self> {
        if committed_epoch >= snapshot_epoch {
            // Snapshot completed; recover to Idle (no partial graph needed).
            info!(
                %job_id,
                committed_epoch,
                snapshot_epoch,
                "recovered batch refresh job to idle"
            );
            return Ok(Self {
                job_id,
                partial_graph_id: to_partial_graph_id(database_id, Some(job_id)),
                snapshot_backfill_upstream_tables: batch_refresh_info.upstream_table_ids,
                snapshot_epoch,
                node_actors: Default::default(),
                state_table_ids: Default::default(),
                max_committed_epoch: Some(committed_epoch),
                status: BatchRefreshJobStatus::Idle {
                    last_committed_epoch: committed_epoch,
                },
                tracking_job: None,
                upstream_lag: GLOBAL_META_METRICS
                    .snapshot_backfill_lag
                    .with_guarded_label_values(&[&format!("{}", job_id)]),
            });
        }

        // Snapshot still in-progress; recover to ConsumingSnapshot.
        info!(
            %job_id,
            committed_epoch,
            snapshot_epoch,
            "recovered batch refresh job to consuming snapshot"
        );

        let barrier_control_committed_epoch = Some(committed_epoch);

        let node_actors = InflightFragmentInfo::actor_ids_to_collect(fragment_infos.values());
        let state_table_ids: HashSet<_> =
            InflightFragmentInfo::existing_table_ids(fragment_infos.values()).collect();

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
            upstream_fragment_downstreams: Default::default(),
            downstreams,
            snapshot_backfill_upstream_tables: snapshot_backfill_upstream_tables.clone(),
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

        let mut prev_epoch_fake_physical_time = Epoch(committed_epoch).physical_time();
        let mut pending_non_checkpoint_barriers = vec![];

        let locality_fragment_state_table_mapping =
            crate::barrier::rpc::build_locality_fragment_state_table_mapping(&info.fragment_infos);
        let backfill_order_state = BackfillOrderState::recover_from_fragment_infos(
            &backfill_order,
            &info.fragment_infos,
            locality_fragment_state_table_mapping,
        );

        let create_mview_tracker = CreateMviewProgressTracker::recover(
            job_id,
            &info.fragment_infos,
            backfill_order_state,
            version_stat,
        );

        let first_barrier_info = BatchRefreshJobStatus::new_fake_barrier(
            &mut prev_epoch_fake_physical_time,
            &mut pending_non_checkpoint_barriers,
            PbBarrierKind::Initial,
        );

        let partial_graph_id = to_partial_graph_id(database_id, Some(job_id));
        let tracking_job = TrackingJob::recovered(job_id, &info.fragment_infos);

        partial_graph_recoverer.recover_graph(
            partial_graph_id,
            initial_mutation,
            &first_barrier_info,
            &node_actors,
            state_table_ids.iter().copied(),
            new_actors,
            BatchRefreshBarrierStats::new(job_id, snapshot_epoch),
        )?;

        Ok(Self {
            job_id,
            partial_graph_id,
            snapshot_backfill_upstream_tables: batch_refresh_info.upstream_table_ids,
            snapshot_epoch,
            node_actors,
            state_table_ids,
            max_committed_epoch: barrier_control_committed_epoch,
            status: BatchRefreshJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time,
                version_stats: version_stat.clone(),
                create_mview_tracker,
                info,
                snapshot_epoch,
                pending_non_checkpoint_barriers,
                stop_at_epoch: None,
            },
            tracking_job: Some(tracking_job),
            upstream_lag: GLOBAL_META_METRICS
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&format!("{}", job_id)]),
        })
    }
}

// ── Barrier injection ─────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    fn inject_barrier(
        partial_graph_id: PartialGraphId,
        partial_graph_manager: &mut PartialGraphManager,
        node_actors: &HashMap<WorkerId, HashSet<ActorId>>,
        state_table_ids: &HashSet<TableId>,
        barrier_info: BarrierInfo,
        new_actors: Option<StreamJobActorsToCreate>,
        mutation: Option<Mutation>,
        notifiers: Vec<Notifier>,
        first_create_info: Option<CreateSnapshotBackfillJobCommandInfo>,
    ) -> MetaResult<()> {
        partial_graph_manager.inject_barrier(
            partial_graph_id,
            mutation,
            node_actors,
            state_table_ids.iter().copied(),
            node_actors.keys().copied(),
            new_actors,
            PartialGraphBarrierInfo::new(
                first_create_info.map_or_else(
                    PostCollectCommand::barrier,
                    CreateSnapshotBackfillJobCommandInfo::into_post_collect,
                ),
                barrier_info,
                notifiers,
                state_table_ids.clone(),
            ),
        )?;
        Ok(())
    }
}

// ── Barrier forwarding and collection ─────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    pub(super) fn on_new_upstream_barrier(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        barrier_info: &BarrierInfo,
        mutation: Option<(Mutation, Vec<Notifier>)>,
    ) -> MetaResult<()> {
        if matches!(
            self.status,
            BatchRefreshJobStatus::Idle { .. } | BatchRefreshJobStatus::Resetting { .. }
        ) {
            return Ok(());
        }

        let progress_epoch =
            if let Some(max_committed_epoch) = self.max_committed_epoch {
                max(max_committed_epoch, self.snapshot_epoch)
            } else {
                self.snapshot_epoch
            };
        self.upstream_lag.set(
            barrier_info
                .prev_epoch
                .value()
                .0
                .saturating_sub(progress_epoch) as _,
        );
        let (mut mutation, mut notifiers) = match mutation {
            Some((mutation, notifiers)) => (Some(mutation), notifiers),
            None => (None, vec![]),
        };
        for (barrier_to_inject, mutation) in self
            .status
            .on_new_upstream_epoch(barrier_info, mutation.take())
        {
            Self::inject_barrier(
                self.partial_graph_id,
                partial_graph_manager,
                &self.node_actors,
                &self.state_table_ids,
                barrier_to_inject,
                None,
                mutation,
                take(&mut notifiers),
                None,
            )?;
        }
        assert!(mutation.is_none(), "must have consumed mutation");
        assert!(notifiers.is_empty(), "must consumed notifiers");
        Ok(())
    }

    pub(crate) fn collect(&mut self, collected_barrier: CollectedBarrier<'_>) {
        self.status.update_progress(
            collected_barrier
                .resps
                .values()
                .flat_map(|resp| &resp.create_mview_progress),
        );
    }
}

// ── Completing ────────────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    pub(super) fn start_completing(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        min_upstream_inflight_epoch: Option<u64>,
        upstream_committed_epoch: u64,
    ) -> Option<(
        u64,
        HashMap<WorkerId, BarrierCompleteResponse>,
        PartialGraphBarrierInfo,
        bool,
    )> {
        if upstream_committed_epoch < self.snapshot_epoch {
            return None;
        }

        let (stop_at_epoch, epoch_end_bound) = match &self.status {
            BatchRefreshJobStatus::ConsumingSnapshot { stop_at_epoch, .. } => {
                let epoch_end_bound = if let Some(stop_epoch) = stop_at_epoch {
                    min_upstream_inflight_epoch
                        .map(|upstream_epoch| {
                            if upstream_epoch < *stop_epoch {
                                Excluded(upstream_epoch)
                            } else {
                                std::ops::Bound::Unbounded
                            }
                        })
                        .unwrap_or(std::ops::Bound::Unbounded)
                } else {
                    min_upstream_inflight_epoch
                        .map(Excluded)
                        .unwrap_or(std::ops::Bound::Unbounded)
                };
                (*stop_at_epoch, epoch_end_bound)
            }
            BatchRefreshJobStatus::Idle { .. } | BatchRefreshJobStatus::Resetting { .. } => {
                return None;
            }
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        };

        partial_graph_manager
            .start_completing(
                self.partial_graph_id,
                epoch_end_bound,
                |_non_checkpoint_epoch, resps, _| {
                    self.status.update_progress(
                        resps
                            .values()
                            .flat_map(|resp| &resp.create_mview_progress),
                    );
                },
            )
            .map(|(epoch, resps, info)| {
                self.status.update_progress(
                    resps
                        .values()
                        .flat_map(|resp| &resp.create_mview_progress),
                );
                let is_stop = if let Some(stop_epoch) = stop_at_epoch
                    && epoch == stop_epoch
                {
                    assert!(!info.post_collect_command.should_checkpoint());
                    self.ack_completed(partial_graph_manager, epoch);
                    true
                } else {
                    false
                };
                (epoch, resps, info, is_stop)
            })
    }

    pub(super) fn ack_completed(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        completed_epoch: u64,
    ) {
        partial_graph_manager.ack_completed(self.partial_graph_id, completed_epoch);
        if let Some(prev_max_committed_epoch) = self.max_committed_epoch.replace(completed_epoch) {
            assert!(completed_epoch > prev_max_committed_epoch);
        }
    }

    /// Transition to idle after the stop barrier has been committed.
    ///
    /// The partial graph is cleanly drained at this point, so we remove it
    /// (not reset) and go directly to `Idle`.
    pub(super) fn transition_to_idle(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
    ) {
        let last_committed_epoch = self
            .max_committed_epoch
            .unwrap_or(self.snapshot_epoch);
        info!(
            job_id = %self.job_id,
            last_committed_epoch,
            "batch refresh job: transitioned to idle"
        );

        partial_graph_manager.remove_partial_graphs(vec![self.partial_graph_id]);
        self.node_actors.clear();
        self.state_table_ids.clear();
        self.status = BatchRefreshJobStatus::Idle {
            last_committed_epoch,
        };
    }

    /// Called when the partial graph reset is confirmed (drop only).
    pub(super) fn on_partial_graph_reset(&mut self) {
        match &mut self.status {
            BatchRefreshJobStatus::Resetting { notifiers } => {
                for notifier in notifiers.drain(..) {
                    notifier.notify_collected();
                }
            }
            _ => {
                panic!(
                    "batch refresh job {}: on_partial_graph_reset in unexpected state {:?}",
                    self.job_id, self.status
                );
            }
        }
    }
}

// ── Query methods ─────────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    pub(crate) fn is_valid_after_worker_err(&self, worker_id: WorkerId) -> bool {
        self.status
            .fragment_infos()
            .map(|fragment_infos| {
                !InflightFragmentInfo::contains_worker(fragment_infos.values(), worker_id)
            })
            .unwrap_or(true)
    }

    pub(crate) fn gen_backfill_progress(&self) -> BackfillProgress {
        let progress = match &self.status {
            BatchRefreshJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                stop_at_epoch,
                ..
            } => {
                if stop_at_epoch.is_some() {
                    "BatchRefresh Stopping".to_owned()
                } else if create_mview_tracker.is_finished() {
                    "Snapshot finished".to_owned()
                } else {
                    let progress = create_mview_tracker.gen_backfill_progress();
                    format!("BatchRefresh Snapshot [{}]", progress)
                }
            }
            BatchRefreshJobStatus::Idle { .. } => "BatchRefresh Idle".to_owned(),
            BatchRefreshJobStatus::Resetting { .. } => "BatchRefresh Resetting".to_owned(),
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        };
        BackfillProgress {
            progress,
            backfill_type: PbBackfillType::SnapshotBackfill,
        }
    }

    pub(super) fn gen_fragment_backfill_progress(&self) -> Vec<FragmentBackfillProgress> {
        match &self.status {
            BatchRefreshJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                info,
                stop_at_epoch,
                ..
            } => {
                if stop_at_epoch.is_some() {
                    collect_done_fragments(self.job_id, &info.fragment_infos)
                } else {
                    create_mview_tracker.collect_fragment_progress(&info.fragment_infos, true)
                }
            }
            BatchRefreshJobStatus::Idle { .. } | BatchRefreshJobStatus::Resetting { .. } => vec![],
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        }
    }

    /// Returns the pinned upstream log epoch and upstream table IDs.
    pub(super) fn pinned_upstream_log_epoch(&self) -> (u64, HashSet<TableId>) {
        let epoch = match &self.status {
            BatchRefreshJobStatus::Idle {
                last_committed_epoch,
            } => *last_committed_epoch,
            _ => max(
                self.max_committed_epoch.unwrap_or(0),
                self.snapshot_epoch,
            ),
        };
        (epoch, self.snapshot_backfill_upstream_tables.clone())
    }

    pub(super) fn take_tracking_job(&mut self) -> Option<TrackingJob> {
        self.tracking_job.take()
    }

    pub fn fragment_infos_with_job_id(
        &self,
    ) -> impl Iterator<Item = (&InflightFragmentInfo, JobId)> + '_ {
        self.status
            .fragment_infos()
            .into_iter()
            .flat_map(|fragments| fragments.values().map(|fragment| (fragment, self.job_id)))
    }

    pub(super) fn collect_reschedule_blocked_fragment_ids(
        &self,
        blocked_fragment_ids: &mut HashSet<FragmentId>,
    ) {
        let Some(info) = self.status.creating_job_info() else {
            return;
        };
        for (fragment_id, fragment) in &info.fragment_infos {
            if fragment_has_online_unreschedulable_scan(fragment) {
                blocked_fragment_ids.insert(*fragment_id);
                collect_fragment_upstream_fragment_ids(fragment, blocked_fragment_ids);
            }
        }
    }

    pub(super) fn collect_no_shuffle_fragment_relations(
        &self,
        no_shuffle_relations: &mut Vec<(FragmentId, FragmentId)>,
    ) {
        let Some(info) = self.status.creating_job_info() else {
            return;
        };
        for (upstream_fragment_id, downstreams) in &info.upstream_fragment_downstreams {
            no_shuffle_relations.extend(
                downstreams
                    .iter()
                    .filter(|d| d.dispatcher_type == DispatcherType::NoShuffle)
                    .map(|d| (*upstream_fragment_id, d.downstream_fragment_id)),
            );
        }
        for (fragment_id, downstreams) in &info.downstreams {
            no_shuffle_relations.extend(
                downstreams
                    .iter()
                    .filter(|d| d.dispatcher_type == DispatcherType::NoShuffle)
                    .map(|d| (*fragment_id, d.downstream_fragment_id)),
            );
        }
    }
}

// ── Drop handling ─────────────────────────────────────────────────────────────

impl BatchRefreshJobCheckpointControl {
    /// Drop this batch refresh job.
    pub(super) fn drop(
        &mut self,
        notifiers: &mut Vec<Notifier>,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> bool {
        match &mut self.status {
            BatchRefreshJobStatus::Resetting {
                notifiers: existing_notifiers,
            } => {
                for notifier in &mut *notifiers {
                    notifier.notify_started();
                }
                existing_notifiers.append(notifiers);
                true
            }
            BatchRefreshJobStatus::ConsumingSnapshot { .. } => {
                for notifier in &mut *notifiers {
                    notifier.notify_started();
                }
                partial_graph_manager.reset_partial_graphs([self.partial_graph_id]);
                self.status = BatchRefreshJobStatus::Resetting {
                    notifiers: take(notifiers),
                };
                true
            }
            BatchRefreshJobStatus::Idle { .. } => {
                // No partial graph to reset; notify and allow removal.
                for mut notifier in notifiers.drain(..) {
                    notifier.notify_started();
                    notifier.notify_collected();
                }
                true
            }
            BatchRefreshJobStatus::PlaceHolder => unreachable!(),
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn fragment_has_online_unreschedulable_scan(fragment: &InflightFragmentInfo) -> bool {
    let mut has_unreschedulable_scan = false;
    visit_stream_node_cont(&fragment.nodes, |node| {
        if let Some(NodeBody::StreamScan(stream_scan)) = node.node_body.as_ref() {
            let scan_type = stream_scan.stream_scan_type();
            if !scan_type.is_reschedulable(true) {
                has_unreschedulable_scan = true;
                return false;
            }
        }
        true
    });
    has_unreschedulable_scan
}

fn collect_fragment_upstream_fragment_ids(
    fragment: &InflightFragmentInfo,
    upstream_fragment_ids: &mut HashSet<FragmentId>,
) {
    visit_stream_node_cont(&fragment.nodes, |node| {
        if let Some(NodeBody::Merge(merge)) = node.node_body.as_ref() {
            upstream_fragment_ids.insert(merge.upstream_fragment_id);
        }
        true
    });
}
