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

mod render;

use std::cmp::max;
use std::collections::{HashMap, HashSet, VecDeque, hash_map};
use std::mem::replace;
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::atomic::AtomicU32;

use risingwave_common::catalog::TableId;
use risingwave_common::id::{JobId, SinkId};
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntGauge};
use risingwave_common::util::epoch::{Epoch, EpochPair};
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_meta_model::{WorkerId, streaming_job};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::ddl_service::PbBackfillType;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::id::{ActorId, FragmentId, IcebergCompactionTaskId};
use risingwave_pb::stream_plan::IcebergPkIndexCompactionContext;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::iceberg_pk_index_compaction_context::{Phase, ResolverTaskInput};
use risingwave_pb::stream_service::BarrierCompleteResponse;

use self::render::{
    CompactionTransition, IcebergV3FragmentPartition, RenderedResolver,
    build_compaction_transitions, partition_resolver,
};
use super::{
    BatchRefreshRenderResult, CreatingStreamingJobControl, IndependentCheckpointJob,
    IndependentCheckpointJobControl, IndependentCheckpointJobStatus, IndependentJobControl,
    IndependentJobInfo, InitialPartialGraphRequest, RenderedIndependentJobActors,
    SnapshotPhaseControl, add_initial_partial_graph, build_initial_add_mutation,
};
use crate::MetaResult;
use crate::barrier::command::{
    IcebergPkIndexCompactionOverwrite, PostCollectCommand, ThrottleConfigMap,
    UpstreamTableLogEpochs, extract_throttle_config,
};
use crate::barrier::context::CreateIndependentStreamingJobCommandInfo;
use crate::barrier::info::BarrierInfo;
use crate::barrier::partial_graph::{
    CollectedBarrier, PartialGraphBarrierInfo, PartialGraphManager, PartialGraphRecoverer,
    PartialGraphStat,
};
use crate::barrier::progress::TrackingJob;
use crate::barrier::rpc::{ControlStreamManager, to_partial_graph_id};
use crate::barrier::{BackfillProgress, BarrierKind, FragmentBackfillProgress, TracedEpoch};
use crate::controller::fragment::InflightFragmentInfo;
use crate::controller::scale::LoadedFragment;
use crate::manager::iceberg_pk_index_sink::CompactionOverwrite;
use crate::model::{FragmentDownstreamRelation, StreamActor};
use crate::notification::{CollectionNotifier, NotifierStarter};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::ExtendedFragmentBackfillOrder;

struct IcebergV3BarrierStats {
    consuming_snapshot_barrier_latency: LabelGuardedHistogram,
    consuming_log_store_barrier_latency: LabelGuardedHistogram,
    inflight_barrier_num: LabelGuardedIntGauge,
    snapshot_epoch: u64,
}

fn synthetic_compaction_epoch(prev_epoch: Epoch, curr_epoch: Epoch) -> MetaResult<Epoch> {
    let synthetic_physical_time = prev_epoch.physical_time().checked_add(1).ok_or_else(|| {
        anyhow::anyhow!(
            "cannot advance synthetic compaction epoch after {}",
            prev_epoch.0
        )
    })?;
    let synthetic = Epoch::from_physical_time(synthetic_physical_time);
    if synthetic <= prev_epoch || synthetic >= curr_epoch {
        return Err(anyhow::anyhow!(
            "cannot allocate synthetic compaction epoch between {} and {}",
            prev_epoch.0,
            curr_epoch.0
        )
        .into());
    }
    Ok(synthetic)
}

impl IcebergV3BarrierStats {
    fn new(job_id: JobId, snapshot_epoch: u64) -> Self {
        let table_id_str = format!("{}", job_id);
        Self {
            snapshot_epoch,
            consuming_snapshot_barrier_latency: GLOBAL_META_METRICS
                .snapshot_backfill_barrier_latency
                .with_guarded_label_values(&[table_id_str.as_str(), "consuming_snapshot"]),
            consuming_log_store_barrier_latency: GLOBAL_META_METRICS
                .snapshot_backfill_barrier_latency
                .with_guarded_label_values(&[table_id_str.as_str(), "consuming_log_store"]),
            inflight_barrier_num: GLOBAL_META_METRICS
                .snapshot_backfill_inflight_barrier_num
                .with_guarded_label_values(&[&table_id_str]),
        }
    }
}

impl PartialGraphStat for IcebergV3BarrierStats {
    fn observe_barrier_latency(&self, epoch: EpochPair, barrier_latency_secs: f64) {
        let barrier_latency_metrics = if epoch.prev < self.snapshot_epoch {
            &self.consuming_snapshot_barrier_latency
        } else {
            &self.consuming_log_store_barrier_latency
        };
        barrier_latency_metrics.observe(barrier_latency_secs);
    }

    fn observe_barrier_num(&self, inflight_barrier_num: usize, _collected_barrier_num: usize) {
        self.inflight_barrier_num.set(inflight_barrier_num as _);
    }
}

#[derive(Debug)]
struct IcebergV3Input {
    phase: IcebergV3InputPhase,
    /// Original upstream barriers retained during snapshot consumption and then replayed from the
    /// log store after the snapshot finishes.
    pending_upstream_barriers: VecDeque<BarrierInfo>,
}

#[derive(Debug)]
enum IcebergV3InputPhase {
    Snapshot(SnapshotPhaseControl),
    LogStore { tracking_job: Option<TrackingJob> },
}

#[derive(Debug)]
enum IcebergV3JobStatus {
    Running(IcebergV3Input),
    Compacting {
        input: IcebergV3Input,
        apply: CompactionApply,
    },
}

#[derive(Debug)]
struct CompactionApply {
    notifier: CollectionNotifier,
    phase: CompactionApplyPhase,
}

#[derive(Debug)]
enum CompactionApplyPhase {
    /// B1 is in flight. Its acknowledgement injects B2 and transfers to
    /// `EndCompletion`.
    BeginAck {
        task_id: IcebergCompactionTaskId,
        begin_epoch: u64,
        end_barrier: BarrierInfo,
        /// Actors from the normal-input, resolver, and long-running partitions.
        node_actors: HashMap<WorkerId, HashSet<ActorId>>,
        end_transition: CompactionTransition,
        overwrite: CompactionOverwrite,
    },
    /// B2 is in flight and the state machine still owns the overwrite. Starting B2 completion
    /// moves the overwrite into `CompleteBarrierTask` and transfers to `EndAck`.
    EndCompletion {
        end_epoch: u64,
        overwrite: CompactionOverwrite,
    },
    /// B2 and its overwrite are being committed by `CompleteBarrierTask`.
    EndAck { end_epoch: u64 },
}

#[derive(Debug)]
pub(crate) struct IcebergV3RenderResult {
    active: BatchRefreshRenderResult,
    active_stream_actors: HashMap<FragmentId, Vec<StreamActor>>,
    active_downstreams: FragmentDownstreamRelation,
    resolver: RenderedResolver,
    partition: IcebergV3FragmentPartition,
}

impl IcebergV3RenderResult {
    pub(crate) fn active_fragment_infos(&self) -> &HashMap<FragmentId, InflightFragmentInfo> {
        &self.active.fragment_infos
    }

    pub(crate) fn active_downstreams(&self) -> &FragmentDownstreamRelation {
        &self.active_downstreams
    }

    pub(crate) fn all_fragment_infos(&self) -> impl Iterator<Item = &InflightFragmentInfo> + '_ {
        self.active
            .fragment_infos
            .values()
            .chain(std::iter::once(&self.resolver.fragment_info))
    }
}

/// Independent control for an Iceberg V3 sink. It owns snapshot and log-store progress for the
/// job's entire lifetime and retains the inactive resolver graph for future compaction commands.
pub(crate) struct IcebergV3JobCheckpointControl {
    control: IndependentJobControl,
    status: IcebergV3JobStatus,
    fragment_infos: HashMap<FragmentId, InflightFragmentInfo>,
    node_actors: HashMap<WorkerId, HashSet<ActorId>>,
    max_pending_barrier_num: usize,
    upstream_lag: risingwave_common::metrics::LabelGuardedIntGauge,
    /// Current actor definitions for the normal-input and long-running partitions. Scaling must
    /// update these together with `fragment_infos`.
    active_stream_actors: HashMap<FragmentId, Vec<StreamActor>>,
    /// Fragment-level relations of the active graph. Actor-level transition edges are derived from
    /// these relations for each compaction command.
    active_downstreams: FragmentDownstreamRelation,
    resolver: RenderedResolver,
    partition: IcebergV3FragmentPartition,
}

impl IcebergV3JobCheckpointControl {
    pub(crate) fn render_actors_and_build_job_info(
        fragments: &HashMap<FragmentId, LoadedFragment>,
        downstreams: &FragmentDownstreamRelation,
        definition: &str,
        actor_id_generator: &AtomicU32,
        worker_nodes: &HashMap<WorkerId, WorkerNode>,
        control_stream_manager: &ControlStreamManager,
        database_resource_group: &str,
        streaming_job_model: &streaming_job::Model,
        partial_graph_id: risingwave_pb::id::PartialGraphId,
    ) -> MetaResult<IcebergV3RenderResult> {
        let rendered = RenderedIndependentJobActors::render(
            fragments,
            downstreams,
            definition,
            actor_id_generator,
            worker_nodes,
            database_resource_group,
            streaming_job_model,
        )?;
        let mut active_downstreams = downstreams.clone();
        let (active, resolver, partition) = partition_resolver(rendered, &mut active_downstreams)?;
        let active_stream_actors = active.stream_actors.clone();
        let active = active.build_job_info(
            &active_downstreams,
            partial_graph_id,
            control_stream_manager,
        )?;
        Ok(IcebergV3RenderResult {
            active,
            active_stream_actors,
            active_downstreams,
            resolver,
            partition,
        })
    }

    pub(crate) fn create<'a>(
        entry: hash_map::VacantEntry<'a, JobId, IndependentCheckpointJobControl>,
        create_info: CreateIndependentStreamingJobCommandInfo,
        notifier: Option<&mut NotifierStarter>,
        snapshot_backfill_upstream_tables: HashSet<TableId>,
        snapshot_epoch: u64,
        version_stat: &HummockVersionStats,
        term_id: &str,
        partial_graph_manager: &mut PartialGraphManager,
        render_result: IcebergV3RenderResult,
    ) -> MetaResult<&'a mut Self> {
        let info = create_info.info.clone();
        let job_id = info.stream_job_fragments.stream_job_id();
        let database_id = info.streaming_job.database_id();
        let partial_graph_id = to_partial_graph_id(database_id, Some(job_id));

        let IcebergV3RenderResult {
            active:
                BatchRefreshRenderResult {
                    fragment_infos,
                    node_actors,
                    actors_to_create,
                },
            active_stream_actors,
            active_downstreams,
            resolver,
            partition,
        } = render_result;
        let (snapshot, initial_barrier_info) =
            SnapshotPhaseControl::for_new_job(snapshot_epoch, &fragment_infos, &info, version_stat);
        assert!(
            info.cdc_table_snapshot_splits.is_none(),
            "should not have cdc backfill for snapshot backfill job"
        );
        let initial_mutation = build_initial_add_mutation(
            &fragment_infos,
            &info.fragment_backfill_ordering,
            Default::default(),
        );

        let max_pending_barrier_num = super::snapshot_backfill_max_pending_barrier_num(
            &partial_graph_manager.control_stream_manager().env.opts,
        );

        let iceberg_job = Self {
            control: IndependentJobControl::new(IndependentJobInfo::from_fragment_infos(
                database_id,
                job_id,
                snapshot_epoch,
                snapshot_backfill_upstream_tables,
                fragment_infos
                    .values()
                    .chain(std::iter::once(&resolver.fragment_info)),
            )),
            status: IcebergV3JobStatus::Running(IcebergV3Input {
                phase: IcebergV3InputPhase::Snapshot(snapshot),
                pending_upstream_barriers: VecDeque::new(),
            }),
            fragment_infos,
            upstream_lag: GLOBAL_META_METRICS
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&format!("{}", job_id)]),
            node_actors,
            max_pending_barrier_num,
            active_stream_actors,
            active_downstreams,
            resolver,
            partition,
        };

        let mut graph_adder = partial_graph_manager.add_partial_graph(
            partial_graph_id,
            term_id,
            IcebergV3BarrierStats::new(job_id, snapshot_epoch),
        );
        if let Err(error) = add_initial_partial_graph(
            &mut graph_adder,
            partial_graph_id,
            InitialPartialGraphRequest {
                node_actors: &iceberg_job.node_actors,
                state_table_ids: &iceberg_job.control.info.state_table_ids,
                barrier_info: initial_barrier_info,
                actors_to_create,
                mutation: initial_mutation,
                notifier,
                create_info,
            },
        ) {
            graph_adder.failed();
            entry.insert(IndependentCheckpointJobControl::resetting(
                iceberg_job.control.info.snapshot_backfill_upstream_tables,
            ));
            return Err(error);
        }
        graph_adder.added();

        let control = entry.insert(IndependentCheckpointJobControl::iceberg_v3(
            job_id,
            partial_graph_id,
            IndependentCheckpointJobStatus::Initial { snapshot_epoch },
            iceberg_job,
        ));
        let Some(IndependentCheckpointJob::IcebergV3(job)) = control.running_mut() else {
            unreachable!()
        };
        Ok(job)
    }

    fn recover_snapshot_phase(
        job_id: JobId,
        snapshot_backfill_upstream_tables: &HashSet<TableId>,
        upstream_table_log_epochs: &UpstreamTableLogEpochs,
        snapshot_epoch: u64,
        committed_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
        fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
        backfill_order: &ExtendedFragmentBackfillOrder,
        version_stat: &HummockVersionStats,
    ) -> MetaResult<(IcebergV3Input, BarrierInfo)> {
        let (snapshot, first_barrier_info) = SnapshotPhaseControl::for_recovery(
            job_id,
            snapshot_epoch,
            committed_epoch,
            fragment_infos,
            backfill_order,
            version_stat,
        );
        Ok((
            IcebergV3Input {
                phase: IcebergV3InputPhase::Snapshot(snapshot),
                pending_upstream_barriers:
                    CreatingStreamingJobControl::resolve_upstream_log_epochs(
                        snapshot_backfill_upstream_tables,
                        upstream_table_log_epochs,
                        snapshot_epoch,
                        upstream_barrier_info,
                    )?
                    .into(),
            },
            first_barrier_info,
        ))
    }

    fn recover_log_store_phase(
        snapshot_backfill_upstream_tables: &HashSet<TableId>,
        upstream_table_log_epochs: &UpstreamTableLogEpochs,
        committed_epoch: u64,
        upstream_barrier_info: &BarrierInfo,
    ) -> MetaResult<(IcebergV3Input, BarrierInfo)> {
        let mut pending_upstream_barriers: VecDeque<_> =
            CreatingStreamingJobControl::resolve_upstream_log_epochs(
                snapshot_backfill_upstream_tables,
                upstream_table_log_epochs,
                committed_epoch,
                upstream_barrier_info,
            )?
            .into();
        let mut first_barrier_info = pending_upstream_barriers
            .pop_front()
            .expect("resolved upstream log epochs should not be empty");
        assert!(first_barrier_info.kind.is_checkpoint());
        first_barrier_info.kind = BarrierKind::Initial;
        Ok((
            IcebergV3Input {
                phase: IcebergV3InputPhase::LogStore { tracking_job: None },
                pending_upstream_barriers,
            },
            first_barrier_info,
        ))
    }

    pub(crate) fn recover(
        control: IndependentJobControl,
        upstream_table_log_epochs: &UpstreamTableLogEpochs,
        upstream_barrier_info: &BarrierInfo,
        backfill_order: ExtendedFragmentBackfillOrder,
        version_stat: &HummockVersionStats,
        initial_mutation: Mutation,
        term_id: &str,
        partial_graph_recoverer: &mut PartialGraphRecoverer<'_>,
        render_result: IcebergV3RenderResult,
    ) -> MetaResult<Self> {
        let job_id = control.info.job_id;
        let partial_graph_id = control.info.partial_graph_id;
        let snapshot_epoch = control.info.snapshot_epoch;
        let committed_epoch = control
            .max_committed_epoch
            .expect("recovered independent job should have committed");
        let IcebergV3RenderResult {
            active:
                BatchRefreshRenderResult {
                    fragment_infos,
                    node_actors,
                    actors_to_create,
                },
            active_stream_actors,
            active_downstreams,
            resolver,
            partition,
        } = render_result;

        let (status, first_barrier_info) = if committed_epoch < snapshot_epoch {
            Self::recover_snapshot_phase(
                job_id,
                &control.info.snapshot_backfill_upstream_tables,
                upstream_table_log_epochs,
                snapshot_epoch,
                committed_epoch,
                upstream_barrier_info,
                &fragment_infos,
                &backfill_order,
                version_stat,
            )?
        } else {
            Self::recover_log_store_phase(
                &control.info.snapshot_backfill_upstream_tables,
                upstream_table_log_epochs,
                committed_epoch,
                upstream_barrier_info,
            )?
        };

        partial_graph_recoverer.recover_graph(
            partial_graph_id,
            term_id,
            initial_mutation,
            &first_barrier_info,
            &node_actors,
            control.info.state_table_ids.iter().copied(),
            actors_to_create,
            IcebergV3BarrierStats::new(job_id, snapshot_epoch),
        )?;
        let max_pending_barrier_num = super::snapshot_backfill_max_pending_barrier_num(
            &partial_graph_recoverer.control_stream_manager().env.opts,
        );
        Ok(Self {
            control,
            status: IcebergV3JobStatus::Running(status),
            fragment_infos,
            node_actors,
            max_pending_barrier_num,
            upstream_lag: GLOBAL_META_METRICS
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&format!("{}", job_id)]),
            active_stream_actors,
            active_downstreams,
            resolver,
            partition,
        })
    }

    pub(crate) fn pinned_upstream_tables(&self) -> &HashSet<TableId> {
        &self.control.info.snapshot_backfill_upstream_tables
    }

    pub(crate) fn gen_backfill_progress(&self) -> Option<BackfillProgress> {
        match &self.status {
            IcebergV3JobStatus::Running(IcebergV3Input {
                phase: IcebergV3InputPhase::Snapshot(snapshot),
                ..
            })
            | IcebergV3JobStatus::Compacting {
                input:
                    IcebergV3Input {
                        phase: IcebergV3InputPhase::Snapshot(snapshot),
                        ..
                    },
                ..
            } => {
                let progress = if snapshot.create_mview_tracker.is_finished() {
                    "Snapshot finished".to_owned()
                } else {
                    format!(
                        "Snapshot [{}]",
                        snapshot.create_mview_tracker.gen_backfill_progress()
                    )
                };
                Some(BackfillProgress {
                    progress,
                    backfill_type: PbBackfillType::SnapshotBackfill,
                })
            }
            IcebergV3JobStatus::Running(IcebergV3Input {
                phase: IcebergV3InputPhase::LogStore { .. },
                ..
            })
            | IcebergV3JobStatus::Compacting {
                input:
                    IcebergV3Input {
                        phase: IcebergV3InputPhase::LogStore { .. },
                        ..
                    },
                ..
            } => None,
        }
    }

    pub(crate) fn gen_fragment_backfill_progress(&self) -> Vec<FragmentBackfillProgress> {
        match &self.status {
            IcebergV3JobStatus::Running(IcebergV3Input {
                phase: IcebergV3InputPhase::Snapshot(snapshot),
                ..
            })
            | IcebergV3JobStatus::Compacting {
                input:
                    IcebergV3Input {
                        phase: IcebergV3InputPhase::Snapshot(snapshot),
                        ..
                    },
                ..
            } => snapshot
                .create_mview_tracker
                .collect_fragment_progress(&self.fragment_infos, true),
            IcebergV3JobStatus::Running(IcebergV3Input {
                phase: IcebergV3InputPhase::LogStore { .. },
                ..
            })
            | IcebergV3JobStatus::Compacting {
                input:
                    IcebergV3Input {
                        phase: IcebergV3InputPhase::LogStore { .. },
                        ..
                    },
                ..
            } => vec![],
        }
    }

    pub(crate) fn fragment_infos(&self) -> &HashMap<FragmentId, InflightFragmentInfo> {
        &self.fragment_infos
    }

    pub(crate) fn resolver_fragment_info(&self) -> &InflightFragmentInfo {
        &self.resolver.fragment_info
    }

    pub(crate) fn on_new_upstream_barrier(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        barrier_info: &BarrierInfo,
        mutation: Option<(
            risingwave_pb::stream_plan::barrier_mutation::Mutation,
            Option<&mut NotifierStarter>,
        )>,
    ) -> MetaResult<()> {
        match &mut self.status {
            IcebergV3JobStatus::Running(input) => {
                let (mut mutation, mut notifier) = mutation
                    .map(|(mutation, notifier)| (Some(mutation), notifier))
                    .unwrap_or_default();
                let progress_epoch = self
                    .control
                    .max_committed_epoch
                    .map_or(self.control.info.snapshot_epoch, |committed_epoch| {
                        max(committed_epoch, self.control.info.snapshot_epoch)
                    });
                self.upstream_lag.set(
                    barrier_info
                        .prev_epoch
                        .value()
                        .0
                        .saturating_sub(progress_epoch) as _,
                );
                let available = self.max_pending_barrier_num.saturating_sub(
                    partial_graph_manager.pending_barrier_num(self.control.info.partial_graph_id),
                );
                input
                    .pending_upstream_barriers
                    .push_back(barrier_info.clone());
                let barriers_to_inject = match &mut input.phase {
                    IcebergV3InputPhase::Snapshot(snapshot) => {
                        mutation = mutation.or_else(|| snapshot.take_start_backfill_mutation());
                        if available == 0 && mutation.is_none() {
                            vec![]
                        } else {
                            vec![snapshot.next_fake_barrier(&barrier_info.kind)]
                        }
                    }
                    IcebergV3InputPhase::LogStore { .. } => {
                        let barrier_num_to_inject = available.max(usize::from(mutation.is_some()));
                        let barrier_num_to_inject =
                            barrier_num_to_inject.min(input.pending_upstream_barriers.len());
                        input
                            .pending_upstream_barriers
                            .drain(..barrier_num_to_inject)
                            .collect()
                    }
                };

                for barrier_to_inject in barriers_to_inject {
                    let barrier_mutation = mutation.take();
                    let barrier_notifier = notifier.take();
                    partial_graph_manager.inject_barrier(
                        self.control.info.partial_graph_id,
                        barrier_mutation,
                        &self.node_actors,
                        self.control.info.state_table_ids.iter().copied(),
                        self.node_actors.keys().copied(),
                        None,
                        PartialGraphBarrierInfo::new(
                            PostCollectCommand::barrier(),
                            barrier_to_inject,
                            barrier_notifier,
                            self.control.info.state_table_ids.clone(),
                        ),
                    )?;
                }
            }
            IcebergV3JobStatus::Compacting { input, .. } => {
                assert!(
                    mutation.is_none(),
                    "active Iceberg compaction must not receive a second mutation"
                );
                input
                    .pending_upstream_barriers
                    .push_back(barrier_info.clone());
            }
        }
        Ok(())
    }

    pub(crate) fn pre_apply_throttle(
        &mut self,
        throttle_config: &mut ThrottleConfigMap,
    ) -> Option<risingwave_pb::stream_plan::barrier_mutation::Mutation> {
        let mutation = extract_throttle_config(throttle_config, |fragment_id, stream_node| {
            if let Some(fragment_info) = self.fragment_infos.get_mut(&fragment_id) {
                fragment_info.nodes = stream_node.clone();
                true
            } else {
                false
            }
        });
        match &self.status {
            IcebergV3JobStatus::Running(_) => mutation,
            // The normal input is stopped while compacting. Retain the updated plan so actors
            // restarted by the End barrier use it, but do not inject a throttle mutation now.
            IcebergV3JobStatus::Compacting { .. } => None,
        }
    }

    pub(crate) fn collect(&mut self, collected_barrier: CollectedBarrier<'_>) -> bool {
        let (input, can_finish_snapshot) = match &mut self.status {
            IcebergV3JobStatus::Running(input) => (input, true),
            IcebergV3JobStatus::Compacting { input, .. } => (input, false),
        };
        match &mut input.phase {
            IcebergV3InputPhase::Snapshot(snapshot) => {
                let progress = collected_barrier
                    .resps
                    .values()
                    .flat_map(|response| &response.create_mview_progress);
                let snapshot_finished = snapshot.apply_progress(progress);
                // B1 may contain tail progress produced before the normal-input actors stop. No
                // backfill actor runs after B1, so a compacting job records that progress but
                // defers the Snapshot -> LogStore transition until normal execution resumes.
                if can_finish_snapshot && snapshot_finished {
                    let IcebergV3InputPhase::Snapshot(mut snapshot) = replace(
                        &mut input.phase,
                        IcebergV3InputPhase::LogStore { tracking_job: None },
                    ) else {
                        unreachable!("snapshot finished outside the snapshot phase")
                    };
                    input
                        .pending_upstream_barriers
                        .push_front(snapshot.finish_snapshot_barrier());
                    input.phase = IcebergV3InputPhase::LogStore {
                        tracking_job: Some(snapshot.create_mview_tracker.into_tracking_job()),
                    };
                }
            }
            IcebergV3InputPhase::LogStore { .. } => {}
        }
        // Ordinary snapshot jobs force a checkpoint to merge into the database graph. Iceberg V3
        // remains independent after catch-up, so its normal checkpoint cadence is sufficient.
        false
    }

    pub(crate) fn start_apply_compaction(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        upstream_barrier: &BarrierInfo,
        task_id: IcebergCompactionTaskId,
        overwrite: IcebergPkIndexCompactionOverwrite,
        notifier: &mut NotifierStarter,
    ) -> MetaResult<()> {
        let output_data_file_paths = output_file_paths(&overwrite.output_result.data_files)?;
        let (begin_barrier, end_barrier) = self.next_compaction_barriers(upstream_barrier)?;
        let transitions = build_compaction_transitions(
            &self.fragment_infos,
            &self.active_stream_actors,
            &self.active_downstreams,
            &self.resolver,
            &self.partition,
            self.control.info.partial_graph_id,
            partial_graph_manager.control_stream_manager(),
        )?;
        let sink_id = SinkId::new(self.control.info.job_id.as_raw_id());
        let resolver_task_input = ResolverTaskInput {
            output_data_file_paths,
            input_data_file_paths: overwrite.input_file_paths.clone(),
            read_snapshot_id: overwrite.read_snapshot_id,
        };
        let compaction_overwrite = CompactionOverwrite {
            sink_id,
            epoch: end_barrier.prev_epoch(),
            schema_id: overwrite.output_result.schema_id,
            partition_spec_id: overwrite.output_result.partition_spec_id,
            output_files: overwrite.output_result.data_files,
            input_file_paths: overwrite.input_file_paths,
            read_snapshot_id: overwrite.read_snapshot_id,
        };
        let mut mutation = transitions.begin_compaction.mutation;
        mutation.iceberg_pk_index_compaction = Some(IcebergPkIndexCompactionContext {
            sink_id,
            task_id,
            phase: Phase::Begin as i32,
            resolver_task_input: Some(resolver_task_input),
        });
        partial_graph_manager.inject_barrier(
            self.control.info.partial_graph_id,
            Some(Mutation::Update(mutation)),
            &transitions.node_actors,
            self.control.info.state_table_ids.iter().copied(),
            transitions.node_actors.keys().copied(),
            Some(transitions.begin_compaction.actors_to_create),
            PartialGraphBarrierInfo::new(
                PostCollectCommand::barrier(),
                begin_barrier.clone(),
                None,
                self.control.info.state_table_ids.clone(),
            ),
        )?;

        // This empty Running value is only an ownership placeholder while moving `input`
        // into Compacting. It is replaced before the method returns and is never observed.
        let input = match std::mem::replace(
            &mut self.status,
            IcebergV3JobStatus::Running(IcebergV3Input {
                phase: IcebergV3InputPhase::LogStore { tracking_job: None },
                pending_upstream_barriers: VecDeque::new(),
            }),
        ) {
            IcebergV3JobStatus::Running(input) => input,
            IcebergV3JobStatus::Compacting { .. } => {
                unreachable!("compacting status was rejected before barrier injection")
            }
        };
        self.status = IcebergV3JobStatus::Compacting {
            input,
            apply: CompactionApply {
                notifier: notifier.add_notify(),
                phase: CompactionApplyPhase::BeginAck {
                    task_id,
                    begin_epoch: begin_barrier.prev_epoch(),
                    end_barrier,
                    node_actors: transitions.node_actors,
                    end_transition: transitions.end_compaction,
                    overwrite: compaction_overwrite,
                },
            },
        };
        Ok(())
    }

    fn next_compaction_barriers(
        &mut self,
        upstream_barrier: &BarrierInfo,
    ) -> MetaResult<(BarrierInfo, BarrierInfo)> {
        match &mut self.status {
            IcebergV3JobStatus::Running(input) => {
                let IcebergV3Input {
                    phase,
                    pending_upstream_barriers,
                } = input;
                match phase {
                    IcebergV3InputPhase::Snapshot(snapshot) => {
                        pending_upstream_barriers.push_back(upstream_barrier.clone());
                        let begin = snapshot.next_fake_barrier(&BarrierKind::Checkpoint(vec![]));
                        let end = snapshot.next_fake_barrier(&BarrierKind::Checkpoint(vec![]));
                        Ok((begin, end))
                    }
                    IcebergV3InputPhase::LogStore { .. } => {
                        let BarrierKind::Checkpoint(checkpoint_epochs) = &upstream_barrier.kind
                        else {
                            return Err(anyhow::anyhow!(
                                "Iceberg pk-index compaction requires a checkpoint command barrier"
                            )
                            .into());
                        };
                        let prev_epoch = upstream_barrier.prev_epoch.value();
                        let curr_epoch = upstream_barrier.curr_epoch.value();
                        let synthetic = synthetic_compaction_epoch(prev_epoch, curr_epoch)?;
                        Ok((
                            BarrierInfo {
                                prev_epoch: TracedEpoch::new(prev_epoch),
                                curr_epoch: TracedEpoch::new(synthetic),
                                kind: BarrierKind::Checkpoint(checkpoint_epochs.clone()),
                            },
                            BarrierInfo {
                                prev_epoch: TracedEpoch::new(synthetic),
                                curr_epoch: TracedEpoch::new(curr_epoch),
                                kind: BarrierKind::Checkpoint(vec![synthetic.0]),
                            },
                        ))
                    }
                }
            }
            IcebergV3JobStatus::Compacting { .. } => Err(anyhow::anyhow!(
                "Iceberg V3 job {} is already applying compaction",
                self.control.info.job_id
            )
            .into()),
        }
    }

    #[expect(clippy::type_complexity)]
    pub(crate) fn start_completing(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        min_upstream_inflight_barrier: Option<u64>,
    ) -> Option<(
        u64,
        HashMap<WorkerId, BarrierCompleteResponse>,
        PartialGraphBarrierInfo,
        Option<TrackingJob>,
        Option<CompactionOverwrite>,
    )> {
        let epoch_end_bound = min_upstream_inflight_barrier
            .map(Excluded)
            .unwrap_or(Unbounded);
        let (epoch, responses, info) = partial_graph_manager.start_completing(
            self.control.info.partial_graph_id,
            epoch_end_bound,
            |_non_checkpoint_epoch, _, _| {},
        )?;

        let tracking_job = match &mut self.status {
            IcebergV3JobStatus::Running(IcebergV3Input {
                phase: IcebergV3InputPhase::LogStore { tracking_job },
                ..
            })
            | IcebergV3JobStatus::Compacting {
                input:
                    IcebergV3Input {
                        phase: IcebergV3InputPhase::LogStore { tracking_job },
                        ..
                    },
                ..
            } if epoch == self.control.info.snapshot_epoch => tracking_job.take(),
            _ => None,
        };

        let overwrite = match &mut self.status {
            IcebergV3JobStatus::Running(_) => None,
            IcebergV3JobStatus::Compacting {
                apply: CompactionApply { phase, .. },
                ..
            } => match phase {
                CompactionApplyPhase::BeginAck { begin_epoch, .. } => {
                    if epoch <= *begin_epoch {
                        None
                    } else {
                        unreachable!(
                            "collected epoch {epoch} passed unacknowledged compaction Begin epoch {begin_epoch}"
                        )
                    }
                }
                CompactionApplyPhase::EndCompletion { end_epoch, .. } => {
                    let end_epoch = *end_epoch;
                    match epoch.cmp(&end_epoch) {
                        std::cmp::Ordering::Less => None,
                        std::cmp::Ordering::Equal => {
                            let CompactionApplyPhase::EndCompletion { overwrite, .. } =
                                std::mem::replace(
                                    phase,
                                    CompactionApplyPhase::EndAck { end_epoch },
                                )
                            else {
                                unreachable!()
                            };
                            Some(overwrite)
                        }
                        std::cmp::Ordering::Greater => unreachable!(
                            "collected epoch {epoch} passed compaction End epoch {end_epoch}"
                        ),
                    }
                }
                CompactionApplyPhase::EndAck { end_epoch } => unreachable!(
                    "started another completion while compaction End epoch {end_epoch} is committing"
                ),
            },
        };
        Some((epoch, responses, info, tracking_job, overwrite))
    }

    pub(crate) fn ack_completed(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        completed_epoch: u64,
    ) -> MetaResult<()> {
        match &mut self.status {
            IcebergV3JobStatus::Running(input) => {
                partial_graph_manager
                    .ack_completed(self.control.info.partial_graph_id, completed_epoch);
                self.control.ack_completed(completed_epoch);
                if completed_epoch == self.control.info.snapshot_epoch {
                    let IcebergV3InputPhase::LogStore { tracking_job } = &input.phase else {
                        unreachable!("snapshot epoch completed outside the log-store phase")
                    };
                    assert!(
                        tracking_job.is_none(),
                        "tracking job should have been taken at start_completing"
                    );
                }
            }
            IcebergV3JobStatus::Compacting {
                apply:
                    CompactionApply {
                        phase: CompactionApplyPhase::BeginAck { begin_epoch, .. },
                        ..
                    },
                ..
            } if completed_epoch < *begin_epoch => {
                partial_graph_manager
                    .ack_completed(self.control.info.partial_graph_id, completed_epoch);
                self.control.ack_completed(completed_epoch);
            }
            IcebergV3JobStatus::Compacting {
                apply:
                    CompactionApply {
                        phase:
                            CompactionApplyPhase::BeginAck {
                                task_id,
                                begin_epoch,
                                end_barrier,
                                node_actors,
                                end_transition,
                                ..
                            },
                        ..
                    },
                ..
            } if completed_epoch == *begin_epoch => {
                partial_graph_manager
                    .ack_completed(self.control.info.partial_graph_id, completed_epoch);
                self.control.ack_completed(completed_epoch);

                let end_epoch = end_barrier.prev_epoch();
                let sink_id = SinkId::new(self.control.info.job_id.as_raw_id());
                let mut mutation = end_transition.mutation.clone();
                mutation.iceberg_pk_index_compaction = Some(IcebergPkIndexCompactionContext {
                    sink_id,
                    task_id: *task_id,
                    phase: Phase::End as i32,
                    resolver_task_input: None,
                });
                partial_graph_manager.inject_barrier(
                    self.control.info.partial_graph_id,
                    Some(Mutation::Update(mutation)),
                    node_actors,
                    self.control.info.state_table_ids.iter().copied(),
                    node_actors.keys().copied(),
                    Some(end_transition.actors_to_create.clone()),
                    PartialGraphBarrierInfo::new(
                        PostCollectCommand::barrier(),
                        end_barrier.clone(),
                        None,
                        self.control.info.state_table_ids.clone(),
                    ),
                )?;

                // B2 injection succeeded. Temporarily install an empty Running state to move the
                // overwrite into the next explicit compaction phase.
                let IcebergV3JobStatus::Compacting { input, apply } = std::mem::replace(
                    &mut self.status,
                    IcebergV3JobStatus::Running(IcebergV3Input {
                        phase: IcebergV3InputPhase::LogStore { tracking_job: None },
                        pending_upstream_barriers: VecDeque::new(),
                    }),
                ) else {
                    unreachable!()
                };
                let CompactionApplyPhase::BeginAck { overwrite, .. } = apply.phase else {
                    unreachable!()
                };
                self.status = IcebergV3JobStatus::Compacting {
                    input,
                    apply: CompactionApply {
                        notifier: apply.notifier,
                        phase: CompactionApplyPhase::EndCompletion {
                            end_epoch,
                            overwrite,
                        },
                    },
                };
            }
            IcebergV3JobStatus::Compacting {
                apply:
                    CompactionApply {
                        phase: CompactionApplyPhase::BeginAck { begin_epoch, .. },
                        ..
                    },
                ..
            } => unreachable!(
                "acknowledged epoch {completed_epoch} after compaction Begin epoch {begin_epoch}"
            ),
            IcebergV3JobStatus::Compacting {
                apply:
                    CompactionApply {
                        phase: CompactionApplyPhase::EndCompletion { end_epoch, .. },
                        ..
                    },
                ..
            } => unreachable!(
                "acknowledged epoch {completed_epoch} before compaction End epoch {end_epoch} entered the completion task"
            ),
            IcebergV3JobStatus::Compacting {
                apply:
                    CompactionApply {
                        phase: CompactionApplyPhase::EndAck { end_epoch },
                        ..
                    },
                ..
            } if completed_epoch < *end_epoch => {
                partial_graph_manager
                    .ack_completed(self.control.info.partial_graph_id, completed_epoch);
                self.control.ack_completed(completed_epoch);
            }
            IcebergV3JobStatus::Compacting {
                apply:
                    CompactionApply {
                        phase: CompactionApplyPhase::EndAck { end_epoch },
                        ..
                    },
                ..
            } if completed_epoch == *end_epoch => {
                partial_graph_manager
                    .ack_completed(self.control.info.partial_graph_id, completed_epoch);
                self.control.ack_completed(completed_epoch);
                let IcebergV3JobStatus::Compacting { input, apply } = std::mem::replace(
                    &mut self.status,
                    IcebergV3JobStatus::Running(IcebergV3Input {
                        phase: IcebergV3InputPhase::LogStore { tracking_job: None },
                        pending_upstream_barriers: VecDeque::new(),
                    }),
                ) else {
                    unreachable!()
                };
                self.status = IcebergV3JobStatus::Running(input);
                apply.notifier.notify_collected();
            }
            IcebergV3JobStatus::Compacting {
                apply:
                    CompactionApply {
                        phase: CompactionApplyPhase::EndAck { end_epoch },
                        ..
                    },
                ..
            } => unreachable!(
                "acknowledged epoch {completed_epoch} after compaction End epoch {end_epoch}"
            ),
        }
        Ok(())
    }
}

fn output_file_paths(files: &[iceberg::spec::SerializedDataFile]) -> MetaResult<Vec<String>> {
    files
        .iter()
        .map(|file| {
            let value = serde_json::to_value(file).map_err(anyhow::Error::from)?;
            value
                .get("file_path")
                .and_then(|path| path.as_str())
                .map(str::to_owned)
                .ok_or_else(|| {
                    anyhow::anyhow!("compaction output file is missing file_path").into()
                })
        })
        .collect()
}

pub(crate) fn is_iceberg_v3_fragment_nodes<'a>(
    nodes: impl IntoIterator<Item = &'a risingwave_pb::stream_plan::PbStreamNode>,
) -> bool {
    nodes.into_iter().any(|root| {
        let mut found = false;
        visit_stream_node_cont(root, |node| {
            if matches!(
                node.node_body,
                Some(
                    risingwave_pb::stream_plan::stream_node::NodeBody::IcebergWithPkIndexWriter(_)
                )
            ) {
                found = true;
            }
            !found
        });
        found
    })
}

#[cfg(test)]
mod tests {
    use risingwave_common::util::epoch::EPOCH_SPILL_TIME_MASK;

    use super::*;

    #[test]
    fn test_synthetic_compaction_epoch() {
        let prev_epoch = Epoch::from_physical_time(10);
        let curr_epoch = Epoch::from_physical_time(12);

        let synthetic = synthetic_compaction_epoch(prev_epoch, curr_epoch).unwrap();

        assert_eq!(synthetic, Epoch::from_physical_time(11));
        assert_eq!(synthetic.0 & EPOCH_SPILL_TIME_MASK, 0);
        assert!(prev_epoch < synthetic);
        assert!(synthetic < curr_epoch);
    }

    #[test]
    fn test_synthetic_compaction_epoch_rejects_tight_gap() {
        let prev_epoch = Epoch::from_physical_time(10);
        let curr_epoch = Epoch::from_physical_time(11);

        assert!(synthetic_compaction_epoch(prev_epoch, curr_epoch).is_err());
    }
}
