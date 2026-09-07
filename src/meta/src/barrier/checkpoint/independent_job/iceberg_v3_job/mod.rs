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
use std::mem::take;
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
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_plan::iceberg_pk_index_compaction_context::{Phase, ResolverTaskInput};
use risingwave_pb::stream_plan::{IcebergPkIndexCompactionContext, UpdateMutation};
use risingwave_pb::stream_service::BarrierCompleteResponse;

use self::render::{
    IcebergV3StaticActors, RenderedResolver, build_static_actors, partition_resolver,
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
use crate::barrier::notifier::{CollectionNotifier, NotifierStarter};
use crate::barrier::partial_graph::{
    CollectedBarrier, PartialGraphBarrierInfo, PartialGraphManager, PartialGraphRecoverer,
    PartialGraphStat,
};
use crate::barrier::rpc::to_partial_graph_id;
use crate::barrier::{BackfillProgress, BarrierKind, FragmentBackfillProgress, TracedEpoch};
use crate::controller::fragment::InflightFragmentInfo;
use crate::controller::scale::LoadedFragment;
use crate::manager::iceberg_pk_index_sink::CompactionOverwrite;
use crate::model::FragmentDownstreamRelation;
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::ExtendedFragmentBackfillOrder;

struct IcebergV3BarrierStats {
    consuming_snapshot_barrier_latency: LabelGuardedHistogram,
    consuming_log_store_barrier_latency: LabelGuardedHistogram,
    inflight_barrier_num: LabelGuardedIntGauge,
    snapshot_epoch: u64,
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
enum IcebergV3InputPhase {
    Snapshot {
        snapshot: SnapshotPhaseControl,
        /// Original upstream barriers retained until the input starts log-store replay.
        pending_upstream_barriers: VecDeque<BarrierInfo>,
    },
    LogStore {
        /// Original upstream barriers waiting to be consumed from the log store.
        pending_upstream_barriers: VecDeque<BarrierInfo>,
    },
}

#[derive(Debug)]
enum IcebergV3JobStatus {
    Running(IcebergV3InputPhase),
    Compacting {
        input_phase: IcebergV3InputPhase,
        apply: CompactionApply,
    },
}

#[derive(Debug)]
struct CompactionApply {
    task_id: IcebergCompactionTaskId,
    overwrite: Option<CompactionOverwrite>,
    begin_barrier: BarrierInfo,
    end_barrier: BarrierInfo,
    notifier: CollectionNotifier,
}

#[derive(Debug)]
pub(crate) struct IcebergV3RenderResult {
    active: BatchRefreshRenderResult,
    active_downstreams: FragmentDownstreamRelation,
    resolver: RenderedResolver,
    static_actors: IcebergV3StaticActors,
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
    resolver: RenderedResolver,
    actors: IcebergV3StaticActors,
}

impl IcebergV3JobCheckpointControl {
    pub(crate) fn render_actors_and_build_job_info(
        fragments: &HashMap<FragmentId, LoadedFragment>,
        downstreams: &FragmentDownstreamRelation,
        definition: &str,
        actor_id_generator: &AtomicU32,
        worker_nodes: &HashMap<WorkerId, WorkerNode>,
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
        let static_actors =
            build_static_actors(&rendered, downstreams, partial_graph_id, worker_nodes)?;
        let mut active_downstreams = downstreams.clone();
        let (active, resolver) = partition_resolver(rendered, &mut active_downstreams)?;
        let active = active.build_job_info(&active_downstreams, partial_graph_id, worker_nodes);
        Ok(IcebergV3RenderResult {
            active,
            active_downstreams,
            resolver,
            static_actors,
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
            active_downstreams: _,
            resolver,
            static_actors,
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
            status: IcebergV3JobStatus::Running(IcebergV3InputPhase::Snapshot {
                snapshot,
                pending_upstream_barriers: VecDeque::new(),
            }),
            fragment_infos,
            upstream_lag: GLOBAL_META_METRICS
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&format!("{}", job_id)]),
            node_actors,
            max_pending_barrier_num,
            resolver,
            actors: static_actors,
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
    ) -> MetaResult<(IcebergV3InputPhase, BarrierInfo)> {
        let (snapshot, first_barrier_info) = SnapshotPhaseControl::for_recovery(
            job_id,
            snapshot_epoch,
            committed_epoch,
            fragment_infos,
            backfill_order,
            version_stat,
        );
        Ok((
            IcebergV3InputPhase::Snapshot {
                snapshot,
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
    ) -> MetaResult<(IcebergV3InputPhase, BarrierInfo)> {
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
            IcebergV3InputPhase::LogStore {
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
            active_downstreams: _,
            resolver,
            static_actors,
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
            resolver,
            actors: static_actors,
        })
    }

    pub(crate) fn pinned_upstream_tables(&self) -> &HashSet<TableId> {
        &self.control.info.snapshot_backfill_upstream_tables
    }

    pub(crate) fn gen_backfill_progress(&self) -> Option<BackfillProgress> {
        match &self.status {
            IcebergV3JobStatus::Running(IcebergV3InputPhase::Snapshot { snapshot, .. }) => {
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
            IcebergV3JobStatus::Running(IcebergV3InputPhase::LogStore { .. }) => None,
            IcebergV3JobStatus::Compacting {
                input_phase: IcebergV3InputPhase::Snapshot { snapshot, .. },
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
            IcebergV3JobStatus::Compacting {
                input_phase: IcebergV3InputPhase::LogStore { .. },
                ..
            } => None,
        }
    }

    pub(crate) fn gen_fragment_backfill_progress(&self) -> Vec<FragmentBackfillProgress> {
        match &self.status {
            IcebergV3JobStatus::Running(IcebergV3InputPhase::Snapshot { snapshot, .. }) => snapshot
                .create_mview_tracker
                .collect_fragment_progress(&self.fragment_infos, true),
            IcebergV3JobStatus::Running(IcebergV3InputPhase::LogStore { .. }) => vec![],
            IcebergV3JobStatus::Compacting {
                input_phase: IcebergV3InputPhase::Snapshot { snapshot, .. },
                ..
            } => snapshot
                .create_mview_tracker
                .collect_fragment_progress(&self.fragment_infos, true),
            IcebergV3JobStatus::Compacting {
                input_phase: IcebergV3InputPhase::LogStore { .. },
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
            IcebergV3JobStatus::Running(input_phase) => {
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
                let barriers_to_inject = match input_phase {
                    IcebergV3InputPhase::Snapshot {
                        snapshot,
                        pending_upstream_barriers,
                    } => {
                        pending_upstream_barriers.push_back(barrier_info.clone());
                        mutation = mutation.or_else(|| snapshot.take_start_backfill_mutation());
                        if available == 0 && mutation.is_none() {
                            vec![]
                        } else {
                            vec![snapshot.next_fake_barrier(&barrier_info.kind)]
                        }
                    }
                    IcebergV3InputPhase::LogStore {
                        pending_upstream_barriers,
                    } => {
                        pending_upstream_barriers.push_back(barrier_info.clone());
                        let barrier_num_to_inject = available.max(usize::from(mutation.is_some()));
                        pending_upstream_barriers
                            .drain(..barrier_num_to_inject.min(pending_upstream_barriers.len()))
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
            IcebergV3JobStatus::Compacting { input_phase, .. } => {
                assert!(
                    mutation.is_none(),
                    "active Iceberg compaction must not receive a second mutation"
                );
                match input_phase {
                    IcebergV3InputPhase::Snapshot {
                        pending_upstream_barriers,
                        ..
                    }
                    | IcebergV3InputPhase::LogStore {
                        pending_upstream_barriers,
                    } => pending_upstream_barriers.push_back(barrier_info.clone()),
                }
            }
        }
        Ok(())
    }

    pub(crate) fn pre_apply_throttle(
        &mut self,
        throttle_config: &mut ThrottleConfigMap,
    ) -> Option<risingwave_pb::stream_plan::barrier_mutation::Mutation> {
        match &mut self.status {
            IcebergV3JobStatus::Running(_) => {
                let mutation =
                    extract_throttle_config(throttle_config, |fragment_id, stream_node| {
                        if let Some(fragment_info) = self.fragment_infos.get_mut(&fragment_id) {
                            fragment_info.nodes = stream_node.clone();
                            true
                        } else {
                            false
                        }
                    });
                if mutation.is_some() {
                    self.actors.refresh_build_plans(&self.fragment_infos);
                }
                mutation
            }
            IcebergV3JobStatus::Compacting { .. } => None,
        }
    }

    pub(crate) fn collect(&mut self, collected_barrier: CollectedBarrier<'_>) -> bool {
        match &mut self.status {
            IcebergV3JobStatus::Running(input_phase) => match input_phase {
                IcebergV3InputPhase::Snapshot {
                    snapshot,
                    pending_upstream_barriers,
                } => {
                    let progress = collected_barrier
                        .resps
                        .values()
                        .flat_map(|response| &response.create_mview_progress);
                    if snapshot.apply_progress(progress) {
                        pending_upstream_barriers.push_front(snapshot.finish_snapshot_barrier());
                        *input_phase = IcebergV3InputPhase::LogStore {
                            pending_upstream_barriers: take(pending_upstream_barriers),
                        };
                    }
                }
                IcebergV3InputPhase::LogStore { .. } => {}
            },
            IcebergV3JobStatus::Compacting { input_phase, .. } => match input_phase {
                IcebergV3InputPhase::Snapshot {
                    snapshot,
                    pending_upstream_barriers,
                } => {
                    let progress = collected_barrier
                        .resps
                        .values()
                        .flat_map(|response| &response.create_mview_progress);
                    if snapshot.apply_progress(progress) {
                        pending_upstream_barriers.push_front(snapshot.finish_snapshot_barrier());
                        *input_phase = IcebergV3InputPhase::LogStore {
                            pending_upstream_barriers: take(pending_upstream_barriers),
                        };
                    }
                }
                IcebergV3InputPhase::LogStore { .. } => {}
            },
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
        let mutation = combine_updates(
            &self.actors.normal_input.detach_mutation,
            &self.actors.resolver.attach_mutation,
            IcebergPkIndexCompactionContext {
                sink_id,
                task_id,
                phase: Phase::Begin as i32,
                resolver_task_input: Some(resolver_task_input),
            },
        );
        let node_actors = merge_node_actors([&self.node_actors, &self.actors.resolver.node_actors]);
        partial_graph_manager.inject_barrier(
            self.control.info.partial_graph_id,
            Some(mutation),
            &node_actors,
            self.control.info.state_table_ids.iter().copied(),
            node_actors.keys().copied(),
            Some(self.actors.resolver.actors_to_create.clone()),
            PartialGraphBarrierInfo::new(
                PostCollectCommand::barrier(),
                begin_barrier.clone(),
                None,
                self.control.info.state_table_ids.clone(),
            ),
        )?;

        let input_phase = match std::mem::replace(
            &mut self.status,
            IcebergV3JobStatus::Running(IcebergV3InputPhase::LogStore {
                pending_upstream_barriers: VecDeque::new(),
            }),
        ) {
            IcebergV3JobStatus::Running(input_phase) => input_phase,
            IcebergV3JobStatus::Compacting { .. } => {
                unreachable!("compacting status was rejected before barrier injection")
            }
        };
        self.status = IcebergV3JobStatus::Compacting {
            input_phase,
            apply: CompactionApply {
                task_id,
                overwrite: Some(compaction_overwrite),
                begin_barrier,
                end_barrier,
                notifier: notifier.add_notify(),
            },
        };
        Ok(())
    }

    fn next_compaction_barriers(
        &mut self,
        upstream_barrier: &BarrierInfo,
    ) -> MetaResult<(BarrierInfo, BarrierInfo)> {
        match &mut self.status {
            IcebergV3JobStatus::Running(input_phase) => match input_phase {
                IcebergV3InputPhase::Snapshot {
                    snapshot,
                    pending_upstream_barriers,
                } => {
                    pending_upstream_barriers.push_back(upstream_barrier.clone());
                    let begin = snapshot.next_fake_barrier(&BarrierKind::Checkpoint(vec![]));
                    let end = snapshot.next_fake_barrier(&BarrierKind::Checkpoint(vec![]));
                    Ok((begin, end))
                }
                IcebergV3InputPhase::LogStore { .. } => {
                    let BarrierKind::Checkpoint(checkpoint_epochs) = &upstream_barrier.kind else {
                        return Err(anyhow::anyhow!(
                            "Iceberg pk-index compaction requires a checkpoint command barrier"
                        )
                        .into());
                    };
                    let prev_epoch = upstream_barrier.prev_epoch.value();
                    let curr_epoch = upstream_barrier.curr_epoch.value();
                    let synthetic = Epoch(prev_epoch.0 + 1);
                    if synthetic >= curr_epoch {
                        return Err(anyhow::anyhow!(
                            "cannot allocate synthetic compaction epoch between {} and {}",
                            prev_epoch.0,
                            curr_epoch.0
                        )
                        .into());
                    }
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
            },
            IcebergV3JobStatus::Compacting { .. } => Err(anyhow::anyhow!(
                "Iceberg V3 job {} is already applying compaction",
                self.control.info.job_id
            )
            .into()),
        }
    }

    fn inject_end_compaction_barrier(
        &self,
        partial_graph_manager: &mut PartialGraphManager,
    ) -> MetaResult<()> {
        match &self.status {
            IcebergV3JobStatus::Running(_) => {
                unreachable!("end compaction barrier requested while running")
            }
            IcebergV3JobStatus::Compacting { apply, .. } => {
                let sink_id = SinkId::new(self.control.info.job_id.as_raw_id());
                let mutation = combine_updates(
                    &self.actors.resolver.detach_mutation,
                    &self.actors.normal_input.attach_mutation,
                    IcebergPkIndexCompactionContext {
                        sink_id,
                        task_id: apply.task_id,
                        phase: Phase::End as i32,
                        resolver_task_input: None,
                    },
                );
                let node_actors = merge_node_actors([
                    &self.actors.long_running_node_actors,
                    &self.actors.resolver.node_actors,
                    &self.actors.normal_input.node_actors,
                ]);
                partial_graph_manager.inject_barrier(
                    self.control.info.partial_graph_id,
                    Some(mutation),
                    &node_actors,
                    self.control.info.state_table_ids.iter().copied(),
                    node_actors.keys().copied(),
                    Some(self.actors.normal_input.actors_to_create.clone()),
                    PartialGraphBarrierInfo::new(
                        PostCollectCommand::barrier(),
                        apply.end_barrier.clone(),
                        None,
                        self.control.info.state_table_ids.clone(),
                    ),
                )
            }
        }
    }

    #[expect(clippy::type_complexity)]
    pub(crate) fn start_completing(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        min_upstream_inflight_barrier: Option<u64>,
    ) -> MetaResult<
        Option<(
            u64,
            HashMap<WorkerId, BarrierCompleteResponse>,
            PartialGraphBarrierInfo,
            bool,
            Option<CompactionOverwrite>,
        )>,
    > {
        let epoch_end_bound = min_upstream_inflight_barrier
            .map(Excluded)
            .unwrap_or(Unbounded);
        let Some((epoch, responses, info)) = partial_graph_manager.start_completing(
            self.control.info.partial_graph_id,
            epoch_end_bound,
            |_non_checkpoint_epoch, _, _| {},
        ) else {
            return Ok(None);
        };

        let inject_end = matches!(
            &self.status,
            IcebergV3JobStatus::Compacting { apply, .. }
                if epoch == apply.begin_barrier.prev_epoch()
        );
        if inject_end {
            self.inject_end_compaction_barrier(partial_graph_manager)?;
        }
        let overwrite = match &mut self.status {
            IcebergV3JobStatus::Running(_) => None,
            IcebergV3JobStatus::Compacting { apply, .. }
                if epoch < apply.begin_barrier.prev_epoch()
                    || epoch == apply.begin_barrier.prev_epoch() =>
            {
                None
            }
            IcebergV3JobStatus::Compacting { apply, .. }
                if epoch == apply.end_barrier.prev_epoch() =>
            {
                Some(
                    apply
                        .overwrite
                        .take()
                        .expect("compaction overwrite must be committed exactly once"),
                )
            }
            IcebergV3JobStatus::Compacting { apply, .. } => unreachable!(
                "unexpected collected epoch {epoch} while applying compaction between {} and {}",
                apply.begin_barrier.prev_epoch(),
                apply.end_barrier.prev_epoch(),
            ),
        };
        Ok(Some((epoch, responses, info, false, overwrite)))
    }

    pub(crate) fn ack_completed(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        completed_epoch: u64,
    ) {
        match &self.status {
            IcebergV3JobStatus::Running(_) => {
                partial_graph_manager
                    .ack_completed(self.control.info.partial_graph_id, completed_epoch);
                self.control.ack_completed(completed_epoch);
            }
            IcebergV3JobStatus::Compacting { apply, .. }
                if completed_epoch <= apply.begin_barrier.prev_epoch() =>
            {
                partial_graph_manager
                    .ack_completed(self.control.info.partial_graph_id, completed_epoch);
                self.control.ack_completed(completed_epoch);
            }
            IcebergV3JobStatus::Compacting { apply, .. }
                if completed_epoch == apply.end_barrier.prev_epoch() =>
            {
                partial_graph_manager
                    .ack_completed(self.control.info.partial_graph_id, completed_epoch);
                self.control.ack_completed(completed_epoch);
                let IcebergV3JobStatus::Compacting { input_phase, apply } = std::mem::replace(
                    &mut self.status,
                    IcebergV3JobStatus::Running(IcebergV3InputPhase::LogStore {
                        pending_upstream_barriers: VecDeque::new(),
                    }),
                ) else {
                    unreachable!()
                };
                self.status = IcebergV3JobStatus::Running(input_phase);
                apply.notifier.notify_collected();
            }
            IcebergV3JobStatus::Compacting { apply, .. } => unreachable!(
                "unexpected completed epoch {completed_epoch} while applying compaction between {} and {}",
                apply.begin_barrier.prev_epoch(),
                apply.end_barrier.prev_epoch(),
            ),
        }
    }
}

fn merge_node_actors<'a>(
    maps: impl IntoIterator<Item = &'a HashMap<WorkerId, HashSet<ActorId>>>,
) -> HashMap<WorkerId, HashSet<ActorId>> {
    let mut result: HashMap<WorkerId, HashSet<ActorId>> = HashMap::new();
    for map in maps {
        for (worker_id, actors) in map {
            result
                .entry(*worker_id)
                .or_default()
                .extend(actors.iter().copied());
        }
    }
    result
}

fn combine_updates(
    first: &Mutation,
    second: &Mutation,
    compaction: IcebergPkIndexCompactionContext,
) -> Mutation {
    let mut result = UpdateMutation::default();
    for mutation in [first, second] {
        let Mutation::Update(update) = mutation else {
            unreachable!("static topology activations are update mutations")
        };
        result
            .dispatcher_update
            .extend(update.dispatcher_update.clone());
        result.merge_update.extend(update.merge_update.clone());
        result
            .actor_vnode_bitmap_update
            .extend(update.actor_vnode_bitmap_update.clone());
        result.dropped_actors.extend(update.dropped_actors.clone());
        result.actor_splits.extend(update.actor_splits.clone());
        result
            .actor_new_dispatchers
            .extend(update.actor_new_dispatchers.clone());
    }
    result.iceberg_pk_index_compaction = Some(compaction);
    Mutation::Update(result)
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
