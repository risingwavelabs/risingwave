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
use risingwave_common::id::JobId;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntGauge};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::stream_graph_visitor::visit_stream_node_cont;
use risingwave_meta_model::{WorkerId, streaming_job};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::ddl_service::PbBackfillType;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::id::{ActorId, FragmentId};
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_service::BarrierCompleteResponse;

use self::render::{RenderedResolver, partition_resolver};
use super::{
    BatchRefreshRenderResult, CreatingStreamingJobControl, IndependentCheckpointJob,
    IndependentCheckpointJobControl, IndependentCheckpointJobStatus, IndependentJobControl,
    IndependentJobInfo, InitialPartialGraphRequest, RenderedIndependentJobActors,
    SnapshotPhaseControl, add_initial_partial_graph, build_initial_add_mutation,
};
use crate::MetaResult;
use crate::barrier::command::{
    PostCollectCommand, ThrottleConfigMap, UpstreamTableLogEpochs, extract_throttle_config,
};
use crate::barrier::context::CreateIndependentStreamingJobCommandInfo;
use crate::barrier::info::BarrierInfo;
use crate::barrier::notifier::NotifierStarter;
use crate::barrier::partial_graph::{
    CollectedBarrier, PartialGraphBarrierInfo, PartialGraphManager, PartialGraphRecoverer,
    PartialGraphStat,
};
use crate::barrier::rpc::to_partial_graph_id;
use crate::barrier::{BackfillProgress, BarrierKind, FragmentBackfillProgress};
use crate::controller::fragment::InflightFragmentInfo;
use crate::controller::scale::LoadedFragment;
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
}

#[derive(Debug)]
pub(crate) struct IcebergV3RenderResult {
    active: BatchRefreshRenderResult,
    active_downstreams: FragmentDownstreamRelation,
    resolver: RenderedResolver,
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
    /// Active graph relations retained for switching the normal input partition during compaction.
    #[allow(dead_code)]
    active_downstreams: FragmentDownstreamRelation,
    resolver: RenderedResolver,
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
        let mut active_downstreams = downstreams.clone();
        let (active, resolver) = partition_resolver(rendered, &mut active_downstreams)?;
        let active = active.build_job_info(&active_downstreams, partial_graph_id, worker_nodes);
        Ok(IcebergV3RenderResult {
            active,
            active_downstreams,
            resolver,
        })
    }

    pub(crate) fn create<'a>(
        entry: hash_map::VacantEntry<'a, JobId, IndependentCheckpointJobControl>,
        create_info: CreateIndependentStreamingJobCommandInfo,
        notifier: Option<&mut NotifierStarter>,
        snapshot_backfill_upstream_tables: HashSet<TableId>,
        snapshot_epoch: u64,
        version_stat: &HummockVersionStats,
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
            active_downstreams,
            resolver,
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
            active_downstreams,
            resolver,
        };

        let mut graph_adder = partial_graph_manager.add_partial_graph(
            partial_graph_id,
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
            active_downstreams,
            resolver,
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
            active_downstreams,
            resolver,
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
        }
    }

    pub(crate) fn gen_fragment_backfill_progress(&self) -> Vec<FragmentBackfillProgress> {
        match &self.status {
            IcebergV3JobStatus::Running(IcebergV3InputPhase::Snapshot { snapshot, .. }) => snapshot
                .create_mview_tracker
                .collect_fragment_progress(&self.fragment_infos, true),
            IcebergV3JobStatus::Running(IcebergV3InputPhase::LogStore { .. }) => vec![],
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
        match &mut self.status {
            IcebergV3JobStatus::Running(input_phase) => {
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
        }
        Ok(())
    }

    pub(crate) fn pre_apply_throttle(
        &mut self,
        throttle_config: &mut ThrottleConfigMap,
    ) -> Option<risingwave_pb::stream_plan::barrier_mutation::Mutation> {
        extract_throttle_config(throttle_config, |fragment_id, stream_node| {
            if let Some(fragment_info) = self.fragment_infos.get_mut(&fragment_id) {
                fragment_info.nodes = stream_node.clone();
                true
            } else {
                false
            }
        })
    }

    pub(crate) fn collect(&mut self, collected_barrier: CollectedBarrier<'_>) -> bool {
        match &mut self.status {
            IcebergV3JobStatus::Running(input_phase) => {
                let progress = collected_barrier
                    .resps
                    .values()
                    .flat_map(|response| &response.create_mview_progress);
                if let IcebergV3InputPhase::Snapshot {
                    snapshot,
                    pending_upstream_barriers,
                } = input_phase
                    && snapshot.apply_progress(progress)
                {
                    pending_upstream_barriers.push_front(snapshot.finish_snapshot_barrier());
                    *input_phase = IcebergV3InputPhase::LogStore {
                        pending_upstream_barriers: take(pending_upstream_barriers),
                    };
                }
            }
        }
        // Ordinary snapshot jobs force a checkpoint to merge into the database graph. Iceberg V3
        // remains independent after catch-up, so its normal checkpoint cadence is sufficient.
        false
    }

    pub(crate) fn start_completing(
        &mut self,
        partial_graph_manager: &mut PartialGraphManager,
        min_upstream_inflight_barrier: Option<u64>,
    ) -> Option<(
        u64,
        HashMap<WorkerId, BarrierCompleteResponse>,
        PartialGraphBarrierInfo,
        bool,
    )> {
        let epoch_end_bound = min_upstream_inflight_barrier
            .map(Excluded)
            .unwrap_or(Unbounded);
        partial_graph_manager
            .start_completing(
                self.control.info.partial_graph_id,
                epoch_end_bound,
                |_non_checkpoint_epoch, _, _| {},
            )
            .map(|(epoch, responses, info)| (epoch, responses, info, false))
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
        }
    }
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
