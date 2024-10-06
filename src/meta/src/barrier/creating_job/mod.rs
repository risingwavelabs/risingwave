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

mod barrier_control;
mod status;

use std::cmp::max;
use std::collections::HashMap;
use std::sync::Arc;

use prometheus::HistogramTimer;
use risingwave_common::metrics::{LabelGuardedHistogram, LabelGuardedIntGauge};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::info;

use crate::barrier::command::CommandContext;
use crate::barrier::creating_job::barrier_control::CreatingStreamingJobBarrierControl;
use crate::barrier::creating_job::status::{
    CreatingJobInjectBarrierInfo, CreatingStreamingJobStatus,
};
use crate::barrier::info::InflightGraphInfo;
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::rpc::ControlStreamManager;
use crate::barrier::{Command, CreateStreamingJobCommandInfo, SnapshotBackfillInfo};
use crate::manager::WorkerId;
use crate::rpc::metrics::MetaMetrics;
use crate::MetaResult;

#[derive(Debug)]
pub(super) struct CreatingStreamingJobControl {
    pub(super) info: CreateStreamingJobCommandInfo,
    pub(super) snapshot_backfill_info: SnapshotBackfillInfo,
    backfill_epoch: u64,

    barrier_control: CreatingStreamingJobBarrierControl,
    status: CreatingStreamingJobStatus,

    upstream_lag: LabelGuardedIntGauge<1>,
    upstream_wait_progress_latency: LabelGuardedHistogram<1>,
}

impl CreatingStreamingJobControl {
    pub(super) fn new(
        info: CreateStreamingJobCommandInfo,
        snapshot_backfill_info: SnapshotBackfillInfo,
        backfill_epoch: u64,
        version_stat: &HummockVersionStats,
        metrics: &MetaMetrics,
        initial_mutation: Mutation,
    ) -> Self {
        info!(
            table_id = info.table_fragments.table_id().table_id,
            definition = info.definition,
            "new creating job"
        );
        let snapshot_backfill_actors = info.table_fragments.snapshot_backfill_actor_ids();
        let mut create_mview_tracker = CreateMviewProgressTracker::default();
        create_mview_tracker.update_tracking_jobs(Some((&info, None)), [], version_stat);
        let fragment_info: HashMap<_, _> = info.new_fragment_info().collect();

        let table_id = info.table_fragments.table_id();
        let table_id_str = format!("{}", table_id.table_id);

        let actors_to_create = info.table_fragments.actors_to_create();

        Self {
            info,
            snapshot_backfill_info,
            barrier_control: CreatingStreamingJobBarrierControl::new(
                table_id,
                backfill_epoch,
                metrics,
            ),
            backfill_epoch,
            status: CreatingStreamingJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time: 0,
                pending_commands: vec![],
                version_stats: version_stat.clone(),
                create_mview_tracker,
                snapshot_backfill_actors,
                graph_info: InflightGraphInfo::new(fragment_info),
                backfill_epoch,
                pending_non_checkpoint_barriers: vec![],
                initial_barrier_info: Some((actors_to_create, initial_mutation)),
            },
            upstream_lag: metrics
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&table_id_str]),
            upstream_wait_progress_latency: metrics
                .snapshot_backfill_upstream_wait_progress_latency
                .with_guarded_label_values(&[&table_id_str]),
        }
    }

    /// Attach an upstream epoch to be notified on the finish of the creating job.
    /// Return whether the job is finished or not, and if finished, the upstream_epoch won't be attached.
    pub(super) fn attach_upstream_wait_finish_epoch(&mut self, upstream_epoch: u64) -> bool {
        match &mut self.status {
            CreatingStreamingJobStatus::Finishing(upstream_epoch_to_notify) => {
                assert_eq!(
                    *upstream_epoch_to_notify, None,
                    "should not attach wait finish epoch for twice"
                );
                if self.barrier_control.is_empty() {
                    true
                } else {
                    *upstream_epoch_to_notify = Some(upstream_epoch);
                    false
                }
            }
            CreatingStreamingJobStatus::ConsumingSnapshot { .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { .. } => {
                unreachable!("should not attach upstream wait finish epoch at non-finishing status")
            }
        }
    }

    pub(super) fn start_wait_progress_timer(&self) -> HistogramTimer {
        self.upstream_wait_progress_latency.start_timer()
    }

    pub(super) fn is_wait_on_worker(&self, worker_id: WorkerId) -> bool {
        self.barrier_control.is_wait_on_worker(worker_id)
            || self
                .status
                .active_graph_info()
                .map(|info| info.contains_worker(worker_id))
                .unwrap_or(false)
    }

    pub(super) fn on_new_worker_node_map(&self, node_map: &HashMap<WorkerId, WorkerNode>) {
        if let Some(info) = self.status.active_graph_info() {
            info.on_new_worker_node_map(node_map)
        }
    }

    pub(super) fn gen_ddl_progress(&self) -> DdlProgress {
        let progress = match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                ..
            } => {
                if create_mview_tracker.has_pending_finished_jobs() {
                    "Snapshot finished".to_string()
                } else {
                    let progress = create_mview_tracker
                        .gen_ddl_progress()
                        .remove(&self.info.table_fragments.table_id().table_id)
                        .expect("should exist");
                    format!("Snapshot [{}]", progress.progress)
                }
            }
            CreatingStreamingJobStatus::ConsumingLogStore {
                log_store_progress_tracker,
                ..
            } => {
                format!(
                    "LogStore [{}]",
                    log_store_progress_tracker.gen_ddl_progress()
                )
            }
            CreatingStreamingJobStatus::Finishing(_) => {
                format!(
                    "Finishing [epoch count: {}]",
                    self.barrier_control.inflight_barrier_count()
                )
            }
        };
        DdlProgress {
            id: self.info.table_fragments.table_id().table_id as u64,
            statement: self.info.definition.clone(),
            progress,
        }
    }

    pub(super) fn pinned_upstream_log_epoch(&self) -> Option<u64> {
        if matches!(&self.status, CreatingStreamingJobStatus::Finishing(_)) {
            return None;
        }
        // TODO: when supporting recoverable snapshot backfill, we should use the max epoch that has committed
        Some(max(
            self.barrier_control.max_collected_epoch().unwrap_or(0),
            self.backfill_epoch,
        ))
    }

    pub(super) fn may_inject_fake_barrier(
        &mut self,
        control_stream_manager: &mut ControlStreamManager,
        is_checkpoint: bool,
    ) -> MetaResult<()> {
        if let Some(barriers_to_inject) = self.status.may_inject_fake_barrier(is_checkpoint) {
            let graph_info = self
                .status
                .active_graph_info()
                .expect("must exist when having barriers to inject");
            let table_id = self.info.table_fragments.table_id();
            for CreatingJobInjectBarrierInfo {
                curr_epoch,
                prev_epoch,
                kind,
                new_actors,
                mutation,
            } in barriers_to_inject
            {
                let node_to_collect = control_stream_manager.inject_barrier(
                    Some(table_id),
                    mutation,
                    (&curr_epoch, &prev_epoch),
                    &kind,
                    graph_info,
                    Some(graph_info),
                    new_actors,
                    vec![],
                    vec![],
                )?;
                self.barrier_control.enqueue_epoch(
                    prev_epoch.value().0,
                    node_to_collect,
                    kind.is_checkpoint(),
                );
            }
        }
        Ok(())
    }

    pub(super) fn on_new_command(
        &mut self,
        control_stream_manager: &mut ControlStreamManager,
        command_ctx: &Arc<CommandContext>,
    ) -> MetaResult<()> {
        let table_id = self.info.table_fragments.table_id();
        let start_consume_upstream = if let Command::MergeSnapshotBackfillStreamingJobs(
            jobs_to_merge,
        ) = &command_ctx.command
        {
            jobs_to_merge.contains_key(&table_id)
        } else {
            false
        };
        let progress_epoch =
            if let Some(max_collected_epoch) = self.barrier_control.max_collected_epoch() {
                max(max_collected_epoch, self.backfill_epoch)
            } else {
                self.backfill_epoch
            };
        self.upstream_lag.set(
            command_ctx
                .prev_epoch
                .value()
                .0
                .saturating_sub(progress_epoch) as _,
        );
        match &mut self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot {
                pending_commands, ..
            } => {
                assert!(
                    !start_consume_upstream,
                    "should not start consuming upstream for a job that are consuming snapshot"
                );
                pending_commands.push(command_ctx.clone());
            }
            CreatingStreamingJobStatus::ConsumingLogStore { graph_info, .. } => {
                let node_to_collect = control_stream_manager.inject_barrier(
                    Some(table_id),
                    None,
                    (&command_ctx.curr_epoch, &command_ctx.prev_epoch),
                    &command_ctx.kind,
                    graph_info,
                    if start_consume_upstream {
                        None
                    } else {
                        Some(graph_info)
                    },
                    None,
                    vec![],
                    vec![],
                )?;
                self.barrier_control.enqueue_epoch(
                    command_ctx.prev_epoch.value().0,
                    node_to_collect,
                    command_ctx.kind.is_checkpoint(),
                );
                let prev_epoch = command_ctx.prev_epoch.value().0;
                if start_consume_upstream {
                    info!(
                        table_id = self.info.table_fragments.table_id().table_id,
                        prev_epoch, "start consuming upstream"
                    );
                    self.status = CreatingStreamingJobStatus::Finishing(None);
                }
            }
            CreatingStreamingJobStatus::Finishing { .. } => {
                assert!(
                    !start_consume_upstream,
                    "should not start consuming upstream for a job again"
                );
            }
        };
        Ok(())
    }

    pub(super) fn collect(
        &mut self,
        epoch: u64,
        worker_id: WorkerId,
        resp: BarrierCompleteResponse,
    ) {
        self.status.update_progress(&resp.create_mview_progress);
        self.barrier_control.collect(epoch, worker_id, resp);
    }

    pub(super) fn should_merge_to_upstream(&self) -> Option<InflightGraphInfo> {
        if let CreatingStreamingJobStatus::ConsumingLogStore {
            graph_info,
            ref log_store_progress_tracker,
        } = &self.status
            && log_store_progress_tracker.is_finished()
        {
            Some(graph_info.clone())
        } else {
            None
        }
    }

    pub(super) fn start_completing(&mut self) -> Option<(u64, Vec<BarrierCompleteResponse>, bool)> {
        self.barrier_control.start_completing()
    }

    pub(super) fn ack_completed(&mut self, completed_epoch: u64) -> Option<u64> {
        self.barrier_control.ack_completed(completed_epoch);
        if let CreatingStreamingJobStatus::Finishing(upstream_epoch_to_notify) = &self.status {
            if self.barrier_control.is_empty() {
                *upstream_epoch_to_notify
            } else {
                None
            }
        } else {
            None
        }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.barrier_control.is_empty()
            && matches!(&self.status, CreatingStreamingJobStatus::Finishing { .. })
    }
}
