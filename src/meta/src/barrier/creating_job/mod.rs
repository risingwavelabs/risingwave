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
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_meta_model_v2::WorkerId;
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
use crate::rpc::metrics::MetaMetrics;
use crate::MetaResult;

#[derive(Debug)]
pub(super) struct CreatingStreamingJobControl {
    pub(super) info: CreateStreamingJobCommandInfo,
    pub(super) snapshot_backfill_info: SnapshotBackfillInfo,
    backfill_epoch: u64,

    graph_info: InflightGraphInfo,

    barrier_control: CreatingStreamingJobBarrierControl,
    status: CreatingStreamingJobStatus,

    upstream_lag: LabelGuardedIntGauge<1>,
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
            graph_info: InflightGraphInfo::new(fragment_info),
            status: CreatingStreamingJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time: 0,
                pending_upstream_barriers: vec![],
                version_stats: version_stat.clone(),
                create_mview_tracker,
                snapshot_backfill_actors,
                backfill_epoch,
                pending_non_checkpoint_barriers: vec![],
                initial_barrier_info: Some((actors_to_create, initial_mutation)),
            },
            upstream_lag: metrics
                .snapshot_backfill_lag
                .with_guarded_label_values(&[&table_id_str]),
        }
    }

    pub(super) fn is_wait_on_worker(&self, worker_id: WorkerId) -> bool {
        self.barrier_control.is_wait_on_worker(worker_id)
            || (self.status.is_finishing() && self.graph_info.contains_worker(worker_id))
    }

    pub(super) fn on_new_worker_node_map(&self, node_map: &HashMap<WorkerId, WorkerNode>) {
        self.graph_info.on_new_worker_node_map(node_map)
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
        if self.status.is_finishing() {
            None
        } else {
            // TODO: when supporting recoverable snapshot backfill, we should use the max epoch that has committed
            Some(max(
                self.barrier_control.max_collected_epoch().unwrap_or(0),
                self.backfill_epoch,
            ))
        }
    }

    fn inject_barrier(
        table_id: TableId,
        control_stream_manager: &mut ControlStreamManager,
        barrier_control: &mut CreatingStreamingJobBarrierControl,
        pre_applied_graph_info: &InflightGraphInfo,
        applied_graph_info: Option<&InflightGraphInfo>,
        CreatingJobInjectBarrierInfo {
            curr_epoch,
            prev_epoch,
            kind,
            new_actors,
            mutation,
        }: CreatingJobInjectBarrierInfo,
    ) -> MetaResult<()> {
        let node_to_collect = control_stream_manager.inject_barrier(
            Some(table_id),
            mutation,
            (&curr_epoch, &prev_epoch),
            &kind,
            pre_applied_graph_info,
            applied_graph_info,
            new_actors,
            vec![],
            vec![],
        )?;
        barrier_control.enqueue_epoch(prev_epoch.value().0, node_to_collect, kind.is_checkpoint());
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
        if start_consume_upstream {
            info!(
                table_id = self.info.table_fragments.table_id().table_id,
                prev_epoch = command_ctx.prev_epoch.value().0,
                "start consuming upstream"
            );
        }
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
        if let Some(barrier_to_inject) = self
            .status
            .on_new_upstream_epoch(command_ctx, start_consume_upstream)
        {
            Self::inject_barrier(
                self.info.table_fragments.table_id(),
                control_stream_manager,
                &mut self.barrier_control,
                &self.graph_info,
                if start_consume_upstream {
                    None
                } else {
                    Some(&self.graph_info)
                },
                barrier_to_inject,
            )?;
        }
        Ok(())
    }

    pub(super) fn collect(
        &mut self,
        epoch: u64,
        worker_id: WorkerId,
        resp: BarrierCompleteResponse,
        control_stream_manager: &mut ControlStreamManager,
    ) -> MetaResult<()> {
        let prev_barriers_to_inject = self.status.update_progress(&resp.create_mview_progress);
        self.barrier_control.collect(epoch, worker_id, resp);
        if let Some(prev_barriers_to_inject) = prev_barriers_to_inject {
            let table_id = self.info.table_fragments.table_id();
            for info in prev_barriers_to_inject {
                Self::inject_barrier(
                    table_id,
                    control_stream_manager,
                    &mut self.barrier_control,
                    &self.graph_info,
                    Some(&self.graph_info),
                    info,
                )?;
            }
        }
        Ok(())
    }

    pub(super) fn should_merge_to_upstream(&self) -> Option<InflightGraphInfo> {
        if let CreatingStreamingJobStatus::ConsumingLogStore {
            ref log_store_progress_tracker,
        } = &self.status
            && log_store_progress_tracker.is_finished()
        {
            Some(self.graph_info.clone())
        } else {
            None
        }
    }
}

pub(super) enum CompleteJobType {
    /// The first barrier
    First,
    Normal,
    /// The last barrier to complete
    Finished,
}

impl CreatingStreamingJobControl {
    pub(super) fn start_completing(
        &mut self,
        min_upstream_inflight_epoch: Option<u64>,
    ) -> Option<(u64, Vec<BarrierCompleteResponse>, CompleteJobType)> {
        let (finished_at_epoch, epoch_end_bound) = match &self.status {
            CreatingStreamingJobStatus::Finishing(finish_at_epoch) => {
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
        };
        self.barrier_control.start_completing(epoch_end_bound).map(
            |(epoch, resps, is_first_commit)| {
                let status = if let Some(finish_at_epoch) = finished_at_epoch {
                    assert!(!is_first_commit);
                    if epoch == finish_at_epoch {
                        self.barrier_control.ack_completed(epoch);
                        assert!(self.barrier_control.is_empty());
                        CompleteJobType::Finished
                    } else {
                        CompleteJobType::Normal
                    }
                } else if is_first_commit {
                    CompleteJobType::First
                } else {
                    CompleteJobType::Normal
                };
                (epoch, resps, status)
            },
        )
    }

    pub(super) fn ack_completed(&mut self, completed_epoch: u64) {
        self.barrier_control.ack_completed(completed_epoch);
    }

    pub(super) fn is_finished(&self) -> bool {
        self.barrier_control.is_empty() && self.status.is_finishing()
    }
}
