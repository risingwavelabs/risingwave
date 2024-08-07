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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::mem::take;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use tracing::{debug, info, warn};

use crate::barrier::command::CommandContext;
use crate::barrier::info::InflightGraphInfo;
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::rpc::ControlStreamManager;
use crate::barrier::{
    BarrierKind, Command, CreateStreamingJobCommandInfo, SnapshotBackfillInfo, TracedEpoch,
};
use crate::manager::WorkerId;
use crate::model::ActorId;
use crate::MetaResult;

#[derive(Debug)]
struct CreatingStreamingJobEpochState {
    node_to_collect: HashSet<WorkerId>,
    resps: Vec<BarrierCompleteResponse>,
}

#[derive(Debug)]
pub(super) enum CreatingStreamingJobStatus {
    ConsumingSnapshot {
        prev_epoch_fake_physical_time: u64,
        pending_commands: Vec<Arc<CommandContext>>,
        version_stats: HummockVersionStats,
        create_mview_tracker: CreateMviewProgressTracker,
        graph_info: InflightGraphInfo,
        /// The `prev_epoch` of pending non checkpoint barriers
        pending_non_checkpoint_barriers: Vec<u64>,
        snapshot_backfill_actors: HashMap<WorkerId, HashSet<ActorId>>,
    },
    ConsumingLogStore {
        graph_info: InflightGraphInfo,
    },
    Finishing(u64), // The prev epoch that marks it as Finishing
    Finished(u64),  // The prev epoch that marks it as Finished
}

#[derive(Debug)]
pub(super) struct CreatingStreamingJobControl {
    pub(super) info: CreateStreamingJobCommandInfo,
    pub(super) snapshot_backfill_info: SnapshotBackfillInfo,
    // key is prev_epoch of barrier
    inflight_barrier_queue: BTreeMap<u64, CreatingStreamingJobEpochState>,
    pub(super) collected_barrier: Vec<(u64, Vec<BarrierCompleteResponse>)>,
    backfill_epoch: Epoch,
    pub(super) status: CreatingStreamingJobStatus,
}

impl CreatingStreamingJobControl {
    pub(super) fn new(
        info: CreateStreamingJobCommandInfo,
        snapshot_backfill_info: SnapshotBackfillInfo,
        backfill_epoch: Epoch,
        version_stat: &HummockVersionStats,
    ) -> Self {
        info!(
            table_id = info.table_fragments.table_id().table_id,
            definition = info.definition,
            "new creating job"
        );
        let mut create_mview_tracker = CreateMviewProgressTracker::default();
        create_mview_tracker.update_tracking_jobs(Some((&info, None)), [], version_stat);
        let fragment_info: HashMap<_, _> = info.new_fragment_info().collect();
        let snapshot_backfill_actors_set = info.table_fragments.snapshot_backfill_actor_ids();
        let mut snapshot_backfill_actors: HashMap<_, HashSet<_>> = HashMap::new();
        for fragment in fragment_info.values() {
            for (actor_id, worker_node) in &fragment.actors {
                if snapshot_backfill_actors_set.contains(actor_id) {
                    snapshot_backfill_actors
                        .entry(*worker_node)
                        .or_default()
                        .insert(*actor_id);
                }
            }
        }

        Self {
            info,
            snapshot_backfill_info,
            inflight_barrier_queue: Default::default(),
            collected_barrier: vec![],
            backfill_epoch,
            status: CreatingStreamingJobStatus::ConsumingSnapshot {
                prev_epoch_fake_physical_time: 0,
                pending_commands: vec![],
                version_stats: version_stat.clone(),
                create_mview_tracker,
                graph_info: InflightGraphInfo::new(fragment_info),
                pending_non_checkpoint_barriers: vec![],
                snapshot_backfill_actors,
            },
        }
    }

    pub(super) fn is_wait_on_worker(&self, worker_id: WorkerId) -> bool {
        self.inflight_barrier_queue
            .values()
            .any(|epoch_state| epoch_state.node_to_collect.contains(&worker_id))
            || {
                match &self.status {
                    CreatingStreamingJobStatus::ConsumingSnapshot { graph_info, .. }
                    | CreatingStreamingJobStatus::ConsumingLogStore { graph_info, .. } => {
                        graph_info.contains_worker(worker_id)
                    }
                    CreatingStreamingJobStatus::Finishing(_)
                    | CreatingStreamingJobStatus::Finished(_) => false,
                }
            }
    }

    fn latest_epoch(&self) -> Option<u64> {
        self.inflight_barrier_queue
            .last_key_value()
            .map(|(epoch, _)| *epoch)
            .or_else(|| self.collected_barrier.last().map(|(epoch, _)| *epoch))
    }

    pub(super) fn gen_ddl_progress(&self) -> DdlProgress {
        let progress = match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot {
                create_mview_tracker,
                ..
            } => {
                if create_mview_tracker.has_pending_finished_jobs() {
                    "ConsumingSnapshot finished".to_string()
                } else {
                    let progress = create_mview_tracker
                        .gen_ddl_progress()
                        .remove(&self.info.table_fragments.table_id().table_id)
                        .expect("should exist");
                    format!("ConsumingSnapshot [{}]", progress.progress)
                }
            }
            CreatingStreamingJobStatus::ConsumingLogStore { .. } => {
                format!(
                    "ConsumingLogStore [remaining epoch count: {}]",
                    self.inflight_barrier_queue.len()
                )
            }
            CreatingStreamingJobStatus::Finishing(_) => {
                format!(
                    "Finishing [remaining epoch count: {}]",
                    self.inflight_barrier_queue.len()
                )
            }
            CreatingStreamingJobStatus::Finished(_) => {
                format!(
                    "Finished [remaining epoch count: {}]",
                    self.inflight_barrier_queue.len()
                )
            }
        };
        DdlProgress {
            id: self.info.table_fragments.table_id().table_id as u64,
            statement: self.info.definition.clone(),
            progress,
        }
    }

    pub(super) fn backfill_progress(&self) -> Option<u64> {
        match &self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot { .. } => Some(self.backfill_epoch.0),
            CreatingStreamingJobStatus::ConsumingLogStore { .. } => {
                if let Some((latest_collected_epoch, _)) = self.collected_barrier.last()
                    && *latest_collected_epoch > self.backfill_epoch.0
                {
                    Some(*latest_collected_epoch)
                } else {
                    Some(self.backfill_epoch.0)
                }
            }
            CreatingStreamingJobStatus::Finishing(_) | CreatingStreamingJobStatus::Finished(_) => {
                if self.inflight_barrier_queue.is_empty() {
                    None
                } else if let Some((latest_collected_epoch, _)) = self.collected_barrier.last()
                    && *latest_collected_epoch > self.backfill_epoch.0
                {
                    Some(*latest_collected_epoch)
                } else {
                    Some(self.backfill_epoch.0)
                }
            }
        }
    }

    pub(super) fn may_inject_fake_barrier(
        &mut self,
        control_stream_manager: &mut ControlStreamManager,
        node_map: &HashMap<WorkerId, WorkerNode>,
        is_checkpoint: bool,
    ) -> MetaResult<()> {
        if let CreatingStreamingJobStatus::ConsumingSnapshot {
            prev_epoch_fake_physical_time,
            pending_commands,
            create_mview_tracker,
            graph_info,
            pending_non_checkpoint_barriers,
            ..
        } = &mut self.status
        {
            let table_id = Some(self.info.table_fragments.table_id());
            if create_mview_tracker.has_pending_finished_jobs() {
                pending_non_checkpoint_barriers.push(self.backfill_epoch.0);

                let prev_epoch = Epoch::from_physical_time(*prev_epoch_fake_physical_time);
                let node_to_collect = control_stream_manager.inject_barrier(
                    table_id,
                    node_map,
                    None,
                    (
                        &TracedEpoch::new(self.backfill_epoch),
                        &TracedEpoch::new(prev_epoch),
                    ),
                    &BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers)),
                    graph_info,
                    Some(graph_info),
                    HashMap::new(),
                )?;
                let graph_info = take(graph_info);
                let pending_commands = take(pending_commands);
                self.enqueue_epoch(prev_epoch.0, node_to_collect);
                // finish consuming snapshot
                for command in pending_commands {
                    let node_to_collect = control_stream_manager.inject_barrier(
                        table_id,
                        node_map,
                        command.to_mutation(),
                        (&command.curr_epoch, &command.prev_epoch),
                        &command.kind,
                        &graph_info,
                        Some(&graph_info),
                        HashMap::new(),
                    )?;
                    self.enqueue_epoch(command.prev_epoch.value().0, node_to_collect);
                }
                self.status = CreatingStreamingJobStatus::ConsumingLogStore { graph_info };
            } else {
                let prev_epoch =
                    TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
                *prev_epoch_fake_physical_time += 1;
                let curr_epoch =
                    TracedEpoch::new(Epoch::from_physical_time(*prev_epoch_fake_physical_time));
                pending_non_checkpoint_barriers.push(prev_epoch.value().0);
                let kind = if is_checkpoint {
                    BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers))
                } else {
                    BarrierKind::Barrier
                };
                let node_to_collect = control_stream_manager.inject_barrier(
                    table_id,
                    node_map,
                    None,
                    (&curr_epoch, &prev_epoch),
                    &kind,
                    graph_info,
                    Some(graph_info),
                    HashMap::new(),
                )?;
                self.enqueue_epoch(prev_epoch.value().0, node_to_collect);
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
        let to_finish = if let Command::FinishCreateSnapshotBackfillStreamingJobs(jobs_to_finish) =
            &command_ctx.command
        {
            jobs_to_finish.contains_key(&table_id)
        } else {
            false
        };
        match &mut self.status {
            CreatingStreamingJobStatus::ConsumingSnapshot {
                pending_commands, ..
            } => {
                assert!(
                    !to_finish,
                    "should not finish a job that are consuming snapshot"
                );
                pending_commands.push(command_ctx.clone());
            }
            CreatingStreamingJobStatus::ConsumingLogStore { graph_info } => {
                let node_to_collect = control_stream_manager.inject_barrier(
                    Some(table_id),
                    &command_ctx.node_map,
                    if to_finish {
                        // erase the mutation on upstream except the last Finish command
                        command_ctx.to_mutation()
                    } else {
                        None
                    },
                    (&command_ctx.curr_epoch, &command_ctx.prev_epoch),
                    &command_ctx.kind,
                    graph_info,
                    if to_finish { None } else { Some(graph_info) },
                    HashMap::new(),
                )?;
                self.enqueue_epoch(command_ctx.prev_epoch.value().0, node_to_collect);
                if to_finish {
                    self.status =
                        CreatingStreamingJobStatus::Finishing(command_ctx.prev_epoch.value().0);
                }
            }
            CreatingStreamingJobStatus::Finishing(_) | CreatingStreamingJobStatus::Finished(_) => {
                assert!(!to_finish, "should not finish a job for twice");
            }
        }
        Ok(())
    }

    pub(super) fn enqueue_epoch(&mut self, epoch: u64, node_to_collect: HashSet<WorkerId>) {
        debug!(
            epoch,
            ?node_to_collect,
            table_id = self.info.table_fragments.table_id().table_id,
            "creating job enqueue epoch"
        );
        if let Some(latest_epoch) = self.latest_epoch() {
            assert!(epoch > latest_epoch, "{} {}", epoch, latest_epoch);
        }
        if node_to_collect.is_empty() {
            self.collected_barrier.push((epoch, vec![]));
        } else {
            self.inflight_barrier_queue.insert(
                epoch,
                CreatingStreamingJobEpochState {
                    node_to_collect,
                    resps: vec![],
                },
            );
        }
    }

    pub(super) fn all_collected(&self) -> bool {
        self.inflight_barrier_queue.is_empty()
    }

    pub(super) fn collect(
        &mut self,
        epoch: u64,
        worker_id: WorkerId,
        resp: BarrierCompleteResponse,
    ) -> Option<u64> {
        debug!(
            epoch,
            worker_id,
            table_id = self.info.table_fragments.table_id().table_id,
            "collect barrier from worker"
        );

        if let CreatingStreamingJobStatus::ConsumingSnapshot {
            create_mview_tracker,
            version_stats,
            ..
        } = &mut self.status
        {
            create_mview_tracker.update_tracking_jobs(
                None,
                resp.create_mview_progress.iter(),
                version_stats,
            );
        }

        let state = self
            .inflight_barrier_queue
            .get_mut(&epoch)
            .expect("should exist");
        assert!(state.node_to_collect.remove(&worker_id));
        state.resps.push(resp);
        while let Some((_, state)) = self.inflight_barrier_queue.first_key_value() {
            if state.node_to_collect.is_empty() {
                let (epoch, state) = self.inflight_barrier_queue.pop_first().expect("non-empty");
                self.collected_barrier.push((epoch, state.resps));
            } else {
                break;
            }
        }
        debug!(
            epoch,
            worker_id,
            collected = ?self.collected_barrier.iter().map(|(epoch, _)| *epoch).collect_vec(),
            inflight = ?self.inflight_barrier_queue.keys().collect_vec(),
            "collect"
        );
        if self.all_collected() {
            if let CreatingStreamingJobStatus::Finished(finished_epoch) = self.status {
                Some(finished_epoch)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub(super) fn should_finish(&self) -> Option<InflightGraphInfo> {
        if let CreatingStreamingJobStatus::ConsumingLogStore { graph_info } = &self.status {
            // TODO: should have a new policy before merged
            let len = self
                .collected_barrier
                .iter()
                .filter(|(epoch, _)| *epoch > self.backfill_epoch.0)
                .count();
            warn!(len, "jobs finished, waiting to collect more");
            if len > 10 {
                Some(graph_info.clone())
            } else {
                None
            }
        } else {
            None
        }
    }
}
