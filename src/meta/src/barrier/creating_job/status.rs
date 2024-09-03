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

use std::collections::{HashMap, HashSet};
use std::mem::take;
use std::sync::Arc;

use risingwave_common::util::epoch::Epoch;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_plan::barrier_mutation::Mutation;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use risingwave_pb::stream_service::BuildActorInfo;

use crate::barrier::command::CommandContext;
use crate::barrier::info::InflightGraphInfo;
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::{BarrierKind, TracedEpoch};
use crate::manager::WorkerId;
use crate::model::ActorId;

#[derive(Debug)]
pub(super) enum CreatingStreamingJobStatus {
    ConsumingSnapshot {
        prev_epoch_fake_physical_time: u64,
        pending_commands: Vec<Arc<CommandContext>>,
        version_stats: HummockVersionStats,
        create_mview_tracker: CreateMviewProgressTracker,
        graph_info: InflightGraphInfo,
        backfill_epoch: u64,
        /// The `prev_epoch` of pending non checkpoint barriers
        pending_non_checkpoint_barriers: Vec<u64>,
        snapshot_backfill_actors: HashMap<WorkerId, HashSet<ActorId>>,
        /// Info of the first barrier: (`actors_to_create`, `mutation`)
        /// Take the mutation out when injecting the first barrier
        initial_barrier_info: Option<(HashMap<WorkerId, Vec<BuildActorInfo>>, Mutation)>,
    },
    ConsumingLogStore {
        graph_info: InflightGraphInfo,
        start_consume_log_store_epoch: u64,
    },
    ConsumingUpstream {
        start_consume_upstream_epoch: u64,
        graph_info: InflightGraphInfo,
    },
    Finishing {
        start_consume_upstream_epoch: u64,
    },
}

pub(super) struct CreatingJobInjectBarrierInfo {
    pub curr_epoch: TracedEpoch,
    pub prev_epoch: TracedEpoch,
    pub kind: BarrierKind,
    pub new_actors: Option<HashMap<WorkerId, Vec<BuildActorInfo>>>,
    pub mutation: Option<Mutation>,
}

impl CreatingStreamingJobStatus {
    pub(super) fn active_graph_info(&self) -> Option<&InflightGraphInfo> {
        match self {
            CreatingStreamingJobStatus::ConsumingSnapshot { graph_info, .. }
            | CreatingStreamingJobStatus::ConsumingLogStore { graph_info, .. }
            | CreatingStreamingJobStatus::ConsumingUpstream { graph_info, .. } => Some(graph_info),
            CreatingStreamingJobStatus::Finishing { .. } => {
                // when entering `Finishing`, the graph will have been added to the upstream graph,
                // and therefore the separate graph info is inactive.
                None
            }
        }
    }

    pub(super) fn update_progress(
        &mut self,
        create_mview_progress: impl IntoIterator<Item = &CreateMviewProgress>,
    ) {
        if let Self::ConsumingSnapshot {
            create_mview_tracker,
            ref version_stats,
            ..
        } = self
        {
            create_mview_tracker.update_tracking_jobs(None, create_mview_progress, version_stats);
        }
    }

    /// return
    /// - Some(vec[(`curr_epoch`, `prev_epoch`, `barrier_kind`)]) of barriers to newly inject
    /// - Some(`graph_info`) when the status should transit to `ConsumingLogStore`
    pub(super) fn may_inject_fake_barrier(
        &mut self,
        is_checkpoint: bool,
    ) -> Option<(Vec<CreatingJobInjectBarrierInfo>, Option<InflightGraphInfo>)> {
        if let CreatingStreamingJobStatus::ConsumingSnapshot {
            prev_epoch_fake_physical_time,
            pending_commands,
            create_mview_tracker,
            graph_info,
            pending_non_checkpoint_barriers,
            ref backfill_epoch,
            initial_barrier_info,
            ..
        } = self
        {
            if create_mview_tracker.has_pending_finished_jobs() {
                assert!(initial_barrier_info.is_none());
                pending_non_checkpoint_barriers.push(*backfill_epoch);

                let prev_epoch = Epoch::from_physical_time(*prev_epoch_fake_physical_time);
                let barriers_to_inject =
                    [CreatingJobInjectBarrierInfo {
                        curr_epoch: TracedEpoch::new(Epoch(*backfill_epoch)),
                        prev_epoch: TracedEpoch::new(prev_epoch),
                        kind: BarrierKind::Checkpoint(take(pending_non_checkpoint_barriers)),
                        new_actors: None,
                        mutation: None,
                    }]
                    .into_iter()
                    .chain(pending_commands.drain(..).map(|command_ctx| {
                        CreatingJobInjectBarrierInfo {
                            curr_epoch: command_ctx.curr_epoch.clone(),
                            prev_epoch: command_ctx.prev_epoch.clone(),
                            kind: command_ctx.kind.clone(),
                            new_actors: None,
                            mutation: None,
                        }
                    }))
                    .collect();

                let graph_info = take(graph_info);
                Some((barriers_to_inject, Some(graph_info)))
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
                let (new_actors, mutation) =
                    if let Some((new_actors, mutation)) = initial_barrier_info.take() {
                        (Some(new_actors), Some(mutation))
                    } else {
                        Default::default()
                    };
                Some((
                    vec![CreatingJobInjectBarrierInfo {
                        curr_epoch,
                        prev_epoch,
                        kind,
                        new_actors,
                        mutation,
                    }],
                    None,
                ))
            }
        } else {
            None
        }
    }
}
