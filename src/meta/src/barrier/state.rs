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

use std::collections::HashSet;
use std::mem::take;

use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::Epoch;
use risingwave_pb::meta::PausedReason;

use crate::barrier::info::{BarrierInfo, InflightDatabaseInfo, InflightSubscriptionInfo};
use crate::barrier::{BarrierKind, Command, CreateStreamingJobType, TracedEpoch};
use crate::controller::fragment::InflightFragmentInfo;

/// The latest state of `GlobalBarrierWorker` after injecting the latest barrier.
pub(super) struct BarrierWorkerState {
    /// The last sent `prev_epoch`
    ///
    /// There's no need to persist this field. On recovery, we will restore this from the latest
    /// committed snapshot in `HummockManager`.
    in_flight_prev_epoch: TracedEpoch,

    /// The `prev_epoch` of pending non checkpoint barriers
    pending_non_checkpoint_barriers: Vec<u64>,

    /// Inflight running actors info.
    pub(crate) inflight_graph_info: InflightDatabaseInfo,

    pub(crate) inflight_subscription_info: InflightSubscriptionInfo,

    /// Whether the cluster is paused and the reason.
    paused_reason: Option<PausedReason>,
}

impl BarrierWorkerState {
    pub fn new() -> Self {
        Self {
            in_flight_prev_epoch: TracedEpoch::new(Epoch::now()),
            pending_non_checkpoint_barriers: vec![],
            inflight_graph_info: InflightDatabaseInfo::empty(),
            inflight_subscription_info: InflightSubscriptionInfo::default(),
            paused_reason: None,
        }
    }

    pub fn recovery(
        in_flight_prev_epoch: TracedEpoch,
        inflight_graph_info: InflightDatabaseInfo,
        inflight_subscription_info: InflightSubscriptionInfo,
        paused_reason: Option<PausedReason>,
    ) -> Self {
        Self {
            in_flight_prev_epoch,
            pending_non_checkpoint_barriers: vec![],
            inflight_graph_info,
            inflight_subscription_info,
            paused_reason,
        }
    }

    pub fn paused_reason(&self) -> Option<PausedReason> {
        self.paused_reason
    }

    fn set_paused_reason(&mut self, paused_reason: Option<PausedReason>) {
        if self.paused_reason != paused_reason {
            tracing::info!(current = ?self.paused_reason, new = ?paused_reason, "update paused state");
            self.paused_reason = paused_reason;
        }
    }

    pub fn in_flight_prev_epoch(&self) -> &TracedEpoch {
        &self.in_flight_prev_epoch
    }

    /// Returns the `BarrierInfo` for the next barrier, and updates the state.
    pub fn next_barrier_info(
        &mut self,
        command: Option<&Command>,
        is_checkpoint: bool,
        curr_epoch: TracedEpoch,
    ) -> Option<BarrierInfo> {
        if self.inflight_graph_info.is_empty()
            && !matches!(&command, Some(Command::CreateStreamingJob { .. }))
        {
            return None;
        };
        assert!(
            self.in_flight_prev_epoch.value() < curr_epoch.value(),
            "curr epoch regress. {} > {}",
            self.in_flight_prev_epoch.value(),
            curr_epoch.value()
        );
        let prev_epoch = self.in_flight_prev_epoch.clone();
        self.in_flight_prev_epoch = curr_epoch.clone();
        self.pending_non_checkpoint_barriers
            .push(prev_epoch.value().0);
        let kind = if is_checkpoint {
            let epochs = take(&mut self.pending_non_checkpoint_barriers);
            BarrierKind::Checkpoint(epochs)
        } else {
            BarrierKind::Barrier
        };
        Some(BarrierInfo {
            prev_epoch,
            curr_epoch,
            kind,
        })
    }

    /// Returns the inflight actor infos that have included the newly added actors in the given command. The dropped actors
    /// will be removed from the state after the info get resolved.
    ///
    /// Return (`graph_info`, `subscription_info`, `table_ids_to_commit`, `jobs_to_wait`, `prev_paused_reason`)
    pub fn apply_command(
        &mut self,
        command: Option<&Command>,
    ) -> (
        InflightDatabaseInfo,
        InflightSubscriptionInfo,
        HashSet<TableId>,
        HashSet<TableId>,
        Option<PausedReason>,
    ) {
        // update the fragment_infos outside pre_apply
        let fragment_changes = if let Some(Command::CreateStreamingJob {
            job_type: CreateStreamingJobType::SnapshotBackfill(_),
            ..
        }) = command
        {
            None
        } else if let Some(fragment_changes) = command.and_then(Command::fragment_changes) {
            self.inflight_graph_info.pre_apply(&fragment_changes);
            Some(fragment_changes)
        } else {
            None
        };
        if let Some(command) = &command {
            self.inflight_subscription_info.pre_apply(command);
        }

        let info = self.inflight_graph_info.clone();
        let subscription_info = self.inflight_subscription_info.clone();

        if let Some(fragment_changes) = fragment_changes {
            self.inflight_graph_info.post_apply(&fragment_changes);
        }

        let mut table_ids_to_commit: HashSet<_> = info.existing_table_ids().collect();
        let mut jobs_to_wait = HashSet::new();
        if let Some(Command::MergeSnapshotBackfillStreamingJobs(jobs_to_merge)) = command {
            for (table_id, (_, graph_info)) in jobs_to_merge {
                jobs_to_wait.insert(*table_id);
                table_ids_to_commit.extend(InflightFragmentInfo::existing_table_ids(
                    graph_info.fragment_infos(),
                ));
                self.inflight_graph_info.extend(graph_info.clone());
            }
        }

        if let Some(command) = command {
            self.inflight_subscription_info.post_apply(command);
        }

        let prev_paused_reason = self.paused_reason;
        let curr_paused_reason = Command::next_paused_reason(command, prev_paused_reason);
        self.set_paused_reason(curr_paused_reason);

        (
            info,
            subscription_info,
            table_ids_to_commit,
            jobs_to_wait,
            prev_paused_reason,
        )
    }
}
