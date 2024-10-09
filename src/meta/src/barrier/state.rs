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

use risingwave_common::util::epoch::Epoch;
use risingwave_pb::meta::PausedReason;

use crate::barrier::info::{InflightGraphInfo, InflightSubscriptionInfo};
use crate::barrier::{Command, CreateStreamingJobType, TracedEpoch};

/// `BarrierManagerState` defines the necessary state of `GlobalBarrierManager`.
pub struct BarrierManagerState {
    /// The last sent `prev_epoch`
    ///
    /// There's no need to persist this field. On recovery, we will restore this from the latest
    /// committed snapshot in `HummockManager`.
    in_flight_prev_epoch: Option<TracedEpoch>,

    /// Inflight running actors info.
    pub(crate) inflight_graph_info: InflightGraphInfo,

    pub(crate) inflight_subscription_info: InflightSubscriptionInfo,

    /// Whether the cluster is paused and the reason.
    paused_reason: Option<PausedReason>,
}

impl BarrierManagerState {
    pub fn new(
        in_flight_prev_epoch: Option<TracedEpoch>,
        inflight_graph_info: InflightGraphInfo,
        inflight_subscription_info: InflightSubscriptionInfo,
        paused_reason: Option<PausedReason>,
    ) -> Self {
        Self {
            in_flight_prev_epoch,
            inflight_graph_info,
            inflight_subscription_info,
            paused_reason,
        }
    }

    pub fn paused_reason(&self) -> Option<PausedReason> {
        self.paused_reason
    }

    pub fn set_paused_reason(&mut self, paused_reason: Option<PausedReason>) {
        if self.paused_reason != paused_reason {
            tracing::info!(current = ?self.paused_reason, new = ?paused_reason, "update paused state");
            self.paused_reason = paused_reason;
        }
    }

    pub fn in_flight_prev_epoch(&self) -> Option<&TracedEpoch> {
        self.in_flight_prev_epoch.as_ref()
    }

    /// Returns the epoch pair for the next barrier, and updates the state.
    pub fn next_epoch_pair(&mut self, command: &Command) -> Option<(TracedEpoch, TracedEpoch)> {
        if self.inflight_graph_info.is_empty()
            && !matches!(&command, Command::CreateStreamingJob { .. })
        {
            return None;
        };
        let in_flight_prev_epoch = self
            .in_flight_prev_epoch
            .get_or_insert_with(|| TracedEpoch::new(Epoch::now()));
        let prev_epoch = in_flight_prev_epoch.clone();
        let next_epoch = prev_epoch.next();
        *in_flight_prev_epoch = next_epoch.clone();
        Some((prev_epoch, next_epoch))
    }

    /// Returns the inflight actor infos that have included the newly added actors in the given command. The dropped actors
    /// will be removed from the state after the info get resolved.
    pub fn apply_command(
        &mut self,
        command: &Command,
    ) -> (InflightGraphInfo, InflightSubscriptionInfo) {
        // update the fragment_infos outside pre_apply
        let fragment_changes = if let Command::CreateStreamingJob {
            job_type: CreateStreamingJobType::SnapshotBackfill(_),
            ..
        } = command
        {
            None
        } else if let Some(fragment_changes) = command.fragment_changes() {
            self.inflight_graph_info.pre_apply(&fragment_changes);
            Some(fragment_changes)
        } else {
            None
        };
        self.inflight_subscription_info.pre_apply(command);

        let info = self.inflight_graph_info.clone();
        let subscription_info = self.inflight_subscription_info.clone();

        if let Some(fragment_changes) = fragment_changes {
            self.inflight_graph_info.post_apply(&fragment_changes);
        }
        self.inflight_subscription_info.post_apply(command);

        (info, subscription_info)
    }
}
