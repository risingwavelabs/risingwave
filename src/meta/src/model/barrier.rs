// Copyright 2023 RisingWave Labs
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

use risingwave_pb::meta::PausedReason;

use crate::barrier::TracedEpoch;

/// `BarrierManagerState` defines the necessary state of `GlobalBarrierManager`.
pub struct BarrierManagerState {
    /// The last sent `prev_epoch`
    ///
    /// There's no need to persist this field. On recovery, we will restore this from the latest
    /// committed snapshot in `HummockManager`.
    in_flight_prev_epoch: TracedEpoch,

    /// Whether the cluster is paused and the reason.
    paused_reason: Option<PausedReason>,
}

impl BarrierManagerState {
    pub fn new(in_flight_prev_epoch: TracedEpoch, paused_reason: Option<PausedReason>) -> Self {
        Self {
            in_flight_prev_epoch,
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

    pub fn in_flight_prev_epoch(&self) -> &TracedEpoch {
        &self.in_flight_prev_epoch
    }

    /// Returns the epoch pair for the next barrier, and updates the state.
    pub fn next_epoch_pair(&mut self) -> (TracedEpoch, TracedEpoch) {
        let prev_epoch = self.in_flight_prev_epoch.clone();
        let next_epoch = prev_epoch.next();
        self.in_flight_prev_epoch = next_epoch.clone();
        (prev_epoch, next_epoch)
    }
}
