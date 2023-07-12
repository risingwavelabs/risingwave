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

use risingwave_common::util::epoch::INVALID_EPOCH;

use crate::barrier::TracedEpoch;

/// `BarrierManagerState` defines the necessary state of `GlobalBarrierManager`.
pub struct BarrierManagerState {
    /// The last sent `prev_epoch`
    ///
    /// There's no need to persist this field. On recovery, we will restore this from the latest
    /// committed snapshot in `HummockManager`.
    in_flight_prev_epoch: TracedEpoch,
}

impl BarrierManagerState {
    pub fn new() -> Self {
        Self {
            in_flight_prev_epoch: TracedEpoch::new(INVALID_EPOCH.into()),
        }
    }

    pub fn update_inflight_prev_epoch(&mut self, new_epoch: TracedEpoch) {
        self.in_flight_prev_epoch = new_epoch;
    }

    pub fn in_flight_prev_epoch(&self) -> TracedEpoch {
        self.in_flight_prev_epoch.clone()
    }
}
