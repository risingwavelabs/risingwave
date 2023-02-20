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

use risingwave_storage::StateStoreImpl;
use tokio::time::Instant;

pub enum SourceThrottlerImpl {
    MaxWaitBarrier(MaxWaitBarrierThrottler),
    StateStore(StateStoreImpl),
}

/// A constant to multiply when calculating the maximum time to wait for a barrier. This is due to
/// some latencies in network and cost in meta.
const WAIT_BARRIER_MULTIPLE_TIMES: u128 = 5;

pub struct MaxWaitBarrierThrottler {
    max_wait_barrier_time_ms: u128,
    last_barrier_time: Instant,
}

impl MaxWaitBarrierThrottler {
    pub fn new(barrier_interval_ms: u128) -> Self {
        Self {
            max_wait_barrier_time_ms: barrier_interval_ms * WAIT_BARRIER_MULTIPLE_TIMES,
            last_barrier_time: Instant::now(),
        }
    }

    fn should_pause(&self) -> bool {
        // We allow data to flow for `WAIT_BARRIER_MULTIPLE_TIMES` *
        // `expected_barrier_latency_ms` milliseconds, considering some
        // other latencies like network and cost in Meta.
        self.last_barrier_time.elapsed().as_millis() > self.max_wait_barrier_time_ms
    }
}

impl SourceThrottlerImpl {
    pub fn should_pause(&self) -> bool {
        match self {
            SourceThrottlerImpl::MaxWaitBarrier(inner) => inner.should_pause(),
            SourceThrottlerImpl::StateStore(inner) => {
                if let Some(hummock) = inner.as_hummock() {
                    return hummock.need_write_throttling();
                }
                false
            }
        }
    }

    pub fn on_barrier(&mut self) {
        #[allow(clippy::single_match)]
        match self {
            SourceThrottlerImpl::MaxWaitBarrier(inner) => {
                inner.last_barrier_time = Instant::now();
            }
            _ => {}
        }
    }
}
