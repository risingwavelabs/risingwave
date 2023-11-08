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

use std::fmt::{Debug, Formatter};
use std::num::NonZeroU32;

use governor::clock::MonotonicClock;
use governor::{InsufficientCapacity, Quota, RateLimiter};
use risingwave_common::catalog::Schema;

use super::*;

/// Flow Control Executor is used to control the rate of the input executor.
///
/// Currently it is placed after the `BackfillExecutor`:
/// upstream `MaterializeExecutor` -> `BackfillExecutor` -> `FlowControlExecutor`
///
/// The rate limit is set statically at the moment, and cannot be changed in a running
/// stream graph.
///
/// It is used to throttle problematic MVs that are consuming too much resources.
pub struct FlowControlExecutor {
    input: BoxedExecutor,
    rate_limit: Option<u32>,
}

impl FlowControlExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(input: Box<dyn Executor>, rate_limit: Option<u32>) -> Self {
        Self { input, rate_limit }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let get_rate_limiter = |rate_limit: u32| {
            let quota = Quota::per_second(NonZeroU32::new(rate_limit).unwrap());
            let clock = MonotonicClock;
            RateLimiter::direct_with_clock(quota, &clock)
        };
        let rate_limiter = self.rate_limit.map(get_rate_limiter);
        #[for_await]
        for msg in self.input.execute() {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    let chunk_cardinality = chunk.cardinality();
                    let Some(n) = NonZeroU32::new(chunk_cardinality as u32) else {
                        // Handle case where chunk is empty
                        continue;
                    };
                    if let Some(rate_limiter) = &rate_limiter {
                        let result = rate_limiter.until_n_ready(n).await;
                        if let Err(InsufficientCapacity(_max_cells)) = result {
                            tracing::error!(
                                "Rate Limit {:?} smaller than chunk cardinality {chunk_cardinality}",
                                self.rate_limit,
                            );
                        }
                    }
                    yield Message::Chunk(chunk);
                }
                _ => yield msg,
            }
        }
    }
}

impl Debug for FlowControlExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowControlExecutor")
            .field("rate_limit", &self.rate_limit)
            .finish()
    }
}

impl Executor for FlowControlExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        self.input.pk_indices()
    }

    fn identity(&self) -> &str {
        "FlowControlExecutor"
    }
}
