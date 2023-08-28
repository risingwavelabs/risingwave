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
use governor::{Quota, RateLimiter};
use risingwave_common::catalog::Schema;

use super::*;

/// `FlowControlExecutor` project data with the `expr`. The `expr` takes a chunk of data,
/// and returns a new data chunk. And then, `FlowControlExecutor` will insert, delete
/// or update element into next operator according to the result of the expression.
pub struct FlowControlExecutor {
    input: BoxedExecutor,
    rate_limit: u32,
}

impl FlowControlExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(input: Box<dyn Executor>, rate_limit: u32) -> Self {
        Self { input, rate_limit }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let quota = Quota::per_second(NonZeroU32::new(self.rate_limit).unwrap());
        let clock = MonotonicClock;
        let rate_limiter = RateLimiter::direct_with_clock(quota, &clock);
        #[for_await]
        for msg in self.input.execute() {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    let result = rate_limiter
                        .until_n_ready(NonZeroU32::new(chunk.cardinality() as u32).unwrap())
                        .await;
                    assert!(
                        result.is_ok(),
                        "the capacity of rate_limiter must be larger than the cardinality of chunk"
                    );
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
