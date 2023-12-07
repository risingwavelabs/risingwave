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
    actor_ctx: ActorContextRef,
    identity: String,
    rate_limit: Option<u32>,
}

impl FlowControlExecutor {
    pub fn new(
        input: Box<dyn Executor>,
        actor_ctx: ActorContextRef,
        rate_limit: Option<u32>,
    ) -> Self {
        let identity = if rate_limit.is_some() {
            format!("{} (flow controlled)", input.identity())
        } else {
            input.identity().to_owned()
        };
        Self {
            input,
            actor_ctx,
            identity,
            rate_limit,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let get_rate_limiter = |rate_limit: u32| {
            let quota = Quota::per_second(NonZeroU32::new(rate_limit).unwrap());
            let clock = MonotonicClock;
            RateLimiter::direct_with_clock(quota, &clock)
        };
        let mut rate_limiter = self.rate_limit.map(get_rate_limiter);
        if self.rate_limit.is_some() {
            tracing::info!(id = self.actor_ctx.id, rate_limit = ?self.rate_limit, "actor starts with rate limit",);
        }

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
                        let limit = NonZeroU32::new(self.rate_limit.unwrap()).unwrap();
                        if n <= limit {
                            // `InsufficientCapacity` should never happen because we have done the check
                            rate_limiter.until_n_ready(n).await.unwrap();
                            yield Message::Chunk(chunk);
                        } else {
                            // Cut the chunk into smaller chunks
                            for chunk in chunk.split(limit.get() as usize) {
                                let n = NonZeroU32::new(chunk.cardinality() as u32).unwrap();
                                // Ditto.
                                rate_limiter.until_n_ready(n).await.unwrap();
                                yield Message::Chunk(chunk);
                            }
                        }
                    } else {
                        yield Message::Chunk(chunk);
                    }
                }
                Message::Barrier(barrier) => {
                    if let Some(mutation) = barrier.mutation.as_ref() {
                        if let Mutation::Throttle(actor_to_apply) = mutation.as_ref() {
                            if let Some(limit) = actor_to_apply.get(&self.actor_ctx.id) {
                                self.rate_limit = *limit;
                                rate_limiter = self.rate_limit.map(get_rate_limiter);
                                tracing::info!(
                                    id = self.actor_ctx.id,
                                    new_rate_limit = ?self.rate_limit,
                                    "actor rate limit changed",
                                );
                            }
                        }
                    }

                    yield Message::Barrier(barrier);
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
        &self.identity
    }
}
