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
use std::time::Duration;

use governor::clock::MonotonicClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{InsufficientCapacity, Quota, RateLimiter as GovernorRateLimiter};
use risingwave_common::catalog::Schema;
use tokio::sync::Semaphore;
use tokio::time::sleep;

use super::*;

#[cfg(not(madsim))]
type RateLimiterImpl = DefaultRateLimiter;
#[cfg(madsim)]
type RateLimiterImpl = SimRateLimiter;

/// Rate limiter. We can abstract this out when there's more use-cases for it.
/// Otherwise for now we can keep it local to `flow_control`.
trait RateLimiter {
    fn new(rate_limit: u32) -> Self;
    async fn until_n_ready(&self, n: usize);
}

struct DefaultRateLimiter {
    inner: GovernorRateLimiter<NotKeyed, InMemoryState, MonotonicClock>,
    rate_limit: u32,
}

impl RateLimiter for DefaultRateLimiter {
    fn new(rate_limit: u32) -> Self {
        let quota = Quota::per_second(NonZeroU32::new(rate_limit).unwrap());
        let clock = MonotonicClock;
        DefaultRateLimiter {
            inner: GovernorRateLimiter::direct_with_clock(quota, &clock),
            rate_limit,
        }
    }

    async fn until_n_ready(&self, n: usize) {
        let result = self
            .inner
            .until_n_ready(NonZeroU32::new(n as u32).unwrap())
            .await;
        if let Err(InsufficientCapacity(n)) = result {
            tracing::error!(
                "Rate Limit {:?} smaller than chunk cardinality {n}",
                self.rate_limit,
            );
        }
    }
}

struct SimRateLimiter {
    inner: Arc<Semaphore>,
    rate_limit: u32,
}

impl RateLimiter for SimRateLimiter {
    fn new(rate_limit: u32) -> Self {
        Self {
            inner: Arc::new(Semaphore::new(rate_limit as usize)),
            rate_limit,
        }
    }

    async fn until_n_ready(&self, n: usize) {
        if n > self.rate_limit as usize {
            tracing::error!(
                "Rate Limit {:?} smaller than chunk cardinality {n}",
                self.rate_limit,
            );
            return;
        }
        for _ in 0..n {
            let semaphore_ref = self.inner.clone();
            if let Ok(permit) = semaphore_ref.acquire_owned().await {
                tokio::spawn(async move {
                    sleep(Duration::from_secs(1)).await;
                    drop(permit);
                })
            };
        }
    }
}

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
        #[cfg(madsim)]
        tracing::warn!("FlowControlExecutor rate limiter is disabled in madsim as it will spawn system threads");
        Self { input, rate_limit }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let rate_limiter = self.rate_limit.map(RateLimiterImpl::new);
        #[for_await]
        for msg in self.input.execute() {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    if let Some(rate_limiter) = &rate_limiter {
                        rate_limiter.until_n_ready(chunk.cardinality()).await;
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
