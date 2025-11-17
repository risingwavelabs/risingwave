// Copyright 2025 RisingWave Labs
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

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::atomic::{AtomicUsize, Ordering};

use governor::Quota;

type RateLimiter = governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::MonotonicClock,
>;

/// `LogSuppressor` is a helper to suppress log spamming.
pub struct LogSuppressor {
    /// The number of times the log has been suppressed. Will be returned and cleared when the
    /// rate limiter allows next log to be printed.
    suppressed_count: AtomicUsize,

    /// Inner rate limiter.
    rate_limiter: RateLimiter,
}

#[derive(Debug)]
pub struct LogSuppressed;

impl LogSuppressor {
    pub fn new(rate_limiter: RateLimiter) -> Self {
        Self {
            suppressed_count: AtomicUsize::new(0),
            rate_limiter,
        }
    }

    /// Create a `LogSuppressor` that allows `per_second` logs per second.
    pub fn per_second(per_second: u32) -> Self {
        Self::new(RateLimiter::direct(Quota::per_second(
            NonZeroU32::new(per_second).unwrap(),
        )))
    }

    /// Create a `LogSuppressor` that allows `per_minute` logs per minute.
    pub fn per_minute(per_minute: u32) -> Self {
        Self::new(RateLimiter::direct(Quota::per_minute(
            NonZeroU32::new(per_minute).unwrap(),
        )))
    }

    /// Check if the log should be suppressed.
    /// If the log should be suppressed, return `Err(LogSuppressed)`.
    /// Otherwise, return `Ok(Some(..))` with count of suppressed messages since last check,
    /// or `Ok(None)` if there's none.
    pub fn check(&self) -> core::result::Result<Option<NonZeroUsize>, LogSuppressed> {
        match self.rate_limiter.check() {
            Ok(()) => Ok(NonZeroUsize::new(
                self.suppressed_count.swap(0, Ordering::Relaxed),
            )),
            Err(_) => {
                self.suppressed_count.fetch_add(1, Ordering::Relaxed);
                Err(LogSuppressed)
            }
        }
    }
}

impl Default for LogSuppressor {
    /// Default rate limiter allows 1 log per second.
    fn default() -> Self {
        Self::per_second(1)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;
    use std::time::{Duration, Instant};

    use tracing_subscriber::util::SubscriberInitExt;

    use super::*;

    #[tokio::test]
    async fn demo() {
        let _logger = tracing_subscriber::fmt::Subscriber::builder()
            .with_max_level(tracing::Level::ERROR)
            .set_default();

        let mut interval = tokio::time::interval(Duration::from_millis(10));

        let mut allowed = 0;
        let mut suppressed = 0;

        let start = Instant::now();

        for _ in 0..1000 {
            interval.tick().await;
            static RATE_LIMITER: LazyLock<LogSuppressor> =
                LazyLock::new(|| LogSuppressor::per_second(5));

            if let Ok(suppressed_count) = RATE_LIMITER.check() {
                suppressed += suppressed_count.map(|v| v.get()).unwrap_or_default();
                allowed += 1;
                tracing::error!(suppressed_count, "failed to foo bar");
            }
        }
        let duration = Instant::now().duration_since(start);

        tracing::error!(
            allowed,
            suppressed,
            ?duration,
            rate = allowed as f64 / duration.as_secs_f64()
        );
    }
}
