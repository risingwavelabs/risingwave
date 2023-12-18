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

use std::num::NonZeroU32;
use std::sync::atomic::{AtomicUsize, Ordering};

use governor::Quota;

type RateLimiter = governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::MonotonicClock,
>;

/// `LogSuppresser` is a helper to suppress log spamming.
pub struct LogSuppresser {
    /// The number of times the log has been suppressed. Will be returned and cleared when the
    /// rate limiter allows next log to be printed.
    suppressed_count: AtomicUsize,

    /// Inner rate limiter.
    rate_limiter: RateLimiter,
}

#[derive(Debug)]
pub struct LogSuppressed;

impl LogSuppresser {
    pub fn new(rate_limiter: RateLimiter) -> Self {
        Self {
            suppressed_count: AtomicUsize::new(0),
            rate_limiter,
        }
    }

    /// Check if the log should be suppressed.
    /// If the log should be suppressed, return `Err(LogSuppressed)`.
    /// Otherwise, return `Ok(usize)` with count of suppressed messages before.
    pub fn check(&self) -> core::result::Result<usize, LogSuppressed> {
        match self.rate_limiter.check() {
            Ok(()) => Ok(self.suppressed_count.swap(0, Ordering::Relaxed)),
            Err(_) => {
                self.suppressed_count.fetch_add(1, Ordering::Relaxed);
                Err(LogSuppressed)
            }
        }
    }
}

impl Default for LogSuppresser {
    /// Default rate limiter allows 1 log per second.
    fn default() -> Self {
        Self::new(RateLimiter::direct(Quota::per_second(
            NonZeroU32::new(1).unwrap(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;
    use std::time::Duration;

    use super::*;

    #[tokio::test]
    async fn demo() {
        let mut interval = tokio::time::interval(Duration::from_millis(100));
        for _ in 0..100 {
            interval.tick().await;
            static RATE_LIMITER: LazyLock<LogSuppresser> = LazyLock::new(|| {
                LogSuppresser::new(RateLimiter::direct(Quota::per_second(
                    NonZeroU32::new(5).unwrap(),
                )))
            });

            if let Ok(suppressed_count) = RATE_LIMITER.check() {
                println!("failed to foo bar. suppressed_count = {}", suppressed_count);
            }
        }
    }
}
