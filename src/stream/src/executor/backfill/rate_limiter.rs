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

use std::num::NonZeroU32;
use std::time::{Duration, Instant};

use governor::clock::MonotonicClock;
use governor::middleware::NoOpMiddleware;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter as GovernorRateLimiter};
use parking_lot::Mutex;

#[derive(Debug, Clone, Default)]
pub struct TickInfo {
    pub processed_snapshot_rows: usize,
    pub processed_upstream_rows: usize,
}

pub enum BackfillRateLimiter {
    Infinite(InfiniteRateLimiter),
    Fixed(FixedRateLimiter),
    Adaptive(AdaptiveRateLimiter),
}

impl BackfillRateLimiter {
    pub fn infinite() -> Self {
        BackfillRateLimiter::Infinite(InfiniteRateLimiter::new(()))
    }

    pub fn fixed(rate: NonZeroU32) -> Self {
        BackfillRateLimiter::Fixed(FixedRateLimiter::new(rate))
    }

    pub fn adaptive(config: AdaptiveRateLimiterConfig) -> Self {
        BackfillRateLimiter::Adaptive(AdaptiveRateLimiter::new(config))
    }

    pub fn tick(&self, info: TickInfo) {
        match self {
            BackfillRateLimiter::Infinite(r) => r.tick(info),
            BackfillRateLimiter::Fixed(r) => r.tick(info),
            BackfillRateLimiter::Adaptive(r) => r.tick(info),
        }
    }

    pub fn check(&self) -> bool {
        match self {
            BackfillRateLimiter::Infinite(r) => r.check(),
            BackfillRateLimiter::Fixed(r) => r.check(),
            BackfillRateLimiter::Adaptive(r) => r.check(),
        }
    }

    pub async fn wait(&self) {
        match self {
            BackfillRateLimiter::Infinite(r) => r.wait().await,
            BackfillRateLimiter::Fixed(r) => r.wait().await,
            BackfillRateLimiter::Adaptive(r) => r.wait().await,
        }
    }
}

/// Shared behavior for Backfill rate limiters.
pub trait RateLimiter {
    type Config;

    fn new(config: Self::Config) -> Self;

    fn tick(&self, info: TickInfo);

    fn check(&self) -> bool;

    async fn wait(&self);
}

pub struct InfiniteRateLimiter;

impl RateLimiter for InfiniteRateLimiter {
    type Config = ();

    fn new(_: ()) -> Self {
        InfiniteRateLimiter
    }

    fn tick(&self, _: TickInfo) {}

    fn check(&self) -> bool {
        true
    }

    async fn wait(&self) {}
}

pub struct FixedRateLimiter(
    GovernorRateLimiter<NotKeyed, InMemoryState, MonotonicClock, NoOpMiddleware<Instant>>,
);

impl RateLimiter for FixedRateLimiter {
    type Config = NonZeroU32;

    fn new(rate: NonZeroU32) -> Self {
        FixedRateLimiter(GovernorRateLimiter::direct_with_clock(
            Quota::per_second(rate),
            &MonotonicClock,
        ))
    }

    fn tick(&self, _: TickInfo) {}

    fn check(&self) -> bool {
        self.0.check().is_ok()
    }

    async fn wait(&self) {
        self.0.until_ready().await
    }
}

#[derive(Debug, Clone)]
pub struct AdaptiveRateLimiterConfig {
    pub min_rate_limit: f64,
    pub max_rate_limit: f64,
    pub init_rate_limit: f64,
    pub step_min: f64,
    pub step_max: f64,
    pub step_ratio: f64,
}

struct AdaptiveRateLimiterInner {
    rate: f64,
    quota: f64,
    last_tick: Instant,
    last_update: Instant,
}

pub struct AdaptiveRateLimiter {
    inner: Mutex<AdaptiveRateLimiterInner>,

    config: AdaptiveRateLimiterConfig,
}

impl AdaptiveRateLimiterInner {
    fn tick(&mut self) {
        let now = Instant::now();

        let refill = now.duration_since(self.last_tick).as_secs_f64() * self.rate;
        self.quota = self.rate.min(self.quota + refill);

        self.last_tick = now;
    }

    fn check(&self) -> bool {
        self.quota >= 1.0
    }

    fn next(&self) -> Option<Duration> {
        if self.check() {
            return None;
        }

        let gap = Duration::from_secs_f64((1.0 - self.quota) / self.rate);
        Some(gap)
    }

    fn update(&mut self, config: &AdaptiveRateLimiterConfig, info: TickInfo) {
        let now = Instant::now();
        let dur = now.duration_since(self.last_update);

        let real_rate = info.processed_snapshot_rows as f64 / dur.as_secs_f64();

        let step = (self.rate * config.step_ratio).clamp(config.step_min, config.step_max);
        if real_rate >= self.rate {
            self.rate += step;
        } else {
            self.rate -= step;
        }
        self.rate = self
            .rate
            .clamp(config.min_rate_limit, config.max_rate_limit);

        self.last_update = now;
    }
}

impl RateLimiter for AdaptiveRateLimiter {
    type Config = AdaptiveRateLimiterConfig;

    fn new(config: Self::Config) -> Self {
        let now = Instant::now();
        Self {
            inner: Mutex::new(AdaptiveRateLimiterInner {
                rate: config.init_rate_limit,
                quota: config.init_rate_limit,
                last_tick: now,
                last_update: now,
            }),
            config,
        }
    }

    fn tick(&self, info: TickInfo) {
        let mut inner = self.inner.lock();
        inner.tick();
        inner.update(&self.config, info);
    }

    fn check(&self) -> bool {
        let mut inner = self.inner.lock();
        inner.tick();
        inner.check()
    }

    async fn wait(&self) {
        let wait = {
            let mut inner = self.inner.lock();
            inner.tick();
            inner.next()
        };

        if let Some(wait) = wait {
            tokio::time::sleep(wait).await;
        }
    }
}
