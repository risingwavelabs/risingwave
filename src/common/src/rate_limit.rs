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

use std::future::Future;
use std::num::NonZeroU64;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use pin_project_lite::pin_project;
use risingwave_common::catalog::TableId;
use risingwave_common::metrics::LabelGuardedUintGaugeVec;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common_metrics::{
    register_guarded_uint_gauge_vec_with_registry, LabelGuardedUintGauge,
};
use tokio::time::Sleep;

static METRICS: LazyLock<LabelGuardedUintGaugeVec<1>> = LazyLock::new(|| {
    register_guarded_uint_gauge_vec_with_registry!(
        "backfill_rate_limit_bytes",
        "backfill rate limit bytes per second",
        &["table_id"],
        &GLOBAL_METRICS_REGISTRY
    )
    .unwrap()
});

pin_project! {
    #[derive(Debug)]
    #[project = DelayProj]
    pub enum Delay {
        Noop,
        Sleep{#[pin] sleep: Sleep},
        Infinite,
    }
}

impl Delay {
    pub fn new(duration: Duration) -> Self {
        match duration {
            Duration::ZERO => Self::Noop,
            Duration::MAX => Self::Infinite,
            dur => Self::Sleep {
                sleep: tokio::time::sleep(dur),
            },
        }
    }

    pub fn infinite() -> Self {
        Self::Infinite
    }
}

impl From<Duration> for Delay {
    fn from(value: Duration) -> Self {
        Self::new(value)
    }
}

impl Future for Delay {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            DelayProj::Noop => Poll::Ready(()),
            DelayProj::Sleep { sleep } => sleep.poll(cx),
            DelayProj::Infinite => Poll::Pending,
        }
    }
}

/// Rate limit policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimit {
    /// Rate limit disabled.
    Disabled,
    /// Rate limit with fixed rate.
    Fixed(NonZeroU64),
    /// Pause with 0 rate.
    Pause,
}

impl RateLimit {
    /// Return if the rate limit is set to pause policy.
    pub fn is_paused(&self) -> bool {
        matches! { self, Self::Pause }
    }

    pub fn to_u64(self) -> u64 {
        self.into()
    }
}

impl From<RateLimit> for u64 {
    fn from(rate_limit: RateLimit) -> Self {
        match rate_limit {
            RateLimit::Disabled => u64::MAX,
            RateLimit::Fixed(rate) => rate.get(),
            RateLimit::Pause => 0,
        }
    }
}

// Adapt to the old rate limit policy.
impl From<Option<u32>> for RateLimit {
    fn from(value: Option<u32>) -> Self {
        match value {
            None => Self::Disabled,
            Some(0) => Self::Pause,
            Some(rate) => Self::Fixed(unsafe { NonZeroU64::new_unchecked(rate as _) }),
        }
    }
}

/// Shared behavior for rate limiters.
pub trait RateLimiterTrait: Send + Sync + 'static {
    /// Return current throttle policy.
    fn rate_limit(&self) -> RateLimit;

    /// Try to book a request with given quota at the moment.
    ///
    /// On success, the request can be served immdeiately without needs of other operations.
    ///
    /// On failure, the minimal interval for retries is returned.
    fn try_book(&self, quota: u64) -> Result<(), Duration>;

    /// Book a request with given quota.
    ///
    /// Return the duration to serve the request since now.
    fn book(&self, quota: u64) -> Duration;

    /// Book a request with given quota and suspend task until there is enough quota and consume.
    #[must_use]
    fn wait(&self, quota: u64) -> Delay {
        self.book(quota).into()
    }
}

/// A rate limiter that supports multiple rate limit policy and online policy switch.
pub struct RateLimiter {
    inner: ArcSwap<Box<dyn RateLimiterTrait>>,
}

impl RateLimiter {
    fn new_inner(rate_limit: RateLimit) -> Box<dyn RateLimiterTrait> {
        match rate_limit {
            RateLimit::Disabled => Box::new(InfiniteRatelimiter),
            RateLimit::Fixed(rate) => Box::new(FixedRateLimiter::new(rate)),
            RateLimit::Pause => Box::new(PausedRateLimiter),
        }
    }

    /// Create a new rate limiter with given rate limit policy.
    pub fn new(rate_limit: RateLimit) -> Self {
        let inner: Box<dyn RateLimiterTrait> = Self::new_inner(rate_limit);
        let inner = ArcSwap::new(Arc::new(inner));
        Self { inner }
    }

    /// Update rate limit policy of the rate limiter.
    ///
    /// Returns the old rate limit policy.
    pub fn update(&self, rate_limit: RateLimit) -> RateLimit {
        let old = self.rate_limit();
        if self.rate_limit() == rate_limit {
            return old;
        }
        let inner = Self::new_inner(rate_limit);
        self.inner.store(Arc::new(inner));
        old
    }

    /// Monitor the rate limiter with related table id.
    pub fn monitored(self, table_id: impl Into<TableId>) -> MonitoredRateLimiter {
        let metric = METRICS.with_guarded_label_values(&[&table_id.into().to_string()]);
        let rate_limit = AtomicU64::new(self.rate_limit().to_u64());
        MonitoredRateLimiter {
            inner: self,
            metric,
            rate_limit,
        }
    }
}

impl RateLimiterTrait for RateLimiter {
    fn rate_limit(&self) -> RateLimit {
        self.inner.load().rate_limit()
    }

    fn try_book(&self, quota: u64) -> Result<(), Duration> {
        self.inner.load().try_book(quota)
    }

    fn book(&self, quota: u64) -> Duration {
        self.inner.load().book(quota)
    }
}

/// A rate limiter that supports multiple rate limit policy, online policy switch and metrics support.
pub struct MonitoredRateLimiter {
    inner: RateLimiter,
    metric: LabelGuardedUintGauge<1>,
    rate_limit: AtomicU64,
}

impl Deref for MonitoredRateLimiter {
    type Target = RateLimiter;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl RateLimiterTrait for MonitoredRateLimiter {
    fn rate_limit(&self) -> RateLimit {
        self.inner.rate_limit()
    }

    fn try_book(&self, quota: u64) -> Result<(), Duration> {
        let res = self.inner.try_book(quota);
        self.report();
        res
    }

    fn book(&self, quota: u64) -> Duration {
        let res = self.inner.book(quota);
        self.report();
        res
    }
}

impl MonitoredRateLimiter {
    /// Report the rate limit policy to the metric if updated.
    ///
    /// `report` is called automatically by each `until` call.
    fn report(&self) {
        let rate_limit = self.inner.rate_limit().to_u64();
        if rate_limit != self.rate_limit.load(Ordering::Relaxed) {
            self.rate_limit.store(rate_limit, Ordering::Relaxed);
            self.metric.set(rate_limit);
        }
    }
}

#[derive(Debug)]
pub struct InfiniteRatelimiter;

impl RateLimiterTrait for InfiniteRatelimiter {
    fn rate_limit(&self) -> RateLimit {
        RateLimit::Disabled
    }

    fn try_book(&self, _: u64) -> Result<(), Duration> {
        Ok(())
    }

    fn book(&self, _: u64) -> Duration {
        Duration::ZERO
    }
}

#[derive(Debug)]
pub struct PausedRateLimiter;

impl RateLimiterTrait for PausedRateLimiter {
    fn rate_limit(&self) -> RateLimit {
        RateLimit::Pause
    }

    fn try_book(&self, _: u64) -> Result<(), Duration> {
        Err(Duration::MAX)
    }

    fn book(&self, _: u64) -> Duration {
        Duration::MAX
    }

    fn wait(&self, _: u64) -> Delay {
        Delay::infinite()
    }
}

#[derive(Debug)]
pub struct FixedRateLimiter {
    inner: LeakBucket,
    rate: NonZeroU64,
}

impl FixedRateLimiter {
    pub fn new(rate: NonZeroU64) -> Self {
        let inner = LeakBucket::new(rate);
        Self { inner, rate }
    }
}

impl RateLimiterTrait for FixedRateLimiter {
    fn rate_limit(&self) -> RateLimit {
        RateLimit::Fixed(self.rate)
    }

    fn try_book(&self, quota: u64) -> Result<(), Duration> {
        self.inner.try_book(quota)
    }

    fn book(&self, quota: u64) -> Duration {
        self.inner.book(quota)
    }
}

/// A GCRA-like leak bucket visual scheduler that never deny request even whose weight is larger than tau and only count TAT.
#[derive(Debug)]
pub struct LeakBucket {
    /// Weight scale per 1.0 unit quota in nanosecond.
    ///
    /// scale is always non-zero.
    ///
    /// scale = rate / 1 (in second)
    scale: AtomicU64,

    /// Last request's TAT (Theoretical Arrival Time) in nanosecond.
    ltat: AtomicU64,

    /// Zero time instant.
    origin: Instant,

    /// Request count of the last window.
    window_requests: AtomicU64,
    /// Total waited request duration (nanos) of the last window.
    window_wait_nanos: AtomicU64,
}

impl LeakBucket {
    const NANO: u64 = Duration::from_secs(1).as_nanos() as u64;

    /// calculate the weight scale per 1.0 unit quota in nanosecond.
    fn scale(rate: NonZeroU64) -> u64 {
        std::cmp::max(Self::NANO / rate.get(), 1)
    }

    /// Create a new GCRA-like leak bucket visual scheduler with given rate.
    fn new(rate: NonZeroU64) -> Self {
        let scale = Self::scale(rate);

        let origin = Instant::now();
        let scale = AtomicU64::new(scale);

        Self {
            scale,
            ltat: AtomicU64::new(0),
            origin,
            window_requests: AtomicU64::new(0),
            window_wait_nanos: AtomicU64::new(0),
        }
    }

    /// Try to book a request with given quota at the moment.
    ///
    /// On success, the request can be served immdeiately without needs of other operations.
    ///
    /// On failure, the minimal interval for retries is returned.
    ///
    /// Note: `try_book` only update `awt` (Average Wait Time) on success.
    fn try_book(&self, quota: u64) -> Result<(), Duration> {
        let now = Instant::now();
        let tnow = now.duration_since(self.origin).as_nanos() as u64;

        let weight = quota * self.scale.load(Ordering::Relaxed);

        let mut ltat = self.ltat.load(Ordering::Acquire);
        loop {
            let tat = ltat + weight;

            if tat > tnow {
                return Err(Duration::from_nanos(tat - tnow));
            }

            let ltat_new = std::cmp::max(tat, tnow);

            match self
                .ltat
                .compare_exchange(ltat, ltat_new, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(cur) => ltat = cur,
            }
        }

        self.window_requests.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    /// Book a request with given quota.
    ///
    /// Return the duration to serve the request since now.
    fn book(&self, quota: u64) -> Duration {
        let now = Instant::now();
        let tnow = now.duration_since(self.origin).as_nanos() as u64;

        let weight = quota * self.scale.load(Ordering::Relaxed);

        let mut ltat = self.ltat.load(Ordering::Acquire);
        let tat = loop {
            let tat = ltat + weight;

            let ltat_new = std::cmp::max(tat, tnow);

            match self
                .ltat
                .compare_exchange(ltat, ltat_new, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => break tat,
                Err(cur) => ltat = cur,
            }
        };

        self.window_requests.fetch_add(1, Ordering::Relaxed);

        if tat <= tnow {
            Duration::ZERO
        } else {
            let nanos = tat - tnow;
            self.window_wait_nanos.fetch_add(nanos, Ordering::Relaxed);
            Duration::from_nanos(nanos)
        }
    }

    // TODO(MrCroxx): Reserved for adaptive rate limiter.
    /// Average wait time per request of the last window.
    fn _awt(&self) -> Duration {
        let requests = self.window_requests.load(Ordering::Relaxed);
        if requests == 0 {
            Duration::ZERO
        } else {
            let nanos = self.window_wait_nanos.load(Ordering::Relaxed);
            Duration::from_nanos(nanos / requests)
        }
    }

    // TODO(MrCroxx): Reserved for adaptive rate limiter.
    /// Clear the aws calculation window.
    fn _reset_awt(&self) {
        self.window_requests.store(0, Ordering::Relaxed);
        self.window_wait_nanos.store(0, Ordering::Relaxed);
    }

    // TODO(MrCroxx): Reserved for adaptive rate limiter.
    /// Update rate limit with the given rate.
    fn _update(&self, rate: NonZeroU64) {
        let scale = Self::scale(rate);
        self.scale.store(scale, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use rand::{thread_rng, Rng};

    use super::*;

    const ERATIO: f64 = 0.05;
    const THREADS: usize = 8;
    const RATE: u64 = 1000;
    const DURATION: Duration = Duration::from_secs(10);

    /// Run with:
    ///
    /// ```bash
    /// RUST_BACKTRACE=1 cargo test --package risingwave_common --lib -- rate_limit::tests::test_leak_bucket --exact --show-output --ignored
    /// ```
    #[ignore]
    #[test]
    fn test_leak_bucket() {
        let v = Arc::new(AtomicU64::new(0));
        let vs = Arc::new(LeakBucket::new(RATE.try_into().unwrap()));
        let task = |quota: u64, v: Arc<AtomicU64>, vs: Arc<LeakBucket>| {
            let start = Instant::now();
            loop {
                if start.elapsed() >= DURATION {
                    break;
                }
                let dur = vs.book(quota);
                std::thread::sleep(dur);

                v.fetch_add(quota, Ordering::Relaxed);
            }
        };
        let mut handles = vec![];
        let mut rng = thread_rng();
        for _ in 0..THREADS {
            let rate = rng.gen_range(10..20);
            let handle = std::thread::spawn({
                let v = v.clone();
                let limiter = vs.clone();
                move || task(rate, v, limiter)
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let error = (v.load(Ordering::Relaxed) as isize
            - RATE as isize * DURATION.as_secs() as isize)
            .unsigned_abs();
        let eratio = error as f64 / (RATE as f64 * DURATION.as_secs_f64());
        assert!(eratio < ERATIO, "eratio: {}, target: {}", eratio, ERATIO);
        println!("eratio {eratio} < ERATIO {ERATIO}");
    }
}
