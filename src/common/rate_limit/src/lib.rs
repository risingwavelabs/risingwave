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

use std::future::Future;
use std::num::NonZeroU64;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use parking_lot::Mutex;
use pin_project_lite::pin_project;
use risingwave_common::catalog::TableId;
use risingwave_common::metrics::LabelGuardedUintGaugeVec;
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;
use risingwave_common_metrics::{
    LabelGuardedUintGauge, register_guarded_uint_gauge_vec_with_registry,
};
use tokio::sync::oneshot;
use tokio::time::Sleep;

static METRICS: LazyLock<LabelGuardedUintGaugeVec> = LazyLock::new(|| {
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
        Wait{#[pin] rx: oneshot::Receiver<()> },
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
            DelayProj::Wait { rx } => rx.poll(cx).map(|_| ()),
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

#[derive(Debug)]
pub enum Check {
    Ok,
    Retry(Duration),
    RetryAfter(oneshot::Receiver<()>),
}

impl Check {
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }
}

/// Shared behavior for rate limiters.
pub trait RateLimiterTrait: Send + Sync + 'static {
    /// Return current throttle policy.
    fn rate_limit(&self) -> RateLimit;

    /// Check if the request with the given quota is supposed to be allowed at the moment.
    ///
    /// On success, the quota will be consumed. [`Check::Ok`] is returned.
    /// The caller is supposed to proceed the request with the given quota.
    ///
    /// On failure, [`Check::Retry`] or [`Check::RetryAfter`] is returned.
    /// The caller is supposed to retry the check after the given duration or retry after receiving the signal.
    fn check(&self, quota: u64) -> Check;
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
            RateLimit::Pause => Box::new(PausedRateLimiter::default()),
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

    pub fn rate_limit(&self) -> RateLimit {
        self.inner.load().rate_limit()
    }

    pub fn check(&self, quota: u64) -> Check {
        self.inner.load().check(quota)
    }

    pub async fn wait(&self, quota: u64) {
        loop {
            match self.check(quota) {
                Check::Ok => return,
                Check::Retry(duration) => {
                    tokio::time::sleep(duration).await;
                }
                Check::RetryAfter(rx) => {
                    let _ = rx.await;
                }
            }
        }
    }
}

impl RateLimiterTrait for RateLimiter {
    /// Return current throttle policy.
    fn rate_limit(&self) -> RateLimit {
        self.rate_limit()
    }

    /// Check if the request with the given quota is supposed to be allowed at the moment.
    ///
    /// On success, the quota will be consumed. [`Check::Ok`] is returned.
    /// The caller is supposed to proceed the request with the given quota.
    ///
    /// On failure, [`Check::Retry`] or [`Check::RetryAfter`] is returned.
    /// The caller is supposed to retry the check after the given duration or retry after receiving the signal.
    fn check(&self, quota: u64) -> Check {
        self.check(quota)
    }
}

/// A rate limiter that supports multiple rate limit policy, online policy switch and metrics support.
pub struct MonitoredRateLimiter {
    inner: RateLimiter,
    metric: LabelGuardedUintGauge,
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

    fn check(&self, quota: u64) -> Check {
        let check = self.inner.check(quota);
        if matches! { check, Check::Ok} {
            self.report();
        }
        check
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

    fn check(&self, _: u64) -> Check {
        Check::Ok
    }
}

#[derive(Debug)]
pub struct PausedRateLimiter {
    waiters: Mutex<Vec<oneshot::Sender<()>>>,
}

impl Default for PausedRateLimiter {
    fn default() -> Self {
        Self {
            waiters: Mutex::new(vec![]),
        }
    }
}

impl Drop for PausedRateLimiter {
    fn drop(&mut self) {
        for tx in self.waiters.lock().drain(..) {
            let _ = tx.send(());
        }
    }
}

impl RateLimiterTrait for PausedRateLimiter {
    fn rate_limit(&self) -> RateLimit {
        RateLimit::Pause
    }

    fn check(&self, _: u64) -> Check {
        let (tx, rx) = oneshot::channel();
        self.waiters.lock().push(tx);
        Check::RetryAfter(rx)
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

    fn check(&self, quota: u64) -> Check {
        match self.inner.check(quota) {
            Ok(()) => Check::Ok,
            Err(duration) => Check::Retry(duration),
        }
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

    /// Total allowed quotas.
    total_allowed_quotas: AtomicU64,
    /// Total waited nanos.
    total_waited_nanos: AtomicI64,
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
            total_allowed_quotas: AtomicU64::new(0),
            total_waited_nanos: AtomicI64::new(0),
        }
    }

    /// Check if the request with the given quota is supposed to be allowed at the moment.
    ///
    /// On success, the quota will be consumed. The caller is supposed to proceed the quota.
    ///
    /// On failure, the minimal duration to retry `check()` is returned.
    fn check(&self, quota: u64) -> Result<(), Duration> {
        let now = Instant::now();
        let tnow = now.duration_since(self.origin).as_nanos() as u64;

        let weight = quota * self.scale.load(Ordering::Relaxed);

        let mut ltat = self.ltat.load(Ordering::Acquire);
        let tat = loop {
            let tat = ltat + weight;

            if tat > tnow {
                self.total_waited_nanos
                    .fetch_add((tat - tnow) as i64, Ordering::Relaxed);
                return Err(Duration::from_nanos(tat - tnow));
            }

            let ltat_new = std::cmp::max(tat, tnow);

            match self
                .ltat
                .compare_exchange(ltat, ltat_new, Ordering::Release, Ordering::Acquire)
            {
                Ok(_) => break tat,
                Err(cur) => ltat = cur,
            }
        };

        self.total_allowed_quotas
            .fetch_add(quota, Ordering::Relaxed);
        self.total_waited_nanos
            .fetch_sub((tnow - tat) as i64, Ordering::Relaxed);

        Ok(())
    }

    // // TODO(MrCroxx): Reserved for adaptive rate limiter.
    /// Average wait time per quota.
    ///
    /// Positive value indicates waits, negative value indicates there is spare rate limit.
    fn _avg_wait_nanos_per_quota(&self) -> i64 {
        let quotas = self.total_allowed_quotas.load(Ordering::Relaxed);
        if quotas == 0 {
            0
        } else {
            let nanos = self.total_waited_nanos.load(Ordering::Relaxed);
            nanos / quotas as i64
        }
    }

    // // TODO(MrCroxx): Reserved for adaptive rate limiter.
    /// Reset statistics.
    fn _reset_stats(&self) {
        self.total_allowed_quotas.store(0, Ordering::Relaxed);
        self.total_waited_nanos.store(0, Ordering::Relaxed);
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
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use rand::{Rng, rng as thread_rng};

    use super::*;

    const ERATIO: f64 = 0.05;
    const THREADS: usize = 8;
    const RATE: u64 = 1000;
    const DURATION: Duration = Duration::from_secs(10);

    /// To run this test:
    ///
    /// ```bash
    /// cargo test --package risingwave_common_rate_limit --lib -- tests::test_leak_bucket --exact --show-output --ignored
    /// ```
    #[ignore]
    #[test]
    fn test_leak_bucket() {
        let v = Arc::new(AtomicU64::new(0));
        let lb = Arc::new(LeakBucket::new(RATE.try_into().unwrap()));
        let task = |quota: u64, v: Arc<AtomicU64>, vs: Arc<LeakBucket>| {
            let start = Instant::now();
            loop {
                if start.elapsed() >= DURATION {
                    break;
                }
                while let Err(dur) = vs.check(quota) {
                    std::thread::sleep(dur);
                }
                if start.elapsed() >= DURATION {
                    break;
                }

                v.fetch_add(quota, Ordering::Relaxed);
            }
        };
        let mut handles = vec![];
        let mut rng = thread_rng();
        for _ in 0..THREADS {
            let rate = rng.random_range(10..20);
            let handle = std::thread::spawn({
                let v = v.clone();
                let limiter = lb.clone();
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

    /// To run this test:
    ///
    /// ```bash
    /// cargo test --package risingwave_common_rate_limit --lib -- tests::test_leak_bucket_overflow --exact --show-output --ignored
    /// ```
    #[ignore]
    #[test]
    fn test_leak_bucket_overflow() {
        let v = Arc::new(AtomicU64::new(0));
        let lb = Arc::new(LeakBucket::new(RATE.try_into().unwrap()));
        let task = |quota: u64, v: Arc<AtomicU64>, vs: Arc<LeakBucket>| {
            let start = Instant::now();
            loop {
                if start.elapsed() >= DURATION {
                    break;
                }
                while let Err(dur) = vs.check(quota) {
                    std::thread::sleep(dur);
                }
                if start.elapsed() >= DURATION {
                    break;
                }

                v.fetch_add(quota, Ordering::Relaxed);
            }
        };
        let mut handles = vec![];
        let mut rng = thread_rng();
        for _ in 0..THREADS {
            let rate = rng.random_range(500..1500);
            let handle = std::thread::spawn({
                let v = v.clone();
                let limiter = lb.clone();
                move || task(rate, v, limiter)
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let got = v.load(Ordering::Relaxed);
        let expected = RATE * DURATION.as_secs();
        let error = (v.load(Ordering::Relaxed) as isize
            - RATE as isize * DURATION.as_secs() as isize)
            .unsigned_abs();
        let eratio = error as f64 / (RATE as f64 * DURATION.as_secs_f64());
        assert!(
            eratio < ERATIO,
            "eratio: {}, target: {}, got: {}, expected: {}",
            eratio,
            ERATIO,
            got,
            expected
        );
        println!("eratio {eratio} < ERATIO {ERATIO}");
    }

    #[tokio::test]
    async fn test_pause_and_resume() {
        let l = Arc::new(RateLimiter::new(RateLimit::Pause));

        let delay = l.wait(1);

        let ll = l.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            ll.update(RateLimit::Disabled);
        });

        tokio::time::sleep(Duration::from_millis(1000)).await;
        delay.await;
    }
}
