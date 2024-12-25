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
        if duration.is_zero() {
            Self::Noop
        } else {
            Self::Sleep {
                sleep: tokio::time::sleep(duration),
            }
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

/// Throttle policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Throttle {
    /// Throttle disabled.
    Disabled,
    /// Throttle with fixed rate.
    Fixed(NonZeroU64),
    /// Pause with 0 rate.
    Pause,
}

impl Throttle {
    /// Return if the throttle is set to pause policy.
    pub fn is_paused(&self) -> bool {
        matches! { self, Self::Pause }
    }

    pub fn to_u64(self) -> u64 {
        self.into()
    }
}

impl From<Throttle> for u64 {
    fn from(throttle: Throttle) -> Self {
        match throttle {
            Throttle::Disabled => u64::MAX,
            Throttle::Fixed(rate) => rate.get(),
            Throttle::Pause => 0,
        }
    }
}

/// Shared behavior for throttlers.
pub trait ThrottlerTrait: Send + Sync + 'static {
    /// Return current throttle policy.
    fn throttle(&self) -> Throttle;

    /// Check if the quota is enough at the moment.
    fn check(&self, quota: u64) -> bool;

    /// Suspend task until there is enough quota and consume.
    #[must_use]
    fn until(&self, quota: u64) -> Delay;
}

pub struct Throttler {
    inner: ArcSwap<Box<dyn ThrottlerTrait>>,
    metric: LabelGuardedUintGauge<1>,
    throttle: AtomicU64,
}

impl Throttler {
    /// Create a new throttler with given throttle policy.
    pub fn new(table_id: TableId, throttle: Throttle) -> Self {
        let inner = Self::new_inner(throttle);

        let throttle = inner.throttle().to_u64();
        let metric = METRICS.with_guarded_label_values(&[&table_id.to_string()]);
        metric.set(throttle);

        let inner = ArcSwap::new(Arc::new(inner));
        let throttle = throttle.into();
        Self {
            inner,
            metric,
            throttle,
        }
    }

    fn new_inner(throttle: Throttle) -> Box<dyn ThrottlerTrait> {
        match throttle {
            Throttle::Disabled => Box::new(InfiniteThrottler),
            Throttle::Fixed(rate) => Box::new(FixedThrottler::new(rate)),
            Throttle::Pause => Box::new(PausedThrottler),
        }
    }

    /// Get the current throttle policy of the throttler.
    pub fn throttle(&self) -> Throttle {
        self.inner.load().throttle()
    }

    /// Update throttle policy of the throttler.
    pub fn update(&self, throttle: Throttle) {
        if self.throttle() == throttle {
            return;
        }
        let inner = Self::new_inner(throttle);
        self.inner.store(Arc::new(inner));
    }

    /// Check if request with given quota is allowed at the moment.
    ///
    /// Note: `check` will not update the quota of the leak bucket.
    pub fn check(&self, quota: u64) -> bool {
        self.inner.load().check(quota)
    }

    /// Return a future that suspend the task for duration that the request with given quota will be allowed.
    ///
    /// Note: The quota of the leak bucket will be updated.
    pub fn until(&self, quota: u64) -> Delay {
        let delay = self.inner.load().until(quota);
        self.report();
        delay
    }

    /// Report the throttle policy to the metric if updated.
    ///
    /// `report` is called automatically by each `until` call.
    pub fn report(&self) {
        let throttle = self.inner.load().throttle().to_u64();
        if throttle != self.throttle.load(Ordering::Relaxed) {
            self.throttle.store(throttle, Ordering::Relaxed);
            self.metric.set(throttle);
        }
    }
}

#[derive(Debug)]
pub struct InfiniteThrottler;

impl ThrottlerTrait for InfiniteThrottler {
    fn throttle(&self) -> Throttle {
        Throttle::Disabled
    }

    fn check(&self, _: u64) -> bool {
        true
    }

    fn until(&self, _: u64) -> Delay {
        Delay::new(Duration::ZERO)
    }
}

#[derive(Debug)]
pub struct PausedThrottler;

impl ThrottlerTrait for PausedThrottler {
    fn throttle(&self) -> Throttle {
        Throttle::Pause
    }

    fn check(&self, _: u64) -> bool {
        false
    }

    fn until(&self, _: u64) -> Delay {
        Delay::infinite()
    }
}

#[derive(Debug)]
pub struct FixedThrottler {
    inner: LeakBucket,
    rate: NonZeroU64,
}

impl FixedThrottler {
    pub fn new(rate: NonZeroU64) -> Self {
        let inner = LeakBucket::new(rate);
        Self { inner, rate }
    }
}

impl ThrottlerTrait for FixedThrottler {
    fn throttle(&self) -> Throttle {
        Throttle::Fixed(self.rate)
    }

    fn check(&self, quota: u64) -> bool {
        self.inner.check(quota)
    }

    fn until(&self, quota: u64) -> Delay {
        self.inner.delay(quota).into()
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
        }
    }

    /// Check if request with given quota is allowed at the moment.
    ///
    /// Note: `check` will not update the quota of the leak bucket.
    fn check(&self, quota: u64) -> bool {
        let now = Instant::now();
        let tnow = now.duration_since(self.origin).as_nanos() as u64;

        let weight = quota * self.scale.load(Ordering::Relaxed);

        let ltat = self.ltat.load(Ordering::Acquire);
        ltat + weight <= tnow
    }

    /// Calculate the duration that the request with given quota will be allowed.
    ///
    /// Note: The quota of the leak bucket will be updated after calling `delay`.
    fn delay(&self, quota: u64) -> Duration {
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

        let t = self.origin + Duration::from_nanos(tat);
        if t <= now {
            Duration::ZERO
        } else {
            t.duration_since(now)
        }
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
    /// RUST_BACKTRACE=1 cargo test --package risingwave_common --lib -- throttle::tests::test_leak_bucket --exact --show-output --ignored
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
                let dur = vs.delay(quota);
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
