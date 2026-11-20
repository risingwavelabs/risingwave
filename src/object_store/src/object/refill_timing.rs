// Copyright 2022 RisingWave Labs
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

//! Q17 experiment: correlate one successful physical refill read with its HTTP phases.
//! Logical followers joining an existing physical read do not start another sample.
use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::task::{Context, Wake, Waker};
use std::time::{Duration, Instant};

use futures::task::AtomicWaker;
use opendal::{HttpTransport, HttpTransporter};
use prometheus::{
    HistogramVec, IntCounterVec, exponential_buckets, histogram_opts,
    register_histogram_vec_with_registry, register_int_counter_vec_with_registry,
};
use risingwave_common::monitor::GLOBAL_METRICS_REGISTRY;

tokio::task_local! {
    static REFILL: bool;
    static HTTP: Cell<Phases>;
}

#[derive(Default, Copy, Clone)]
struct Phases {
    calls: usize,
    sent: Option<Instant>,
    headers: Option<Instant>,
    collected: Option<Instant>,
}

static READS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    register_int_counter_vec_with_registry!(
        "object_store_refill_timing_events_total",
        "Q17 refill timing coverage",
        &["event"],
        &GLOBAL_METRICS_REGISTRY
    )
    .unwrap()
});
static TIMES: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec_with_registry!(
    histogram_opts!("object_store_refill_get_phase_seconds", "Q17 phases of the same successful single-HTTP refill GET; wake waits overlap their enclosing phase", exponential_buckets(0.00001, 2.0, 20).unwrap()), &["phase"], &GLOBAL_METRICS_REGISTRY).unwrap()
});

static RANGE_TIMES: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec_with_registry!(
        histogram_opts!(
            "object_store_refill_range_phase_seconds",
            "Q17 successful data refill range: permit acquisition through cache insertion",
            exponential_buckets(0.00001, 2.0, 20).unwrap()
        ),
        &["mode", "phase"],
        &GLOBAL_METRICS_REGISTRY
    )
    .unwrap()
});

pub fn observe_range(spawn: bool, before_read: Duration, read: Duration, apply: Duration) {
    let mode = if spawn { "range_spawn" } else { "scheduler" };
    for (phase, duration) in [
        ("before_read", before_read),
        ("store_read", read),
        ("apply", apply),
    ] {
        RANGE_TIMES
            .with_label_values(&[mode, phase])
            .observe(duration.as_secs_f64());
    }
}

pub async fn refill_get<F: Future>(read: F) -> F::Output {
    READS.with_label_values(&["logical"]).inc();
    REFILL.scope(true, read).await
}

pub(super) fn is_refill() -> bool {
    REFILL.try_with(|flag| *flag).unwrap_or(false)
}

pub(super) async fn with_origin<F: Future>(refill: bool, read: F) -> F::Output {
    if refill {
        REFILL.scope(true, read).await
    } else {
        read.await
    }
}

fn update(f: impl FnOnce(&mut Phases)) {
    let _ = HTTP.try_with(|cell| {
        let mut phases = cell.get();
        f(&mut phases);
        cell.set(phases);
    });
}

pub(super) fn body_collected() {
    update(|t| t.collected = Some(Instant::now()));
}

pub(super) struct TimedTransport(pub HttpTransporter);

impl HttpTransport for TimedTransport {
    async fn fetch(
        &self,
        req: http::Request<opendal::Buffer>,
    ) -> opendal::Result<http::Response<opendal::HttpBody>> {
        if HTTP.try_with(|_| ()).is_err() || req.method() != http::Method::GET {
            return self.0.fetch(req).await;
        }
        update(|t| {
            t.calls += 1;
            t.sent = Some(Instant::now());
        });
        let response = self.0.fetch(req).await?;
        if response.status().is_success() {
            update(|t| t.headers = Some(Instant::now()));
        }
        Ok(response)
    }
}

struct ReadWake {
    start: Instant,
    first: AtomicU64,
    target: AtomicWaker,
}

impl Wake for ReadWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let ns = self.start.elapsed().as_nanos() as u64 + 1;
        let _ = self
            .first
            .compare_exchange(0, ns, Ordering::Relaxed, Ordering::Relaxed);
        self.target.wake();
    }
}

pub(super) async fn measure<T, E>(read: impl Future<Output = Result<T, E>>) -> Result<T, E> {
    READS.with_label_values(&["physical"]).inc();
    HTTP.scope(Cell::default(), async {
        let start = Instant::now();
        let signal = Arc::new(ReadWake {
            start,
            first: AtomicU64::new(0),
            target: AtomicWaker::new(),
        });
        let waker = Waker::from(signal.clone());
        let mut previous = start;
        let mut wake_head = Duration::ZERO;
        let mut wake_body = Duration::ZERO;
        tokio::pin!(read);
        let result = poll_fn(|cx| {
            signal.target.register(cx.waker());
            let ns = signal.first.swap(0, Ordering::Relaxed);
            let now = Instant::now();
            if ns != 0 {
                let from = (start + Duration::from_nanos(ns - 1)).max(previous);
                let t = HTTP.with(Cell::get);
                if t.collected.is_none() {
                    if t.headers.is_some() {
                        wake_body += now.saturating_duration_since(from);
                    } else if t.sent.is_some() {
                        wake_head += now.saturating_duration_since(from);
                    }
                }
            }
            let poll = read.as_mut().poll(&mut Context::from_waker(&waker));
            previous = Instant::now();
            poll
        })
        .await;
        let end = Instant::now();
        let t = HTTP.with(Cell::get);
        match (result.is_ok(), t.calls, t.sent, t.headers, t.collected) {
            (true, 1, Some(sent), Some(headers), Some(collected))
                if sent <= headers && headers <= collected =>
            {
                for (phase, duration) in [
                    ("total", end - start),
                    ("headers", headers - sent),
                    ("body", collected - headers),
                    ("other", (sent - start) + (end - collected)),
                    ("wake_headers", wake_head),
                    ("wake_body", wake_body),
                ] {
                    TIMES
                        .with_label_values(&[phase])
                        .observe(duration.as_secs_f64());
                }
            }
            _ => READS.with_label_values(&["incomplete_or_retried"]).inc(),
        }
        result
    })
    .await
}
