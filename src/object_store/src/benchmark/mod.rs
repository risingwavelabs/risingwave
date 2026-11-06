// Copyright 2026 RisingWave Labs
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

//! Fixed-work object-store benchmarks and a timed fast-reader interference experiment.
//! No Meta connection, Kubernetes resources, or production SST discovery is involved.

mod basic;
mod read;
// These tests measure real-time/network behavior, not simulated performance.
#[cfg(all(test, not(madsim)))]
mod tests;

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures::TryStreamExt;
use hdrhistogram::Histogram;
use rand::{RngCore, SeedableRng};
use risingwave_common::util::tokio_util::sync::CancellationToken;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio::task::JoinSet;
use tokio::time::Instant;

use crate::object::ObjectStoreImpl;
use crate::object::object_metrics::ObjectStoreMetrics;

/// Sizes are bytes and times are milliseconds, including in the JSON report.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BenchmarkConfig {
    pub profile: Profile,
    pub objects: usize,
    pub object_size: usize,
    pub concurrency: usize,
    pub range_size: usize,
    pub read_iterations: usize,
    /// Per-basic-phase wall-time budget; zero selects the original fixed-work counts.
    pub basic_duration_ms: u64,
    /// Per-basic-phase cap, including failed attempts. Data preparation is separate.
    pub basic_max_operations: usize,
    pub block_size: usize,
    pub pause_ms: u64,
    pub jitter_ms: u64,
    pub long_pause_every: usize,
    pub long_pause_ms: u64,
    pub short_get_size: usize,
    pub metadata_objects: usize,
    pub delete_batch_size: usize,
    pub fast_readers: usize,
    pub interference_ms: u64,
    pub seed: u64,
    pub keep_data: bool,
    /// Read a retained prefix without replacing or deleting its large objects.
    pub reuse_data: bool,
    /// Runs all three fixed-work modes, in this order, on the same ranges.
    pub read_order: Vec<ReadMode>,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            profile: Profile::All,
            objects: 16,
            object_size: 64 << 20,
            concurrency: 8,
            range_size: 1 << 20,
            read_iterations: 16,
            basic_duration_ms: 30_000,
            basic_max_operations: 10_000,
            block_size: 64 << 10,
            pause_ms: 10_000,
            jitter_ms: 0,
            long_pause_every: 0,
            long_pause_ms: 1000,
            short_get_size: 4 << 20,
            metadata_objects: 1000,
            delete_batch_size: 100,
            fast_readers: 2,
            interference_ms: 10_000,
            seed: 42,
            keep_data: false,
            reuse_data: false,
            read_order: vec![
                ReadMode::Continuous,
                ReadMode::Intermittent,
                ReadMode::Chunked,
            ],
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    Basic,
    Compactor,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadMode {
    Continuous,
    Intermittent,
    Chunked,
}

impl ReadMode {
    fn name(self) -> &'static str {
        match self {
            Self::Continuous => "continuous",
            Self::Intermittent => "intermittent",
            Self::Chunked => "chunked",
        }
    }
}

impl BenchmarkConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.basic_max_operations > 0,
            "basic_max_operations must be positive"
        );
        ensure!(
            Instant::now()
                .checked_add(Duration::from_millis(self.basic_duration_ms))
                .is_some(),
            "basic duration exceeds the platform clock range"
        );
        ensure!(
            self.objects > 0 && self.concurrency > 0,
            "objects and concurrency must be positive"
        );
        ensure!(
            self.object_size > 0 && self.object_size <= 1 << 30,
            "object_size must be in 1..=1 GiB (one shared payload is allocated)"
        );
        ensure!(
            self.range_size > 0 && self.range_size <= self.object_size,
            "range_size must fit in object_size"
        );
        ensure!(
            self.block_size > 0 && self.block_size <= self.object_size,
            "block_size must fit in object_size"
        );
        ensure!(
            self.short_get_size > 0 && self.short_get_size <= self.object_size,
            "short_get_size must fit in object_size"
        );
        ensure!(
            self.metadata_objects > 0 && (1..=1000).contains(&self.delete_batch_size),
            "metadata_objects must be positive and delete_batch_size must be in 1..=1000"
        );
        ensure!(
            self.fast_readers > 0 && self.interference_ms > 0,
            "fast_readers and interference_ms must be positive"
        );
        ensure!(
            self.pause_ms
                .checked_add(self.jitter_ms)
                .and_then(|n| n.checked_add(self.long_pause_ms))
                .is_some(),
            "pause duration overflow"
        );
        ensure!(
            self.read_order.len() == 3
                && [
                    ReadMode::Continuous,
                    ReadMode::Intermittent,
                    ReadMode::Chunked
                ]
                .iter()
                .all(|m| self.read_order.contains(m)),
            "read_order must contain each read mode exactly once"
        );
        ensure!(
            self.objects.checked_mul(self.object_size).is_some(),
            "dataset size overflow"
        );
        ensure!(
            self.metadata_objects
                .checked_mul(4096)
                .and_then(|n| n.checked_add(self.objects * self.object_size))
                .is_some(),
            "total dataset size overflow"
        );
        let max_pause = self.pause_ms + self.jitter_ms + self.long_pause_ms;
        ensure!(
            Instant::now()
                .checked_add(Duration::from_millis(max_pause))
                .is_some()
                && Instant::now()
                    .checked_add(Duration::from_millis(self.interference_ms))
                    .is_some(),
            "pause or interference duration exceeds the platform clock range"
        );
        ensure!(
            self.read_iterations > 0 && self.objects.checked_mul(self.read_iterations).is_some(),
            "read_iterations must be positive and the operation count must fit usize"
        );
        ensure!(
            !self.reuse_data || (self.profile == Profile::Compactor && self.keep_data),
            "reuse_data requires the compactor profile and keep_data"
        );
        ensure!(
            self.profile == Profile::Basic
                || self
                    .fast_readers
                    .checked_add(self.concurrency)
                    .is_some_and(|n| n <= self.objects),
            "compactor profile needs objects >= fast_readers + concurrency, so cohorts use distinct objects"
        );
        Ok(())
    }
}

/// Client-observed latencies, with microsecond resolution and three significant digits.
#[derive(Debug, Serialize)]
pub struct Distribution {
    pub samples: u64,
    pub mean_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

pub(super) struct Timings(Histogram<u64>);

impl Default for Timings {
    fn default() -> Self {
        Self(Histogram::new(3).expect("valid histogram precision"))
    }
}

impl Timings {
    fn record(&mut self, elapsed: Duration) {
        // The histogram auto-resizes; a u64 microsecond range has bounded memory.
        self.0
            .record(elapsed.as_micros().min(u64::MAX as u128) as u64)
            .expect("auto-resizing histogram");
    }

    fn merge(&mut self, other: &Self) {
        self.0.add(&other.0).expect("matching histogram precision");
    }

    fn summary(&self) -> Option<Distribution> {
        (!self.0.is_empty()).then(|| Distribution {
            samples: self.0.len(),
            mean_ms: self.0.mean() / 1000.0,
            p50_ms: self.0.value_at_quantile(0.50) as f64 / 1000.0,
            p95_ms: self.0.value_at_quantile(0.95) as f64 / 1000.0,
            p99_ms: self.0.value_at_quantile(0.99) as f64 / 1000.0,
            max_ms: self.0.max() as f64 / 1000.0,
        })
    }
}

#[derive(Default)]
pub(super) struct Sample {
    bytes: u64,
    items: u64,
    pause: Duration,
    first_data: Timings,
    first_poll: Timings,
    resume_poll: Timings,
    max_stream_chunk: usize,
}

pub(super) struct Outcome {
    sample: Sample,
    error: Option<anyhow::Error>,
    interrupted: bool,
    measured_elapsed: Option<Duration>,
}

impl Outcome {
    fn from_result(result: Result<Sample>) -> Self {
        match result {
            Ok(sample) => Self {
                sample,
                error: None,
                interrupted: false,
                measured_elapsed: None,
            },
            Err(error) => Self {
                sample: Sample::default(),
                error: Some(error),
                interrupted: false,
                measured_elapsed: None,
            },
        }
    }
}

#[derive(Default)]
pub(super) struct Accumulator {
    attempted: u64,
    succeeded: u64,
    failed: u64,
    interrupted: u64,
    bytes: u64,
    items: u64,
    pause: Duration,
    latency: Timings,
    failed_latency: Timings,
    first_data: Timings,
    first_poll: Timings,
    resume_poll: Timings,
    max_stream_chunk: usize,
    errors: Vec<String>,
}

impl Accumulator {
    fn add(&mut self, outcome: Outcome, elapsed: Duration) {
        self.attempted += 1;
        if !outcome.interrupted {
            let timings = if outcome.error.is_some() {
                &mut self.failed_latency
            } else {
                &mut self.latency
            };
            timings.record(outcome.measured_elapsed.unwrap_or(elapsed));
        }
        self.bytes += outcome.sample.bytes;
        self.items += outcome.sample.items;
        self.pause += outcome.sample.pause;
        self.first_data.merge(&outcome.sample.first_data);
        self.first_poll.merge(&outcome.sample.first_poll);
        self.resume_poll.merge(&outcome.sample.resume_poll);
        self.max_stream_chunk = self.max_stream_chunk.max(outcome.sample.max_stream_chunk);
        if outcome.interrupted {
            self.interrupted += 1;
        } else if let Some(error) = outcome.error {
            self.failed += 1;
            if self.errors.len() < 8 {
                self.errors.push(format!("{}", error.as_report()));
            }
        } else {
            self.succeeded += 1;
        }
    }

    fn merge(&mut self, other: Self) {
        self.attempted += other.attempted;
        self.succeeded += other.succeeded;
        self.failed += other.failed;
        self.interrupted += other.interrupted;
        self.bytes += other.bytes;
        self.items += other.items;
        self.pause += other.pause;
        self.latency.merge(&other.latency);
        self.failed_latency.merge(&other.failed_latency);
        self.first_data.merge(&other.first_data);
        self.first_poll.merge(&other.first_poll);
        self.resume_poll.merge(&other.resume_poll);
        self.max_stream_chunk = self.max_stream_chunk.max(other.max_stream_chunk);
        self.errors
            .extend(other.errors.into_iter().take(8 - self.errors.len()));
    }

    fn report(
        self,
        name: String,
        elapsed: Duration,
        expected: Option<usize>,
        cancelled: bool,
    ) -> PhaseReport {
        let incomplete = expected.is_some_and(|n| n as u64 != self.attempted);
        let seconds = elapsed.as_secs_f64();
        PhaseReport {
            item_unit: match name.as_str() {
                "preflight" => "functional_checks",
                "list" => "enumerated_objects",
                "batch_delete" => "deleted_objects",
                "multipart_put" | "prepare_data" | "prepare_metadata" => "uploaded_objects",
                "validate_retained_data" => "validated_objects",
                _ => "completed_object_reads",
            }
            .into(),
            operation_unit: operation_unit(&name).into(),
            name,
            status: if cancelled {
                "cancelled"
            } else if self.failed > 0 || incomplete {
                "failed"
            } else {
                "completed"
            }
            .into(),
            elapsed_ms: seconds * 1000.0,
            wall_elapsed_ms: seconds * 1000.0,
            setup_elapsed_ms: 0.0,
            setup_operations: 0,
            budget: None,
            stop_reason: if cancelled { "cancelled" } else { "fixed_work" }.into(),
            started_unix_ms: 0,
            drain_ms: 0.0,
            peak_inflight_logical_operations: 0,
            attempted: self.attempted,
            succeeded: self.succeeded,
            failed: self.failed,
            interrupted: self.interrupted,
            final_error_rate: (self.succeeded + self.failed > 0)
                .then(|| self.failed as f64 / (self.succeeded + self.failed) as f64),
            bytes: self.bytes,
            items: self.items,
            mib_per_sec: if seconds > 0.0 {
                self.bytes as f64 / (1 << 20) as f64 / seconds
            } else {
                0.0
            },
            logical_ops_per_sec: if seconds > 0.0 {
                self.succeeded as f64 / seconds
            } else {
                0.0
            },
            items_per_sec: if seconds > 0.0 {
                self.items as f64 / seconds
            } else {
                0.0
            },
            active_pause_ms: self.pause.as_secs_f64() * 1000.0,
            latency: self.latency.summary(),
            failed_latency: self.failed_latency.summary(),
            first_data: self.first_data.summary(),
            first_poll: self.first_poll.summary(),
            resume_poll: self.resume_poll.summary(),
            max_stream_chunk_bytes: self.max_stream_chunk,
            interference: None,
            errors: self.errors,
        }
    }
}

fn operation_unit(name: &str) -> &'static str {
    match name {
        "multipart_put" | "prepare_data" => "complete_object_upload",
        "range_get" => "ranged_get",
        "block_get" => "block_get",
        "list" => "complete_prefix_scan",
        "batch_delete" => "delete_batch",
        "preflight" => "functional_check",
        "prepare_metadata" => "small_object_upload",
        "validate_retained_data" => "object_metadata_check",
        _ => "object_read",
    }
}

#[derive(Debug, Serialize)]
pub struct MeasurementBudget {
    pub duration_ms: u64,
    pub duration_basis: String,
    pub max_operations: usize,
}

/// Per-cohort context for a timed interference comparison, not a performance verdict.
#[derive(Debug, Serialize)]
pub struct InterferenceCohort {
    pub comparison_valid: bool,
    pub invalid_reasons: Vec<String>,
    pub expected_workers: usize,
    pub failed_workers: usize,
    /// Sum of worker lifetimes within the observation window, excluding readiness/drain.
    pub active_worker_seconds: f64,
    pub object_pool_start: usize,
    pub object_pool_objects: usize,
    /// Eligible object capacity, not unique bytes actually read during the window.
    pub object_pool_bytes: u64,
    /// Distinct objects targeted by attempts, including initial readiness requests.
    pub requested_objects: usize,
    pub range_bytes: usize,
}

#[derive(Debug, Serialize)]
pub struct PhaseReport {
    pub name: String,
    pub operation_unit: String,
    pub item_unit: String,
    pub status: String,
    /// Rate denominator: measured wall time. Basic phases finish in-flight I/O;
    /// interference phases exclude their separately reported drain.
    /// DELETE replenishment between rounds is excluded.
    pub elapsed_ms: f64,
    pub wall_elapsed_ms: f64,
    pub setup_elapsed_ms: f64,
    pub setup_operations: u64,
    pub budget: Option<MeasurementBudget>,
    pub stop_reason: String,
    pub started_unix_ms: u128,
    /// Cleanup/drain after the timed interference window, excluded from its rates.
    pub drain_ms: f64,
    pub peak_inflight_logical_operations: usize,
    pub attempted: u64,
    pub succeeded: u64,
    pub failed: u64,
    /// Administratively stopped at the observation-window boundary, not I/O failures.
    pub interrupted: u64,
    pub final_error_rate: Option<f64>,
    /// Application bytes, including progress from failed streaming operations.
    pub bytes: u64,
    pub items: u64,
    pub mib_per_sec: f64,
    pub logical_ops_per_sec: f64,
    pub items_per_sec: f64,
    pub active_pause_ms: f64,
    /// Successful logical operations only; includes intentional pauses.
    pub latency: Option<Distribution>,
    pub failed_latency: Option<Distribution>,
    /// Stream initialization through receipt of the first complete nonempty chunk.
    pub first_data: Option<Distribution>,
    /// First next-chunk await, excluding explicit stream opening; may include lazy initialization.
    pub first_poll: Option<Distribution>,
    /// Subsequent next-chunk awaits, excluding the first item, pauses and EOF.
    /// In continuous mode these are subsequent reads without a pause.
    pub resume_poll: Option<Distribution>,
    pub max_stream_chunk_bytes: usize,
    pub interference: Option<InterferenceCohort>,
    pub errors: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct BenchmarkReport {
    pub schema_version: u32,
    pub profile_version: u32,
    pub prefix: String,
    pub config: BenchmarkConfig,
    pub planned_dataset_bytes: u64,
    pub phases: Vec<PhaseReport>,
    pub cleanup_status: String,
    pub cleanup_errors: Vec<String>,
    pub cancelled: bool,
    pub backpressure_verified: bool,
    pub notes: Vec<String>,
}

impl BenchmarkReport {
    pub fn succeeded(&self) -> bool {
        !self.cancelled
            && self.phases.iter().all(|p| {
                p.status == "completed"
                    && p.interference.as_ref().is_none_or(|c| c.comparison_valid)
            })
            && matches!(self.cleanup_status.as_str(), "completed" | "kept")
    }
}

/// Human-readable diagnostics on stderr; stdout remains a single JSON document.
pub fn terminal_summary(report: &BenchmarkReport) -> String {
    use std::fmt::Write;
    let mut text = String::new();
    writeln!(
        text,
        "\nObject-store benchmark (schema {}, profile {})",
        report.schema_version, report.profile_version
    )
    .unwrap();
    writeln!(
        text,
        "{:<46} {:<10} {:>9} {:>10} {:>10} {:>10} {:>10} {:>7}",
        "Phase / logical operation",
        "Status",
        "Samples",
        "MiB/s",
        "Ops/s",
        "p50 ms",
        "p99 ms",
        "Errors"
    )
    .unwrap();
    for phase in &report.phases {
        let percentile = |f: fn(&Distribution) -> f64| {
            phase
                .latency
                .as_ref()
                .map_or_else(|| "-".into(), |d| format!("{:.2}", f(d)))
        };
        writeln!(
            text,
            "{:<46} {:<10} {:>9} {:>10.2} {:>10.2} {:>10} {:>10} {:>7}",
            phase.name,
            phase.status,
            phase.latency.as_ref().map_or(0, |d| d.samples),
            phase.mib_per_sec,
            phase.logical_ops_per_sec,
            percentile(|d| d.p50_ms),
            percentile(|d| d.p99_ms),
            phase.failed
        )
        .unwrap();
        writeln!(text, "  unit={}; measured={:.1} ms; wall={:.1} ms; stop={}; items={} {} ({:.2}/s); interrupted={}",
            phase.operation_unit, phase.elapsed_ms, phase.wall_elapsed_ms, phase.stop_reason,
            phase.items, phase.item_unit, phase.items_per_sec, phase.interrupted).unwrap();
        if phase.setup_operations > 0 {
            writeln!(
                text,
                "  excluded setup: {} uploads, {:.1} ms",
                phase.setup_operations, phase.setup_elapsed_ms
            )
            .unwrap();
        }
        if phase.first_data.is_some()
            || phase.first_poll.is_some()
            || phase.resume_poll.is_some()
            || phase.active_pause_ms > 0.0
        {
            writeln!(
                text,
                "  active pause across readers={:.1} ms",
                phase.active_pause_ms
            )
            .unwrap();
            for (name, timing) in [
                ("first_data", &phase.first_data),
                ("first_poll", &phase.first_poll),
                ("resume_poll", &phase.resume_poll),
            ] {
                if let Some(d) = timing {
                    writeln!(
                        text,
                        "  {name}: n={}; p95={:.2} ms; p99={:.2} ms; max={:.2} ms",
                        d.samples, d.p95_ms, d.p99_ms, d.max_ms
                    )
                    .unwrap();
                } else {
                    writeln!(text, "  {name}: n=0; p95=-; p99=-; max=-").unwrap();
                }
            }
        }
        if let Some(c) = &phase.interference {
            writeln!(text, "  comparison={}; workers={}; failed_workers={}; active_worker_seconds={:.3}; pool={} objects / {} bytes; requested_objects={}; range={} bytes",
                if c.comparison_valid { "valid" } else { "invalid" }, c.expected_workers,
                c.failed_workers, c.active_worker_seconds, c.object_pool_objects,
                c.object_pool_bytes, c.requested_objects, c.range_bytes).unwrap();
            for reason in &c.invalid_reasons {
                writeln!(text, "  invalid comparison: {reason}").unwrap();
            }
        }
    }
    writeln!(
        text,
        "Cleanup: {}; prefix: {}",
        report.cleanup_status, report.prefix
    )
    .unwrap();
    writeln!(text, "Latency samples are successful logical operations, not HTTP requests or PUTPARTs. Small samples do not establish a stable p99.").unwrap();
    writeln!(
        text,
        "Cache state and server-side backpressure are unverified. Full details are in JSON.\n"
    )
    .unwrap();
    text
}

#[derive(Clone, Default)]
pub(super) struct Inflight {
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

struct InflightGuard(Arc<AtomicUsize>);

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Inflight {
    fn enter(&self) -> InflightGuard {
        let active = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.peak.fetch_max(active, Ordering::Relaxed);
        InflightGuard(self.active.clone())
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
}

pub(super) async fn jobs<F, Fut>(
    name: &str,
    count: usize,
    concurrency: usize,
    token: &CancellationToken,
    execute: F,
) -> PhaseReport
where
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Outcome> + Send + 'static,
{
    eprintln!("benchmark: {name} ({count} logical operations, {concurrency} workers)");
    let started_unix_ms = unix_ms();
    let (stats, elapsed, peak) = execute_jobs(count, concurrency, token, None, execute).await;
    let mut report = stats.report(name.into(), elapsed, Some(count), token.is_cancelled());
    report.started_unix_ms = started_unix_ms;
    report.peak_inflight_logical_operations = peak;
    report
}

/// Stop scheduling at the deadline; finish in-flight operations before returning.
async fn execute_jobs<F, Fut>(
    count: usize,
    concurrency: usize,
    token: &CancellationToken,
    deadline: Option<Instant>,
    execute: F,
) -> (Accumulator, Duration, usize)
where
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Outcome> + Send + 'static,
{
    let started = Instant::now();
    let execute = Arc::new(execute);
    let inflight = Inflight::default();
    let mut tasks = JoinSet::new();
    for worker in 0..concurrency.min(count) {
        let execute = execute.clone();
        let token = token.clone();
        let inflight = inflight.clone();
        tasks.spawn(async move {
            let mut stats = Accumulator::default();
            for index in (worker..count).step_by(concurrency) {
                if token.is_cancelled() || deadline.is_some_and(|d| Instant::now() >= d) {
                    break;
                }
                let started = Instant::now();
                let guard = inflight.enter();
                let outcome = execute(index).await;
                drop(guard);
                stats.add(outcome, started.elapsed());
                tokio::task::yield_now().await;
            }
            stats
        });
    }
    let mut stats = Accumulator::default();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(worker) => stats.merge(worker),
            Err(error) => {
                stats.failed += 1;
                stats
                    .errors
                    .push(format!("worker failed: {}", error.as_report()));
            }
        }
    }
    (stats, started.elapsed(), inflight.peak())
}

pub(super) fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// Counts only retries exposed by RW's outer monitoring layer, not SDK retries.
pub fn observed_retries(metrics: &ObjectStoreMetrics) -> u64 {
    use prometheus::core::Collector;
    metrics
        .request_retry_count
        .collect()
        .iter()
        .flat_map(|family| family.get_metric())
        .map(|metric| {
            metric
                .get_counter()
                .as_ref()
                .map_or(0, |counter| counter.value() as u64)
        })
        .sum()
}

/// Only objects whose keys are constructed here are ever deleted.
pub async fn run(
    store: Arc<ObjectStoreImpl>,
    config: BenchmarkConfig,
    prefix: String,
    token: CancellationToken,
) -> Result<BenchmarkReport> {
    config.validate()?;
    ensure!(
        prefix.starts_with("risectl-bench/")
            && prefix.ends_with('/')
            && prefix.len() > "risectl-bench/".len(),
        "expected a unique risectl-bench/<run-id>/ prefix"
    );
    let config = Arc::new(config);
    let basic = config.profile != Profile::Compactor;
    let compactor = config.profile != Profile::Basic;
    let large_keys = Arc::new(
        (0..config.objects)
            .map(|i| format!("{prefix}data/{i:08}"))
            .collect::<Vec<_>>(),
    );
    let metadata_keys = Arc::new(if basic {
        (0..config.metadata_objects)
            .map(|i| format!("{prefix}metadata/{i:08}"))
            .collect::<Vec<_>>()
    } else {
        vec![]
    });
    let mut phases = vec![];

    let ready = if config.reuse_data {
        true
    } else {
        phases.push(basic::preflight(store.clone(), &prefix, &token).await);
        phases.last().is_some_and(|p| p.status == "completed")
    };
    if config.reuse_data {
        let s = store.clone();
        let keys = large_keys.clone();
        let size = config.object_size;
        phases.push(
            jobs(
                "validate_retained_data",
                keys.len(),
                config.concurrency,
                &token,
                move |i| {
                    let s = s.clone();
                    let key = keys[i].clone();
                    async move {
                        Outcome::from_result(
                            async {
                                let metadata = s.metadata(&key).await?;
                                ensure!(
                                    metadata.total_size == size,
                                    "retained object {key} has size {}, expected {size}",
                                    metadata.total_size
                                );
                                Ok(Sample {
                                    items: 1,
                                    ..Default::default()
                                })
                            }
                            .await,
                        )
                    }
                },
            )
            .await,
        );
    } else if ready && !token.is_cancelled() {
        // Prepare payload before measuring PUT; share immutable bytes between workers.
        let mut payload = vec![0; config.object_size];
        rand::rngs::StdRng::seed_from_u64(config.seed).fill_bytes(&mut payload);
        let payload = Bytes::from(payload);
        let s = store.clone();
        let keys = large_keys.clone();
        let upload_payload = payload.clone();
        phases.push(
            jobs(
                "prepare_data",
                keys.len(),
                config.concurrency,
                &token,
                move |i| {
                    let s = s.clone();
                    let key = keys[i].clone();
                    let payload = payload.clone();
                    async move {
                        Outcome::from_result(
                            async {
                                let mut writer = s.streaming_upload(&key).await?;
                                for part in payload.chunks(1 << 20) {
                                    writer.write_bytes(Bytes::copy_from_slice(part)).await?;
                                }
                                writer.finish().await?;
                                Ok(Sample {
                                    bytes: payload.len() as u64,
                                    items: 1,
                                    ..Default::default()
                                })
                            }
                            .await,
                        )
                    }
                },
            )
            .await,
        );
        if basic && phases.last().is_some_and(|p| p.status == "completed") && !token.is_cancelled()
        {
            phases.push(
                basic::uploads(
                    store.clone(),
                    large_keys.clone(),
                    upload_payload,
                    config.clone(),
                    &token,
                )
                .await,
            );
        }
    }
    let data_ready = phases
        .iter()
        .find(|p| p.name == "prepare_data" || p.name == "validate_retained_data")
        .is_some_and(|p| p.status == "completed");
    if basic && data_ready && !token.is_cancelled() {
        phases
            .extend(basic::reads(store.clone(), large_keys.clone(), config.clone(), &token).await);
    }
    if compactor && data_ready && !token.is_cancelled() {
        for mode in &config.read_order {
            if token.is_cancelled() {
                break;
            }
            let s = store.clone();
            let keys = large_keys.clone();
            let cfg = config.clone();
            let mode = *mode;
            let cancel = token.clone();
            phases.push(
                jobs(
                    &format!("compactor_{}", mode.name()),
                    config.objects,
                    config.concurrency,
                    &token,
                    move |i| {
                        read::read_object(
                            s.clone(),
                            keys[i].clone(),
                            cfg.clone(),
                            mode,
                            i,
                            cancel.clone(),
                        )
                    },
                )
                .await,
            );
        }
        if !token.is_cancelled() {
            phases.extend(
                read::interference(store.clone(), large_keys.clone(), config.clone(), &token).await,
            );
        }
    }
    let mut metadata_started = false;
    if basic && data_ready && !token.is_cancelled() {
        metadata_started = true;
        let s = store.clone();
        let keys = metadata_keys.clone();
        phases.push(
            jobs(
                "prepare_metadata",
                keys.len(),
                config.concurrency,
                &token,
                move |i| {
                    let s = s.clone();
                    let key = keys[i].clone();
                    async move {
                        Outcome::from_result(
                            async {
                                s.upload(&key, Bytes::from(vec![i as u8; 4096])).await?;
                                Ok(Sample {
                                    bytes: 4096,
                                    items: 1,
                                    ..Default::default()
                                })
                            }
                            .await,
                        )
                    }
                },
            )
            .await,
        );
        if phases.last().is_some_and(|p| p.status == "completed") {
            phases.extend(
                basic::metadata(store.clone(), metadata_keys.clone(), config.clone(), &token).await,
            );
        }
    }

    let mut expected = vec![if config.reuse_data {
        "validate_retained_data"
    } else {
        "prepare_data"
    }];
    if basic {
        expected.extend([
            "multipart_put",
            "range_get",
            "block_get",
            "prepare_metadata",
            "list",
            "batch_delete",
        ]);
    }
    if compactor {
        expected.extend([
            "compactor_continuous",
            "compactor_intermittent",
            "compactor_chunked",
            "interference_fast_only/fast",
            "interference_continuous_competitors/fast",
            "interference_continuous_competitors/competitors",
            "interference_slow_competitors/fast",
            "interference_slow_competitors/competitors",
        ]);
    }
    for name in expected {
        if !phases.iter().any(|phase| phase.name == name) {
            let mut skipped =
                Accumulator::default().report(name.into(), Duration::ZERO, None, false);
            skipped.status = "skipped".into();
            skipped
                .errors
                .push("prerequisite failed or cancellation requested".into());
            phases.push(skipped);
        }
    }
    let mut cleanup_errors = vec![];
    if !config.keep_data {
        // Ignore cancellation for cleanup; never scan or delete the bucket as a whole.
        let keys: Vec<_> = large_keys
            .iter()
            .chain(metadata_keys.iter().take(if metadata_started {
                metadata_keys.len()
            } else {
                0
            }))
            .cloned()
            .collect();
        for batch in keys.chunks(config.delete_batch_size) {
            if let Err(error) = store
                .delete_objects(batch)
                .await
                .context("cleanup test objects")
            {
                cleanup_errors.push(format!("{}", error.as_report()));
            }
        }
    }
    Ok(BenchmarkReport {
        schema_version: 5,
        profile_version: 5,
        prefix,
        planned_dataset_bytes: (config.objects * config.object_size) as u64 + metadata_keys.len() as u64 * 4096,
        config: (*config).clone(),
        phases,
        cleanup_status: if config.keep_data { "kept" } else if cleanup_errors.is_empty() { "completed" } else { "failed" }.into(),
        cleanup_errors,
        cancelled: token.is_cancelled(),
        backpressure_verified: false,
        notes: vec![
            "Logical operations use the RW client; SDK/internal retries and HTTP request counts are not fully observed.".into(),
            "Basic phases stop scheduling at their wall-time or operation limit, then finish in-flight I/O. DELETE replenishment consumes the wall-time budget but is excluded from measured rates; setup and measured time are separate.".into(),
            "Upload latency is a complete streaming upload, not PUTPART latency. Successful and failed logical-operation latency are separate; interrupted operations have no completed latency.".into(),
            "Quantiles include sample counts and are descriptive, not evidence of a stable tail with a small sample.".into(),
            "LIST measures complete RW enumeration and may include metadata HEAD requests; it is not HTTP LIST requests/sec.".into(),
            "Cleanup deletes known keys; failed/cancelled multipart uploads may require server lifecycle cleanup.".into(),
            "Cache state is uncontrolled. Read order is recorded; intentional pauses are included in end-to-end latency.".into(),
            "Interference cohorts rotate disjoint object pools and reset deterministic request sequences for each comparison. Pool bytes are eligible capacity, not unique bytes read; requested object counts include readiness. Worker-seconds measure lifetimes within the observation window, not time spent in I/O. A worker failure invalidates both sides of a comparison.".into(),
            "Compactor reads consume whole stream chunks and pause between them; chunk sizes are transport-dependent (short_get_size in chunked mode). first_poll measures the first next-chunk await (possibly lazy initialization); resume_poll measures subsequent next-chunk awaits, excluding pauses and EOF (continuous mode has no pause). Both may complete from HTTP/TCP buffers; it is not network-only latency. Readiness consumes and excludes one whole chunk before the interference window.".into(),
            "No automatic performance pass/fail or server-side backpressure claim is made.".into(),
        ],
    })
}
