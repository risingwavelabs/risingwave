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

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::ops::Range;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::ensure;
use bytes::Bytes;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::util::tokio_util::sync::CancellationToken;
use thiserror_ext::AsReport;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::{
    Accumulator, BenchmarkConfig, InterferenceCohort, Outcome, PhaseReport, ReadMode, Sample,
};
use crate::object::{ObjectDataStream, ObjectError, ObjectStoreImpl};

// Stable deterministic mixing for offsets/jitter; not a cryptographic generator.
pub(super) fn offset(seed: u64, index: usize, maximum: usize) -> usize {
    let mut x = seed
        .wrapping_add(index as u64)
        .wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    ((x ^ (x >> 31)) as u128 % (maximum as u128 + 1)) as usize
}

#[derive(Debug, thiserror::Error)]
#[error("read interrupted by cancellation or observation-window boundary")]
struct Interrupted;

struct Request {
    key: String,
    start: usize,
    length: usize,
    mode: ReadMode,
    reader_id: usize,
    ready: Option<(Arc<Barrier>, Arc<OnceLock<Instant>>)>,
}

#[try_stream(ok = Bytes, error = ObjectError)]
async fn short_gets(
    store: Arc<ObjectStoreImpl>,
    key: String,
    base: usize,
    length: usize,
    chunk: usize,
) {
    for start in (base..base + length).step_by(chunk) {
        let end = start.saturating_add(chunk).min(base + length);
        let data = store.read(&key, start..end).await?;
        if data.len() != end - start {
            Err(ObjectError::internal(
                "short ranged GET returned unexpected length",
            ))?;
        }
        yield data;
    }
}

/// Consume whole SDK stream chunks without an AsyncRead adapter or background draining task.
pub(super) async fn read_object(
    store: Arc<ObjectStoreImpl>,
    key: String,
    config: Arc<BenchmarkConfig>,
    mode: ReadMode,
    reader_id: usize,
    token: CancellationToken,
) -> Outcome {
    let length = config.object_size;
    read_request(
        store,
        Request {
            key,
            start: 0,
            length,
            mode,
            reader_id,
            ready: None,
        },
        config,
        token,
    )
    .await
}

async fn read_request(
    store: Arc<ObjectStoreImpl>,
    request: Request,
    config: Arc<BenchmarkConfig>,
    token: CancellationToken,
) -> Outcome {
    let response = async {
        match request.mode {
            ReadMode::Chunked => Ok(short_gets(
                store,
                request.key.clone(),
                request.start,
                request.length,
                config.short_get_size,
            )
            .boxed()),
            _ => Ok(store
                .streaming_read(&request.key, request.start..request.start + request.length)
                .await?
                .into_stream()),
        }
    };
    read_response(response, &request, &config, &token).await
}

// Consume the complete item returned by the stream. In particular, do not retain
// a partially consumed Bytes in an AsyncRead adapter across a pause. An await can
// still complete immediately when the HTTP transport or kernel has buffered data.
async fn next_chunk(
    stream: &mut ObjectDataStream,
    sample: &mut Sample,
    first_poll: bool,
    token: &CancellationToken,
) -> anyhow::Result<Option<usize>> {
    let started = Instant::now();
    loop {
        let next = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(Interrupted.into()),
            next = stream.next() => next.transpose()?,
        };
        let Some(bytes) = next else {
            return Ok(None);
        };
        if bytes.is_empty() {
            continue;
        }
        let size = bytes.len();
        sample.bytes += size as u64;
        sample.max_stream_chunk = sample.max_stream_chunk.max(size);
        if first_poll {
            sample.first_poll.record(started.elapsed());
        } else {
            sample.resume_poll.record(started.elapsed());
        }
        // Drop the entire item before pausing or polling the stream again.
        return Ok(Some(size));
    }
}

async fn read_response(
    response: impl Future<Output = Result<ObjectDataStream, ObjectError>>,
    request: &Request,
    config: &BenchmarkConfig,
    token: &CancellationToken,
) -> Outcome {
    let mut sample = Sample::default();
    let first_started = Instant::now();
    let mut measured_started = None;
    // Initialization may be lazy. First-data timing includes opening the stream
    // and receiving its first whole chunk, with no separate one-byte probe.
    let initialized = async {
        let mut stream = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(Interrupted.into()),
            result = response => result?,
        };
        let size = next_chunk(&mut stream, &mut sample, true, token).await?;
        let size = size.ok_or_else(|| anyhow::anyhow!("unexpected EOF before first chunk"))?;
        ensure!(
            size <= request.length,
            "GET returned more bytes than the requested range"
        );
        sample.first_data.record(first_started.elapsed());
        Ok::<_, anyhow::Error>((stream, request.length - size))
    }
    .await;

    if let Some((barrier, start)) = &request.ready {
        // Even an initialization error joins readiness, so peers cannot deadlock.
        tokio::select! {
            _ = token.cancelled() => {},
            _ = barrier.wait() => {},
        }
        while start.get().is_none() && !token.is_cancelled() {
            tokio::task::yield_now().await;
        }
        measured_started = start.get().copied();
        // The entire readiness chunk was consumed before the observation window.
        sample.bytes = 0;
        sample.first_data = Default::default();
        sample.first_poll = Default::default();
        sample.resume_poll = Default::default();
    }

    let result = async {
        let (mut stream, mut remaining) = initialized?;
        let mut chunks = 1;
        while remaining > 0 {
            if request.mode != ReadMode::Continuous {
                let jitter = offset(
                    config.seed ^ request.reader_id as u64,
                    chunks,
                    config.jitter_ms as usize,
                ) as u64;
                let long = if config.long_pause_every > 0 && chunks % config.long_pause_every == 0 {
                    config.long_pause_ms
                } else {
                    0
                };
                let pause = Duration::from_millis(config.pause_ms + jitter + long);
                if !pause.is_zero() {
                    let started = Instant::now();
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => {
                            sample.pause += started.elapsed();
                            return Err(Interrupted.into());
                        },
                        _ = tokio::time::sleep(pause) => {},
                    }
                    sample.pause += started.elapsed();
                }
            }
            let size = next_chunk(&mut stream, &mut sample, false, token).await?;
            let size = size.ok_or_else(|| {
                anyhow::anyhow!("unexpected EOF with {remaining} bytes remaining")
            })?;
            ensure!(
                size <= remaining,
                "GET returned more bytes than the requested range"
            );
            remaining -= size;
            chunks += 1;
        }
        // Check EOF without a byte probe or an additional pause.
        ensure!(
            next_chunk(&mut stream, &mut sample, false, token)
                .await?
                .is_none(),
            "GET returned more bytes than the requested range"
        );
        sample.items = 1;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let interrupted = result.as_ref().is_err_and(|e| e.is::<Interrupted>());
    Outcome {
        sample,
        error: result.err().filter(|_| !interrupted),
        interrupted,
        measured_elapsed: measured_started.map(|start| start.elapsed()),
    }
}

fn cohort_pool(config: &BenchmarkConfig, is_fast: bool) -> Range<usize> {
    // Validation guarantees at least one distinct object per concurrent worker.
    let fast_objects = (config.objects as u128 * config.fast_readers as u128
        / (config.fast_readers + config.concurrency) as u128) as usize;
    if is_fast {
        0..fast_objects
    } else {
        fast_objects..config.objects
    }
}

/// Each worker cycles its own strided partition. The sequence restarts per comparison.
fn interference_request(
    config: &BenchmarkConfig,
    worker: usize,
    iteration: usize,
) -> (usize, Range<usize>) {
    let is_fast = worker < config.fast_readers;
    let pool = cohort_pool(config, is_fast);
    let (local_worker, workers, length) = if is_fast {
        (worker, config.fast_readers, config.range_size)
    } else {
        (
            worker - config.fast_readers,
            config.concurrency,
            config.object_size,
        )
    };
    let owned = (pool.len() - 1 - local_worker) / workers + 1;
    let object = pool.start + local_worker + (iteration % owned) * workers;
    let start = offset(
        config.seed ^ worker as u64,
        iteration,
        config.object_size - length,
    );
    (object, start..start + length)
}

#[derive(Default)]
struct Cohort {
    stats: Accumulator,
    objects: HashSet<usize>,
    active: Duration,
    failed_workers: usize,
}

/// The fast cohort is identical in all three experiments. Competitors either
/// consume continuously or intermittently. Both cohorts run through the window.
pub(super) async fn interference(
    store: Arc<ObjectStoreImpl>,
    keys: Arc<Vec<String>>,
    config: Arc<BenchmarkConfig>,
    shutdown: &CancellationToken,
) -> Vec<PhaseReport> {
    let mut reports = vec![];
    for (name, competitors, mode) in [
        ("fast_only", 0, ReadMode::Continuous),
        (
            "continuous_competitors",
            config.concurrency,
            ReadMode::Continuous,
        ),
        (
            "slow_competitors",
            config.concurrency,
            ReadMode::Intermittent,
        ),
    ] {
        if shutdown.is_cancelled() {
            break;
        }
        reports.extend(
            interference_comparison(
                store.clone(),
                keys.clone(),
                config.clone(),
                shutdown,
                name,
                competitors,
                mode,
            )
            .await,
        );
    }
    reports
}

/// Run one complete comparison; request cursors and readiness are local to this window.
pub(super) async fn interference_comparison(
    store: Arc<ObjectStoreImpl>,
    keys: Arc<Vec<String>>,
    config: Arc<BenchmarkConfig>,
    shutdown: &CancellationToken,
    name: &str,
    competitors: usize,
    mode: ReadMode,
) -> Vec<PhaseReport> {
    let mut reports = vec![];
    eprintln!(
        "benchmark: interference/{name} ({} fast + {competitors} competitors)",
        config.fast_readers
    );
    let end_window = shutdown.child_token();
    let barrier = Arc::new(Barrier::new(config.fast_readers + competitors + 1));
    let clock = Arc::new(OnceLock::new());
    let mut tasks = JoinSet::new();
    let mut worker_cohorts = HashMap::new();
    let fast_inflight = super::Inflight::default();
    let competitor_inflight = super::Inflight::default();
    for id in 0..config.fast_readers + competitors {
        let is_fast = id < config.fast_readers;
        let inflight = if is_fast {
            fast_inflight.clone()
        } else {
            competitor_inflight.clone()
        };
        let s = store.clone();
        let cfg = config.clone();
        let token = end_window.clone();
        let keys = keys.clone();
        let ready = barrier.clone();
        let clock = clock.clone();
        let task = tasks.spawn(async move {
            let mut stats = Accumulator::default();
            let mut objects = HashSet::new();
            let mut iteration = 0usize;
            if is_fast {
                tokio::select! { _ = ready.wait() => {}, _ = token.cancelled() => {} }
                while clock.get().is_none() && !token.is_cancelled() {
                    tokio::task::yield_now().await;
                }
            }
            let mut initial = !is_fast;
            while !token.is_cancelled() {
                let (object, range) = interference_request(&cfg, id, iteration);
                objects.insert(object);
                iteration = iteration.wrapping_add(1);
                let request = Request {
                    key: keys[object].clone(),
                    start: range.start,
                    length: range.len(),
                    mode: if is_fast { ReadMode::Continuous } else { mode },
                    reader_id: id,
                    ready: initial.then(|| (ready.clone(), clock.clone())),
                };
                initial = false;
                let started = Instant::now();
                let guard = inflight.enter();
                let outcome = read_request(s.clone(), request, cfg.clone(), token.clone()).await;
                drop(guard);
                let failed = outcome.error.is_some();
                stats.add(outcome, started.elapsed());
                if failed {
                    break;
                }
                tokio::task::yield_now().await;
            }
            (stats, objects, Instant::now())
        });
        worker_cohorts.insert(task.id(), is_fast);
    }
    tokio::select! {
        _ = barrier.wait() => {},
        _ = shutdown.cancelled() => {},
    }
    let started = Instant::now();
    let started_unix_ms = super::unix_ms();
    let _ = clock.set(started);
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_millis(config.interference_ms)) => {},
        _ = shutdown.cancelled() => {},
    }
    let elapsed = started.elapsed();
    end_window.cancel();
    let mut fast = Cohort::default();
    let mut slow = Cohort::default();
    while let Some(result) = tasks.join_next_with_id().await {
        let id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        let cohort = if worker_cohorts[&id] {
            &mut fast
        } else {
            &mut slow
        };
        match result {
            Ok((_, (worker, objects, finished))) => {
                cohort.failed_workers += usize::from(worker.failed > 0);
                cohort.active += finished.saturating_duration_since(started).min(elapsed);
                cohort.objects.extend(objects);
                cohort.stats.merge(worker);
            }
            Err(error) => {
                cohort.failed_workers += 1;
                cohort.stats.errors.push(format!(
                    "worker failed; lifetime unavailable: {}",
                    error.as_report()
                ));
            }
        }
    }
    let drain_ms = started.elapsed().saturating_sub(elapsed).as_secs_f64() * 1000.0;
    let mut invalid_reasons = vec![];
    if shutdown.is_cancelled() {
        invalid_reasons.push("observation window cancelled".into());
    }
    for (name, cohort) in [("fast", &fast), ("competitors", &slow)] {
        if cohort.failed_workers > 0 {
            invalid_reasons.push(format!("{name}: {} workers failed", cohort.failed_workers));
        }
    }
    if fast.stats.succeeded == 0 {
        invalid_reasons.push("fast cohort completed no reads".into());
    }
    for (is_fast, cohort) in [(true, fast), (false, slow)] {
        if !is_fast && competitors == 0 {
            continue;
        }
        let pool = cohort_pool(&config, is_fast);
        let no_fast_reads = is_fast && cohort.stats.succeeded == 0;
        let mut report = cohort.stats.report(
            format!(
                "interference_{name}/{}",
                if is_fast { "fast" } else { "competitors" }
            ),
            elapsed,
            None,
            shutdown.is_cancelled(),
        );
        if !shutdown.is_cancelled() && (cohort.failed_workers > 0 || no_fast_reads) {
            report.status = "failed".into();
            if no_fast_reads {
                report.errors.push("fast cohort completed no reads".into());
            }
        }
        report.interference = Some(InterferenceCohort {
            comparison_valid: invalid_reasons.is_empty(),
            invalid_reasons: invalid_reasons.clone(),
            expected_workers: if is_fast {
                config.fast_readers
            } else {
                competitors
            },
            failed_workers: cohort.failed_workers,
            active_worker_seconds: cohort.active.as_secs_f64(),
            object_pool_start: pool.start,
            object_pool_objects: pool.len(),
            object_pool_bytes: (pool.len() * config.object_size) as u64,
            requested_objects: cohort.objects.len(),
            range_bytes: if is_fast {
                config.range_size
            } else {
                config.object_size
            },
        });
        report.drain_ms = drain_ms;
        report.wall_elapsed_ms += drain_ms;
        report.stop_reason = if shutdown.is_cancelled() {
            "cancelled"
        } else {
            "observation_window"
        }
        .into();
        report.started_unix_ms = started_unix_ms;
        report.peak_inflight_logical_operations = if is_fast {
            fast_inflight.peak()
        } else {
            competitor_inflight.peak()
        };
        reports.push(report);
    }
    reports
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn pending_stream_open_is_interrupted_by_shutdown_or_window_end() {
        for cancel_run in [true, false] {
            let run = CancellationToken::new();
            let window = run.child_token();
            let cancel = if cancel_run {
                run.clone()
            } else {
                window.clone()
            };
            let (opened, opening) = tokio::sync::oneshot::channel();
            let response = async {
                opened.send(()).unwrap();
                std::future::pending::<Result<ObjectDataStream, ObjectError>>().await
            };
            let canceller = tokio::spawn(async move {
                opening.await.unwrap();
                tokio::time::sleep(Duration::from_millis(10)).await;
                cancel.cancel();
            });
            let request = Request {
                key: "object".into(),
                start: 0,
                length: 10,
                mode: ReadMode::Intermittent,
                reader_id: 0,
                // Cancellation must also release a worker that never reaches readiness.
                ready: Some((Arc::new(Barrier::new(2)), Arc::new(OnceLock::new()))),
            };
            let outcome = tokio::time::timeout(
                Duration::from_secs(1),
                read_response(response, &request, &BenchmarkConfig::default(), &window),
            )
            .await
            .expect("a pending stream open must not delay cancellation");
            canceller.await.unwrap();
            assert_eq!(run.is_cancelled(), cancel_run);
            assert!(outcome.interrupted);
            assert!(outcome.error.is_none());
            assert_eq!(outcome.sample.bytes, 0);
            let mut stats = Accumulator::default();
            stats.add(outcome, Duration::from_millis(10));
            assert_eq!(stats.interrupted, 1);
            assert_eq!(stats.failed, 0);
            assert!(stats.latency.summary().is_none());
            assert!(stats.failed_latency.summary().is_none());
        }
    }

    #[tokio::test]
    async fn stream_open_errors_remain_failures_without_cancellation() {
        let outcome = read_response(
            async { Err(ObjectError::internal("stream open failed")) },
            &Request {
                key: "object".into(),
                start: 0,
                length: 10,
                mode: ReadMode::Continuous,
                reader_id: 0,
                ready: None,
            },
            &BenchmarkConfig::default(),
            &CancellationToken::new(),
        )
        .await;
        assert!(!outcome.interrupted);
        assert!(outcome.error.is_some());
        let mut stats = Accumulator::default();
        stats.add(outcome, Duration::from_millis(10));
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.interrupted, 0);
        assert_eq!(stats.failed_latency.summary().unwrap().samples, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn readiness_excludes_the_whole_first_chunk() {
        let barrier = Arc::new(Barrier::new(2));
        let clock = Arc::new(OnceLock::new());
        let ready = barrier.clone();
        let start = clock.clone();
        let coordinator = tokio::spawn(async move {
            ready.wait().await;
            start.set(Instant::now()).unwrap();
        });
        let stream = futures::stream::iter([
            Ok(Bytes::from_static(b"0123")),
            Ok(Bytes::from_static(b"456789")),
        ])
        .boxed();
        let outcome = read_response(
            async { Ok(stream) },
            &Request {
                key: "object".into(),
                start: 0,
                length: 10,
                mode: ReadMode::Intermittent,
                reader_id: 0,
                ready: Some((barrier, clock)),
            },
            &BenchmarkConfig {
                pause_ms: 10,
                ..Default::default()
            },
            &CancellationToken::new(),
        )
        .await;
        coordinator.await.unwrap();
        assert!(outcome.error.is_none());
        assert_eq!(outcome.sample.bytes, 6);
        assert_eq!(outcome.sample.items, 1);
        assert!(outcome.sample.first_data.summary().is_none());
        assert!(outcome.sample.first_poll.summary().is_none());
        assert_eq!(outcome.sample.resume_poll.summary().unwrap().samples, 1);
        assert_eq!(outcome.sample.pause, Duration::from_millis(10));
    }

    #[tokio::test]
    async fn whole_chunk_reads_reject_truncated_and_oversized_bodies() {
        // Exercise early EOF, an oversized first/last chunk and extra data after
        // the requested length. No byte adapter may hide an oversized tail.
        for sizes in [vec![4], vec![11], vec![4, 7], vec![10, 1]] {
            let total: usize = sizes.iter().sum();
            let stream =
                futures::stream::iter(sizes.into_iter().map(|size| Ok(Bytes::from(vec![0; size]))))
                    .boxed();
            let outcome = read_response(
                async { Ok(stream) },
                &Request {
                    key: "object".into(),
                    start: 0,
                    length: 10,
                    mode: ReadMode::Continuous,
                    reader_id: 0,
                    ready: None,
                },
                &BenchmarkConfig::default(),
                &CancellationToken::new(),
            )
            .await;
            assert!(outcome.error.is_some());
            assert!(!outcome.interrupted);
            assert_eq!(outcome.sample.items, 0);
            assert_eq!(outcome.sample.bytes, total as u64);
        }
    }

    #[test]
    fn rotating_sequences_cover_disjoint_pools_and_reset_reproducibly() {
        let mut config = BenchmarkConfig {
            fast_readers: 2,
            concurrency: 2,
            ..Default::default()
        };
        for objects in [4, 5, 20, 21] {
            config.objects = objects;
            let sequence = |worker| {
                (0..objects * 2)
                    .map(|i| interference_request(&config, worker, i))
                    .collect::<Vec<_>>()
            };
            let mut fast = HashSet::new();
            let mut competitors = HashSet::new();
            let mut owners = HashMap::new();
            for worker in 0..4 {
                let requests = sequence(worker);
                // Each comparison starts at request zero, independently of timing/mode.
                assert_eq!(requests, sequence(worker));
                let own: HashSet<_> = requests.iter().map(|(object, _)| *object).collect();
                for object in &own {
                    assert!(owners.insert(*object, worker).is_none());
                }
                let pool = if worker < 2 {
                    &mut fast
                } else {
                    &mut competitors
                };
                pool.extend(own);
                let length = if worker < 2 {
                    config.range_size
                } else {
                    config.object_size
                };
                assert!(
                    requests
                        .iter()
                        .all(|(_, range)| range.len() == length && range.end <= config.object_size)
                );
                if worker < 2 {
                    assert!(requests.iter().any(|(_, range)| range.start > 0));
                    assert!(requests.windows(2).any(|pair| pair[0].1 != pair[1].1));
                }
            }
            assert!(fast.is_disjoint(&competitors));
            assert_eq!(fast.len(), objects / 2);
            assert_eq!(competitors.len(), objects - objects / 2);
            assert_eq!(owners.len(), objects);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn whole_chunks_are_consumed_before_pausing_and_polling_again() {
        let config = BenchmarkConfig {
            object_size: 10,
            pause_ms: 10,
            ..Default::default()
        };
        // The first four bytes must be consumed together. The next measured
        // await polls the delayed stream item, with no buffered tail to return.
        let stream = futures::stream::once(async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(Bytes::from_static(b"0123"))
        })
        .chain(futures::stream::once(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(Bytes::from_static(b"456789"))
        }))
        .boxed();
        let outcome = read_response(
            async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Ok(stream)
            },
            &Request {
                key: "object".into(),
                start: 0,
                length: 10,
                mode: ReadMode::Intermittent,
                reader_id: 0,
                ready: None,
            },
            &config,
            &CancellationToken::new(),
        )
        .await;
        assert!(outcome.error.is_none());
        assert_eq!(outcome.sample.bytes, 10);
        let first = outcome.sample.first_data.summary().unwrap();
        let resume_poll = outcome.sample.resume_poll.summary().unwrap();
        assert!((resume_poll.max_ms - 100.0).abs() < 0.1);
        let first_poll = outcome.sample.first_poll.summary().unwrap();
        assert_eq!(first_poll.samples, 1);
        assert!((first_poll.max_ms - 20.0).abs() < 0.1);
        assert!((first.max_ms - 50.0).abs() < 0.1);
        assert!(first.max_ms < resume_poll.max_ms);
        assert_eq!(resume_poll.samples, 1);
        assert_eq!(outcome.sample.max_stream_chunk, 6);
        let pause_ms = outcome.sample.pause.as_secs_f64() * 1000.0;
        assert_eq!(pause_ms, 10.0);

        let mut stats = Accumulator::default();
        stats.add(outcome, Duration::from_millis(130));
        let report = super::super::BenchmarkReport {
            schema_version: 5,
            profile_version: 5,
            prefix: "test/".into(),
            config,
            planned_dataset_bytes: 10,
            phases: vec![stats.report(
                "compactor_intermittent".into(),
                Duration::from_millis(130),
                None,
                false,
            )],
            cleanup_status: "completed".into(),
            cleanup_errors: vec![],
            cancelled: false,
            backpressure_verified: false,
            notes: vec![],
        };
        let summary = super::super::terminal_summary(&report);
        assert!(summary.contains("first_data: n=1; p95="));
        assert!(summary.contains("first_poll: n=1; p95="));
        assert!(summary.contains("resume_poll: n=1; p95="));
        assert!(summary.contains(&format!("max={:.2} ms", resume_poll.max_ms)));
        assert!(summary.contains(&format!("active pause across readers={pause_ms:.1} ms")));
    }
}
