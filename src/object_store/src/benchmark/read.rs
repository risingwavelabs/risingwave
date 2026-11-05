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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::ensure;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::util::tokio_util::sync::CancellationToken;
use tokio::io::AsyncReadExt;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio::time::Instant;

use super::{Accumulator, BenchmarkConfig, Outcome, PhaseReport, ReadMode, Sample};
use crate::object::{ObjectDataStreamReader, ObjectError, ObjectStoreImpl};

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
    length: usize,
    mode: ReadMode,
    reader_id: usize,
    ready: Option<(Arc<Barrier>, Arc<OnceLock<Instant>>)>,
}

#[try_stream(ok = Bytes, error = ObjectError)]
async fn short_gets(store: Arc<ObjectStoreImpl>, key: String, length: usize, chunk: usize) {
    for start in (0..length).step_by(chunk) {
        let end = start.saturating_add(chunk).min(length);
        let data = store.read(&key, start..end).await?;
        if data.len() != end - start {
            Err(ObjectError::internal(
                "short ranged GET returned unexpected length",
            ))?;
        }
        yield data;
    }
}

/// B is independent of SDK stream chunks. No background body-draining task is created.
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
    let mut sample = Sample::default();
    let chunk_size = Arc::new(AtomicUsize::new(0));
    let max_chunk = chunk_size.clone();
    let first_started = Instant::now();
    let mut measured_started = None;
    // Initialization may be lazy. First-data timing includes stream creation and the
    // first nonempty read, not just the return of streaming_read().
    let initialized = async {
        let stream = match request.mode {
            ReadMode::Chunked => {
                short_gets(store, request.key, request.length, config.short_get_size).boxed()
            }
            _ => store
                .streaming_read(&request.key, 0..request.length)
                .await?
                .into_stream(),
        };
        let stream = stream.inspect_ok(move |bytes| {
            max_chunk.fetch_max(bytes.len(), Ordering::Relaxed);
        });
        let mut reader = ObjectDataStreamReader::new(stream);
        let mut first = [0];
        tokio::select! {
            biased;
            _ = token.cancelled() => return Err(Interrupted.into()),
            result = reader.read_exact(&mut first) => { result?; }
        }
        Ok::<_, anyhow::Error>(reader)
    }
    .await;

    if initialized.is_ok() {
        sample.bytes = 1;
        sample.first_data.record(first_started.elapsed());
    }
    if let Some((barrier, start)) = request.ready {
        // Even an initialization error joins readiness, so peers cannot deadlock.
        tokio::select! {
            _ = token.cancelled() => {},
            _ = barrier.wait() => {},
        }
        while start.get().is_none() && !token.is_cancelled() {
            tokio::task::yield_now().await;
        }
        measured_started = start.get().copied();
        // The byte used for readiness was received before the observation window.
        sample.bytes = 0;
        sample.first_data = Default::default();
    }

    let result = async {
        let mut reader = initialized?;
        let mut remaining = request.length - 1;
        let mut buffer = vec![0; config.consume_size];
        let mut unit = 0;
        let mut first_unit = true;
        let mut resumed = false;
        while remaining > 0 {
            let size = if first_unit {
                (config.consume_size - 1).min(remaining)
            } else {
                config.consume_size.min(remaining)
            };
            first_unit = false;
            if size > 0 {
                let started = Instant::now();
                // Read a byte separately to distinguish resumption from time to
                // accumulate the complete consumption unit.
                tokio::select! {
                    biased;
                    _ = token.cancelled() => return Err(Interrupted.into()),
                    result = reader.read_exact(&mut buffer[..1]) => { result?; }
                }
                sample.bytes += 1;
                remaining -= 1;
                if resumed {
                    sample.resume.record(started.elapsed());
                }
                let mut filled = 1;
                // Preserve partial progress if a later read fails or is cancelled.
                while filled < size {
                    let n = tokio::select! {
                        biased;
                        _ = token.cancelled() => return Err(Interrupted.into()),
                        result = reader.read(&mut buffer[filled..size]) => result?,
                    };
                    ensure!(n > 0, "unexpected EOF with {remaining} bytes remaining");
                    sample.bytes += n as u64;
                    remaining -= n;
                    filled += n;
                }
                sample.consume.record(started.elapsed());
            }
            unit += 1;
            if remaining == 0 {
                break;
            }
            if request.mode != ReadMode::Continuous {
                let jitter = offset(
                    config.seed ^ request.reader_id as u64,
                    unit,
                    config.jitter_ms as usize,
                ) as u64;
                let long = if config.long_pause_every > 0 && unit % config.long_pause_every == 0 {
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
                    resumed = true;
                } else {
                    resumed = false;
                }
            }
        }
        // Known-length reads still check EOF: extra bytes invalidate the range.
        let mut byte = [0];
        let n = tokio::select! {
            biased;
            _ = token.cancelled() => return Err(Interrupted.into()),
            result = reader.read(&mut byte) => result?,
        };
        ensure!(n == 0, "GET returned more bytes than the requested range");
        sample.items = 1;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    sample.max_stream_chunk = chunk_size.load(Ordering::Relaxed);
    let interrupted = result.as_ref().is_err_and(|e| e.is::<Interrupted>());
    Outcome {
        sample,
        error: result.err().filter(|_| !interrupted),
        interrupted,
        measured_elapsed: measured_started.map(|start| start.elapsed()),
    }
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
        eprintln!(
            "benchmark: interference/{name} ({} fast + {competitors} competitors)",
            config.fast_readers
        );
        let end_window = shutdown.child_token();
        let barrier = Arc::new(Barrier::new(config.fast_readers + competitors + 1));
        let clock = Arc::new(OnceLock::new());
        let mut tasks = JoinSet::new();
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
            let key = keys[id % keys.len()].clone();
            let ready = barrier.clone();
            let clock = clock.clone();
            tasks.spawn(async move {
                let mut stats = Accumulator::default();
                if is_fast {
                    tokio::select! { _ = ready.wait() => {}, _ = token.cancelled() => {} }
                    while clock.get().is_none() && !token.is_cancelled() {
                        tokio::task::yield_now().await;
                    }
                }
                let mut initial = !is_fast;
                while !token.is_cancelled() {
                    let request = Request {
                        key: key.clone(),
                        length: if is_fast {
                            cfg.range_size
                        } else {
                            cfg.object_size
                        },
                        mode: if is_fast { ReadMode::Continuous } else { mode },
                        reader_id: id,
                        ready: initial.then(|| (ready.clone(), clock.clone())),
                    };
                    initial = false;
                    let started = Instant::now();
                    let guard = inflight.enter();
                    let outcome =
                        read_request(s.clone(), request, cfg.clone(), token.clone()).await;
                    drop(guard);
                    let failed = outcome.error.is_some();
                    stats.add(outcome, started.elapsed());
                    if failed {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                (is_fast, stats)
            });
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
        let mut fast = Accumulator::default();
        let mut slow = Accumulator::default();
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok((true, worker)) => fast.merge(worker),
                Ok((false, worker)) => slow.merge(worker),
                Err(error) => {
                    fast.failed += 1;
                    fast.errors.push(format!("worker failed: {error}"));
                }
            }
        }
        let drain_ms = started.elapsed().saturating_sub(elapsed).as_secs_f64() * 1000.0;
        let mut fast_report = fast.report(
            format!("interference_{name}/fast"),
            elapsed,
            None,
            shutdown.is_cancelled(),
        );
        fast_report.drain_ms = drain_ms;
        fast_report.started_unix_ms = started_unix_ms;
        fast_report.peak_inflight_logical_operations = fast_inflight.peak();
        reports.push(fast_report);
        if competitors > 0 {
            let mut slow_report = slow.report(
                format!("interference_{name}/competitors"),
                elapsed,
                None,
                shutdown.is_cancelled(),
            );
            slow_report.drain_ms = drain_ms;
            slow_report.started_unix_ms = started_unix_ms;
            slow_report.peak_inflight_logical_operations = competitor_inflight.peak();
            reports.push(slow_report);
        }
    }
    reports
}
