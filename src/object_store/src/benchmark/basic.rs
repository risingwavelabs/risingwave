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

use thiserror_ext::AsReport;

use super::*;

fn limits(config: &BenchmarkConfig, fixed_count: usize) -> (usize, Duration) {
    let duration = Duration::from_millis(config.basic_duration_ms);
    let count = if duration.is_zero() {
        fixed_count.min(config.basic_max_operations)
    } else {
        config.basic_max_operations
    };
    (count, duration)
}

fn finish(
    stats: Accumulator,
    name: &str,
    elapsed: Duration,
    count: usize,
    duration: Duration,
    token: &CancellationToken,
) -> PhaseReport {
    let attempts = stats.attempted;
    let mut report = stats.report(
        name.into(),
        elapsed,
        duration.is_zero().then_some(count),
        token.is_cancelled(),
    );
    if attempts == 0 && !token.is_cancelled() {
        report.status = "failed".into();
        report
            .errors
            .push("measurement ended without any operations".into());
    }
    report.budget = Some(MeasurementBudget {
        duration_ms: duration.as_millis() as u64,
        max_operations: count,
        duration_basis: "phase_wall_time".into(),
    });
    report.stop_reason = if token.is_cancelled() {
        "cancelled"
    } else if attempts >= count as u64 {
        "operation_limit"
    } else {
        "duration"
    }
    .into();
    report
}

async fn measure<F, Fut>(
    name: &str,
    fixed_count: usize,
    concurrency: usize,
    config: &BenchmarkConfig,
    token: &CancellationToken,
    execute: F,
) -> PhaseReport
where
    F: Fn(usize) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Outcome> + Send + 'static,
{
    let (count, duration) = limits(config, fixed_count);
    eprintln!(
        "benchmark: {name} ({} ms or {count} operations, {concurrency} workers)",
        duration.as_millis()
    );
    let started_unix_ms = unix_ms();
    let deadline = (!duration.is_zero()).then(|| Instant::now() + duration);
    let (stats, elapsed, peak) = execute_jobs(count, concurrency, token, deadline, execute).await;
    let mut report = finish(stats, name, elapsed, count, duration, token);
    report.started_unix_ms = started_unix_ms;
    report.peak_inflight_logical_operations = peak;
    report
}

/// Each worker owns a disjoint subset, including when object count is not a
/// multiple of concurrency. Repeated uploads cannot overwrite another worker.
fn upload_index(index: usize, objects: usize, workers: usize) -> usize {
    let worker = index % workers;
    let owned = (objects - 1 - worker) / workers + 1;
    worker + (index / workers % owned) * workers
}

pub(super) async fn uploads(
    store: Arc<ObjectStoreImpl>,
    keys: Arc<Vec<String>>,
    payload: Bytes,
    config: Arc<BenchmarkConfig>,
    token: &CancellationToken,
) -> PhaseReport {
    let workers = config.concurrency.min(keys.len());
    measure(
        "multipart_put",
        keys.len(),
        workers,
        &config,
        token,
        move |i| {
            let store = store.clone();
            let key = keys[upload_index(i, keys.len(), workers)].clone();
            let payload = payload.clone();
            async move {
                Outcome::from_result(
                    async {
                        let mut writer = store.streaming_upload(&key).await?;
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
    .await
}

pub(super) async fn reads(
    store: Arc<ObjectStoreImpl>,
    keys: Arc<Vec<String>>,
    config: Arc<BenchmarkConfig>,
    token: &CancellationToken,
) -> Vec<PhaseReport> {
    let mut phases = vec![];
    for (name, size) in [
        ("range_get", config.range_size),
        ("block_get", config.block_size),
    ] {
        let store = store.clone();
        let keys = keys.clone();
        let cfg = config.clone();
        phases.push(
            measure(
                name,
                config.objects * config.read_iterations,
                config.concurrency,
                &config,
                token,
                move |i| {
                    let store = store.clone();
                    let key = keys[i % keys.len()].clone();
                    let cfg = cfg.clone();
                    async move {
                        Outcome::from_result(
                            async {
                                let offset = read::offset(cfg.seed, i, cfg.object_size - size);
                                let data = store.read(&key, offset..offset + size).await?;
                                ensure!(
                                    data.len() == size,
                                    "unexpected GET length: {} != {size}",
                                    data.len()
                                );
                                Ok(Sample {
                                    bytes: data.len() as u64,
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
    }
    phases
}

pub(super) async fn metadata(
    store: Arc<ObjectStoreImpl>,
    keys: Arc<Vec<String>>,
    config: Arc<BenchmarkConfig>,
    token: &CancellationToken,
) -> Vec<PhaseReport> {
    let s = store.clone();
    let prefix = keys[0].rsplit_once('/').unwrap().0.to_owned() + "/";
    let expected = keys.len();
    let list = measure(
        "list",
        config.concurrency,
        config.concurrency,
        &config,
        token,
        move |_| {
            let s = s.clone();
            let prefix = prefix.clone();
            async move {
                Outcome::from_result(
                    async {
                        let mut stream = s.list(&prefix, None, None).await?;
                        let mut count = 0;
                        while stream.try_next().await?.is_some() {
                            count += 1;
                        }
                        ensure!(
                            count == expected,
                            "LIST returned {count} objects, expected {expected}"
                        );
                        Ok(Sample {
                            items: count as u64,
                            ..Default::default()
                        })
                    }
                    .await,
                )
            }
        },
    )
    .await;
    vec![list, deletes(store, keys, config, token).await]
}

async fn deletes(
    store: Arc<ObjectStoreImpl>,
    keys: Arc<Vec<String>>,
    config: Arc<BenchmarkConfig>,
    token: &CancellationToken,
) -> PhaseReport {
    let batch = config.delete_batch_size;
    let batches = keys.len().div_ceil(batch);
    let (limit, duration) = limits(&config, batches);
    eprintln!(
        "benchmark: batch_delete ({} wall ms or {limit} batches; replenishment excluded from rates)",
        duration.as_millis()
    );
    let started_unix_ms = unix_ms();
    let wall_started = Instant::now();
    let mut elapsed = Duration::ZERO;
    let mut setup_elapsed = Duration::ZERO;
    let mut setup_operations = 0;
    let mut setup_errors = vec![];
    let mut peak = 0;
    let mut stats = Accumulator::default();
    let mut previous_count = 0;
    while stats.attempted < limit as u64
        && !token.is_cancelled()
        && (duration.is_zero() || wall_started.elapsed() < duration)
    {
        // All deletes from the preceding round have finished. Replenish outside
        // measurement, so every measured DELETE targets existing objects and PUTs
        // do not compete with the DELETE cohort.
        if previous_count > 0 {
            let s = store.clone();
            let keys = keys.clone();
            let (setup, cost, _) = execute_jobs(
                previous_count,
                config.concurrency,
                token,
                (!duration.is_zero()).then_some(wall_started + duration),
                move |i| {
                    let s = s.clone();
                    let key = keys[i].clone();
                    async move {
                        Outcome::from_result(
                            async {
                                s.upload(&key, Bytes::from(vec![i as u8; 4096])).await?;
                                Ok(Sample::default())
                            }
                            .await,
                        )
                    }
                },
            )
            .await;
            setup_elapsed += cost;
            setup_operations += setup.attempted;
            if setup.failed > 0 {
                setup_errors = setup.errors;
                break;
            }
            if setup.attempted != previous_count as u64
                || (!duration.is_zero() && wall_started.elapsed() >= duration)
            {
                // The wall-time budget or shutdown can stop replenishment too.
                // Partially replenished keys remain known to final cleanup.
                break;
            }
        }
        let count = batches.min(limit - stats.attempted as usize);
        let deadline = (!duration.is_zero()).then_some(wall_started + duration);
        let s = store.clone();
        let deleted = keys.clone();
        let (round, cost, round_peak) =
            execute_jobs(count, config.concurrency, token, deadline, move |i| {
                let s = s.clone();
                let keys = deleted[i * batch..((i + 1) * batch).min(deleted.len())].to_vec();
                async move {
                    Outcome::from_result(
                        async {
                            s.delete_objects(&keys).await?;
                            Ok(Sample {
                                items: keys.len() as u64,
                                ..Default::default()
                            })
                        }
                        .await,
                    )
                }
            })
            .await;
        let failed = round.failed > 0;
        let attempts = round.attempted;
        elapsed += cost;
        peak = peak.max(round_peak);
        stats.merge(round);
        // A partial round is only possible at a deadline/cancellation, so no next
        // round can accidentally issue DELETE on a missing key.
        if failed || attempts < count as u64 || duration.is_zero() {
            break;
        }
        previous_count = (count * batch).min(keys.len());
    }
    let mut report = finish(stats, "batch_delete", elapsed, limit, duration, token);
    report.started_unix_ms = started_unix_ms;
    report.wall_elapsed_ms = wall_started.elapsed().as_secs_f64() * 1000.0;
    report.setup_elapsed_ms = setup_elapsed.as_secs_f64() * 1000.0;
    report.setup_operations = setup_operations;
    report.peak_inflight_logical_operations = peak;
    if !setup_errors.is_empty() {
        report.status = if token.is_cancelled() {
            "cancelled"
        } else {
            "failed"
        }
        .into();
        report.stop_reason = "setup_failed".into();
        report.errors.extend(
            setup_errors
                .into_iter()
                .map(|e| format!("DELETE replenishment: {e}")),
        );
    } else if report.failed > 0 {
        report.stop_reason = "io_failure".into();
    }
    report
}

/// A tiny fresh-prefix compatibility check before allocating/uploading large data.
/// Retained-data runs bypass this write check and validate their existing objects.
pub(super) async fn preflight(
    store: Arc<ObjectStoreImpl>,
    prefix: &str,
    token: &CancellationToken,
) -> PhaseReport {
    let key = format!("{prefix}check/probe");
    let prefix = format!("{prefix}check/");
    jobs("preflight", 1, 1, token, move |_| {
        let store = store.clone();
        let key = key.clone();
        let prefix = prefix.clone();
        async move {
            let result = async {
                let data = Bytes::from((0..4096).map(|i| (i % 251) as u8).collect::<Vec<_>>());
                store
                    .upload(&key, data.clone())
                    .await
                    .context("preflight PUT")?;
                ensure!(
                    store
                        .metadata(&key)
                        .await
                        .context("preflight HEAD")?
                        .total_size
                        == data.len(),
                    "preflight HEAD returned an incorrect size"
                );
                ensure!(
                    store
                        .read(&key, 0..data.len())
                        .await
                        .context("preflight GET")?
                        == data,
                    "preflight GET returned incorrect content"
                );
                for range in [0..1, 17..83, 4095..4096] {
                    ensure!(
                        store
                            .read(&key, range.clone())
                            .await
                            .context("preflight Range GET")?
                            == data.slice(range),
                        "preflight Range GET returned incorrect content"
                    );
                }
                let listed = store
                    .list(&prefix, None, None)
                    .await
                    .context("preflight LIST")?
                    .try_collect::<Vec<_>>()
                    .await?;
                ensure!(
                    listed.len() == 1 && listed[0].key == key,
                    "preflight LIST returned incorrect keys"
                );
                store
                    .delete_objects(std::slice::from_ref(&key))
                    .await
                    .context("preflight DELETE")?;
                ensure!(
                    store
                        .list(&prefix, None, None)
                        .await?
                        .try_next()
                        .await?
                        .is_none(),
                    "preflight object is still listed after DELETE"
                );
                Ok(Sample {
                    items: 1,
                    ..Default::default()
                })
            }
            .await;
            // Retry cleanup even when a functional assertion failed. The probe is
            // never intentionally retained, including with --keep-data.
            let cleanup = store
                .delete_objects(&[key])
                .await
                .context("preflight probe cleanup");
            Outcome::from_result(match (result, cleanup) {
                (Ok(sample), Ok(())) => Ok(sample),
                (Err(error), Ok(())) => Err(error),
                (Ok(_), Err(error)) => Err(error),
                (Err(error), Err(cleanup)) => {
                    Err(error.context(format!("{}", cleanup.as_report())))
                }
            })
        }
    })
    .await
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use super::upload_index;

    #[test]
    fn uploads_have_disjoint_worker_keys() {
        for objects in 1..18 {
            for workers in 1..=objects {
                let mut owners = vec![None; objects];
                for index in 0..objects * workers * 3 {
                    let key = upload_index(index, objects, workers);
                    let worker = index % workers;
                    assert!(owners[key].is_none_or(|owner| owner == worker));
                    owners[key] = Some(worker);
                }
                assert!(owners.iter().all(Option::is_some));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn duration_stops_scheduling_but_finishes_inflight_work() {
        use super::*;
        let cfg = BenchmarkConfig {
            basic_duration_ms: 10,
            basic_max_operations: 100,
            ..Default::default()
        };
        let phase = measure(
            "multipart_put",
            1,
            2,
            &cfg,
            &CancellationToken::new(),
            |_| async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Outcome::from_result(Ok(Sample {
                    items: 1,
                    ..Default::default()
                }))
            },
        )
        .await;
        assert_eq!(phase.status, "completed");
        assert_eq!(phase.succeeded, 2);
        assert!(phase.elapsed_ms >= 30.0);
        assert_eq!(phase.stop_reason, "duration");
        assert_eq!(phase.interrupted, 0);
        assert_eq!(phase.peak_inflight_logical_operations, 2);
    }
}
