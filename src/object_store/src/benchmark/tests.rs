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

use super::*;
use crate::object::{InMemObjectStore, ObjectStore, ObjectStoreConfig, build_remote_object_store};

fn config() -> BenchmarkConfig {
    BenchmarkConfig {
        objects: 3,
        object_size: 10,
        concurrency: 1,
        range_size: 5,
        block_size: 3,
        consume_size: 3,
        pause_ms: 1,
        short_get_size: 4,
        basic_duration_ms: 0,
        metadata_objects: 5,
        delete_batch_size: 2,
        fast_readers: 1,
        interference_ms: 10,
        ..Default::default()
    }
}

fn memory() -> Arc<ObjectStoreImpl> {
    Arc::new(ObjectStoreImpl::InMem(
        InMemObjectStore::for_test().monitored(
            Arc::new(ObjectStoreMetrics::unused()),
            Arc::new(ObjectStoreConfig::default()),
        ),
    ))
}

#[tokio::test]
async fn consumption_units_and_short_get_tail() {
    let store = memory();
    store
        .upload("object", Bytes::from_static(b"0123456789"))
        .await
        .unwrap();
    for mode in [
        ReadMode::Continuous,
        ReadMode::Intermittent,
        ReadMode::Chunked,
    ] {
        let outcome = read::read_object(
            store.clone(),
            "object".into(),
            Arc::new(config()),
            mode,
            0,
            CancellationToken::new(),
        )
        .await;
        assert!(outcome.error.is_none());
        assert_eq!(outcome.sample.bytes, 10);
        assert_eq!(outcome.sample.items, 1);
        assert_eq!(outcome.sample.consume.0.len(), 4);
        assert_eq!(outcome.sample.first_data.0.len(), 1);
        if mode == ReadMode::Continuous {
            assert_eq!(outcome.sample.pause, Duration::ZERO);
            assert_eq!(outcome.sample.resume.0.len(), 0);
        } else {
            // Three pauses, and no fourth sleep after the final partial unit.
            assert!(outcome.sample.pause >= Duration::from_millis(3));
            assert_eq!(outcome.sample.resume.0.len(), 3);
        }
    }
}

#[tokio::test]
async fn one_byte_units_and_cancellation_preserve_progress() {
    let store = memory();
    store
        .upload("object", Bytes::from_static(b"0123456789"))
        .await
        .unwrap();
    let mut cfg = config();
    cfg.consume_size = 1;
    cfg.pause_ms = 60_000;
    let token = CancellationToken::new();
    let cancel = token.clone();
    let task = tokio::spawn(read::read_object(
        store,
        "object".into(),
        Arc::new(cfg),
        ReadMode::Intermittent,
        0,
        token,
    ));
    tokio::time::sleep(Duration::from_millis(10)).await;
    cancel.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    assert!(outcome.interrupted);
    assert!(outcome.error.is_none());
    assert_eq!(outcome.sample.bytes, 1);
    let mut stats = Accumulator::default();
    stats.add(outcome, Duration::from_millis(10));
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.interrupted, 1);
    assert_eq!(stats.latency.0.len(), 0); // No fabricated completed-request latency.
}

#[tokio::test]
async fn all_profiles_clean_only_owned_objects() {
    let store = memory();
    store
        .upload("production/object", Bytes::from_static(b"untouched"))
        .await
        .unwrap();
    let report = run(
        store.clone(),
        config(),
        "risectl-bench/test/".into(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(report.succeeded(), "{report:?}");
    for name in [
        "multipart_put",
        "range_get",
        "block_get",
        "list",
        "batch_delete",
        "compactor_continuous",
        "compactor_intermittent",
        "compactor_chunked",
        "interference_fast_only/fast",
        "interference_continuous_competitors/fast",
        "interference_slow_competitors/fast",
    ] {
        let phase = report.phases.iter().find(|p| p.name == name).unwrap();
        assert!(phase.succeeded > 0, "{phase:?}");
        if let Some(cohort) = &phase.interference {
            assert!(cohort.comparison_valid, "{cohort:?}");
            assert_eq!(cohort.failed_workers, 0);
            assert!(cohort.active_worker_seconds > 0.0);
            assert!(
                cohort.active_worker_seconds
                    <= cohort.expected_workers as f64 * phase.elapsed_ms / 1000.0 + 1e-9
            );
        }
    }
    for name in [
        "compactor_continuous",
        "compactor_intermittent",
        "compactor_chunked",
    ] {
        assert_eq!(
            report.phases.iter().find(|p| p.name == name).unwrap().bytes,
            30
        );
    }
    let remaining: Vec<_> = store
        .list("", None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].key, "production/object");
    assert!(!report.backpressure_verified);
}

#[tokio::test]
async fn interference_initialization_errors_do_not_deadlock() {
    let store = memory();
    let cfg = Arc::new(config());
    let keys = Arc::new(vec![
        "missing1".into(),
        "missing2".into(),
        "missing3".into(),
    ]);
    let phases = tokio::time::timeout(
        Duration::from_secs(2),
        read::interference(store, keys, cfg, &CancellationToken::new()),
    )
    .await
    .unwrap();
    assert!(phases.iter().all(|p| p.status == "failed"));
}

#[tokio::test]
async fn competitor_failures_invalidate_the_paired_fast_comparison() {
    let store = memory();
    // The fast pool contains object zero. Both competitor objects are missing.
    store
        .upload("fast", Bytes::from_static(b"0123456789"))
        .await
        .unwrap();
    let mut cfg = config();
    cfg.interference_ms = 30;
    let phases = read::interference(
        store,
        Arc::new(vec!["fast".into(), "missing1".into(), "missing2".into()]),
        Arc::new(cfg),
        &CancellationToken::new(),
    )
    .await;
    let baseline = &phases[0];
    assert_eq!(baseline.status, "completed");
    assert!(baseline.interference.as_ref().unwrap().comparison_valid);
    for pair in phases[1..].chunks_exact(2) {
        let fast = &pair[0];
        let competitor = &pair[1];
        assert_eq!(fast.status, "completed"); // Preserve the actual fast I/O outcome.
        assert!(fast.succeeded > 0);
        assert_eq!(competitor.status, "failed");
        for phase in pair {
            let context = phase.interference.as_ref().unwrap();
            assert!(!context.comparison_valid);
            assert!(
                context
                    .invalid_reasons
                    .iter()
                    .any(|reason| reason.contains("competitors: 1 workers failed"))
            );
        }
        let fast_context = fast.interference.as_ref().unwrap();
        let competitor_context = competitor.interference.as_ref().unwrap();
        assert_eq!(fast_context.failed_workers, 0);
        assert_eq!(competitor_context.failed_workers, 1);
        assert_eq!(fast_context.object_pool_objects, 1);
        assert_eq!(competitor_context.object_pool_objects, 2);
        assert_eq!(fast_context.object_pool_bytes, 10);
        assert_eq!(competitor_context.object_pool_bytes, 20);
        assert_eq!(fast_context.requested_objects, 1);
        assert_eq!(competitor_context.requested_objects, 1);
        assert!(competitor_context.active_worker_seconds < fast_context.active_worker_seconds);
    }
    let report = BenchmarkReport {
        schema_version: 3,
        profile_version: 3,
        prefix: "test/".into(),
        config: config(),
        planned_dataset_bytes: 30,
        // Even a consumer looking only at successful fast phases must see invalidity.
        phases: phases
            .into_iter()
            .filter(|p| p.name.ends_with("/fast"))
            .collect(),
        cleanup_status: "completed".into(),
        cleanup_errors: vec![],
        cancelled: false,
        backpressure_verified: false,
        notes: vec![],
    };
    assert!(!report.succeeded());
    let summary = terminal_summary(&report);
    assert!(summary.contains("comparison=invalid"));
    assert!(summary.contains("invalid comparison: competitors: 1 workers failed"));
}

#[tokio::test]
async fn kept_data_and_pre_cancelled_run() {
    let store = memory();
    let mut cfg = config();
    cfg.profile = Profile::Compactor;
    cfg.keep_data = true;
    let report = run(
        store.clone(),
        cfg,
        "risectl-bench/kept/".into(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(report.succeeded());
    assert_eq!(
        store
            .list("risectl-bench/kept/", None, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len(),
        3
    );
    let mut reuse = config();
    reuse.profile = Profile::Compactor;
    reuse.keep_data = true;
    reuse.reuse_data = true;
    reuse.read_order.reverse();
    let reused = run(
        store.clone(),
        reuse,
        "risectl-bench/kept/".into(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(reused.succeeded());
    assert_eq!(reused.phases[0].name, "validate_retained_data");
    assert_eq!(reused.phases[1].name, "compactor_chunked");
    let token = CancellationToken::new();
    token.cancel();
    let report = run(
        store.clone(),
        config(),
        "risectl-bench/cancelled/".into(),
        token,
    )
    .await
    .unwrap();
    assert!(!report.succeeded());
    assert_eq!(report.cleanup_status, "completed");
    assert!(
        store
            .list("risectl-bench/cancelled/", None, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
}

#[test]
fn rejects_invalid_experiments_and_merges_percentiles() {
    let mut cfg = config();
    cfg.read_order[2] = ReadMode::Continuous;
    assert!(cfg.validate().is_err());
    cfg = config();
    cfg.objects = 1;
    assert!(cfg.validate().is_err());
    cfg = config();
    cfg.consume_size = 0;
    assert!(cfg.validate().is_err());
    let mut a = Timings::default();
    let mut b = Timings::default();
    for _ in 0..99 {
        a.record(Duration::from_millis(1));
    }
    b.record(Duration::from_millis(100));
    a.merge(&b);
    let summary = a.summary().unwrap();
    assert_eq!(summary.samples, 100);
    assert!(summary.p99_ms < 2.0); // Pooled samples, not the worst per-worker p99.
    assert!(summary.max_ms >= 100.0);
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct HttpGet {
    key: String,
    range: std::ops::Range<usize>,
}

type RequestLog = Arc<std::sync::Mutex<Vec<HttpGet>>>;

/// Minimal S3 protocol fixture: records actual keys and Range headers and can truncate
/// a body while keeping the advertised length, unlike an in-memory store.
async fn protocol_store(
    truncate: bool,
) -> (
    Arc<ObjectStoreImpl>,
    RequestLog,
    tokio::task::JoinHandle<()>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let ranges = Arc::new(std::sync::Mutex::new(vec![]));
    let recorded = ranges.clone();
    let task = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let ranges = recorded.clone();
            tokio::spawn(async move {
                let mut header = vec![];
                loop {
                    let mut byte = [0];
                    if socket.read_exact(&mut byte).await.is_err() {
                        return;
                    }
                    header.push(byte[0]);
                    if header.ends_with(b"\r\n\r\n") {
                        break;
                    }
                    if header.len() > 32_768 {
                        return;
                    }
                }
                let header = String::from_utf8(header).unwrap();
                if header.starts_with("HEAD ") {
                    socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\n",
                        )
                        .await
                        .unwrap();
                    return;
                }
                let range = header
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("range")
                            .then(|| value.trim().to_owned())
                    })
                    .expect("range header");
                let (start, end) = range
                    .strip_prefix("bytes=")
                    .unwrap()
                    .split_once('-')
                    .unwrap();
                let start: usize = start.parse().unwrap();
                let end: usize = end.parse().unwrap();
                let key = header
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .rsplit('/')
                    .next()
                    .unwrap()
                    .to_owned();
                ranges.lock().unwrap().push(HttpGet {
                    key,
                    range: start..end + 1,
                });
                let response = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {start}-{end}/10\r\nConnection: close\r\n\r\n",
                    end - start + 1
                );
                if socket.write_all(response.as_bytes()).await.is_err() {
                    return;
                }
                let data = b"0123456789";
                let body = if truncate {
                    &data[start..(start + 5).min(end + 1)]
                } else {
                    &data[start..=end]
                };
                // Observation-window cancellation may close a request after its header.
                let _ = socket.write_all(body).await;
            });
        }
    });
    let mut cfg = ObjectStoreConfig::default();
    cfg.retry.streaming_read_retry_attempts = 0;
    cfg.retry.read_retry_attempts = 0;
    let cfg = Arc::new(cfg);
    let metrics = Arc::new(ObjectStoreMetrics::unused());
    let store = crate::object::OpendalObjectStore::new_minio_engine(
        &format!("minio://test-key:test-secret@http://{endpoint}/bucket"),
        cfg.clone(),
        metrics.clone(),
    )
    .unwrap()
    .monitored(metrics, cfg);
    (Arc::new(ObjectStoreImpl::Opendal(store)), ranges, task)
}

#[tokio::test]
async fn actual_http_ranges_stay_large_during_pauses_and_split_only_in_chunked_mode() {
    let (store, ranges, server) = protocol_store(false).await;
    for mode in [
        ReadMode::Continuous,
        ReadMode::Intermittent,
        ReadMode::Chunked,
    ] {
        ranges.lock().unwrap().clear();
        let outcome = read::read_object(
            store.clone(),
            "object".into(),
            Arc::new(config()),
            mode,
            0,
            CancellationToken::new(),
        )
        .await;
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        let expected = if mode == ReadMode::Chunked {
            vec![0..4, 4..8, 8..10]
        } else {
            std::iter::once(0..10).collect()
        };
        assert_eq!(
            ranges
                .lock()
                .unwrap()
                .iter()
                .map(|r| r.range.clone())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(outcome.sample.bytes, 10);
    }
    server.abort();
}

async fn capture_comparison_requests(
    objects: usize,
    competitors: usize,
    mode: ReadMode,
) -> Vec<Vec<HttpGet>> {
    let (store, ranges, server) = protocol_store(false).await;
    let cfg = Arc::new(BenchmarkConfig {
        objects,
        fast_readers: 2,
        concurrency: 2,
        interference_ms: 60_000,
        ..config()
    });
    let shutdown = CancellationToken::new();
    let cancel = shutdown.clone();
    let task = tokio::spawn(async move {
        read::interference_comparison(
            store,
            Arc::new((0..objects).map(|i| i.to_string()).collect()),
            cfg,
            &cancel,
            "wire_sequence",
            competitors,
            mode,
        )
        .await
    });
    // Stop by observed request count, not by an assumption about CI scheduling speed.
    let observed = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let mut workers = vec![vec![]; 2 + competitors];
            for request in ranges.lock().unwrap().iter() {
                let object: usize = request.key.parse().unwrap();
                assert!(object < objects);
                let worker = if object < objects / 2 {
                    object % 2
                } else {
                    2 + (object - objects / 2) % 2
                };
                workers[worker].push(request.clone());
            }
            if workers.iter().all(|requests| requests.len() >= 8) {
                for requests in &mut workers {
                    requests.truncate(8);
                }
                return workers;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    shutdown.cancel();
    task.await.unwrap();
    server.abort();
    observed.expect("each worker must issue the required HTTP requests")
}

#[tokio::test]
async fn actual_interference_sequences_reset_and_expand_with_the_dataset() {
    let mut working_sets = vec![];
    for objects in [4, 12] {
        let baseline = capture_comparison_requests(objects, 0, ReadMode::Continuous).await;
        let continuous = capture_comparison_requests(objects, 2, ReadMode::Continuous).await;
        let slow = capture_comparison_requests(objects, 2, ReadMode::Intermittent).await;
        assert_eq!(baseline, continuous[..2]);
        assert_eq!(baseline, slow[..2]);
        assert_eq!(continuous[2..], slow[2..]);
        let keys = |workers: &[Vec<HttpGet>]| {
            workers
                .iter()
                .flatten()
                .map(|r| r.key.clone())
                .collect::<std::collections::HashSet<_>>()
        };
        let fast_keys = keys(&baseline);
        let competitor_keys = keys(&slow[2..]);
        assert!(fast_keys.is_disjoint(&competitor_keys));
        assert_eq!(fast_keys.len(), objects / 2);
        assert_eq!(competitor_keys.len(), objects / 2);
        assert!(baseline.iter().flatten().all(|r| r.range.len() == 5));
        assert!(baseline.iter().flatten().any(|r| r.range.start != 0));
        assert!(slow[2..].iter().flatten().all(|r| r.range == (0..10)));
        working_sets.push(fast_keys.len() + competitor_keys.len());
    }
    assert!(working_sets[1] > working_sets[0]);
}

#[tokio::test]
async fn truncated_http_body_is_a_failure_with_partial_progress() {
    let (store, ranges, server) = protocol_store(true).await;
    let outcome = read::read_object(
        store,
        "object".into(),
        Arc::new(config()),
        ReadMode::Continuous,
        0,
        CancellationToken::new(),
    )
    .await;
    assert!(outcome.error.is_some());
    assert!(!outcome.interrupted);
    assert_eq!(outcome.sample.bytes, 5);
    assert_eq!(ranges.lock().unwrap().len(), 1);
    server.abort();
}

/// Run explicitly against disposable local MinIO, never a customer's bucket.
#[tokio::test]
#[ignore = "requires RW_BENCH_TEST_STORE and disposable S3-compatible storage"]
async fn real_s3_profiles() {
    let url = std::env::var("RW_BENCH_TEST_STORE").expect("RW_BENCH_TEST_STORE");
    let mut object_config = ObjectStoreConfig {
        upload_part_size: 5 << 20,
        opendal_writer_abort_on_err: true,
        ..Default::default()
    };
    object_config.s3.developer.use_opendal =
        std::env::var("RW_BENCH_TEST_BACKEND").as_deref() != Ok("aws");
    let metrics = Arc::new(ObjectStoreMetrics::unused());
    let store = Arc::new(
        build_remote_object_store(&url, metrics, "benchmark-test", Arc::new(object_config)).await,
    );
    let cfg = BenchmarkConfig {
        objects: 3,
        object_size: 12 << 20,
        concurrency: 2,
        fast_readers: 1,
        consume_size: 1 << 20,
        short_get_size: 4 << 20,
        pause_ms: 1,
        basic_duration_ms: 25,
        basic_max_operations: 100,
        metadata_objects: 1013,
        delete_batch_size: 5,
        interference_ms: 200,
        ..Default::default()
    };
    let prefix = format!(
        "risectl-bench/integration-{}-{}/",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let report = run(store.clone(), cfg, prefix.clone(), CancellationToken::new())
        .await
        .unwrap();
    if let Ok(path) = std::env::var("RW_BENCH_TEST_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
    assert!(report.succeeded(), "{report:#?}");
    assert!(
        store
            .list(&prefix, None, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    for name in [
        "compactor_continuous",
        "compactor_intermittent",
        "compactor_chunked",
    ] {
        assert_eq!(
            report.phases.iter().find(|p| p.name == name).unwrap().bytes,
            36 << 20
        );
    }
}

#[tokio::test]
async fn fixed_work_reports_natural_completion_and_truncated_uploads_and_deletes() {
    for (cap, reason) in [(100, "fixed_work"), (2, "operation_limit")] {
        let mut cfg = config();
        cfg.profile = Profile::Basic;
        cfg.basic_max_operations = cap;
        let report = run(
            memory(),
            cfg,
            format!("risectl-bench/stop-reason-{cap}/"),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(report.succeeded(), "{report:?}");
        for name in ["multipart_put", "batch_delete"] {
            let phase = report.phases.iter().find(|p| p.name == name).unwrap();
            assert_eq!(phase.stop_reason, reason, "{phase:?}");
            assert_eq!(phase.succeeded, if cap == 100 { 3 } else { 2 });
        }
    }
}

#[tokio::test]
async fn basic_caps_measurement_reuses_keys_and_replenishes_deletes() {
    let store = memory();
    let mut cfg = config();
    cfg.profile = Profile::Basic;
    cfg.basic_duration_ms = 10_000;
    cfg.basic_max_operations = 7;
    cfg.keep_data = true;
    let report = run(
        store.clone(),
        cfg,
        "risectl-bench/budget/".into(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    assert!(report.succeeded(), "{report:#?}");
    for name in [
        "multipart_put",
        "range_get",
        "block_get",
        "list",
        "batch_delete",
    ] {
        let phase = report.phases.iter().find(|p| p.name == name).unwrap();
        assert_eq!(phase.attempted, 7);
        assert_eq!(phase.latency.as_ref().unwrap().samples, 7);
        assert_eq!(phase.stop_reason, "operation_limit");
    }
    let deleted = report
        .phases
        .iter()
        .find(|p| p.name == "batch_delete")
        .unwrap();
    assert_eq!(deleted.operation_unit, "delete_batch");
    assert_eq!(deleted.setup_operations, 10); // Two complete five-object replenishments.
    assert_eq!(deleted.items, 12); // Two five-object rounds and one two-object batch.
    assert!(deleted.wall_elapsed_ms >= deleted.elapsed_ms + deleted.setup_elapsed_ms);
    let remaining = store
        .list("risectl-bench/budget/", None, None)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        remaining
            .iter()
            .filter(|o| o.key.contains("/data/"))
            .count(),
        3
    );
    assert!(!remaining.iter().any(|o| o.key.contains("/check/")));
    let summary = terminal_summary(&report);
    assert!(summary.contains("delete_batch"));
    assert!(summary.contains("complete_object_upload"));
    assert!(summary.contains("Samples"));
    assert!(summary.contains("excluded setup"));
}

#[test]
fn successful_failed_and_interrupted_latency_have_separate_denominators() {
    let mut stats = Accumulator::default();
    stats.add(
        Outcome::from_result(Ok(Sample::default())),
        Duration::from_millis(1),
    );
    stats.add(
        Outcome::from_result(Err(anyhow::anyhow!("I/O failure"))),
        Duration::from_millis(100),
    );
    stats.add(
        Outcome {
            sample: Sample::default(),
            error: None,
            interrupted: true,
            measured_elapsed: None,
        },
        Duration::from_secs(1),
    );
    let report = stats.report("list".into(), Duration::from_secs(1), None, false);
    assert_eq!(report.latency.as_ref().unwrap().samples, 1);
    assert_eq!(report.failed_latency.as_ref().unwrap().samples, 1);
    assert_eq!(report.final_error_rate, Some(0.5));
    assert_eq!(report.interrupted, 1);
    assert_eq!(report.operation_unit, "complete_prefix_scan");
}

async fn preflight_fault_report(cleanup_failure: bool) -> BenchmarkReport {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let puts = Arc::new(AtomicUsize::new(0));
    let count = puts.clone();
    let server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let puts = count.clone();
            tokio::spawn(async move {
                let mut header = vec![];
                while !header.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    if socket.read_exact(&mut byte).await.is_err() {
                        return;
                    }
                    header.push(byte[0]);
                    if header.len() > 32_768 {
                        return;
                    }
                }
                let header = String::from_utf8(header).unwrap();
                let field = |key: &str| {
                    header.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case(key)
                            .then(|| value.trim().to_owned())
                    })
                };
                let length = field("content-length")
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(0);
                let mut body = vec![0; length];
                if socket.read_exact(&mut body).await.is_err() {
                    return;
                }
                let (status, length, response) = if header.starts_with("PUT ") {
                    puts.fetch_add(1, Ordering::SeqCst);
                    ("200 OK", 0, vec![])
                } else if header.starts_with("HEAD ") {
                    ("200 OK", 4096, vec![])
                } else if header.starts_with("GET ") {
                    let range = field("range").unwrap();
                    let (start, end) = range
                        .strip_prefix("bytes=")
                        .unwrap()
                        .split_once('-')
                        .unwrap();
                    let start: usize = start.parse().unwrap();
                    let end: usize = end.parse().unwrap();
                    let mut data = (start..=end).map(|i| (i % 251) as u8).collect::<Vec<_>>();
                    if start == 17 {
                        data[0] ^= 1;
                    } // Correct length, wrong range content.
                    ("206 Partial Content", data.len(), data)
                } else if cleanup_failure
                    && (header.starts_with("POST ") || header.starts_with("DELETE "))
                {
                    let data = b"<Error><Code>AccessDenied</Code><Message>injected cleanup failure</Message></Error>".to_vec();
                    ("403 Forbidden", data.len(), data)
                } else if header.starts_with("POST ") {
                    let data = b"<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>"
                        .to_vec();
                    ("200 OK", data.len(), data)
                } else {
                    ("204 No Content", 0, vec![])
                };
                let response_header = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n"
                );
                if socket.write_all(response_header.as_bytes()).await.is_ok() {
                    let _ = socket.write_all(&response).await;
                }
            });
        }
    });
    let mut cfg = ObjectStoreConfig::default();
    cfg.retry.read_retry_attempts = 0;
    cfg.retry.upload_retry_attempts = 0;
    cfg.retry.delete_retry_attempts = 0;
    cfg.retry.delete_objects_retry_attempts = 0;
    let cfg = Arc::new(cfg);
    let metrics = Arc::new(ObjectStoreMetrics::unused());
    let store = crate::object::OpendalObjectStore::new_minio_engine(
        &format!("minio://test-key:test-secret@http://{endpoint}/bucket"),
        cfg.clone(),
        metrics.clone(),
    )
    .unwrap()
    .monitored(metrics, cfg);
    let report = run(
        Arc::new(ObjectStoreImpl::Opendal(store)),
        config(),
        "risectl-bench/corrupt/".into(),
        CancellationToken::new(),
    )
    .await
    .unwrap();
    server.abort();
    assert_eq!(puts.load(Ordering::SeqCst), 1); // Only the 4 KiB probe was uploaded.
    report
}

#[tokio::test]
async fn corrupt_range_is_rejected_before_large_data_preparation() {
    let report = preflight_fault_report(false).await;
    let check = &report.phases[0];
    assert_eq!(check.name, "preflight");
    assert_eq!(check.status, "failed");
    assert!(check.errors[0].contains("incorrect content"), "{check:?}");
    assert_eq!(
        report
            .phases
            .iter()
            .find(|p| p.name == "prepare_data")
            .unwrap()
            .status,
        "skipped"
    );
}

#[tokio::test]
async fn preflight_and_final_cleanup_failures_keep_the_primary_error_and_json() {
    let report = preflight_fault_report(true).await;
    assert!(!report.succeeded());
    let error = &report.phases[0].errors[0];
    assert!(error.contains("incorrect content"), "{error}");
    assert!(error.contains("preflight probe cleanup"), "{error}");
    assert_eq!(report.cleanup_status, "failed");
    assert!(!report.cleanup_errors.is_empty());
    assert!(
        report
            .cleanup_errors
            .iter()
            .any(|e| e.contains("injected cleanup failure"))
    );
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(json["cleanup_status"], "failed");
    assert!(
        json["phases"][0]["errors"][0]
            .as_str()
            .unwrap()
            .contains("incorrect content")
    );
}
