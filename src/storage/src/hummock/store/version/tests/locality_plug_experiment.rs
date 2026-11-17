// Copyright 2026 RisingWave Labs
// SPDX-License-Identifier: Apache-2.0

// Exercise the storage entry point used by Locality's flushed snapshot reader:
// bounded version iter -> Locality mode routing -> SST iterator -> Foyer -> IO.
// This is not a full Locality/Join executor or an S3 performance benchmark.
use std::ops::Bound::{Excluded, Unbounded};
use std::time::{Duration, Instant};

use risingwave_common::config::storage::ReadSchedulerConfig;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{
    InMemObjectStore, ObjectStore, ObjectStoreConfig, ObjectStoreImpl,
};

use super::*;
use crate::hummock::iterator::test_utils::mock_sstable_store_with_object_store;
use crate::hummock::test_utils::{gen_default_test_sstable, test_key_of, test_value_of};
use crate::hummock::{CachePolicy, SstableStoreRef, TableHolder};
use crate::monitor::StoreLocalStatistic;
use crate::store::{PrefetchOptions, StateStoreIter};

// Optional diagnostic output is disabled for latency measurements. It observes
// existing events without introducing gates into registration or Foyer fetches.
struct Trace;
struct Fields(String);
impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        write!(&mut self.0, " {}={value:?}", field.name()).unwrap();
    }
}
static TRACE_LINES: std::sync::Mutex<Vec<(f64, String)>> = std::sync::Mutex::new(Vec::new());
fn trace_line(fields: Fields) {
    static START: std::sync::LazyLock<Instant> = std::sync::LazyLock::new(Instant::now);
    TRACE_LINES
        .lock()
        .unwrap()
        .push((START.elapsed().as_secs_f64() * 1e6, fields.0));
}
impl tracing::Subscriber for Trace {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        matches!(
            metadata.target(),
            "state_store_read_batch" | "object_store_read_scheduler"
        )
    }

    fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        static ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut fields = Fields(format!(" span={id} name={}", attrs.metadata().name()));
        attrs.record(&mut fields);
        trace_line(fields);
        tracing::span::Id::from_u64(id)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut fields = Fields(String::new());
        event.record(&mut fields);
        trace_line(fields);
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

async fn scan(
    store: SstableStoreRef,
    sst: TableHolder,
    info: SstableInfo,
    blocks: usize,
    stop_in_last_block: bool,
) -> (f64, f64, usize, f64) {
    let first = test_key_of(0);
    let table_id = first.user_key.table_id;
    let end: TableKey<Bytes> = FullKey::decode(&sst.meta.block_metas[blocks].smallest_key)
        .user_key
        .table_key
        .copy_into();
    let reader = HummockVersionReader::new(store.clone(), store.read_metrics.clone());
    let start = Instant::now();
    let mut iter = reader
        .iter(
            (Unbounded, Excluded(end.clone())),
            first.epoch_with_gap.pure_epoch(),
            table_id,
            TableOption::default(),
            ReadOptions {
                prefetch_options: PrefetchOptions::prefetch_for_locality_backfill(),
                ..Default::default()
            },
            (vec![], vec![], build_version_from_sstable(table_id, info)),
        )
        .await
        .unwrap();
    let first_us = start.elapsed().as_secs_f64() * 1e6;
    let mut rows = 0;
    let mut max_next_us = 0.0_f64;
    loop {
        let next_start = Instant::now();
        let next = iter.try_next().await.unwrap();
        max_next_us = max_next_us.max(next_start.elapsed().as_secs_f64() * 1e6);
        let Some((key, value)) = next else {
            break;
        };
        assert_eq!(key, test_key_of(rows).to_ref());
        assert_eq!(value, test_value_of(rows));
        rows += 1;
        if stop_in_last_block && key >= FullKey::decode(&sst.meta.block_metas[15].smallest_key) {
            break;
        }
    }
    if !stop_in_last_block {
        assert_eq!(test_key_of(rows).user_key.table_key.as_ref(), end.as_ref());
    }
    (
        first_us,
        start.elapsed().as_secs_f64() * 1e6,
        rows,
        max_next_us,
    )
}

async fn point(store: SstableStoreRef, sst: TableHolder, idx: usize) -> f64 {
    let start = Instant::now();
    store
        .get(
            &sst,
            idx,
            CachePolicy::default(),
            &mut StoreLocalStatistic::default(),
        )
        .await
        .unwrap();
    start.elapsed().as_secs_f64() * 1e6
}

#[test]
#[ignore = "explicit Locality storage-path merge/latency experiment"]
fn locality_plug_experiment() {
    if std::env::var_os("RW_LOCALITY_TRACE").is_some() {
        tracing::subscriber::set_global_default(Trace).unwrap();
    }
    let main = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let streaming = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    let trials: usize = std::env::var("RW_LOCALITY_TRIALS")
        .ok()
        .map(|v| v.parse().unwrap())
        .unwrap_or(50);
    let scenario_filter = std::env::var("RW_LOCALITY_SCENARIO").ok();
    main.block_on(run_experiment(&streaming, trials, scenario_filter));
    for (us, fields) in TRACE_LINES.lock().unwrap().drain(..) {
        println!("TRACE us={us:.3}{fields}");
    }
}

async fn run_experiment(
    streaming: &tokio::runtime::Runtime,
    trials: usize,
    scenario_filter: Option<String>,
) {
    let registry = Registry::new();
    let metrics = Arc::new(ObjectStoreMetrics::new(&registry));
    let backend = InMemObjectStore::for_test();
    let object = Arc::new(
        ObjectStoreImpl::InMem(backend.clone().monitored(
            metrics.clone(),
            Arc::new(ObjectStoreConfig {
                read_scheduler: ReadSchedulerConfig {
                    use_dedicated_io_runtime: true,
                    dedicated_io_runtime_workers: 2.try_into().unwrap(),
                    max_admitted_groups: 128.try_into().unwrap(),
                    max_inflight_reads: 32.try_into().unwrap(),
                    max_batch_requests: 16.try_into().unwrap(),
                    max_read_bytes: (1024 * 1024).try_into().unwrap(),
                    max_gap_bytes: 0,
                    ..Default::default()
                },
                ..Default::default()
            }),
        ))
        .with_read_coalescing(),
    );
    // Foyer captures the node-initialization runtime, not rw-streaming.
    let mut store = mock_sstable_store_with_object_store(object).await;
    let inner = Arc::get_mut(&mut store).unwrap();
    inner.blockwise_prefetch.locality_backfill_enabled = true;
    inner.blockwise_prefetch.enabled = false;
    inner.blockwise_prefetch.max_blocks = 16.try_into().unwrap();
    inner.blockwise_prefetch.max_bytes = (1024 * 1024).try_into().unwrap();
    inner.read_metrics = Arc::new(HummockStateStoreMetrics::new(&registry, MetricLevel::Info));
    let (sst, info) =
        gen_default_test_sstable(default_builder_opt_for_test(), 1, store.clone()).await;
    let (point_sst, _) =
        gen_default_test_sstable(default_builder_opt_for_test(), 2, store.clone()).await;
    assert!(sst.meta.block_metas.len() > 32);
    for blocks in sst.meta.block_metas[..32].windows(2) {
        assert_eq!(blocks[0].offset + blocks[0].len, blocks[1].offset);
    }
    assert!(
        sst.meta.block_metas[..16]
            .iter()
            .map(|m| m.len.max(m.uncompressed_size) as usize)
            .sum::<usize>()
            < 1024 * 1024
    );
    let physical = metrics.operation_size.with_label_values(&["read"]);
    #[allow(deprecated)]
    let event = |name: &str| -> u64 {
        registry
            .gather()
            .iter()
            .filter(|f| f.get_name() == "object_store_read_scheduler_events")
            .flat_map(|f| f.get_metric())
            .filter(|m| {
                m.get_label()
                    .iter()
                    .any(|l| l.get_name() == "event" && l.get_value() == name)
            })
            .map(|m| m.get_counter().as_ref().unwrap().value() as u64)
            .sum()
    };
    for delay_us in [0, 2000] {
        backend.set_read_delay_for_test(Duration::from_micros(delay_us));
        for scenario in [
            "cold16",
            "cold32",
            "stop_last",
            "single",
            "warm_neighbors",
            "warm_head",
            "cold8",
            "warm_gaps",
            "point",
            "mixed",
            "shared",
            "racing_point",
            "existing_point",
        ] {
            if scenario_filter
                .as_ref()
                .is_some_and(|filter| filter != scenario)
            {
                continue;
            }
            let blocks = if scenario == "single" {
                1
            } else if matches!(scenario, "cold32" | "stop_last") {
                32
            } else {
                16
            };
            for trial in 0..trials + 5 {
                store.clear_block_cache().await.unwrap();
                for idx in 0..blocks {
                    let warm = match scenario {
                        "warm_neighbors" => idx != 0,
                        "warm_head" => idx != 1,
                        "cold8" => idx >= 8,
                        "warm_gaps" => idx % 2 == 0,
                        _ => false,
                    };
                    if warm {
                        point(store.clone(), sst.clone(), idx).await;
                    }
                }
                let before_gets = physical.get_sample_count();
                let before_bytes = physical.get_sample_sum();
                let before_batch = event("submitted_batch_members");
                let before_single = event("sealed_batch_single");
                let before_multi = event("sealed_batch_multiple");
                let before_outer = store.read_metrics.block_cache_outer_fetch.get();
                let before_registered = store.read_metrics.prefetch_registered_ready.get()
                    + store.read_metrics.prefetch_registered_pending.get();
                let before_pending = store.read_metrics.prefetch_registered_pending.get();
                let mut point_us = 0.;
                let (first_us, total_us, rows, max_next_us) = if scenario == "point" {
                    point_us = streaming
                        .spawn(point(store.clone(), point_sst.clone(), 0))
                        .await
                        .unwrap();
                    (0., point_us, 0, 0.)
                } else {
                    let existing_point = if scenario == "existing_point" {
                        let task = streaming.spawn(point(store.clone(), sst.clone(), 7));
                        while store.read_metrics.block_cache_outer_fetch.get() == before_outer {
                            tokio::task::yield_now().await;
                        }
                        // Let this existing producer reach scheduler admission before
                        // creating the scan; no production task is artificially held.
                        tokio::task::yield_now().await;
                        Some(task)
                    } else {
                        None
                    };
                    let scan = streaming.spawn(scan(
                        store.clone(),
                        sst.clone(),
                        info.clone(),
                        blocks,
                        scenario == "stop_last",
                    ));
                    let point_task = if matches!(scenario, "mixed" | "shared" | "racing_point") {
                        let store = store.clone();
                        let sst = if scenario != "mixed" {
                            sst.clone()
                        } else {
                            point_sst.clone()
                        };
                        Some(streaming.spawn(async move {
                            // Observe registration without postponing the scan or its producers.
                            let required = if scenario == "shared" { 15 } else { 1 };
                            while store.read_metrics.prefetch_registered_pending.get()
                                < before_pending + required
                            {
                                tokio::task::yield_now().await;
                            }
                            point(store, sst, if scenario != "mixed" { 7 } else { 0 }).await
                        }))
                    } else {
                        existing_point
                    };
                    let sample = scan.await.unwrap();
                    if let Some(task) = point_task {
                        point_us = task.await.unwrap();
                    }
                    sample
                };
                // Count all speculative work after dropping the scan, outside latency samples.
                if scenario == "stop_last" {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                let fetched_blocks = if scenario == "stop_last" { 16 } else { blocks };
                let gets = physical.get_sample_count() - before_gets;
                let outer = store.read_metrics.block_cache_outer_fetch.get() - before_outer;
                let registered = store.read_metrics.prefetch_registered_ready.get()
                    + store.read_metrics.prefetch_registered_pending.get()
                    - before_registered;
                let expected_outer = match scenario {
                    "single" | "warm_neighbors" | "warm_head" | "point" => 1,
                    "warm_gaps" | "cold8" => 8,
                    "mixed" => 17,
                    _ => fetched_blocks as u64,
                };
                assert_eq!(outer, expected_outer, "{scenario}");
                assert_eq!(
                    registered,
                    if scenario == "point" {
                        0
                    } else {
                        (fetched_blocks - fetched_blocks.div_ceil(16)) as u64
                    }
                );
                assert!(gets > 0 && gets <= outer);
                let unique_bytes: u64 = if scenario == "point" {
                    u64::from(point_sst.meta.block_metas[0].len)
                } else {
                    sst.meta.block_metas[..fetched_blocks]
                        .iter()
                        .enumerate()
                        .filter(|(idx, _)| match scenario {
                            "warm_neighbors" => *idx == 0,
                            "warm_head" => *idx == 1,
                            "cold8" => *idx < 8,
                            "warm_gaps" => *idx % 2 != 0,
                            _ => true,
                        })
                        .map(|(_, block)| u64::from(block.len))
                        .sum::<u64>()
                        + if scenario == "mixed" {
                            u64::from(point_sst.meta.block_metas[0].len)
                        } else {
                            0
                        }
                };
                let bytes = (physical.get_sample_sum() - before_bytes) as u64;
                assert!(bytes >= unique_bytes);
                if trial >= 5 {
                    println!(
                        "LOCALITY scenario={scenario} delay_us={delay_us} trial={} gets={gets} outer={outer} registered={registered} collected={} flush_single={} flush_multi={} bytes={} unique_bytes={unique_bytes} first_us={first_us:.3} total_us={total_us:.3} point_us={point_us:.3} max_next_us={max_next_us:.3} rows={rows}",
                        trial - 5,
                        event("submitted_batch_members") - before_batch,
                        event("sealed_batch_single") - before_single,
                        event("sealed_batch_multiple") - before_multi,
                        physical.get_sample_sum() - before_bytes
                    );
                }
            }
        }
    }
    store.block_cache().close().await.unwrap();
}
