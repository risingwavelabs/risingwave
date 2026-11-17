// Copyright 2026 RisingWave Labs
// SPDX-License-Identifier: Apache-2.0

// Storage-boundary mechanism experiment, not an executor-pattern benchmark.
// Each of four tasks scans one vnode of one committed SST. Real Locality actors
// scan their assigned vnode bitmap sequentially through a flushed snapshot reader,
// deserialize rows and yield chunks under downstream backpressure. Those parts,
// upstream writes and actual barriers are not executed here.
// Fixed-row restarts and gated starts deliberately construct boundary cases;
// ordinary production barriers retain the iterator when no upstream data arrives.
// The independent point task calls get(), whereas HashJoin misses use a prefix
// iterator driven by input rows. Its latency is not a Join critical-path result.
// Shared physical vnode-boundary blocks are forced by this fixture, not measured
// from the SF10 SST layout. Do not infer their production frequency from it.
use std::ops::Bound::{Excluded, Included};
use std::time::{Duration, Instant};

use futures::future::join_all;
use risingwave_common::config::storage::ReadSchedulerConfig;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{
    InMemObjectStore, ObjectStore, ObjectStoreConfig, ObjectStoreImpl,
};

use super::*;
use crate::hummock::iterator::test_utils::mock_sstable_store_with_object_store;
use crate::hummock::test_utils::{gen_test_sstable_with_table_ids, test_key_of};
use crate::hummock::{CachePolicy, SstableStoreRef, TableHolder};
use crate::monitor::StoreLocalStatistic;
use crate::store::{PrefetchOptions, StateStoreIter};

const VNODES: usize = 4;
const ROWS: usize = 4097;
const TABLE: u32 = 34;
const POINT_TABLE: u32 = 32;

fn key(table: u32, vnode: usize, row: usize) -> FullKey<Vec<u8>> {
    let mut bytes = VirtualNode::from_index(vnode).to_be_bytes().to_vec();
    bytes.extend_from_slice(format!("key_{row:05}").as_bytes());
    FullKey::for_test(
        TableId::new(table),
        bytes,
        test_key_of(0).epoch_with_gap.pure_epoch(),
    )
}

fn value(vnode: usize, row: usize) -> Vec<u8> {
    let mut bytes = vec![(row % 251) as u8; 512];
    bytes[..8].copy_from_slice(&(vnode as u64).to_le_bytes());
    bytes[8..16].copy_from_slice(&(row as u64).to_le_bytes());
    bytes
}

fn mode(scenario: &str) -> PrefetchOptions {
    match scenario {
        "prefix_control" => PrefetchOptions::prefetch_for_prefix_scan(),
        "snapshot_control" => PrefetchOptions::prefetch_for_snapshot_backfill(),
        _ => PrefetchOptions::prefetch_for_locality_backfill(),
    }
}

async fn segment(
    store: SstableStoreRef,
    info: SstableInfo,
    vnode: usize,
    start_row: usize,
    stop_row: usize,
    prefetch: PrefetchOptions,
    release: Option<Arc<tokio::sync::Notify>>,
) -> (f64, f64) {
    let reader = HummockVersionReader::new(store.clone(), store.read_metrics.clone());
    let lower = if start_row == 0 {
        Included(TableKey(Bytes::copy_from_slice(
            &VirtualNode::from_index(vnode).to_be_bytes(),
        )))
    } else {
        Excluded(
            key(TABLE, vnode, start_row - 1)
                .user_key
                .table_key
                .to_ref()
                .copy_into::<Bytes>(),
        )
    };
    let upper = Excluded(TableKey(Bytes::copy_from_slice(
        &VirtualNode::from_index(vnode + 1).to_be_bytes(),
    )));
    let started = Instant::now();
    let mut iter = reader
        .iter(
            (lower, upper),
            test_key_of(0).epoch_with_gap.pure_epoch(),
            TableId::new(TABLE),
            TableOption::default(),
            ReadOptions {
                prefetch_options: prefetch,
                ..Default::default()
            },
            (
                vec![],
                vec![],
                build_version_from_sstable(TableId::new(TABLE), info),
            ),
        )
        .await
        .unwrap();
    let first_us = started.elapsed().as_secs_f64() * 1e6;
    let mut max_next_us = 0.0_f64;
    for row in start_row..stop_row {
        let start = Instant::now();
        let (actual_key, actual_value) = iter.try_next().await.unwrap().unwrap();
        max_next_us = max_next_us.max(start.elapsed().as_secs_f64() * 1e6);
        assert_eq!(actual_key, key(TABLE, vnode, row).to_ref());
        assert_eq!(actual_value, value(vnode, row));
        if row == 3800
            && let Some(release) = &release
        {
            release.notify_one();
        }
    }
    if stop_row == ROWS {
        assert!(iter.try_next().await.unwrap().is_none());
    }
    // Returning drops the iterator and its retained responses, matching refresh/stop.
    (first_us, max_next_us)
}

async fn actor(
    store: SstableStoreRef,
    info: SstableInfo,
    vnode: usize,
    scenario: &'static str,
    start_gate: Option<Arc<tokio::sync::Notify>>,
    release: Option<Arc<tokio::sync::Notify>>,
) -> (f64, f64, f64, f64) {
    if let Some(gate) = start_gate {
        gate.notified().await;
    }
    let started = Instant::now();
    let restart = match scenario {
        "restart1" => Some(1),
        "restart128" => Some(128),
        "restart1024" => Some(1024),
        _ => None,
    };
    let stop = match scenario {
        "stop1" => 1,
        "stop128" => 128,
        _ => ROWS,
    };
    let start_row = if scenario == "resumed_neighbors" && vnode % 2 == 0 {
        ROWS - 128
    } else {
        0
    };
    let (first, mut wait) = segment(
        store.clone(),
        info.clone(),
        vnode,
        start_row,
        restart.unwrap_or(stop),
        mode(scenario),
        release,
    )
    .await;
    let resume = if let Some(row) = restart {
        let (resume, next_wait) =
            segment(store, info, vnode, row, ROWS, mode(scenario), None).await;
        wait = wait.max(next_wait);
        resume
    } else {
        0.
    };
    (first, started.elapsed().as_secs_f64() * 1e6, resume, wait)
}

async fn points(store: SstableStoreRef, info: SstableInfo) -> Vec<f64> {
    let reader = HummockVersionReader::new(store.clone(), store.read_metrics.clone());
    let version = build_version_from_sstable(TableId::new(POINT_TABLE), info);
    let mut samples = Vec::new();
    for row in (0..8).map(|i| i * 128 + 1) {
        let started = Instant::now();
        let actual = reader
            .get(
                key(POINT_TABLE, 0, row)
                    .user_key
                    .table_key
                    .to_ref()
                    .copy_into::<Bytes>(),
                test_key_of(0).epoch_with_gap.pure_epoch(),
                TableId::new(POINT_TABLE),
                TableOption::default(),
                ReadOptions::default(),
                (vec![], vec![], version.clone()),
                |_key, value: &[u8]| Ok(value.to_vec()),
            )
            .await
            .unwrap()
            .unwrap();
        samples.push(started.elapsed().as_secs_f64() * 1e6);
        assert_eq!(actual, value(0, row));
    }
    samples
}

fn block_for(sst: &TableHolder, key: &FullKey<Vec<u8>>) -> usize {
    sst.meta
        .block_metas
        .partition_point(|m| FullKey::decode(&m.smallest_key) <= key.to_ref())
        .saturating_sub(1)
}

#[test]
#[ignore = "explicit multi-vnode Locality reader experiment"]
fn locality_scoped_experiment() {
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
    let trials =
        std::env::var("RW_LOCALITY_SCOPE_TRIALS").map_or(10, |s| s.parse::<usize>().unwrap());
    main.block_on(run(&streaming, trials));
}

async fn run(streaming: &tokio::runtime::Runtime, trials: usize) {
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
    let mut store = mock_sstable_store_with_object_store(object).await;
    let inner = Arc::get_mut(&mut store).unwrap();
    inner.blockwise_prefetch.locality_backfill_enabled = true;
    inner.blockwise_prefetch.snapshot_backfill_enabled = true;
    inner.blockwise_prefetch.enabled = true;
    inner.blockwise_prefetch.max_blocks = 16.try_into().unwrap();
    inner.blockwise_prefetch.max_bytes = (1024 * 1024).try_into().unwrap();
    inner.read_metrics = Arc::new(HummockStateStoreMetrics::new(&registry, MetricLevel::Info));
    assert!(!store.block_cache().is_hybrid());
    let mut opts = default_builder_opt_for_test();
    opts.block_capacity = 64 * 1024;
    let (sst, info) = gen_test_sstable_with_table_ids(
        opts.clone(),
        1,
        (0..VNODES).flat_map(|v| {
            (0..ROWS).map(move |r| (key(TABLE, v, r), HummockValue::put(value(v, r))))
        }),
        store.clone(),
        vec![TABLE],
    )
    .await;
    let (point_sst, point_info) = gen_test_sstable_with_table_ids(
        opts,
        2,
        (0..1025).map(|r| (key(POINT_TABLE, 0, r), HummockValue::put(value(0, r)))),
        store.clone(),
        vec![POINT_TABLE],
    )
    .await;
    for vnode in 1..VNODES {
        let first = block_for(&sst, &key(TABLE, vnode, 0));
        let previous = block_for(&sst, &key(TABLE, vnode - 1, ROWS - 1));
        assert_eq!(
            first, previous,
            "vnode boundary must share a physical block"
        );
    }
    let total_bytes: u64 = sst.meta.block_metas.iter().map(|m| u64::from(m.len)).sum();
    println!(
        "SCOPE_SETUP blocks={} bytes={total_bytes} vnodes={VNODES} rows_per_vnode={ROWS} boundary_blocks_shared=3",
        sst.meta.block_metas.len()
    );
    let physical = metrics.operation_size.with_label_values(&["read"]);
    for delay_us in [0, 2000] {
        for scenario in [
            "adjacent",
            "resumed_neighbors",
            "staggered_start",
            "restart1",
            "restart128",
            "restart1024",
            "stop1",
            "stop128",
            "warm_partial",
            "point_mix",
            "prefix_control",
            "snapshot_control",
        ] {
            if std::env::var("RW_LOCALITY_SCOPE_CASE").is_ok_and(|s| s != scenario) {
                continue;
            }
            for trial in 0..trials + 2 {
                backend.set_read_delay_for_test(Duration::ZERO);
                store.clear_block_cache().await.unwrap();
                let mut initially_warm = HashSet::new();
                if scenario == "warm_partial" {
                    for idx in (1..sst.meta.block_metas.len()).step_by(3) {
                        store
                            .get(
                                &sst,
                                idx,
                                CachePolicy::default(),
                                &mut StoreLocalStatistic::default(),
                            )
                            .await
                            .unwrap();
                        initially_warm.insert(idx);
                    }
                }
                backend.set_read_delay_for_test(Duration::from_micros(delay_us));
                let before_gets = physical.get_sample_count();
                let before_bytes = physical.get_sample_sum();
                let before_outer = store.read_metrics.block_cache_outer_fetch.get();
                let started = Instant::now();
                let gates = [
                    Arc::new(tokio::sync::Notify::new()),
                    Arc::new(tokio::sync::Notify::new()),
                ];
                let scans = (0..VNODES)
                    .map(|v| {
                        streaming.spawn(actor(
                            store.clone(),
                            info.clone(),
                            v,
                            scenario,
                            (scenario == "staggered_start" && v % 2 == 1)
                                .then(|| gates[v / 2].clone()),
                            (scenario == "staggered_start" && v % 2 == 0)
                                .then(|| gates[v / 2].clone()),
                        ))
                    })
                    .collect::<Vec<_>>();
                let point_task = (scenario == "point_mix")
                    .then(|| streaming.spawn(points(store.clone(), point_info.clone())));
                let samples = join_all(scans)
                    .await
                    .into_iter()
                    .map(Result::unwrap)
                    .collect::<Vec<_>>();
                let scan_us = started.elapsed().as_secs_f64() * 1e6;
                let point_times = if let Some(task) = point_task {
                    task.await.unwrap()
                } else {
                    vec![]
                };
                // Settle cancelled/speculative reads outside the measured interval.
                tokio::time::sleep(Duration::from_millis(20)).await;
                #[allow(deprecated)]
                for family in registry.gather() {
                    if matches!(
                        family.get_name(),
                        "object_store_read_scheduler_pending"
                            | "object_store_read_scheduler_waiting"
                            | "object_store_read_in_flight"
                    ) {
                        for metric in family.get_metric() {
                            assert_eq!(metric.get_gauge().as_ref().unwrap().value(), 0.);
                        }
                    }
                }
                let gets = physical.get_sample_count() - before_gets;
                let bytes = (physical.get_sample_sum() - before_bytes) as u64;
                let outer = store.read_metrics.block_cache_outer_fetch.get() - before_outer;
                let mut needed = HashSet::new();
                let stop = match scenario {
                    "stop1" => 1,
                    "stop128" => 128,
                    _ => ROWS,
                };
                for vnode in 0..VNODES {
                    let start_row = if scenario == "resumed_neighbors" && vnode % 2 == 0 {
                        ROWS - 128
                    } else {
                        0
                    };
                    needed.extend(
                        block_for(&sst, &key(TABLE, vnode, start_row))
                            ..=block_for(&sst, &key(TABLE, vnode, stop - 1)),
                    );
                }
                let mut demand_bytes: u64 = needed
                    .difference(&initially_warm)
                    .map(|&i| u64::from(sst.meta.block_metas[i].len))
                    .sum();
                if scenario == "point_mix" {
                    let point_blocks = (0..8)
                        .map(|i| block_for(&point_sst, &key(POINT_TABLE, 0, i * 128 + 1)))
                        .collect::<HashSet<_>>();
                    demand_bytes += point_blocks
                        .iter()
                        .map(|&i| u64::from(point_sst.meta.block_metas[i].len))
                        .sum::<u64>();
                }
                assert!(
                    bytes >= demand_bytes,
                    "{scenario}: bytes={bytes}, demand={demand_bytes}"
                );
                if trial >= 2 {
                    let first = samples
                        .iter()
                        .map(|s| format!("{:.3}", s.0))
                        .collect::<Vec<_>>()
                        .join(",");
                    let resume = samples
                        .iter()
                        .map(|s| format!("{:.3}", s.2))
                        .collect::<Vec<_>>()
                        .join(",");
                    let waits = samples
                        .iter()
                        .map(|s| format!("{:.3}", s.3))
                        .collect::<Vec<_>>()
                        .join(",");
                    let points = point_times
                        .iter()
                        .map(|p| format!("{p:.3}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    println!(
                        "SCOPE scenario={scenario} delay_us={delay_us} trial={} gets={gets} bytes={bytes} demand_bytes={demand_bytes} outer={outer} scan_us={scan_us:.3} first_us={first} resume_us={resume} max_next_us={waits} point_us={points}",
                        trial - 2
                    );
                }
            }
        }
    }
    store.block_cache().close().await.unwrap();
}
