// Copyright 2026 RisingWave Labs
// SPDX-License-Identifier: Apache-2.0

//! Explicit R/B/P experiment; no production policy switches. B requires a test
//! binary with ObjectStoreImpl::read_plug returning None. The backend delay is
//! synthetic, not an S3 model. Run with --features test -- --ignored --nocapture.

use std::ops::Bound::Excluded;
use std::time::{Duration, Instant};

use futures::future::join_all;
use risingwave_common::config::MetricLevel;
use risingwave_common::config::storage::ReadSchedulerConfig;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{
    InMemObjectStore, ObjectStore, ObjectStoreConfig, ObjectStoreImpl,
};

use super::*;
use crate::hummock::iterator::HummockIterator;
use crate::hummock::iterator::test_utils::mock_sstable_store_with_object_store;
use crate::hummock::sstable::{BlockIterator, SstableIteratorReadOptions};
use crate::hummock::test_utils::{default_builder_opt_for_test, gen_test_sstable, test_key_of};
use crate::hummock::{HummockValue, SstableIterator, TableHolder};
use crate::store::PrefetchMode;

const MIB: usize = 1 << 20;
const PRESSURE_CAP: usize = MIB / 2;

type Table = (TableHolder, SstableInfo);

fn value(row: usize) -> Vec<u8> {
    let mut value = vec![(row % 251) as u8; 1024];
    value[..8].copy_from_slice(&(row as u64).to_le_bytes());
    value
}

fn us(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1e6
}

fn check_block(block: BlockHolder, table: &Table, index: usize) {
    let mut iter = BlockIterator::new(block);
    iter.seek_to_first();
    assert_eq!(
        iter.key(),
        FullKey::decode(&table.0.meta.block_metas[index].smallest_key)
    );
}

async fn scan(
    store: SstableStoreRef,
    table: &Table,
    mode: PrefetchMode,
    blocks: usize,
    stop: usize,
) -> (f64, f64, usize) {
    let end = FullKey::decode(&table.0.meta.block_metas[blocks].smallest_key)
        .user_key
        .copy_into();
    let stop_key = FullKey::decode(&table.0.meta.block_metas[stop].smallest_key);
    let mut iter = SstableIterator::new(
        table.0.clone(),
        store,
        Arc::new(SstableIteratorReadOptions {
            prefetch: mode,
            scan_end_user_key: Some(Excluded(end)),
            ..Default::default()
        }),
        &table.1,
    );
    let start = Instant::now();
    iter.rewind().await.unwrap();
    let first = us(start);
    let mut rows = 0;
    while iter.is_valid() && iter.key() < stop_key {
        assert_eq!(iter.key(), test_key_of(rows).to_ref());
        assert_eq!(iter.value().into_user_value().unwrap(), value(rows));
        rows += 1;
        iter.next().await.unwrap();
    }
    assert!(rows > 0);
    if stop == blocks {
        assert!(!iter.is_valid());
    } else {
        assert_eq!(iter.key(), stop_key);
    }
    (first, us(start), rows)
}

#[allow(deprecated)]
async fn drain(registry: &prometheus::Registry) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let idle = registry
                .gather()
                .iter()
                .filter(|family| {
                    matches!(
                        family.get_name(),
                        "object_store_read_in_flight"
                            | "object_store_read_scheduler_pending"
                            | "object_store_read_scheduler_waiting"
                            | "object_store_read_scheduler_in_flight"
                            | "object_store_read_scheduler_reserved"
                            | "object_store_read_scheduler_queued"
                    )
                })
                .all(|family| {
                    family
                        .get_metric()
                        .iter()
                        .all(|m| m.get_gauge().as_ref().unwrap().value() == 0.)
                });
            if idle {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[derive(Default)]
struct Measurements {
    gets: Vec<u64>,
    bytes: Vec<u64>,
    outer: Vec<u64>,
    first: Vec<f64>,
    total: Vec<f64>,
    points: Vec<f64>,
    discarded: Vec<u64>,
}

#[test]
#[ignore = "explicit 16-block R/B/P and latency experiment"]
fn plug_window_io_experiment() {
    let arm = std::env::var("RW_PLUG_ARM").unwrap_or_else(|_| "P".into());
    assert!(matches!(arm.as_str(), "R" | "B" | "P"));
    let cross = std::env::var("RW_PLUG_CROSS").map_or(true, |v| v == "1");
    let samples: usize = std::env::var("RW_PLUG_SAMPLES").map_or(1000, |v| v.parse().unwrap());
    assert!(samples > 0);
    let remote = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let workers: usize = std::env::var("RW_PLUG_WORKERS").map_or(8, |v| v.parse().unwrap());
    let mut caller_builder = if workers == 1 {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(workers);
        builder
    };
    let caller = caller_builder.enable_all().build().unwrap();
    caller.block_on(async {
        let registry = prometheus::Registry::new();
        let metrics = Arc::new(ObjectStoreMetrics::new(&registry));
        let backend = InMemObjectStore::for_test();
        let object = Arc::new(ObjectStoreImpl::InMem(backend.clone().monitored(metrics.clone(), Arc::new(ObjectStoreConfig {
            read_scheduler: ReadSchedulerConfig {
                use_dedicated_io_runtime: true,
                dedicated_io_runtime_workers: 2.try_into().unwrap(),
                max_admitted_groups: 128.try_into().unwrap(),
                max_inflight_reads: 32.try_into().unwrap(),
                max_batch_requests: 16.try_into().unwrap(),
                max_read_bytes: MIB.try_into().unwrap(),
                max_gap_bytes: 0,
                reuse_inflight_reads: true,
            }, ..Default::default()
        })) ).with_read_coalescing());
        assert_eq!(object.read_plug(16).is_none(), arm == "B", "B needs the isolated no-plug binary");
        let mut store = if cross { remote.spawn(mock_sstable_store_with_object_store(object)).await.unwrap() }
            else { mock_sstable_store_with_object_store(object).await };
        let inner = Arc::get_mut(&mut store).unwrap();
        inner.blockwise_prefetch.max_blocks = 16.try_into().unwrap();
        inner.blockwise_prefetch.max_bytes = MIB.try_into().unwrap();
        inner.prefetch_buffer_capacity = PRESSURE_CAP;
        inner.read_metrics = Arc::new(HummockStateStoreMetrics::new(&registry, MetricLevel::Info));
        // Verify the runtime actually used by Foyer, not just where we construct a future.
        let expected = if cross { remote.handle().id() } else { tokio::runtime::Handle::current().id() };
        store.block_cache.get_or_fetch(&SstableBlockIndex { sst_id: 999.into(), block_idx: 0 }, || async move {
            assert_eq!(tokio::runtime::Handle::current().id(), expected);
            Err::<(Box<Block>, foyer::HybridCacheProperties), _>(std::io::Error::other("runtime probe"))
        }).await.unwrap_err();
        let mut tables = Vec::new();
        for id in 1..=6 {
            let options = crate::hummock::SstableBuilderOptions { block_capacity: 60 * 1024, ..default_builder_opt_for_test() };
            let table = gen_test_sstable(options, id, (0..8192).map(|i| (test_key_of(i), HummockValue::put(value(i)))), store.clone()).await;
            assert!(table.0.meta.block_metas.len() > 64);
            assert!(table.0.meta.block_metas[..16].iter().map(|m| m.len.max(m.uncompressed_size) as usize).sum::<usize>() <= MIB);
            tables.push(table);
        }
        println!("FIXTURE arm={arm} cross={cross} G=128 P=32 N=16 B=16 cap={MIB} encoded={:?} decoded={:?}",
            tables[0].0.meta.block_metas[..16].iter().map(|m| m.len).collect::<Vec<_>>(),
            tables[0].0.meta.block_metas[..16].iter().map(|m| m.uncompressed_size).collect::<Vec<_>>());
        let delay: u64 = std::env::var("RW_PLUG_DELAY_US").map_or(2000, |v| v.parse().unwrap());
        backend.set_read_delay_for_test(Duration::from_micros(delay));
        let separate_tasks = std::env::var("RW_PLUG_TASKS").map_or(true, |v| v == "separate");
        let mode = if arm == "R" { PrefetchMode::Range } else { PrefetchMode::Blockwise };
        println!("MODEL workers={workers} separate_tasks={separate_tasks} delay_us={delay}");
        let physical = metrics.operation_size.with_label_values(&["read"]);
        for case in ["cold16", "source_free", "source_pressure", "point", "single_cold", "mixed_points", "early_stop"] {
            if let Ok(filter) = std::env::var("RW_PLUG_CASE") { if case != filter { continue; } }
            store.clear_block_cache().await.unwrap();
            if case == "single_cold" {
                for idx in 1..16 {
                    store.get(&tables[0].0, idx, CachePolicy::default(), &mut StoreLocalStatistic::default()).await.unwrap();
                }
            }
            let held = if case == "source_pressure" {
                let stream = store.prefetch_blocks(&tables[5].0, 0, 16, CachePolicy::Disable, &mut StoreLocalStatistic::default()).await.unwrap();
                assert!(store.get_prefetch_memory_usage() > PRESSURE_CAP);
                Some(stream)
            } else { None };
            let count = if matches!(case, "point" | "single_cold" | "mixed_points") { samples } else { (samples / 10).max(1) };
            let mut data = Measurements::default();
            for trial in 0..count + 20 {
                if matches!(case, "point" | "single_cold") {
                    store.block_cache.memory().remove(&SstableBlockIndex { sst_id: tables[0].0.id, block_idx: 0 });
                } else {
                    store.clear_block_cache().await.unwrap();
                }
                drain(&registry).await;
                let gets = physical.get_sample_count();
                let bytes = physical.get_sample_sum();
                let outer = store.read_metrics.block_cache_outer_fetch.get();
                let discarded = store.read_metrics.prefetch_discarded.get();
                let start = Instant::now();
                let (first, total, point) = match case {
                    "point" => {
                        let block = store.get(&tables[0].0, 0, CachePolicy::default(), &mut StoreLocalStatistic::default()).await.unwrap();
                        let elapsed = us(start);
                        check_block(block, &tables[0], 0);
                        (elapsed, elapsed, Some(elapsed))
                    }
                    "single_cold" => {
                        let result = scan(store.clone(), &tables[0], mode, 16, 16).await;
                        (result.0, result.1, None)
                    }
                    "cold16" => {
                        let result = scan(store.clone(), &tables[0], mode, 16, 16).await;
                        (result.0, result.1, None)
                    }
                    "mixed_points" => {
                        let point_store = store.clone();
                        let point_table = tables[4].clone();
                        let point = async move {
                            let start = Instant::now();
                            let block = point_store.get(&point_table.0, 0, CachePolicy::default(), &mut StoreLocalStatistic::default()).await.unwrap();
                            let end = Instant::now();
                            check_block(block, &point_table, 0);
                            end.duration_since(start).as_secs_f64()*1e6
                        };
                        let (scans, point) = if separate_tasks {
                            let scans = tables[..4].iter().cloned().map(|table| {
                                let store = store.clone();
                                tokio::spawn(async move { scan(store, &table, mode, 16, 16).await })
                            }).collect::<Vec<_>>();
                            let point = tokio::spawn(point);
                            let (scans, point) = tokio::join!(join_all(scans), point);
                            (scans.into_iter().map(Result::unwrap).collect::<Vec<_>>(), point.unwrap())
                        } else {
                            let scans = join_all(tables[..4].iter().map(|t| scan(store.clone(), t, mode, 16, 16)));
                            tokio::join!(scans, point)
                        };
                        (scans.iter().map(|x| x.0).fold(0., f64::max), scans.iter().map(|x| x.1).fold(0., f64::max), Some(point))
                    }
                    _ => {
                        let stop = if case == "early_stop" { 5 } else { 64 };
                        let results = join_all(tables[..4].iter().map(|t| scan(store.clone(), t, mode, 64, stop))).await;
                        (results.iter().map(|x| x.0).fold(0., f64::max), results.iter().map(|x| x.1).fold(0., f64::max), None)
                    }
                };
                if case == "early_stop" { tokio::time::sleep(Duration::from_millis(10)).await; }
                drain(&registry).await;
                let actual_outer = store.read_metrics.block_cache_outer_fetch.get() - outer;
                if arm != "R" && case == "cold16" { assert_eq!(actual_outer, 16); }
                if arm != "R" && case == "single_cold" { assert_eq!(actual_outer, 1); }
                if trial >= 20 {
                    data.gets.push(physical.get_sample_count() - gets);
                    data.bytes.push((physical.get_sample_sum() - bytes) as u64);
                    data.outer.push(actual_outer);
                    data.first.push(first); data.total.push(total);
                    if let Some(point) = point { data.points.push(point); }
                    data.discarded.push(store.read_metrics.prefetch_discarded.get() - discarded);
                }
            }
            drop(held);
            assert_eq!(store.get_prefetch_memory_usage(), 0);
            println!("DATA {{\"arm\":\"{arm}\",\"cross\":{cross},\"case\":\"{case}\",\"gets\":{:?},\"bytes\":{:?},\"outer\":{:?},\"first_us\":{:?},\"total_us\":{:?},\"point_us\":{:?},\"discarded\":{:?}}}", data.gets, data.bytes, data.outer, data.first, data.total, data.points, data.discarded);
        }
        store.block_cache.close().await.unwrap();
    });
}
