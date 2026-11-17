// Copyright 2026 RisingWave Labs
// SPDX-License-Identifier: Apache-2.0

//! Real SST iterator/Foyer/scheduler experiment. Run explicitly with
//! --features test -- --ignored --nocapture.
//! The injected 2ms backend uses Tokio timers; it is not an S3 latency model.
use std::time::Duration;

use futures::future::join_all;
use risingwave_common::config::MetricLevel;
use risingwave_common::config::storage::ReadSchedulerConfig;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{
    InMemObjectStore, ObjectStore, ObjectStoreConfig, ObjectStoreImpl,
};

use super::*;
use crate::hummock::CachePolicy;
use crate::hummock::iterator::test_utils::mock_sstable_store_with_object_store;
use crate::hummock::test_utils::{
    default_builder_opt_for_test, gen_default_test_sstable, test_key_of, test_value_of,
};
use crate::monitor::HummockStateStoreMetrics;

#[derive(Default)]
struct Sample {
    first_us: f64,
    total_us: f64,
    block_wait_us: Vec<f64>,
    max_handles: usize,
    max_retained_bytes: usize,
    rows: usize,
}

async fn scan(
    sst: TableHolder,
    info: SstableInfo,
    store: SstableStoreRef,
    blocks: usize,
    stop_after: usize,
    work_us: u64,
    mode: PrefetchMode,
) -> Sample {
    let options = Arc::new(SstableIteratorReadOptions {
        prefetch: mode,
        scan_end_user_key: Some(Excluded(
            FullKey::decode(&sst.meta.block_metas[blocks].smallest_key)
                .user_key
                .copy_into(),
        )),
        ..Default::default()
    });
    let mut iter = SstableIterator::new(sst.clone(), store.clone(), options, &info);
    let start = Instant::now();
    iter.rewind().await.unwrap();
    let mut sample = Sample {
        first_us: start.elapsed().as_secs_f64() * 1e6,
        ..Default::default()
    };
    let mut worked = usize::MAX;
    while iter.is_valid() && iter.cur_idx < stop_after {
        let idx = iter.cur_idx;
        // Exercise actual row consumption, not seek() resetting prefetch each block.
        assert_eq!(iter.key(), test_key_of(sample.rows).to_ref());
        assert_eq!(
            iter.value().into_user_value().unwrap(),
            test_value_of(sample.rows)
        );
        sample.rows += 1;
        sample.max_handles = sample.max_handles.max(iter.read_ahead.len());
        let retained: usize = sst.meta.block_metas[idx..iter.read_ahead_end.max(idx + 1)]
            .iter()
            .map(|m| m.len.max(m.uncompressed_size) as usize)
            .sum();
        sample.max_retained_bytes = sample.max_retained_bytes.max(retained);
        if worked != idx && work_us != 0 {
            worked = idx;
            let until = Instant::now() + Duration::from_micros(work_us);
            while Instant::now() < until {
                std::hint::spin_loop();
            }
            // Model an operator yielding between chunks of CPU work.
            tokio::task::yield_now().await;
        }
        let next = Instant::now();
        iter.next().await.unwrap();
        if iter.is_valid() && iter.cur_idx != idx {
            sample
                .block_wait_us
                .push(next.elapsed().as_secs_f64() * 1e6);
        }
    }
    sample.total_us = start.elapsed().as_secs_f64() * 1e6;
    if mode == PrefetchMode::Blockwise {
        assert!(sample.max_handles < store.blockwise_prefetch.max_blocks.get());
        assert!(sample.max_retained_bytes <= store.blockwise_prefetch.max_bytes.get());
    }
    sample
}

fn percentile(values: &mut [f64], fraction: f64) -> f64 {
    values.sort_by(f64::total_cmp);
    values[((values.len() - 1) as f64 * fraction) as usize]
}

#[test]
#[ignore = "explicit local latency/merge experiment; needs object-store test feature"]
fn real_iterator_io_experiment() {
    let remote = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    caller.block_on(async {
        for dedicated in [false, true] {
            for separate_foyer in [false, true] {
                let registry = prometheus::Registry::new();
                let object_metrics = Arc::new(ObjectStoreMetrics::new(&registry));
                let backend = InMemObjectStore::for_test();
                let object = Arc::new(ObjectStoreImpl::InMem(backend.clone().monitored(
                    object_metrics.clone(), Arc::new(ObjectStoreConfig {
                        read_scheduler: ReadSchedulerConfig {
                            use_dedicated_io_runtime: dedicated,
                            max_admitted_groups: 4.try_into().unwrap(),
                            max_inflight_reads: 2.try_into().unwrap(),
                            ..Default::default()
                        }, ..Default::default()
                    })) ).with_read_coalescing());
                let mut store = if separate_foyer {
                    remote.spawn(mock_sstable_store_with_object_store(object)).await.unwrap()
                } else { mock_sstable_store_with_object_store(object).await };
                let inner = Arc::get_mut(&mut store).unwrap();
                inner.blockwise_prefetch.max_blocks = 8.try_into().unwrap();
                inner.blockwise_prefetch.max_bytes = (64 * 1024).try_into().unwrap();
                inner.read_metrics = Arc::new(HummockStateStoreMetrics::new(&registry, MetricLevel::Info));
                let mut tables = Vec::new();
                for id in 1..=5 {
                    let table = gen_default_test_sstable(default_builder_opt_for_test(), id, store.clone()).await;
                    assert!(table.0.meta.block_metas.len() > 40);
                    tables.push(table);
                }
                backend.set_read_delay_for_test(Duration::from_millis(2));
                for (scenario, blocks, stop, work_us, scans, mode) in [
                    ("short_bound", 2, 2, 0, 1, PrefetchMode::Blockwise),
                    ("long_fast", 32, 32, 0, 1, PrefetchMode::Blockwise),
                    ("long_work", 32, 32, 500, 1, PrefetchMode::Blockwise),
                    ("early_stop", 32, 5, 500, 1, PrefetchMode::Blockwise),
                    ("mixed_saturated", 32, 32, 500, 4, PrefetchMode::Blockwise),
                    ("range_control", 32, 32, 500, 1, PrefetchMode::Range),
                ] {
                    let mut first = Vec::new(); let mut total = Vec::new();
                    let mut waits = Vec::new(); let mut points = Vec::new();
                    let mut physical = 0; let mut read_bytes = 0.; let mut discarded = 0;
                    let mut max_handles = 0; let mut max_bytes = 0;
                    for trial in 0..6 {
                        store.clear_block_cache().await.unwrap();
                        let physical_metric = object_metrics.operation_size.with_label_values(&["read"]);
                        let before_gets = physical_metric.get_sample_count();
                        let before_bytes = physical_metric.get_sample_sum();
                        let before_discard = store.read_metrics.prefetch_discarded.get();
                        let futures = (0..scans).map(|index| scan(tables[index].0.clone(), tables[index].1.clone(), store.clone(), blocks, stop, work_us, mode));
                        let point = async {
                            let mut latencies = Vec::new();
                            if scenario == "mixed_saturated" {
                                for idx in 0..8 {
                                    let start = Instant::now();
                                    let block = store.get(&tables[4].0, idx, CachePolicy::default(), &mut StoreLocalStatistic::default()).await.unwrap();
                                    drop(block);
                                    latencies.push(start.elapsed().as_secs_f64() * 1e6);
                                }
                            }
                            latencies
                        };
                        let (samples, point_samples) = tokio::join!(join_all(futures), point);
                        // Let discarded speculative work settle outside the timed scan.
                        tokio::time::sleep(Duration::from_millis(40)).await;
                        #[allow(deprecated)]
                        for family in registry.gather() {
                            if family.get_name() == "object_store_read_in_flight" {
                                for metric in family.get_metric() { assert_eq!(metric.get_gauge().as_ref().unwrap().value(), 0.); }
                            }
                        }
                        if trial > 0 {
                            physical += physical_metric.get_sample_count() - before_gets;
                            read_bytes += physical_metric.get_sample_sum() - before_bytes;
                            discarded += store.read_metrics.prefetch_discarded.get() - before_discard;
                            points.extend(point_samples);
                            for sample in samples {
                                first.push(sample.first_us); total.push(sample.total_us); waits.extend(sample.block_wait_us);
                                max_handles = max_handles.max(sample.max_handles);
                                max_bytes = max_bytes.max(sample.max_retained_bytes);
                            }
                        }
                    }
                    let point50 = if points.is_empty() { 0. } else { percentile(&mut points, 0.5) };
                    let point99 = if points.is_empty() { 0. } else { percentile(&mut points, 0.99) };
                    println!("ITER scenario={scenario} dedicated={dedicated} separate_foyer={separate_foyer} trials=5 scans={scans} GETs={physical} bytes={read_bytes:.0} discarded={discarded} max_handles={max_handles} max_bytes={max_bytes} first_p50_us={:.1} first_p99_us={:.1} scan_p50_us={:.1} block_p95_us={:.1} point_p50_us={point50:.1} point_p99_us={point99:.1}", percentile(&mut first, 0.5), percentile(&mut first, 0.99), percentile(&mut total, 0.5), percentile(&mut waits, 0.95));
                }
                store.block_cache().close().await.unwrap();
            }
        }
    });
}

#[tokio::test]
#[ignore = "explicit warm-cache regression experiment"]
async fn warm_iterator_io_experiment() {
    let registry = prometheus::Registry::new();
    let metrics = Arc::new(ObjectStoreMetrics::new(&registry));
    let object = Arc::new(
        ObjectStoreImpl::InMem(
            InMemObjectStore::for_test()
                .monitored(metrics.clone(), Arc::new(ObjectStoreConfig::default())),
        )
        .with_read_coalescing(),
    );
    let mut store = mock_sstable_store_with_object_store(object).await;
    Arc::get_mut(&mut store)
        .unwrap()
        .blockwise_prefetch
        .max_blocks = 8.try_into().unwrap();
    let (sst, info) =
        gen_default_test_sstable(default_builder_opt_for_test(), 1, store.clone()).await;
    for idx in 0..32 {
        store
            .get(
                &sst,
                idx,
                CachePolicy::default(),
                &mut StoreLocalStatistic::default(),
            )
            .await
            .unwrap();
    }
    let before = metrics
        .operation_size
        .with_label_values(&["read"])
        .get_sample_count();
    let mut iter = SstableIterator::new(
        sst.clone(),
        store.clone(),
        Arc::new(SstableIteratorReadOptions::default()),
        &info,
    );
    let future = iter.seek_idx(0, None);
    let future_bytes = std::mem::size_of_val(&future);
    drop(future);
    let mut times = Vec::new();
    for trial in 0..31 {
        let sample = scan(
            sst.clone(),
            info.clone(),
            store.clone(),
            32,
            32,
            0,
            PrefetchMode::Blockwise,
        )
        .await;
        if trial > 0 {
            times.push(sample.total_us);
        }
    }
    assert_eq!(
        metrics
            .operation_size
            .with_label_values(&["read"])
            .get_sample_count(),
        before
    );
    println!(
        "WARM samples=30 response_bytes={} seek_future_bytes={future_bytes} scan_p50_us={:.1} scan_p95_us={:.1}",
        std::mem::size_of::<BlockResponse>(),
        percentile(&mut times, 0.5),
        percentile(&mut times, 0.95)
    );
}
