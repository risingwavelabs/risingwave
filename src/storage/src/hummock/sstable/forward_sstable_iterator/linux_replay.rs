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

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use futures::future::join_all;
use risingwave_common::catalog::TableId;
use risingwave_common::config::MetricLevel;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{ObjectStoreConfig, build_remote_object_store, replay};
use tokio::sync::{Barrier, oneshot};

use super::*;
use crate::hummock::test_utils::test_key_of;
use crate::hummock::{
    CachePolicy, InMemWriter, SstableBuilder, SstableBuilderOptions, SstableStore,
};
use crate::monitor::HummockStateStoreMetrics;

const OBJECTS: usize = 18;
const ROWS_PER_OBJECT: usize = 8_192;
const SCANS_PER_OBJECT: usize = 16;
const POINTS_PER_OBJECT: usize = 17;
const G: usize = 128;
const P: usize = 32;

fn value(object: usize, row: usize) -> [u8; 4_096] {
    let mut value = [0; 4_096];
    value[..4].copy_from_slice(&(object as u32).to_be_bytes());
    value[4..8].copy_from_slice(&(row as u32).to_be_bytes());
    value
}

#[test]
#[ignore = "requires a fresh loopback MinIO bucket and an explicit trace path"]
fn linux_read_scheduler_replay() {
    let addr = std::env::var("RW_REPLAY_MINIO_ADDR")
        .expect("set RW_REPLAY_MINIO_ADDR to the loopback MinIO socket")
        .parse::<std::net::SocketAddr>()
        .expect("RW_REPLAY_MINIO_ADDR must be an IP address and port");
    assert!(addr.is_ipv4() && addr.ip().is_loopback());
    assert!(!cfg!(debug_assertions), "run this fixture with --release");
    let access = std::env::var("MINIO_ROOT_USER").unwrap();
    let secret = std::env::var("MINIO_ROOT_PASSWORD").unwrap();
    let bucket = std::env::var("RW_REPLAY_BUCKET").unwrap();
    let trace = PathBuf::from(std::env::var_os("RW_REPLAY_TRACE").unwrap());
    let url = format!("minio://{access}:{secret}@{addr}/{bucket}");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    println!(
        "workers={} available_parallelism={} arch={} pid={}",
        runtime.metrics().num_workers(),
        std::thread::available_parallelism().unwrap(),
        std::env::consts::ARCH,
        std::process::id(),
    );

    replay::start();
    runtime.block_on(async {
        // Populate a disposable bucket before measuring the cold read phase.
        let registry = prometheus::Registry::new();
        let object_metrics = Arc::new(ObjectStoreMetrics::new(&registry));
        let mut config = ObjectStoreConfig::default();
        config.s3.developer.use_opendal = true;
        config.read_scheduler.max_admitted_groups = G.try_into().unwrap();
        config.read_scheduler.max_inflight_reads = P.try_into().unwrap();
        config.read_scheduler.max_batch_requests = 16.try_into().unwrap();
        config.read_scheduler.max_read_bytes = (1usize << 20).try_into().unwrap();
        config.read_scheduler.max_gap_bytes = 0;
        config.read_scheduler.prefetch_batch_wait_us = 100;
        config.read_scheduler.reuse_inflight_reads = true;
        let remote =
            build_remote_object_store(&url, object_metrics.clone(), "scheduler replay", Arc::new(config))
                .await
                .with_read_coalescing();
        let mut store = SstableStore::for_compactor(
            Arc::new(remote),
            "scheduler-replay".into(),
            32 << 20,
            16 << 20,
            true,
            Arc::new(HummockStateStoreMetrics::new(
                &registry,
                MetricLevel::Info,
            )),
        ).await.unwrap();
        store.blockwise_prefetch.max_blocks = 16.try_into().unwrap();
        store.blockwise_prefetch.max_bytes = (1usize << 20).try_into().unwrap();
        let store = Arc::new(store);
        let mut tables = Vec::new();
        for object in 0..OBJECTS {
            let id = object as u64 + 1;
            let options = SstableBuilderOptions {
                capacity: 40 << 20,
                block_capacity: 64 << 10,
                ..Default::default()
            };
            let mut builder = SstableBuilder::for_test(
                id,
                InMemWriter::from(&options),
                options,
                HashMap::from([(TableId::default(), 1)]),
                HashMap::<TableId, _>::new(),
            );
            for row in 0..ROWS_PER_OBJECT {
                builder.add(test_key_of(row).to_ref(), HummockValue::put(value(object, row).as_slice()))
                    .await.unwrap();
            }
            let built = builder.finish().await.unwrap();
            let info = built.sst_info.sst_info;
            store.put_sst_data(info.object_id, built.writer_output.0).await.unwrap();
            let sst = store.sstable(&info, &mut StoreLocalStatistic::default()).await.unwrap();
            assert!(sst.meta.block_metas.len() >= SCANS_PER_OBJECT);
            tables.push((sst, info));
        }

        let read_op = object_metrics.operation_size.with_label_values(&["read"]);
        let reads_before = read_op.get_sample_count();
        let bytes_before = object_metrics.read_bytes.get();
        let (stop_sampling, mut stop) = oneshot::channel();
        let sample = {
            let registry = registry.clone();
            tokio::spawn(async move {
                let mut ticks = tokio::time::interval(Duration::from_millis(5));
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                let mut samples = 0;
                let mut g_full_with_waiters = 0;
                let mut p_active_full = 0;
                loop {
                    tokio::select! {
                        biased;
                        _ = &mut stop => return (samples, g_full_with_waiters, p_active_full),
                        _ = ticks.tick() => {
                            let all = registry.gather();
                            let gauge = |name, labels: &[(&str, &str)]| {
                                all.iter().find(|family| family.name() == name)
                                    .and_then(|family| family.get_metric().iter().find(|metric| {
                                        labels.iter().all(|&(name, value)| metric.get_label().iter()
                                            .any(|entry| entry.name() == name && entry.value() == value))
                                    }))
                                    .map_or(0, |metric| metric.get_gauge().value() as usize)
                            };
                            let admitted = gauge("object_store_read_scheduler_groups", &[("stage", "admitted")]);
                            let waiting = gauge("object_store_read_scheduler_waiting", &[]);
                            let active = gauge("object_store_read_scheduler_in_flight", &[]);
                            samples += 1;
                            g_full_with_waiters += usize::from(admitted == G && waiting != 0);
                            p_active_full += usize::from(active == P);
                            replay::depth_sample(admitted, waiting, active);
                        }
                    }
                }
            })
        };
        let jobs: usize = tables.iter().map(|(table, _)| {
            let blocks = table.meta.block_metas.len();
            blocks.div_ceil(blocks.div_ceil(SCANS_PER_OBJECT))
        }).sum();
        let start = Arc::new(std::sync::OnceLock::new());
        let launch = Arc::new(Barrier::new(jobs + 1));
        let mut scans = Vec::new();
        let mut points = Vec::new();
        for (object, (table, info)) in tables.iter().enumerate() {
            let blocks = &table.meta.block_metas;
            let stride = blocks.len().div_ceil(SCANS_PER_OBJECT);
            let mut first_row = 0;
            for first in (0..blocks.len()).step_by(stride) {
                let end = (first + stride).min(blocks.len());
                let rows = blocks[first..end].iter().map(|meta| meta.total_key_count as usize).sum::<usize>();
                let start_row = first_row;
                first_row += rows;
                let store = store.clone();
                let table = table.clone();
                let info = info.clone();
                let launch = launch.clone();
                scans.push(tokio::spawn(async move {
                    launch.wait().await;
                    let options = Arc::new(SstableIteratorReadOptions {
                        prefetch: PrefetchMode::Blockwise, ..Default::default()
                    });
                    let mut iter = SstableIterator::new(table, store, options, &info);
                    iter.block_end_idx_exclusive = end;
                    iter.seek_idx(first, None).await.unwrap();
                    for row in start_row..start_row + rows {
                        assert!(iter.is_valid());
                        assert_eq!(iter.key(), test_key_of(row).to_ref());
                        assert_eq!(iter.value(), HummockValue::put(value(object, row).as_slice()));
                        iter.next().await.unwrap();
                    }
                    assert!(!iter.is_valid());
                    rows
                }));
            }
            assert_eq!(first_row, ROWS_PER_OBJECT);
            for probe in 0..POINTS_PER_OBJECT {
                let index = probe * blocks.len() / POINTS_PER_OBJECT;
                let key = &blocks[index].smallest_key;
                let store = &store;
                let sequence = probe * OBJECTS + object;
                let start = start.clone();
                points.push(async move {
                    let start = *start.get().unwrap();
                    tokio::time::sleep_until(
                        tokio::time::Instant::from_std(start + Duration::from_micros(sequence as u64 * 100))
                    ).await;
                    let issued = Instant::now();
                    let block = store.get(table, index, CachePolicy::default(), &mut StoreLocalStatistic::default())
                        .await.unwrap();
                    let mut iter = BlockIterator::new(block);
                    iter.seek_to_first();
                    assert_eq!(iter.key(), FullKey::decode(key));
                    issued.elapsed()
                });
            }
        }
        let lanes = scans.len();
        assert_eq!(jobs, lanes);
        let ((rows, scan_time), mut point_times) =
            tokio::time::timeout(Duration::from_secs(60), async {
                replay::event_marker("cohort_begin");
                let begin = Instant::now();
                start.set(begin).unwrap();
                launch.wait().await;
                let scan = async {
                    let rows: usize = join_all(scans).await.into_iter().map(Result::unwrap).sum();
                    (rows, begin.elapsed())
                };
                tokio::join!(scan, join_all(points))
            }).await.unwrap();
        replay::event_marker("cohort_end");
        stop_sampling.send(()).unwrap();
        let (samples, g_full_with_waiters, p_active_full) = sample.await.unwrap();
        println!("depth_samples={samples} G128_with_waiters={g_full_with_waiters} P32_active={p_active_full}");
        assert_eq!(rows, OBJECTS * ROWS_PER_OBJECT);
        assert_eq!(point_times.len(), OBJECTS * POINTS_PER_OBJECT);
        point_times.sort_unstable();
        let p99 = point_times[(point_times.len() * 99).div_ceil(100) - 1];
        let gets = read_op.get_sample_count() - reads_before;
        let bytes = object_metrics.read_bytes.get() - bytes_before;
        assert!(gets > 0 && bytes > 0);
        for family in registry.gather() {
            let name = family.name();
            for metric in family.get_metric() {
                let label = |key, value| metric.get_label().iter()
                    .any(|entry| entry.name() == key && entry.value() == value);
                if name == "object_store_read_scheduler_events"
                    && label("event", "range_fallback_requests")
                {
                    assert_eq!(metric.get_counter().value(), 0.0);
                }
                if name == "object_store_read_outcomes" && label("stage", "attempt")
                    && !label("outcome", "ok")
                {
                    assert_eq!(metric.get_counter().value(), 0.0);
                }
            }
        }
        println!("objects={OBJECTS} G={G} P={P} lanes={lanes} rows={rows} probes={} scan_ms={} point_p99_us={} completed_gets={gets} bytes={bytes}",
            point_times.len(), scan_time.as_millis(), p99.as_micros());
    });
    drop(runtime);
    replay::flush(&trace).unwrap();
}
