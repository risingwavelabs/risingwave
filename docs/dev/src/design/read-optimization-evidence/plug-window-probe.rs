// Copyright 2026 RisingWave Labs
// SPDX-License-Identifier: Apache-2.0

// Public Foyer boundary diagnostic. No SST decoding and no synthetic backend delay.
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use foyer::{HybridCache, HybridCacheBuilder, HybridGetOrFetch};
use risingwave_common::config::storage::ReadSchedulerConfig;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{
    InMemObjectStore, ObjectStore, ObjectStoreConfig, ObjectStoreImpl,
};

const SIZE: usize = 64 * 1024;
type Cache = HybridCache<u64, Vec<u8>>;
async fn cache() -> Cache {
    HybridCacheBuilder::new()
        .memory(4 << 20)
        .storage()
        .build()
        .await
        .unwrap()
}
fn us(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1e6
}

fn main() {
    let arm = std::env::var("RW_PLUG_ARM").unwrap_or("P".into());
    assert!(matches!(arm.as_str(), "P" | "B"));
    let cross = std::env::var("RW_PLUG_CROSS").map_or(true, |v| v == "1");
    let samples: usize = std::env::var("RW_PLUG_SAMPLES").map_or(1000, |v| v.parse().unwrap());
    let remote = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        tokio::time::timeout(Duration::from_secs(90), async {
            let registry = prometheus::Registry::new();
            let metrics = Arc::new(ObjectStoreMetrics::new(&registry));
            let object = Arc::new(ObjectStoreImpl::InMem(InMemObjectStore::for_test().monitored(metrics.clone(), Arc::new(ObjectStoreConfig {
                read_scheduler: ReadSchedulerConfig {
                    use_dedicated_io_runtime: true,
                    max_admitted_groups: 128.try_into().unwrap(),
                    max_inflight_reads: 32.try_into().unwrap(),
                    max_batch_requests: 16.try_into().unwrap(),
                    max_read_bytes: (16 * SIZE).try_into().unwrap(),
                    max_gap_bytes: 0,
                    reuse_inflight_reads: true,
                    ..Default::default()
                }, ..Default::default()
            })) ).with_read_coalescing());
            object.upload("file", Bytes::from((0u8..16).flat_map(|k| vec![k; SIZE]).collect::<Vec<_>>())).await.unwrap();
            object.upload("point", Bytes::from(vec![16; SIZE])).await.unwrap();
            let cache = if cross { remote.spawn(cache()).await.unwrap() } else { cache().await };
            let expected = if cross { remote.handle().id() } else { tokio::runtime::Handle::current().id() };
            cache.get_or_fetch(&99, || async move {
                assert_eq!(tokio::runtime::Handle::current().id(), expected);
                Ok::<_, std::io::Error>(vec![99])
            }).await.unwrap();
            let metric = metrics.operation_size.with_label_values(&["read"]);
            for case in ["natural16", "released16", "tail_yield8", "tail_after_head", "single_cold", "shared_missing", "shared_point", "independent_point"] {
                let mut gets = Vec::new(); let mut first = Vec::new(); let mut total = Vec::new(); let mut point_latencies = Vec::new();
                for trial in 0..samples+50 {
                    for key in 0..=16 { cache.memory().remove(&key); }
                    if matches!(case, "single_cold" | "shared_missing") {
                        let hot_start = if case == "single_cold" { 1 } else { 2 };
                        for key in hot_start..16 { drop(cache.insert(key, vec![key as u8; SIZE])); }
                    }
                    let (release, gate) = tokio::sync::watch::channel(false);
                    let leader = if case == "shared_missing" {
                        let mut gate = gate.clone(); let object = object.clone();
                        Some(cache.get_or_fetch(&1, || async move {
                            gate.changed().await.unwrap();
                            object.read("file", SIZE..2*SIZE).await.map(|b| b.to_vec())
                        }))
                    } else { None };
                    let before = metric.get_sample_count(); let start = Instant::now();
                    let plug = (arm == "P").then(|| object.read_plug(16).unwrap());
                    let mut responses: Vec<(u64, HybridGetOrFetch<u64, Vec<u8>>)> = Vec::new();
                    for key in (1..16).chain(std::iter::once(0)) {
                        let object = object.clone(); let token = plug.as_ref().map(|p| p.token()); let mut gate = gate.clone();
                        let response = cache.get_or_fetch(&key, || async move {
                            if case == "released16" || (case == "tail_after_head" && key >= 8) || (case == "shared_point" && key == 0) { gate.changed().await.unwrap(); }
                            if case == "tail_yield8" && key >= 8 { for _ in 0..8 { tokio::task::yield_now().await; } }
                            object.read_with_plug("file", key as usize*SIZE..(key as usize+1)*SIZE, token).await.map(|b| b.to_vec())
                        });
                        responses.push((key, response));
                    }
                    drop(plug);
                    if case == "released16" { release.send(true).unwrap(); }
                    let point = if matches!(case, "shared_point" | "independent_point") {
                        let point_start = Instant::now();
                        let shared = case == "shared_point";
                        let key = if shared { 0 } else { 16 };
                        let object = object.clone();
                        let response = cache.get_or_fetch(&key, || async move {
                            assert!(!shared, "shared point must keep the existing producer");
                            object.read("point", 0..SIZE).await.map(|b| b.to_vec())
                        });
                        if shared { assert!(response.need_await()); }
                        Some(tokio::spawn(async move {
                            let entry = response.await.unwrap();
                            assert_eq!(entry.value().as_slice(), vec![key as u8; SIZE]);
                            us(point_start)
                        }))
                    } else { None };
                    if case == "shared_point" { release.send(true).unwrap(); }
                    let (_, head) = responses.pop().unwrap();
                    let head = head.await.unwrap(); let first_us = us(start);
                    assert_eq!(head.value().as_slice(), vec![0; SIZE]);
                    if matches!(case, "tail_after_head" | "shared_missing") { release.send(true).unwrap(); }
                    for (key, response) in responses { assert_eq!(response.await.unwrap().value().as_slice(), vec![key as u8; SIZE]); }
                    if let Some(leader) = leader { assert_eq!(leader.await.unwrap().value().as_slice(), vec![1; SIZE]); }
                    let point_us = if let Some(point) = point { Some(point.await.unwrap()) } else { None };
                    let elapsed = us(start); let count = metric.get_sample_count()-before;
                    assert!((1..=17).contains(&count));
                    if case == "single_cold" { assert_eq!(count, 1); }
                    if case == "shared_missing" { assert_eq!(count, 2); }
                    if case == "tail_after_head" { assert!(count >= 2); }
                    if trial >= 50 { gets.push(count); first.push(first_us); total.push(elapsed); if let Some(t) = point_us { point_latencies.push(t); } }
                }
                println!("BOUNDARY {{\"arm\":\"{arm}\",\"cross\":{cross},\"case\":\"{case}\",\"gets\":{gets:?},\"first_us\":{first:?},\"total_us\":{total:?},\"point_us\":{point_latencies:?}}}");
            }
            cache.close().await.unwrap();
        }).await.unwrap();
    });
}
