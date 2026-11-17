// Local integration probe: the pinned real Foyer and the modified object-store library.
// This exercises their public boundary, not the complete SST iterator or S3 stack.
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    HybridGetOrFetch, PsyncIoEngineConfig, Source,
};
use prometheus::Registry;
use risingwave_common::config::storage::ReadSchedulerConfig;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{
    InMemObjectStore, ObjectStore, ObjectStoreConfig, ObjectStoreImpl, ReadPlug,
};

type Cache = HybridCache<u64, Vec<u8>>;

async fn store(dedicated: bool) -> (Arc<ObjectStoreImpl>, Arc<ObjectStoreMetrics>) {
    let registry = Registry::new();
    let metrics = Arc::new(ObjectStoreMetrics::new(&registry));
    let object = Arc::new(
        ObjectStoreImpl::InMem(InMemObjectStore::for_test().monitored(
            metrics.clone(),
            Arc::new(ObjectStoreConfig {
                read_scheduler: ReadSchedulerConfig {
                    use_dedicated_io_runtime: dedicated,
                    ..Default::default()
                },
                ..Default::default()
            }),
        ))
        .with_read_coalescing(),
    );
    object
        .upload("file", Bytes::from_static(b"abcdef"))
        .await
        .unwrap();
    (object, metrics)
}

// Mirror current SstableStore registration without decoding SST blocks.
fn register(
    cache: &Cache,
    store: &Arc<ObjectStoreImpl>,
    plug: &ReadPlug,
    key: u64,
) -> HybridGetOrFetch<u64, Vec<u8>> {
    let plug = (!(cache.is_hybrid() && cache.contains(&key))).then_some(plug);
    let token = plug.map(ReadPlug::token);
    let store = store.clone();
    let fetch_range = key as usize..key as usize + 1;
    cache.get_or_fetch(&key, || async move {
        store
            .read_with_plug("file", fetch_range, token)
            .await
            .map(|b| b.to_vec())
    })
}
fn gets(metrics: &ObjectStoreMetrics) -> u64 {
    metrics
        .operation_size
        .with_label_values(&["read"])
        .get_sample_count()
}
async fn memory_cache() -> Cache {
    HybridCacheBuilder::new()
        .memory(1 << 20)
        .storage()
        .build()
        .await
        .unwrap()
}
async fn correctness() {
    let (object, metrics) = store(false).await;
    let cache = memory_cache().await;
    // Cold neighbors are submitted only by the Foyer producers and merge once.
    let plug = object.read_plug(2).unwrap();
    let a = register(&cache, &object, &plug, 0);
    let b = register(&cache, &object, &plug, 1);
    drop(plug);
    assert_eq!(a.await.unwrap().value(), b"a");
    assert_eq!(b.await.unwrap().value(), b"b");
    assert_eq!(gets(&metrics), 1);
    // Warm hits must never issue new external reads or wait for plug closure.
    let plug = object.read_plug(2).unwrap();
    let a = register(&cache, &object, &plug, 0);
    assert!(!a.need_await());
    assert_eq!(a.await.unwrap().source(), Source::Memory);
    assert_eq!(gets(&metrics), 1);
    drop(plug);
    // A confirmed hot neighbor is excluded: the remaining cold read needs no timer.
    cache.memory().remove(&0);
    let plug = object.read_plug(2).unwrap();
    let a = register(&cache, &object, &plug, 0);
    let b = register(&cache, &object, &plug, 1);
    drop(plug);
    assert_eq!(a.await.unwrap().value(), b"a");
    assert_eq!(b.await.unwrap().source(), Source::Memory);
    assert_eq!(gets(&metrics), 2);
    cache.close().await.unwrap();
    println!("PASS memory hit, adjacent cold merge, mixed hot/cold");

    let dir = tempfile::tempdir().unwrap();
    let device = FsDeviceBuilder::new(dir.path())
        .with_capacity(16 << 20)
        .build()
        .unwrap();
    let cache: Cache = HybridCacheBuilder::new()
        .memory(1 << 20)
        .storage()
        .with_io_engine_config(PsyncIoEngineConfig::new())
        .with_engine_config(
            BlockEngineConfig::new(device)
                .with_block_size(1 << 20)
                .with_buffer_pool_size(4 << 20),
        )
        .build()
        .await
        .unwrap();
    drop(cache.storage_writer(0).insert(vec![b'a']).unwrap());
    cache.storage().wait().await;
    assert!(cache.contains(&0));
    assert!(!cache.memory().contains(&0));
    let before = gets(&metrics);
    let plug = object.read_plug(2).unwrap();
    let a = register(&cache, &object, &plug, 0);
    assert_eq!(a.await.unwrap().source(), Source::Disk);
    assert_eq!(gets(&metrics), before);
    cache.memory().remove(&0);
    // Delete after the positive hint but before the current-thread producer runs.
    let a = register(&cache, &object, &plug, 0);
    cache.storage().delete(&0);
    let a = tokio::time::timeout(Duration::from_secs(5), a)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(a.value(), b"a");
    assert_eq!(a.source(), Source::Outer);
    assert_eq!(gets(&metrics), before + 1);
    drop(a);
    drop(plug);
    // A follower of a disk-only get can supply the external fallback on a miss.
    let get = cache.get(&2);
    let plug = object.read_plug(2).unwrap();
    let follower = register(&cache, &object, &plug, 2);
    assert!(follower.need_await());
    drop(plug);
    assert_eq!(get.await.unwrap().unwrap().value(), b"c");
    assert_eq!(follower.await.unwrap().value(), b"c");
    assert_eq!(gets(&metrics), before + 2);
    cache.close().await.unwrap();
    println!("PASS disk hit, stale positive hint, disk-get follower supplies fallback");
}

async fn measure_missing_neighbor(dedicated: bool) {
    let (object, metrics) = store(dedicated).await;
    let cache = memory_cache().await;
    let mut elapsed = Vec::new();
    for _ in 0..100 {
        cache.memory().remove(&0);
        cache.memory().remove(&1);
        let (release, gate) = tokio::sync::oneshot::channel();
        let remote = object.clone();
        let leader = cache.get_or_fetch(&1, || async move {
            gate.await.unwrap();
            remote.read("file", 1..2).await.map(|b| b.to_vec())
        });
        let plug = object.read_plug(2).unwrap();
        let a = register(&cache, &object, &plug, 0);
        let follower = register(&cache, &object, &plug, 1);
        assert!(follower.need_await());
        // Dropping this consumer must preserve the producer while a follower remains.
        drop(leader);
        let started = Instant::now();
        drop(plug);
        assert_eq!(a.await.unwrap().value(), b"a");
        elapsed.push(started.elapsed().as_secs_f64() * 1e6);
        release.send(()).unwrap();
        assert_eq!(follower.await.unwrap().value(), b"b");
    }
    // Exactly one external producer per key, despite follower/cancelled-consumer roles.
    assert_eq!(gets(&metrics), 200);
    elapsed.sort_by(f64::total_cmp);
    println!(
        "MEASURE scenario=shared_missing dedicated={dedicated} n=100 GETs={} caller_us[p50,p95,p99]=[{:.1},{:.1},{:.1}]",
        gets(&metrics),
        elapsed[49],
        elapsed[94],
        elapsed[98]
    );
    cache.close().await.unwrap();
}
async fn measure_ready_neighbors(dedicated: bool) {
    let (object, metrics) = store(dedicated).await;
    let cache = memory_cache().await;
    let mut elapsed = Vec::new();
    for _ in 0..100 {
        cache.memory().remove(&0);
        cache.memory().remove(&1);
        let plug = object.read_plug(2).unwrap();
        let a = register(&cache, &object, &plug, 0);
        let b = register(&cache, &object, &plug, 1);
        let started = Instant::now();
        drop(plug);
        assert_eq!(a.await.unwrap().value(), b"a");
        assert_eq!(b.await.unwrap().value(), b"b");
        elapsed.push(started.elapsed().as_secs_f64() * 1e6);
    }
    assert!((100..=200).contains(&gets(&metrics)));
    elapsed.sort_by(f64::total_cmp);
    println!(
        "MEASURE scenario=ready_neighbors dedicated={dedicated} n=100 GETs={} caller_us[p50,p95,p99]=[{:.1},{:.1},{:.1}]",
        gets(&metrics),
        elapsed[49],
        elapsed[94],
        elapsed[98]
    );
    cache.close().await.unwrap();
}

async fn measure_plug(dedicated: bool, cross_runtime: bool, remote: &tokio::runtime::Handle) {
    let (object, metrics) = store(dedicated).await;
    let cache = if cross_runtime {
        remote.spawn(memory_cache()).await.unwrap()
    } else {
        memory_cache().await
    };
    let expected_runtime = if cross_runtime {
        remote.id()
    } else {
        tokio::runtime::Handle::current().id()
    };
    cache
        .get_or_fetch(&77, || async move {
            assert_eq!(tokio::runtime::Handle::current().id(), expected_runtime);
            Ok::<_, std::io::Error>(vec![0])
        })
        .await
        .unwrap();
    // All scenarios use the same Foyer producers and immediate in-memory backend.
    // Delay inside the second producer models interleaved fallback readiness.
    for (scenario, yields) in [
        ("point", 0),
        ("single_cold_window", 0),
        ("single_slot", 0),
        ("pair_ready", 0),
        ("pair_yield1", 1),
        ("pair_yield8", 8),
        ("pair_yield1_with_point", 1),
    ] {
        let mut elapsed = Vec::new();
        let mut demand_elapsed = Vec::new();
        let mut point_elapsed = Vec::new();
        let mut physical = 0;
        for iteration in 0..550 {
            cache.memory().remove(&0);
            cache.memory().remove(&1);
            cache.memory().remove(&2);
            let before = gets(&metrics);
            let start = Instant::now();
            let plug = (scenario != "point").then(|| {
                object
                    .read_plug(if scenario == "single_slot" { 1 } else { 8 })
                    .unwrap()
            });
            let token = plug.as_ref().map(ReadPlug::token);
            let store = object.clone();
            let first = cache.get_or_fetch(&0, || async move {
                store
                    .read_with_plug("file", 0..1, token)
                    .await
                    .map(|b| b.to_vec())
            });
            let second = if scenario.starts_with("pair") {
                let token = plug.as_ref().map(ReadPlug::token);
                let store = object.clone();
                Some(cache.get_or_fetch(&1, || async move {
                    for _ in 0..yields {
                        tokio::task::yield_now().await;
                    }
                    store
                        .read_with_plug("file", 1..2, token)
                        .await
                        .map(|b| b.to_vec())
                }))
            } else {
                None
            };
            drop(plug);
            let point = if scenario.ends_with("with_point") {
                let store = object.clone();
                let started = Instant::now();
                let response = cache.get_or_fetch(&2, || async move {
                    store.read("file", 4..5).await.map(|b| b.to_vec())
                });
                Some(tokio::spawn(async move {
                    assert_eq!(response.await.unwrap().value(), b"e");
                    started.elapsed().as_secs_f64() * 1e6
                }))
            } else {
                None
            };
            assert_eq!(first.await.unwrap().value(), b"a");
            let demand_us = start.elapsed().as_secs_f64() * 1e6;
            if let Some(second) = second {
                assert_eq!(second.await.unwrap().value(), b"b");
            }
            let total_us = start.elapsed().as_secs_f64() * 1e6;
            let point_us = if let Some(point) = point {
                Some(point.await.unwrap())
            } else {
                None
            };
            if iteration >= 50 {
                elapsed.push(total_us);
                demand_elapsed.push(demand_us);
                if let Some(point_us) = point_us {
                    point_elapsed.push(point_us);
                }
                physical += gets(&metrics) - before;
            }
        }
        elapsed.sort_by(f64::total_cmp);
        demand_elapsed.sort_by(f64::total_cmp);
        point_elapsed.sort_by(f64::total_cmp);
        println!(
            "DEMAND scenario={scenario} dedicated={dedicated} cross_runtime={cross_runtime} us[p50,p95,p99]=[{:.2},{:.2},{:.2}]",
            demand_elapsed[249], demand_elapsed[474], demand_elapsed[494]
        );
        if !point_elapsed.is_empty() {
            println!(
                "MIXED_POINT dedicated={dedicated} cross_runtime={cross_runtime} us[p50,p95,p99]=[{:.2},{:.2},{:.2}]",
                point_elapsed[249], point_elapsed[474], point_elapsed[494]
            );
        }
        println!(
            "PLUG scenario={scenario} dedicated={dedicated} cross_runtime={cross_runtime} n=500 GETs={physical} us[p50,p95,p99]=[{:.2},{:.2},{:.2}]",
            elapsed[249], elapsed[474], elapsed[494]
        );
    }
    cache.close().await.unwrap();
}

fn main() {
    assert!(
        std::env::args().len() == 1,
        "candidate policies were removed; run without options"
    );
    println!("PROBE public_foyer_boundary immediate_backend warmup=50 samples=500");
    let remote = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(30), async {
                correctness().await;
                for dedicated in [false, true] {
                    measure_missing_neighbor(dedicated).await;
                    measure_ready_neighbors(dedicated).await;
                    for cross_runtime in [false, true] {
                        measure_plug(dedicated, cross_runtime, remote.handle()).await;
                    }
                }
            })
            .await
            .unwrap();
        });
}
