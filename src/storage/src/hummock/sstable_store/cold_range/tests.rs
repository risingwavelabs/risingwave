// Copyright 2026 RisingWave Labs
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use foyer::{BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, PsyncIoEngineConfig};
use prometheus::Registry;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_object_store::object::{
    InMemObjectStore, ObjectStore, ObjectStoreConfig, ObjectStoreImpl,
};

use super::*;
use crate::hummock::iterator::test_utils::mock_sstable_store_with_object_store;
use crate::hummock::test_utils::{default_builder_opt_for_test, gen_default_test_sstable};
use crate::hummock::{BlockIterator, SstableStoreRef, TableHolder};
use crate::monitor::ObjectStoreMetrics;

async fn setup(
    disk: bool,
) -> (
    SstableStoreRef,
    TableHolder,
    InMemObjectStore,
    Arc<ObjectStoreMetrics>,
    Registry,
    tempfile::TempDir,
) {
    let registry = Registry::new();
    let metrics = Arc::new(ObjectStoreMetrics::new(&registry));
    let backend = InMemObjectStore::for_test();
    let object = Arc::new(
        ObjectStoreImpl::InMem(
            backend
                .clone()
                .monitored(metrics.clone(), Arc::new(ObjectStoreConfig::default())),
        )
        .with_read_coalescing(),
    );
    let mut store = mock_sstable_store_with_object_store(object).await;
    Arc::get_mut(&mut store).unwrap().read_metrics = Arc::new(HummockStateStoreMetrics::new(
        &registry,
        risingwave_common::config::MetricLevel::Info,
    ));
    Arc::get_mut(&mut store).unwrap().scan_prefetch.max_blocks = Some(16.try_into().unwrap());
    Arc::get_mut(&mut store).unwrap().scan_prefetch.max_bytes = (1024 * 1024).try_into().unwrap();
    let dir = tempfile::tempdir().unwrap();
    if disk {
        let device = FsDeviceBuilder::new(dir.path())
            .with_capacity(16 << 20)
            .build()
            .unwrap();
        Arc::get_mut(&mut store).unwrap().block_cache = HybridCacheBuilder::new()
            .memory(64 << 20)
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
    }
    let (sst, _) =
        gen_default_test_sstable(default_builder_opt_for_test(), 10, store.clone()).await;
    store.clear_block_cache().await.unwrap();
    (store, sst, backend, metrics, registry, dir)
}

#[tokio::test]
async fn metadata_and_oversized_scan_reach_the_scheduler() {
    use crate::hummock::{HummockIterator, SstableIterator, SstableIteratorReadOptions};
    use crate::store::PrefetchMode;

    let registry = Registry::new();
    let metrics = Arc::new(ObjectStoreMetrics::new(&registry));
    let mut config = ObjectStoreConfig::default();
    config.read_scheduler.max_read_bytes = 1.try_into().unwrap();
    let object =
        ObjectStoreImpl::InMem(InMemObjectStore::for_test().monitored(metrics, Arc::new(config)))
            .with_read_coalescing();
    let store = mock_sstable_store_with_object_store(Arc::new(object)).await;
    let (_, info) =
        gen_default_test_sstable(default_builder_opt_for_test(), 11, store.clone()).await;
    store.clear_meta_cache().await.unwrap();
    store.clear_block_cache().await.unwrap();
    let requests = |route: &str| {
        registry
            .gather()
            .iter()
            .filter(|family| family.name() == "object_store_read_requests")
            .flat_map(|family| family.get_metric())
            .filter(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|label| label.name() == "route" && label.value() == route)
            })
            .map(|metric| metric.get_counter().value())
            .sum::<f64>()
    };
    let before_meta = requests("scheduled_other");
    let sst = store
        .sstable(&info, &mut StoreLocalStatistic::default())
        .await
        .unwrap();
    assert_eq!(requests("scheduled_other") - before_meta, 1.0);

    let options = Arc::new(SstableIteratorReadOptions {
        prefetch: PrefetchMode::ColdRanges,
        cache_policy: CachePolicy::Disable,
        ..Default::default()
    });
    let mut iter = SstableIterator::new(sst, store.clone(), options, &info);
    let before_scan = requests("scheduled_oversized");
    iter.rewind().await.unwrap();
    let mut stats = StoreLocalStatistic::default();
    iter.collect_local_statistic(&mut stats);
    assert!(stats.cache_data_prefetch_block_count > 1);
    assert_eq!(requests("scheduled_oversized") - before_scan, 1.0);
    drop(iter);
    store.block_cache.close().await.unwrap();
}

#[tokio::test]
async fn missing_sst_does_not_repeat_a_cold_read() {
    use crate::hummock::{HummockIterator, SstableIterator, SstableIteratorReadOptions};
    use crate::store::PrefetchMode;

    let (store, _, backend, metrics, _, _dir) = setup(false).await;
    let (sst, info) =
        gen_default_test_sstable(default_builder_opt_for_test(), 11, store.clone()).await;
    store.clear_block_cache().await.unwrap();
    backend
        .delete(&store.get_sst_data_path(sst.id))
        .await
        .unwrap();
    let reads = metrics
        .operation_latency
        .with_label_values(&["mem", "read"]);
    for policy in [
        CachePolicy::default(),
        CachePolicy::NotFill,
        CachePolicy::Disable,
    ] {
        let options = Arc::new(SstableIteratorReadOptions {
            prefetch: PrefetchMode::ColdRanges,
            cache_policy: policy,
            ..Default::default()
        });
        let mut iter = SstableIterator::new(sst.clone(), store.clone(), options, &info);
        let before = reads.get_sample_count();
        assert!(iter.rewind().await.is_err());
        assert_eq!(reads.get_sample_count() - before, 1);
        assert_eq!(store.get_prefetch_memory_usage(), 0);
    }
    store.block_cache.close().await.unwrap();
}

#[tokio::test]
async fn parked_iterator_releases_budget_on_last_window_block() {
    use crate::hummock::{HummockIterator, SstableIterator, SstableIteratorReadOptions};
    use crate::store::PrefetchMode;
    let (mut store, _, _, _, _, _dir) = setup(false).await;
    let (sst, info) =
        gen_default_test_sstable(default_builder_opt_for_test(), 11, store.clone()).await;
    let required = ColdRangePrefetch::reservation_bytes(&sst.meta.block_metas[..2]).unwrap();
    let inner = Arc::get_mut(&mut store).unwrap();
    inner.scan_prefetch.max_blocks = Some(2.try_into().unwrap());
    inner.prefetch_buffer_capacity = required;
    let opts = Arc::new(SstableIteratorReadOptions {
        prefetch: PrefetchMode::ColdRanges,
        cache_policy: CachePolicy::Disable,
        ..Default::default()
    });
    let mut first = SstableIterator::new(sst.clone(), store.clone(), opts.clone(), &info);
    first.rewind().await.unwrap();
    assert_eq!(store.get_prefetch_memory_usage(), required);
    while first.value_meta().block_id == Some(0) {
        first.next().await.unwrap();
    }
    assert_eq!(first.value_meta().block_id, Some(1));
    assert_eq!(store.get_prefetch_memory_usage(), 0);
    let mut second = SstableIterator::new(sst.clone(), store.clone(), opts, &info);
    second.rewind().await.unwrap();
    assert_eq!(
        store.read_metrics.cold_range_prefetch_budget_denied.get(),
        0
    );
    assert_eq!(store.get_prefetch_memory_usage(), required);
    assert_eq!(
        first.key(),
        FullKey::decode(&sst.meta.block_metas[1].smallest_key)
    );
    drop(second);
    assert_eq!(store.get_prefetch_memory_usage(), 0);
}

#[tokio::test]
async fn cold_range_window_inherits_legacy_block_limit_unless_overridden() {
    for (legacy_limit, explicit_limit, expected) in [(1, None, 0), (2, None, 2), (16, Some(3), 3)] {
        let (mut store, sst, _, _, _, _dir) = setup(false).await;
        let inner = Arc::get_mut(&mut store).unwrap();
        inner.max_prefetch_block_number = legacy_limit;
        inner.scan_prefetch.max_blocks =
            explicit_limit.map(|n| std::num::NonZeroUsize::new(n).unwrap());
        let mut stats = StoreLocalStatistic::default();
        let window = store
            .prefetch_cold_ranges(
                &sst,
                0,
                sst.meta.block_metas.len(),
                CachePolicy::Disable,
                &mut stats,
            )
            .await;
        assert_eq!(window.is_some(), expected > 0);
        assert_eq!(stats.cache_data_prefetch_block_count, expected);
        drop(window);
        // Private IO tasks may still be observing cancellation.
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.get_prefetch_memory_usage() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

async fn seed_hot(store: &SstableStore, sst: &Sstable, idx: usize, disk: bool) {
    if disk {
        let (range, capacity) = sst.calculate_block_info(idx);
        let data = store
            .store
            .read(&store.get_sst_data_path(sst.id), range)
            .await
            .unwrap();
        let key = SstableBlockIndex {
            sst_id: sst.id,
            block_idx: idx as _,
        };
        drop(
            store
                .block_cache
                .storage_writer(key)
                .insert(Box::new(Block::decode(data, capacity).unwrap()))
                .unwrap(),
        );
        store.block_cache.storage().wait().await;
        assert!(!store.block_cache.memory().contains(&key));
        assert!(store.block_cache.contains(&key));
    } else {
        store
            .get(
                sst,
                idx,
                CachePolicy::default(),
                &mut StoreLocalStatistic::default(),
            )
            .await
            .unwrap();
    }
}

fn inflight(registry: &Registry, stage: &str) -> f64 {
    registry
        .gather()
        .iter()
        .filter(|f| f.name() == "object_store_read_in_flight")
        .flat_map(|f| f.get_metric())
        .filter(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "stage" && l.value() == stage)
        })
        .map(|m| m.get_gauge().as_ref().unwrap().value())
        .sum()
}

#[tokio::test]
async fn buffered_overlap_metrics_track_io_completion_consumption_and_drop() {
    for (policy, label) in [
        (CachePolicy::default(), "fill"),
        (CachePolicy::NotFill, "not_fill"),
        (CachePolicy::Disable, "disable"),
    ] {
        let (store, sst, _, metrics, _, _dir) = setup(false).await;
        let mut stats = StoreLocalStatistic::default();
        let policy_reads = store
            .read_metrics
            .cold_range_buffered_overlap_reads
            .with_label_values(&[label]);
        let policy_bytes = store
            .read_metrics
            .cold_range_buffered_overlap_bytes
            .with_label_values(&[label]);
        let (range, _) = sst.calculate_block_info(2);
        let mut window = store
            .prefetch_cold_ranges(&sst, 0, 7, policy, &mut stats)
            .await
            .unwrap();
        // This current-thread runtime has not polled the IO task yet.
        assert!(!store.buffered_cold_ranges.contains(sst.id, &range));
        let Some(Part::Read(_, read)) = window.parts.front() else {
            panic!("expected range");
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while !read.0.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // The head has not been consumed, but the task's result already owns bytes.
        assert!(store.buffered_cold_ranges.contains(sst.id, &range));
        let physical = metrics.operation_size.with_label_values(&["read"]);
        let before = physical.get_sample_count();
        if matches!(policy, CachePolicy::Fill(_)) {
            let first = store.get_block_response(&sst, 2, policy).await.unwrap();
            let follower = store.get_block_response(&sst, 2, policy).await.unwrap();
            assert_eq!(policy_reads.get(), 0);
            let (a, b) = tokio::join!(first.wait(), follower.wait());
            assert!(a.is_ok() && b.is_ok());
            // A memory hit and the Foyer follower are not external read attempts.
            store.get(&sst, 2, policy, &mut stats).await.unwrap();
        } else {
            store.get(&sst, 2, policy, &mut stats).await.unwrap();
        }
        assert_eq!(policy_reads.get(), 1);
        assert_eq!(policy_bytes.get(), range.len() as u64);
        assert_eq!(physical.get_sample_count() - before, 1);
        drop(
            window
                .next(&store, &sst, policy, &mut stats)
                .await
                .unwrap()
                .unwrap(),
        );
        store.clear_block_cache().await.unwrap();
        // Consumed block 0 is no longer classified as buffered, even without cache fill.
        store.get(&sst, 0, policy, &mut stats).await.unwrap();
        assert_eq!(policy_reads.get(), 1);
        drop(window);
        assert!(!store.buffered_cold_ranges.contains(sst.id, &range));
        store.get(&sst, 3, policy, &mut stats).await.unwrap();
        assert_eq!(policy_reads.get(), 1);
        store.block_cache.close().await.unwrap();
    }
}

#[tokio::test]
async fn buffered_overlap_metadata_drops_with_unconsumed_task_result() {
    let (store, sst, _, _, _, _dir) = setup(false).await;
    let window = store
        .prefetch_cold_ranges(
            &sst,
            0,
            7,
            CachePolicy::default(),
            &mut StoreLocalStatistic::default(),
        )
        .await
        .unwrap();
    let (range, _) = sst.calculate_block_info(0);
    let Some(Part::Read(_, read)) = window.parts.front() else {
        panic!("expected range");
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        while !read.0.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(store.buffered_cold_ranges.contains(sst.id, &range));
    drop(window);
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.buffered_cold_ranges.contains(sst.id, &range) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(store.get_prefetch_memory_usage(), 0);
    store.block_cache.close().await.unwrap();
}

#[tokio::test]
async fn split_singleton_joins_existing_producer_without_duplicate_io() {
    let (store, sst, _, metrics, _, _dir) = setup(false).await;
    seed_hot(&store, &sst, 1, false).await;
    let key = SstableBlockIndex {
        sst_id: sst.id,
        block_idx: 0,
    };
    let (range, capacity) = sst.calculate_block_info(0);
    let remote = store.store.clone();
    let path = store.get_sst_data_path(sst.id);
    let (release, gate) = tokio::sync::oneshot::channel();
    let leader = store.block_cache.get_or_fetch(&key, || async move {
        gate.await.unwrap();
        let data = remote.read(&path, range).await?;
        Ok::<_, HummockError>(Box::new(Block::decode(data, capacity)?))
    });
    let reads = metrics.operation_size.with_label_values(&["read"]);
    let before = reads.get_sample_count();
    let mut stats = StoreLocalStatistic::default();
    let mut window = store
        .prefetch_cold_ranges(&sst, 0, 2, CachePolicy::default(), &mut stats)
        .await
        .unwrap();
    assert!(matches!(window.parts.front(), Some(Part::Single(0, _))));
    release.send(()).unwrap();
    let leader = leader.await.unwrap();
    let block = window
        .next(&store, &sst, CachePolicy::default(), &mut stats)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(block.entry(), BlockEntry::HybridCache(entry) if std::ptr::eq(entry.value(), leader.value()))
    );
    assert_eq!(reads.get_sample_count() - before, 1);
    assert_eq!(stats.cache_data_prefetch_count, 0);
    assert_eq!(stats.cache_data_block_total, 1);
    drop(window);
    store.block_cache.close().await.unwrap();
}

#[tokio::test]
async fn split_singletons_register_eagerly_and_survive_window_drop_for_point_waiters() {
    let (store, sst, backend, metrics, registry, _dir) = setup(false).await;
    seed_hot(&store, &sst, 1, false).await;
    backend.set_read_delay_for_test(Duration::from_millis(100));
    let reads = metrics.operation_size.with_label_values(&["read"]);
    let before = reads.get_sample_count();
    let window = store
        .prefetch_cold_ranges(
            &sst,
            0,
            3,
            CachePolicy::default(),
            &mut StoreLocalStatistic::default(),
        )
        .await
        .unwrap();
    assert!(matches!(window.parts.front(), Some(Part::Single(0, _))));
    assert!(matches!(window.parts.back(), Some(Part::Single(2, _))));
    // No window consumption is necessary to start both cold sides.
    tokio::time::timeout(Duration::from_secs(2), async {
        while inflight(&registry, "physical") != 2.0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let point_a = store
        .get_block_response(&sst, 0, CachePolicy::default())
        .await
        .unwrap();
    let point_c = store
        .get_block_response(&sst, 2, CachePolicy::default())
        .await
        .unwrap();
    drop(window);
    let (a, c) = tokio::join!(point_a.wait(), point_c.wait());
    assert!(a.is_ok() && c.is_ok());
    assert_eq!(reads.get_sample_count() - before, 2);
    assert_eq!(store.get_prefetch_memory_usage(), 0);
    store.block_cache.close().await.unwrap();
}

#[tokio::test]
async fn cold_range_decodes_and_fills_only_consumed_blocks() {
    for policy in [
        CachePolicy::default(),
        CachePolicy::NotFill,
        CachePolicy::Disable,
    ] {
        let (store, sst, _, metrics, _, _dir) = setup(false).await;
        let path = store.get_sst_data_path(sst.id);
        let mut bytes = store.store.read(&path, ..).await.unwrap().to_vec();
        bytes[sst.meta.block_metas[1].offset as usize] ^= 1;
        store.store.upload(&path, bytes.into()).await.unwrap();
        let reads = metrics.operation_size.with_label_values(&["read"]);
        let before = reads.get_sample_count();
        let mut stats = StoreLocalStatistic::default();
        let mut window = store
            .prefetch_cold_ranges(&sst, 0, 16, policy, &mut stats)
            .await
            .unwrap();
        let reserved = store.get_prefetch_memory_usage();
        let block = window
            .next(&store, &sst, policy, &mut stats)
            .await
            .unwrap()
            .unwrap();
        let mut iter = BlockIterator::new(block);
        iter.seek_to_first();
        assert_eq!(
            iter.key(),
            FullKey::decode(&sst.meta.block_metas[0].smallest_key)
        );
        assert_eq!(reads.get_sample_count() - before, 1);
        assert!(window.buffered.is_some());
        assert_eq!(store.get_prefetch_memory_usage(), reserved);
        for idx in 0..16 {
            let key = SstableBlockIndex {
                sst_id: sst.id,
                block_idx: idx,
            };
            assert_eq!(
                store.block_cache.memory().contains(&key),
                idx == 0 && matches!(policy, CachePolicy::Fill(_))
            );
        }
        // Corruption in a later block is reported only when it is consumed.
        assert!(window.next(&store, &sst, policy, &mut stats).await.is_err());
        drop(window);
        assert_eq!(store.get_prefetch_memory_usage(), 0);
        // The caller's decoded head remains valid after releasing the raw range.
        assert!(iter.is_valid());
        store.block_cache.close().await.unwrap();
    }
}

#[tokio::test]
async fn cold_range_short_windows_skip_async_admission() {
    let (mut store, sst, _, _, _, _dir) = setup(false).await;
    assert!(!store.can_prefetch_cold_ranges(&sst, 0, 1));
    let end = sst.meta.block_metas.len();
    assert!(!store.can_prefetch_cold_ranges(&sst, end - 1, end));
    assert!(!store.can_prefetch_cold_ranges(&sst, end, end));
    assert!(store.can_prefetch_cold_ranges(&sst, 0, 2));
    Arc::get_mut(&mut store).unwrap().scan_prefetch.max_blocks = Some(1.try_into().unwrap());
    assert!(!store.can_prefetch_cold_ranges(&sst, 0, 2));
    Arc::get_mut(&mut store).unwrap().scan_prefetch.max_blocks = Some(16.try_into().unwrap());
    let first = &sst.meta.block_metas[0];
    Arc::get_mut(&mut store).unwrap().scan_prefetch.max_bytes =
        (first.len.max(first.uncompressed_size) as usize)
            .try_into()
            .unwrap();
    assert!(!store.can_prefetch_cold_ranges(&sst, 0, 2));
    assert!(
        store
            .prefetch_cold_ranges(
                &sst,
                0,
                2,
                CachePolicy::default(),
                &mut StoreLocalStatistic::default()
            )
            .await
            .is_none()
    );
    assert_eq!(store.get_prefetch_memory_usage(), 0);
    store.block_cache.close().await.unwrap();
}

#[tokio::test]
async fn cold_range_merge_scan_keeps_batching_with_fixed_budget() {
    use crate::hummock::iterator::{HummockIterator, MergeIterator};
    use crate::hummock::test_utils::{gen_test_sstable, test_key_of};
    use crate::hummock::value::HummockValue;
    use crate::hummock::{SstableIterator, SstableIteratorReadOptions};
    use crate::store::PrefetchMode;

    for compression in [
        crate::hummock::CompressionAlgorithm::None,
        crate::hummock::CompressionAlgorithm::Lz4,
        crate::hummock::CompressionAlgorithm::Zstd,
    ] {
        let (mut store, _, _, metrics, _, _dir) = setup(false).await;
        let mut tables = Vec::new();
        for (id, offset) in [(200, 4096), (201, 4096), (202, 0)] {
            let mut opts = default_builder_opt_for_test();
            opts.block_capacity = 32 * 1024;
            opts.compression_algorithm = compression;
            tables.push(
                gen_test_sstable(
                    opts,
                    id,
                    (offset..offset + 8192)
                        .map(|i| (test_key_of(i), HummockValue::put(vec![i as u8; 256]))),
                    store.clone(),
                )
                .await,
            );
        }
        store.clear_block_cache().await.unwrap();
        // Independent fixed budget: do not derive admission expectations from the
        // reservation formula being tested. All three raw windows total about 1.5 MiB.
        const CAPACITY: usize = 4 * 1024 * 1024;
        Arc::get_mut(&mut store).unwrap().prefetch_buffer_capacity = CAPACITY;
        let options = Arc::new(SstableIteratorReadOptions {
            prefetch: PrefetchMode::ColdRanges,
            ..Default::default()
        });
        let children = tables
            .iter()
            .map(|(sst, info)| {
                SstableIterator::new(sst.clone(), store.clone(), options.clone(), info)
            })
            .collect::<Vec<_>>();
        let mut iter = MergeIterator::new(children);
        let physical = metrics.operation_size.with_label_values(&["read"]);
        let before = physical.get_sample_count();
        iter.rewind().await.unwrap();
        let head_usage = store.get_prefetch_memory_usage();
        for i in 0..2048 {
            assert!(iter.is_valid());
            assert_eq!(iter.key(), test_key_of(i).to_ref());
            iter.next().await.unwrap();
            assert!(store.get_prefetch_memory_usage() <= CAPACITY);
        }
        let gets = physical.get_sample_count() - before;
        let denied = store.read_metrics.cold_range_prefetch_budget_denied.get();
        println!(
            "fixed_budget_merge compression={compression:?} capacity={CAPACITY} head_usage={head_usage} GETs={gets} denied={denied}"
        );
        drop(iter);
        assert_eq!(store.get_prefetch_memory_usage(), 0);
        store.block_cache.close().await.unwrap();
        assert_eq!(gets, 4, "two parked heads and two active 16-block windows");
        assert_eq!(denied, 0);
    }
}

#[tokio::test]
async fn cold_range_shrinks_to_available_budget_before_demand_fallback() {
    for policy in [
        CachePolicy::default(),
        CachePolicy::NotFill,
        CachePolicy::Disable,
    ] {
        let (mut store, sst, _, metrics, _, _dir) = setup(false).await;
        const CAPACITY: usize = 64 * 1024;
        Arc::get_mut(&mut store).unwrap().prefetch_buffer_capacity = CAPACITY;
        let physical = metrics.operation_size.with_label_values(&["read"]);
        let before = physical.get_sample_count();
        let mut stats = StoreLocalStatistic::default();
        let mut window = store
            .prefetch_cold_ranges(&sst, 0, 16, policy, &mut stats)
            .await
            .expect("a smaller multi-block window must fit");
        let blocks = stats.cache_data_prefetch_block_count;
        assert!((2..16).contains(&blocks));
        assert_eq!(
            store.read_metrics.cold_range_prefetch_budget_shrunk.get(),
            1
        );
        let mut max_decoded = 0;
        let raw: usize = sst.meta.block_metas[..blocks as usize]
            .iter()
            .map(|m| m.len as usize)
            .sum();
        for _ in 0..blocks {
            let block = window
                .next(&store, &sst, policy, &mut stats)
                .await
                .unwrap()
                .unwrap();
            max_decoded = max_decoded.max(block.estimated_memory_weight());
            assert!(store.get_prefetch_memory_usage() <= CAPACITY);
            assert!(store.get_prefetch_memory_usage() >= raw + max_decoded);
        }
        assert!(
            window
                .next(&store, &sst, policy, &mut stats)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(physical.get_sample_count() - before, 1);
        assert_eq!(
            store.read_metrics.cold_range_prefetch_budget_denied.get(),
            0
        );
        drop(window);
        assert_eq!(store.get_prefetch_memory_usage(), 0);
        store.block_cache.close().await.unwrap();
    }
}

#[tokio::test]
async fn cold_range_hot_window_uses_actual_pinned_weights() {
    for policy in [CachePolicy::default(), CachePolicy::NotFill] {
        let (mut store, sst, _, metrics, _, _dir) = setup(false).await;
        let mut pinned = 0;
        for idx in 0..16 {
            let block = store
                .get(
                    &sst,
                    idx,
                    CachePolicy::default(),
                    &mut StoreLocalStatistic::default(),
                )
                .await
                .unwrap();
            pinned += block.estimated_memory_weight();
        }
        const CAPACITY: usize = 128 * 1024;
        Arc::get_mut(&mut store).unwrap().prefetch_buffer_capacity = CAPACITY;
        let physical = metrics.operation_size.with_label_values(&["read"]);
        let before = physical.get_sample_count();
        let mut stats = StoreLocalStatistic::default();
        let mut window = store
            .prefetch_cold_ranges(&sst, 0, 16, policy, &mut stats)
            .await
            .unwrap();
        let reserved = store.get_prefetch_memory_usage();
        assert!((pinned..=CAPACITY).contains(&reserved));
        assert_eq!(window.parts.len(), 16);
        // Remove cache ownership; the window must retain the entries safely.
        store.clear_block_cache().await.unwrap();
        for _ in 0..16 {
            assert!(
                window
                    .next(&store, &sst, policy, &mut stats)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        assert_eq!(physical.get_sample_count(), before);
        assert_eq!(stats.cache_data_prefetch_count, 0);
        drop(window);
        assert_eq!(store.get_prefetch_memory_usage(), 0);
        store.block_cache.close().await.unwrap();
    }
}

#[tokio::test]
async fn cold_range_shrink_does_not_start_excluded_disk_lookups() {
    let (mut store, sst, _, metrics, _, _dir) = setup(true).await;
    seed_hot(&store, &sst, 15, true).await;
    let excluded = SstableBlockIndex {
        sst_id: sst.id,
        block_idx: 15,
    };
    let physical = metrics.operation_size.with_label_values(&["read"]);
    let before = physical.get_sample_count();
    Arc::get_mut(&mut store).unwrap().prefetch_buffer_capacity = 0;
    let mut stats = StoreLocalStatistic::default();
    assert!(
        store
            .prefetch_cold_ranges(&sst, 0, 16, CachePolicy::default(), &mut stats)
            .await
            .is_none()
    );
    tokio::task::yield_now().await;
    assert!(!store.block_cache.memory().contains(&excluded));
    assert_eq!(physical.get_sample_count(), before);
    Arc::get_mut(&mut store).unwrap().prefetch_buffer_capacity = 64 * 1024;
    let mut window = store
        .prefetch_cold_ranges(&sst, 0, 16, CachePolicy::default(), &mut stats)
        .await
        .unwrap();
    assert!((2..16).contains(&stats.cache_data_prefetch_block_count));
    while window
        .next(&store, &sst, CachePolicy::default(), &mut stats)
        .await
        .unwrap()
        .is_some()
    {}
    assert_eq!(physical.get_sample_count(), before + 1);
    assert!(!store.block_cache.memory().contains(&excluded));
    assert!(store.block_cache.contains(&excluded));
    drop(window);
    assert_eq!(store.get_prefetch_memory_usage(), 0);
    store.block_cache.close().await.unwrap();
}

#[tokio::test]
async fn cold_range_shared_results_keep_concurrent_decode_reservations() {
    let (store, _, _, _, _, _dir) = setup(false).await;
    let mut opts = default_builder_opt_for_test();
    opts.compression_algorithm = crate::hummock::CompressionAlgorithm::Lz4;
    let (sst, _) = gen_default_test_sstable(opts, 11, store.clone()).await;
    store.clear_block_cache().await.unwrap();
    for idx in (1..16).step_by(2) {
        seed_hot(&store, &sst, idx, false).await;
    }
    let mut stats = StoreLocalStatistic::default();
    let mut window = store
        .prefetch_cold_ranges(&sst, 0, 16, CachePolicy::default(), &mut stats)
        .await
        .unwrap();
    assert_eq!(
        window
            .parts
            .iter()
            .filter(|part| matches!(part, Part::Single(_, _)))
            .count(),
        8
    );
    let reserved = store.get_prefetch_memory_usage();
    let mut blocks = Vec::new();
    for _ in 0..16 {
        blocks.push(
            window
                .next(&store, &sst, CachePolicy::default(), &mut stats)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let actual = blocks
        .iter()
        .map(|block| block.estimated_memory_weight())
        .sum::<usize>();
    assert!(
        reserved >= actual,
        "cache results may all be alive together"
    );
    drop(window);
    assert_eq!(store.get_prefetch_memory_usage(), 0);
    store.block_cache.close().await.unwrap();
}

#[tokio::test]
async fn cold_ranges_respect_cache_policy_for_memory_and_disk_hits() {
    for disk in [false, true] {
        for policy in [
            CachePolicy::default(),
            CachePolicy::NotFill,
            CachePolicy::Disable,
        ] {
            let (store, sst, _, metrics, _, _dir) = setup(disk).await;
            seed_hot(&store, &sst, 3, disk).await;
            let physical = metrics.operation_size.with_label_values(&["read"]);
            let before = physical.get_sample_count();
            let before_bytes = physical.get_sample_sum();
            let mut stats = StoreLocalStatistic::default();
            let mut window = store
                .prefetch_cold_ranges(&sst, 0, 7, policy, &mut stats)
                .await
                .unwrap();
            for idx in 0..7 {
                let block = window
                    .next(&store, &sst, policy, &mut stats)
                    .await
                    .unwrap()
                    .unwrap();
                let uses_cache = matches!(policy, CachePolicy::Fill(_))
                    || (policy == CachePolicy::NotFill && idx == 3);
                assert_eq!(
                    matches!(block.entry(), BlockEntry::HybridCache(_)),
                    uses_cache
                );
                assert_eq!(matches!(block.entry(), BlockEntry::Owned(_)), !uses_cache);
                if let CachePolicy::Fill(hint) = policy
                    && idx != 3
                    && let BlockEntry::HybridCache(entry) = block.entry()
                {
                    assert_eq!(
                        entry.properties().hint(),
                        if idx == 0 { hint } else { Hint::Low }
                    );
                }
                let mut iter = BlockIterator::new(block);
                iter.seek_to_first();
                assert_eq!(
                    iter.key(),
                    FullKey::decode(&sst.meta.block_metas[idx].smallest_key)
                );
            }
            let bypass = policy == CachePolicy::Disable;
            assert_eq!(
                physical.get_sample_count() - before,
                if bypass { 1 } else { 2 }
            );
            assert_eq!(
                stats.cache_data_prefetch_block_count,
                if bypass { 7 } else { 6 }
            );
            let expected_bytes: usize = sst.meta.block_metas[..7]
                .iter()
                .enumerate()
                .filter(|(idx, _)| bypass || *idx != 3)
                .map(|(_, meta)| meta.len as usize)
                .sum();
            assert_eq!(
                (physical.get_sample_sum() - before_bytes) as usize,
                expected_bytes
            );
            assert_eq!(
                store
                    .read_metrics
                    .cold_range_prefetch_gap_bytes
                    .get_sample_count(),
                u64::from(!bypass)
            );
            if disk && bypass {
                // Disable must not even promote the disk hit into memory.
                assert!(!store.block_cache.memory().contains(&SstableBlockIndex {
                    sst_id: sst.id,
                    block_idx: 3
                }));
            }
            drop(window);
            assert_eq!(store.get_prefetch_memory_usage(), 0);
            store.block_cache.storage().wait().await;
            for idx in [0, 1, 2, 4, 5, 6] {
                let key = SstableBlockIndex {
                    sst_id: sst.id,
                    block_idx: idx,
                };
                let cached = store.block_cache.get(&key).await.unwrap();
                assert_eq!(cached.is_some(), matches!(policy, CachePolicy::Fill(_)));
            }
            store.block_cache.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn cold_ranges_disable_requires_object_even_when_all_blocks_are_cached() {
    for policy in [CachePolicy::NotFill, CachePolicy::Disable] {
        let (store, sst, _, _, _, _dir) = setup(false).await;
        for idx in 0..7 {
            seed_hot(&store, &sst, idx, false).await;
        }
        store.delete(sst.id).await.unwrap();
        let mut stats = StoreLocalStatistic::default();
        let mut window = store
            .prefetch_cold_ranges(&sst, 0, 7, policy, &mut stats)
            .await
            .unwrap();
        if policy == CachePolicy::Disable {
            assert!(window.next(&store, &sst, policy, &mut stats).await.is_err());
        } else {
            assert_eq!(stats.cache_data_prefetch_count, 0);
            for idx in 0..7 {
                let block = window
                    .next(&store, &sst, policy, &mut stats)
                    .await
                    .unwrap()
                    .unwrap();
                let mut iter = BlockIterator::new(block);
                iter.seek_to_first();
                assert_eq!(
                    iter.key(),
                    FullKey::decode(&sst.meta.block_metas[idx].smallest_key)
                );
            }
        }
        drop(window);
        assert_eq!(store.get_prefetch_memory_usage(), 0);
        store.block_cache.close().await.unwrap();
    }
}

#[tokio::test]
async fn cold_range_budget_rejects_before_io_and_preserves_full_cold_window() {
    for (policy, fits) in [
        CachePolicy::default(),
        CachePolicy::NotFill,
        CachePolicy::Disable,
    ]
    .into_iter()
    .flat_map(|policy| (0..3).map(move |fits| (policy, fits)))
    {
        let (mut store, sst, _, metrics, _, _dir) = setup(false).await;
        let required = ColdRangePrefetch::reservation_bytes(&sst.meta.block_metas[..16]).unwrap();
        Arc::get_mut(&mut store).unwrap().prefetch_buffer_capacity = match fits {
            0 => 0,
            1 => ColdRangePrefetch::reservation_bytes(&sst.meta.block_metas[..2]).unwrap() - 1,
            _ => required,
        };
        let physical = metrics.operation_size.with_label_values(&["read"]);
        let before = physical.get_sample_count();
        let mut stats = StoreLocalStatistic::default();
        let window = store
            .prefetch_cold_ranges(&sst, 0, 16, policy, &mut stats)
            .await;
        if fits < 2 {
            assert!(window.is_none());
            assert_eq!(store.get_prefetch_memory_usage(), 0);
            assert_eq!(stats.cache_data_prefetch_count, 0);
            assert_eq!(
                store.read_metrics.cold_range_prefetch_budget_denied.get(),
                1
            );
            tokio::task::yield_now().await;
            assert_eq!(physical.get_sample_count(), before);
            // The ordinary demand path still makes progress at zero prefetch capacity.
            store.get(&sst, 0, policy, &mut stats).await.unwrap();
            assert_eq!(physical.get_sample_count(), before + 1);
        } else {
            let mut window = window.unwrap();
            assert_eq!(stats.cache_data_prefetch_count, 1);
            assert_eq!(stats.cache_data_prefetch_block_count, 16);
            assert_eq!(store.get_prefetch_memory_usage(), required);
            let mut decoded_weight = 0;
            for _ in 0..16 {
                let block = window
                    .next(&store, &sst, policy, &mut stats)
                    .await
                    .unwrap()
                    .unwrap();
                decoded_weight = decoded_weight.max(block.estimated_memory_weight());
                assert_eq!(store.get_prefetch_memory_usage(), required);
            }
            let compressed: usize = sst.meta.block_metas[..16]
                .iter()
                .map(|meta| meta.len as usize)
                .sum();
            assert!(required >= compressed + decoded_weight);
            assert_eq!(physical.get_sample_count(), before + 1);
            assert_eq!(
                store.read_metrics.cold_range_prefetch_budget_denied.get(),
                0
            );
            drop(window);
            assert_eq!(store.get_prefetch_memory_usage(), 0);
        }
        store.block_cache.close().await.unwrap();
    }
}

#[tokio::test]
async fn cold_range_zero_budget_iterator_uses_demand_reads() {
    use crate::hummock::{HummockIterator, SstableIterator, SstableIteratorReadOptions};
    use crate::store::PrefetchMode;

    for policy in [
        CachePolicy::default(),
        CachePolicy::NotFill,
        CachePolicy::Disable,
    ] {
        let (mut store, _, _, metrics, _, _dir) = setup(false).await;
        let (sst, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 11, store.clone()).await;
        store.clear_block_cache().await.unwrap();
        Arc::get_mut(&mut store).unwrap().prefetch_buffer_capacity = 0;
        let physical = metrics.operation_size.with_label_values(&["read"]);
        let before = physical.get_sample_count();
        let mut iter = SstableIterator::new(
            sst.clone(),
            store.clone(),
            Arc::new(SstableIteratorReadOptions {
                prefetch: PrefetchMode::ColdRanges,
                cache_policy: policy,
                ..Default::default()
            }),
            &info,
        );
        for idx in [0, 1] {
            let key = FullKey::decode(&sst.meta.block_metas[idx].smallest_key);
            tokio::time::timeout(Duration::from_secs(2), iter.seek(key))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(iter.key(), key);
            assert_eq!(physical.get_sample_count(), before + idx as u64 + 1);
            assert_eq!(store.get_prefetch_memory_usage(), 0);
        }
        let mut stats = StoreLocalStatistic::default();
        iter.collect_local_statistic(&mut stats);
        assert_eq!(stats.cache_data_prefetch_count, 0);
        assert_eq!(
            store.read_metrics.cold_range_prefetch_budget_denied.get(),
            2
        );
        drop(iter);
        store.block_cache.close().await.unwrap();
    }
}

#[tokio::test]
async fn cold_range_budget_survives_abort_before_first_poll() {
    let (store, sst, _, metrics, _, _dir) = setup(false).await;
    let physical = metrics.operation_size.with_label_values(&["read"]);
    let before = physical.get_sample_count();
    let window = store
        .prefetch_cold_ranges(
            &sst,
            0,
            7,
            CachePolicy::default(),
            &mut StoreLocalStatistic::default(),
        )
        .await
        .unwrap();
    let reserved = store.get_prefetch_memory_usage();
    assert!(reserved > 0);
    drop(window);
    // Aborting a spawned task is asynchronous, even before it was first polled.
    assert_eq!(store.get_prefetch_memory_usage(), reserved);
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.get_prefetch_memory_usage() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(physical.get_sample_count(), before);
    store.block_cache.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_range_concurrent_windows_cannot_overbook_budget() {
    let (mut store, sst, backend, _, registry, _dir) = setup(false).await;
    let required = ColdRangePrefetch::reservation_bytes(&sst.meta.block_metas[..7]).unwrap();
    Arc::get_mut(&mut store).unwrap().prefetch_buffer_capacity = required * 2;
    backend.set_read_delay_for_test(Duration::from_secs(30));
    let barrier = Arc::new(tokio::sync::Barrier::new(9));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let store = store.clone();
        let sst = sst.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let window = store
                .prefetch_cold_ranges(
                    &sst,
                    0,
                    7,
                    CachePolicy::default(),
                    &mut StoreLocalStatistic::default(),
                )
                .await;
            assert!(store.get_prefetch_memory_usage() <= required * 2);
            window
        }));
    }
    barrier.wait().await;
    let mut windows = Vec::new();
    for task in tasks {
        if let Some(window) = task.await.unwrap() {
            windows.push(window);
        }
    }
    assert_eq!(windows.len(), 2);
    assert_eq!(store.get_prefetch_memory_usage(), required * 2);
    assert_eq!(
        store.read_metrics.cold_range_prefetch_budget_denied.get(),
        6
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        // Both windows submitted their reads. They may share one physical GET.
        while inflight(&registry, "logical") != 2.0 || inflight(&registry, "physical") < 1.0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(windows);
    tokio::time::timeout(Duration::from_secs(2), async {
        while store.get_prefetch_memory_usage() != 0
            || inflight(&registry, "logical") != 0.0
            || inflight(&registry, "physical") != 0.0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    store.block_cache.close().await.unwrap();
}

#[tokio::test]
async fn cold_range_metrics_count_only_internal_gaps() {
    for (hot, gaps, runs) in [
        (vec![], vec![], 1),
        (vec![0, 6], vec![], 1),
        (vec![3], vec![3..4], 2),
        (vec![2, 3], vec![2..4], 2),
        (vec![1, 3, 5], vec![1..2, 3..4, 5..6], 4),
        ((0..7).collect(), vec![], 0),
    ] {
        let (store, sst, _, _, _, _dir) = setup(false).await;
        for idx in hot.iter().copied() {
            seed_hot(&store, &sst, idx, false).await;
        }
        let mut window = store
            .prefetch_cold_ranges(
                &sst,
                0,
                7,
                CachePolicy::default(),
                &mut StoreLocalStatistic::default(),
            )
            .await
            .unwrap();
        let metrics = &store.read_metrics;
        assert_eq!(
            metrics.cold_range_prefetch_window_runs.get_sample_count(),
            1
        );
        assert_eq!(
            metrics.cold_range_prefetch_window_runs.get_sample_sum(),
            runs as f64
        );
        assert_eq!(
            metrics.cold_range_prefetch_read_bytes.get_sample_count(),
            if runs == 4 { 0 } else { runs }
        );
        assert_eq!(
            metrics.cold_range_prefetch_gap_bytes.get_sample_count(),
            gaps.len() as u64
        );
        assert_eq!(
            metrics.cold_range_prefetch_gap_blocks.get_sample_sum(),
            gaps.iter().map(|gap| gap.len()).sum::<usize>() as f64
        );
        let gap_bytes: u64 = gaps
            .into_iter()
            .flat_map(|gap| &sst.meta.block_metas[gap])
            .map(|meta| u64::from(meta.len))
            .sum();
        assert_eq!(
            metrics.cold_range_prefetch_gap_bytes.get_sample_sum(),
            gap_bytes as f64
        );
        let cold_bytes: u64 = (0..7)
            .filter(|idx| !hot.contains(idx))
            .map(|idx| u64::from(sst.meta.block_metas[idx].len))
            .sum();
        assert_eq!(
            metrics.cold_range_prefetch_read_bytes.get_sample_sum(),
            if runs == 4 { 0.0 } else { cold_bytes as f64 }
        );
        assert_eq!(
            metrics.cold_range_prefetch_single_blocks.get(),
            if runs == 4 { 4 } else { 0 }
        );
        // Metrics describe planning even before consuming the window.
        for _ in 0..7 {
            assert!(
                window
                    .next(
                        &store,
                        &sst,
                        CachePolicy::default(),
                        &mut StoreLocalStatistic::default()
                    )
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        drop(window);
        store.block_cache.close().await.unwrap();
    }
}

#[tokio::test]
async fn cold_range_metrics_do_not_count_window_boundaries_as_gaps() {
    let (mut store, sst, _, _, _, _dir) = setup(false).await;
    Arc::get_mut(&mut store).unwrap().scan_prefetch.max_blocks = Some(3.try_into().unwrap());
    for start in [0, 3, 6] {
        let mut window = store
            .prefetch_cold_ranges(
                &sst,
                start,
                9,
                CachePolicy::default(),
                &mut StoreLocalStatistic::default(),
            )
            .await
            .unwrap();
        for _ in 0..3 {
            assert!(
                window
                    .next(
                        &store,
                        &sst,
                        CachePolicy::default(),
                        &mut StoreLocalStatistic::default()
                    )
                    .await
                    .unwrap()
                    .is_some()
            );
        }
    }
    assert_eq!(
        store
            .read_metrics
            .cold_range_prefetch_window_runs
            .get_sample_count(),
        3
    );
    assert_eq!(
        store
            .read_metrics
            .cold_range_prefetch_window_runs
            .get_sample_sum(),
        3.0
    );
    assert_eq!(
        store
            .read_metrics
            .cold_range_prefetch_gap_bytes
            .get_sample_count(),
        0
    );
    store.block_cache.close().await.unwrap();
}

#[tokio::test]
async fn cold_ranges_submit_both_sides_before_consumption_and_cancel_on_drop() {
    for (disk, policy) in [false, true].into_iter().flat_map(|disk| {
        [
            CachePolicy::default(),
            CachePolicy::NotFill,
            CachePolicy::Disable,
        ]
        .map(move |policy| (disk, policy))
    }) {
        let (store, sst, backend, _, registry, _dir) = setup(disk).await;
        seed_hot(&store, &sst, 3, disk).await;
        backend.set_read_delay_for_test(Duration::from_secs(30));
        let window = store
            .prefetch_cold_ranges(&sst, 0, 7, policy, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        assert!(store.get_prefetch_memory_usage() > 0);
        // Neither cold run has completed, and no iterator block has been consumed.
        tokio::time::timeout(Duration::from_secs(2), async {
            while inflight(&registry, "physical")
                != if policy == CachePolicy::Disable {
                    1.0
                } else {
                    2.0
                }
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both sides of the hot block must start independently");
        drop(window);
        tokio::time::timeout(Duration::from_secs(2), async {
            while inflight(&registry, "physical") != 0. || store.get_prefetch_memory_usage() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping the window must abort private reads and release reservations");
        store.block_cache.close().await.unwrap();
    }
}

#[tokio::test]
async fn cold_ranges_preserve_disk_hits_and_fall_back_on_stale_index() {
    for (policy, stale) in [CachePolicy::default(), CachePolicy::NotFill]
        .into_iter()
        .flat_map(|policy| [false, true].map(move |stale| (policy, stale)))
    {
        let (store, sst, _, metrics, _, _dir) = setup(true).await;
        seed_hot(&store, &sst, 3, true).await;
        let physical = metrics.operation_size.with_label_values(&["read"]);
        let before = physical.get_sample_count();
        let before_bytes = physical.get_sample_sum();
        let mut stats = StoreLocalStatistic::default();
        let mut window = store
            .prefetch_cold_ranges(&sst, 0, 7, policy, &mut stats)
            .await
            .unwrap();
        if stale {
            // Current-thread runtime: the cache-only producer has not polled yet.
            store.block_cache.storage().delete(&SstableBlockIndex {
                sst_id: sst.id,
                block_idx: 3,
            });
        }
        for idx in 0..7 {
            let block = tokio::time::timeout(
                Duration::from_secs(2),
                window.next(&store, &sst, policy, &mut stats),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();
            if idx == 3 && !stale {
                assert!(
                    matches!(block.entry(), BlockEntry::HybridCache(e) if e.source() == foyer::Source::Disk)
                );
            }
            if policy == CachePolicy::NotFill && (idx != 3 || stale) {
                assert!(matches!(block.entry(), BlockEntry::Owned(_)));
                assert!(
                    store
                        .block_cache
                        .get(&SstableBlockIndex {
                            sst_id: sst.id,
                            block_idx: idx as _
                        })
                        .await
                        .unwrap()
                        .is_none()
                );
            }
            let mut iter = BlockIterator::new(block);
            iter.seek_to_first();
            assert_eq!(
                iter.key(),
                FullKey::decode(&sst.meta.block_metas[idx].smallest_key)
            );
        }
        assert!(
            window
                .next(&store, &sst, policy, &mut stats)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            physical.get_sample_count() - before,
            if stale { 3 } else { 2 }
        );
        assert_eq!(
            store.read_metrics.cold_range_prefetch_hint_misses.get(),
            u64::from(stale)
        );
        assert_eq!(
            store
                .read_metrics
                .cold_range_prefetch_gap_bytes
                .get_sample_count(),
            1
        );
        assert_eq!(
            store
                .read_metrics
                .cold_range_prefetch_gap_bytes
                .get_sample_sum(),
            sst.meta.block_metas[3].len as f64
        );
        let expected: usize = sst.meta.block_metas[..7]
            .iter()
            .enumerate()
            .filter(|(idx, _)| stale || *idx != 3)
            .map(|(_, meta)| meta.len as usize)
            .sum();
        assert_eq!(
            (physical.get_sample_sum() - before_bytes) as usize,
            expected
        );
        drop(window);
        assert_eq!(store.get_prefetch_memory_usage(), 0);
        store.block_cache.close().await.unwrap();
    }
}
