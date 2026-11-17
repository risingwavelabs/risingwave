// Copyright 2026 RisingWave Labs
// SPDX-License-Identifier: Apache-2.0

//! Bounded, opt-in read-ahead of contiguous memory/disk-cache misses.
//! Each cold run starts immediately; consuming a preceding run never starts the next one.
//! Direct ranges do not participate in Foyer single-flight. Positive cache hints
//! use cache-only lookups with a demand-read fallback if the hint becomes stale.

use std::ops::Range;

use risingwave_object_store::object::ObjectResult;

use super::*;

enum Part {
    Cached(BlockHolder),
    CacheLookup(usize, foyer::HybridGet<SstableBlockIndex, Box<Block>>),
    Read(Range<usize>, PendingRead),
}

struct PendingRead(tokio::task::JoinHandle<ObjectResult<Bytes>>);

impl Drop for PendingRead {
    fn drop(&mut self) {
        // These tasks belong exclusively to this iterator, not to a shared cache producer.
        self.0.abort();
    }
}

pub(crate) struct ColdRangePrefetch {
    parts: VecDeque<Part>,
    decoded: VecDeque<BlockHolder>,
    start: usize,
    // Includes pinned hits and the larger of compressed/decoded bytes for cold blocks.
    // Tasks retain the reservation until cancellation has actually been observed.
    _tracker: Arc<MemoryUsageTracker>,
}

impl ColdRangePrefetch {
    pub(crate) async fn next(
        &mut self,
        store: &SstableStore,
        sst: &Sstable,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<Option<BlockHolder>> {
        if let Some(block) = self.decoded.pop_front() {
            return Ok(Some(block));
        }
        let Some(part) = self.parts.pop_front() else {
            return Ok(None);
        };
        match part {
            Part::Cached(block) => {
                stats.cache_data_block_total += 1;
                Ok(Some(block))
            }
            Part::CacheLookup(idx, lookup) => {
                match lookup.await.map_err(HummockError::foyer_error)? {
                    Some(entry) => {
                        stats.cache_data_block_total += 1;
                        // A cache-only lookup may join another caller's external fetch.
                        if entry.source() == foyer::Source::Outer {
                            stats.cache_data_block_miss += 1;
                        }
                        Ok(Some(BlockHolder::from_hybrid_cache_entry(entry)))
                    }
                    // A positive index hint is not a reservation. Eviction/collision must
                    // never turn a missing block into a hole or leave it waiting for a plug.
                    None => {
                        store.read_metrics.cold_range_prefetch_hint_misses.inc();
                        store.get(sst, idx, policy, stats).await.map(Some)
                    }
                }
            }
            Part::Read(range, mut read) => {
                let data = (&mut read.0).await.map_err(|error| {
                    HummockError::other(format!("cold-range read task: {error}"))
                })??;
                let expected = sst.meta.block_metas[range.clone()]
                    .iter()
                    .map(|meta| meta.len as usize)
                    .sum::<usize>();
                if data.len() != expected {
                    return Err(ObjectError::internal("unexpected cold-range read length").into());
                }
                let CachePolicy::Fill(hint) = policy else {
                    unreachable!("cold ranges require cache-fill policy");
                };
                let mut offset = 0;
                for idx in range {
                    let meta = &sst.meta.block_metas[idx];
                    let end = offset + meta.len as usize;
                    // Decode on consumption, like Range, rather than moving decoding onto IO workers.
                    let block = Block::decode_with_copy(
                        data.slice(offset..end),
                        meta.uncompressed_size as usize,
                        true,
                    )?;
                    let hint = if idx == self.start { hint } else { Hint::Low };
                    let entry = store.block_cache.insert_with_properties(
                        SstableBlockIndex {
                            sst_id: sst.id,
                            block_idx: idx as _,
                        },
                        Box::new(block),
                        HybridCacheProperties::default().with_hint(hint),
                    );
                    self.decoded
                        .push_back(BlockHolder::from_hybrid_cache_entry(entry));
                    offset = end;
                }
                Ok(self.decoded.pop_front())
            }
        }
    }
}

impl SstableStore {
    pub(crate) fn prefetch_cold_ranges(
        &self,
        sst: &Sstable,
        start: usize,
        scan_end: usize,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> Option<ColdRangePrefetch> {
        fail_point!("disable_block_cache", |_| None);
        if !matches!(policy, CachePolicy::Fill(_))
            || self.prefetch_buffer_usage.load(Ordering::Acquire) > self.prefetch_buffer_capacity
        {
            return None;
        }
        let config = self.blockwise_prefetch;
        let metas = &sst.meta.block_metas;
        let mut end = start;
        let mut bytes = 0usize;
        for meta in metas
            .iter()
            .take(scan_end.min(start.saturating_add(config.max_blocks.get())))
            .skip(start)
        {
            let next = bytes.saturating_add(meta.len.max(meta.uncompressed_size) as usize);
            if next > config.max_bytes.get() {
                break;
            }
            end += 1;
            bytes = next;
        }
        // Keep single-block reads on the ordinary get path, including Foyer sharing.
        if end <= start + 1 {
            return None;
        }
        let tracker = Arc::new(MemoryUsageTracker::new(
            self.prefetch_buffer_usage.clone(),
            bytes,
        ));
        let mut parts = VecDeque::new();
        let mut cold_start = start;
        for idx in start..end {
            let key = SstableBlockIndex {
                sst_id: sst.id,
                block_idx: idx as _,
            };
            let cached = if let Some(entry) = self.block_cache.memory().get(&key) {
                // Pin actual memory entries rather than trusting a presence check.
                Some(Part::Cached(BlockHolder::from_hybrid_cache_entry(entry)))
            } else if self.block_cache.is_hybrid() && self.block_cache.contains(&key) {
                // Foyer starts the cache lookup now. Do not await disk IO while planning
                // later cold runs; a stale positive hint has an explicit fallback above.
                Some(Part::CacheLookup(idx, self.block_cache.get(&key)))
            } else {
                None
            };
            if let Some(cached) = cached {
                self.start_cold_range(sst, cold_start..idx, &tracker, &mut parts, stats);
                parts.push_back(cached);
                cold_start = idx + 1;
            }
        }
        self.start_cold_range(sst, cold_start..end, &tracker, &mut parts, stats);
        // Count only gaps bracketed by two submitted cold runs in this window.
        // Index hints are deliberately counted as hints, not verified disk hits.
        let mut previous_end = None;
        let mut runs = 0;
        for part in &parts {
            if let Part::Read(blocks, _) = part {
                if let Some(previous_end) = previous_end {
                    let gap_bytes: usize = metas[previous_end..blocks.start]
                        .iter()
                        .map(|meta| meta.len as usize)
                        .sum();
                    self.read_metrics
                        .cold_range_prefetch_gap_bytes
                        .observe(gap_bytes as f64);
                    self.read_metrics
                        .cold_range_prefetch_gap_blocks
                        .observe((blocks.start - previous_end) as f64);
                }
                previous_end = Some(blocks.end);
                runs += 1;
            }
        }
        self.read_metrics
            .cold_range_prefetch_window_runs
            .observe(runs as f64);
        Some(ColdRangePrefetch {
            parts,
            decoded: VecDeque::new(),
            start,
            _tracker: tracker,
        })
    }

    fn start_cold_range(
        &self,
        sst: &Sstable,
        blocks: Range<usize>,
        tracker: &Arc<MemoryUsageTracker>,
        parts: &mut VecDeque<Part>,
        stats: &mut StoreLocalStatistic,
    ) {
        if blocks.is_empty() {
            return;
        }
        let first = &sst.meta.block_metas[blocks.start];
        let last = &sst.meta.block_metas[blocks.end - 1];
        let range = first.offset as usize..(last.offset as usize + last.len as usize);
        let path = self.get_sst_data_path(sst.id);
        let object = self.store.clone();
        let tracker = tracker.clone();
        stats.cache_data_prefetch_count += 1;
        stats.cache_data_prefetch_block_count += blocks.len() as u64;
        self.read_metrics
            .cold_range_prefetch_read_bytes
            .observe(range.len() as f64);
        // Spawn while constructing the window, before awaiting any read or consuming any row.
        let task = tokio::spawn(async move {
            let _tracker = tracker;
            object.read(&path, range).await
        });
        parts.push_back(Part::Read(blocks, PendingRead(task)));
    }
}

#[cfg(test)]
mod tests {
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
        let object = Arc::new(ObjectStoreImpl::InMem(
            backend
                .clone()
                .monitored(metrics.clone(), Arc::new(ObjectStoreConfig::default())),
        ));
        let mut store = mock_sstable_store_with_object_store(object).await;
        Arc::get_mut(&mut store).unwrap().read_metrics = Arc::new(HummockStateStoreMetrics::new(
            &registry,
            risingwave_common::config::MetricLevel::Info,
        ));
        Arc::get_mut(&mut store)
            .unwrap()
            .blockwise_prefetch
            .max_blocks = 16.try_into().unwrap();
        Arc::get_mut(&mut store)
            .unwrap()
            .blockwise_prefetch
            .max_bytes = (1024 * 1024).try_into().unwrap();
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

    fn physical_inflight(registry: &Registry) -> f64 {
        registry
            .gather()
            .iter()
            .filter(|f| f.name() == "object_store_read_in_flight")
            .flat_map(|f| f.get_metric())
            .filter(|m| {
                m.get_label()
                    .iter()
                    .any(|l| l.name() == "stage" && l.value() == "physical")
            })
            .map(|m| m.get_gauge().as_ref().unwrap().value())
            .sum()
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
                runs
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
                cold_bytes as f64
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
        Arc::get_mut(&mut store)
            .unwrap()
            .blockwise_prefetch
            .max_blocks = 3.try_into().unwrap();
        for start in [0, 3, 6] {
            let mut window = store
                .prefetch_cold_ranges(
                    &sst,
                    start,
                    9,
                    CachePolicy::default(),
                    &mut StoreLocalStatistic::default(),
                )
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
        for disk in [false, true] {
            let (store, sst, backend, _, registry, _dir) = setup(disk).await;
            seed_hot(&store, &sst, 3, disk).await;
            backend.set_read_delay_for_test(Duration::from_secs(30));
            let window = store
                .prefetch_cold_ranges(
                    &sst,
                    0,
                    7,
                    CachePolicy::default(),
                    &mut StoreLocalStatistic::default(),
                )
                .unwrap();
            assert!(store.get_prefetch_memory_usage() > 0);
            // Neither cold run has completed, and no iterator block has been consumed.
            tokio::time::timeout(Duration::from_secs(2), async {
                while physical_inflight(&registry) != 2. {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("both sides of the hot block must start independently");
            drop(window);
            tokio::time::timeout(Duration::from_secs(2), async {
                while physical_inflight(&registry) != 0. || store.get_prefetch_memory_usage() != 0 {
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
        for stale in [false, true] {
            let (store, sst, _, metrics, _, _dir) = setup(true).await;
            seed_hot(&store, &sst, 3, true).await;
            let physical = metrics.operation_size.with_label_values(&["read"]);
            let before = physical.get_sample_count();
            let before_bytes = physical.get_sample_sum();
            let mut stats = StoreLocalStatistic::default();
            let mut window = store
                .prefetch_cold_ranges(&sst, 0, 7, CachePolicy::default(), &mut stats)
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
                    window.next(&store, &sst, CachePolicy::default(), &mut stats),
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
                let mut iter = BlockIterator::new(block);
                iter.seek_to_first();
                assert_eq!(
                    iter.key(),
                    FullKey::decode(&sst.meta.block_metas[idx].smallest_key)
                );
            }
            assert!(
                window
                    .next(&store, &sst, CachePolicy::default(), &mut stats)
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
}
