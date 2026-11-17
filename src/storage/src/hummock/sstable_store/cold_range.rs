// Copyright 2026 RisingWave Labs
// SPDX-License-Identifier: Apache-2.0

//! Bounded, opt-in read-ahead of contiguous memory/disk-cache misses.
//! Each cold run starts immediately; consuming a preceding run never starts the next one.
//! Multi-block direct ranges do not participate in Foyer single-flight.
//! Singleton Fill segments register ordinary shared fetches eagerly. Positive cache hints
//! use cache-only lookups with a demand-read fallback if the hint becomes stale.
//! NotFill reuses cache hits but owns fetched blocks; Disable bypasses cache lookups
//! and reads the whole bounded window without filling the cache.

use std::ops::Range;

use risingwave_object_store::object::ObjectResult;

use super::*;

// Planning may pin existing memory entries, but never starts IO or Foyer fetches.
// Missing disk hints are resolved only after the final prefix has been admitted.
enum PlannedBlock {
    Cached(BlockHolder),
    CacheLookup,
    Cold,
}

enum Part {
    Cached(BlockHolder),
    CacheLookup(usize, foyer::HybridGet<SstableBlockIndex, Box<Block>>),
    Single(usize, HummockResult<BlockResponse>),
    Read(Range<usize>, PendingRead),
}

struct ReadOutput {
    data: Bytes,
    observation: Option<BufferedRangeGuard>,
}

struct PendingRead(tokio::task::JoinHandle<ObjectResult<ReadOutput>>);

impl Drop for PendingRead {
    fn drop(&mut self) {
        // These tasks belong exclusively to this iterator, not to a shared cache producer.
        self.0.abort();
    }
}

// Retain the compressed run, but decode/fill only blocks actually consumed.
struct BufferedRun {
    data: Bytes,
    blocks: Range<usize>,
    offset: usize,
    observation: Option<BufferedRangeGuard>,
}

pub(crate) struct ColdRangePrefetch {
    parts: VecDeque<Part>,
    buffered: Option<BufferedRun>,
    start: usize,
    // Charge raw buffers, pinned cache results, one direct-decode peak and bookkeeping.
    // Delivered blocks belong to the caller/Foyer; this is not a query-wide RSS limit.
    // Private read tasks retain the reservation until cancellation is observed.
    _tracker: Arc<MemoryUsageTracker>,
}

impl ColdRangePrefetch {
    fn planned_reservation_bytes(
        metas: &[BlockMeta],
        blocks: &[PlannedBlock],
        planning_capacity: usize,
        policy: CachePolicy,
    ) -> Option<usize> {
        let overhead = size_of::<Part>()
            .checked_add(size_of::<SstableBlockIndex>())?
            .checked_add(BufferedRangeGuard::estimated_overhead())?;
        let mut bytes = size_of::<Self>()
            .checked_add(size_of::<MemoryUsageTracker>())?
            .checked_add(2 * size_of::<usize>())? // Arc reference counts.
            .checked_add(planning_capacity.checked_mul(size_of::<PlannedBlock>())?)?;
        let mut decode_peak = 0;
        for (idx, (meta, block)) in metas.iter().zip(blocks).enumerate() {
            bytes = bytes.checked_add(overhead)?;
            if let PlannedBlock::Cached(holder) = block {
                // These entries are already decoded and can all remain pinned together.
                bytes = bytes.checked_add(holder.estimated_memory_weight())?;
                continue;
            }
            let decoded = Block::estimated_decode_memory_weight(
                meta.len.max(meta.uncompressed_size) as usize,
            )?;
            bytes = bytes.checked_add(meta.len as usize)?;
            let shared_singleton = matches!(policy, CachePolicy::Fill(_))
                && matches!(block, PlannedBlock::Cold)
                && (idx == 0 || !matches!(blocks[idx - 1], PlannedBlock::Cold))
                && (idx + 1 == blocks.len() || !matches!(blocks[idx + 1], PlannedBlock::Cold));
            if matches!(block, PlannedBlock::CacheLookup) || shared_singleton {
                // Cache lookups and shared singleton producers may complete concurrently
                // and keep all their decoded results alive before consumption.
                bytes = bytes.checked_add(decoded)?;
            } else {
                // Direct runs retain encoded bytes but decode only one block at a time.
                decode_peak = decode_peak.max(decoded);
            }
        }
        bytes.checked_add(decode_peak)
    }

    #[cfg(test)]
    fn reservation_bytes(metas: &[BlockMeta]) -> Option<usize> {
        let blocks: Vec<_> = metas.iter().map(|_| PlannedBlock::Cold).collect();
        Self::planned_reservation_bytes(metas, &blocks, blocks.capacity(), CachePolicy::default())
    }

    pub(crate) async fn next(
        &mut self,
        store: &SstableStore,
        sst: &Sstable,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<Option<BlockHolder>> {
        if self.buffered.is_some() {
            return self.decode_next(store, sst, policy).map(Some);
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
            Part::Single(_, response) => {
                let block = response?.wait().await?;
                stats.cache_data_block_total += 1;
                if matches!(block.entry(), BlockEntry::HybridCache(entry) if entry.source() == foyer::Source::Outer)
                {
                    stats.cache_data_block_miss += 1;
                }
                Ok(Some(block))
            }
            Part::Read(range, mut read) => {
                let output = (&mut read.0).await.map_err(|error| {
                    HummockError::other(format!("cold-range read task: {error}"))
                })??;
                let expected = sst.meta.block_metas[range.clone()]
                    .iter()
                    .map(|meta| meta.len as usize)
                    .sum::<usize>();
                if output.data.len() != expected {
                    return Err(ObjectError::internal("unexpected cold-range read length").into());
                }
                self.buffered = Some(BufferedRun {
                    data: output.data,
                    blocks: range,
                    offset: 0,
                    observation: output.observation,
                });
                self.decode_next(store, sst, policy).map(Some)
            }
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.parts.is_empty() && self.buffered.is_none()
    }

    fn decode_next(
        &mut self,
        store: &SstableStore,
        sst: &Sstable,
        policy: CachePolicy,
    ) -> HummockResult<BlockHolder> {
        let run = self.buffered.as_mut().expect("buffered cold run");
        let idx = run.blocks.start;
        let meta = &sst.meta.block_metas[idx];
        let end = run.offset + meta.len as usize;
        // Copy uncompressed blocks too: a returned block must not retain the entire run.
        let block = Block::decode_with_copy(
            run.data.slice(run.offset..end),
            meta.uncompressed_size as usize,
            true,
        )?;
        let holder = if let CachePolicy::Fill(hint) = policy {
            let hint = if idx == self.start { hint } else { Hint::Low };
            let entry = store.block_cache.insert_with_properties(
                SstableBlockIndex {
                    sst_id: sst.id,
                    block_idx: idx as _,
                },
                Box::new(block),
                HybridCacheProperties::default().with_hint(hint),
            );
            BlockHolder::from_hybrid_cache_entry(entry)
        } else {
            BlockHolder::from_owned_block(Box::new(block))
        };
        if let Some(observation) = &run.observation {
            observation.consumed_to(meta.offset as usize + meta.len as usize);
        }
        run.offset = end;
        run.blocks.start += 1;
        if run.blocks.is_empty() {
            self.buffered = None;
        }
        Ok(holder)
    }
}

impl SstableStore {
    // Cheap admission check before constructing the iterator's boxed async future.
    pub(crate) fn can_prefetch_cold_ranges(
        &self,
        sst: &Sstable,
        start: usize,
        scan_end: usize,
    ) -> bool {
        let config = self.blockwise_prefetch;
        start < scan_end.min(sst.meta.block_metas.len()).saturating_sub(1)
            && config.max_blocks.get() > 1
            && sst.meta.block_metas[start..start + 2]
                .iter()
                .map(|meta| meta.len.max(meta.uncompressed_size) as usize)
                .fold(0usize, usize::saturating_add)
                <= config.max_bytes.get()
    }

    pub(crate) async fn prefetch_cold_ranges(
        &self,
        sst: &Sstable,
        start: usize,
        scan_end: usize,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> Option<ColdRangePrefetch> {
        fail_point!("disable_block_cache", |_| None);
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
        let requested_end = end;
        let mut plan: Vec<_> = (start..end)
            .map(|idx| {
                let key = SstableBlockIndex {
                    sst_id: sst.id,
                    block_idx: idx as _,
                };
                if policy == CachePolicy::Disable {
                    PlannedBlock::Cold
                } else if let Some(entry) = self.block_cache.memory().get(&key) {
                    PlannedBlock::Cached(BlockHolder::from_hybrid_cache_entry(entry))
                } else if self.block_cache.is_hybrid() && self.block_cache.contains(&key) {
                    PlannedBlock::CacheLookup
                } else {
                    PlannedBlock::Cold
                }
            })
            .collect();
        let tracker = loop {
            // Do not wait or discard batching immediately when the desired window is
            // too large. Try smaller prefixes, each with an atomic reservation.
            if plan.len() < 2 {
                self.read_metrics.cold_range_prefetch_budget_denied.inc();
                return None;
            }
            end = start + plan.len();
            if let Some(tracker) = ColdRangePrefetch::planned_reservation_bytes(
                &metas[start..end],
                &plan,
                plan.capacity(),
                policy,
            )
            .and_then(|bytes| {
                MemoryUsageTracker::try_new(
                    self.prefetch_buffer_usage.clone(),
                    bytes,
                    self.prefetch_buffer_capacity,
                )
            }) {
                break Arc::new(tracker);
            }
            // Dropping excluded cache hits releases their pins before trying again.
            plan.pop();
        };
        if end < requested_end {
            self.read_metrics.cold_range_prefetch_budget_shrunk.inc();
        }
        let mut window = ColdRangePrefetch {
            parts: VecDeque::with_capacity(end - start),
            buffered: None,
            start,
            _tracker: tracker,
        };
        let mut cold_start = start;
        for (offset, planned) in plan.into_iter().enumerate() {
            let idx = start + offset;
            let cached = match planned {
                PlannedBlock::Cached(block) => Some(Part::Cached(block)),
                PlannedBlock::CacheLookup => Some(Part::CacheLookup(
                    idx,
                    self.block_cache.get(&SstableBlockIndex {
                        sst_id: sst.id,
                        block_idx: idx as _,
                    }),
                )),
                PlannedBlock::Cold => None,
            };
            if let Some(cached) = cached {
                self.start_cold_range(sst, cold_start..idx, policy, &mut window, stats)
                    .await;
                window.parts.push_back(cached);
                cold_start = idx + 1;
            }
        }
        self.start_cold_range(sst, cold_start..end, policy, &mut window, stats)
            .await;
        // Count only gaps bracketed by two submitted cold runs in this window.
        // Index hints are deliberately counted as hints, not verified disk hits.
        let mut previous_end = None;
        let mut runs = 0;
        for part in &window.parts {
            let blocks = match part {
                Part::Read(blocks, _) => blocks.clone(),
                Part::Single(idx, _) => *idx..*idx + 1,
                _ => continue,
            };
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
        self.read_metrics
            .cold_range_prefetch_window_runs
            .observe(runs as f64);
        Some(window)
    }

    async fn start_cold_range(
        &self,
        sst: &Sstable,
        blocks: Range<usize>,
        policy: CachePolicy,
        window: &mut ColdRangePrefetch,
        stats: &mut StoreLocalStatistic,
    ) {
        if blocks.is_empty() {
            return;
        }
        if blocks.len() == 1
            && let CachePolicy::Fill(hint) = policy
        {
            let idx = blocks.start;
            let hint = if idx == window.start { hint } else { Hint::Low };
            // Fill registers a Foyer producer without awaiting IO. Register every run
            // while planning, so a hot gap does not serialize subsequent cold reads.
            let response = self
                .get_block_response(sst, idx, CachePolicy::Fill(hint))
                .await;
            window.parts.push_back(Part::Single(idx, response));
            self.read_metrics.cold_range_prefetch_single_blocks.inc();
            return;
        }
        let first = &sst.meta.block_metas[blocks.start];
        let last = &sst.meta.block_metas[blocks.end - 1];
        let range = first.offset as usize..(last.offset as usize + last.len as usize);
        let path = self.get_sst_data_path(sst.id);
        let object = self.store.clone();
        let tracker = window._tracker.clone();
        stats.cache_data_prefetch_count += 1;
        stats.cache_data_prefetch_block_count += blocks.len() as u64;
        self.read_metrics
            .cold_range_prefetch_read_bytes
            .observe(range.len() as f64);
        // Spawn while constructing the window, before awaiting any read or consuming any row.
        let observations = self.buffered_cold_ranges.clone();
        let sst_id = sst.id;
        let task = tokio::spawn(async move {
            let _tracker = tracker;
            let data = object.read(&path, range.clone()).await?;
            // Register on IO completion, including completed tasks not yet consumed.
            // Invalid short responses are not reusable buffered data.
            let observation =
                (data.len() == range.len()).then(|| observations.register(sst_id, range));
            Ok(ReadOutput { data, observation })
        });
        window
            .parts
            .push_back(Part::Read(blocks, PendingRead(task)));
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
            while physical_inflight(&registry) != 2.0 {
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
        Arc::get_mut(&mut store)
            .unwrap()
            .blockwise_prefetch
            .max_blocks = 1.try_into().unwrap();
        assert!(!store.can_prefetch_cold_ranges(&sst, 0, 2));
        Arc::get_mut(&mut store)
            .unwrap()
            .blockwise_prefetch
            .max_blocks = 16.try_into().unwrap();
        let first = &sst.meta.block_metas[0];
        Arc::get_mut(&mut store)
            .unwrap()
            .blockwise_prefetch
            .max_bytes = (first.len.max(first.uncompressed_size) as usize)
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

    // Run explicitly to compare decode delivery; timing is diagnostic, not a CI assertion.
    #[tokio::test]
    #[ignore]
    async fn cold_range_delivery_probe() {
        use std::time::Instant;

        use crate::hummock::test_utils::{gen_test_sstable, test_key_of};
        use crate::hummock::value::HummockValue;

        let (store, _, backend, metrics, _, _dir) = setup(false).await;
        let mut opts = default_builder_opt_for_test();
        opts.block_capacity = 32 * 1024;
        let (sst, _) = gen_test_sstable(
            opts,
            20,
            (0..8192).map(|i| (test_key_of(i), HummockValue::put(vec![i as u8; 256]))),
            store.clone(),
        )
        .await;
        assert!(sst.meta.block_metas.len() >= 16);
        for delay_ms in [0, 2] {
            backend.set_read_delay_for_test(Duration::from_millis(delay_ms));
            for consume in [1, 16] {
                let mut first = Vec::new();
                let mut delivery = Vec::new();
                let mut total = Vec::new();
                for round in 0..35 {
                    store.clear_block_cache().await.unwrap();
                    let physical = metrics.operation_size.with_label_values(&["read"]);
                    let before = physical.get_sample_count();
                    let mut stats = StoreLocalStatistic::default();
                    let start = Instant::now();
                    let mut window = store
                        .prefetch_cold_ranges(&sst, 0, 16, CachePolicy::default(), &mut stats)
                        .await
                        .unwrap();
                    // Isolate delivery CPU from IO, while also measuring their total.
                    if let Some(Part::Read(_, read)) = window.parts.front() {
                        while !read.0.is_finished() {
                            tokio::task::yield_now().await;
                        }
                    } else {
                        panic!("expected a contiguous cold range");
                    }
                    let ready = Instant::now();
                    let block = window
                        .next(&store, &sst, CachePolicy::default(), &mut stats)
                        .await
                        .unwrap()
                        .unwrap();
                    std::hint::black_box(&block);
                    let first_us = start.elapsed().as_micros();
                    let delivery_us = ready.elapsed().as_micros();
                    drop(block);
                    for _ in 1..consume {
                        std::hint::black_box(
                            window
                                .next(&store, &sst, CachePolicy::default(), &mut stats)
                                .await
                                .unwrap()
                                .unwrap(),
                        );
                    }
                    drop(window);
                    let total_us = start.elapsed().as_micros();
                    assert_eq!(physical.get_sample_count() - before, 1);
                    assert_eq!(store.get_prefetch_memory_usage(), 0);
                    if round >= 5 {
                        first.push(first_us);
                        delivery.push(delivery_us);
                        total.push(total_us);
                    }
                }
                first.sort_unstable();
                delivery.sort_unstable();
                total.sort_unstable();
                println!(
                    "delivery_probe delay_ms={delay_ms} consume={consume} first_us={} ready_to_first_us={} total_us={} samples={}",
                    first[15],
                    delivery[15],
                    total[15],
                    first.len()
                );
            }
        }
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
            let required =
                ColdRangePrefetch::reservation_bytes(&sst.meta.block_metas[..16]).unwrap();
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
            while physical_inflight(&registry) != 2.0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(windows);
        tokio::time::timeout(Duration::from_secs(2), async {
            while store.get_prefetch_memory_usage() != 0 || physical_inflight(&registry) != 0.0 {
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
                while physical_inflight(&registry)
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
}
