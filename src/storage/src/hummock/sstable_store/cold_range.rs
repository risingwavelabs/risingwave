// Copyright 2026 RisingWave Labs
// SPDX-License-Identifier: Apache-2.0

//! Bounded read-ahead of contiguous memory/disk-cache misses.
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
        observe_buffered_overlap: bool,
    ) -> Option<usize> {
        let overhead = size_of::<Part>()
            .checked_add(size_of::<SstableBlockIndex>())?
            .checked_add(if observe_buffered_overlap {
                BufferedRangeGuard::estimated_overhead()
            } else {
                0
            })?;
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
        Self::planned_reservation_bytes(
            metas,
            &blocks,
            blocks.capacity(),
            CachePolicy::default(),
            false,
        )
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
                    // never turn a missing block into a hole in the returned scan.
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
                let result = (&mut read.0)
                    .await
                    .map_err(|error| HummockError::other(format!("cold-range read task: {error}")))?
                    .and_then(|output| {
                        let expected = sst.meta.block_metas[range.clone()]
                            .iter()
                            .map(|meta| meta.len as usize)
                            .sum::<usize>();
                        if output.data.len() != expected {
                            return Err(ObjectError::read_length_mismatch(
                                "unexpected cold-range read length",
                            ));
                        }
                        Ok(output)
                    });
                let output = match result {
                    Err(error) if range.len() > 1 && error.is_read_range_error() => {
                        // A speculative neighbor can extend past EOF while the head is valid.
                        // Discard the window so the iterator retries the current block alone.
                        self.parts.clear();
                        return Ok(None);
                    }
                    other => other?,
                };
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
        let config = self.scan_prefetch;
        start < scan_end.min(sst.meta.block_metas.len()).saturating_sub(1)
            && config
                .max_blocks
                .map_or(self.max_prefetch_block_number, std::num::NonZeroUsize::get)
                > 1
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
        let config = self.scan_prefetch;
        let metas = &sst.meta.block_metas;
        let mut end = start;
        let mut bytes = 0usize;
        for meta in metas
            .iter()
            .take(
                scan_end.min(
                    start.saturating_add(
                        config
                            .max_blocks
                            .map_or(self.max_prefetch_block_number, std::num::NonZeroUsize::get),
                    ),
                ),
            )
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
                self.scan_prefetch.observe_buffered_overlap,
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
        let observations = self.buffered_overlap_index();
        let sst_id = sst.id;
        let task = tokio::spawn(async move {
            let _tracker = tracker;
            let data = object.read(&path, range.clone()).await?;
            // Register on IO completion, including completed tasks not yet consumed.
            // Invalid short responses are not reusable buffered data.
            let observation = observations
                .filter(|_| data.len() == range.len())
                .map(|index| index.register(sst_id, range));
            Ok(ReadOutput { data, observation })
        });
        window
            .parts
            .push_back(Part::Read(blocks, PendingRead(task)));
    }
}

#[cfg(test)]
mod tests;
