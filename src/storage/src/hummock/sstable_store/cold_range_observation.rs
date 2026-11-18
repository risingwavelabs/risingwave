// Copyright 2026 RisingWave Labs
// SPDX-License-Identifier: Apache-2.0

//! Diagnostic metadata only. Never used to serve, wait for, or suppress a read.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use risingwave_hummock_sdk::HummockSstableObjectId;

#[derive(Default)]
pub(super) struct BufferedRanges {
    ranges: Mutex<HashMap<HummockSstableObjectId, Vec<Arc<BufferedRange>>>>,
}

struct BufferedRange {
    // Advance only after decoding and optional cache insertion have succeeded.
    start: AtomicUsize,
    end: usize,
}

pub(super) struct BufferedRangeGuard {
    index: Arc<BufferedRanges>,
    sst_id: HummockSstableObjectId,
    range: Arc<BufferedRange>,
}

impl BufferedRanges {
    pub(super) fn register(
        self: &Arc<Self>,
        sst_id: HummockSstableObjectId,
        range: Range<usize>,
    ) -> BufferedRangeGuard {
        let range = Arc::new(BufferedRange {
            start: AtomicUsize::new(range.start),
            end: range.end,
        });
        self.ranges
            .lock()
            .unwrap()
            .entry(sst_id)
            .or_default()
            .push(range.clone());
        BufferedRangeGuard {
            index: self.clone(),
            sst_id,
            range,
        }
    }

    // Block-aligned reads are either fully present or absent. Count a block once,
    // even when several windows retain it. No SST IDs become metric labels.
    pub(super) fn contains(&self, sst_id: HummockSstableObjectId, block: &Range<usize>) -> bool {
        self.ranges
            .lock()
            .unwrap()
            .get(&sst_id)
            .is_some_and(|ranges| {
                ranges.iter().any(|range| {
                    range.start.load(Ordering::Relaxed) <= block.start && block.end <= range.end
                })
            })
    }
}

impl BufferedRangeGuard {
    pub(super) fn consumed_to(&self, offset: usize) {
        self.range.start.store(offset, Ordering::Relaxed);
    }

    pub(super) fn estimated_overhead() -> usize {
        // Metadata/Arc/hash-entry allowance per possible run; allocator overhead is not an RSS bound.
        size_of::<Self>() + size_of::<BufferedRange>() + 8 * size_of::<usize>()
    }
}

impl Drop for BufferedRangeGuard {
    fn drop(&mut self) {
        let mut index = self.index.ranges.lock().unwrap();
        if let Some(ranges) = index.get_mut(&self.sst_id) {
            ranges.retain(|range| !Arc::ptr_eq(range, &self.range));
            if ranges.is_empty() {
                index.remove(&self.sst_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observations_are_scoped_to_sst_consumption_and_last_window() {
        let index = Arc::new(BufferedRanges::default());
        let first = index.register(1.into(), 0..30);
        let duplicate = index.register(1.into(), 0..30);
        assert!(!index.contains(2.into(), &(10..20)));
        assert!(!index.contains(1.into(), &(30..40)));
        first.consumed_to(20);
        assert!(index.contains(1.into(), &(10..20)));
        drop(duplicate);
        assert!(!index.contains(1.into(), &(10..20)));
        assert!(index.contains(1.into(), &(20..30)));
        drop(first);
        assert!(index.ranges.lock().unwrap().is_empty());
    }
}
