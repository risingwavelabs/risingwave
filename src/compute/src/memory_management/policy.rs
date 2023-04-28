// Copyright 2023 RisingWave Labs
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

use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use risingwave_batch::task::BatchManager;
use risingwave_common::util::epoch::Epoch;
use risingwave_stream::task::LocalStreamManager;

use super::{MemoryControl, MemoryControlStats};

/// `JemallocMemoryControl` is a memory control policy that uses jemalloc statistics to control. It
/// assumes that most memory is used by streaming engine and does memory control over LRU watermark
/// based on jemalloc statistics.
#[derive(Debug)]
pub struct JemallocMemoryControl {
    threshold_graceful: usize,
    threshold_aggressive: usize,
}

impl JemallocMemoryControl {
    const THRESHOLD_AGGRESSIVE: f64 = 0.9;
    const THRESHOLD_GRACEFUL: f64 = 0.7;

    pub fn new(total_memory: usize) -> Self {
        let threshold_graceful = (total_memory as f64 * Self::THRESHOLD_GRACEFUL) as usize;
        let threshold_aggressive = (total_memory as f64 * Self::THRESHOLD_AGGRESSIVE) as usize;
        Self {
            threshold_graceful,
            threshold_aggressive,
        }
    }
}

impl MemoryControl for JemallocMemoryControl {
    fn apply(
        &self,
        interval_ms: u32,
        prev_memory_stats: MemoryControlStats,
        _batch_manager: Arc<BatchManager>,
        _stream_manager: Arc<LocalStreamManager>,
        watermark_epoch: Arc<AtomicU64>,
    ) -> MemoryControlStats {
        let (jemalloc_allocated_mib, jemalloc_active_mib) = advance_jemalloc_epoch(
            prev_memory_stats.jemalloc_allocated_mib,
            prev_memory_stats.jemalloc_active_mib,
        );

        // Streaming memory control
        //
        // We calculate the watermark of the LRU cache, which provides hints for streaming executors
        // on cache eviction. Here we do the calculation based on jemalloc statistics.

        let (lru_watermark_step, lru_watermark_time_ms, lru_physical_now) = calculate_lru_watermark(
            jemalloc_allocated_mib,
            self.threshold_graceful,
            self.threshold_aggressive,
            interval_ms,
            prev_memory_stats,
        );

        set_lru_watermark_time_ms(watermark_epoch, lru_watermark_time_ms);

        MemoryControlStats {
            jemalloc_allocated_mib,
            jemalloc_active_mib,
            lru_watermark_step,
            lru_watermark_time_ms,
            lru_physical_now_ms: lru_physical_now,
        }
    }
}

fn advance_jemalloc_epoch(
    prev_jemalloc_allocated_mib: usize,
    prev_jemalloc_active_mib: usize,
) -> (usize, usize) {
    use tikv_jemalloc_ctl::{epoch as jemalloc_epoch, stats as jemalloc_stats};

    let jemalloc_epoch_mib = jemalloc_epoch::mib().unwrap();
    if let Err(e) = jemalloc_epoch_mib.advance() {
        tracing::warn!("Jemalloc epoch advance failed! {:?}", e);
    }

    let jemalloc_allocated_mib = jemalloc_stats::allocated::mib().unwrap();
    let jemalloc_active_mib = jemalloc_stats::active::mib().unwrap();
    (
        jemalloc_allocated_mib.read().unwrap_or_else(|e| {
            tracing::warn!("Jemalloc read allocated failed! {:?}", e);
            prev_jemalloc_allocated_mib
        }),
        jemalloc_active_mib.read().unwrap_or_else(|e| {
            tracing::warn!("Jemalloc read active failed! {:?}", e);
            prev_jemalloc_active_mib
        }),
    )
}

fn calculate_lru_watermark(
    cur_used_memory_bytes: usize,
    threshold_graceful: usize,
    threshold_aggressive: usize,
    interval_ms: u32,
    prev_memory_stats: MemoryControlStats,
) -> (u64, u64, u64) {
    let mut watermark_time_ms = prev_memory_stats.lru_watermark_time_ms;
    let last_step = prev_memory_stats.lru_watermark_step;
    let last_used_memory_bytes = prev_memory_stats.jemalloc_allocated_mib;

    // The watermark calculation works in the following way:
    //
    // 1. When the streaming memory usage is below the graceful threshold, we do not evict
    // any caches, and simply reset the step to 0.
    //
    // 2. When the memory usage is between the graceful and aggressive threshold:
    //   - If the last eviction memory usage decreases after last eviction, we set the eviction step
    //     to 1.
    //   - Otherwise, we set the step to last_step + 1.
    //
    // 3. When the memory usage exceeds the aggressive threshold:
    //   - If the memory usage decreases after the last eviction, we set the eviction step to
    //     last_step.
    //   - Otherwise, we set the step to last_step * 2.

    let mut step = if cur_used_memory_bytes < threshold_graceful {
        // Do not evict if the memory usage is lower than `threshold_graceful`
        0
    } else if cur_used_memory_bytes < threshold_aggressive {
        // Gracefully evict
        if last_used_memory_bytes > cur_used_memory_bytes {
            1
        } else {
            last_step + 1
        }
    } else if last_used_memory_bytes < cur_used_memory_bytes {
        // Aggressively evict
        if last_step == 0 {
            2
        } else {
            last_step * 2
        }
    } else {
        last_step
    };

    let physical_now = Epoch::physical_now();
    if (physical_now - prev_memory_stats.lru_watermark_time_ms) / (interval_ms as u64) < step {
        // We do not increase the step and watermark here to prevent a too-advanced watermark. The
        // original condition is `prev_watermark_time_ms + interval_ms * step > now`.
        step = last_step;
        watermark_time_ms = physical_now;
    } else {
        watermark_time_ms += interval_ms as u64 * step;
    }

    (step, watermark_time_ms, physical_now)
}

fn set_lru_watermark_time_ms(watermark_epoch: Arc<AtomicU64>, time_ms: u64) {
    use std::sync::atomic::Ordering;

    let epoch = Epoch::from_physical_time(time_ms).0;
    watermark_epoch.as_ref().store(epoch, Ordering::Relaxed);
}
