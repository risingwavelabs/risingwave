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

use risingwave_common::util::epoch::Epoch;
use risingwave_jni_core::jvm_runtime::load_jvm_memory_stats;
use tikv_jemalloc_ctl::{epoch as jemalloc_epoch, stats as jemalloc_stats};

struct MemoryControlStats {
    pub used_memory_bytes: usize,
    pub lru_watermark_step: u64,
    pub lru_watermark_time_ms: u64,
    pub lru_physical_now_ms: u64,
}

impl Default for MemoryControlStats {
    fn default() -> Self {
        let physical_now = Epoch::physical_now();
        Self {
            used_memory_bytes: 0,
            lru_watermark_step: 0,
            lru_watermark_time_ms: physical_now,
            lru_physical_now_ms: physical_now,
        }
    }
}

/// `JemallocAndJvmMemoryControl` is a memory control policy that uses jemalloc statistics and
/// jvm memory statistics and to control. It assumes that most memory is used by streaming engine
/// and does memory control over LRU watermark based on jemalloc statistics and jvm memory statistics.
pub struct JemallocAndJvmMemoryControl {
    /// TODO(eric): make them configurable
    threshold_stable: usize,
    threshold_graceful: usize,
    threshold_aggressive: usize,

    /// Stats from previous step
    stats: MemoryControlStats,
}

impl JemallocAndJvmMemoryControl {
    const THRESHOLD_AGGRESSIVE: f64 = 0.9;
    const THRESHOLD_GRACEFUL: f64 = 0.8;
    const THRESHOLD_STABLE: f64 = 0.7;

    pub fn new(total_memory: usize) -> Self {
        let threshold_stable = (total_memory as f64 * Self::THRESHOLD_STABLE) as usize;
        let threshold_graceful = (total_memory as f64 * Self::THRESHOLD_GRACEFUL) as usize;
        let threshold_aggressive = (total_memory as f64 * Self::THRESHOLD_AGGRESSIVE) as usize;

        // let jemalloc_epoch_mib = jemalloc_epoch::mib().unwrap();
        // let jemalloc_allocated_mib = jemalloc_stats::allocated::mib().unwrap();
        // let jemalloc_active_mib = jemalloc_stats::active::mib().unwrap();

        Self {
            threshold_stable,
            threshold_graceful,
            threshold_aggressive,
            stats: MemoryControlStats::default(),
        }
    }
}

impl std::fmt::Debug for JemallocAndJvmMemoryControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JemallocMemoryControl")
            .field("threshold_stable", &self.threshold_stable)
            .field("threshold_graceful", &self.threshold_graceful)
            .field("threshold_aggressive", &self.threshold_aggressive)
            .finish()
    }
}

fn jemalloc_active_memory() -> usize {
    if let Err(e) = tikv_jemalloc_ctl::epoch::advance() {
        tracing::warn!("Jemalloc epoch advance failed! {:?}", e);
    }
    tikv_jemalloc_ctl::stats::active::read().unwrap()
}

impl JemallocAndJvmMemoryControl {
    pub fn tick(&mut self, interval_ms: u32, watermark_epoch: Arc<AtomicU64>) {
        let jemalloc_active_bytes = jemalloc_active_memory();
        let (jvm_allocated_bytes, _) = load_jvm_memory_stats();
        let cur_used_memory_bytes = jemalloc_active_bytes + jvm_allocated_bytes;

        let last_step = self.stats.lru_watermark_step;
        let last_used_memory_bytes = self.stats.used_memory_bytes;

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

        let mut step = if cur_used_memory_bytes < self.threshold_stable {
            // Do not evict if the memory usage is lower than `threshold_stable`
            0
        } else if cur_used_memory_bytes < self.threshold_graceful {
            // Evict in equal speed of time before `threshold_graceful`
            1
        } else if cur_used_memory_bytes < self.threshold_aggressive {
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
        let watermark_time_ms =
            if (physical_now - self.stats.lru_watermark_time_ms) / (interval_ms as u64) < step {
                // We do not increase the step and watermark here to prevent a too-advanced watermark. The
                // original condition is `prev_watermark_time_ms + interval_ms * step > now`.
                step = last_step;
                physical_now
            } else {
                // Increase by (steps * interval)
                self.stats.lru_watermark_time_ms + (interval_ms as u64 * step)
            };

        set_lru_watermark_time_ms(watermark_epoch, watermark_time_ms);

        self.stats = MemoryControlStats {
            used_memory_bytes: cur_used_memory_bytes,
            lru_watermark_step: step,
            lru_watermark_time_ms: watermark_time_ms,
            lru_physical_now_ms: physical_now,
        }
    }
}

fn set_lru_watermark_time_ms(watermark_epoch: Arc<AtomicU64>, time_ms: u64) {
    use std::sync::atomic::Ordering;

    let epoch = Epoch::from_physical_time(time_ms).0;
    watermark_epoch.as_ref().store(epoch, Ordering::Relaxed);
}
