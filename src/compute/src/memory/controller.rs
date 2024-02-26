// Copyright 2024 RisingWave Labs
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

use std::sync::Arc;

use risingwave_common::util::epoch::Epoch;
use risingwave_jni_core::jvm_runtime::load_jvm_memory_stats;
use risingwave_stream::executor::monitor::StreamingMetrics;

/// Internal state of [`LruWatermarkController`] that saves the state in previous tick.
struct State {
    pub used_memory_bytes: usize,
    pub lru_watermark_step: u64,
    pub lru_watermark_time_ms: u64,
}

impl Default for State {
    fn default() -> Self {
        let physical_now = Epoch::physical_now();
        Self {
            used_memory_bytes: 0,
            lru_watermark_step: 0,
            lru_watermark_time_ms: physical_now,
        }
    }
}

/// `LruWatermarkController` controls LRU Watermark (epoch) according to actual memory usage statistics
/// collected from Jemalloc and JVM.
///
/// Basically, it works as a negative feedback loop: collect memory statistics, and then set the LRU watarmarking
/// according to maintain the memory usage in a proper level.
///
/// ```text
///     ┌───────────────────┐        ┌───────────────────┐
///     │       INPUT       │        │       OUTPUT      │
/// ┌───►     (observe)     ├───────►│     (control)     ├───┐
/// │   │ Memory Statistics │        │ New LRU Watermark │   │
/// │   └───────────────────┘        └───────────────────┘   │
/// │                                                        │
/// └────────────────────────────────────────────────────────┘
/// ```
///
/// Check the function [`Self::tick()`] to see the control policy.
pub struct LruWatermarkController {
    metrics: Arc<StreamingMetrics>,

    threshold_stable: usize,
    threshold_graceful: usize,
    threshold_aggressive: usize,

    /// The state from previous tick
    state: State,
}

impl LruWatermarkController {
    // TODO(eric): make them configurable
    const THRESHOLD_AGGRESSIVE: f64 = 0.9;
    const THRESHOLD_GRACEFUL: f64 = 0.8;
    const THRESHOLD_STABLE: f64 = 0.7;

    pub fn new(total_memory: usize, metrics: Arc<StreamingMetrics>) -> Self {
        let threshold_stable = (total_memory as f64 * Self::THRESHOLD_STABLE) as usize;
        let threshold_graceful = (total_memory as f64 * Self::THRESHOLD_GRACEFUL) as usize;
        let threshold_aggressive = (total_memory as f64 * Self::THRESHOLD_AGGRESSIVE) as usize;

        Self {
            metrics,
            threshold_stable,
            threshold_graceful,
            threshold_aggressive,
            state: State::default(),
        }
    }
}

impl std::fmt::Debug for LruWatermarkController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LruWatermarkController")
            .field("threshold_stable", &self.threshold_stable)
            .field("threshold_graceful", &self.threshold_graceful)
            .field("threshold_aggressive", &self.threshold_aggressive)
            .finish()
    }
}

/// Get memory statistics from Jemalloc
///
/// - `stats.allocated`: Total number of bytes allocated by the application.
/// - `stats.active`: Total number of bytes in active pages allocated by the application. This is a multiple of the page size, and greater than or equal to `stats.allocated`. This does not include `stats.arenas.<i>.pdirty`, `stats.arenas.<i>.pmuzzy`, nor pages entirely devoted to allocator metadata.
/// - `stats.resident`: Total number of bytes in physically resident data pages mapped by the allocator.
/// - `stats.metadata`: Total number of bytes dedicated to jemalloc metadata.
///
/// Reference: <https://jemalloc.net/jemalloc.3.html>
fn jemalloc_memory_stats() -> (usize, usize, usize, usize) {
    if let Err(e) = tikv_jemalloc_ctl::epoch::advance() {
        tracing::warn!("Jemalloc epoch advance failed! {:?}", e);
    }
    let allocated = tikv_jemalloc_ctl::stats::allocated::read().unwrap();
    let active = tikv_jemalloc_ctl::stats::active::read().unwrap();
    let resident = tikv_jemalloc_ctl::stats::resident::read().unwrap();
    let metadata = tikv_jemalloc_ctl::stats::metadata::read().unwrap();
    (allocated, active, resident, metadata)
}

impl LruWatermarkController {
    pub fn tick(&mut self, interval_ms: u32) -> Epoch {
        // NOTE: Be careful! The meaning of `allocated` and `active` differ in JeMalloc and JVM
        let (
            jemalloc_allocated_bytes,
            jemalloc_active_bytes,
            jemalloc_resident_bytes,
            jemalloc_metadata_bytes,
        ) = jemalloc_memory_stats();
        let (jvm_allocated_bytes, jvm_active_bytes) = load_jvm_memory_stats();

        let cur_used_memory_bytes = jemalloc_active_bytes + jvm_allocated_bytes;

        let last_step = self.state.lru_watermark_step;
        let last_used_memory_bytes = self.state.used_memory_bytes;

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
            if (physical_now - self.state.lru_watermark_time_ms) / (interval_ms as u64) < step {
                // We do not increase the step and watermark here to prevent a too-advanced watermark. The
                // original condition is `prev_watermark_time_ms + interval_ms * step > now`.
                step = last_step;
                physical_now
            } else {
                // Increase by (steps * interval)
                self.state.lru_watermark_time_ms + (interval_ms as u64 * step)
            };

        self.state = State {
            used_memory_bytes: cur_used_memory_bytes,
            lru_watermark_step: step,
            lru_watermark_time_ms: watermark_time_ms,
        };

        self.metrics
            .lru_current_watermark_time_ms
            .set(watermark_time_ms as i64);
        self.metrics.lru_physical_now_ms.set(physical_now as i64);
        self.metrics.lru_watermark_step.set(step as i64);
        self.metrics
            .jemalloc_allocated_bytes
            .set(jemalloc_allocated_bytes as i64);
        self.metrics
            .jemalloc_active_bytes
            .set(jemalloc_active_bytes as i64);
        self.metrics
            .jemalloc_resident_bytes
            .set(jemalloc_resident_bytes as i64);
        self.metrics
            .jemalloc_metadata_bytes
            .set(jemalloc_metadata_bytes as i64);
        self.metrics
            .jvm_allocated_bytes
            .set(jvm_allocated_bytes as i64);
        self.metrics.jvm_active_bytes.set(jvm_active_bytes as i64);

        Epoch::from_physical_time(watermark_time_ms)
    }
}
