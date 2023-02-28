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
#[cfg(target_os = "linux")]
use risingwave_common::util::epoch::Epoch;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::task::LocalStreamManager;

/// The minimal memory requirement of computing tasks in megabytes.
pub const MIN_COMPUTE_MEMORY_MB: usize = 512;
/// The memory reserved for system usage (stack and code segment of processes, allocation overhead,
/// network buffer, etc.) in megabytes.
pub const SYSTEM_RESERVED_MEMORY_MB: usize = 512;
/// TODO(Yuanxin)
const STREAM_MEMORY_PROPORTION: f64 = 0.7;
/// TODO(Yuanxin)
const BATCH_MEMORY_PROPORTION: f64 = 1.0 - STREAM_MEMORY_PROPORTION;

/// When `enable_managed_cache` is set, compute node will launch a [`GlobalMemoryManager`] to limit
/// the memory usage.
#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
pub struct GlobalMemoryManager {
    /// All cached data before the watermark should be evicted.
    watermark_epoch: Arc<AtomicU64>,
    /// Total memory that can be allocated by the compute node for computing tasks (stream & batch)
    /// in bytes.
    total_compute_memory_bytes: usize,
    /// Barrier interval.
    barrier_interval_ms: u32,
    metrics: Arc<StreamingMetrics>,
}

pub type GlobalMemoryManagerRef = Arc<GlobalMemoryManager>;

impl GlobalMemoryManager {
    #[cfg(target_os = "linux")]
    const BATCH_KILL_QUERY_THRESHOLD: f64 = 0.8;
    #[cfg(target_os = "linux")]
    const STREAM_EVICTION_THRESHOLD_AGGRESSIVE: f64 = 0.9;
    #[cfg(target_os = "linux")]
    const STREAM_EVICTION_THRESHOLD_GRACEFUL: f64 = 0.7;

    pub fn new(
        total_compute_memory_bytes: usize,
        barrier_interval_ms: u32,
        metrics: Arc<StreamingMetrics>,
    ) -> Arc<Self> {
        // Arbitrarily set a minimal barrier interval in case it is too small,
        // especially when it's 0.
        let barrier_interval_ms = std::cmp::max(barrier_interval_ms, 10);

        Arc::new(Self {
            watermark_epoch: Arc::new(0.into()),
            total_compute_memory_bytes,
            barrier_interval_ms,
            metrics,
        })
    }

    pub fn get_watermark_epoch(&self) -> Arc<AtomicU64> {
        self.watermark_epoch.clone()
    }

    #[cfg(target_os = "linux")]
    fn set_watermark_time_ms(&self, time_ms: u64) {
        use std::sync::atomic::Ordering;

        let epoch = Epoch::from_physical_time(time_ms).0;
        let watermark_epoch = self.watermark_epoch.as_ref();
        watermark_epoch.store(epoch, Ordering::Relaxed);
    }

    // FIXME: remove such limitation after #7180
    /// Jemalloc is not supported on Windows, because of tikv-jemalloc's own reasons.
    /// See the comments for the macro `enable_jemalloc_on_linux!()`
    #[cfg(not(target_os = "linux"))]
    #[expect(clippy::unused_async)]
    pub async fn run(self: Arc<Self>, _: Arc<BatchManager>, _: Arc<LocalStreamManager>) {}

    /// Memory manager will get memory usage from batch and streaming, and do some actions.
    /// 1. if batch exceeds, kill running query.
    /// 2. if streaming exceeds, evict cache by watermark.
    #[cfg(target_os = "linux")]
    pub async fn run(
        self: Arc<Self>,
        batch_manager: Arc<BatchManager>,
        stream_manager: Arc<LocalStreamManager>,
    ) {
        use std::time::Duration;

        use pretty_bytes::converter::convert;
        use tikv_jemalloc_ctl::{epoch as jemalloc_epoch, stats as jemalloc_stats};

        let total_batch_memory_bytes =
            (self.total_compute_memory_bytes as f64 * BATCH_MEMORY_PROPORTION) as usize;
        let batch_memory_threshold =
            (total_batch_memory_bytes as f64 * Self::BATCH_KILL_QUERY_THRESHOLD) as usize;
        let total_stream_memory_bytes =
            self.total_compute_memory_bytes as f64 * STREAM_MEMORY_PROPORTION;
        let stream_memory_threshold_graceful =
            (total_stream_memory_bytes * Self::STREAM_EVICTION_THRESHOLD_GRACEFUL) as usize;
        let stream_memory_threshold_aggressive =
            (total_stream_memory_bytes * Self::STREAM_EVICTION_THRESHOLD_AGGRESSIVE) as usize;

        tracing::info!(
            "Total memory for batch tasks: {}, total memory for streaming tasks: {}",
            convert(total_batch_memory_bytes as f64),
            convert(total_stream_memory_bytes as f64)
        );

        let mut watermark_time_ms = Epoch::physical_now();
        let mut last_stream_used_memory_bytes = 0;
        let mut step = 0;

        let jemalloc_epoch_mib = jemalloc_epoch::mib().unwrap();
        let jemalloc_allocated_mib = jemalloc_stats::allocated::mib().unwrap();
        let mut last_jemalloc_allocated_mib = 0;

        let mut tick_interval =
            tokio::time::interval(Duration::from_millis(self.barrier_interval_ms as u64));

        loop {
            // Wait for a while to check if need eviction.
            tick_interval.tick().await;

            if let Err(e) = jemalloc_epoch_mib.advance() {
                tracing::warn!("Jemalloc epoch advance failed! {:?}", e);
            }

            let jemalloc_allocated_mib = jemalloc_allocated_mib.read().unwrap_or_else(|e| {
                tracing::warn!("Jemalloc read allocated failed! {:?}", e);
                last_jemalloc_allocated_mib
            });
            last_jemalloc_allocated_mib = jemalloc_allocated_mib;

            // TODO(Yuanxin): comment
            let batch_used_memory_bytes = batch_manager.total_mem_usage();
            if batch_used_memory_bytes > batch_memory_threshold {
                batch_manager.kill_queries("excessive batch memory usage".to_string());
            }

            // TODO(Yuanxin): Rewrite the comment.
            // The strategy works as follow:
            //
            // 1. When the memory usage is below the graceful threshold, we do not evict any caches
            // and reset the step to 0.
            //
            // 2. When the memory usage is between the graceful and aggressive threshold:
            //   - If the last eviction memory usage decrease after last eviction, we set the
            //     eviction step to 1
            //   - or else we set the step to last_step + 1
            //
            // 3. When the memory usage exceeds aggressive threshold:
            //   - If the memory usage decrease after last eviction, we set the eviction step to
            //     last_step
            //   - or else we set the step to last_step * 2

            let cur_stream_used_memory_bytes = stream_manager.total_mem_usage();
            let last_step = step;
            step = if cur_stream_used_memory_bytes < stream_memory_threshold_graceful {
                // Do not evict if the memory usage is lower than `mem_threshold_graceful`
                0
            } else if cur_stream_used_memory_bytes < stream_memory_threshold_aggressive {
                // Gracefully evict
                if last_stream_used_memory_bytes > cur_stream_used_memory_bytes {
                    1
                } else {
                    step + 1
                }
            } else if last_stream_used_memory_bytes < cur_stream_used_memory_bytes {
                // Aggressively evict
                if step == 0 {
                    2
                } else {
                    step * 2
                }
            } else {
                step
            };

            last_stream_used_memory_bytes = cur_stream_used_memory_bytes;

            // if watermark_time_ms + self.barrier_interval_ms as u64 * step > now, we do not
            // increase the step, and set the epoch to now time epoch.
            let physical_now = Epoch::physical_now();
            if (physical_now - watermark_time_ms) / (self.barrier_interval_ms as u64) < step {
                step = last_step;
                watermark_time_ms = physical_now;
            } else {
                watermark_time_ms += self.barrier_interval_ms as u64 * step;
            }

            self.metrics
                .lru_current_watermark_time_ms
                .set(watermark_time_ms as i64);
            self.metrics.lru_physical_now_ms.set(physical_now as i64);
            self.metrics.lru_watermark_step.set(step as i64);
            self.metrics.lru_runtime_loop_count.inc();
            self.metrics
                .jemalloc_allocated_bytes
                .set(jemalloc_allocated_mib as i64);
            self.metrics
                .stream_total_mem_usage
                .set(cur_stream_used_memory_bytes as i64);
            self.metrics
                .batch_total_mem_usage
                .set(batch_used_memory_bytes as i64);

            self.set_watermark_time_ms(watermark_time_ms);
        }
    }
}
