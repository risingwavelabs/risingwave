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
use risingwave_common::system_param::local_manager::{
    LocalSystemParamManagerRef, SystemParamsReaderRef,
};
#[cfg(target_os = "linux")]
use risingwave_common::util::epoch::Epoch;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::task::LocalStreamManager;
use tokio::select;

/// The minimal memory requirement of computing tasks in megabytes.
pub const MIN_COMPUTE_MEMORY_MB: usize = 512;
/// The memory reserved for system usage (stack and code segment of processes, allocation overhead,
/// network buffer, etc.) in megabytes.
pub const SYSTEM_RESERVED_MEMORY_MB: usize = 512;

/// When `enable_managed_cache` is set, compute node will launch a [`GlobalMemoryManager`] to limit
/// the memory usage.
#[cfg_attr(not(target_os = "linux"), expect(dead_code))]
pub struct GlobalMemoryManager {
    /// All cached data before the watermark should be evicted.
    watermark_epoch: Arc<AtomicU64>,
    /// Total memory can be allocated by the process.
    total_memory_available_bytes: usize,
    /// Read the latest barrier interval from system parameter.
    system_param_manager: LocalSystemParamManagerRef,
    metrics: Arc<StreamingMetrics>,
}

pub type GlobalMemoryManagerRef = Arc<GlobalMemoryManager>;

impl GlobalMemoryManager {
    #[cfg(target_os = "linux")]
    const EVICTION_THRESHOLD_AGGRESSIVE: f64 = 0.9;
    #[cfg(target_os = "linux")]
    const EVICTION_THRESHOLD_GRACEFUL: f64 = 0.7;

    pub fn new(
        total_memory_available_bytes: usize,
        system_param_manager: LocalSystemParamManagerRef,
        metrics: Arc<StreamingMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            watermark_epoch: Arc::new(0.into()),
            total_memory_available_bytes,
            system_param_manager,
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
        _batch_mgr: Arc<BatchManager>,
        _stream_mgr: Arc<LocalStreamManager>,
    ) {
        use std::time::Duration;

        use tikv_jemalloc_ctl::{epoch as jemalloc_epoch, stats as jemalloc_stats};

        let mem_threshold_graceful =
            (self.total_memory_available_bytes as f64 * Self::EVICTION_THRESHOLD_GRACEFUL) as usize;
        let mem_threshold_aggressive = (self.total_memory_available_bytes as f64
            * Self::EVICTION_THRESHOLD_AGGRESSIVE) as usize;

        let mut watermark_time_ms = Epoch::physical_now();
        let mut last_total_bytes_used = 0;
        let mut step = 0;

        let jemalloc_epoch_mib = jemalloc_epoch::mib().unwrap();
        let jemalloc_allocated_mib = jemalloc_stats::allocated::mib().unwrap();

        let mut barrier_interval_ms =
            Self::get_eviction_check_interval(&self.system_param_manager.get_params());
        let mut tick_interval = tokio::time::interval(Duration::from_millis(barrier_interval_ms));
        let mut params_rx = self.system_param_manager.watch_params();

        loop {
            // Wait for a while to check if need eviction.
            select! {
                // Barrier interval has changed.
                Ok(_) = params_rx.changed() => {
                    let new_barrier_interval_ms = Self::get_eviction_check_interval(&params_rx.borrow());
                    if new_barrier_interval_ms != barrier_interval_ms
                     {
                        tick_interval = tokio::time::interval(Duration::from_millis(new_barrier_interval_ms));
                        barrier_interval_ms = new_barrier_interval_ms;
                    }
                }
                _ = tick_interval.tick() => {},
            }

            if let Err(e) = jemalloc_epoch_mib.advance() {
                tracing::warn!("Jemalloc epoch advance failed! {:?}", e);
            }

            let cur_total_bytes_used = jemalloc_allocated_mib.read().unwrap_or_else(|e| {
                tracing::warn!("Jemalloc read allocated failed! {:?}", e);
                last_total_bytes_used
            });

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

            let last_step = step;
            step = if cur_total_bytes_used < mem_threshold_graceful {
                // Do not evict if the memory usage is lower than `mem_threshold_graceful`
                0
            } else if cur_total_bytes_used < mem_threshold_aggressive {
                // Gracefully evict
                if last_total_bytes_used > cur_total_bytes_used {
                    1
                } else {
                    step + 1
                }
            } else if last_total_bytes_used < cur_total_bytes_used {
                // Aggressively evict
                if step == 0 {
                    2
                } else {
                    step * 2
                }
            } else {
                step
            };

            last_total_bytes_used = cur_total_bytes_used;

            // if watermark_time_ms + self.barrier_interval_ms as u64 * step > now, we do not
            // increase the step, and set the epoch to now time epoch.
            let physical_now = Epoch::physical_now();
            if (physical_now - watermark_time_ms) / (barrier_interval_ms) < step {
                step = last_step;
                watermark_time_ms = physical_now;
            } else {
                watermark_time_ms += barrier_interval_ms * step;
            }

            self.metrics
                .lru_current_watermark_time_ms
                .set(watermark_time_ms as i64);
            self.metrics.lru_physical_now_ms.set(physical_now as i64);
            self.metrics.lru_watermark_step.set(step as i64);
            self.metrics.lru_runtime_loop_count.inc();
            self.metrics
                .jemalloc_allocated_bytes
                .set(cur_total_bytes_used as i64);
            self.metrics
                .stream_total_mem_usage
                .set(_stream_mgr.get_total_mem_val().get());

            self.set_watermark_time_ms(watermark_time_ms);
        }
    }

    fn get_eviction_check_interval(params: &SystemParamsReaderRef) -> u64 {
        // Arbitrarily set a minimal barrier interval in case it is too small,
        // especially when it's 0.
        std::cmp::max(params.load().barrier_interval_ms() as u64, 10)
    }
}
