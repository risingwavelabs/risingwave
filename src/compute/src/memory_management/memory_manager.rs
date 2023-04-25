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
use std::time::Duration;

use risingwave_batch::task::BatchManager;
use risingwave_common::util::epoch::Epoch;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::task::LocalStreamManager;

use super::MemoryControlRef;
use crate::memory_management::MemoryControlStats;

/// Compute node uses [`GlobalMemoryManager`] to limit the memory usage.
pub struct GlobalMemoryManager {
    /// All cached data before the watermark should be evicted.
    watermark_epoch: Arc<AtomicU64>,
    /// Loop interval of running control policy
    interval_ms: u32,
    metrics: Arc<StreamingMetrics>,
    /// The memory control policy for computing tasks.
    memory_control_policy: MemoryControlRef,
}

pub type GlobalMemoryManagerRef = Arc<GlobalMemoryManager>;

impl GlobalMemoryManager {
    pub fn new(
        interval_ms: u32,
        metrics: Arc<StreamingMetrics>,
        memory_control_policy: MemoryControlRef,
    ) -> Arc<Self> {
        // Arbitrarily set a minimal barrier interval in case it is too small,
        // especially when it's 0.
        let interval_ms = std::cmp::max(interval_ms, 10);

        tracing::info!("memory control policy: {:?}", &memory_control_policy);

        Arc::new(Self {
            watermark_epoch: Arc::new(0.into()),
            interval_ms,
            metrics,
            memory_control_policy,
        })
    }

    pub fn get_watermark_epoch(&self) -> Arc<AtomicU64> {
        self.watermark_epoch.clone()
    }

    /// Memory manager will get memory usage statistics from batch and streaming and perform memory
    /// control accordingly.
    pub async fn run(
        self: Arc<Self>,
        batch_manager: Arc<BatchManager>,
        stream_manager: Arc<LocalStreamManager>,
    ) {
        // Keep same interval with the barrier interval
        let mut tick_interval =
            tokio::time::interval(Duration::from_millis(self.interval_ms as u64));

        let mut memory_control_stats = MemoryControlStats {
            jemalloc_allocated_mib: 0,
            lru_watermark_step: 0,
            lru_watermark_time_ms: Epoch::physical_now(),
            lru_physical_now_ms: Epoch::physical_now(),
        };

        loop {
            // Wait for a while to check if need eviction.
            tick_interval.tick().await;

            memory_control_stats = self.memory_control_policy.apply(
                self.interval_ms,
                memory_control_stats,
                batch_manager.clone(),
                stream_manager.clone(),
                self.watermark_epoch.clone(),
            );

            self.metrics
                .lru_current_watermark_time_ms
                .set(memory_control_stats.lru_watermark_time_ms as i64);
            self.metrics
                .lru_physical_now_ms
                .set(memory_control_stats.lru_physical_now_ms as i64);
            self.metrics
                .lru_watermark_step
                .set(memory_control_stats.lru_watermark_step as i64);
            self.metrics.lru_runtime_loop_count.inc();
            self.metrics
                .jemalloc_allocated_bytes
                .set(memory_control_stats.jemalloc_allocated_mib as i64);
        }
    }
}
