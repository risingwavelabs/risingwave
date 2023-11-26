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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_stream::executor::monitor::StreamingMetrics;

use super::controller::LruWatermarkController;

/// Compute node uses [`MemoryManager`] to limit the memory usage.
pub struct MemoryManager {
    /// All cached data before the watermark should be evicted.
    watermark_epoch: Arc<AtomicU64>,

    metrics: Arc<StreamingMetrics>,

    controller: Mutex<LruWatermarkController>,
}

impl MemoryManager {
    // Arbitrarily set a minimal barrier interval in case it is too small,
    // especially when it's 0.
    const MIN_TICK_INTERVAL_MS: u32 = 10;

    pub fn new(metrics: Arc<StreamingMetrics>, total_memory_bytes: usize) -> Arc<Self> {
        let controller = Mutex::new(LruWatermarkController::new(
            total_memory_bytes,
            metrics.clone(),
        ));
        tracing::info!("LRU watermark controller: {:?}", &controller);

        Arc::new(Self {
            watermark_epoch: Arc::new(0.into()),
            metrics,
            controller,
        })
    }

    pub fn get_watermark_epoch(&self) -> Arc<AtomicU64> {
        self.watermark_epoch.clone()
    }

    pub async fn run(
        self: Arc<Self>,
        initial_interval_ms: u32,
        mut system_params_change_rx: tokio::sync::watch::Receiver<SystemParamsReaderRef>,
    ) {
        // Loop interval of running control policy
        let mut interval_ms = std::cmp::max(initial_interval_ms, Self::MIN_TICK_INTERVAL_MS);
        tracing::info!(
            "start running MemoryManager with interval {}ms",
            interval_ms
        );

        // Keep same interval with the barrier interval
        let mut tick_interval = tokio::time::interval(Duration::from_millis(interval_ms as u64));

        loop {
            // Wait for a while to check if need eviction.
            tokio::select! {
                Ok(_) = system_params_change_rx.changed() => {
                    let params = system_params_change_rx.borrow().load();
                    let new_interval_ms = std::cmp::max(params.barrier_interval_ms(), Self::MIN_TICK_INTERVAL_MS);
                    if new_interval_ms != interval_ms {
                        interval_ms = new_interval_ms;
                        tick_interval = tokio::time::interval(Duration::from_millis(interval_ms as u64));
                        tracing::info!("updated MemoryManager interval to {}ms", interval_ms);
                    }
                }

                _ = tick_interval.tick() => {
                    let new_watermark_epoch = self.controller.lock().unwrap().tick(interval_ms);
                    self.watermark_epoch.store(new_watermark_epoch.0, Ordering::Relaxed);

                    self.metrics.lru_runtime_loop_count.inc();
                }
            }
        }
    }
}
