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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use risingwave_common::sequence::AtomicSequence;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_stream::executor::monitor::StreamingMetrics;

use super::controller::LruWatermarkController;

pub struct MemoryManagerConfig {
    /// [`MemoryManager`] will try to control the jemalloc-reported memory usage
    /// to be lower than this
    pub target_memory: usize,

    pub threshold_aggressive: f64,
    pub threshold_graceful: f64,
    pub threshold_stable: f64,

    pub eviction_factor_stable: f64,
    pub eviction_factor_graceful: f64,
    pub eviction_factor_aggressive: f64,

    pub metrics: Arc<StreamingMetrics>,
}

/// Compute node uses [`MemoryManager`] to limit the memory usage.
pub struct MemoryManager {
    /// All cached data before the watermark should be evicted.
    watermark_sequence: Arc<AtomicSequence>,

    metrics: Arc<StreamingMetrics>,

    controller: Mutex<LruWatermarkController>,
}

impl MemoryManager {
    // Arbitrarily set a minimal barrier interval in case it is too small,
    // especially when it's 0.
    const MIN_TICK_INTERVAL_MS: u32 = 10;

    pub fn new(config: MemoryManagerConfig) -> Arc<Self> {
        let controller = Mutex::new(LruWatermarkController::new(&config));
        tracing::info!("LRU watermark controller: {:?}", &controller);

        Arc::new(Self {
            watermark_sequence: Arc::new(0.into()),
            metrics: config.metrics,
            controller,
        })
    }

    pub fn get_watermark_sequence(&self) -> Arc<AtomicU64> {
        self.watermark_sequence.clone()
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
                     let new_watermark_sequence = self.controller.lock().unwrap().tick();

                     self.watermark_sequence.store(new_watermark_sequence, Ordering::Relaxed);

                     self.metrics.lru_runtime_loop_count.inc();
                }
            }
        }
    }
}
