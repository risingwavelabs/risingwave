// Copyright 2023 Singularity Data
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
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::task::LocalStreamManager;

/// When `enable_managed_cache` is set, compute node will launch a [`GlobalMemoryManager`] to limit
/// the memory usage.
#[cfg(not(target_os = "linux"))]
pub struct GlobalMemoryManager {
    /// All cached data before the watermark should be evicted.
    watermark_epoch: Arc<AtomicU64>,
}

impl GlobalMemoryManager {
    pub fn new(
        _total_memory_available_bytes: usize,
        _barrier_interval_ms: u32,
        _metrics: Arc<StreamingMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            watermark_epoch: Arc::new(0.into()),
        })
    }

    pub fn get_watermark_epoch(&self) -> Arc<AtomicU64> {
        self.watermark_epoch.clone()
    }

    #[allow(clippy::unused_async)]
    pub async fn run(self: Arc<Self>, _: Arc<BatchManager>, _: Arc<LocalStreamManager>) {}
}
