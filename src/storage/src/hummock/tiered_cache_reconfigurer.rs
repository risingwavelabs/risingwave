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

use std::sync::atomic::Ordering;

use foyer::HybridCache;
use risingwave_common::system_param::local_manager::SystemParamsReaderRef;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_hummock_sdk::HummockSstableObjectId;
use tokio::sync::watch;

use super::{Block, Sstable, SstableBlockIndex};

/// Background runner to update tiered cache configuration via the system parameters.
pub struct TieredCacheReconfigurer {
    meta_cache: HybridCache<HummockSstableObjectId, Box<Sstable>>,
    block_cache: HybridCache<SstableBlockIndex, Box<Block>>,
}

impl TieredCacheReconfigurer {
    pub fn new(
        meta_cache: HybridCache<HummockSstableObjectId, Box<Sstable>>,
        block_cache: HybridCache<SstableBlockIndex, Box<Block>>,
    ) -> Self {
        Self {
            meta_cache,
            block_cache,
        }
    }

    pub async fn run(self, mut rx: watch::Receiver<SystemParamsReaderRef>) {
        loop {
            if let Err(e) = rx.changed().await {
                tracing::error!("Tiered cache reconfigurer exit with error: {}", e);
            }

            let p = rx.borrow().load();
            if p.minitrace() {
                self.meta_cache.enable_tracing();
                self.block_cache.enable_tracing();
                tracing::info!("Enable minitrace for tiered cache.");
            } else {
                self.meta_cache.disable_tracing();
                self.block_cache.disable_tracing();
                tracing::info!("Disable minitrace for tiered cache.");
            }
            let minitrace_tiered_cache_read_ms = p.minitrace_tiered_cache_read_ms();
            let minitrace_tiered_cache_write_ms = p.minitrace_tiered_cache_write_ms();

            tracing::info!(
                "Tiered cache trace record threshold: [read = {:?}] [write = {:?}]",
                minitrace_tiered_cache_read_ms,
                minitrace_tiered_cache_write_ms
            );

            let meta_cache_trace_config = self.meta_cache.trace_config();
            let block_cache_trace_config = self.meta_cache.trace_config();

            meta_cache_trace_config
                .record_hybrid_get_threshold_us
                .store(
                    minitrace_tiered_cache_read_ms as usize * 1000,
                    Ordering::Relaxed,
                );
            meta_cache_trace_config
                .record_hybrid_obtain_threshold_us
                .store(
                    minitrace_tiered_cache_read_ms as usize * 1000,
                    Ordering::Relaxed,
                );
            meta_cache_trace_config
                .record_hybrid_fetch_threshold_us
                .store(
                    minitrace_tiered_cache_read_ms as usize * 1000,
                    Ordering::Relaxed,
                );

            meta_cache_trace_config
                .record_hybrid_insert_threshold_us
                .store(
                    minitrace_tiered_cache_write_ms as usize * 1000,
                    Ordering::Relaxed,
                );
            meta_cache_trace_config
                .record_hybrid_remove_threshold_us
                .store(
                    minitrace_tiered_cache_write_ms as usize * 1000,
                    Ordering::Relaxed,
                );

            block_cache_trace_config
                .record_hybrid_get_threshold_us
                .store(
                    minitrace_tiered_cache_read_ms as usize * 1000,
                    Ordering::Relaxed,
                );
            block_cache_trace_config
                .record_hybrid_obtain_threshold_us
                .store(
                    minitrace_tiered_cache_read_ms as usize * 1000,
                    Ordering::Relaxed,
                );
            block_cache_trace_config
                .record_hybrid_fetch_threshold_us
                .store(
                    minitrace_tiered_cache_read_ms as usize * 1000,
                    Ordering::Relaxed,
                );

            block_cache_trace_config
                .record_hybrid_insert_threshold_us
                .store(
                    minitrace_tiered_cache_write_ms as usize * 1000,
                    Ordering::Relaxed,
                );
            block_cache_trace_config
                .record_hybrid_remove_threshold_us
                .store(
                    minitrace_tiered_cache_write_ms as usize * 1000,
                    Ordering::Relaxed,
                );
        }
    }
}
