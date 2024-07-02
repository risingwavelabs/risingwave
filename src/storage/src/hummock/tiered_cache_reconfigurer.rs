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

use std::time::Duration;

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
            if rx.changed().await.is_err() {
                tracing::warn!("Tiered cache reconfigurer get updates error.");
                continue;
            }

            let p = rx.borrow().load();
            if p.enable_tracing() {
                self.meta_cache.enable_tracing();
                self.block_cache.enable_tracing();
                tracing::info!("Enable minitrace for tiered cache.");
            } else {
                self.meta_cache.disable_tracing();
                self.block_cache.disable_tracing();
                tracing::info!("Disable minitrace for tiered cache.");
            }
            let read = p.tracing_threshold_tiered_cache_read_ms();
            let write = p.tracing_threshold_tiered_cache_write_ms();

            tracing::info!(
                "Tiered cache trace record threshold: [read = {:?}] [write = {:?}]",
                read,
                write
            );

            let meta_cache_trace_config = self.meta_cache.tracing_config();
            let block_cache_trace_config = self.meta_cache.tracing_config();

            meta_cache_trace_config
                .set_record_hybrid_get_threshold(Duration::from_millis(read as _));
            meta_cache_trace_config
                .set_record_hybrid_obtain_threshold(Duration::from_millis(read as _));
            meta_cache_trace_config
                .set_record_hybrid_fetch_threshold(Duration::from_millis(read as _));

            meta_cache_trace_config
                .set_record_hybrid_insert_threshold(Duration::from_millis(write as _));
            meta_cache_trace_config
                .set_record_hybrid_remove_threshold(Duration::from_millis(write as _));

            block_cache_trace_config
                .set_record_hybrid_get_threshold(Duration::from_millis(read as _));
            block_cache_trace_config
                .set_record_hybrid_obtain_threshold(Duration::from_millis(read as _));
            block_cache_trace_config
                .set_record_hybrid_fetch_threshold(Duration::from_millis(read as _));

            block_cache_trace_config
                .set_record_hybrid_insert_threshold(Duration::from_millis(write as _));
            block_cache_trace_config
                .set_record_hybrid_remove_threshold(Duration::from_millis(write as _));
        }
    }
}
