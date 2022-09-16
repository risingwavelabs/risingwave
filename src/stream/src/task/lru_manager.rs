// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::alloc::{Allocator, Global};
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use global_stats_alloc::INSTRUMENTED_JEMALLOC;
use lru::{DefaultHasher, LruCache};
use risingwave_common::util::epoch::Epoch;
use tokio::time::sleep;

pub struct ManagedLruCache<K, V, S = DefaultHasher, A: Clone + Allocator = Global> {
    inner: LruCache<K, V, S, A>,
    /// The entry with epoch less than water should be evicted.
    watermark_epoch: Arc<AtomicU64>,
}

impl<K, V, S, A: Clone + Allocator> ManagedLruCache<K, V, S, A> {}

pub struct LruManager {
    watermark_epoch: Arc<AtomicU64>,
    total_memory_available_bytes: usize,
    barrier_interval_ms: u32,
}

impl LruManager {
    pub fn new(total_memory_available_bytes: usize, barrier_interval_ms: u32) -> Self {
        Self {
            watermark_epoch: Arc::new(0.into()),
            total_memory_available_bytes,
            barrier_interval_ms,
        }
    }

    pub fn create_cache_with_hasher_in<K: Hash + Eq, V, S: BuildHasher, A: Clone + Allocator>(
        &self,
        hasher: S,
        alloc: A,
    ) -> ManagedLruCache<K, V, S, A> {
        ManagedLruCache {
            inner: LruCache::unbounded_with_hasher_in(hasher, alloc),
            watermark_epoch: self.watermark_epoch.clone(),
        }
    }

    fn set_watermark_time(&self, time: u64) {
        let epoch = Epoch::from_physical_time(time).0;
        let watermark_epoch = self.watermark_epoch.as_ref();
        watermark_epoch.store(epoch, Ordering::Relaxed);
    }

    pub async fn run(self: Arc<Self>) {
        let mem_threshold_graceful = (self.total_memory_available_bytes as f64 * 0.7) as usize;
        let mem_threshold_aggressive = (self.total_memory_available_bytes as f64 * 0.9) as usize;

        let mut watermark_time = 0u64;
        let mut last_total_bytes_used = 0;
        let mut step = 0;

        loop {
            // Wait for a while to check if need eviction.
            sleep(Duration::from_micros(self.barrier_interval_ms as u64)).await;

            let stats = INSTRUMENTED_JEMALLOC.stats();
            let cur_total_bytes_used = stats.bytes_allocated - stats.bytes_deallocated;

            step = if cur_total_bytes_used < mem_threshold_graceful {
                // Do not evict if the memory usage is lower than `mem_threshold_graceful`
                0
            } else if cur_total_bytes_used > mem_threshold_graceful
                && cur_total_bytes_used < mem_threshold_aggressive
            {
                // Gracefully evict
                1
            } else if last_total_bytes_used < cur_total_bytes_used {
                // Aggressively evict
                if step == 0 { 2 } else { step * 2 }
            } else {
                step
            };

            last_total_bytes_used = cur_total_bytes_used;
            watermark_time += self.barrier_interval_ms as u64 * step;

            self.set_watermark_time(watermark_time);
        }
        
    }
}