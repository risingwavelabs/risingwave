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

use std::alloc::Allocator;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use global_stats_alloc::INSTRUMENTED_JEMALLOC;
use lru::LruCache;
use risingwave_common::util::epoch::Epoch;

use super::ManagedLruCache;

/// When `enable_managed_cache` is set, compute node will launch a [`LruManager`] to limit the
/// memory usage.
pub struct LruManager {
    /// All cached data before the watermark should be evicted.
    watermark_epoch: Arc<AtomicU64>,
    /// Total memory can be allocated by the process.
    total_memory_available_bytes: usize,
    /// Barrier interval.
    barrier_interval_ms: u32,
}

pub type LruManagerRef = Arc<LruManager>;

impl LruManager {
    const EVICTION_THRESHOLD_AGGRESSIVE: f64 = 0.9;
    const EVICTION_THRESHOLD_GRACEFUL: f64 = 0.7;

    pub fn new(total_memory_available_bytes: usize, barrier_interval_ms: u32) -> Arc<Self> {
        Arc::new(Self {
            watermark_epoch: Arc::new(0.into()),
            total_memory_available_bytes,
            barrier_interval_ms,
        })
    }

    pub fn create_cache<K: Hash + Eq, V>(&self) -> ManagedLruCache<K, V> {
        ManagedLruCache {
            inner: LruCache::unbounded(),
            watermark_epoch: self.watermark_epoch.clone(),
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

    pub fn create_cache_with_hasher<K: Hash + Eq, V, S: BuildHasher>(
        &self,
        hasher: S,
    ) -> ManagedLruCache<K, V, S> {
        ManagedLruCache {
            inner: LruCache::unbounded_with_hasher(hasher),
            watermark_epoch: self.watermark_epoch.clone(),
        }
    }

    fn set_watermark_time_ms(&self, time_ms: u64) {
        let epoch = Epoch::from_physical_time(time_ms).0;
        let watermark_epoch = self.watermark_epoch.as_ref();
        watermark_epoch.store(epoch, Ordering::Relaxed);
    }

    pub async fn run(self: Arc<Self>) {
        let mem_threshold_graceful =
            (self.total_memory_available_bytes as f64 * Self::EVICTION_THRESHOLD_GRACEFUL) as usize;
        let mem_threshold_aggressive = (self.total_memory_available_bytes as f64
            * Self::EVICTION_THRESHOLD_AGGRESSIVE) as usize;

        let mut watermark_time_ms = Epoch::physical_now();
        let mut last_total_bytes_used = 0;
        let mut step = 0;

        let mut tick_interval =
            tokio::time::interval(Duration::from_millis(self.barrier_interval_ms as u64));
        loop {
            // Wait for a while to check if need eviction.
            tick_interval.tick().await;

            let stats = INSTRUMENTED_JEMALLOC.stats();
            let cur_total_bytes_used = stats
                .bytes_allocated
                .saturating_sub(stats.bytes_deallocated);

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
            } else if last_total_bytes_used > cur_total_bytes_used {
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
            watermark_time_ms += self.barrier_interval_ms as u64 * step;

            self.set_watermark_time_ms(watermark_time_ms);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::EvictableHashMap;

    #[test]
    fn test_len_after_evict() {
        let target_cap = 114;
        let items_count = 514;
        let mut map = EvictableHashMap::new(target_cap);

        for i in 0..items_count {
            map.put(i, ());
        }
        assert_eq!(map.len(), items_count);

        map.evict_to_target_cap();
        assert_eq!(map.len(), target_cap);

        assert!(map.get(&(items_count - target_cap - 1)).is_none());
        assert!(map.get(&(items_count - target_cap)).is_some());
    }
}
