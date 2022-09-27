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
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use auto_enums::auto_enum;
use global_stats_alloc::INSTRUMENTED_JEMALLOC;
use lru::{DefaultHasher, LruCache};
use risingwave_common::util::epoch::Epoch;
use tokio::time::sleep;

/// A wrapper for [`LruCache`] which provides manual eviction.
pub struct EvictableHashMap<K, V, S = DefaultHasher, A: Clone + Allocator = Global> {
    inner: LruCache<K, V, S, A>,

    /// Target capacity to keep when calling `evict_to_target_cap`.
    target_cap: usize,
}

impl<K: Hash + Eq, V, A: Clone + Allocator> EvictableHashMap<K, V, DefaultHasher, A> {
    /// Create a [`EvictableHashMap`] with the given target capacity and allocator.
    pub fn new_in(target_cap: usize, alloc: A) -> Self {
        Self::with_hasher_in(target_cap, DefaultHasher::new(), alloc)
    }
}

impl<K: Hash + Eq, V, S: BuildHasher, A: Clone + Allocator> EvictableHashMap<K, V, S, A> {
    /// Create a [`EvictableHashMap`] with the given target capacity, hasher and allocator.
    pub fn with_hasher_in(target_cap: usize, hasher: S, alloc: A) -> Self {
        Self {
            inner: LruCache::unbounded_with_hasher_in(hasher, alloc),
            target_cap,
        }
    }
}

impl<K: Hash + Eq, V> EvictableHashMap<K, V> {
    /// Create a [`EvictableHashMap`] with the given target capacity.
    pub fn new(target_cap: usize) -> EvictableHashMap<K, V> {
        EvictableHashMap::with_hasher(target_cap, DefaultHasher::new())
    }
}

impl<K: Hash + Eq, V, S: BuildHasher> EvictableHashMap<K, V, S> {
    /// Create a [`EvictableHashMap`] with the given target capacity and haser.
    pub fn with_hasher(target_cap: usize, hasher: S) -> Self {
        Self {
            inner: LruCache::unbounded_with_hasher(hasher),
            target_cap,
        }
    }
}

impl<K: Hash + Eq, V, S: BuildHasher, A: Clone + Allocator> EvictableHashMap<K, V, S, A> {
    pub fn target_cap(&self) -> usize {
        self.target_cap
    }

    /// Evict items in the map and only keep up-to `target_cap` items.
    pub fn evict_to_target_cap(&mut self) {
        self.inner.resize(self.target_cap);
        self.inner.resize(usize::MAX);
    }

    /// An iterator visiting all values in most-recently used order. The iterator element type is
    /// &V.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_k, v)| v)
    }

    /// An iterator visiting all values mutably in most-recently used order. The iterator element
    /// type is &mut V.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.iter_mut().map(|(_k, v)| v)
    }
}

impl<K, V, S, A: Clone + Allocator> Deref for EvictableHashMap<K, V, S, A> {
    type Target = LruCache<K, V, S, A>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V, S, A: Clone + Allocator> DerefMut for EvictableHashMap<K, V, S, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub struct ManagedLruCache<K, V, S = DefaultHasher, A: Clone + Allocator = Global> {
    inner: LruCache<K, V, S, A>,
    /// The entry with epoch less than water should be evicted.
    watermark_epoch: Arc<AtomicU64>,
}

impl<K: Hash + Eq, V, S: BuildHasher, A: Clone + Allocator> ManagedLruCache<K, V, S, A> {
    /// Evict epochs lower than the watermark
    pub fn evict(&mut self) {
        let epoch = self.watermark_epoch.load(Ordering::Relaxed);
        self.inner.evict_by_epoch(epoch);
    }

    /// An iterator visiting all values in most-recently used order. The iterator element type is
    /// &V.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.iter().map(|(_k, v)| v)
    }

    /// An iterator visiting all values mutably in most-recently used order. The iterator element
    /// type is &mut V.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        self.iter_mut().map(|(_k, v)| v)
    }
}

impl<K, V, S, A: Clone + Allocator> Deref for ManagedLruCache<K, V, S, A> {
    type Target = LruCache<K, V, S, A>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<K, V, S, A: Clone + Allocator> DerefMut for ManagedLruCache<K, V, S, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub struct LruManager {
    watermark_epoch: Arc<AtomicU64>,
    total_memory_available_bytes: usize,
    barrier_interval_ms: u32,
}

pub type LruManagerRef = Arc<LruManager>;

pub enum ExecutorCache<K, V, S = DefaultHasher, A: Clone + Allocator = Global> {
    /// An managed cache. Eviction depends on the node memory usage.
    Managed(ManagedLruCache<K, V, S, A>),
    /// An local cache. Eviction depends on local executor cache limit setting.
    Local(EvictableHashMap<K, V, S, A>),
}

impl<K: Hash + Eq, V, S: BuildHasher, A: Clone + Allocator> ExecutorCache<K, V, S, A> {
    /// Evict epochs lower than the watermark
    pub fn evict(&mut self) {
        match self {
            ExecutorCache::Managed(cache) => cache.evict(),
            ExecutorCache::Local(cache) => cache.evict_to_target_cap(),
        }
    }

    /// An iterator visiting all values in most-recently used order. The iterator element type is
    /// &V.
    #[auto_enum(Iterator)]
    pub fn values(&self) -> impl Iterator<Item = &V> {
        match self {
            ExecutorCache::Managed(cache) => cache.iter().map(|(_k, v)| v),
            ExecutorCache::Local(cache) => cache.iter().map(|(_k, v)| v),
        }
    }

    /// An iterator visiting all values mutably in most-recently used order. The iterator element
    /// type is &mut V.
    #[auto_enum(Iterator)]
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> {
        match self {
            ExecutorCache::Managed(cache) => cache.iter_mut().map(|(_k, v)| v),
            ExecutorCache::Local(cache) => cache.iter_mut().map(|(_k, v)| v),
        }
    }
}

impl<K, V, S, A: Clone + Allocator> Deref for ExecutorCache<K, V, S, A> {
    type Target = LruCache<K, V, S, A>;

    fn deref(&self) -> &Self::Target {
        match self {
            ExecutorCache::Managed(cache) => &cache.inner,
            ExecutorCache::Local(cache) => &cache.inner,
        }
    }
}

impl<K, V, S, A: Clone + Allocator> DerefMut for ExecutorCache<K, V, S, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            ExecutorCache::Managed(cache) => &mut cache.inner,
            ExecutorCache::Local(cache) => &mut cache.inner,
        }
    }
}

impl LruManager {
    const EVICTION_THRESHOLD_AGGRESSIVE: f64 = 0.9;
    const EVICTION_THRESHOLD_GRACEFUL: f64 = 0.7f64;

    pub fn new(total_memory_available_bytes: usize, barrier_interval_ms: u32) -> Arc<Self> {
        let manager = Arc::new(Self {
            watermark_epoch: Arc::new(0.into()),
            total_memory_available_bytes,
            barrier_interval_ms,
        });

        // Run a background memory monitor
        tokio::spawn(manager.clone().run());

        manager
    }

    /// We should not call `run` in unit test.
    #[cfg(test)]
    pub fn for_test() -> Arc<Self> {
        Arc::new(Self {
            watermark_epoch: Arc::new(0.into()),
            total_memory_available_bytes: 0,
            barrier_interval_ms: 0,
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

    fn set_watermark_time(&self, time: u64) {
        let epoch = Epoch::from_physical_time(time).0;
        let watermark_epoch = self.watermark_epoch.as_ref();
        watermark_epoch.store(epoch, Ordering::Relaxed);
    }

    pub async fn run(self: Arc<Self>) {
        let mem_threshold_graceful =
            (self.total_memory_available_bytes as f64 * Self::EVICTION_THRESHOLD_GRACEFUL) as usize;
        let mem_threshold_aggressive = (self.total_memory_available_bytes as f64
            * Self::EVICTION_THRESHOLD_AGGRESSIVE) as usize;

        let mut watermark_time = Epoch::physical_now();
        let mut last_total_bytes_used = 0;
        let mut step = 0;

        loop {
            // Wait for a while to check if need eviction.
            sleep(Duration::from_millis(self.barrier_interval_ms as u64)).await;

            let stats = INSTRUMENTED_JEMALLOC.stats();
            let cur_total_bytes_used = stats.bytes_allocated - stats.bytes_deallocated;

            step = if cur_total_bytes_used < mem_threshold_graceful {
                // Do not evict if the memory usage is lower than `mem_threshold_graceful`
                0
            } else if cur_total_bytes_used < mem_threshold_aggressive {
                // Gracefully evict
                if last_total_bytes_used < cur_total_bytes_used {
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
            watermark_time += self.barrier_interval_ms as u64 * step;

            self.set_watermark_time(watermark_time);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
