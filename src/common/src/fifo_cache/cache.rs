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

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::fifo_cache::ghost::GhostCache;
use crate::fifo_cache::most::MainCache;
use crate::fifo_cache::small::SmallHotCache;
use crate::fifo_cache::{CacheItem, CacheKey, CacheValue};

pub struct CacheHandle<K: CacheKey, V: CacheValue> {
    item: *mut CacheItem<K, V>,
}

unsafe impl<K: CacheKey, V: CacheValue> Send for CacheHandle<K, V> {}
unsafe impl<K: CacheKey, V: CacheValue> Sync for CacheHandle<K, V> {}

impl<K: CacheKey, V: CacheValue> Clone for CacheHandle<K, V> {
    fn clone(&self) -> Self {
        Self { item: self.item }
    }
}
pub struct FIFOCacheShard<K: CacheKey, V: CacheValue> {
    map: HashMap<K, CacheHandle<K, V>>,
    small: SmallHotCache<K, V>,
    main: MainCache<K, V>,
    ghost: GhostCache,
    evict_small_times: usize,
    evict_main_times: usize,
    insert_in_ghost: usize,

    capacity: usize,
}

impl<K: CacheKey, V: CacheValue> FIFOCacheShard<K, V> {
    pub fn new(capacity: usize) -> Self {
        let small = SmallHotCache::new(capacity / 5);
        let main = MainCache::new(capacity * 4 / 5);
        Self {
            map: HashMap::new(),
            small,
            main,
            ghost: GhostCache::new(),
            evict_main_times: 0,
            evict_small_times: 0,
            insert_in_ghost: 0,
            capacity,
        }
    }

    pub fn get(&mut self, k: &K) -> Option<V> {
        if let Some(handle) = self.map.get_mut(k) {
            unsafe {
                let v = (*handle.item).value.clone();
                (*handle.item).inc_freq();
                return Some(v);
            }
        }
        None
    }

    pub fn size(&self) -> usize {
        self.small.size() + self.main.size()
    }

    pub fn clear(&mut self) {
        self.small.clear();
        self.main.clear();
    }

    pub fn is_full(&self) -> bool {
        self.size() > self.capacity
    }

    pub fn contains(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub fn evict(&mut self, ghost_capacity: usize, deleted: &mut Vec<Box<CacheItem<K, V>>>) {
        if self.small.is_full() {
            if let Some(item) = self.small.evict() {
                if item.get_freq() > 0 {
                    self.main.insert(item);
                } else {
                    self.evict_small_times += 1;
                    self.ghost.insert(item.hash(), ghost_capacity);
                    self.map.remove(&item.key);
                    deleted.push(item);
                }
            }
        }
        if self.main.is_full() {
            if let Some(item) = self.main.evict() {
                self.evict_main_times += 1;
                deleted.push(item);
            }
        }
    }

    pub fn insert(
        &mut self,
        key: K,
        value: V,
        h: u64,
        cost: usize,
        deleted: &mut Vec<Box<CacheItem<K, V>>>,
    ) {
        if let Some(handle) = self.map.get_mut(&key) {
            unsafe {
                (*handle.item).inc_freq();
            }
            return;
        }
        let mut to_delete = vec![];
        let ghost_capacity = (self.small.count() + self.main.count()) * 10;
        while self.is_full() {
            self.evict(ghost_capacity, &mut to_delete);
        }
        for item in &to_delete {
            self.map.remove(&item.key);
        }
        if !to_delete.is_empty() {
            deleted.extend(to_delete);
        }
        let is_ghost = self.ghost.is_ghost(h);
        let mut item = Box::new(CacheItem::new(key.clone(), value, cost));
        let addr = item.as_mut();
        // let addr = Box::into_raw(item);
        let handle = CacheHandle { item: addr };
        self.map.insert(key.clone(), handle);
        if is_ghost {
            self.insert_in_ghost += 1;
            self.main.insert(item);
        } else {
            self.small.insert(item);
        }
    }

    fn debug_print(&mut self) -> String {
        let ret = format!(
            "evict_small_times: {} evict_main_times: {}, insert_in_ghost: {}",
            self.evict_small_times, self.evict_main_times, self.insert_in_ghost,
        );
        self.evict_small_times = 0;
        self.evict_main_times = 0;
        self.insert_in_ghost = 0;
        ret
    }
}

pub struct FIFOCache<K: CacheKey, T: CacheValue> {
    shards: Vec<Mutex<FIFOCacheShard<K, T>>>,
    usage_counters: Vec<Arc<AtomicUsize>>,
}

impl<K: CacheKey, T: CacheValue> FIFOCache<K, T> {
    pub fn new(num_shard_bits: usize, capacity: usize) -> Self {
        let num_shards = 1 << num_shard_bits;
        let mut shards = Vec::with_capacity(num_shards);
        let per_shard = capacity / num_shards;
        let mut usage_counters = Vec::with_capacity(num_shards * 2);
        for _ in 0..num_shards {
            let shard = FIFOCacheShard::new(per_shard);
            usage_counters.push(shard.small.get_size_counter());
            usage_counters.push(shard.main.get_size_counter());
            shards.push(Mutex::new(shard));
        }
        Self {
            shards,
            usage_counters,
        }
    }

    pub fn contains(self: &Arc<Self>, key: &K) -> bool {
        let shard = self.shards[self.shard(key)].lock();
        shard.contains(key)
    }

    pub fn lookup(self: &Arc<Self>, key: &K) -> Option<T> {
        let mut shard = self.shards[self.shard(key)].lock();
        shard.get(key)
    }

    fn shard(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        hash as usize % self.shards.len()
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            let mut shard = shard.lock();
            shard.clear();
        }
    }

    pub fn get_memory_usage(&self) -> usize {
        self.usage_counters
            .iter()
            .map(|x| x.load(std::sync::atomic::Ordering::Acquire))
            .sum()
    }

    pub fn debug_print(&self) -> String {
        let mut s = "FIFOCache: [".to_string();
        for shard in &self.shards {
            let mut shard = shard.lock();
            s += &(shard.debug_print() + ", ");
        }
        s.pop();
        s.pop();
        s + "]"
    }

    pub fn insert(self: &Arc<Self>, key: K, value: T, charge: usize) {
        let mut to_delete = vec![];
        // Drop the entries outside lock to avoid deadlock.
        let mut hasher = DefaultHasher::default();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        {
            let mut shard = self.shards[hash as usize % self.shards.len()].lock();
            shard.insert(key, value, hash, charge, &mut to_delete);
        }
    }
}
