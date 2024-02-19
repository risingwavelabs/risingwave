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

use std::sync::Arc;

use dashmap::DashMap;

use crate::fifo_cache::ghost::GhostCache;
use crate::fifo_cache::most::MostCache;
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
pub struct FIFOCache<K: CacheKey, V: CacheValue> {
    map: Arc<DashMap<K, CacheHandle<K, V>>>,
    small: SmallHotCache<K, V>,
    main: MostCache<K, V>,
    ghost: GhostCache<K>,
    capacity: usize,
}

impl<K: CacheKey, V: CacheValue> FIFOCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        let main = MostCache::new(capacity * 9 / 10);
        Self {
            map: Arc::new(DashMap::new()),
            small: SmallHotCache::new(),
            main,
            ghost: GhostCache::new(),
            capacity,
        }
    }

    pub fn get(&self, k: &K) -> Option<V> {
        if let Some(handle) = self.map.get(k) {
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

    pub fn clear(&self) {
        self.small.clear();
        self.main.clear();
    }

    pub fn is_full(&self) -> bool {
        self.size() > self.capacity
    }

    pub fn contains(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub fn evict(&self, ghost_capacity: usize, deleted: &mut Vec<Box<CacheItem<K, V>>>) {
        if self.small.size() > self.capacity / 10 {
            if let Some(item) = self.small.evict() {
                if item.get_freq() > 1 {
                    self.main.insert(item);
                } else {
                    self.ghost.insert(&item.key, ghost_capacity);
                    self.map.remove(&item.key);
                    deleted.push(item);
                }
            }
        }
        if self.main.is_full() {
            if let Some(item) = self.main.evict() {
                deleted.push(item);
            }
        }
    }

    pub fn insert(&self, key: K, value: V, cost: usize) {
        let mut deleted = vec![];
        let cache_count = self.small.count() + self.main.count();
        let ghost_capacity = std::cmp::max(cache_count, self.capacity / 1000);
        while self.is_full() {
            self.evict(ghost_capacity, &mut deleted);
        }
        for item in &deleted {
            self.map.remove(&item.key);
        }
        let is_ghost = self.ghost.is_ghost(&key);
        let item = Box::new(CacheItem::new(key.clone(), value, cost));
        unsafe {
            let addr = Box::into_raw(item);
            let handle = CacheHandle { item: addr };
            if is_ghost {
                self.main.insert(Box::from_raw(addr));
            } else {
                self.small.insert(Box::from_raw(addr));
            }
            self.map.insert(key.clone(), handle);
        }
    }
}


