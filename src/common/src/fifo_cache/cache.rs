use std::sync::Arc;

use dashmap::DashMap;

use crate::fifo_cache::ghost::GhostCache;
use crate::fifo_cache::most::MostCache;
use crate::fifo_cache::small::SmallHotCache;
use crate::fifo_cache::{CacheItem, CacheKey, CacheValue, KIND_GHOST};

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
    main: Arc<MostCache<K, V>>,
    ghost: GhostCache<K, V>,
    capacity: usize,
}

impl<K: CacheKey, V: CacheValue> FIFOCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        let main = Arc::new(MostCache::new(capacity * 9 / 10));
        Self {
            map: Arc::new(DashMap::new()),
            small: SmallHotCache::new(main.clone()),
            main,
            ghost: GhostCache::new(),
            capacity,
        }
    }

    pub fn get(&self, k: &K) -> Option<V> {
        if let Some(handle) = self.map.get(k) {
            unsafe {
                let v = (*handle.item).value.clone();
                if (*handle.item).kind() != KIND_GHOST {
                    (*handle.item).inc_freq();
                } else {
                    // insert item of ghost to main.
                    if let Some(item) = self.ghost.remove(k) {
                        item.reset_freq();
                        self.main.insert(item);
                    }
                }
                return Some(v);
            }
        }
        None
    }

    pub fn size(&self) -> usize {
        self.small.size() + self.main.size()
    }

    pub fn is_full(&self) -> bool {
        self.size() > self.capacity
    }

    pub fn evict(&self, ghost_capacity: usize, deleted: &mut Vec<Box<CacheItem<K, V>>>) {
        if self.small.size() > self.capacity / 10 {
            if let Some(item) = self.small.evict() {
                self.ghost.insert(item, ghost_capacity, deleted);
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
        let small_count = self.small.count();
        let ghost_capacity = small_count + std::cmp::max( small_count + 1, self.main.count());
        while self.is_full() {
            self.evict(ghost_capacity, &mut deleted);
        }
        for item in &deleted {
            self.map.remove(&item.key);
        }
        let item = Box::new(CacheItem::new(key.clone(), value, cost));
        unsafe {
            let addr = Box::into_raw(item);
            let handle = CacheHandle { item: addr };
            self.small.insert(Box::from_raw(addr));
            self.map.insert(key.clone(), handle);
        }
    }
}
