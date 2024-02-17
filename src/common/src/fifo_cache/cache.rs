use std::sync::Arc;

use dashmap::DashMap;

use crate::fifo_cache::ghost::GhostCache;
use crate::fifo_cache::most::MostCache;
use crate::fifo_cache::small::SmallHotCache;
use crate::fifo_cache::{CacheItem, CacheKey, CacheValue};

pub struct CacheHandle<K: CacheKey, V: CacheValue> {
    item: *mut CacheItem<K, V>,
}

impl<K: CacheKey, V: CacheValue> Clone for CacheHandle<K, V> {
    fn clone(&self) -> Self {
        Self { item: self.item }
    }
}
pub struct FIFOCache<K: CacheKey, V: CacheValue> {
    map: DashMap<K, CacheHandle<K, V>>,
    small: SmallHotCache<K, V>,
    main: Arc<MostCache<K, V>>,
    ghost: GhostCache<K, V>,
    capacity: usize,
}

impl<K: CacheKey, V: CacheValue> FIFOCache<K, V> {
    pub fn new(capacity: usize, ghost_capacity: usize) -> Self {
        let main = Arc::new(MostCache::new(capacity * 9 / 10));
        Self {
            map: DashMap::new(),
            small: SmallHotCache::new(main.clone()),
            main,
            ghost: GhostCache::new(ghost_capacity),
            capacity,
        }
    }

    pub fn get(&self, k: &K) -> Option<V> {
        if let Some(handle) = self.map.get(k) {
            unsafe {
                let v = (*handle.item).value.clone();
                if (*handle.item).kind() != 12 {
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

    pub fn evict(&self, deleted: &mut Vec<Box<CacheItem<K, V>>>) {
        let item = if self.small.size() > self.capacity / 10 {
            self.small.evict()
        } else {
            self.main.evict()
        };
        if let Some(item) = item {
            self.ghost.insert(item, deleted);
        }
    }

    pub fn insert(&self, key: K, value: V, cost: usize) {
        let mut deleted = vec![];
        while self.is_full() {
            self.evict(&mut deleted);
        }
        for item in deleted {
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
