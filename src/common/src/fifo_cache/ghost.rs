use crossbeam_queue::ArrayQueue;
use dashmap::DashMap;

use crate::fifo_cache::{CacheItem, CacheKey, CacheValue};

pub struct GhostCache<K: CacheKey, V: CacheValue> {
    map: DashMap<K, Box<CacheItem<K, V>>>,
    queue: ArrayQueue<K>,
    capacity: usize,
}

impl<K: CacheKey, V: CacheValue> GhostCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            map: DashMap::default(),
            queue: ArrayQueue::new(capacity),
            capacity,
        }
    }

    pub fn insert(&self, item: Box<CacheItem<K, V>>, deleted: &mut Vec<Box<CacheItem<K, V>>>) {
        let mut key = item.key.clone();
        item.mark_ghost();
        self.map.insert(item.key.clone(), item);
        while let Err(e) = self.queue.push(key) {
            if let Some(expire_key) = self.queue.pop() {
                if let Some((_, expire_item)) = self.map.remove(&expire_key) {
                    deleted.push(expire_item);
                }
            }
            key = e;
        }
    }

    pub fn remove(&self, key: &K) -> Option<Box<CacheItem<K, V>>> {
        let (_, item) = self.map.remove(key)?;
        Some(item)
    }
}
