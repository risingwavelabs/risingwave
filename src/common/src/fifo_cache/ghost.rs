use crossbeam_queue::SegQueue;
use dashmap::DashMap;

use crate::fifo_cache::{CacheItem, CacheKey, CacheValue};

pub struct GhostCache<K: CacheKey, V: CacheValue> {
    map: DashMap<K, Box<CacheItem<K, V>>>,
    queue: SegQueue<K>,
}

impl<K: CacheKey, V: CacheValue> GhostCache<K, V> {
    pub fn new() -> Self {
        Self {
            map: DashMap::default(),
            queue: SegQueue::new(),
        }
    }

    pub fn insert(&self, item: Box<CacheItem<K, V>>, max_capacity: usize, deleted: &mut Vec<Box<CacheItem<K, V>>>) {
        let key = item.key.clone();
        item.mark_ghost();
        if self.map.insert(item.key.clone(), item).is_some() {
            return;
        }
        // avoid push fail
        while self.queue.len() >= max_capacity {
            if let Some(expire_key) = self.queue.pop() {
                if let Some((_, expire_item)) = self.map.remove(&expire_key) {
                    deleted.push(expire_item);
                }
            }
        }
        self.queue.push(key);
    }

    pub fn remove(&self, key: &K) -> Option<Box<CacheItem<K, V>>> {
        let (_, item) = self.map.remove(key)?;
        Some(item)
    }
}
