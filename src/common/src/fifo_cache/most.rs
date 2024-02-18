use std::sync::atomic::AtomicUsize;

use crossbeam_queue::SegQueue;

use crate::fifo_cache::{CacheItem, CacheKey, CacheValue};

pub struct MostCache<K: CacheKey, V: CacheValue> {
    queue: SegQueue<Box<CacheItem<K, V>>>,
    cost: AtomicUsize,
    capacity: usize,
}

impl<K: CacheKey, V: CacheValue> MostCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: SegQueue::new(),
            cost: AtomicUsize::new(0),
            capacity,
        }
    }

    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.size() >= self.capacity
    }

    #[inline(always)]
    pub fn size(&self) -> usize {
        self.cost.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn count(&self) -> usize {
        self.queue.len()
    }

    pub fn evict(&self) -> Option<Box<CacheItem<K, V>>> {
        while self.size() > 0 {
            let item = self.queue.pop()?;
            if item.dec_freq() {
                self.queue.push(item);
            } else {
                self.cost
                    .fetch_sub(item.cost(), std::sync::atomic::Ordering::Release);
                return Some(item);
            }
        }
        None
    }

    pub fn insert(&self, item: Box<CacheItem<K, V>>) {
        assert!(item.mark_main());
        self.cost
            .fetch_add(item.cost(), std::sync::atomic::Ordering::Release);
        self.queue.push(item);
    }
}
