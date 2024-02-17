use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crossbeam_queue::SegQueue;

use crate::fifo_cache::most::MostCache;
use crate::fifo_cache::{CacheItem, CacheKey, CacheValue};

pub struct SmallHotCache<K: CacheKey, V: CacheValue> {
    main: Arc<MostCache<K, V>>,
    queue: SegQueue<Box<CacheItem<K, V>>>,
    cost: AtomicUsize,
}

impl<K: CacheKey, V: CacheValue> SmallHotCache<K, V> {
    pub fn new(main: Arc<MostCache<K, V>>) -> Self {
        Self {
            main,
            queue: SegQueue::new(),
            cost: AtomicUsize::new(0),
        }
    }

    pub fn size(&self) -> usize {
        self.cost.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn evict(&self) -> Option<Box<CacheItem<K, V>>> {
        while self.size() > 0 {
            let item = self.queue.pop()?;
            self.cost
                .fetch_sub(item.cost(), std::sync::atomic::Ordering::Release);
            if item.get_freq() > 1 {
                self.main.insert(item);
            } else {
                return Some(item);
            }
        }
        None
    }

    pub fn insert(&self, item: Box<CacheItem<K, V>>) {
        assert!(item.mark_small());
        self.cost
            .fetch_add(item.cost(), std::sync::atomic::Ordering::Release);
        self.queue.push(item);
    }
}
