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

use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_queue::SegQueue;

use crate::fifo_cache::{CacheItem, CacheKey, CacheValue};

const MAX_EVICT_LOOP: usize = 20;

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
        let mut idx = 0;
        while let Some(item) = self.queue.pop() {
            if item.dec_freq() && idx < MAX_EVICT_LOOP {
                idx += 1;
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

    pub fn clear(&self) {
        while !self.queue.is_empty() {
            self.queue.pop();
        }
        self.cost.store(0, Ordering::Release);
    }
}
