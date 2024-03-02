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

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::fifo_cache::{CacheItem, CacheKey, CacheValue};

const MAX_EVICT_LOOP: usize = 20;

pub struct MainCache<K: CacheKey, V: CacheValue> {
    queue: VecDeque<Box<CacheItem<K, V>>>,
    cost: Arc<AtomicUsize>,
    capacity: usize,
}

impl<K: CacheKey, V: CacheValue> MainCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::new(),
            cost: Arc::new(AtomicUsize::new(0)),
            capacity,
        }
    }

    pub fn get_size_counter(&self) -> Arc<AtomicUsize> {
        self.cost.clone()
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

    pub fn evict(&mut self) -> Option<Box<CacheItem<K, V>>> {
        let mut idx = 0;
        while let Some(mut item) = self.queue.pop_front() {
            if !item.dec_freq() || idx >= MAX_EVICT_LOOP {
                self.cost
                    .fetch_sub(item.cost(), std::sync::atomic::Ordering::Release);
                return Some(item);
            }
            self.queue.push_back(item);
            idx += 1;
        }
        None
    }

    pub fn insert(&mut self, mut item: Box<CacheItem<K, V>>) {
        item.mark_main();
        self.cost
            .fetch_add(item.cost(), std::sync::atomic::Ordering::Release);
        self.queue.push_back(item);
    }

    pub fn clear(&mut self) {
        self.queue.clear();
        self.cost.store(0, Ordering::Release);
    }
}
