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

use crossbeam_queue::SegQueue;
use dashmap::DashSet;

use crate::fifo_cache::CacheKey;

pub struct GhostCache<K: CacheKey> {
    map: DashSet<K>,
    queue: SegQueue<K>,
}

impl<K: CacheKey> GhostCache<K> {
    pub fn new() -> Self {
        Self {
            map: DashSet::default(),
            queue: SegQueue::new(),
        }
    }

    pub fn insert(&self, key: &K, max_capacity: usize) {
        if !self.map.insert(key.clone()) {
            return;
        }
        // avoid push fail
        while self.queue.len() >= max_capacity {
            if let Some(expire_key) = self.queue.pop() {
                self.map.remove(&expire_key);
            }
        }
        self.queue.push(key.clone());
    }

    pub fn is_ghost(&self, key: &K) -> bool {
        self.map.contains(key)
    }
}
