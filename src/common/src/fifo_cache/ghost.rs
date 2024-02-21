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

use std::collections::{HashSet, VecDeque};

pub struct GhostCache {
    map: HashSet<u64>,
    queue: VecDeque<u64>,
}

impl GhostCache {
    pub fn new() -> Self {
        Self {
            map: HashSet::default(),
            queue: VecDeque::new(),
        }
    }

    pub fn insert(&mut self, key_hash: u64, max_capacity: usize) {
        if !self.map.insert(key_hash) {
            return;
        }
        // avoid push fail
        while self.queue.len() >= max_capacity {
            if let Some(expire_key) = self.queue.pop_front() {
                self.map.remove(&expire_key);
            }
        }
        self.queue.push_back(key_hash);
    }

    pub fn is_ghost(&self, key: u64) -> bool {
        self.map.contains(&key)
    }
}
