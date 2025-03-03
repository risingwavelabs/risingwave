// Copyright 2025 RisingWave Labs
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

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

use parking_lot::RwLock;

pub type Count = Arc<AtomicU32>;

#[derive(Clone)]
pub struct CountMap(Arc<RwLock<HashMap<u64, Count>>>);

impl CountMap {
    pub(crate) fn new() -> Self {
        let inner = Arc::new(RwLock::new(HashMap::new()));
        {
            let inner = inner.clone();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                    if Self::should_gc(&inner.read()) {
                        Self::gc(&mut inner.write());
                    }
                }
            });
        }
        CountMap(inner)
    }

    pub(crate) fn new_or_get_counter(&self, id: u64) -> Count {
        {
            let map = self.0.read();
            if let Some(counter) = map.get(&id) {
                return counter.clone();
            }
        }
        let mut map = self.0.write();
        let counter = map
            .entry(id)
            .or_insert_with(|| Arc::new(AtomicU32::new(0)))
            .clone();
        counter
    }

    pub fn collect(&self) -> HashMap<u64, u32> {
        let map = self.0.read();
        map.iter()
            .map(|(&k, v)| (k, v.load(std::sync::atomic::Ordering::Relaxed)))
            .collect()
    }

    /// GC at 50% drop rate
    pub fn should_gc(map: &HashMap<u64, Count>) -> bool {
        let total = map.len();
        let dropped = map
            .iter()
            .filter(|(_, v)| Arc::strong_count(v) <= 1)
            .count();
        if dropped * 2 > total {
            return true;
        }
        false
    }

    pub fn gc(map: &mut HashMap<u64, Count>) {
        map.retain(|_, v| Arc::strong_count(v) > 1);
    }
}
