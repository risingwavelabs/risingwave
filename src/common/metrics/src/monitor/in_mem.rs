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

//! This module contains data structures for in-memory monitoring.
//! It is intentionally decoupled from Prometheus.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use parking_lot::RwLock;

pub type Count = Arc<AtomicU64>;

#[derive(Clone)]
pub struct CountMap(Arc<RwLock<HashMap<u64, Count>>>);

impl CountMap {
    pub fn new() -> Self {
        let inner = Arc::new(RwLock::new(HashMap::new()));
        #[cfg(all(not(test), not(madsim)))]
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

    pub fn new_or_get_counter(&self, id: u64) -> Count {
        {
            let map = self.0.read();
            if let Some(counter) = map.get(&id) {
                return counter.clone();
            }
        }
        let mut map = self.0.write();
        map.entry(id)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone()
    }

    pub fn collect(&self, operator_ids: &[u64]) -> HashMap<u64, u64> {
        let map = self.0.read();
        operator_ids
            .iter()
            .filter_map(|id| {
                map.get(id)
                    .map(|v| (*id, v.load(std::sync::atomic::Ordering::Relaxed)))
            })
            .collect()
    }

    /// GC policy: if more than half of the counters are dropped, then do GC.
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
        tracing::info!("Size of CountMap after GC: {}", map.len());
    }
}
