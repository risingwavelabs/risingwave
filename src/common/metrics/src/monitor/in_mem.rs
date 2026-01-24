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
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Weak};

use parking_lot::Mutex;

pub type Count = Arc<AtomicU64>;

pub trait CountMapIdTrait = Copy + std::hash::Hash + Eq;

pub struct GuardedCount<I: CountMapIdTrait> {
    id: I,
    pub count: Count,
    parent: Weak<Mutex<InnerCountMap<I>>>,
}

impl<I: CountMapIdTrait> GuardedCount<I> {
    pub fn new(id: I, parent: &Arc<Mutex<InnerCountMap<I>>>) -> (Count, Self) {
        let guard = GuardedCount {
            id,
            count: Arc::new(AtomicU64::new(0)),
            parent: Arc::downgrade(parent),
        };
        (guard.count.clone(), guard)
    }
}

impl<I: CountMapIdTrait> Drop for GuardedCount<I> {
    fn drop(&mut self) {
        if let Some(parent) = self.parent.upgrade() {
            let mut map = parent.lock();
            map.inner.remove(&self.id);
        }
    }
}

pub struct InnerCountMap<I: CountMapIdTrait> {
    inner: HashMap<I, Count>,
}

#[derive(Clone)]
pub struct CountMap<I: CountMapIdTrait>(Arc<Mutex<InnerCountMap<I>>>);

impl<I: CountMapIdTrait> CountMap<I> {
    pub fn new() -> Self {
        let inner = Arc::new(Mutex::new(InnerCountMap {
            inner: HashMap::new(),
        }));
        CountMap(inner)
    }

    pub fn new_count(&self, id: I) -> GuardedCount<I> {
        let inner = &self.0;
        let (count, guarded_count) = GuardedCount::new(id, inner);
        let mut map = inner.lock();
        map.inner.insert(id, count);
        guarded_count
    }

    pub fn collect(&self, ids: &[I]) -> HashMap<I, u64> {
        let map = self.0.lock();
        ids.iter()
            .filter_map(|id| {
                map.inner
                    .get(id)
                    .map(|v| (*id, v.load(std::sync::atomic::Ordering::Relaxed)))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_map() {
        let count_map = CountMap::<u64>::new();
        let count1 = count_map.new_count(1);
        let count2 = count_map.new_count(2);
        count1
            .count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        count2
            .count
            .fetch_add(2, std::sync::atomic::Ordering::Relaxed);
        let counts = count_map.collect(&[1, 2]);
        assert_eq!(counts[&1], 1);
        assert_eq!(counts[&2], 2);
    }

    #[test]
    fn test_count_map_drop() {
        let count_map = CountMap::<u64>::new();
        let count1 = count_map.new_count(1);
        let count2 = count_map.new_count(2);
        count1
            .count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        count2
            .count
            .fetch_add(2, std::sync::atomic::Ordering::Relaxed);
        let counts = count_map.collect(&[1, 2]);
        assert_eq!(counts[&1], 1);
        assert_eq!(counts[&2], 2);
        drop(count1);
        let counts = count_map.collect(&[1, 2]);
        assert_eq!(counts.get(&1), None);
        assert_eq!(counts.get(&2), Some(2).as_ref());
    }
}
