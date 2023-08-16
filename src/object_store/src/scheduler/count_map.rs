// Copyright 2023 RisingWave Labs
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

use std::cmp::Ordering::Equal;
use std::collections::BTreeMap;
use std::ops::Bound::Included;

#[derive(Debug)]
struct Counter<V> {
    count: usize,
    value: V,
}

#[derive(Debug)]
pub struct CountMap<K: Ord, V> {
    map: BTreeMap<K, Counter<V>>,
}

impl<K: Ord, V> Default for CountMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord, V> CountMap<K, V> {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.insert_with_count(key, value, 1)
    }

    /// `count` must >= 1.
    pub fn insert_with_count(&mut self, key: K, value: V, count: usize) {
        assert!(count >= 1);
        let old = self.map.insert(key, Counter { count, value });
        assert!(old.is_none());
    }

    pub fn increase(&mut self, key: &K) {
        self.map.get_mut(key).unwrap().count += 1;
    }

    pub fn decrease(&mut self, key: &K) -> Option<V> {
        let mut cursor = self.map.lower_bound_mut(Included(key));
        assert_eq!(key.cmp(cursor.key().unwrap()), Equal);

        let counter = cursor.value_mut().unwrap();
        counter.count -= 1;

        if counter.count == 0 {
            let (_, counter) = cursor.remove_current().unwrap();
            return Some(counter.value);
        }
        None
    }
}
