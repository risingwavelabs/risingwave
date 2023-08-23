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

use std::collections::BTreeMap;
use std::ops::{Bound, RangeInclusive};

use risingwave_common::estimate_size::{EstimateSize, KvSize};

pub struct EstimatedBTreeMap<K, V> {
    inner: BTreeMap<K, V>,
    heap_size: KvSize,
}

impl<K, V> EstimatedBTreeMap<K, V> {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
            heap_size: KvSize::new(),
        }
    }

    pub fn inner(&self) -> &BTreeMap<K, V> {
        &self.inner
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl<K, V> Default for EstimatedBTreeMap<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> EstimatedBTreeMap<K, V>
where
    K: EstimateSize + Ord,
    V: EstimateSize,
{
    pub fn first_key_value(&self) -> Option<(&K, &V)> {
        self.inner.first_key_value()
    }

    pub fn last_key_value(&self) -> Option<(&K, &V)> {
        self.inner.last_key_value()
    }

    pub fn insert(&mut self, key: K, row: V) {
        let key_size = self.heap_size.add_val(&key);
        self.heap_size.add_val(&row);
        if let Some(old_row) = self.inner.insert(key, row) {
            self.heap_size.sub_size(key_size);
            self.heap_size.sub_val(&old_row);
        }
    }

    pub fn remove(&mut self, key: &K) {
        if let Some(row) = self.inner.remove(key) {
            self.heap_size.sub(key, &row);
        }
    }

    #[expect(dead_code)]
    pub fn clear(&mut self) {
        self.inner.clear();
        self.heap_size.set(0);
    }

    /// Retain the given range of entries in the map, removing others.
    pub fn retain_range(&mut self, range: RangeInclusive<&K>) -> (BTreeMap<K, V>, BTreeMap<K, V>)
    where
        K: Clone,
    {
        let start = *range.start();
        let end = *range.end();

        // [ left, [mid], right ]
        let mut mid_right = self.inner.split_off(start);
        let mid_right_split_key = mid_right.lower_bound(Bound::Excluded(end)).key().cloned();
        let right = if let Some(ref mid_right_split_key) = mid_right_split_key {
            mid_right.split_off(mid_right_split_key)
        } else {
            Default::default()
        };
        let mid = mid_right;
        let left = std::mem::replace(&mut self.inner, mid);

        for (k, v) in &left {
            self.heap_size.sub(k, v);
        }
        for (k, v) in &right {
            self.heap_size.sub(k, v);
        }

        (left, right)
    }
}

impl<K, V> EstimateSize for EstimatedBTreeMap<K, V>
where
    K: EstimateSize,
    V: EstimateSize,
{
    fn estimated_heap_size(&self) -> usize {
        self.heap_size.size()
    }
}

#[cfg(test)]
mod tests {
    use super::EstimatedBTreeMap;

    #[test]
    fn test_retain_range() {
        let mut map = EstimatedBTreeMap::new();

        let (left, right) = map.retain_range(&1..=&10);
        assert!(left.is_empty());
        assert!(right.is_empty());

        map.insert(1, "hello".to_string());
        map.insert(6, "world".to_string());
        let (left, right) = map.retain_range(&6..=&6);
        assert_eq!(map.len(), 1);
        assert_eq!(map.inner[&6], "world".to_string());
        assert_eq!(left.len(), 1);
        assert_eq!(left[&1], "hello".to_string());
        assert!(right.is_empty());

        map.insert(8, "risingwave".to_string());
        map.insert(3, "great".to_string());
        map.insert(0, "wooow".to_string());
        let (left, right) = map.retain_range(&2..=&7);
        assert_eq!(map.len(), 2);
        assert_eq!(map.inner[&3], "great".to_string());
        assert_eq!(map.inner[&6], "world".to_string());
        assert_eq!(left.len(), 1);
        assert_eq!(left[&0], "wooow".to_string());
        assert_eq!(right.len(), 1);
        assert_eq!(right[&8], "risingwave".to_string());
    }
}
