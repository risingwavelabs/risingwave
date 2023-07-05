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

use risingwave_common::estimate_size::{EstimateSize, KvSize};

/// Inner top-N cache structure for [`super::TopNStateCache`].
pub struct TopNCache<K: Ord + EstimateSize, V: EstimateSize> {
    /// The capacity of the cache.
    capacity: usize,
    /// Ordered cache entries.
    entries: BTreeMap<K, V>,
    kv_heap_size: KvSize,
}

impl<K: Ord + EstimateSize, V: EstimateSize> EstimateSize for TopNCache<K, V> {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Add btreemap internal size.
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.kv_heap_size.size()
    }
}

impl<K: Ord + EstimateSize, V: EstimateSize> TopNCache<K, V> {
    /// Create a new cache with specified capacity and order requirements.
    /// To create a cache with unlimited capacity, use `usize::MAX` for `capacity`.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Default::default(),
            kv_heap_size: KvSize::new(),
        }
    }

    /// Get the capacity of the cache.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the number of entries in the cache.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clear the cache.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.kv_heap_size.set(0);
    }

    /// Insert an entry into the cache.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let key_size = self.kv_heap_size.add_val(&key);
        self.kv_heap_size.add_val(&value);
        let old_val = self.entries.insert(key, value);
        if let Some(old_val) = &old_val {
            self.kv_heap_size.sub_size(key_size);
            self.kv_heap_size.sub_val(old_val);
        }
        // evict if capacity is reached
        while self.entries.len() > self.capacity {
            if let Some((key, val)) = self.entries.pop_last() {
                self.kv_heap_size.sub(&key, &val);
            }
        }
        old_val
    }

    /// Remove an entry from the cache.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        let old_val = self.entries.remove(key);
        if let Some(val) = &old_val {
            self.kv_heap_size.sub(key, val);
        }
        old_val
    }

    /// Get the first (smallest) key-value pair in the cache.
    pub fn first_key_value(&self) -> Option<(&K, &V)> {
        self.entries.first_key_value()
    }

    /// Get the first (smallest) key in the cache.
    pub fn first_key(&self) -> Option<&K> {
        self.first_key_value().map(|(k, _)| k)
    }

    /// Get the last (largest) key-value pair in the cache.
    pub fn last_key_value(&self) -> Option<(&K, &V)> {
        self.entries.last_key_value()
    }

    /// Get the last (largest) key in the cache.
    pub fn last_key(&self) -> Option<&K> {
        self.last_key_value().map(|(k, _)| k)
    }

    /// Iterate over the values in the cache.
    pub fn values(&self) -> impl Iterator<Item = &V> {
        self.entries.values()
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_top_n_cache() {
        let mut cache = TopNCache::new(3);
        assert_eq!(cache.capacity(), 3);
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
        assert!(cache.first_key_value().is_none());
        assert!(cache.first_key().is_none());
        assert!(cache.last_key_value().is_none());
        assert!(cache.last_key().is_none());
        assert!(cache.values().collect_vec().is_empty());

        let old_val = cache.insert(5, "hello".to_string());
        assert!(old_val.is_none());
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
        assert_eq!(cache.values().collect_vec(), vec!["hello"]);

        cache.insert(3, "world".to_string());
        cache.insert(1, "risingwave!".to_string());
        assert_eq!(cache.len(), 3);
        assert_eq!(
            cache.first_key_value(),
            Some((&1, &"risingwave!".to_string()))
        );
        assert_eq!(cache.first_key(), Some(&1));
        assert_eq!(cache.last_key_value(), Some((&5, &"hello".to_string())));
        assert_eq!(cache.last_key(), Some(&5));
        assert_eq!(
            cache.values().collect_vec(),
            vec!["risingwave!", "world", "hello"]
        );

        cache.insert(0, "foo".to_string());
        assert_eq!(cache.capacity(), 3);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.first_key(), Some(&0));
        assert_eq!(cache.last_key(), Some(&3));
        assert_eq!(
            cache.values().collect_vec(),
            vec!["foo", "risingwave!", "world"]
        );

        let old_val = cache.remove(&0);
        assert_eq!(old_val, Some("foo".to_string()));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.first_key(), Some(&1));
        assert_eq!(cache.last_key(), Some(&3));
        cache.remove(&3);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.first_key(), Some(&1));
        assert_eq!(cache.last_key(), Some(&1));
        let old_val = cache.remove(&100); // can remove non-existing key
        assert!(old_val.is_none());
        assert_eq!(cache.len(), 1);

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.capacity(), 3);
        assert_eq!(cache.first_key(), None);
        assert_eq!(cache.last_key(), None);
    }
}
