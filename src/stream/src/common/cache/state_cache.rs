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

use risingwave_common::array::Op;

use super::TopNCache;

/// A common interface for state table cache.
pub trait StateCache {
    type Key: Ord;
    type Value;

    /// Type of state cache filler, for syncing the cache with the state table.
    type Filler<'a>: StateCacheFiller<Key = Self::Key, Value = Self::Value> + 'a
    where
        Self: 'a;

    /// Check if the cache is synced with the state table.
    fn is_synced(&self) -> bool;

    /// Begin syncing the cache with the state table.
    fn begin_syncing(&mut self) -> Self::Filler<'_>;

    /// Insert an entry into the cache. Should not break cache validity.
    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Option<Self::Value>;

    /// Delete an entry from the cache. Should not break cache validity.
    fn delete(&mut self, key: &Self::Key) -> Option<Self::Value>;

    /// Apply a batch of operations to the cache. Should not break cache validity.
    fn apply_batch(&mut self, batch: impl IntoIterator<Item = (Op, Self::Key, Self::Value)>);

    /// Clear the cache.
    fn clear(&mut self);

    /// Iterate over the values in the cache.
    fn values(&self) -> impl Iterator<Item = &Self::Value>;

    /// Get the reference of first key-value pair in the cache.
    fn first_key_value(&self) -> Option<(&Self::Key, &Self::Value)>;
}

pub trait StateCacheFiller {
    type Key: Ord;
    type Value;

    /// Get the capacity of the cache.
    fn capacity(&self) -> Option<usize>;

    /// Insert an entry into the cache without cache validity check.
    fn insert_unchecked(&mut self, key: Self::Key, value: Self::Value);

    /// Finish syncing the cache with the state table. This should mark the cache as synced.
    fn finish(self);
}

/// An implementation of [`StateCache`] that uses a [`TopNCache`] as the underlying cache, with
/// limited capacity.
pub struct TopNStateCache<K: Ord, V> {
    table_row_count: Option<usize>,
    cache: TopNCache<K, V>,
    synced: bool,
}

impl<K: Ord, V> TopNStateCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            table_row_count: None,
            cache: TopNCache::new(capacity),
            synced: false,
        }
    }

    pub fn with_table_row_count(capacity: usize, table_row_count: usize) -> Self {
        Self {
            table_row_count: Some(table_row_count),
            cache: TopNCache::new(capacity),
            synced: false,
        }
    }

    pub fn set_table_row_count(&mut self, table_row_count: usize) {
        self.table_row_count = Some(table_row_count);
    }

    fn row_count_matched(&self) -> bool {
        self.table_row_count
            .map(|n| n == self.cache.len())
            .unwrap_or(false)
    }

    /// Insert an entry with the assumption that the cache is SYNCED.
    fn insert_synced(&mut self, key: K, value: V) -> Option<V> {
        let old_v = if self.row_count_matched()
            || self.cache.is_empty()
            || &key <= self.cache.last_key().unwrap()
        {
            self.cache.insert(key, value)
        } else {
            None
        };
        // In other cases, we can't insert this key because we're not sure whether there're keys
        // less than it in the table. So we only update table row count.
        self.table_row_count = self.table_row_count.map(|n| n + 1);
        old_v
    }

    /// Delete an entry with the assumption that the cache is SYNCED.
    fn delete_synced(&mut self, key: &K) -> Option<V> {
        let old_val = self.cache.remove(key);
        self.table_row_count = self.table_row_count.map(|n| n - 1);
        if self.cache.is_empty() && !self.row_count_matched() {
            // The cache becomes empty, but there're still rows in the table, so mark it as not
            // synced.
            self.synced = false;
        }
        old_val
    }
}

impl<K: Ord, V> StateCache for TopNStateCache<K, V> {
    type Filler<'a> = &'a mut Self where Self: 'a;
    type Key = K;
    type Value = V;

    fn is_synced(&self) -> bool {
        self.synced
    }

    fn begin_syncing(&mut self) -> Self::Filler<'_> {
        self.synced = false;
        self.cache.clear();
        self
    }

    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Option<Self::Value> {
        if self.synced {
            self.insert_synced(key, value)
        } else {
            None
        }
    }

    fn delete(&mut self, key: &Self::Key) -> Option<Self::Value> {
        if self.synced {
            self.delete_synced(key)
        } else {
            None
        }
    }

    fn apply_batch(&mut self, batch: impl IntoIterator<Item = (Op, Self::Key, Self::Value)>) {
        if self.synced {
            for (op, key, value) in batch {
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        self.insert_synced(key, value);
                    }
                    Op::Delete | Op::UpdateDelete => {
                        self.delete_synced(&key);
                        if !self.synced {
                            break;
                        }
                    }
                }
            }
        }
    }

    fn clear(&mut self) {
        self.cache.clear();
        self.synced = false;
    }

    fn values(&self) -> impl Iterator<Item = &Self::Value> {
        assert!(self.synced);
        self.cache.values()
    }

    fn first_key_value(&self) -> Option<(&Self::Key, &Self::Value)> {
        assert!(self.synced);
        self.cache.first_key_value()
    }
}

impl<K: Ord, V> StateCacheFiller for &mut TopNStateCache<K, V> {
    type Key = K;
    type Value = V;

    fn capacity(&self) -> Option<usize> {
        Some(self.cache.capacity())
    }

    fn insert_unchecked(&mut self, key: Self::Key, value: Self::Value) {
        self.cache.insert(key, value);
    }

    fn finish(self) {
        self.synced = true;
    }
}

/// An implementation of [`StateCache`] that uses a [`BTreeMap`] as the underlying cache, with no
/// capacity limit.
pub struct OrderedStateCache<K: Ord, V> {
    cache: BTreeMap<K, V>,
    synced: bool,
}

impl<K: Ord, V> OrderedStateCache<K, V> {
    pub fn new() -> Self {
        Self {
            cache: BTreeMap::new(),
            synced: false,
        }
    }
}

impl<K: Ord, V> Default for OrderedStateCache<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord, V> StateCache for OrderedStateCache<K, V> {
    type Filler<'a> = &'a mut Self where Self: 'a;
    type Key = K;
    type Value = V;

    fn is_synced(&self) -> bool {
        self.synced
    }

    fn begin_syncing(&mut self) -> Self::Filler<'_> {
        self.synced = false;
        self.cache.clear();
        self
    }

    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Option<Self::Value> {
        if self.synced {
            self.cache.insert(key, value)
        } else {
            None
        }
    }

    fn delete(&mut self, key: &Self::Key) -> Option<Self::Value> {
        if self.synced {
            self.cache.remove(key)
        } else {
            None
        }
    }

    fn apply_batch(&mut self, batch: impl IntoIterator<Item = (Op, Self::Key, Self::Value)>) {
        if self.synced {
            for (op, key, value) in batch {
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        self.cache.insert(key, value);
                    }
                    Op::Delete | Op::UpdateDelete => {
                        self.cache.remove(&key);
                    }
                }
            }
        }
    }

    fn clear(&mut self) {
        self.cache.clear();
        self.synced = false;
    }

    fn values(&self) -> impl Iterator<Item = &Self::Value> {
        assert!(self.synced);
        self.cache.values()
    }

    fn first_key_value(&self) -> Option<(&Self::Key, &Self::Value)> {
        assert!(self.synced);
        self.cache.first_key_value()
    }
}

impl<K: Ord, V> StateCacheFiller for &mut OrderedStateCache<K, V> {
    type Key = K;
    type Value = V;

    fn capacity(&self) -> Option<usize> {
        None
    }

    fn insert_unchecked(&mut self, key: Self::Key, value: Self::Value) {
        self.cache.insert(key, value);
    }

    fn finish(self) {
        self.synced = true;
    }
}
