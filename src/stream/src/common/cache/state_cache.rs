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
    fn insert(&mut self, key: Self::Key, value: Self::Value);

    /// Delete an entry from the cache. Should not break cache validity.
    fn delete(&mut self, key: Self::Key);

    /// Apply a batch of operations to the cache. Should not break cache validity.
    fn apply_batch(&mut self, batch: impl IntoIterator<Item = (Op, Self::Key, Self::Value)>);

    /// Iterate over the values in the cache.
    fn values(&self) -> impl Iterator<Item = &Self::Value>;
}

pub trait StateCacheFiller {
    type Key: Ord;
    type Value;

    /// Get the capacity of the cache.
    fn capacity(&self) -> Option<usize>;

    /// Append an entry to the cache. The key is expected to be larger than all existing keys.
    fn append(&mut self, key: Self::Key, value: Self::Value);

    /// Finish syncing the cache with the state table. This should mark the cache as synced.
    fn finish(self);
}

/// An implementation of [`StateCache`] that uses a [`TopNCache`] as the underlying cache, with
/// limited capacity.
pub struct TopNStateCache<K: Ord, V> {
    cache: TopNCache<K, V>,
    synced: bool,
}

impl<K: Ord, V> TopNStateCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: TopNCache::new(capacity),
            synced: false,
        }
    }

    fn insert_unchecked(&mut self, key: K, value: V) {
        self.cache.insert(key, value);
    }

    fn delete_unchecked(&mut self, key: K) {
        self.cache.remove(key);
        if self.cache.is_empty() {
            // The cache becomes empty, but we don't know if there're still rows in the table,
            // so mark it as not synced conservatively.
            self.synced = false;
        }
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

    fn insert(&mut self, key: Self::Key, value: Self::Value) {
        if self.synced {
            if let Some(last_key) = self.cache.last_key() && &key > last_key {
                // We can't insert this key because we're not sure whether there're
                // keys less than it in the table.
                return
            }
            self.insert_unchecked(key, value);
        }
    }

    fn delete(&mut self, key: Self::Key) {
        if self.synced {
            self.delete_unchecked(key);
        }
    }

    fn apply_batch(&mut self, batch: impl IntoIterator<Item = (Op, Self::Key, Self::Value)>) {
        if self.synced {
            for (op, key, value) in batch {
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        if let Some(last_key) = self.cache.last_key() && &key > last_key {
                            // We can't insert this key because we're not sure whether there're
                            // keys less than it in the table.
                            continue;
                        }
                        self.insert_unchecked(key, value);
                    }
                    Op::Delete | Op::UpdateDelete => {
                        self.delete_unchecked(key);
                        if !self.synced {
                            break;
                        }
                    }
                }
            }
        }
    }

    fn values(&self) -> impl Iterator<Item = &Self::Value> {
        assert!(self.synced);
        self.cache.iter_values()
    }
}

impl<K: Ord, V> StateCacheFiller for &mut TopNStateCache<K, V> {
    type Key = K;
    type Value = V;

    fn capacity(&self) -> Option<usize> {
        Some(self.cache.capacity())
    }

    fn append(&mut self, key: Self::Key, value: Self::Value) {
        debug_assert!(&key >= self.cache.last_key().unwrap_or(&key));
        self.insert_unchecked(key, value);
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

    fn insert(&mut self, key: Self::Key, value: Self::Value) {
        if self.synced {
            self.cache.insert(key, value);
        }
    }

    fn delete(&mut self, key: Self::Key) {
        if self.synced {
            self.cache.remove(&key);
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

    fn values(&self) -> impl Iterator<Item = &Self::Value> {
        assert!(self.synced);
        self.cache.values()
    }
}

impl<K: Ord, V> StateCacheFiller for &mut OrderedStateCache<K, V> {
    type Key = K;
    type Value = V;

    fn capacity(&self) -> Option<usize> {
        None
    }

    fn append(&mut self, key: Self::Key, value: Self::Value) {
        debug_assert!(&key >= self.cache.last_key_value().map(|(k, _)| k).unwrap_or(&key));
        self.cache.insert(key, value);
    }

    fn finish(self) {
        self.synced = true;
    }
}
