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
use risingwave_common::estimate_size::{EstimateSize, KvSize};

use super::{StateCache, StateCacheFiller};

/// An implementation of [`StateCache`] that uses a [`BTreeMap`] as the underlying cache, with no
/// capacity limit.
pub struct OrderedStateCache<K: Ord + EstimateSize, V: EstimateSize> {
    cache: BTreeMap<K, V>,
    synced: bool,
    kv_heap_size: KvSize,
}

impl<K: Ord + EstimateSize, V: EstimateSize> EstimateSize for OrderedStateCache<K, V> {
    fn estimated_heap_size(&self) -> usize {
        // TODO: Add btreemap internal size
        // https://github.com/risingwavelabs/risingwave/issues/9713
        self.kv_heap_size.size()
    }
}

impl<K: Ord + EstimateSize, V: EstimateSize> OrderedStateCache<K, V> {
    pub fn new() -> Self {
        Self {
            cache: BTreeMap::new(),
            synced: false,
            kv_heap_size: KvSize::new(),
        }
    }

    fn insert_cache(&mut self, key: K, value: V) -> Option<V> {
        let key_size = self.kv_heap_size.add_val(&key);
        self.kv_heap_size.add_val(&value);
        let old_val = self.cache.insert(key, value);
        if let Some(old_val) = &old_val {
            self.kv_heap_size.sub_size(key_size);
            self.kv_heap_size.sub_val(old_val);
        }
        old_val
    }

    fn delete_cache(&mut self, key: &K) -> Option<V> {
        let old_val = self.cache.remove(key);
        if let Some(old_val) = &old_val {
            self.kv_heap_size.sub(key, old_val);
        }
        old_val
    }
}

impl<K: Ord + EstimateSize, V: EstimateSize> Default for OrderedStateCache<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord + EstimateSize, V: EstimateSize> StateCache for OrderedStateCache<K, V> {
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
            self.insert_cache(key, value)
        } else {
            None
        }
    }

    fn delete(&mut self, key: &Self::Key) -> Option<Self::Value> {
        if self.synced {
            self.delete_cache(key)
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

impl<K: Ord + EstimateSize, V: EstimateSize> StateCacheFiller for &mut OrderedStateCache<K, V> {
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
