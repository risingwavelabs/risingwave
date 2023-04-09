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

use super::{StateCache, StateCacheFiller};

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
