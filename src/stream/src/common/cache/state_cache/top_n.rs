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

use risingwave_common::array::Op;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common_proc_macro::EstimateSize;

use super::{StateCache, StateCacheFiller};
use crate::common::cache::TopNCache;

/// An implementation of [`StateCache`] that uses a [`TopNCache`] as the underlying cache, with
/// limited capacity.
#[derive(EstimateSize)]
pub struct TopNStateCache<K: Ord + EstimateSize, V: EstimateSize> {
    table_row_count: Option<usize>,
    cache: TopNCache<K, V>,
    synced: bool,
}

impl<K: Ord + EstimateSize, V: EstimateSize> TopNStateCache<K, V> {
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

impl<K: Ord + EstimateSize, V: EstimateSize> StateCache for TopNStateCache<K, V> {
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

impl<K: Ord + EstimateSize, V: EstimateSize> StateCacheFiller for &mut TopNStateCache<K, V> {
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
