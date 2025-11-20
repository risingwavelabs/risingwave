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

use risingwave_common::array::Op;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_common_estimate_size::collections::EstimatedBTreeMap;

use super::{StateCache, StateCacheFiller, PercentileMode};

/// An implementation of [`StateCache`] that keeps all entries in an two-heap in-memory map.
#[derive(Clone, EstimateSize)]
pub struct PercentileStateCache<K: Ord + EstimateSize + Clone, V: EstimateSize + Clone> {
    table_row_count: usize,
    lower_cache: EstimatedBTreeMap<K, V>,
    upper_cache: EstimatedBTreeMap<K, V>,
    fraction: f64,
    mode: PercentileMode,
    synced: bool,
}

impl<K: Ord + EstimateSize + Clone, V: EstimateSize + Clone> PercentileStateCache<K, V> {
    pub fn new(fraction: f64, mode: PercentileMode) -> Self {
        Self {
            table_row_count: 0,
            lower_cache: Default::default(),
            upper_cache: Default::default(),
            fraction,
            mode,
            synced: false,
        }
    }

    fn balance(&mut self) {
        if !self.synced {
            return;
        }
        let lower_target = self.target_lower_len();

        while self.lower_cache.len() > lower_target {
            if let Some((key, value)) = Self::pop_last(&mut self.lower_cache) {
                self.upper_cache.insert(key, value);
            } else {
                break;
            }
        }

        while self.lower_cache.len() < lower_target {
            if let Some((key, value)) = Self::pop_first(&mut self.upper_cache) {
                self.lower_cache.insert(key, value);
            } else {
                self.invalidate();
                return;
            }
        }
    }

    fn insert_internal(&mut self, key: K, value: V) -> Option<V> {
        self.table_row_count = self.table_row_count.saturating_add(1);

        let replaced = match self.lower_cache.last_key_value() {
            None => self.lower_cache.insert(key, value),
            Some((last_key, _)) if key <= *last_key => self.lower_cache.insert(key, value),
            _ => self.upper_cache.insert(key, value),
        };

        self.balance();
        replaced
    }

    fn delete_internal(&mut self, key: &K) -> Option<V> {
        if self.table_row_count == 0 {
            return None;
        }
        self.table_row_count -= 1;

        let removed_lower = self.lower_cache.remove(key);
        let removed_upper = if removed_lower.is_none() {
            self.upper_cache.remove(key)
        } else {
            None
        };

        if removed_lower.is_none() && removed_upper.is_none() {
            self.invalidate();
            return None;
        }

        self.balance();
        removed_lower.or(removed_upper)
    }

    fn target_lower_len(&self) -> usize {
        if self.table_row_count == 0 {
            return 0;
        }
        match self.mode {
            PercentileMode::Disc => {
                let idx = if self.fraction == 0.0 {
                    0
                } else {
                    (self.fraction * self.table_row_count as f64).ceil() as usize - 1
                };
                (idx + 1).min(self.table_row_count)
            }
            PercentileMode::Cont => {
                if self.table_row_count == 1 {
                    1
                } else {
                    let rn = self.fraction * (self.table_row_count - 1) as f64;
                    (rn.floor() as usize + 1).min(self.table_row_count)
                }
            }
        }
    }

    fn invalidate(&mut self) {
        self.synced = false;
        self.lower_cache.clear();
        self.upper_cache.clear();
    }

    fn pop_first(map: &mut EstimatedBTreeMap<K, V>) -> Option<(K, V)> {
        let key = map.iter().next().map(|(k, _)| k.clone())?;
        map.remove(&key).map(|v| (key, v))
    }

    fn pop_last(map: &mut EstimatedBTreeMap<K, V>) -> Option<(K, V)> {
        let key = map.iter().next_back().map(|(k, _)| k.clone())?;
        map.remove(&key).map(|v| (key, v))
    }
}

impl<K: Ord + EstimateSize + Clone, V: EstimateSize + Clone> StateCache for PercentileStateCache<K, V>  {
    type Filler<'a>
        = &'a mut Self
    where
        Self: 'a;
    type Key = K;
    type Value = V;

    fn is_synced(&self) -> bool {
        self.synced
    }

    fn begin_syncing(&mut self) -> Self::Filler<'_> {
        self.synced = false;
        self.table_row_count = 0;
        self.lower_cache.clear();
        self.upper_cache.clear();
        self
    }

    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Option<Self::Value> {
        if self.synced {
            self.insert_internal(key, value)
        } else {
            None
        }
    }

    fn delete(&mut self, key: &Self::Key) -> Option<Self::Value> {
        if self.synced {
            self.delete_internal(key)
        } else {
            None
        }
    }

    fn apply_batch(&mut self, batch: impl IntoIterator<Item = (Op, Self::Key, Self::Value)>) {
        if self.synced {
            for (op, key, value) in batch {
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        self.insert_internal(key, value);
                    }
                    Op::Delete | Op::UpdateDelete => {
                        self.delete_internal(&key);
                        if !self.synced {
                            break;
                        }
                    }
                }
            }
        }
    }

    fn clear(&mut self) {
        self.table_row_count = 0;
        self.lower_cache.clear();
        self.upper_cache.clear();
        self.synced = false;
    }

    // values in PercentileStateCache only have single value
    fn values(&self) -> impl Iterator<Item = &Self::Value> {
        assert!(self.synced);
        match self.mode {
            PercentileMode::Disc => {
                let vec: Vec<&V> = self
                    .lower_cache
                    .last_key_value()
                    .map(|(_, v)| vec![v])
                    .unwrap_or_default();
                vec.into_iter()
            }
            // TODO: use V to complete interpolation operation in percentile_cont
            PercentileMode::Cont => {
                let vec: Vec<&V> = self.lower_cache.values().collect();
                vec.into_iter()
            }
        }
    }

    fn first_key_value(&self) -> Option<(&Self::Key, &Self::Value)> {
        assert!(self.synced);
        match self.mode {
            PercentileMode::Disc => {
                self.lower_cache.last_key_value()
            }
            // TODO: use V to complete interpolation operation in percentile_cont
            PercentileMode::Cont => {
                self.lower_cache.last_key_value()
            }
        }
    }
}

impl<K: Ord + EstimateSize + Clone, V: EstimateSize + Clone> StateCacheFiller
    for &mut PercentileStateCache<K, V>
{
    type Key = K;
    type Value = V;

    fn capacity(&self) -> Option<usize> {
        None
    }

    fn insert_unchecked(&mut self, key: Self::Key, value: Self::Value) {
        self.insert_internal(key, value);
    }

    fn finish(self) {
        self.synced = true;
    }
}
