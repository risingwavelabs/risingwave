// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;

use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::types::{Datum, DatumRef};
use smallvec::SmallVec;

use super::minput::StateCacheInputBatch;

pub mod array_agg;
pub mod extreme;
pub mod string_agg;

/// Cache key type.
pub type CacheKey = Vec<u8>;

/// Common cache structure for managed table states (non-append-only `min`/`max`, `string_agg`).
pub struct OrderedCache<V> {
    /// The capacity of the cache.
    capacity: usize,
    /// Ordered cache entries.
    entries: BTreeMap<CacheKey, V>,
}

impl<V> OrderedCache<V> {
    /// Create a new cache with specified capacity and order requirements.
    /// To create a cache with unlimited capacity, use `usize::MAX` for `capacity`.
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: Default::default(),
        }
    }

    /// Get the capacity of the cache.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get the number of entries in the cache.
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
    }

    /// Insert an entry into the cache.
    /// Key: [`CacheKey`] composed of serialized order-by fields.
    /// Value: The value fields that are to be aggregated.
    pub fn insert(&mut self, key: CacheKey, value: V) {
        self.entries.insert(key, value);
        // evict if capacity is reached
        while self.entries.len() > self.capacity {
            self.entries.pop_last();
        }
    }

    /// Remove an entry from the cache.
    pub fn remove(&mut self, key: CacheKey) {
        self.entries.remove(&key);
    }

    /// Get the last (largest) key in the cache
    pub fn last_key(&self) -> Option<&CacheKey> {
        self.entries.last_key_value().map(|(k, _)| k)
    }

    /// Get the first (smallest) value in the cache.
    pub fn first_value(&self) -> Option<&V> {
        self.entries.first_key_value().map(|(_, v)| v)
    }

    /// Iterate over the values in the cache.
    pub fn iter_values(&self) -> impl Iterator<Item = &V> {
        self.entries.values()
    }
}

/// Trait that defines the interface of state table cache.
pub trait StateCache: Send + Sync + 'static {
    /// Check if the cache is synced with state table.
    fn is_synced(&self) -> bool;

    /// Apply a batch of updates to the cache.
    fn apply_batch(&mut self, batch: StateCacheInputBatch<'_>);

    /// Begin syncing the cache with state table.
    fn begin_syncing(&mut self) -> StateCacheFiller<'_>;

    /// Get the aggregation output.
    fn get_output(&self) -> Datum;
}

/// Cache maintenance interface.
/// Note that this trait must be private, so that only [`StateCacheFiller`] can use it.
trait StateCacheMaintain: Send + Sync + 'static {
    /// Insert an entry to the cache without checking row count, capacity, key order, etc.
    /// Just insert into the inner BTreeMap.
    fn insert_unchecked(&mut self, key: CacheKey, value: SmallVec<[DatumRef<'_>; 2]>);

    /// Mark the cache as synced.
    fn set_synced(&mut self);
}

/// A temporary handle for filling the state cache.
pub struct StateCacheFiller<'a> {
    capacity: usize,
    cache: &'a mut dyn StateCacheMaintain,
}

impl<'a> StateCacheFiller<'a> {
    /// Get the capacity of the cache to be filled.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Insert an entry to the cache.
    pub fn insert(&mut self, key: CacheKey, value: SmallVec<[DatumRef<'_>; 2]>) {
        self.cache.insert_unchecked(key, value)
    }
}

impl<'a> Drop for StateCacheFiller<'a> {
    fn drop(&mut self) {
        self.cache.set_synced();
    }
}

/// Trait that defines aggregators that aggregate entries in an [`OrderedCache`].
pub trait StateCacheAggregator {
    /// The cache value type.
    type Value: Send + Sync;

    /// Convert cache value into compact representation.
    fn convert_cache_value(&self, value: SmallVec<[DatumRef<'_>; 2]>) -> Self::Value;

    /// Aggregate all entries in the ordered cache.
    fn aggregate(&self, cache: &OrderedCache<Self::Value>) -> Datum;
}

/// A [`StateCache`] implementation that uses [`OrderedCache`] as the cache.
pub struct GenericStateCache<Agg>
where
    Agg: StateCacheAggregator + Send + Sync + 'static,
{
    /// Aggregator implementation.
    aggregator: Agg,

    /// The inner ordered cache.
    cache: OrderedCache<Agg::Value>,

    /// Number of all items in the state store.
    total_count: usize,

    /// Sync status of the state cache.
    synced: bool,
}

impl<Agg> GenericStateCache<Agg>
where
    Agg: StateCacheAggregator + Send + Sync + 'static,
{
    pub fn new(aggregator: Agg, capacity: usize, total_count: usize) -> Self {
        Self {
            aggregator,
            cache: OrderedCache::new(capacity),
            total_count,
            synced: total_count == 0,
        }
    }
}

impl<Agg> StateCache for GenericStateCache<Agg>
where
    Agg: StateCacheAggregator + Send + Sync + 'static,
{
    fn is_synced(&self) -> bool {
        self.synced
    }

    fn apply_batch(&mut self, mut batch: StateCacheInputBatch<'_>) {
        if self.synced {
            // only insert/delete entries if the cache is synced
            while let Some((op, key, value)) = batch.next() {
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        self.total_count += 1;
                        if self.cache.len() == self.total_count - 1
                            || &key < self.cache.last_key().unwrap()
                        {
                            self.cache
                                .insert(key, self.aggregator.convert_cache_value(value));
                        }
                    }
                    Op::Delete | Op::UpdateDelete => {
                        self.total_count -= 1;
                        self.cache.remove(key);
                        if self.total_count > 0 /* still has rows after deletion */ && self.cache.is_empty()
                        {
                            // the cache is empty, but the state table is not, so it's not synced
                            // any more
                            self.synced = false;
                            break;
                        }
                    }
                }
            }
        }

        // count remaining ops
        let op_counts = batch.counts_by(|(op, _, _)| op);
        self.total_count += op_counts.get(&Op::Insert).unwrap_or(&0)
            + op_counts.get(&Op::UpdateInsert).unwrap_or(&0);
        self.total_count -= op_counts.get(&Op::Delete).unwrap_or(&0)
            + op_counts.get(&Op::UpdateDelete).unwrap_or(&0);
    }

    fn begin_syncing(&mut self) -> StateCacheFiller<'_> {
        self.cache.clear(); // ensure the cache is clear before syncing
        StateCacheFiller {
            capacity: self.cache.capacity(),
            cache: self,
        }
    }

    fn get_output(&self) -> Datum {
        self.aggregator.aggregate(&self.cache)
    }
}

impl<Agg> StateCacheMaintain for GenericStateCache<Agg>
where
    Agg: StateCacheAggregator + Send + Sync + 'static,
{
    fn insert_unchecked(&mut self, key: CacheKey, value: SmallVec<[DatumRef<'_>; 2]>) {
        let value = self.aggregator.convert_cache_value(value);
        self.cache.insert(key, value);
    }

    fn set_synced(&mut self) {
        self.synced = true;
    }
}
