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

use risingwave_common::array::Row;
use risingwave_common::types::Datum;

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
    /// Insert a state table record to the cache.
    fn insert(&mut self, key: CacheKey, state_row: &Row);

    /// Delete a state table record from the cache.
    fn delete(&mut self, key: CacheKey);

    /// Clear the cache.
    fn clear(&mut self);

    /// Get the capacity of the cache.
    fn capacity(&self) -> usize;

    /// Get the number of entries in the cache.
    fn len(&self) -> usize;

    /// Check if the cache is empty.
    fn is_empty(&self) -> bool;

    /// Get the last (largest) key in the cache.
    fn last_key(&self) -> Option<&CacheKey>;

    /// Get the aggregation output.
    fn get_output(&self) -> Datum;
}

/// Trait that defines aggregators that aggregate entries in an [`OrderedCache`].
pub trait StateCacheAggregator {
    /// The cache value type.
    type Value: Send + Sync;

    /// Extract cache value from state table row.
    fn state_row_to_cache_value(&self, state_row: &Row) -> Self::Value;

    /// Aggregate all entries in the ordered cache.
    fn aggregate(&self, cache: &OrderedCache<Self::Value>) -> Datum;
}

/// A [`StateCache`] implementation that uses [`OrderedCache`] as the cache.
pub struct GenericStateCache<Agg>
where
    Agg: StateCacheAggregator + Send + Sync + 'static,
{
    cache: OrderedCache<Agg::Value>,
    aggregator: Agg,
}

impl<Agg> GenericStateCache<Agg>
where
    Agg: StateCacheAggregator + Send + Sync + 'static,
{
    pub fn new(capacity: usize, aggregator: Agg) -> Self {
        Self {
            cache: OrderedCache::new(capacity),
            aggregator,
        }
    }
}

impl<Agg> StateCache for GenericStateCache<Agg>
where
    Agg: StateCacheAggregator + Send + Sync + 'static,
{
    fn insert(&mut self, key: CacheKey, state_row: &Row) {
        let value = self.aggregator.state_row_to_cache_value(state_row);
        self.cache.insert(key, value);
    }

    fn delete(&mut self, key: CacheKey) {
        self.cache.remove(key);
    }

    fn clear(&mut self) {
        self.cache.clear();
    }

    fn capacity(&self) -> usize {
        self.cache.capacity()
    }

    fn len(&self) -> usize {
        self.cache.len()
    }

    fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    fn last_key(&self) -> Option<&CacheKey> {
        self.cache.last_key()
    }

    fn get_output(&self) -> Datum {
        self.aggregator.aggregate(&self.cache)
    }
}
