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

use risingwave_common::types::{Datum, DatumRef};
use smallvec::SmallVec;

use super::minput::StateCacheInputBatch;
use crate::common::cache::{StateCache, StateCacheFiller};

pub mod array_agg;
pub mod extreme;
pub mod string_agg;

/// Cache key type.
pub type CacheKey = Vec<u8>;

/// Trait that defines the interface of state table cache for stateful streaming agg.
pub trait AggStateCache {
    /// Check if the cache is synced with state table.
    fn is_synced(&self) -> bool;

    /// Apply a batch of updates to the cache.
    fn apply_batch(&mut self, batch: StateCacheInputBatch<'_>);

    /// Begin syncing the cache with state table.
    fn begin_syncing(&mut self) -> Box<dyn AggStateCacheFiller + Send + Sync + '_>;

    /// Get the aggregation output.
    fn get_output(&self) -> Datum;
}

/// Trait that defines agg state cache syncing interface.
pub trait AggStateCacheFiller {
    /// Get the capacity of the cache to be filled. `None` means unlimited.
    fn capacity(&self) -> Option<usize>;

    /// Insert an entry to the cache without checking row count, capacity, key order, etc.
    /// Just insert into the inner cache structure, e.g. `BTreeMap`.
    fn append(&mut self, key: CacheKey, value: SmallVec<[DatumRef<'_>; 2]>);

    /// Mark the cache as synced.
    fn finish(self: Box<Self>);
}

/// Trait that defines aggregators that aggregate over an iterator of cached values.
pub trait MInputAggregator {
    /// The cache value type.
    type Value;

    /// Convert cache value into compact representation.
    fn convert_cache_value(&self, value: SmallVec<[DatumRef<'_>; 2]>) -> Self::Value;

    /// Aggregate cached values.
    fn aggregate<'a>(&'a self, values: impl Iterator<Item = &'a Self::Value>) -> Datum;
}

/// A generic implementation of [`AggStateCache`] that combines a [`StateCache`] and an
/// [`MInputAggregator`].
pub struct GenericAggStateCache<C, A>
where
    C: StateCache<Key = CacheKey, Value = A::Value>,
    A: MInputAggregator,
{
    state_cache: C,
    aggregator: A,
}

impl<C, A> GenericAggStateCache<C, A>
where
    C: StateCache<Key = CacheKey, Value = A::Value>,
    A: MInputAggregator,
{
    pub fn new(state_cache: C, aggregator: A) -> Self {
        Self {
            state_cache,
            aggregator,
        }
    }
}

impl<C, A> AggStateCache for GenericAggStateCache<C, A>
where
    C: StateCache<Key = CacheKey, Value = A::Value>,
    for<'a> C::Filler<'a>: Send + Sync,
    A: MInputAggregator + Send + Sync,
{
    fn is_synced(&self) -> bool {
        self.state_cache.is_synced()
    }

    fn apply_batch(&mut self, batch: StateCacheInputBatch<'_>) {
        self.state_cache.apply_batch(
            batch.map(|(op, key, value)| (op, key, self.aggregator.convert_cache_value(value))),
        );
    }

    fn begin_syncing(&mut self) -> Box<dyn AggStateCacheFiller + Send + Sync + '_> {
        Box::new(GenericAggStateCacheFiller::<'_, C, A> {
            cache_filler: self.state_cache.begin_syncing(),
            aggregator: &self.aggregator,
        })
    }

    fn get_output(&self) -> Datum {
        self.aggregator.aggregate(self.state_cache.values())
    }
}

pub struct GenericAggStateCacheFiller<'filler, C, A>
where
    C: StateCache<Key = CacheKey, Value = A::Value> + 'filler,
    A: MInputAggregator,
{
    cache_filler: C::Filler<'filler>,
    aggregator: &'filler A,
}

impl<'filler, C, A> AggStateCacheFiller for GenericAggStateCacheFiller<'filler, C, A>
where
    C: StateCache<Key = CacheKey, Value = A::Value>,
    A: MInputAggregator,
{
    fn capacity(&self) -> Option<usize> {
        self.cache_filler.capacity()
    }

    fn append(&mut self, key: CacheKey, value: SmallVec<[DatumRef<'_>; 2]>) {
        self.cache_filler
            .append(key, self.aggregator.convert_cache_value(value));
    }

    fn finish(self: Box<Self>) {
        self.cache_filler.finish()
    }
}
