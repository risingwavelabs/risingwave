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
use risingwave_common::types::{Datum, DatumRef};
use smallvec::SmallVec;

use super::minput::StateCacheInputBatch;
use crate::common::cache::OrderedCache;

pub mod array_agg;
pub mod extreme;
pub mod string_agg;

/// Cache key type.
pub type CacheKey = Vec<u8>;

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
    /// Just insert into the inner cache structure, e.g. `BTreeMap`.
    fn insert_unchecked(&mut self, key: CacheKey, value: SmallVec<[DatumRef<'_>; 2]>);

    /// Mark the cache as synced.
    fn set_synced(&mut self);
}

/// A temporary handle for filling the state cache.
/// The state cache will be marked as synced automatically when this handle is dropped.
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

    /// Finish the cache filling process.
    /// Must be called after inserting all entries to mark the cache as synced.
    pub fn finish(self) {
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
    fn aggregate<'a>(&'a self, values: impl Iterator<Item = &'a Self::Value>) -> Datum;
}

/// A sorted [`StateCache`] implementation with capacity limit.
pub struct TopNStateCache<Agg>
where
    Agg: StateCacheAggregator + Send + Sync + 'static,
{
    /// Aggregator implementation.
    aggregator: Agg,

    /// The inner ordered cache.
    cache: OrderedCache<CacheKey, Agg::Value>,

    /// Sync status of the state cache.
    synced: bool,
}

impl<Agg> TopNStateCache<Agg>
where
    Agg: StateCacheAggregator + Send + Sync + 'static,
{
    pub fn new(aggregator: Agg, capacity: usize) -> Self {
        Self {
            aggregator,
            cache: OrderedCache::new(capacity),
            synced: false,
        }
    }
}

impl<Agg> StateCache for TopNStateCache<Agg>
where
    Agg: StateCacheAggregator + Send + Sync + 'static,
{
    fn is_synced(&self) -> bool {
        self.synced
    }

    fn apply_batch(&mut self, batch: StateCacheInputBatch<'_>) {
        if self.synced {
            if self.cache.is_empty() {
                // the empty cache may be marked as synced before, but we can't
                // insert into it safely, so make it unsynced and bye bye.
                self.synced = false;
            } else {
                // only insert/delete entries if the cache is synced
                for (op, key, value) in batch {
                    match op {
                        Op::Insert | Op::UpdateInsert => {
                            if &key < self.cache.last_key().expect("the cache must not be empty") {
                                self.cache
                                    .insert(key, self.aggregator.convert_cache_value(value));
                            }
                        }
                        Op::Delete | Op::UpdateDelete => {
                            self.cache.remove(key);
                            if self.cache.is_empty() {
                                // the cache becomes empty, but we don't know if there're still rows
                                // in the table, so mark it as not synced conservatively.
                                self.synced = false;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    fn begin_syncing(&mut self) -> StateCacheFiller<'_> {
        self.synced = false;
        self.cache.clear(); // ensure that the cache is empty before syncing
        StateCacheFiller {
            capacity: self.cache.capacity(),
            cache: self,
        }
    }

    fn get_output(&self) -> Datum {
        assert!(self.synced);
        self.aggregator.aggregate(self.cache.iter_values())
    }
}

impl<Agg> StateCacheMaintain for TopNStateCache<Agg>
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

/// A sorted [`StateCache`] implementation without capacity limit.
/// In other words, this cache is a fully synced cache.
pub struct SortedStateCache<Agg>
where
    Agg: StateCacheAggregator + Send + Sync + 'static,
{
    /// Aggregator implementation.
    aggregator: Agg,

    /// The inner ordered cache.
    cache: BTreeMap<CacheKey, Agg::Value>,

    /// Sync status of the state cache.
    /// If true, the cache is FULLY synced with corresponding state table.
    synced: bool,
}

impl<Agg> SortedStateCache<Agg>
where
    Agg: StateCacheAggregator + Send + Sync + 'static,
{
    pub fn new(aggregator: Agg) -> Self {
        Self {
            aggregator,
            cache: Default::default(),
            synced: false,
        }
    }
}

impl<Agg> StateCache for SortedStateCache<Agg>
where
    Agg: StateCacheAggregator + Send + Sync + 'static,
{
    fn is_synced(&self) -> bool {
        self.synced
    }

    fn apply_batch(&mut self, batch: StateCacheInputBatch<'_>) {
        if self.synced {
            // only insert/delete entries if the cache is synced
            for (op, key, value) in batch {
                match op {
                    Op::Insert | Op::UpdateInsert => {
                        self.cache
                            .insert(key, self.aggregator.convert_cache_value(value));
                    }
                    Op::Delete | Op::UpdateDelete => {
                        self.cache.remove(&key);
                    }
                }
            }
        }
    }

    fn begin_syncing(&mut self) -> StateCacheFiller<'_> {
        self.synced = false;
        self.cache.clear(); // ensure that the cache is empty before syncing
        StateCacheFiller {
            capacity: usize::MAX, // use `usize::MAX` for unlimited capacity
            cache: self,
        }
    }

    fn get_output(&self) -> Datum {
        assert!(self.synced);
        self.aggregator.aggregate(self.cache.values())
    }
}

impl<Agg> StateCacheMaintain for SortedStateCache<Agg>
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
