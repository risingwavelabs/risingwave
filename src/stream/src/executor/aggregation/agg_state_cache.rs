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

use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::{ArrayImpl, Op};
use risingwave_common::buffer::Bitmap;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::types::{Datum, DatumRef};
use risingwave_common::util::ordered::OrderedRowSerde;
use risingwave_common_proc_macro::EstimateSize;
use smallvec::SmallVec;

use super::minput_agg_impl::MInputAggregator;
use crate::common::cache::{StateCache, StateCacheFiller};

/// Cache key type.
type CacheKey = Vec<u8>;

// TODO(yuchao): May extract common logic here to `struct [Data/Stream]ChunkRef` if there's other
// usage in the future. https://github.com/risingwavelabs/risingwave/pull/5908#discussion_r1002896176
pub struct StateCacheInputBatch<'a> {
    idx: usize,
    ops: Ops<'a>,
    visibility: Option<&'a Bitmap>,
    columns: &'a [&'a ArrayImpl],
    cache_key_serializer: &'a OrderedRowSerde,
    arg_col_indices: &'a [usize],
    order_col_indices: &'a [usize],
}

impl<'a> StateCacheInputBatch<'a> {
    pub fn new(
        ops: Ops<'a>,
        visibility: Option<&'a Bitmap>,
        columns: &'a [&'a ArrayImpl],
        cache_key_serializer: &'a OrderedRowSerde,
        arg_col_indices: &'a [usize],
        order_col_indices: &'a [usize],
    ) -> Self {
        let first_idx = visibility.map_or(0, |v| v.next_set_bit(0).unwrap_or(ops.len()));
        Self {
            idx: first_idx,
            ops,
            visibility,
            columns,
            cache_key_serializer,
            arg_col_indices,
            order_col_indices,
        }
    }
}

impl<'a> Iterator for StateCacheInputBatch<'a> {
    type Item = (Op, CacheKey, SmallVec<[DatumRef<'a>; 2]>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.ops.len() {
            None
        } else {
            let op = self.ops[self.idx];
            let key = {
                let mut key = Vec::new();
                self.cache_key_serializer.serialize_datums(
                    self.order_col_indices
                        .iter()
                        .map(|col_idx| self.columns[*col_idx].value_at(self.idx)),
                    &mut key,
                );
                key
            };
            let value = self
                .arg_col_indices
                .iter()
                .map(|col_idx| self.columns[*col_idx].value_at(self.idx))
                .collect();
            self.idx = self.visibility.map_or(self.idx + 1, |v| {
                v.next_set_bit(self.idx + 1).unwrap_or(self.ops.len())
            });
            Some((op, key, value))
        }
    }
}

/// Trait that defines the interface of state table cache for stateful streaming agg.
pub trait AggStateCache: EstimateSize {
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

/// A generic implementation of [`AggStateCache`] that combines a [`StateCache`] and an
/// [`MInputAggregator`].
#[derive(EstimateSize)]
pub struct GenericAggStateCache<C, A>
where
    C: StateCache<Key = CacheKey, Value = A::Value>,
    A: MInputAggregator,
{
    state_cache: C,
    #[estimate_size(ignore)]
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
            .insert_unchecked(key, self.aggregator.convert_cache_value(value));
    }

    fn finish(self: Box<Self>) {
        self.cache_filler.finish()
    }
}
