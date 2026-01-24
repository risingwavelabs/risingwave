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

//! Object-safe version of [`StateCache`] for aggregation.

use risingwave_common::array::StreamChunk;
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, Datum, ToOwnedDatum};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::memcmp_encoding::MemcmpEncoded;
use risingwave_common::util::row_serde::OrderedRowSerde;
use risingwave_common_estimate_size::EstimateSize;
use smallvec::SmallVec;

use crate::common::state_cache::{StateCache, StateCacheFiller};

/// Cache key type.
type CacheKey = MemcmpEncoded;

#[derive(Debug)]
pub struct CacheValue(SmallVec<[Datum; 2]>);

/// Trait that defines the interface of state table cache for stateful streaming agg.
pub trait AggStateCache: EstimateSize {
    /// Check if the cache is synced with state table.
    fn is_synced(&self) -> bool;

    /// Apply a batch of updates to the cache.
    fn apply_batch(
        &mut self,
        chunk: &StreamChunk,
        cache_key_serializer: &OrderedRowSerde,
        arg_col_indices: &[usize],
        order_col_indices: &[usize],
    );

    /// Begin syncing the cache with state table.
    fn begin_syncing(&mut self) -> Box<dyn AggStateCacheFiller + Send + Sync + '_>;

    /// Output batches from the cache.
    fn output_batches(&self, chunk_size: usize) -> Box<dyn Iterator<Item = StreamChunk> + '_>;

    /// Output the first value.
    fn output_first(&self) -> Datum;
}

/// Trait that defines agg state cache syncing interface.
pub trait AggStateCacheFiller {
    /// Get the capacity of the cache to be filled. `None` means unlimited.
    fn capacity(&self) -> Option<usize>;

    /// Insert an entry to the cache without checking row count, capacity, key order, etc.
    /// Just insert into the inner cache structure, e.g. `BTreeMap`.
    fn append(&mut self, key: CacheKey, value: CacheValue);

    /// Mark the cache as synced.
    fn finish(self: Box<Self>);
}

/// A wrapper over generic [`StateCache`] that implements [`AggStateCache`].
#[derive(EstimateSize)]
pub struct GenericAggStateCache<C>
where
    C: StateCache<Key = CacheKey, Value = CacheValue>,
{
    state_cache: C,
    input_types: Vec<DataType>,
}

impl<C> GenericAggStateCache<C>
where
    C: StateCache<Key = CacheKey, Value = CacheValue>,
{
    pub fn new(state_cache: C, input_types: &[DataType]) -> Self {
        Self {
            state_cache,
            input_types: input_types.to_vec(),
        }
    }
}

impl<C> AggStateCache for GenericAggStateCache<C>
where
    C: StateCache<Key = CacheKey, Value = CacheValue>,
    for<'a> C::Filler<'a>: Send + Sync,
{
    fn is_synced(&self) -> bool {
        self.state_cache.is_synced()
    }

    fn apply_batch(
        &mut self,
        chunk: &StreamChunk,
        cache_key_serializer: &OrderedRowSerde,
        arg_col_indices: &[usize],
        order_col_indices: &[usize],
    ) {
        let rows = chunk.rows().map(|(op, row)| {
            let key = {
                let mut key = Vec::new();
                cache_key_serializer.serialize_datums(
                    order_col_indices
                        .iter()
                        .map(|col_idx| row.datum_at(*col_idx)),
                    &mut key,
                );
                key.into()
            };
            let value = CacheValue(
                arg_col_indices
                    .iter()
                    .map(|col_idx| row.datum_at(*col_idx).to_owned_datum())
                    .collect(),
            );
            (op, key, value)
        });
        self.state_cache.apply_batch(rows);
    }

    fn begin_syncing(&mut self) -> Box<dyn AggStateCacheFiller + Send + Sync + '_> {
        Box::new(GenericAggStateCacheFiller::<'_, C> {
            cache_filler: self.state_cache.begin_syncing(),
        })
    }

    fn output_batches(&self, chunk_size: usize) -> Box<dyn Iterator<Item = StreamChunk> + '_> {
        let mut values = self.state_cache.values();
        Box::new(std::iter::from_fn(move || {
            // build data chunk from rows
            let mut builder = DataChunkBuilder::new(self.input_types.clone(), chunk_size);
            for row in &mut values {
                if let Some(chunk) = builder.append_one_row(row.0.as_slice()) {
                    return Some(chunk.into());
                }
            }
            builder.consume_all().map(|chunk| chunk.into())
        }))
    }

    fn output_first(&self) -> Datum {
        let value = self.state_cache.values().next()?;
        value.0[0].clone()
    }
}

pub struct GenericAggStateCacheFiller<'filler, C>
where
    C: StateCache<Key = CacheKey, Value = CacheValue> + 'filler,
{
    cache_filler: C::Filler<'filler>,
}

impl<C> AggStateCacheFiller for GenericAggStateCacheFiller<'_, C>
where
    C: StateCache<Key = CacheKey, Value = CacheValue>,
{
    fn capacity(&self) -> Option<usize> {
        self.cache_filler.capacity()
    }

    fn append(&mut self, key: CacheKey, value: CacheValue) {
        self.cache_filler.insert_unchecked(key, value);
    }

    fn finish(self: Box<Self>) {
        self.cache_filler.finish()
    }
}

impl FromIterator<Datum> for CacheValue {
    fn from_iter<T: IntoIterator<Item = Datum>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl EstimateSize for CacheValue {
    fn estimated_heap_size(&self) -> usize {
        let data_heap_size: usize = self.0.iter().map(|datum| datum.estimated_heap_size()).sum();
        self.0.len() * std::mem::size_of::<Datum>() + data_heap_size
    }
}
