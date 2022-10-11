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

use async_trait::async_trait;
use risingwave_common::array::stream_chunk::Ops;
use risingwave_common::array::ArrayImpl;
use risingwave_common::buffer::Bitmap;
use risingwave_common::types::Datum;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use crate::executor::StreamExecutorResult;

mod array_agg;
mod extreme;
mod string_agg;

pub use array_agg::ManagedArrayAggState;
pub use extreme::GenericExtremeState;
pub use string_agg::ManagedStringAggState;

/// A trait over all table-structured states.
///
/// It is true that this interface also fits to value managed state, but we won't implement
/// `ManagedTableState` for them. We want to reduce the overhead of `BoxedFuture`. For
/// `ManagedValueState`, we can directly forward its async functions to `ManagedStateImpl`, instead
/// of adding a layer of indirection caused by async traits.
#[async_trait]
pub trait ManagedTableState<S: StateStore>: Send + Sync + 'static {
    async fn apply_chunk(
        &mut self,
        ops: Ops<'_>,
        visibility: Option<&Bitmap>,
        columns: &[&ArrayImpl],
        state_table: &mut StateTable<S>,
    ) -> StreamExecutorResult<()>;

    /// Get the output of the state. Must flush before getting output.
    async fn get_output(&mut self, state_table: &StateTable<S>) -> StreamExecutorResult<Datum>;
}

/// Common cache structure for managed table states (non-append-only `min`/`max`, `string_agg`).
pub struct Cache<K: Ord, V> {
    /// The capacity of the cache.
    capacity: usize,
    /// Ordered cache entries.
    entries: BTreeMap<K, V>,
}

impl<K: Ord, V> Cache<K, V> {
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
    /// Key: `OrderedRow` composed of order by fields.
    /// Value: The value fields that are to be aggregated.
    pub fn insert(&mut self, key: K, value: V) {
        self.entries.insert(key, value);
        // evict if capacity is reached
        while self.entries.len() > self.capacity {
            self.entries.pop_last();
        }
    }

    /// Remove an entry from the cache.
    pub fn remove(&mut self, key: K) {
        self.entries.remove(&key);
    }

    /// Get the last (largest) key in the cache
    pub fn last_key(&self) -> Option<&K> {
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
