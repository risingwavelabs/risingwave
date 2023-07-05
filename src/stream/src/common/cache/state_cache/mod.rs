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

pub use ordered::*;
use risingwave_common::array::Op;
use risingwave_common::estimate_size::EstimateSize;
pub use top_n::*;

mod ordered;
mod top_n;

/// A common interface for state table cache.
pub trait StateCache: EstimateSize {
    type Key: Ord + EstimateSize;
    type Value: EstimateSize;

    /// Type of state cache filler, for syncing the cache with the state table.
    type Filler<'a>: StateCacheFiller<Key = Self::Key, Value = Self::Value> + 'a
    where
        Self: 'a;

    /// Check if the cache is synced with the state table.
    fn is_synced(&self) -> bool;

    /// Begin syncing the cache with the state table.
    fn begin_syncing(&mut self) -> Self::Filler<'_>;

    /// Insert an entry into the cache. Should not break cache validity.
    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Option<Self::Value>;

    /// Delete an entry from the cache. Should not break cache validity.
    fn delete(&mut self, key: &Self::Key) -> Option<Self::Value>;

    /// Apply a batch of operations to the cache. Should not break cache validity.
    fn apply_batch(&mut self, batch: impl IntoIterator<Item = (Op, Self::Key, Self::Value)>);

    /// Clear the cache.
    fn clear(&mut self);

    /// Iterate over the values in the cache.
    fn values(&self) -> impl Iterator<Item = &Self::Value>;

    /// Get the reference of first key-value pair in the cache.
    fn first_key_value(&self) -> Option<(&Self::Key, &Self::Value)>;
}

pub trait StateCacheFiller {
    type Key: Ord;
    type Value;

    /// Get the capacity of the cache.
    fn capacity(&self) -> Option<usize>;

    /// Insert an entry into the cache without cache validity check.
    fn insert_unchecked(&mut self, key: Self::Key, value: Self::Value);

    /// Finish syncing the cache with the state table. This should mark the cache as synced.
    fn finish(self);
}
