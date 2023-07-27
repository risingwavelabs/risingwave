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

use std::cmp::Reverse;
use risingwave_common::array::Op;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DefaultOrdered, ScalarImpl, ScalarRefImpl};
use crate::common::cache::{StateCache, StateCacheFiller, TopNStateCache};

/// The watermark cache key is just an OwnedRow wrapped in `DefaultOrdered`.
/// This is because we want to use the `DefaultOrdered` implementation of `Ord`.
/// The assumption is that the watermark column is the first column in the row.
/// So it should automatically be ordered by the watermark column.
/// We disregard the ordering of the remaining PK datums.
type WatermarkCacheKey = Reverse<DefaultOrdered<OwnedRow>>;

/// Updates to Cache:
/// -----------------
/// INSERT
///    A. Cache evicted. Update cached value.
///    B. Cache uninitialized. Initialize cache, insert into TopNCache.
///    C. Cache not empty. Insert into TopNCache.
///
/// DELETE
///    A. Matches lowest value pk. Remove lowest value. Mark cache as Evicted.
///       Later on Barrier we will refresh the cache with table scan.
///       Since on barrier we will clean up all values before watermark,
///       We have less rows to scan.
///    B. Does not match. Do nothing.
///
/// UPDATE
///    Nothing. Watermark is part of pk. Pk won't change.
///
/// BARRIER
///    State table commit. See below.
///
/// STATE TABLE COMMIT
///     A. Decide whether to do state cleaning:
///        if watermark_to_be_cleaned < smallest val OR no value in cache + cache is synced: No need issue delete range.
///        if watermark_to_be_cleaned => smallest val OR cache not synced: Issue delete ranges.
///
///     B. Refreshing the cache:
///        On barrier, do table scan from `[most_recently_cleaned_watermark, +inf)`.
///        Take the Top N rows and insert into cache.
///        This has to be implemented in `state_table`.
///        We don't need to store any values, just the keys.
///
/// TODO(kwannoel): Optimization: We can use cache to do point delete rather than range delete.
/// FIXME(kwannoel): This should be a trait.
/// Then only when building state table with watermark we will initialize it.
/// Otherwise point it to a no-op implementation.
/// TODO(kwannoel): Add tests for it.
#[derive(EstimateSize, Clone)]
pub(crate) struct StateTableWatermarkCache {
    pk_indices: Vec<usize>,
    inner: TopNStateCache<WatermarkCacheKey, ()>,
}

pub(crate) enum StateTableWatermarkCacheEntry {
    /// There is some lowest value.
    Lowest(ScalarImpl),
    /// Cache out of sync, we can't rely on it.
    NotSynced,
    /// No values in cache
    Empty,
}

impl StateTableWatermarkCacheEntry {
    pub fn not_synced(&self) -> bool {
        matches!(self, Self::NotSynced)
    }
}

impl StateTableWatermarkCache {
    pub fn new(pk_indices: Vec<usize>) -> Self {
        Self {
            pk_indices,
            inner: TopNStateCache::new(2048), // TODO: This number is arbitrary
        }
    }

    /// Get the lowest value.
    fn first_key(&self) -> Option<&WatermarkCacheKey> {
        self.inner.first_key_value().map(|(k, _)| k)
    }

    pub fn lowest_value(&self) -> Option<ScalarRefImpl<'_>> {
        self.first_key().and_then(|k| k.0.0.datum_at(0))
    }

    /// Insert a new value.
    pub fn insert(&mut self, key: impl Row) -> Option<()> {
        self.inner.insert(Reverse(DefaultOrdered(key.into_owned_row())), ())
    }

    /// Delete a value
    pub fn delete(&mut self, key: & impl Row) -> Option<()> {
        self.inner.delete(&Reverse(DefaultOrdered(key.into_owned_row())))
    }
}

impl StateCache for StateTableWatermarkCache {
    type Key = WatermarkCacheKey;
    type Value = ();
    type Filler<'a> = &'a mut TopNStateCache<WatermarkCacheKey, ()>;

    fn is_synced(&self) -> bool {
        self.inner.is_synced()
    }

    fn begin_syncing(&mut self) -> Self::Filler<'_> {
        self.inner.begin_syncing()
    }

    fn insert(&mut self, key: Self::Key, value: Self::Value) -> Option<Self::Value> {
        self.inner.insert(key, value)
    }

    fn delete(&mut self, key: &Self::Key) -> Option<Self::Value> {
        self.inner.delete(key)
    }

    fn apply_batch(&mut self, batch: impl IntoIterator<Item=(Op, Self::Key, Self::Value)>) {
        self.inner.apply_batch(batch)
    }

    fn clear(&mut self) {
        self.inner.clear()
    }

    fn values(&self) -> impl Iterator<Item=&Self::Value> {
        self.inner.values()
    }

    fn first_key_value(&self) -> Option<(&Self::Key, &Self::Value)> {
        self.inner.first_key_value()
    }
}