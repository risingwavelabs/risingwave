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

use risingwave_common::array::Op;
use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{DefaultOrdered, ScalarRefImpl};

use crate::common::cache::{StateCache, TopNStateCache};

/// The watermark cache key is just an `OwnedRow` wrapped in `DefaultOrdered`.
/// This is because we want to use the `DefaultOrdered` implementation of `Ord`.
/// The assumption is that the watermark column is the first column in the row.
/// So it should automatically be ordered by the watermark column.
/// We disregard the ordering of the remaining PK datums.
///
/// TODO(kwannoel):
/// We can also store bytes from encoded pk.
/// The benefit is we deserialization of PK when refiling the cache.
type WatermarkCacheKey = DefaultOrdered<OwnedRow>;

/// Cache Invariants
/// -----------------
/// - If cache is synced, it will ALWAYS contain TopN entries. NOTE: TopN here refers to the lowest
///   N entries, where the first is the minimum and so on.
/// - Cache will not contain NULL values in the watermark column, they are ignored in watermark
///   state cleaning. They will not be included in the cache's table row count either, since they
///   are treated as invisible.
///
/// Updates to Cache
/// -----------------
/// INSERT
///    A. Cache evicted. Update cached value.
///    B. Cache uninitialized. Initialize cache, insert into `TopNCache`.
///    C. Cache not empty. Insert into `TopNCache`.
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
///    A. Decide whether to do state cleaning:
///        if `watermark_to_be_cleaned` < smallest val
///           OR no value in cache + cache is synced:
///        No need issue delete range.
///        if `watermark_to_be_cleaned` => smallest val OR cache not synced:
///        Issue delete ranges.
///
///    B. Refreshing the cache:
///        On barrier, do table scan from `most_recently_cleaned_watermark` (inclusive) to +inf.
///        Take the Top N rows and insert into cache.
///        This has to be implemented in `state_table`.
///        We do not need to store any values, just the keys.
///
/// TODO(kwannoel):
/// Optimization: If cache is not full,
/// we can also do point delete for each cache entry.
/// Not sure if this is more optimal,
/// we have to measure this scenario to see if it is indeed better.
#[derive(EstimateSize, Clone)]
pub struct StateTableWatermarkCache {
    inner: TopNStateCache<WatermarkCacheKey, ()>,
}

impl StateTableWatermarkCache {
    pub fn new(size: usize) -> Self {
        Self {
            inner: TopNStateCache::new(size),
        }
    }

    /// NOTE(kwannoel): Unused. Requires row count from State Table.
    /// On first initialization, we can use `row_count` 0.
    /// But if state table is reconstructed after recovery, we need to obtain row count meta-data.
    #[allow(dead_code)]
    fn new_with_row_count(size: usize, row_count: usize) -> Self {
        Self {
            inner: TopNStateCache::with_table_row_count(size, row_count),
        }
    }

    /// Get the lowest key.
    fn first_key(&self) -> Option<&WatermarkCacheKey> {
        self.inner.first_key_value().map(|(k, _)| k)
    }

    // Get the watermark value from the top key.
    pub fn lowest_key(&self) -> Option<ScalarRefImpl<'_>> {
        self.first_key().and_then(|k| k.0.datum_at(0))
    }

    /// Insert a new value.
    pub fn insert(&mut self, key: &impl Row) {
        if !key.is_null_at(0) {
            self.inner.insert(DefaultOrdered(key.into_owned_row()), ());
        }
    }

    /// Delete a value
    /// If the watermark col is NULL, it will just be ignored.
    pub fn delete(&mut self, key: &impl Row) {
        if !key.is_null_at(0) {
            self.inner.delete(&DefaultOrdered(key.into_owned_row()));
        }
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity_inner()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn set_table_row_count(&mut self, table_row_count: usize) {
        self.inner.set_table_row_count(table_row_count)
    }

    #[cfg(test)]
    pub fn get_table_row_count(&self) -> &Option<usize> {
        self.inner.get_table_row_count()
    }
}

impl StateCache for StateTableWatermarkCache {
    type Filler<'a> = &'a mut TopNStateCache<WatermarkCacheKey, ()>;
    type Key = WatermarkCacheKey;
    type Value = ();

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

    fn apply_batch(&mut self, batch: impl IntoIterator<Item = (Op, Self::Key, Self::Value)>) {
        self.inner.apply_batch(batch)
    }

    fn clear(&mut self) {
        self.inner.clear()
    }

    fn values(&self) -> impl Iterator<Item = &Self::Value> {
        self.inner.values()
    }

    fn first_key_value(&self) -> Option<(&Self::Key, &Self::Value)> {
        self.inner.first_key_value()
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::types::{Scalar, Timestamptz};

    use super::*;
    use crate::common::cache::StateCacheFiller;

    /// With capacity 3, test the following sequence of inserts:
    /// Insert
    /// [SYNC] an empty table first.
    /// [1000] should insert, cache is empty.
    /// [999] should insert, smaller than 1000, should be lowest value.
    /// [2000] should NOT insert.
    /// It is larger than 1000, and we don't know the state table size,
    /// for instance if state table just recovered, and does not have row count meta data.
    /// This means there could be a row like [1001] in the state table.
    /// [900, ...], should insert.
    /// [800], should insert, smaller than 900, should be lowest value.
    #[test]
    fn test_state_table_watermark_cache_inserts() {
        let v1 = [
            Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        let v2 = [
            Some(Timestamptz::from_secs(999).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        let v3 = [
            Some(Timestamptz::from_secs(2000).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        let v4 = [
            Some(Timestamptz::from_secs(900).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        let v5 = [
            Some(Timestamptz::from_secs(800).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        let mut cache = StateTableWatermarkCache::new(3);
        assert_eq!(cache.capacity(), 3);
        let filler = cache.begin_syncing();
        filler.finish();

        // Test 1000
        cache.insert(&v1);
        assert_eq!(cache.len(), 1);
        assert_eq!(
            cache.lowest_key(),
            Some(v1[0].as_ref().unwrap().as_scalar_ref_impl())
        );

        // Test 999
        cache.insert(&v2);
        assert_eq!(cache.len(), 2);
        assert_eq!(
            cache.lowest_key(),
            Some(v2[0].as_ref().unwrap().as_scalar_ref_impl())
        );

        // Test 2000
        cache.insert(&v3);
        assert_eq!(cache.len(), 2);
        assert_eq!(
            cache.lowest_key(),
            Some(v2[0].as_ref().unwrap().as_scalar_ref_impl())
        );

        // Test 900
        cache.insert(&v4);
        assert_eq!(cache.len(), 3);
        assert_eq!(
            cache.lowest_key(),
            Some(v4[0].as_ref().unwrap().as_scalar_ref_impl())
        );

        // Test 800
        cache.insert(&v5);
        assert_eq!(cache.len(), 3);
        assert_eq!(
            cache.lowest_key(),
            Some(v5[0].as_ref().unwrap().as_scalar_ref_impl())
        );
    }

    #[test]
    fn test_state_table_watermark_cache_delete_non_existent_value() {
        let mut cache = StateTableWatermarkCache::new(3);
        assert_eq!(cache.capacity(), 3);
        let filler = cache.begin_syncing();
        filler.finish();
        let v1 = [
            Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        cache.delete(&v1);
    }

    /// With capacity 3, test the following sequence of inserts:
    /// Insert
    /// [1000, ...], should insert, cache is empty.
    /// [999, ...], should insert, smaller than 1000, should be lowest value.
    /// [2000, ...], should insert, although larger than largest val (1000), cache rows still match
    /// state table rows. [3000, ...], should be ignored
    /// [900, ...], should evict 2000
    #[test]
    fn test_state_table_watermark_cache_with_row_count_inserts() {
        let mut cache = StateTableWatermarkCache::new_with_row_count(3, 0);
        assert_eq!(cache.capacity(), 3);
        let filler = cache.begin_syncing();
        filler.finish();
        assert!(cache.first_key_value().is_none());
        assert!(cache.lowest_key().is_none());

        let v1 = [
            Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()),
        ];
        cache.insert(&v1);
        let lowest = cache.lowest_key().unwrap();
        assert_eq!(lowest, v1[0].clone().unwrap().as_scalar_ref_impl());

        let v2 = [
            Some(Timestamptz::from_secs(999).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        cache.insert(&v2);
        assert_eq!(cache.len(), 2);
        let lowest = cache.lowest_key().unwrap();
        assert_eq!(lowest, v2[0].clone().unwrap().as_scalar_ref_impl());

        let v3 = [
            Some(Timestamptz::from_secs(2000).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        cache.insert(&v3);
        assert_eq!(cache.len(), 3);

        let v4 = [
            Some(Timestamptz::from_secs(3000).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        cache.insert(&v4);
        assert_eq!(cache.len(), 3);

        let v5 = [
            Some(Timestamptz::from_secs(900).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        cache.insert(&v5);
        assert_eq!(cache.len(), 3);
        let lowest = cache.lowest_key().unwrap();
        assert_eq!(lowest, v5[0].clone().unwrap().as_scalar_ref_impl());
    }

    /// With capacity 3, seed the following sequence of inserts:
    /// Insert:
    /// [1000, ...]
    /// [999, ...]
    /// [2000, ...]
    /// [3000, ...]
    /// [900, ...]
    ///
    /// In the cache there should be:
    /// [900, 999, 1000]
    /// Then run one DELETE.
    /// [999].
    ///
    /// The cache should be:
    /// [900, 1000].
    /// Lowest val: 900.
    ///
    /// Then run one INSERT.
    /// [1001].
    /// This should be ignored. It is larger than the largest val in the cache (1000).
    /// And cache no longer matches state table rows.
    ///
    /// Then run another INSERT.
    /// [950].
    /// This should be accepted. It is smaller than the largest val in the cache (1000).
    ///
    /// Then run DELETE.
    /// [900].
    /// Lowest val: 1000.
    ///
    /// Then run DELETE.
    /// [1000].
    /// Lowest val: None.
    ///
    /// Then run INSERT.
    /// Cache should be out of sync, should reject the insert.
    /// Cache len = 0.
    #[test]
    fn test_state_table_watermark_cache_with_row_count_deletes() {
        // Initial INSERT
        let mut cache = StateTableWatermarkCache::new_with_row_count(3, 0);
        assert_eq!(cache.capacity(), 3);
        let filler = cache.begin_syncing();
        filler.finish();
        let v1 = [
            Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        let v2 = [
            Some(Timestamptz::from_secs(999).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        let v3 = [
            Some(Timestamptz::from_secs(2000).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        let v4 = [
            Some(Timestamptz::from_secs(3000).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        let v5 = [
            Some(Timestamptz::from_secs(900).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        cache.insert(&v1);
        cache.insert(&v2);
        cache.insert(&v3);
        cache.insert(&v4);
        cache.insert(&v5);

        // First Delete
        cache.delete(&v2);
        assert_eq!(cache.len(), 2);
        let lowest = cache.lowest_key().unwrap();
        assert_eq!(lowest, v5[0].clone().unwrap().as_scalar_ref_impl());

        // Insert 1001
        let v6 = [
            Some(Timestamptz::from_secs(1001).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        cache.insert(&v6);
        assert_eq!(cache.len(), 2);

        // Insert 950
        let v7 = [
            Some(Timestamptz::from_secs(950).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];
        cache.insert(&v7);
        assert_eq!(cache.len(), 3);

        // Delete 950
        cache.delete(&v7);
        assert_eq!(cache.len(), 2);

        // DELETE
        cache.delete(&v5);
        assert_eq!(cache.len(), 1);
        assert_eq!(
            cache.lowest_key().unwrap(),
            v1[0].clone().unwrap().as_scalar_ref_impl()
        );

        // DELETE
        cache.delete(&v1);
        assert_eq!(cache.len(), 0);
        assert!(!cache.is_synced());

        // INSERT after Out of sync
        cache.insert(&v1);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_watermark_cache_syncing() {
        let v1 = vec![
            Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()),
            Some(1000i64.into()),
        ];
        let v2 = vec![
            Some(Timestamptz::from_secs(3000).unwrap().to_scalar_value()),
            Some(1000i64.into()),
        ];
        let v3 = vec![
            Some(Timestamptz::from_secs(2000).unwrap().to_scalar_value()),
            Some(1000i64.into()),
        ];
        let mut cache = StateTableWatermarkCache::new(3);
        let mut filler = cache.begin_syncing();
        filler.insert_unchecked(DefaultOrdered(v1.into_owned_row()), ());
        filler.insert_unchecked(DefaultOrdered(v2.into_owned_row()), ());
        filler.insert_unchecked(DefaultOrdered(v3.into_owned_row()), ());
        filler.finish();
        assert_eq!(cache.len(), 3);
        assert_eq!(
            cache.lowest_key().unwrap(),
            v1[0].clone().unwrap().as_scalar_ref_impl()
        );
    }
}
