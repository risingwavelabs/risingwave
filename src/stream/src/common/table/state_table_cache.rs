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
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DefaultOrdered, ScalarRefImpl};

use crate::common::cache::{StateCache, TopNStateCache};

/// The watermark cache key is just an `OwnedRow` wrapped in `DefaultOrdered`.
/// This is because we want to use the `DefaultOrdered` implementation of `Ord`.
/// The assumption is that the watermark column is the first column in the row.
/// So it should automatically be ordered by the watermark column.
/// We disregard the ordering of the remaining PK datums.
type WatermarkCacheKey = DefaultOrdered<OwnedRow>;

/// Updates to Cache:
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
///     A. Decide whether to do state cleaning:
///        if `watermark_to_be_cleaned` < smallest val OR no value in cache + cache is synced: No need
/// issue delete range.        if `watermark_to_be_cleaned` => smallest val OR cache not synced: Issue
/// delete ranges.
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
    inner: TopNStateCache<WatermarkCacheKey, ()>,
}

impl StateTableWatermarkCache {
    pub fn new(size: usize) -> Self {
        Self {
            inner: TopNStateCache::new(size), // TODO: This number is arbitrary
        }
    }

    /// Get the lowest key.
    fn top_key(&self) -> Option<&WatermarkCacheKey> {
        self.inner.top_key_value().map(|(k, _)| k)
    }

    // Get the watermark value from the top key.
    pub fn lowest_key(&self) -> Option<ScalarRefImpl<'_>> {
        self.top_key().and_then(|k| k.0.datum_at(0))
    }

    /// Insert a new value.
    pub fn insert(&mut self, key: impl Row) -> Option<()> {
        self.inner
            .insert(DefaultOrdered(key.into_owned_row()), ())
    }

    /// Delete a value
    pub fn delete(&mut self, key: &impl Row) -> Option<()> {
        self.inner
            .delete(&DefaultOrdered(key.into_owned_row()))
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
    use itertools::Itertools;
    use risingwave_common::types::{Scalar, Timestamptz};
    use crate::common::cache::StateCacheFiller;

    use super::*;

    // TODO: Test out of sync -> sync
    // TODO: Test eviction.
    #[test]
    fn test_state_table_watermark_cache_insert_then_delete() {
        let mut cache = StateTableWatermarkCache::new(3);
        let filler = cache.begin_syncing();
        filler.finish();
        assert!(
            DefaultOrdered(Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()))
            < DefaultOrdered(Some(Timestamptz::from_secs(999).unwrap().to_scalar_value()))
        );
        assert_eq!(
            DefaultOrdered(Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value())),
            DefaultOrdered(Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()))
        );
        assert!(cache.first_key_value().is_none());
        assert!(cache.lowest_key().is_none());

        let v1 = [
            Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1000).unwrap().to_scalar_value()),
        ];
        let old_v = cache.insert(&v1);
        assert!(old_v.is_none());
        let lowest = cache.lowest_key().unwrap();
        assert_eq!(lowest, v1[0].clone().unwrap().as_scalar_ref_impl());

        let v2 = [
            Some(Timestamptz::from_secs(999).unwrap().to_scalar_value()),
            Some(Timestamptz::from_secs(1234).unwrap().to_scalar_value()),
        ];

        assert!(DefaultOrdered(&v1) > DefaultOrdered(&v2));
        assert!(!(DefaultOrdered(&v1) <= DefaultOrdered(&v2)));
        let old_v = cache.insert(&v2);
        assert!(old_v.is_none());
        let lowest = cache.lowest_key().unwrap();
        assert_eq!(lowest, v2[0].clone().unwrap().as_scalar_ref_impl());

        // cache.insert(3);
        // cache.insert(1);
        // assert_eq!(cache.len(), 3);
        // assert_eq!(
        //     cache.first_key_value(),
        //     Some((&1, &"risingwave!".to_string()))
        // );
        // assert_eq!(cache.first_key(), Some(&1));
        // assert_eq!(cache.last_key_value(), Some((&5, &"hello".to_string())));
        // assert_eq!(cache.last_key(), Some(&5));
        // assert_eq!(
        //     cache.values().collect_vec(),
        //     vec!["risingwave!", "world", "hello"]
        // );
        //
        // cache.insert(0, "foo".to_string());
        // assert_eq!(cache.capacity(), 3);
        // assert_eq!(cache.len(), 3);
        // assert_eq!(cache.first_key(), Some(&0));
        // assert_eq!(cache.last_key(), Some(&3));
        // assert_eq!(
        //     cache.values().collect_vec(),
        //     vec!["foo", "risingwave!", "world"]
        // );
        //
        // let old_val = cache.remove(&0);
        // assert_eq!(old_val, Some("foo".to_string()));
        // assert_eq!(cache.len(), 2);
        // assert_eq!(cache.first_key(), Some(&1));
        // assert_eq!(cache.last_key(), Some(&3));
        // cache.remove(&3);
        // assert_eq!(cache.len(), 1);
        // assert_eq!(cache.first_key(), Some(&1));
        // assert_eq!(cache.last_key(), Some(&1));
        // let old_val = cache.remove(&100); // can remove non-existing key
        // assert!(old_val.is_none());
        // assert_eq!(cache.len(), 1);
        //
        // cache.clear();
        // assert_eq!(cache.len(), 0);
        // assert_eq!(cache.capacity(), 3);
        // assert_eq!(cache.first_key(), None);
        // assert_eq!(cache.last_key(), None);
    }
}
