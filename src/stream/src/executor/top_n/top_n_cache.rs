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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;

use itertools::Itertools;
use risingwave_common::array::{Op, RowRef};
use risingwave_common::row::{CompactedRow, OwnedRow, Row, RowDeserializer, RowExt};
use risingwave_common::types::DataType;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_common_estimate_size::collections::EstimatedBTreeMap;
use risingwave_storage::StateStore;

use super::{GroupKey, ManagedTopNState};
use crate::consistency::{consistency_error, enable_strict_consistency};
use crate::executor::error::StreamExecutorResult;

/// `CacheKey` is composed of `(order_by, remaining columns of pk)`.
pub type CacheKey = (Vec<u8>, Vec<u8>);
pub type Cache = EstimatedBTreeMap<CacheKey, CompactedRow>;

const TOPN_CACHE_HIGH_CAPACITY_FACTOR: usize = 2;
const TOPN_CACHE_MIN_CAPACITY: usize = 10;

/// Cache for [`ManagedTopNState`].
///
/// The key in the maps [`CacheKey`] is `[ order_by + remaining columns of pk ]`. `group_key` is not
/// included.
///
/// # `WITH_TIES`
///
/// `WITH_TIES` supports the semantic of `FETCH FIRST n ROWS WITH TIES` and `RANK() <= n`.
///
/// `OFFSET m FETCH FIRST n ROWS WITH TIES` and `m <= RANK() <= n` are not supported now,
/// since they have different semantics.
pub struct TopNCache<const WITH_TIES: bool> {
    /// Rows in the range `[0, offset)`. Should always be synced with state table.
    pub low: Option<Cache>,

    /// Rows in the range `[offset, offset+limit)`. Should always be synced with state table.
    ///
    /// When `WITH_TIES` is true, it also stores ties for the last element,
    /// and thus the size can be larger than `limit`.
    pub middle: Cache,

    /// Cache of the beginning rows in the range `[offset+limit, ...)`.
    ///
    /// This is very similar to [`TopNStateCache`], which only caches the top-N rows in the table
    /// and only accepts new records that are less than the largest in the cache.
    ///
    /// When `WITH_TIES` is true, it guarantees that the ties of the last element are in the cache,
    /// and thus the size can be larger than `rest_cache_capacity`.
    ///
    /// When the cache becomes empty, if the `table_row_count` is not matched, we need to view the cache
    /// as unsynced and refill it from the state table.
    ///
    /// TODO(rc): later we should reuse [`TopNStateCache`] here.
    ///
    /// [`TopNStateCache`]: crate::common::state_cache::TopNStateCache
    pub high: Cache,
    pub high_cache_capacity: usize,

    pub offset: usize,
    /// Assumption: `limit != 0`
    pub limit: usize,

    /// Number of rows corresponding to the current group.
    /// This is a nice-to-have information. `None` means we don't know the row count,
    /// but it doesn't prevent us from working correctly.
    table_row_count: Option<usize>,

    /// Data types for the full row.
    ///
    /// For debug formatting only.
    data_types: Vec<DataType>,
}

impl<const WITH_TIES: bool> EstimateSize for TopNCache<WITH_TIES> {
    fn estimated_heap_size(&self) -> usize {
        self.low.estimated_heap_size()
            + self.middle.estimated_heap_size()
            + self.high.estimated_heap_size()
    }
}

impl<const WITH_TIES: bool> Debug for TopNCache<WITH_TIES> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TopNCache {{\n  offset: {}, limit: {}, high_cache_capacity: {},\n",
            self.offset, self.limit, self.high_cache_capacity
        )?;

        fn format_cache(
            f: &mut std::fmt::Formatter<'_>,
            cache: &Cache,
            data_types: &[DataType],
        ) -> std::fmt::Result {
            if cache.is_empty() {
                return write!(f, "    <empty>");
            }
            write!(
                f,
                "    {}",
                cache
                    .iter()
                    .format_with("\n    ", |item, f| f(&format_args!(
                        "{:?}, {}",
                        item.0,
                        item.1.deserialize(data_types).unwrap().display(),
                    )))
            )
        }

        writeln!(f, "  low:")?;
        if let Some(low) = &self.low {
            format_cache(f, low, &self.data_types)?;
        } else {
            writeln!(f, "    <none>")?;
        }
        writeln!(f, "\n  middle:")?;
        format_cache(f, &self.middle, &self.data_types)?;
        writeln!(f, "\n  high:")?;
        format_cache(f, &self.high, &self.data_types)?;

        write!(f, "\n}}")?;
        Ok(())
    }
}

/// This trait is used as a bound. It is needed since
/// `TopNCache::<true>::f` and `TopNCache::<false>::f`
/// don't imply `TopNCache::<WITH_TIES>::f`.
pub trait TopNCacheTrait {
    /// Insert input row to corresponding cache range according to its order key.
    ///
    /// Changes in `self.middle` is recorded to `res_ops` and `res_rows`, which will be
    /// used to generate messages to be sent to downstream operators.
    fn insert(&mut self, cache_key: CacheKey, row: impl Row + Send, staging: &mut TopNStaging);

    /// Delete input row from the cache.
    ///
    /// Changes in `self.middle` is recorded to `res_ops` and `res_rows`, which will be
    /// used to generate messages to be sent to downstream operators.
    ///
    /// Because we may need to refill data from the state table to `self.high` during the delete
    /// operation, we need to pass in `group_key`, `epoch` and `managed_state` to do a prefix
    /// scan of the state table.
    fn delete<S: StateStore>(
        &mut self,
        group_key: Option<impl GroupKey>,
        managed_state: &mut ManagedTopNState<S>,
        cache_key: CacheKey,
        row: impl Row + Send,
        staging: &mut TopNStaging,
    ) -> impl Future<Output = StreamExecutorResult<()>> + Send;
}

impl<const WITH_TIES: bool> TopNCache<WITH_TIES> {
    /// `data_types` -- Data types for the full row.
    pub fn new(offset: usize, limit: usize, data_types: Vec<DataType>) -> Self {
        assert!(limit > 0);
        if WITH_TIES {
            // It's trickier to support.
            // Also `OFFSET WITH TIES` has different semantic with `a < RANK() < b`
            assert!(offset == 0, "OFFSET is not supported with WITH TIES");
        }
        let high_cache_capacity = offset
            .checked_add(limit)
            .and_then(|v| v.checked_mul(TOPN_CACHE_HIGH_CAPACITY_FACTOR))
            .unwrap_or(usize::MAX)
            .max(TOPN_CACHE_MIN_CAPACITY);
        Self {
            low: if offset > 0 { Some(Cache::new()) } else { None },
            middle: Cache::new(),
            high: Cache::new(),
            high_cache_capacity,
            offset,
            limit,
            table_row_count: None,
            data_types,
        }
    }

    /// Clear the cache. After this, the cache must be `init` again before use.
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.low.as_mut().map(Cache::clear);
        self.middle.clear();
        self.high.clear();
    }

    /// Get total count of entries in the cache.
    pub fn len(&self) -> usize {
        self.low.as_ref().map(Cache::len).unwrap_or(0) + self.middle.len() + self.high.len()
    }

    pub(super) fn update_table_row_count(&mut self, table_row_count: usize) {
        self.table_row_count = Some(table_row_count)
    }

    pub fn low_is_full(&self) -> bool {
        if let Some(low) = &self.low {
            assert!(low.len() <= self.offset);
            let full = low.len() == self.offset;
            if !full {
                assert!(self.middle.is_empty());
                assert!(self.high.is_empty());
            }
            full
        } else {
            true
        }
    }

    pub fn middle_is_full(&self) -> bool {
        // For WITH_TIES, the middle cache can exceed the capacity.
        if !WITH_TIES {
            assert!(
                self.middle.len() <= self.limit,
                "the middle cache exceeds the capacity\n{self:?}"
            );
        }
        let full = self.middle.len() >= self.limit;
        if full {
            assert!(self.low_is_full());
        } else {
            assert!(
                self.high.is_empty(),
                "the high cache is not empty when middle cache is not full:\n{self:?}"
            );
        }
        full
    }

    pub fn high_is_full(&self) -> bool {
        // For WITH_TIES, the high cache can exceed the capacity.
        if !WITH_TIES {
            assert!(self.high.len() <= self.high_cache_capacity);
        }
        self.high.len() >= self.high_cache_capacity
    }

    fn high_is_synced(&self) -> bool {
        if !self.high.is_empty() {
            true
        } else {
            // check if table row count matches
            self.table_row_count
                .map(|n| n == self.len())
                .unwrap_or(false)
        }
    }

    fn last_cache_key_before_high(&self) -> Option<&CacheKey> {
        let middle_last_key = self.middle.last_key_value().map(|(k, _)| k);
        middle_last_key.or_else(|| {
            self.low
                .as_ref()
                .and_then(Cache::last_key_value)
                .map(|(k, _)| k)
        })
    }
}

impl TopNCacheTrait for TopNCache<false> {
    fn insert(&mut self, cache_key: CacheKey, row: impl Row + Send, staging: &mut TopNStaging) {
        if let Some(row_count) = self.table_row_count.as_mut() {
            *row_count += 1;
        }

        let mut to_insert = (cache_key, (&row).into());
        let mut is_last_of_lower_cache = false; // for saving one key comparison

        let low_is_full = self.low_is_full();
        if let Some(low) = &mut self.low {
            // try insert into low cache

            if !low_is_full {
                low.insert(to_insert.0, to_insert.1);
                return;
            }

            // low cache is full
            let low_last = low.last_entry().unwrap();
            if &to_insert.0 < low_last.key() {
                // make space for the new entry
                let low_last = low_last.remove_entry();
                low.insert(to_insert.0, to_insert.1);
                to_insert = low_last; // move the last entry to the middle cache
                is_last_of_lower_cache = true;
            }
        }

        // try insert into middle cache

        if !self.middle_is_full() {
            self.middle.insert(to_insert.0.clone(), to_insert.1.clone());
            staging.insert(to_insert.0, to_insert.1);
            return;
        }

        // middle cache is full
        let middle_last = self.middle.last_entry().unwrap();
        if is_last_of_lower_cache || &to_insert.0 < middle_last.key() {
            // make space for the new entry
            let middle_last = middle_last.remove_entry();
            self.middle.insert(to_insert.0.clone(), to_insert.1.clone());

            staging.delete(middle_last.0.clone(), middle_last.1.clone());
            staging.insert(to_insert.0, to_insert.1);

            to_insert = middle_last; // move the last entry to the high cache
            is_last_of_lower_cache = true;
        }

        // try insert into high cache

        // The logic is a bit different from the other two caches, because high cache is not
        // guaranteed to be fully synced with the "high part" of the table.

        if is_last_of_lower_cache || self.high_is_synced() {
            // For `is_last_of_lower_cache`, an obvious observation is that the key to insert is
            // always smaller than any key in the high part of the table.

            if self.high.is_empty() {
                // if high cache is empty, we can insert directly
                self.high.insert(to_insert.0, to_insert.1);
                return;
            }

            let high_is_full = self.high_is_full();
            let high_last = self.high.last_entry().unwrap();

            if is_last_of_lower_cache || &to_insert.0 < high_last.key() {
                // we can only insert if the key is smaller than the largest key in the high cache
                if high_is_full {
                    // make space for the new entry
                    high_last.remove_entry();
                }
                self.high.insert(to_insert.0, to_insert.1);
            }
        }
    }

    async fn delete<S: StateStore>(
        &mut self,
        group_key: Option<impl GroupKey>,
        managed_state: &mut ManagedTopNState<S>,
        cache_key: CacheKey,
        row: impl Row + Send,
        staging: &mut TopNStaging,
    ) -> StreamExecutorResult<()> {
        if !enable_strict_consistency() && self.table_row_count == Some(0) {
            // If strict consistency is disabled, and we receive a `DELETE` but the row count is 0, we
            // should not panic. Instead, we pretend that we don't know about the actually row count.
            consistency_error!("table row count is 0, but we receive a DELETE operation");
            self.table_row_count = None;
        }
        if let Some(row_count) = self.table_row_count.as_mut() {
            *row_count -= 1;
        }

        if self.middle_is_full() && &cache_key > self.middle.last_key_value().unwrap().0 {
            // the row is in high
            self.high.remove(&cache_key);
        } else if self.low_is_full()
            && self
                .low
                .as_ref()
                .map(|low| &cache_key > low.last_key_value().unwrap().0)
                .unwrap_or(
                    true, // if low is None, `cache_key` should be in middle
                )
        {
            // the row is in middle
            let removed = self.middle.remove(&cache_key);
            staging.delete(cache_key.clone(), (&row).into());

            if removed.is_none() {
                // the middle cache should always be synced, if the key is not found, then it also doesn't
                // exist in the state table
                consistency_error!(
                    ?group_key,
                    ?cache_key,
                    "cache key not found in middle cache"
                );
                return Ok(());
            }

            // refill the high cache if it's not synced
            if !self.high_is_synced() {
                self.high.clear();
                managed_state
                    .fill_high_cache(
                        group_key,
                        self,
                        self.last_cache_key_before_high().cloned(),
                        self.high_cache_capacity,
                    )
                    .await?;
            }

            // bring one element, if any, from high cache to middle cache
            if !self.high.is_empty() {
                let high_first = self.high.pop_first().unwrap();
                self.middle
                    .insert(high_first.0.clone(), high_first.1.clone());
                staging.insert(high_first.0, high_first.1);
            }

            assert!(self.high.is_empty() || self.middle.len() == self.limit);
        } else {
            // the row is in low
            let low = self.low.as_mut().unwrap();
            let removed = low.remove(&cache_key);

            if removed.is_none() {
                // the low cache should always be synced, if the key is not found, then it also doesn't
                // exist in the state table
                consistency_error!(?group_key, ?cache_key, "cache key not found in low cache");
                return Ok(());
            }

            // bring one element, if any, from middle cache to low cache
            if !self.middle.is_empty() {
                let middle_first = self.middle.pop_first().unwrap();
                staging.delete(middle_first.0.clone(), middle_first.1.clone());
                low.insert(middle_first.0, middle_first.1);

                // fill the high cache if it's not synced
                if !self.high_is_synced() {
                    self.high.clear();
                    managed_state
                        .fill_high_cache(
                            group_key,
                            self,
                            self.last_cache_key_before_high().cloned(),
                            self.high_cache_capacity,
                        )
                        .await?;
                }

                // bring one element, if any, from high cache to middle cache
                if !self.high.is_empty() {
                    let high_first = self.high.pop_first().unwrap();
                    self.middle
                        .insert(high_first.0.clone(), high_first.1.clone());
                    staging.insert(high_first.0, high_first.1);
                }
            }
        }

        Ok(())
    }
}

impl TopNCacheTrait for TopNCache<true> {
    fn insert(&mut self, cache_key: CacheKey, row: impl Row + Send, staging: &mut TopNStaging) {
        if let Some(row_count) = self.table_row_count.as_mut() {
            *row_count += 1;
        }

        assert!(
            self.low.is_none(),
            "Offset is not supported yet for WITH TIES, so low cache should be None"
        );

        let to_insert: (CacheKey, CompactedRow) = (cache_key, (&row).into());

        // try insert into middle cache

        if !self.middle_is_full() {
            self.middle.insert(to_insert.0.clone(), to_insert.1.clone());
            staging.insert(to_insert.0.clone(), to_insert.1);
            return;
        }

        // middle cache is full

        let to_insert_sort_key = &(to_insert.0).0;
        let middle_last_sort_key = self.middle.last_key().unwrap().0.clone();

        match to_insert_sort_key.cmp(&middle_last_sort_key) {
            Ordering::Less => {
                // the row is in middle
                let n_ties_of_last = self
                    .middle
                    .range((middle_last_sort_key.clone(), vec![])..)
                    .count();
                // We evict the last row and its ties only if the number of remaining rows still is
                // still larger than limit, i.e., there are limit-1 other rows.
                //
                // e.g., limit = 3, [1,1,1,1]
                // insert 0 -> [0,1,1,1,1]
                // insert 0 -> [0,0,1,1,1,1]
                // insert 0 -> [0,0,0]
                if self.middle.len() + 1 - n_ties_of_last >= self.limit {
                    // Middle will be full without the last element and its ties after insertion.
                    // Let's move the last element and its ties to high cache first.
                    while let Some(middle_last) = self.middle.last_entry()
                        && middle_last.key().0 == middle_last_sort_key
                    {
                        let middle_last = middle_last.remove_entry();
                        staging.delete(middle_last.0.clone(), middle_last.1.clone());
                        // we can blindly move entries from middle cache to high cache no matter high cache is synced or not
                        self.high.insert(middle_last.0, middle_last.1);
                    }
                }
                if self.high.len() > self.high_cache_capacity {
                    // evict some entries from high cache if it exceeds the capacity
                    let high_last = self.high.pop_last().unwrap();
                    let high_last_sort_key = (high_last.0).0;
                    // Remove all ties of the last element in high cache, for the sake of simplicity.
                    // This may cause repeatedly refill the high cache if number of ties is large.
                    self.high.retain(|k, _| k.0 != high_last_sort_key);
                }

                self.middle.insert(to_insert.0.clone(), to_insert.1.clone());
                staging.insert(to_insert.0, to_insert.1);
            }
            Ordering::Equal => {
                // the row is in middle and is a tie of the last row
                self.middle.insert(to_insert.0.clone(), to_insert.1.clone());
                staging.insert(to_insert.0, to_insert.1);
            }
            Ordering::Greater => {
                // the row is in high

                if self.high_is_synced() {
                    // only insert into high cache if it is synced

                    if self.high.is_empty() {
                        // if high cache is empty, we can insert directly
                        self.high.insert(to_insert.0, to_insert.1);
                        return;
                    }

                    if to_insert_sort_key <= &self.high.last_key().unwrap().0 {
                        // We can only insert if the key is <= the largest key in the high cache.
                        // Note that we have all ties of the last element in the high cache, so we can
                        // safely compare only the sort key.
                        self.high.insert(to_insert.0, to_insert.1);
                    }

                    if self.high.len() > self.high_cache_capacity {
                        // evict some entries from high cache if it exceeds the capacity
                        let high_last = self.high.pop_last().unwrap();
                        let high_last_sort_key = (high_last.0).0;
                        // Remove all ties of the last element in high cache, for the sake of simplicity.
                        // This may cause repeatedly refill the high cache if number of ties is large.
                        self.high.retain(|k, _| k.0 != high_last_sort_key);
                    }
                }
            }
        }
    }

    async fn delete<S: StateStore>(
        &mut self,
        group_key: Option<impl GroupKey>,
        managed_state: &mut ManagedTopNState<S>,
        cache_key: CacheKey,
        row: impl Row + Send,
        staging: &mut TopNStaging,
    ) -> StreamExecutorResult<()> {
        if !enable_strict_consistency() && self.table_row_count == Some(0) {
            // If strict consistency is disabled, and we receive a `DELETE` but the row count is 0, we
            // should not panic. Instead, we pretend that we don't know about the actually row count.
            self.table_row_count = None;
        }
        if let Some(row_count) = self.table_row_count.as_mut() {
            *row_count -= 1;
        }

        assert!(
            self.low.is_none(),
            "Offset is not supported yet for WITH TIES, so low cache should be None"
        );

        if self.middle.is_empty() {
            consistency_error!(
                ?group_key,
                ?cache_key,
                "middle cache is empty, but we receive a DELETE operation"
            );
            staging.delete(cache_key, (&row).into());
            return Ok(());
        }

        let middle_last_sort_key = self.middle.last_key().unwrap().0.clone();

        let to_delete_sort_key = cache_key.0.clone();
        if to_delete_sort_key > middle_last_sort_key {
            // the row is in high
            self.high.remove(&cache_key);
        } else {
            // the row is in middle
            self.middle.remove(&cache_key);
            staging.delete(cache_key.clone(), (&row).into());
            if self.middle.len() >= self.limit {
                // this can happen when there are ties
                return Ok(());
            }

            // refill the high cache if it's not synced
            if !self.high_is_synced() {
                managed_state
                    .fill_high_cache(
                        group_key,
                        self,
                        self.last_cache_key_before_high().cloned(),
                        self.high_cache_capacity,
                    )
                    .await?;
            }

            // bring the first element and its ties, if any, from high cache to middle cache
            if !self.high.is_empty() {
                let high_first = self.high.pop_first().unwrap();
                let high_first_sort_key = (high_first.0).0.clone();
                assert!(high_first_sort_key > middle_last_sort_key);

                self.middle
                    .insert(high_first.0.clone(), high_first.1.clone());
                staging.insert(high_first.0, high_first.1);

                for (cache_key, row) in self.high.extract_if(|k, _| k.0 == high_first_sort_key) {
                    self.middle.insert(cache_key.clone(), row.clone());
                    staging.insert(cache_key, row);
                }
            }
        }

        Ok(())
    }
}

/// Similar to [`TopNCacheTrait`], but for append-only TopN.
pub trait AppendOnlyTopNCacheTrait {
    /// Insert input row to corresponding cache range according to its order key.
    ///
    /// Changes in `self.middle` is recorded to `res_ops` and `res_rows`, which will be
    /// used to generate messages to be sent to downstream operators.
    ///
    /// `managed_state` is required because different from normal TopN, append-only TopN
    /// doesn't insert all rows into the state table.
    fn insert<S: StateStore>(
        &mut self,
        cache_key: CacheKey,
        row_ref: RowRef<'_>,
        staging: &mut TopNStaging,
        managed_state: &mut ManagedTopNState<S>,
        row_deserializer: &RowDeserializer,
    ) -> StreamExecutorResult<()>;
}

impl AppendOnlyTopNCacheTrait for TopNCache<false> {
    fn insert<S: StateStore>(
        &mut self,
        cache_key: CacheKey,
        row_ref: RowRef<'_>,
        staging: &mut TopNStaging,
        managed_state: &mut ManagedTopNState<S>,
        row_deserializer: &RowDeserializer,
    ) -> StreamExecutorResult<()> {
        if self.middle_is_full() && &cache_key >= self.middle.last_key().unwrap() {
            return Ok(());
        }
        managed_state.insert(row_ref);

        // insert input row into corresponding cache according to its sort key
        let mut to_insert = (cache_key, row_ref.into());

        let low_is_full = self.low_is_full();
        if let Some(low) = &mut self.low {
            // try insert into low cache

            if !low_is_full {
                low.insert(to_insert.0, to_insert.1);
                return Ok(());
            }

            // low cache is full
            let low_last = low.last_entry().unwrap();
            if &to_insert.0 < low_last.key() {
                // make space for the new entry
                let low_last = low_last.remove_entry();
                low.insert(to_insert.0, to_insert.1);
                to_insert = low_last; // move the last entry to the middle cache
            }
        }

        // try insert into middle cache

        if !self.middle_is_full() {
            self.middle.insert(to_insert.0.clone(), to_insert.1.clone());
            staging.insert(to_insert.0, to_insert.1);
            return Ok(());
        }

        // The row must be in the range of [offset, offset+limit).
        // the largest row in `cache.middle` needs to be removed.
        let middle_last = self.middle.pop_last().unwrap();
        debug_assert!(to_insert.0 < middle_last.0);
        managed_state.delete(row_deserializer.deserialize(middle_last.1.row.as_ref())?);
        staging.delete(middle_last.0, middle_last.1);

        self.middle.insert(to_insert.0.clone(), to_insert.1.clone());
        staging.insert(to_insert.0, to_insert.1);

        // Unlike normal topN, append only topN does not use the high part of the cache.

        Ok(())
    }
}

impl AppendOnlyTopNCacheTrait for TopNCache<true> {
    fn insert<S: StateStore>(
        &mut self,
        cache_key: CacheKey,
        row_ref: RowRef<'_>,
        staging: &mut TopNStaging,
        managed_state: &mut ManagedTopNState<S>,
        row_deserializer: &RowDeserializer,
    ) -> StreamExecutorResult<()> {
        assert!(
            self.low.is_none(),
            "Offset is not supported yet for WITH TIES, so low cache should be empty"
        );

        let to_insert = (cache_key, row_ref);

        // try insert into middle cache

        if !self.middle_is_full() {
            managed_state.insert(to_insert.1);
            let row: CompactedRow = to_insert.1.into();
            self.middle.insert(to_insert.0.clone(), row.clone());
            staging.insert(to_insert.0, row);
            return Ok(());
        }

        // middle cache is full

        let to_insert_sort_key = &(to_insert.0).0;
        let middle_last_sort_key = self.middle.last_key().unwrap().0.clone();

        match to_insert_sort_key.cmp(&middle_last_sort_key) {
            Ordering::Less => {
                // the row is in middle
                let n_ties_of_last = self
                    .middle
                    .range((middle_last_sort_key.clone(), vec![])..)
                    .count();
                // We evict the last row and its ties only if the number of remaining rows is
                // still larger than limit, i.e., there are limit-1 other rows.
                //
                // e.g., limit = 3, [1,1,1,1]
                // insert 0 -> [0,1,1,1,1]
                // insert 0 -> [0,0,1,1,1,1]
                // insert 0 -> [0,0,0]
                if self.middle.len() + 1 - n_ties_of_last >= self.limit {
                    // middle will be full without the last element and its ties after insertion
                    while let Some(middle_last) = self.middle.last_entry()
                        && middle_last.key().0 == middle_last_sort_key
                    {
                        let middle_last = middle_last.remove_entry();
                        // we don't need to maintain the high part so just delete it from state table
                        managed_state
                            .delete(row_deserializer.deserialize(middle_last.1.row.as_ref())?);
                        staging.delete(middle_last.0, middle_last.1);
                    }
                }

                managed_state.insert(to_insert.1);
                let row: CompactedRow = to_insert.1.into();
                self.middle.insert(to_insert.0.clone(), row.clone());
                staging.insert(to_insert.0, row);
            }
            Ordering::Equal => {
                // the row is in middle and is a tie of the last row
                managed_state.insert(to_insert.1);
                let row: CompactedRow = to_insert.1.into();
                self.middle.insert(to_insert.0.clone(), row.clone());
                staging.insert(to_insert.0, row);
            }
            Ordering::Greater => {
                // the row is in high, do nothing
            }
        }

        Ok(())
    }
}

/// Used to build diff between before and after applying an input chunk, for `TopNCache` (of one group).
/// It should be maintained when an entry is inserted or deleted from the `middle` cache.
#[derive(Debug, Default)]
pub struct TopNStaging {
    to_delete: BTreeMap<CacheKey, CompactedRow>,
    to_insert: BTreeMap<CacheKey, CompactedRow>,
    to_update: BTreeMap<CacheKey, (CompactedRow, CompactedRow)>,
}

impl TopNStaging {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a row into the staging changes. This method must be called when a row is
    /// added to the `middle` cache.
    fn insert(&mut self, cache_key: CacheKey, row: CompactedRow) {
        if let Some(old_row) = self.to_delete.remove(&cache_key) {
            if old_row != row {
                self.to_update.insert(cache_key, (old_row, row));
            }
        } else {
            self.to_insert.insert(cache_key, row);
        }
    }

    /// Delete a row from the staging changes. This method must be called when a row is
    /// removed from the `middle` cache.
    fn delete(&mut self, cache_key: CacheKey, row: CompactedRow) {
        if self.to_insert.remove(&cache_key).is_some() {
            // do nothing more
        } else if let Some((old_row, _)) = self.to_update.remove(&cache_key) {
            self.to_delete.insert(cache_key, old_row);
        } else {
            self.to_delete.insert(cache_key, row);
        }
    }

    /// Get the count of effective changes in the staging.
    pub fn len(&self) -> usize {
        self.to_delete.len() + self.to_insert.len() + self.to_update.len()
    }

    /// Check if the staging is empty.
    pub fn is_empty(&self) -> bool {
        self.to_delete.is_empty() && self.to_insert.is_empty() && self.to_update.is_empty()
    }

    /// Iterate over the changes in the staging.
    pub fn into_changes(self) -> impl Iterator<Item = (Op, CompactedRow)> {
        #[cfg(debug_assertions)]
        {
            let keys = self
                .to_delete
                .keys()
                .chain(self.to_insert.keys())
                .chain(self.to_update.keys())
                .unique()
                .count();
            assert_eq!(
                keys,
                self.to_delete.len() + self.to_insert.len() + self.to_update.len(),
                "should not have duplicate keys with different operations",
            );
        }

        // We expect one `CacheKey` to appear at most once in the staging, and, the order of
        // the outputs of `TopN` doesn't really matter, so we can simply chain the three maps.
        // Although the output order is not important, we still ensure that `Delete`s are emitted
        // before `Insert`s, so that we can avoid temporary violation of the `LIMIT` constraint.
        self.to_update
            .into_values()
            .flat_map(|(old_row, new_row)| {
                [(Op::UpdateDelete, old_row), (Op::UpdateInsert, new_row)]
            })
            .chain(self.to_delete.into_values().map(|row| (Op::Delete, row)))
            .chain(self.to_insert.into_values().map(|row| (Op::Insert, row)))
    }

    /// Iterate over the changes in the staging, and deserialize the rows.
    pub fn into_deserialized_changes(
        self,
        deserializer: &RowDeserializer,
    ) -> impl Iterator<Item = StreamExecutorResult<(Op, OwnedRow)>> + '_ {
        self.into_changes()
            .map(|(op, row)| Ok((op, deserializer.deserialize(row.row.as_ref())?)))
    }
}
