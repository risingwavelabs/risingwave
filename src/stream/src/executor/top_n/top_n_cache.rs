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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::{Op, RowRef};
use risingwave_common::collection::estimate_size::EstimateSize;
use risingwave_common::row::{CompactedRow, Row, RowDeserializer, RowExt};
use risingwave_common::types::DataType;
use risingwave_storage::StateStore;

use crate::executor::error::StreamExecutorResult;
use crate::executor::managed_state::top_n::{GroupKey, ManagedTopNState};

const TOPN_CACHE_HIGH_CAPACITY_FACTOR: usize = 2;

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
    /// Rows in the range `[0, offset)`
    pub low: BTreeMap<CacheKey, CompactedRow>,
    /// Rows in the range `[offset, offset+limit)`
    ///
    /// When `WITH_TIES` is true, it also stores ties for the last element,
    /// and thus the size can be larger than `limit`.
    pub middle: BTreeMap<CacheKey, CompactedRow>,
    /// Rows in the range `[offset+limit, offset+limit+high_capacity)`
    ///
    /// When `WITH_TIES` is true, it also stores ties for the last element,
    /// and thus the size can be larger than `high_capacity`.
    pub high: BTreeMap<CacheKey, CompactedRow>,
    pub high_capacity: usize,
    pub offset: usize,
    /// Assumption: `limit != 0`
    pub limit: usize,

    /// Data types for the full row.
    ///
    /// For debug formatting only.
    data_types: Vec<DataType>,
}

impl<const WITH_TIES: bool> EstimateSize for TopNCache<WITH_TIES> {
    fn estimated_heap_size(&self) -> usize {
        // FIXME: implement a correct size.
        // https://github.com/risingwavelabs/risingwave/issues/8957
        0
    }
}

impl<const WITH_TIES: bool> Debug for TopNCache<WITH_TIES> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TopNCache {{\n  offset: {}, limit: {}, high_capacity: {},\n",
            self.offset, self.limit, self.high_capacity
        )?;

        fn format_cache(
            f: &mut std::fmt::Formatter<'_>,
            cache: &BTreeMap<CacheKey, CompactedRow>,
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
        format_cache(f, &self.low, &self.data_types)?;
        writeln!(f, "\n  middle:")?;
        format_cache(f, &self.middle, &self.data_types)?;
        writeln!(f, "\n  high:")?;
        format_cache(f, &self.high, &self.data_types)?;

        write!(f, "\n}}")?;
        Ok(())
    }
}

/// `CacheKey` is composed of `(order_by, remaining columns of pk)`.
pub type CacheKey = (Vec<u8>, Vec<u8>);

/// This trait is used as a bound. It is needed since
/// `TopNCache::<true>::f` and `TopNCache::<false>::f`
/// don't imply `TopNCache::<WITH_TIES>::f`.
#[async_trait]
pub trait TopNCacheTrait {
    /// Insert input row to corresponding cache range according to its order key.
    ///
    /// Changes in `self.middle` is recorded to `res_ops` and `res_rows`, which will be
    /// used to generate messages to be sent to downstream operators.
    #[allow(clippy::too_many_arguments)]
    fn insert(
        &mut self,
        cache_key: CacheKey,
        row: impl Row + Send,
        res_ops: &mut Vec<Op>,
        res_rows: &mut Vec<CompactedRow>,
    );

    /// Delete input row from the cache.
    ///
    /// Changes in `self.middle` is recorded to `res_ops` and `res_rows`, which will be
    /// used to generate messages to be sent to downstream operators.
    ///
    /// Because we may need to add data from the state table to `self.high` during the delete
    /// operation, we need to pass in `group_key`, `epoch` and `managed_state` to do a prefix
    /// scan of the state table.
    #[allow(clippy::too_many_arguments)]
    async fn delete<S: StateStore>(
        &mut self,
        group_key: Option<impl GroupKey>,
        managed_state: &mut ManagedTopNState<S>,
        cache_key: CacheKey,
        row: impl Row + Send,
        res_ops: &mut Vec<Op>,
        res_rows: &mut Vec<CompactedRow>,
    ) -> StreamExecutorResult<()>;
}

impl<const WITH_TIES: bool> TopNCache<WITH_TIES> {
    /// `data_types` -- Data types for the full row.
    pub fn new(offset: usize, limit: usize, data_types: Vec<DataType>) -> Self {
        assert!(limit != 0);
        if WITH_TIES {
            // It's trickier to support.
            // Also `OFFSET WITH TIES` has different semantic with `a < RANK() < b`
            assert!(offset == 0, "OFFSET is not supported with WITH TIES");
        }
        Self {
            low: BTreeMap::new(),
            middle: BTreeMap::new(),
            high: BTreeMap::new(),
            high_capacity: offset
                .checked_add(limit)
                .and_then(|v| v.checked_mul(TOPN_CACHE_HIGH_CAPACITY_FACTOR))
                .unwrap_or(usize::MAX),
            offset,
            limit,
            data_types,
        }
    }

    /// Clear the cache. After this, the cache must be `init` again before use.
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.low.clear();
        self.middle.clear();
        self.high.clear();
    }

    pub fn is_low_cache_full(&self) -> bool {
        assert!(self.low.len() <= self.offset);
        let full = self.low.len() == self.offset;
        if !full {
            assert!(self.middle.is_empty());
            assert!(self.high.is_empty());
        }
        full
    }

    pub fn is_middle_cache_full(&self) -> bool {
        // For WITH_TIES, the middle cache can exceed the capacity.
        if !WITH_TIES {
            assert!(
                self.middle.len() <= self.limit,
                "the middle cache exceeds the capacity\n{self:?}"
            );
        }
        let full = self.middle.len() >= self.limit;
        if full {
            assert!(self.is_low_cache_full());
        } else {
            assert!(
                self.high.is_empty(),
                "the high cache is not empty when middle cache is not full:\n{self:?}"
            );
        }
        full
    }

    pub fn is_high_cache_full(&self) -> bool {
        // For WITH_TIES, the high cache can exceed the capacity.
        if !WITH_TIES {
            assert!(self.high.len() <= self.high_capacity);
        }
        self.high.len() >= self.high_capacity
    }

    /// Use this method instead of `self.high.insert` directly when possible.
    ///
    /// It only inserts into high cache if the key is smaller than the largest key in the high
    /// cache. Otherwise, we simply ignore the row. We will wait until the high cache becomes
    /// empty and fill it at that time.
    fn insert_high_cache(&mut self, cache_key: CacheKey, row: CompactedRow, is_from_middle: bool) {
        if !self.is_high_cache_full() {
            if is_from_middle {
                self.high.insert(cache_key, row);
                return;
            }
            // For direct insert, we need to check if the key is smaller than the largest key
            if let Some(high_last) = self.high.last_key_value() && cache_key <= *high_last.0 {
                debug_assert!(cache_key != *high_last.0, "cache_key should be unique");
                self.high.insert(cache_key, row);
            }
        } else {
            let high_last = self.high.last_entry().unwrap();
            if cache_key <= *high_last.key() {
                debug_assert!(cache_key != *high_last.key(), "cache_key should be unique");
                high_last.remove_entry();
                self.high.insert(cache_key, row);
            }
        }
    }
}

#[async_trait]
impl TopNCacheTrait for TopNCache<false> {
    fn insert(
        &mut self,
        cache_key: CacheKey,
        row: impl Row + Send,
        res_ops: &mut Vec<Op>,
        res_rows: &mut Vec<CompactedRow>,
    ) {
        if !self.is_low_cache_full() {
            self.low.insert(cache_key, (&row).into());
            return;
        }
        let elem_to_compare_with_middle =
            if let Some(low_last) = self.low.last_entry()
                && cache_key <= *low_last.key() {
                // Take the last element of `cache.low` and insert input row to it.
                let low_last = low_last.remove_entry();
                self.low.insert(cache_key, (&row).into());
                low_last
            } else {
                (cache_key, (&row).into())
            };

        if !self.is_middle_cache_full() {
            self.middle.insert(
                elem_to_compare_with_middle.0,
                elem_to_compare_with_middle.1.clone(),
            );
            res_ops.push(Op::Insert);
            res_rows.push(elem_to_compare_with_middle.1);
            return;
        }

        let mut is_from_middle = false;
        let elem_to_compare_with_high = {
            let middle_last = self.middle.last_entry().unwrap();
            if elem_to_compare_with_middle.0 <= *middle_last.key() {
                // If the row in the range of [offset, offset+limit), the largest row in
                // `cache.middle` needs to be moved to `cache.high`
                let res = middle_last.remove_entry();
                res_ops.push(Op::Delete);
                res_rows.push(res.1.clone());
                res_ops.push(Op::Insert);
                res_rows.push(elem_to_compare_with_middle.1.clone());
                self.middle
                    .insert(elem_to_compare_with_middle.0, elem_to_compare_with_middle.1);
                is_from_middle = true;
                res
            } else {
                elem_to_compare_with_middle
            }
        };

        self.insert_high_cache(
            elem_to_compare_with_high.0,
            elem_to_compare_with_high.1,
            is_from_middle,
        );
    }

    #[allow(clippy::too_many_arguments)]
    async fn delete<S: StateStore>(
        &mut self,
        group_key: Option<impl GroupKey>,
        managed_state: &mut ManagedTopNState<S>,
        cache_key: CacheKey,
        row: impl Row + Send,
        res_ops: &mut Vec<Op>,
        res_rows: &mut Vec<CompactedRow>,
    ) -> StreamExecutorResult<()> {
        if self.is_middle_cache_full() && cache_key > *self.middle.last_key_value().unwrap().0 {
            // The row is in high
            self.high.remove(&cache_key);
        } else if self.is_low_cache_full()
            && (self.offset == 0 || cache_key > *self.low.last_key_value().unwrap().0)
        {
            // The row is in mid
            // Try to fill the high cache if it is empty
            if self.high.is_empty() {
                managed_state
                    .fill_high_cache(
                        group_key,
                        self,
                        self.middle.last_key_value().unwrap().0.clone(),
                        self.high_capacity,
                    )
                    .await?;
            }

            self.middle.remove(&cache_key);
            res_ops.push(Op::Delete);
            res_rows.push((&row).into());

            // Bring one element, if any, from high cache to middle cache
            if !self.high.is_empty() {
                let high_first = self.high.pop_first().unwrap();
                res_ops.push(Op::Insert);
                res_rows.push(high_first.1.clone());
                self.middle.insert(high_first.0, high_first.1);
            }
        } else {
            // The row is in low
            self.low.remove(&cache_key);

            // Bring one element, if any, from middle cache to low cache
            if !self.middle.is_empty() {
                let middle_first = self.middle.pop_first().unwrap();
                res_ops.push(Op::Delete);
                res_rows.push(middle_first.1.clone());
                self.low.insert(middle_first.0, middle_first.1);

                // Try to fill the high cache if it is empty
                if self.high.is_empty() {
                    managed_state
                        .fill_high_cache(
                            group_key,
                            self,
                            self.middle.last_key_value().unwrap().0.clone(),
                            self.high_capacity,
                        )
                        .await?;
                }

                // Bring one element, if any, from high cache to middle cache
                if !self.high.is_empty() {
                    let high_first = self.high.pop_first().unwrap();
                    res_ops.push(Op::Insert);
                    res_rows.push(high_first.1.clone());
                    self.middle.insert(high_first.0, high_first.1);
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl TopNCacheTrait for TopNCache<true> {
    fn insert(
        &mut self,
        cache_key: CacheKey,
        row: impl Row + Send,
        res_ops: &mut Vec<Op>,
        res_rows: &mut Vec<CompactedRow>,
    ) {
        assert!(
            self.low.is_empty(),
            "Offset is not supported yet for WITH TIES, so low cache should be empty"
        );

        let elem_to_compare_with_middle = (cache_key, row);

        if !self.is_middle_cache_full() {
            self.middle.insert(
                elem_to_compare_with_middle.0.clone(),
                (&elem_to_compare_with_middle.1).into(),
            );
            res_ops.push(Op::Insert);
            res_rows.push((&elem_to_compare_with_middle.1).into());
            return;
        }

        let sort_key = &elem_to_compare_with_middle.0 .0;
        let middle_last = self.middle.last_key_value().unwrap();
        let middle_last_order_by = &middle_last.0 .0.clone();

        match sort_key.cmp(middle_last_order_by) {
            Ordering::Less => {
                // The row is in middle.
                let num_ties = self
                    .middle
                    .range((middle_last_order_by.clone(), vec![])..)
                    .count();
                // We evict the last row and its ties only if the number of remaining rows still is
                // still larger than limit, i.e., there are limit-1 other rows.
                //
                // e.g., limit = 3, [1,1,1,1]
                // insert 0 -> [0,1,1,1,1]
                // insert 0 -> [0,0,1,1,1,1]
                // insert 0 -> [0,0,0]
                if self.middle.len() - num_ties + 1 >= self.limit {
                    while let Some(middle_last) = self.middle.last_entry()
                    && middle_last.key().0 == middle_last_order_by.clone() {
                        let middle_last = middle_last.remove_entry();
                        res_ops.push(Op::Delete);
                        res_rows.push(middle_last.1.clone());
                        self.high.insert(middle_last.0, middle_last.1);
                    }
                }
                if self.high.len() >= self.high_capacity {
                    let high_last = self.high.pop_last().unwrap();
                    let high_last_order_by = high_last.0 .0;
                    self.high.drain_filter(|k, _| k.0 == high_last_order_by);
                }

                res_ops.push(Op::Insert);
                res_rows.push((&elem_to_compare_with_middle.1).into());
                self.middle.insert(
                    elem_to_compare_with_middle.0,
                    (&elem_to_compare_with_middle.1).into(),
                );
            }
            Ordering::Equal => {
                // The row is in middle and is a tie with the last row.
                res_ops.push(Op::Insert);
                res_rows.push((&elem_to_compare_with_middle.1).into());
                self.middle.insert(
                    elem_to_compare_with_middle.0,
                    (&elem_to_compare_with_middle.1).into(),
                );
            }
            Ordering::Greater => {
                // The row is in high.
                let elem_to_compare_with_high = elem_to_compare_with_middle;
                self.insert_high_cache(
                    elem_to_compare_with_high.0,
                    elem_to_compare_with_high.1.into(),
                    false,
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn delete<S: StateStore>(
        &mut self,
        group_key: Option<impl GroupKey>,
        managed_state: &mut ManagedTopNState<S>,
        cache_key: CacheKey,
        row: impl Row + Send,
        res_ops: &mut Vec<Op>,
        res_rows: &mut Vec<CompactedRow>,
    ) -> StreamExecutorResult<()> {
        // Since low cache is always empty for WITH_TIES, this unwrap is safe.

        let middle_last = self.middle.last_key_value().unwrap();
        let middle_last_order_by = middle_last.0 .0.clone();

        let sort_key = cache_key.0.clone();
        if sort_key > middle_last_order_by {
            // The row is in high.
            self.high.remove(&cache_key);
        } else {
            // The row is in middle
            self.middle.remove(&cache_key);
            res_ops.push(Op::Delete);
            res_rows.push((&row).into());
            if self.middle.len() >= self.limit {
                // This can happen when there are ties.
                return Ok(());
            }

            // Try to fill the high cache if it is empty
            if self.high.is_empty() {
                managed_state
                    .fill_high_cache(
                        group_key,
                        self,
                        self.middle.last_key_value().unwrap().0.clone(),
                        self.high_capacity,
                    )
                    .await?;
            }

            // Bring elements with the same sort key, if any, from high cache to middle cache.
            if !self.high.is_empty() {
                let high_first = self.high.pop_first().unwrap();
                let high_first_order_by = high_first.0 .0.clone();
                assert!(high_first_order_by > middle_last_order_by);

                res_ops.push(Op::Insert);
                res_rows.push(high_first.1.clone());
                self.middle.insert(high_first.0, high_first.1);

                // We need to trigger insert for all rows with prefix `high_first_order_by`
                // in high cache.
                for (ordered_pk_row, row) in
                    self.high.drain_filter(|k, _| k.0 == high_first_order_by)
                {
                    if ordered_pk_row.0 != high_first_order_by {
                        break;
                    }
                    res_ops.push(Op::Insert);
                    res_rows.push(row.clone());
                    self.middle.insert(ordered_pk_row, row);
                }
            }
        }

        Ok(())
    }
}

/// Similar to [`TopNCacheTrait`], but for append-only TopN.
#[async_trait]
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
        res_ops: &mut Vec<Op>,
        res_rows: &mut Vec<CompactedRow>,
        managed_state: &mut ManagedTopNState<S>,
        row_deserializer: &RowDeserializer,
    ) -> StreamExecutorResult<()>;
}

#[async_trait]
impl AppendOnlyTopNCacheTrait for TopNCache<false> {
    fn insert<S: StateStore>(
        &mut self,
        cache_key: CacheKey,
        row_ref: RowRef<'_>,
        res_ops: &mut Vec<Op>,
        res_rows: &mut Vec<CompactedRow>,
        managed_state: &mut ManagedTopNState<S>,
        row_deserializer: &RowDeserializer,
    ) -> StreamExecutorResult<()> {
        if self.is_middle_cache_full() && &cache_key >= self.middle.last_key_value().unwrap().0 {
            return Ok(());
        }
        managed_state.insert(row_ref);

        // Then insert input row to corresponding cache range according to its order key
        if !self.is_low_cache_full() {
            self.low.insert(cache_key, row_ref.into());
            return Ok(());
        }

        let elem_to_insert_into_middle =
            if let Some(low_last) = self.low.last_entry()
                && &cache_key <= low_last.key() {
                // Take the last element of `cache.low` and insert input row to it.
                let low_last = low_last.remove_entry();
                self.low.insert(cache_key, row_ref.into());
                low_last
            } else {
                (cache_key, row_ref.into())
            };

        if !self.is_middle_cache_full() {
            self.middle.insert(
                elem_to_insert_into_middle.0,
                elem_to_insert_into_middle.1.clone(),
            );
            res_ops.push(Op::Insert);
            res_rows.push(elem_to_insert_into_middle.1);
            return Ok(());
        }

        // The row must be in the range of [offset, offset+limit).
        // the largest row in `cache.middle` needs to be removed.
        let middle_last = self.middle.pop_last().unwrap();
        debug_assert!(elem_to_insert_into_middle.0 < middle_last.0);

        res_ops.push(Op::Delete);
        res_rows.push(middle_last.1.clone());
        managed_state.delete(row_deserializer.deserialize(middle_last.1.row.as_ref())?);

        res_ops.push(Op::Insert);
        res_rows.push(elem_to_insert_into_middle.1.clone());
        self.middle
            .insert(elem_to_insert_into_middle.0, elem_to_insert_into_middle.1);

        // Unlike normal topN, append only topN does not use the high part of the cache.

        Ok(())
    }
}

#[async_trait]
impl AppendOnlyTopNCacheTrait for TopNCache<true> {
    fn insert<S: StateStore>(
        &mut self,
        cache_key: CacheKey,
        row_ref: RowRef<'_>,
        res_ops: &mut Vec<Op>,
        res_rows: &mut Vec<CompactedRow>,
        managed_state: &mut ManagedTopNState<S>,
        row_deserializer: &RowDeserializer,
    ) -> StreamExecutorResult<()> {
        assert!(
            self.low.is_empty(),
            "Offset is not supported yet for WITH TIES, so low cache should be empty"
        );
        let elem_to_compare_with_middle = (cache_key, row_ref);

        if !self.is_middle_cache_full() {
            let row: CompactedRow = elem_to_compare_with_middle.1.into();
            managed_state.insert(elem_to_compare_with_middle.1);
            self.middle
                .insert(elem_to_compare_with_middle.0.clone(), row.clone());
            res_ops.push(Op::Insert);
            res_rows.push(row);
            return Ok(());
        }

        let sort_key = &elem_to_compare_with_middle.0 .0;
        let middle_last = self.middle.last_key_value().unwrap();
        let middle_last_order_by = &middle_last.0 .0.clone();

        match sort_key.cmp(middle_last_order_by) {
            Ordering::Less => {
                // The row is in middle.
                let num_ties = self
                    .middle
                    .range((middle_last_order_by.clone(), vec![])..)
                    .count();
                // We evict the last row and its ties only if the number of remaining rows is
                // still larger than limit, i.e., there are limit-1 other rows.
                //
                // e.g., limit = 3, [1,1,1,1]
                // insert 0 -> [0,1,1,1,1]
                // insert 0 -> [0,0,1,1,1,1]
                // insert 0 -> [0,0,0]
                if self.middle.len() - num_ties + 1 >= self.limit {
                    while let Some(middle_last) = self.middle.last_entry()
                    && &middle_last.key().0 == middle_last_order_by {
                        let middle_last = middle_last.remove_entry();
                        res_ops.push(Op::Delete);
                        res_rows.push(middle_last.1.clone());
                        managed_state.delete(row_deserializer.deserialize(middle_last.1.row.as_ref())?);
                    }
                }

                managed_state.insert(elem_to_compare_with_middle.1);
                res_ops.push(Op::Insert);
                res_rows.push((&elem_to_compare_with_middle.1).into());
                self.middle.insert(
                    elem_to_compare_with_middle.0,
                    (&elem_to_compare_with_middle.1).into(),
                );
            }
            Ordering::Equal => {
                // The row is in middle and is a tie with the last row.
                managed_state.insert(elem_to_compare_with_middle.1);
                res_ops.push(Op::Insert);
                res_rows.push((&elem_to_compare_with_middle.1).into());
                self.middle.insert(
                    elem_to_compare_with_middle.0,
                    (&elem_to_compare_with_middle.1).into(),
                );
            }
            Ordering::Greater => {
                // The row is in high. Do nothing.
            }
        }

        Ok(())
    }
}
