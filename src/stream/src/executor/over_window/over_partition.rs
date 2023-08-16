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

//! Types and functions that store or manipulate state/cache inside one single over window
//! partition.

use std::collections::{BTreeMap, VecDeque};
use std::marker::PhantomData;
use std::ops::{Bound, RangeInclusive};

use futures::stream::select_all;
use futures::{stream, StreamExt, TryStreamExt};
use futures_async_stream::for_await;
use risingwave_common::array::stream_record::Record;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::DataType;
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::OrderType;
use risingwave_expr::window_function::{FrameBounds, StateKey, WindowFuncCall};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::merge_sort::merge_sort;
use risingwave_storage::StateStore;

use super::delta_btree_map::Change;
use super::estimated_btree_map::EstimatedBTreeMap;
use super::sentinel::KeyWithSentinel;
use crate::executor::over_window::delta_btree_map::DeltaBTreeMap;
use crate::executor::test_utils::prelude::StateTable;
use crate::executor::StreamExecutorResult;

pub(super) type CacheKey = KeyWithSentinel<StateKey>;

/// Range cache for one over window partition.
/// The cache entries can be:
///
/// - `(Normal)*`
/// - `Smallest, (Normal)*, Largest`
/// - `(Normal)+, Largest`
/// - `Smallest, (Normal)+`
///
/// This means it's impossible to only have one sentinel in the cache without any normal entry,
/// and, each of the two types of sentinel can only appear once. Also, since sentinels are either
/// smallest or largest, they always appear at the beginning or the end of the cache.
pub(super) type PartitionCache = EstimatedBTreeMap<CacheKey, OwnedRow>;

/// Changes happened in one over window partition.
pub(super) type PartitionDelta = BTreeMap<CacheKey, Change<OwnedRow>>;

pub(super) fn new_empty_partition_cache() -> PartitionCache {
    let mut cache = PartitionCache::new();
    cache.insert(CacheKey::Smallest, OwnedRow::empty());
    cache.insert(CacheKey::Largest, OwnedRow::empty());
    cache
}

/// A wrapper of [`PartitionCache`] that provides helper methods to manipulate the cache.
/// By putting this type inside `private` module, we can avoid misuse of the internal fields and
/// methods.
pub(super) struct OverPartition<'a, S: StateStore> {
    this_partition_key: &'a OwnedRow,
    partition_key_indices: &'a [usize],
    range_cache: &'a mut PartitionCache,
    order_key_data_types: &'a [DataType],
    order_key_order_types: &'a [OrderType],
    _phantom: PhantomData<S>,
}

const MAGIC_BATCH_SIZE: usize = 512;

impl<'a, S: StateStore> OverPartition<'a, S> {
    pub fn new(
        this_partition_key: &'a OwnedRow,
        partition_key_indices: &'a [usize],
        cache: &'a mut PartitionCache,
        order_key_data_types: &'a [DataType],
        order_key_order_types: &'a [OrderType],
    ) -> Self {
        Self {
            this_partition_key,
            partition_key_indices,
            range_cache: cache,
            order_key_data_types,
            order_key_order_types,
            _phantom: PhantomData,
        }
    }

    /// Get the number of cached entries ignoring sentinels.
    pub fn cache_real_len(&self) -> usize {
        let len = self.range_cache.inner().len();
        if len <= 1 {
            debug_assert!(self
                .range_cache
                .inner()
                .first_key_value()
                .map(|(k, _)| k.is_normal())
                .unwrap_or(true));
            return len;
        }
        // len >= 2
        let cache_inner = self.range_cache.inner();
        let sentinels = [
            // sentinels only appear at the beginning and/or the end
            cache_inner.first_key_value().unwrap().0.is_sentinel(),
            cache_inner.last_key_value().unwrap().0.is_sentinel(),
        ];
        len - sentinels.into_iter().filter(|x| *x).count()
    }

    fn cache_real_first_key(&self) -> Option<&StateKey> {
        self.range_cache
            .inner()
            .iter()
            .find(|(k, _)| k.is_normal())
            .map(|(k, _)| k.as_normal_expect())
    }

    fn cache_real_last_key(&self) -> Option<&StateKey> {
        self.range_cache
            .inner()
            .iter()
            .rev()
            .find(|(k, _)| k.is_normal())
            .map(|(k, _)| k.as_normal_expect())
    }

    fn cache_left_is_sentinel(&self) -> bool {
        self.range_cache
            .first_key_value()
            .map(|(k, _)| k.is_sentinel())
            .unwrap_or(false)
    }

    fn cache_right_is_sentinel(&self) -> bool {
        self.range_cache
            .last_key_value()
            .map(|(k, _)| k.is_sentinel())
            .unwrap_or(false)
    }

    /// Write a change record to state table and cache.
    /// This function must be called after finding affected ranges, which means the change records
    /// should never exceed the cached range.
    pub fn write_record(
        &mut self,
        table: &mut StateTable<S>,
        key: StateKey,
        record: Record<OwnedRow>,
    ) {
        table.write_record(record.as_ref());
        match record {
            Record::Insert { new_row } | Record::Update { new_row, .. } => {
                self.range_cache.insert(CacheKey::from(key), new_row);
            }
            Record::Delete { .. } => {
                self.range_cache.remove(&CacheKey::from(key));

                if self.cache_real_len() == 0 && self.range_cache.len() == 1 {
                    // only one sentinel remains, should insert the other
                    self.range_cache
                        .insert(CacheKey::Smallest, OwnedRow::empty());
                    self.range_cache
                        .insert(CacheKey::Largest, OwnedRow::empty());
                }
            }
        }
    }

    /// Find all ranges in the partition that are affected by the given delta.
    /// The returned ranges are guaranteed to be sorted and non-overlapping. All keys in the ranges
    /// are guaranteed to be cached.
    pub async fn find_affected_ranges<'s, 'cache>(
        &'s mut self,
        table: &'_ StateTable<S>,
        calls: &'_ [WindowFuncCall],
        delta: &'cache PartitionDelta,
    ) -> StreamExecutorResult<(
        DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
        Vec<(
            &'cache CacheKey,
            &'cache CacheKey,
            &'cache CacheKey,
            &'cache CacheKey,
        )>,
    )>
    where
        's: 'cache,
    {
        let delta_first = delta.first_key_value().unwrap().0.as_normal_expect();
        let delta_last = delta.last_key_value().unwrap().0.as_normal_expect();

        // ensure the cache covers all delta (if possible)
        self.extend_cache_by_range(table, &delta_first..=&delta_last)
            .await?;

        loop {
            let (left_reached_sentinel, right_reached_sentinel) = {
                // SAFETY: Here we shortly borrow the range cache and turn the reference into a
                // `'cache` one to bypass the borrow checker. This is safe because we only return
                // the reference once we don't need to do any further mutation.
                let cache_inner = unsafe { &*(self.range_cache.inner() as *const _) };
                let ranges =
                    self::find_affected_ranges(calls, DeltaBTreeMap::new(cache_inner, delta));

                if ranges.is_empty() {
                    // no ranges affected, we're done
                    return Ok((DeltaBTreeMap::new(cache_inner, delta), ranges));
                }

                let left_reached_sentinel = ranges.first().unwrap().0.is_sentinel();
                let right_reached_sentinel = ranges.last().unwrap().3.is_sentinel();

                if !left_reached_sentinel && !right_reached_sentinel {
                    // all affected ranges are already cached, we're done
                    return Ok((DeltaBTreeMap::new(cache_inner, delta), ranges));
                }

                (left_reached_sentinel, right_reached_sentinel)
            };

            if left_reached_sentinel {
                // TODO(rc): should count cache miss for this, and also the below
                tracing::info!("partition cache left extension triggered");
                let left_most = self.cache_real_first_key().unwrap_or(delta_first).clone();
                self.extend_cache_leftward_by_n(table, &left_most).await?;
            }
            if right_reached_sentinel {
                tracing::info!("partition cache right extension triggered");
                let right_most = self.cache_real_last_key().unwrap_or(delta_last).clone();
                self.extend_cache_rightward_by_n(table, &right_most).await?;
            }
            tracing::info!("partition cache extended");
        }
    }

    /// Try to load the given range of entries from table into cache.
    /// When the function returns, it's guaranteed that there's no entry in the table that is within
    /// the given range but not in the cache.
    async fn extend_cache_by_range(
        &mut self,
        table: &StateTable<S>,
        range: RangeInclusive<&StateKey>,
    ) -> StreamExecutorResult<()> {
        if self.cache_real_len() == self.range_cache.len() {
            // no sentinel in the cache, meaning we already cached all entries of this partition
            return Ok(());
        }
        assert!(self.range_cache.len() >= 2);

        let cache_real_first_key = self.cache_real_first_key();
        let cache_real_last_key = self.cache_real_last_key();

        if cache_real_first_key.is_some() && *range.end() < cache_real_first_key.unwrap()
            || cache_real_last_key.is_some() && *range.start() > cache_real_last_key.unwrap()
        {
            // completely not overlapping, for the sake of simplicity, we re-init the cache
            std::mem::replace(self.range_cache, new_empty_partition_cache());
        }

        if self.cache_real_len() == 0 {
            // no normal entry in the cache, just load the given range
            let table_pk_range = (
                Bound::Included(self.state_key_to_table_pk(range.start())?),
                Bound::Included(self.state_key_to_table_pk(range.end())?),
            );
            return self
                .extend_cache_by_range_inner(table, table_pk_range)
                .await;
        }

        // get the first and last keys again, now we are guaranteed to have at least a normal key
        let cache_real_first_key = self.cache_real_first_key().unwrap();
        let cache_real_last_key = self.cache_real_last_key().unwrap();

        if self.cache_left_is_sentinel() && *range.start() < cache_real_first_key {
            // extend leftward only if there's smallest sentinel
            let table_pk_range = (
                Bound::Included(self.state_key_to_table_pk(range.start())?),
                Bound::Excluded(self.state_key_to_table_pk(cache_real_first_key)?),
            );
            return self
                .extend_cache_by_range_inner(table, table_pk_range)
                .await;
        }

        if self.cache_right_is_sentinel() && *range.end() > cache_real_last_key {
            // extend rightward only if there's largest sentinel
            let table_pk_range = (
                Bound::Excluded(self.state_key_to_table_pk(cache_real_last_key)?),
                Bound::Included(self.state_key_to_table_pk(range.end())?),
            );
            return self
                .extend_cache_by_range_inner(table, table_pk_range)
                .await;
        }

        // TODO(rc): uncomment the following to enable prefetching rows before the start of the
        // range;
        // self.extend_cache_leftward_by_n(table, range.start()).await?;

        // prefetch rows after the end of the range
        self.extend_cache_rightward_by_n(table, range.end()).await
    }

    async fn extend_cache_by_range_inner(
        &mut self,
        table: &StateTable<S>,
        table_pk_range: (Bound<impl Row>, Bound<impl Row>),
    ) -> StreamExecutorResult<()> {
        let streams = stream::iter(table.vnode_bitmap().iter_vnodes())
            .map(|vnode| {
                table.iter_with_pk_range(
                    &table_pk_range,
                    vnode,
                    PrefetchOptions::new_for_exhaust_iter(),
                )
            })
            .buffer_unordered(10)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .map(Box::pin);

        #[for_await]
        for row in select_all(streams) {
            let row: OwnedRow = row?;
            let key = self.row_to_state_key(&row)?;
            self.range_cache.insert(CacheKey::from(key), row);
        }

        Ok(())
    }

    async fn extend_cache_leftward_by_n(
        &mut self,
        table: &StateTable<S>,
        hint_key: &StateKey,
    ) -> StreamExecutorResult<()> {
        if self.cache_real_len() == self.range_cache.len() {
            // no sentinel in the cache, meaning we already cached all entries of this partition
            return Ok(());
        }
        assert!(self.range_cache.len() >= 2);

        let left_second = {
            let mut iter = self.range_cache.inner().iter();
            let left_first = iter.next().unwrap().0;
            if left_first.is_normal() {
                // the leftside already reaches the beginning of this partition in the table
                return Ok(());
            }
            iter.next().unwrap().0
        };
        let range_to_exclusive = match left_second {
            CacheKey::Normal(smallest_in_cache) => smallest_in_cache,
            CacheKey::Largest => hint_key, // no normal entry in the cache
            _ => unreachable!(),
        }
        .clone();

        let mut to_extend: VecDeque<OwnedRow> = VecDeque::with_capacity(MAGIC_BATCH_SIZE);
        {
            let pk_range = (
                Bound::Included(self.this_partition_key.into_owned_row()),
                Bound::Excluded(self.state_key_to_table_pk(&range_to_exclusive)?),
            );
            let streams: Vec<_> =
                futures::future::try_join_all(table.vnode_bitmap().iter_vnodes().map(|vnode| {
                    table.iter_key_and_val_with_pk_range(
                        &pk_range,
                        vnode,
                        PrefetchOptions::new_for_exhaust_iter(),
                    )
                }))
                .await?
                .into_iter()
                .map(Box::pin)
                .collect();

            #[for_await]
            for kv in merge_sort(streams) {
                let (_, row): (_, OwnedRow) = kv?;

                // For leftward extension, we now must iterate the table in order from the beginning
                // of this partition and fill only the last n rows to the cache.
                // TODO(rc): WE NEED STATE TABLE REVERSE ITERATOR!!
                if to_extend.len() == MAGIC_BATCH_SIZE {
                    to_extend.pop_front();
                }
                to_extend.push_back(row);
            }
        }

        let n_extended = to_extend.len();
        for row in to_extend {
            let key = self.row_to_state_key(&row)?;
            self.range_cache.insert(CacheKey::from(key), row);
        }
        if n_extended < MAGIC_BATCH_SIZE && self.cache_real_len() > 0 {
            // we reached the beginning of this partition in the table
            self.range_cache.remove(&CacheKey::Smallest);
        }

        Ok(())
    }

    async fn extend_cache_rightward_by_n(
        &mut self,
        table: &StateTable<S>,
        hint_key: &StateKey,
    ) -> StreamExecutorResult<()> {
        if self.cache_real_len() == self.range_cache.len() {
            // no sentinel in the cache, meaning we already cached all entries of this partition
            return Ok(());
        }
        assert!(self.range_cache.len() >= 2);

        let right_second = {
            let mut iter = self.range_cache.inner().iter();
            let right_first = iter.next_back().unwrap().0;
            if right_first.is_normal() {
                // the rightside already reaches the end of this partition in the table
                return Ok(());
            }
            iter.next_back().unwrap().0
        };
        let range_from_exclusive = match right_second {
            CacheKey::Normal(largest_in_cache) => largest_in_cache,
            CacheKey::Smallest => hint_key, // no normal entry in the cache
            _ => unreachable!(),
        }
        .clone();

        let mut n_extended = 0usize;
        {
            let pk_range = (
                Bound::Excluded(self.state_key_to_table_pk(&range_from_exclusive)?),
                // currently we can't get the first possible key after this partition, so use
                // `Unbounded` plus manual check for workaround
                Bound::<OwnedRow>::Unbounded,
            );
            let streams: Vec<_> =
                futures::future::try_join_all(table.vnode_bitmap().iter_vnodes().map(|vnode| {
                    table.iter_key_and_val_with_pk_range(
                        &pk_range,
                        vnode,
                        PrefetchOptions::default(),
                    )
                }))
                .await?
                .into_iter()
                .map(Box::pin)
                .collect();

            #[for_await]
            for kv in merge_sort(streams) {
                let (_, row): (_, OwnedRow) = kv?;

                if !Row::eq(
                    self.this_partition_key,
                    &(&row).project(self.partition_key_indices),
                ) {
                    // we've reached the end of this partition
                    break;
                }

                let key = self.row_to_state_key(&row)?;
                self.range_cache.insert(CacheKey::from(key), row);

                n_extended += 1;
                if n_extended == MAGIC_BATCH_SIZE {
                    break;
                }
            }
        }

        if n_extended < MAGIC_BATCH_SIZE && self.cache_real_len() > 0 {
            // we reached the end of this partition in the table
            self.range_cache.remove(&CacheKey::Largest);
        }

        Ok(())
    }

    fn state_key_to_table_pk(&self, key: &StateKey) -> StreamExecutorResult<OwnedRow> {
        Ok(self
            .this_partition_key
            .chain(memcmp_encoding::decode_row(
                &key.order_key,
                self.order_key_data_types,
                self.order_key_order_types,
            )?)
            .chain(key.pk.as_inner())
            .into_owned_row())
    }

    fn row_to_state_key(&self, full_row: impl Row + Copy) -> StreamExecutorResult<StateKey> {
        todo!()
    }

    pub fn shrink_cache_to_range(&mut self, range: RangeInclusive<&StateKey>) {
        todo!()
    }
}

/// Find all affected ranges in the given partition with delta.
///
/// # Returns
///
/// `Vec<(first_frame_start, first_curr_key, last_curr_key, last_frame_end_incl)>`
///
/// Each affected range is a union of many small window frames affected by some adajcent
/// keys in the delta.
///
/// Example:
/// - frame 1: `rows between 2 preceding and current row`
/// - frame 2: `rows between 1 preceding and 2 following`
/// - partition: `[1, 2, 4, 5, 7, 8, 9, 10, 11, 12, 14]`
/// - delta: `[3, 4, 15]`
/// - affected ranges: `[(1, 1, 7, 9), (10, 12, 15, 15)]`
///
/// TODO(rc):
/// Note that, since we assume input chunks have data locality on order key columns, we now only
/// calculate one single affected range. So the affected ranges in the above example will be
/// `(1, 1, 15, 15)`. Later we may optimize this.
fn find_affected_ranges<'cache>(
    calls: &'_ [WindowFuncCall],
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
) -> Vec<(
    &'cache CacheKey,
    &'cache CacheKey,
    &'cache CacheKey,
    &'cache CacheKey,
)> {
    let delta = part_with_delta.delta();

    if part_with_delta.first_key().is_none() {
        // all keys are deleted in the delta
        return vec![];
    }

    if part_with_delta.snapshot().is_empty() {
        // all existing keys are inserted in the delta
        return vec![(
            delta.first_key_value().unwrap().0,
            delta.first_key_value().unwrap().0,
            delta.last_key_value().unwrap().0,
            delta.last_key_value().unwrap().0,
        )];
    }

    let first_key = part_with_delta.first_key().unwrap();
    let last_key = part_with_delta.last_key().unwrap();

    let start_is_unbounded = calls
        .iter()
        .any(|call| call.frame.bounds.start_is_unbounded());
    let end_is_unbounded = calls
        .iter()
        .any(|call| call.frame.bounds.end_is_unbounded());

    let first_curr_key = if end_is_unbounded {
        // If the frame end is unbounded, the frame corresponding to the first key is always
        // affected.
        first_key
    } else {
        calls
            .iter()
            .map(|call| match &call.frame.bounds {
                FrameBounds::Rows(_start, end) => {
                    let mut cursor = part_with_delta
                        .lower_bound(Bound::Included(delta.first_key_value().unwrap().0));
                    for _ in 0..end.n_following_rows().unwrap() {
                        // Note that we have to move before check, to handle situation where the
                        // cursor is at ghost position at first.
                        cursor.move_prev();
                        if cursor.position().is_ghost() {
                            break;
                        }
                    }
                    cursor.key().unwrap_or(first_key)
                }
            })
            .min()
            .expect("# of window function calls > 0")
    };

    let first_frame_start = if start_is_unbounded {
        // If the frame start is unbounded, the first key always need to be included in the affected
        // range.
        first_key
    } else {
        calls
            .iter()
            .map(|call| match &call.frame.bounds {
                FrameBounds::Rows(start, _end) => {
                    let mut cursor = part_with_delta.find(&first_curr_key).unwrap();
                    for _ in 0..start.n_preceding_rows().unwrap() {
                        cursor.move_prev();
                        if cursor.position().is_ghost() {
                            break;
                        }
                    }
                    cursor.key().unwrap_or(first_key)
                }
            })
            .min()
            .expect("# of window function calls > 0")
    };

    let last_curr_key = if start_is_unbounded {
        last_key
    } else {
        calls
            .iter()
            .map(|call| match &call.frame.bounds {
                FrameBounds::Rows(start, _end) => {
                    let mut cursor = part_with_delta
                        .upper_bound(Bound::Included(delta.last_key_value().unwrap().0));
                    for _ in 0..start.n_preceding_rows().unwrap() {
                        cursor.move_next();
                        if cursor.position().is_ghost() {
                            break;
                        }
                    }
                    cursor.key().unwrap_or(last_key)
                }
            })
            .max()
            .expect("# of window function calls > 0")
    };

    let last_frame_end = if end_is_unbounded {
        last_key
    } else {
        calls
            .iter()
            .map(|call| match &call.frame.bounds {
                FrameBounds::Rows(_start, end) => {
                    let mut cursor = part_with_delta.find(&last_curr_key).unwrap();
                    for _ in 0..end.n_following_rows().unwrap() {
                        cursor.move_next();
                        if cursor.position().is_ghost() {
                            break;
                        }
                    }
                    cursor.key().unwrap_or(last_key)
                }
            })
            .max()
            .expect("# of window function calls > 0")
    };

    if first_curr_key > last_curr_key {
        // all affected keys are deleted in the delta
        return vec![];
    }

    vec![(
        first_frame_start,
        first_curr_key,
        last_curr_key,
        last_frame_end,
    )]
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::DataType;
    use risingwave_expr::agg::{AggArgs, AggKind};
    use risingwave_expr::window_function::{Frame, FrameBound, WindowFuncKind};

    use super::*;

    #[test]
    fn test_find_affected_ranges() {
        fn create_call(frame: Frame) -> WindowFuncCall {
            WindowFuncCall {
                kind: WindowFuncKind::Aggregate(AggKind::Sum),
                args: AggArgs::Unary(DataType::Int32, 0),
                return_type: DataType::Int32,
                frame,
            }
        }

        macro_rules! create_snapshot {
            ($( $pk:expr ),* $(,)?) => {
                {
                    #[allow(unused_mut)]
                    let mut snapshot = BTreeMap::new();
                    $(
                        snapshot.insert(
                            StateKey {
                                // order key doesn't matter here
                                order_key: vec![].into(),
                                pk: OwnedRow::new(vec![Some($pk.into())]).into(),
                            },
                            // value row doesn't matter here
                            OwnedRow::empty(),
                        );
                    )*
                    snapshot
                }
            };
        }

        macro_rules! create_change {
            (Delete) => {
                Change::Delete
            };
            (Insert) => {
                Change::Insert(OwnedRow::empty())
            };
        }

        macro_rules! create_delta {
            ($(( $pk:expr, $change:ident )),* $(,)?) => {
                {
                    #[allow(unused_mut)]
                    let mut delta = BTreeMap::new();
                    $(
                        delta.insert(
                            StateKey {
                                // order key doesn't matter here
                                order_key: vec![].into(),
                                pk: OwnedRow::new(vec![Some($pk.into())]).into(),
                            },
                            // value row doesn't matter here
                            create_change!( $change ),
                        );
                    )*
                    delta
                }
            };
        }

        {
            // test all empty
            let snapshot = create_snapshot!();
            let delta = create_delta!();
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);
            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(2),
                FrameBound::Preceding(1),
            ))];
            assert!(find_affected_ranges(&calls, &part_with_delta).is_empty());
        }

        {
            // test insert delta only
            let snapshot = create_snapshot!();
            let delta = create_delta!((1, Insert), (2, Insert), (3, Insert));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);
            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(2),
                FrameBound::Preceding(1),
            ))];
            let affected_ranges = find_affected_ranges(&calls, &part_with_delta);
            assert_eq!(affected_ranges.len(), 1);
            let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                affected_ranges.into_iter().next().unwrap();
            assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(1.into())]));
            assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(1.into())]));
            assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(3.into())]));
        }

        {
            // test simple
            let snapshot = create_snapshot!(1, 2, 3, 4, 5, 6);
            let delta = create_delta!((2, Insert), (3, Delete));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);

            {
                let calls = vec![create_call(Frame::rows(
                    FrameBound::Preceding(2),
                    FrameBound::Preceding(1),
                ))];
                let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                    find_affected_ranges(&calls, &part_with_delta)
                        .into_iter()
                        .next()
                        .unwrap();
                assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(1.into())]));
                assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(2.into())]));
                assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(5.into())]));
                assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(5.into())]));
            }

            {
                let calls = vec![create_call(Frame::rows(
                    FrameBound::Preceding(1),
                    FrameBound::Following(2),
                ))];
                let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                    find_affected_ranges(&calls, &part_with_delta)
                        .into_iter()
                        .next()
                        .unwrap();
                assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(1.into())]));
                assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(1.into())]));
                assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(4.into())]));
                assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(6.into())]));
            }

            {
                let calls = vec![create_call(Frame::rows(
                    FrameBound::CurrentRow,
                    FrameBound::Following(2),
                ))];
                let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                    find_affected_ranges(&calls, &part_with_delta)
                        .into_iter()
                        .next()
                        .unwrap();
                assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(1.into())]));
                assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(1.into())]));
                assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(2.into())]));
                assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(5.into())]));
            }
        }

        {
            // test multiple calls
            let snapshot = create_snapshot!(1, 2, 3, 4, 5, 6);
            let delta = create_delta!((2, Insert), (3, Delete));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);

            let calls = vec![
                create_call(Frame::rows(
                    FrameBound::Preceding(1),
                    FrameBound::Preceding(1),
                )),
                create_call(Frame::rows(
                    FrameBound::Following(1),
                    FrameBound::Following(1),
                )),
            ];
            let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                find_affected_ranges(&calls, &part_with_delta)
                    .into_iter()
                    .next()
                    .unwrap();
            assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(1.into())]));
            assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(1.into())]));
            assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(4.into())]));
            assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(5.into())]));
        }

        {
            // test lag corner case
            let snapshot = create_snapshot!(1, 2, 3, 4, 5, 6);
            let delta = create_delta!((1, Delete), (2, Delete), (3, Delete));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);

            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(1),
                FrameBound::Preceding(1),
            ))];
            let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                find_affected_ranges(&calls, &part_with_delta)
                    .into_iter()
                    .next()
                    .unwrap();
            assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(4.into())]));
            assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(4.into())]));
            assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(4.into())]));
            assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(4.into())]));
        }

        {
            // test lead corner case
            let snapshot = create_snapshot!(1, 2, 3, 4, 5, 6);
            let delta = create_delta!((4, Delete), (5, Delete), (6, Delete));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);

            let calls = vec![create_call(Frame::rows(
                FrameBound::Following(1),
                FrameBound::Following(1),
            ))];
            let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                find_affected_ranges(&calls, &part_with_delta)
                    .into_iter()
                    .next()
                    .unwrap();
            assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(3.into())]));
        }

        {
            // test lag/lead(x, 0) corner case
            let snapshot = create_snapshot!(1, 2, 3, 4);
            let delta = create_delta!((2, Delete), (3, Delete));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);

            let calls = vec![create_call(Frame::rows(
                FrameBound::CurrentRow,
                FrameBound::CurrentRow,
            ))];
            assert!(find_affected_ranges(&calls, &part_with_delta).is_empty());
        }

        {
            // test lag/lead(x, 0) corner case 2
            let snapshot = create_snapshot!(1, 2, 3, 4, 5);
            let delta = create_delta!((2, Delete), (3, Insert), (4, Delete));
            let part_with_delta = DeltaBTreeMap::new(&snapshot, &delta);

            let calls = vec![create_call(Frame::rows(
                FrameBound::CurrentRow,
                FrameBound::CurrentRow,
            ))];
            let (first_frame_start, first_curr_key, last_curr_key, last_frame_end) =
                find_affected_ranges(&calls, &part_with_delta)
                    .into_iter()
                    .next()
                    .unwrap();
            assert_eq!(first_frame_start.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(first_curr_key.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(last_curr_key.pk.0, OwnedRow::new(vec![Some(3.into())]));
            assert_eq!(last_frame_end.pk.0, OwnedRow::new(vec![Some(3.into())]));
        }
    }
}
