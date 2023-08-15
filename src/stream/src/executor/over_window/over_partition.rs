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

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::ops::{Bound, RangeInclusive};

use futures::StreamExt;
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
    range_cache: &'a mut PartitionCache,
    order_key_data_types: &'a [DataType],
    order_key_order_types: &'a [OrderType],
    _phantom: PhantomData<S>,
}

const MAGIC_BATCH_SIZE: usize = 512;

impl<'a, S: StateStore> OverPartition<'a, S> {
    pub fn new(
        this_partition_key: &'a OwnedRow,
        cache: &'a mut PartitionCache,
        order_key_data_types: &'a [DataType],
        order_key_order_types: &'a [OrderType],
    ) -> Self {
        Self {
            this_partition_key,
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
        // ensure the cache covers all delta (if possible)
        let delta_first = delta.first_key_value().unwrap().0.as_normal_expect();
        let delta_last = delta.last_key_value().unwrap().0.as_normal_expect();
        self.extend_cache_by_range(table, delta_first..=delta_last)
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
                self.extend_cache_leftward_by_n(table).await?;
            }
            if right_reached_sentinel {
                tracing::info!("partition cache right extension triggered");
                self.extend_cache_rightward_by_n(table).await?;
            }
            tracing::info!("partition cache extended");
        }
    }

    async fn extend_cache_by_range(
        &mut self,
        table: &StateTable<S>,
        range: RangeInclusive<&StateKey>,
    ) -> StreamExecutorResult<()> {
        // TODO(): load the range of entries and some more into the cache, ensuring there's real
        // entries in cache

        // TODO(): assert must have at least one entry, or, no entry at all

        // TODO(rc): prefetch something before the start of the range;
        self.extend_cache_leftward_by_n(table).await?;

        // prefetch something after the end of the range
        self.extend_cache_rightward_by_n(table).await
    }

    async fn extend_cache_leftward_by_n(
        &mut self,
        table: &StateTable<S>,
    ) -> StreamExecutorResult<()> {
        if self.range_cache.inner().len() <= 1 {
            // empty or only one entry in the table (no sentinel)
            return Ok(());
        }

        let (left_first, left_second) = {
            let mut iter = self.range_cache.inner().iter();
            let first = iter.next().unwrap().0;
            let second = iter.next().unwrap().0;
            (first, second)
        };

        match (left_first, left_second) {
            (CacheKey::Normal(_), _) => {
                // no sentinel here, meaning we've already cached all of the 2 entries
                Ok(())
            }
            (CacheKey::Smallest, CacheKey::Normal(key)) => {
                // For leftward extension, we now must iterate the table in order from the
                // beginning of this partition and fill only the last n rows
                // to the cache. This is called `rotate`. TODO(rc): WE NEED
                // STATE TABLE REVERSE ITERATOR!!
                let rotate = true;

                let key = key.clone();
                self.extend_cache_take_n(
                    table,
                    Bound::Unbounded,
                    Bound::Excluded(&key),
                    MAGIC_BATCH_SIZE,
                    rotate,
                )
                .await
            }
            (CacheKey::Smallest, CacheKey::Largest) => {
                // TODO()
                panic!("must call `extend_cache_from_table_with_range` before");
            }
            _ => {
                unreachable!();
            }
        }
    }

    async fn extend_cache_rightward_by_n(
        &mut self,
        table: &StateTable<S>,
    ) -> StreamExecutorResult<()> {
        if self.range_cache.inner().len() <= 1 {
            // empty or only one entry in the table (no sentinel)
            return Ok(());
        }

        // note the order of keys here
        let (right_second, right_first) = {
            let mut iter = self.range_cache.inner().iter();
            let first = iter.next_back().unwrap().0;
            let second = iter.next_back().unwrap().0;
            (second, first)
        };

        match (right_second, right_first) {
            (_, CacheKey::Normal(_)) => {
                // no sentinel here, meaning we've already cached all of the 2 entries
                Ok(())
            }
            (CacheKey::Normal(key), CacheKey::Largest) => {
                let key = key.clone();
                self.extend_cache_take_n(
                    table,
                    Bound::Excluded(&key),
                    Bound::Unbounded,
                    MAGIC_BATCH_SIZE,
                    false,
                )
                .await
            }
            (CacheKey::Smallest, CacheKey::Largest) => {
                // TODO()
                panic!("must call `extend_cache_from_table_with_range` before");
            }
            _ => {
                unreachable!();
            }
        }
    }

    async fn extend_cache_take_n(
        &mut self,
        table: &StateTable<S>,
        start: Bound<&StateKey>,
        end: Bound<&StateKey>,
        n: usize,
        rotate: bool,
    ) -> StreamExecutorResult<()> {
        debug_assert!(
            table.value_indices().is_none(),
            "we must have full row as value here"
        );

        let mut to_extend = Vec::with_capacity(n.min(MAGIC_BATCH_SIZE));

        {
            let range = (self.convert_bound(start)?, self.convert_bound(end)?);
            let streams: Vec<_> =
                futures::future::try_join_all(table.vnode_bitmap().iter_vnodes().map(|vnode| {
                    table.iter_key_and_val_with_pk_range(
                        &range,
                        vnode,
                        PrefetchOptions {
                            exhaust_iter: n == usize::MAX || rotate,
                        },
                    )
                }))
                .await?
                .into_iter()
                .map(Box::pin)
                .collect();

            #[for_await]
            for kv in merge_sort(streams).take(n) {
                let (_, row): (_, OwnedRow) = kv?;
                // TODO(): fill the cache
            }
        }

        for (key, row) in to_extend {
            self.range_cache.insert(key, row);
        }

        // TODO(): handle the sentinel

        Ok(())
    }

    fn convert_bound<'s, 'k>(
        &'s self,
        bound: Bound<&'k StateKey>,
    ) -> StreamExecutorResult<Bound<impl Row + 'k>>
    where
        's: 'k,
    {
        Ok(match bound {
            Bound::Included(key) => Bound::Included(self.state_key_to_table_pk(key)?),
            Bound::Excluded(key) => Bound::Excluded(self.state_key_to_table_pk(key)?),
            Bound::Unbounded => Bound::Unbounded,
        })
    }

    fn state_key_to_table_pk<'s, 'k>(
        &'s self,
        key: &'k StateKey,
    ) -> StreamExecutorResult<impl Row + 'k>
    where
        's: 'k,
    {
        Ok(self
            .this_partition_key
            .chain(memcmp_encoding::decode_row(
                &key.order_key,
                self.order_key_data_types,
                self.order_key_order_types,
            )?)
            .chain(key.pk.as_inner()))
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
