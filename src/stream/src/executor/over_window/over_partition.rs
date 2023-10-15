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

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::marker::PhantomData;
use std::ops::{Bound, RangeInclusive};

use futures::stream::select_all;
use futures::{stream, StreamExt, TryStreamExt};
use futures_async_stream::for_await;
use risingwave_common::array::stream_record::Record;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::session_config::OverWindowCachePolicy as CachePolicy;
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

const MAGIC_CACHE_SIZE: usize = 1024;
const MAGIC_JITTER_PREVENTION: usize = MAGIC_CACHE_SIZE / 8;

pub(super) fn shrink_partition_cache(
    this_partition_key: &OwnedRow,
    range_cache: &mut PartitionCache,
    cache_policy: CachePolicy,
    recently_accessed_range: RangeInclusive<StateKey>,
) {
    tracing::trace!(
        this_partition_key=?this_partition_key,
        cache_policy=?cache_policy,
        recently_accessed_range=?recently_accessed_range,
        "find the range to retain in the range cache"
    );

    let (start, end) = match cache_policy {
        CachePolicy::Full => {
            // evict nothing if the policy is to cache full partition
            return;
        }
        CachePolicy::Recent => {
            let (sk_start, sk_end) = recently_accessed_range.into_inner();
            let (ck_start, ck_end) = (CacheKey::from(sk_start), CacheKey::from(sk_end));

            let mut cursor = range_cache.inner().upper_bound(Bound::Excluded(&ck_start));
            for _ in 0..MAGIC_JITTER_PREVENTION {
                if cursor.key().is_none() {
                    break;
                }
                cursor.move_prev();
            }
            let start = cursor
                .key()
                .unwrap_or_else(|| range_cache.first_key_value().unwrap().0)
                .clone();

            let mut cursor = range_cache.inner().lower_bound(Bound::Excluded(&ck_end));
            for _ in 0..MAGIC_JITTER_PREVENTION {
                if cursor.key().is_none() {
                    break;
                }
                cursor.move_next();
            }
            let end = cursor
                .key()
                .unwrap_or_else(|| range_cache.last_key_value().unwrap().0)
                .clone();

            (start, end)
        }
        CachePolicy::RecentFirstN => {
            if range_cache.len() <= MAGIC_CACHE_SIZE {
                // no need to evict if cache len <= N
                return;
            } else {
                let (sk_start, _sk_end) = recently_accessed_range.into_inner();
                let ck_start = CacheKey::from(sk_start);

                let mut capacity_remain = MAGIC_CACHE_SIZE; // precision is not important here, code simplicity is first

                let mut cursor = range_cache.inner().upper_bound(Bound::Excluded(&ck_start));
                // go back for at most `MAGIC_JITTER_PREVENTION` entries
                for _ in 0..MAGIC_JITTER_PREVENTION {
                    if cursor.key().is_none() {
                        break;
                    }
                    cursor.move_prev();
                    capacity_remain -= 1;
                }
                let start = cursor
                    .key()
                    .unwrap_or_else(|| range_cache.first_key_value().unwrap().0)
                    .clone();

                let mut cursor = range_cache.inner().lower_bound(Bound::Included(&ck_start));
                // go forward for at most `capacity_remain` entries
                for _ in 0..capacity_remain {
                    if cursor.key().is_none() {
                        break;
                    }
                    cursor.move_next();
                }
                let end = cursor
                    .key()
                    .unwrap_or_else(|| range_cache.last_key_value().unwrap().0)
                    .clone();

                (start, end)
            }
        }
        CachePolicy::RecentLastN => {
            if range_cache.len() <= MAGIC_CACHE_SIZE {
                // no need to evict if cache len <= N
                return;
            } else {
                let (_sk_start, sk_end) = recently_accessed_range.into_inner();
                let ck_end = CacheKey::from(sk_end);

                let mut capacity_remain = MAGIC_CACHE_SIZE; // precision is not important here, code simplicity is first

                let mut cursor = range_cache.inner().lower_bound(Bound::Excluded(&ck_end));
                // go forward for at most `MAGIC_JITTER_PREVENTION` entries
                for _ in 0..MAGIC_JITTER_PREVENTION {
                    if cursor.key().is_none() {
                        break;
                    }
                    cursor.move_next();
                    capacity_remain -= 1;
                }
                let end = cursor
                    .key()
                    .unwrap_or_else(|| range_cache.last_key_value().unwrap().0)
                    .clone();

                let mut cursor = range_cache.inner().upper_bound(Bound::Included(&ck_end));
                // go back for at most `capacity_remain` entries
                for _ in 0..capacity_remain {
                    if cursor.key().is_none() {
                        break;
                    }
                    cursor.move_prev();
                }
                let start = cursor
                    .key()
                    .unwrap_or_else(|| range_cache.first_key_value().unwrap().0)
                    .clone();

                (start, end)
            }
        }
    };

    tracing::trace!(
        this_partition_key=?this_partition_key,
        retain_range=?(&start..=&end),
        "retain range in the range cache"
    );

    let (left_removed, right_removed) = range_cache.retain_range(&start..=&end);
    if range_cache.is_empty() {
        if !left_removed.is_empty() || !right_removed.is_empty() {
            range_cache.insert(CacheKey::Smallest, OwnedRow::empty());
            range_cache.insert(CacheKey::Largest, OwnedRow::empty());
        }
    } else {
        if !left_removed.is_empty() {
            range_cache.insert(CacheKey::Smallest, OwnedRow::empty());
        }
        if !right_removed.is_empty() {
            range_cache.insert(CacheKey::Largest, OwnedRow::empty());
        }
    }
}

/// A wrapper of [`PartitionCache`] that provides helper methods to manipulate the cache.
/// By putting this type inside `private` module, we can avoid misuse of the internal fields and
/// methods.
pub(super) struct OverPartition<'a, S: StateStore> {
    this_partition_key: &'a OwnedRow,
    range_cache: &'a mut PartitionCache,
    cache_policy: CachePolicy,

    calls: &'a [WindowFuncCall],
    partition_key_indices: &'a [usize],
    order_key_data_types: &'a [DataType],
    order_key_order_types: &'a [OrderType],
    order_key_indices: &'a [usize],
    input_pk_indices: &'a [usize],
    state_key_to_table_pk_proj: Vec<usize>,
    _phantom: PhantomData<S>,
}

const MAGIC_BATCH_SIZE: usize = 512;

impl<'a, S: StateStore> OverPartition<'a, S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        this_partition_key: &'a OwnedRow,
        cache: &'a mut PartitionCache,
        cache_policy: CachePolicy,
        calls: &'a [WindowFuncCall],
        partition_key_indices: &'a [usize],
        order_key_data_types: &'a [DataType],
        order_key_order_types: &'a [OrderType],
        order_key_indices: &'a [usize],
        input_pk_indices: &'a [usize],
    ) -> Self {
        // TODO(rc): move the calculation to executor?
        let mut projection = Vec::with_capacity(
            partition_key_indices.len() + order_key_indices.len() + input_pk_indices.len(),
        );
        let mut col_dedup = HashSet::new();
        for (proj_idx, key_idx) in partition_key_indices
            .iter()
            .chain(order_key_indices.iter())
            .chain(input_pk_indices.iter())
            .enumerate()
        {
            if col_dedup.insert(*key_idx) {
                projection.push(proj_idx);
            }
        }
        projection.shrink_to_fit();

        Self {
            this_partition_key,
            range_cache: cache,
            cache_policy,

            calls,
            partition_key_indices,
            order_key_data_types,
            order_key_order_types,
            order_key_indices,
            input_pk_indices,
            state_key_to_table_pk_proj: projection,
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

        if self.cache_policy.is_full() {
            // ensure everything is in the cache
            self.extend_cache_to_boundary(table).await?;
        } else {
            // ensure the cache covers all delta (if possible)
            self.extend_cache_by_range(table, delta_first..=delta_last)
                .await?;
        }

        loop {
            // Terminateability: `extend_cache_leftward_by_n` and `extend_cache_rightward_by_n` keep
            // pushing the cache to the boundary of current partition. In these two methods, when
            // any side of boundary is reached, the sentinel key will be removed, so finally
            // `self::find_affected_ranges` will return ranges without any sentinels.

            let (left_reached_sentinel, right_reached_sentinel) = {
                // SAFETY: Here we shortly borrow the range cache and turn the reference into a
                // `'cache` one to bypass the borrow checker. This is safe because we only return
                // the reference once we don't need to do any further mutation.
                let cache_inner = unsafe { &*(self.range_cache.inner() as *const _) };
                let ranges =
                    self::find_affected_ranges(self.calls, DeltaBTreeMap::new(cache_inner, delta));

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
                tracing::trace!(partition=?self.this_partition_key, "partition cache left extension triggered");
                let left_most = self.cache_real_first_key().unwrap_or(delta_first).clone();
                self.extend_cache_leftward_by_n(table, &left_most).await?;
            }
            if right_reached_sentinel {
                tracing::trace!(partition=?self.this_partition_key, "partition cache right extension triggered");
                let right_most = self.cache_real_last_key().unwrap_or(delta_last).clone();
                self.extend_cache_rightward_by_n(table, &right_most).await?;
            }
            tracing::trace!(partition=?self.this_partition_key, "partition cache extended");
        }
    }

    async fn extend_cache_to_boundary(
        &mut self,
        table: &StateTable<S>,
    ) -> StreamExecutorResult<()> {
        if self.cache_real_len() == self.range_cache.len() {
            // no sentinel in the cache, meaning we already cached all entries of this partition
            return Ok(());
        }

        tracing::trace!(partition=?self.this_partition_key, "loading the whole partition into cache");

        let mut new_cache = PartitionCache::new(); // shouldn't use `new_empty_partition_cache` here because we don't want sentinels
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
        let table_iter = table
            .iter_with_prefix(
                self.this_partition_key,
                sub_range,
                PrefetchOptions::new_for_exhaust_iter(),
            )
            .await?;

        #[for_await]
        for row in table_iter {
            let row: OwnedRow = row?.into_owned_row();
            new_cache.insert(self.row_to_state_key(&row)?.into(), row);
        }
        *self.range_cache = new_cache;

        Ok(())
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
            tracing::debug!(
                partition=?self.this_partition_key,
                cache_first=?cache_real_first_key,
                cache_last=?cache_real_last_key,
                range=?range,
                "modified range is completely non-overlapping with the cached range, re-initializing the cache"
            );
            *self.range_cache = new_empty_partition_cache();
        }

        if self.cache_real_len() == 0 {
            // no normal entry in the cache, just load the given range
            let table_pk_range = (
                Bound::Included(self.state_key_to_table_pk(range.start())?),
                Bound::Included(self.state_key_to_table_pk(range.end())?),
            );
            tracing::debug!(
                partition=?self.this_partition_key,
                table_pk_range=?table_pk_range,
                "cache is empty, just loading the given range"
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
            tracing::trace!(
                partition=?self.this_partition_key,
                table_pk_range=?table_pk_range,
                "loading the left half of given range"
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
            tracing::trace!(
                partition=?self.this_partition_key,
                table_pk_range=?table_pk_range,
                "loading the right half of given range"
            );
            return self
                .extend_cache_by_range_inner(table, table_pk_range)
                .await;
        }

        // TODO(rc): Uncomment the following to enable prefetching rows before the start of the
        // range once we have STATE TABLE REVERSE ITERATOR.
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
                table.iter_with_vnode(
                    vnode,
                    &table_pk_range,
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
            let row: OwnedRow = row?.into_owned_row();
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

        self.extend_cache_leftward_by_n_inner(table, &range_to_exclusive)
            .await?;

        if self.cache_real_len() == 0 {
            // Cache was empty, and extending leftward didn't add anything to the cache, but we
            // can't just remove the smallest sentinel, we must also try extending rightward.
            self.extend_cache_rightward_by_n_inner(table, hint_key)
                .await?;
            if self.cache_real_len() == 0 {
                // still empty, meaning the table is empty
                self.range_cache.remove(&CacheKey::Smallest);
                self.range_cache.remove(&CacheKey::Largest);
            }
        }

        Ok(())
    }

    async fn extend_cache_leftward_by_n_inner(
        &mut self,
        table: &StateTable<S>,
        range_to_exclusive: &StateKey,
    ) -> StreamExecutorResult<()> {
        let mut to_extend: VecDeque<OwnedRow> = VecDeque::with_capacity(MAGIC_BATCH_SIZE);
        {
            let pk_range = (
                Bound::Included(self.this_partition_key.into_owned_row()),
                Bound::Excluded(self.state_key_to_table_pk(range_to_exclusive)?),
            );
            let streams: Vec<_> =
                futures::future::try_join_all(table.vnode_bitmap().iter_vnodes().map(|vnode| {
                    table.iter_with_vnode(vnode, &pk_range, PrefetchOptions::new_for_exhaust_iter())
                }))
                .await?
                .into_iter()
                .map(Box::pin)
                .collect();

            #[for_await]
            for row in merge_sort(streams) {
                let row: OwnedRow = row?.into_owned_row();

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

        self.extend_cache_rightward_by_n_inner(table, &range_from_exclusive)
            .await?;

        if self.cache_real_len() == 0 {
            // Cache was empty, and extending rightward didn't add anything to the cache, but we
            // can't just remove the smallest sentinel, we must also try extending leftward.
            self.extend_cache_leftward_by_n_inner(table, hint_key)
                .await?;
            if self.cache_real_len() == 0 {
                // still empty, meaning the table is empty
                self.range_cache.remove(&CacheKey::Smallest);
                self.range_cache.remove(&CacheKey::Largest);
            }
        }

        Ok(())
    }

    async fn extend_cache_rightward_by_n_inner(
        &mut self,
        table: &StateTable<S>,
        range_from_exclusive: &StateKey,
    ) -> StreamExecutorResult<()> {
        let mut n_extended = 0usize;
        {
            let pk_range = (
                Bound::Excluded(self.state_key_to_table_pk(range_from_exclusive)?),
                // currently we can't get the first possible key after this partition, so use
                // `Unbounded` plus manual check for workaround
                Bound::<OwnedRow>::Unbounded,
            );
            let streams: Vec<_> =
                futures::future::try_join_all(table.vnode_bitmap().iter_vnodes().map(|vnode| {
                    table.iter_with_vnode(vnode, &pk_range, PrefetchOptions::default())
                }))
                .await?
                .into_iter()
                .map(Box::pin)
                .collect();

            #[for_await]
            for row in merge_sort(streams) {
                let row: OwnedRow = row?.into_owned_row();

                if !Row::eq(
                    self.this_partition_key,
                    (&row).project(self.partition_key_indices),
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
            .project(&self.state_key_to_table_pk_proj)
            .into_owned_row())
    }

    fn row_to_state_key(&self, full_row: impl Row + Copy) -> StreamExecutorResult<StateKey> {
        Ok(StateKey {
            order_key: memcmp_encoding::encode_row(
                full_row.project(self.order_key_indices),
                self.order_key_order_types,
            )?,
            pk: full_row
                .project(self.input_pk_indices)
                .into_owned_row()
                .into(),
        })
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
    // XXX(rc): NOTE FOR DEVS
    // Must carefully consider the sentinel keys in the cache when extending this function to
    // support `RANGE` and `GROUPS` frames later. May introduce a return value variant to clearly
    // tell the caller that there exists at least one affected range that touches the sentinel.

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
                    let mut cursor = part_with_delta.find(first_curr_key).unwrap();
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
                    let mut cursor = part_with_delta.find(last_curr_key).unwrap();
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
mod find_affected_ranges_tests {
    //! Function `find_affected_ranges` is important enough to deserve its own test module. We must
    //! test it thoroughly.

    use itertools::Itertools;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_expr::aggregate::{AggArgs, AggKind};
    use risingwave_expr::window_function::{Frame, FrameBound, WindowFuncKind};

    use super::*;

    fn create_call(frame: Frame) -> WindowFuncCall {
        WindowFuncCall {
            kind: WindowFuncKind::Aggregate(AggKind::Sum),
            args: AggArgs::Unary(DataType::Int32, 0),
            return_type: DataType::Int32,
            frame,
        }
    }

    macro_rules! create_cache {
        (..., $( $pk:literal ),* , ...) => {
            {
                let mut cache = create_cache!( $( $pk ),* );
                cache.insert(CacheKey::Smallest, OwnedRow::empty().into());
                cache.insert(CacheKey::Largest, OwnedRow::empty().into());
                cache
            }
        };
        (..., $( $pk:literal ),*) => {
            {
                let mut cache = create_cache!( $( $pk ),* );
                cache.insert(CacheKey::Smallest, OwnedRow::empty().into());
                cache
            }
        };
        ($( $pk:literal ),* , ...) => {
            {
                let mut cache = create_cache!( $( $pk ),* );
                cache.insert(CacheKey::Largest, OwnedRow::empty().into());
                cache
            }
        };
        ($( $pk:literal ),*) => {
            {
                #[allow(unused_mut)]
                let mut cache = BTreeMap::new();
                $(
                    cache.insert(
                        CacheKey::Normal(
                            StateKey {
                                // order key doesn't matter here
                                order_key: vec![].into(),
                                pk: OwnedRow::new(vec![Some($pk.into())]).into(),
                            },
                        ),
                        // value row doesn't matter here
                        OwnedRow::empty(),
                    );
                )*
                cache
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
        ($(( $pk:literal, $change:ident )),* $(,)?) => {
            {
                #[allow(unused_mut)]
                let mut delta = BTreeMap::new();
                $(
                    delta.insert(
                        CacheKey::Normal(
                            StateKey {
                                // order key doesn't matter here
                                order_key: vec![].into(),
                                pk: OwnedRow::new(vec![Some($pk.into())]).into(),
                            },
                        ),
                        // value row doesn't matter here
                        create_change!( $change ),
                    );
                )*
                delta
            }
        };
    }

    fn assert_ranges_eq(
        result: Vec<(&CacheKey, &CacheKey, &CacheKey, &CacheKey)>,
        expected: impl IntoIterator<Item = (ScalarImpl, ScalarImpl, ScalarImpl, ScalarImpl)>,
    ) {
        result
            .into_iter()
            .zip_eq(expected)
            .for_each(|(result, expected)| {
                assert_eq!(
                    result.0.as_normal_expect().pk.0,
                    OwnedRow::new(vec![Some(expected.0)])
                );
                assert_eq!(
                    result.1.as_normal_expect().pk.0,
                    OwnedRow::new(vec![Some(expected.1)])
                );
                assert_eq!(
                    result.2.as_normal_expect().pk.0,
                    OwnedRow::new(vec![Some(expected.2)])
                );
                assert_eq!(
                    result.3.as_normal_expect().pk.0,
                    OwnedRow::new(vec![Some(expected.3)])
                );
            })
    }

    #[test]
    fn test_all_empty() {
        let cache = create_cache!();
        let delta = create_delta!();
        let calls = vec![create_call(Frame::rows(
            FrameBound::Preceding(2),
            FrameBound::Preceding(1),
        ))];
        assert_ranges_eq(
            find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta)),
            [],
        );
    }

    #[test]
    fn test_insert_delta_only() {
        let cache = create_cache!();
        let delta = create_delta!((1, Insert), (2, Insert), (3, Insert));
        let calls = vec![create_call(Frame::rows(
            FrameBound::Preceding(2),
            FrameBound::Preceding(1),
        ))];
        let affected_ranges = find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta));
        assert_ranges_eq(affected_ranges, [(1.into(), 1.into(), 3.into(), 3.into())]);
    }

    #[test]
    fn test_simple() {
        let cache = create_cache!(1, 2, 3, 4, 5, 6);
        let delta = create_delta!((2, Insert), (3, Delete));

        {
            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(2),
                FrameBound::Preceding(1),
            ))];
            assert_ranges_eq(
                find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta)),
                [(1.into(), 2.into(), 5.into(), 5.into())],
            );
        }

        {
            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(1),
                FrameBound::Following(2),
            ))];
            assert_ranges_eq(
                find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta)),
                [(1.into(), 1.into(), 4.into(), 6.into())],
            );
        }

        {
            let calls = vec![create_call(Frame::rows(
                FrameBound::CurrentRow,
                FrameBound::Following(2),
            ))];
            assert_ranges_eq(
                find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta)),
                [(1.into(), 1.into(), 2.into(), 5.into())],
            );
        }
    }

    #[test]
    fn test_multiple_calls() {
        let cache = create_cache!(1, 2, 3, 4, 5, 6);
        let delta = create_delta!((2, Insert), (3, Delete));
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
        assert_ranges_eq(
            find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta)),
            [(1.into(), 1.into(), 4.into(), 5.into())],
        );
    }

    #[test]
    fn test_lag_corner_case() {
        let cache = create_cache!(1, 2, 3, 4, 5, 6);
        let delta = create_delta!((1, Delete), (2, Delete), (3, Delete));
        let calls = vec![create_call(Frame::rows(
            FrameBound::Preceding(1),
            FrameBound::Preceding(1),
        ))];
        assert_ranges_eq(
            find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta)),
            [(4.into(), 4.into(), 4.into(), 4.into())],
        );
    }

    #[test]
    fn test_lead_corner_case() {
        let cache = create_cache!(1, 2, 3, 4, 5, 6);
        let delta = create_delta!((4, Delete), (5, Delete), (6, Delete));
        let calls = vec![create_call(Frame::rows(
            FrameBound::Following(1),
            FrameBound::Following(1),
        ))];
        assert_ranges_eq(
            find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta)),
            [(3.into(), 3.into(), 3.into(), 3.into())],
        );
    }

    #[test]
    fn test_lag_lead_offset_0_corner_case_1() {
        let cache = create_cache!(1, 2, 3, 4);
        let delta = create_delta!((2, Delete), (3, Delete));
        let calls = vec![create_call(Frame::rows(
            FrameBound::CurrentRow,
            FrameBound::CurrentRow,
        ))];
        assert_ranges_eq(
            find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta)),
            [],
        );
    }

    #[test]
    fn test_lag_lead_offset_0_corner_case_2() {
        let cache = create_cache!(1, 2, 3, 4, 5);
        let delta = create_delta!((2, Delete), (3, Insert), (4, Delete));
        let calls = vec![create_call(Frame::rows(
            FrameBound::CurrentRow,
            FrameBound::CurrentRow,
        ))];
        assert_ranges_eq(
            find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta)),
            [(3.into(), 3.into(), 3.into(), 3.into())],
        );
    }

    #[test]
    fn test_empty_with_sentinels() {
        let cache: BTreeMap<KeyWithSentinel<StateKey>, OwnedRow> = create_cache!(..., , ...);
        let delta = create_delta!((1, Insert), (2, Insert));

        {
            let calls = vec![create_call(Frame::rows(
                FrameBound::CurrentRow,
                FrameBound::CurrentRow,
            ))];
            assert_ranges_eq(
                find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta)),
                [(1.into(), 1.into(), 2.into(), 2.into())],
            );
        }

        {
            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(1),
                FrameBound::Preceding(1),
            ))];
            let range = find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta))[0];
            assert!(range.0.is_smallest());
            assert_eq!(
                range.1.as_normal_expect().pk.0,
                OwnedRow::new(vec![Some(1.into())])
            );
            assert!(range.2.is_largest());
            assert!(range.3.is_largest());
        }

        {
            let calls = vec![create_call(Frame::rows(
                FrameBound::Following(1),
                FrameBound::Following(3),
            ))];
            let range = find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta))[0];
            assert!(range.0.is_smallest());
            assert!(range.1.is_smallest());
            assert_eq!(
                range.2.as_normal_expect().pk.0,
                OwnedRow::new(vec![Some(2.into())])
            );
            assert!(range.3.is_largest());
        }
    }

    #[test]
    fn test_with_left_sentinel() {
        let cache = create_cache!(..., 2, 4, 5, 8);
        let delta = create_delta!((3, Insert), (4, Insert), (8, Delete));

        {
            let calls = vec![create_call(Frame::rows(
                FrameBound::Following(1),
                FrameBound::Following(1),
            ))];
            assert_ranges_eq(
                find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta)),
                [(2.into(), 2.into(), 5.into(), 5.into())],
            );
        }

        {
            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(1),
                FrameBound::Following(1),
            ))];
            let range = find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta))[0];
            assert!(range.0.is_smallest());
            assert_eq!(
                range.1.as_normal_expect().pk.0,
                OwnedRow::new(vec![Some(2.into())])
            );
            assert_eq!(
                range.2.as_normal_expect().pk.0,
                OwnedRow::new(vec![Some(5.into())])
            );
            assert_eq!(
                range.3.as_normal_expect().pk.0,
                OwnedRow::new(vec![Some(5.into())])
            );
        }
    }

    #[test]
    fn test_with_right_sentinel() {
        let cache = create_cache!(1, 2, 4, 5, 8, ...);
        let delta = create_delta!((3, Insert), (4, Insert), (5, Delete));

        {
            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(1),
                FrameBound::Preceding(1),
            ))];
            assert_ranges_eq(
                find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta)),
                [(2.into(), 3.into(), 8.into(), 8.into())],
            );
        }

        {
            let calls = vec![create_call(Frame::rows(
                FrameBound::Preceding(1),
                FrameBound::Following(1),
            ))];
            let range = find_affected_ranges(&calls, DeltaBTreeMap::new(&cache, &delta))[0];
            assert_eq!(
                range.0.as_normal_expect().pk.0,
                OwnedRow::new(vec![Some(1.into())])
            );
            assert_eq!(
                range.1.as_normal_expect().pk.0,
                OwnedRow::new(vec![Some(2.into())])
            );
            assert_eq!(
                range.2.as_normal_expect().pk.0,
                OwnedRow::new(vec![Some(8.into())])
            );
            assert!(range.3.is_largest());
        }
    }
}
