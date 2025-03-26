// Copyright 2024 RisingWave Labs
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

use delta_btree_map::{Change, DeltaBTreeMap};
use educe::Educe;
use futures_async_stream::for_await;
use risingwave_common::array::stream_record::Record;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::session_config::OverWindowCachePolicy as CachePolicy;
use risingwave_common::types::{Datum, Sentinelled};
use risingwave_common_estimate_size::collections::EstimatedBTreeMap;
use risingwave_expr::window_function::{
    RangeFrameBounds, RowsFrameBounds, StateKey, WindowFuncCall,
};
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::StateStore;
use static_assertions::const_assert;

use super::general::RowConverter;
use crate::common::table::state_table::StateTable;
use crate::consistency::{consistency_error, enable_strict_consistency};
use crate::executor::over_window::frame_finder::*;
use crate::executor::StreamExecutorResult;

pub(super) type CacheKey = Sentinelled<StateKey>;

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
    deduped_part_key: &OwnedRow,
    range_cache: &mut PartitionCache,
    cache_policy: CachePolicy,
    recently_accessed_range: RangeInclusive<StateKey>,
) {
    tracing::trace!(
        partition=?deduped_part_key,
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

            // find the cursor just before `ck_start`
            let mut cursor = range_cache.inner().upper_bound(Bound::Excluded(&ck_start));
            for _ in 0..MAGIC_JITTER_PREVENTION {
                if cursor.prev().is_none() {
                    // already at the beginning
                    break;
                }
            }
            let start = cursor
                .peek_prev()
                .map(|(k, _)| k)
                .unwrap_or_else(|| range_cache.first_key_value().unwrap().0)
                .clone();

            // find the cursor just after `ck_end`
            let mut cursor = range_cache.inner().lower_bound(Bound::Excluded(&ck_end));
            for _ in 0..MAGIC_JITTER_PREVENTION {
                if cursor.next().is_none() {
                    // already at the end
                    break;
                }
            }
            let end = cursor
                .peek_next()
                .map(|(k, _)| k)
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

                let mut capacity_remain = MAGIC_CACHE_SIZE; // precision is not important here, code simplicity is the first
                const_assert!(MAGIC_JITTER_PREVENTION < MAGIC_CACHE_SIZE);

                // find the cursor just before `ck_start`
                let cursor_just_before_ck_start =
                    range_cache.inner().upper_bound(Bound::Excluded(&ck_start));

                let mut cursor = cursor_just_before_ck_start.clone();
                // go back for at most `MAGIC_JITTER_PREVENTION` entries
                for _ in 0..MAGIC_JITTER_PREVENTION {
                    if cursor.prev().is_none() {
                        // already at the beginning
                        break;
                    }
                    capacity_remain -= 1;
                }
                let start = cursor
                    .peek_prev()
                    .map(|(k, _)| k)
                    .unwrap_or_else(|| range_cache.first_key_value().unwrap().0)
                    .clone();

                let mut cursor = cursor_just_before_ck_start;
                // go forward for at most `capacity_remain` entries
                for _ in 0..capacity_remain {
                    if cursor.next().is_none() {
                        // already at the end
                        break;
                    }
                }
                let end = cursor
                    .peek_next()
                    .map(|(k, _)| k)
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

                let mut capacity_remain = MAGIC_CACHE_SIZE; // precision is not important here, code simplicity is the first
                const_assert!(MAGIC_JITTER_PREVENTION < MAGIC_CACHE_SIZE);

                // find the cursor just after `ck_end`
                let cursor_just_after_ck_end =
                    range_cache.inner().lower_bound(Bound::Excluded(&ck_end));

                let mut cursor = cursor_just_after_ck_end.clone();
                // go forward for at most `MAGIC_JITTER_PREVENTION` entries
                for _ in 0..MAGIC_JITTER_PREVENTION {
                    if cursor.next().is_none() {
                        // already at the end
                        break;
                    }
                    capacity_remain -= 1;
                }
                let end = cursor
                    .peek_next()
                    .map(|(k, _)| k)
                    .unwrap_or_else(|| range_cache.last_key_value().unwrap().0)
                    .clone();

                let mut cursor = cursor_just_after_ck_end;
                // go back for at most `capacity_remain` entries
                for _ in 0..capacity_remain {
                    if cursor.prev().is_none() {
                        // already at the beginning
                        break;
                    }
                }
                let start = cursor
                    .peek_prev()
                    .map(|(k, _)| k)
                    .unwrap_or_else(|| range_cache.first_key_value().unwrap().0)
                    .clone();

                (start, end)
            }
        }
    };

    tracing::trace!(
        partition=?deduped_part_key,
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

#[derive(Default)]
pub(super) struct OverPartitionStats {
    pub lookup_count: u64,
    pub left_miss_count: u64,
    pub right_miss_count: u64,
}

/// [`AffectedRange`] represents a range of keys that are affected by a delta.
/// The [`CacheKey`] fields are keys in the partition range cache + delta, which is
/// represented by [`DeltaBTreeMap`].
///
/// - `first_curr_key` and `last_curr_key` are the current keys of the first and the last
///   windows affected. They are used to pinpoint the bounds where state needs to be updated.
/// - `first_frame_start` and `last_frame_end` are the frame start and end of the first and
///   the last windows affected. They are used to pinpoint the bounds where state needs to be
///   included for computing the new state.
#[derive(Debug, Educe)]
#[educe(Clone, Copy)]
pub(super) struct AffectedRange<'a> {
    pub first_frame_start: &'a CacheKey,
    pub first_curr_key: &'a CacheKey,
    pub last_curr_key: &'a CacheKey,
    pub last_frame_end: &'a CacheKey,
}

impl<'a> AffectedRange<'a> {
    fn new(
        first_frame_start: &'a CacheKey,
        first_curr_key: &'a CacheKey,
        last_curr_key: &'a CacheKey,
        last_frame_end: &'a CacheKey,
    ) -> Self {
        Self {
            first_frame_start,
            first_curr_key,
            last_curr_key,
            last_frame_end,
        }
    }
}

/// A wrapper of [`PartitionCache`] that provides helper methods to manipulate the cache.
/// By putting this type inside `private` module, we can avoid misuse of the internal fields and
/// methods.
pub(super) struct OverPartition<'a, S: StateStore> {
    deduped_part_key: &'a OwnedRow,
    range_cache: &'a mut PartitionCache,
    cache_policy: CachePolicy,

    /// The `ROWS` frame that is the union of all `ROWS` frames of all window functions in this
    /// over window executor.
    super_rows_frame_bounds: RowsFrameBounds,
    range_frames: Vec<&'a RangeFrameBounds>,
    start_is_unbounded: bool,
    end_is_unbounded: bool,
    row_conv: RowConverter<'a>,

    stats: OverPartitionStats,

    _phantom: PhantomData<S>,
    fix_inconsistency: Option<bool>,
}

const MAGIC_BATCH_SIZE: usize = 512;

impl<'a, S: StateStore> OverPartition<'a, S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        deduped_part_key: &'a OwnedRow,
        cache: &'a mut PartitionCache,
        cache_policy: CachePolicy,
        calls: &'a [WindowFuncCall],
        row_conv: RowConverter<'a>,
    ) -> Self {
        let rows_frames = calls
            .iter()
            .filter_map(|call| call.frame.bounds.as_rows())
            .collect::<Vec<_>>();
        // TODO(rc): maybe should avoid repeated merging
        let super_rows_frame_bounds = merge_rows_frames(&rows_frames);
        let range_frames = calls
            .iter()
            .filter_map(|call| call.frame.bounds.as_range())
            .collect::<Vec<_>>();

        let start_is_unbounded = calls
            .iter()
            .any(|call| call.frame.bounds.start_is_unbounded());
        let end_is_unbounded = calls
            .iter()
            .any(|call| call.frame.bounds.end_is_unbounded());

        Self {
            deduped_part_key,
            range_cache: cache,
            cache_policy,

            super_rows_frame_bounds,
            range_frames,
            start_is_unbounded,
            end_is_unbounded,
            row_conv,

            stats: Default::default(),

            _phantom: PhantomData,
            fix_inconsistency: std::env::var("RW_OVER_PARTITION_FIX_INCONSISTENCY")
                .ok()
                .map(|_| true),
        }
    }

    /// Get a summary for the execution happened in the [`OverPartition`] in current round.
    /// This will consume the [`OverPartition`] value itself.
    pub fn summarize(self) -> OverPartitionStats {
        // We may extend this function in the future.
        self.stats
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
    /// are guaranteed to be cached, which means they should be [`Sentinelled::Normal`]s.
    pub async fn find_affected_ranges<'s, 'delta>(
        &'s mut self,
        table: &StateTable<S>,
        delta: &'delta mut PartitionDelta,
    ) -> StreamExecutorResult<(
        DeltaBTreeMap<'delta, CacheKey, OwnedRow>,
        Vec<AffectedRange<'delta>>,
    )>
    where
        'a: 'delta,
        's: 'delta,
    {
        self.ensure_delta_in_cache(table, delta).await?;
        let delta = &*delta; // let's make it immutable

        if delta.is_empty() {
            return Ok((DeltaBTreeMap::new(self.range_cache.inner(), delta), vec![]));
        }

        let delta_first = delta.first_key_value().unwrap().0.as_normal_expect();
        let delta_last = delta.last_key_value().unwrap().0.as_normal_expect();

        let range_frame_logical_curr =
            calc_logical_curr_for_range_frames(&self.range_frames, delta_first, delta_last);

        loop {
            // TERMINATEABILITY: `extend_cache_leftward_by_n` and `extend_cache_rightward_by_n` keep
            // pushing the cache to the boundary of current partition. In these two methods, when
            // any side of boundary is reached, the sentinel key will be removed, so finally
            // `Self::find_affected_ranges_readonly` will return `Ok`.

            // SAFETY: Here we shortly borrow the range cache and turn the reference into a
            // `'delta` one to bypass the borrow checker. This is safe because we only return
            // the reference once we don't need to do any further mutation.
            let cache_inner = unsafe { &*(self.range_cache.inner() as *const _) };
            let part_with_delta = DeltaBTreeMap::new(cache_inner, delta);

            self.stats.lookup_count += 1;
            let res = self
                .find_affected_ranges_readonly(part_with_delta, range_frame_logical_curr.as_ref());

            let (need_extend_leftward, need_extend_rightward) = match res {
                Ok(ranges) => return Ok((part_with_delta, ranges)),
                Err(cache_extend_hint) => cache_extend_hint,
            };

            if need_extend_leftward {
                self.stats.left_miss_count += 1;
                tracing::trace!(partition=?self.deduped_part_key, "partition cache left extension triggered");
                let left_most = self.cache_real_first_key().unwrap_or(delta_first).clone();
                self.extend_cache_leftward_by_n(table, &left_most).await?;
            }
            if need_extend_rightward {
                self.stats.right_miss_count += 1;
                tracing::trace!(partition=?self.deduped_part_key, "partition cache right extension triggered");
                let right_most = self.cache_real_last_key().unwrap_or(delta_last).clone();
                self.extend_cache_rightward_by_n(table, &right_most).await?;
            }
            tracing::trace!(partition=?self.deduped_part_key, "partition cache extended");
        }
    }

    async fn ensure_delta_in_cache(
        &mut self,
        table: &StateTable<S>,
        delta: &mut PartitionDelta,
    ) -> StreamExecutorResult<()> {
        if delta.is_empty() {
            return Ok(());
        }

        let delta_first = delta.first_key_value().unwrap().0.as_normal_expect();
        let delta_last = delta.last_key_value().unwrap().0.as_normal_expect();

        if self.cache_policy.is_full() {
            // ensure everything is in the cache
            self.extend_cache_to_boundary(table).await?;
        } else {
            // TODO(rc): later we should extend cache using `self.super_rows_frame_bounds` and
            // `range_frame_logical_curr` as hints.

            // ensure the cache covers all delta (if possible)
            self.extend_cache_by_range(table, delta_first..=delta_last)
                .await?;
        }

        if !enable_strict_consistency() || self.fix_inconsistency.unwrap_or(false) {
            // in non-strict mode, we should ensure the delta is consistent with the cache
            let cache = self.range_cache.inner();
            delta.retain(|key, change| match &*change {
                Change::Insert(_) => {
                    // this also includes the case of double-insert and ghost-update,
                    // but since we already lost the information, let's just ignore it
                    true
                }
                Change::Delete => {
                    // if the key is not in the cache, it's a ghost-delete
                    let consistent = cache.contains_key(key);
                    if !consistent {
                        consistency_error!(?key, "removing a row with non-existing key");
                    }
                    consistent
                }
            });
        }

        Ok(())
    }

    /// Try to find affected ranges on immutable range cache + delta. If the algorithm reaches
    /// any sentinel node in the cache, which means some entries in the affected range may be
    /// in the state table, it returns an `Err((bool, bool))` to notify the caller that the
    /// left side or the right side or both sides of the cache should be extended.
    ///
    /// TODO(rc): Currently at most one range will be in the result vector. Ideally we should
    /// recognize uncontinuous changes in the delta and find multiple ranges, but that will be
    /// too complex for now.
    fn find_affected_ranges_readonly<'delta>(
        &self,
        part_with_delta: DeltaBTreeMap<'delta, CacheKey, OwnedRow>,
        range_frame_logical_curr: Option<&(Sentinelled<Datum>, Sentinelled<Datum>)>,
    ) -> std::result::Result<Vec<AffectedRange<'delta>>, (bool, bool)> {
        if part_with_delta.first_key().is_none() {
            // nothing is left after applying the delta, meaning all entries are deleted
            return Ok(vec![]);
        }

        let delta_first_key = part_with_delta.delta().first_key_value().unwrap().0;
        let delta_last_key = part_with_delta.delta().last_key_value().unwrap().0;
        let cache_key_pk_len = delta_first_key.as_normal_expect().pk.len();

        if part_with_delta.snapshot().is_empty() {
            // all existing keys are inserted in the delta
            return Ok(vec![AffectedRange::new(
                delta_first_key,
                delta_first_key,
                delta_last_key,
                delta_last_key,
            )]);
        }

        let first_key = part_with_delta.first_key().unwrap();
        let last_key = part_with_delta.last_key().unwrap();

        let first_curr_key = if self.end_is_unbounded || delta_first_key == first_key {
            // If the frame end is unbounded, or, the first key is in delta, then the frame corresponding
            // to the first key is always affected.
            first_key
        } else {
            let mut key = find_first_curr_for_rows_frame(
                &self.super_rows_frame_bounds,
                part_with_delta,
                delta_first_key,
            );

            if let Some((logical_first_curr, _)) = range_frame_logical_curr {
                let logical_curr = logical_first_curr.as_normal_expect(); // otherwise should go `end_is_unbounded` branch
                let new_key = find_left_for_range_frames(
                    &self.range_frames,
                    part_with_delta,
                    logical_curr,
                    cache_key_pk_len,
                );
                key = std::cmp::min(key, new_key);
            }

            key
        };

        let last_curr_key = if self.start_is_unbounded || delta_last_key == last_key {
            // similar to `first_curr_key`
            last_key
        } else {
            let mut key = find_last_curr_for_rows_frame(
                &self.super_rows_frame_bounds,
                part_with_delta,
                delta_last_key,
            );

            if let Some((_, logical_last_curr)) = range_frame_logical_curr {
                let logical_curr = logical_last_curr.as_normal_expect(); // otherwise should go `start_is_unbounded` branch
                let new_key = find_right_for_range_frames(
                    &self.range_frames,
                    part_with_delta,
                    logical_curr,
                    cache_key_pk_len,
                );
                key = std::cmp::max(key, new_key);
            }

            key
        };

        {
            // We quickly return if there's any sentinel in `[first_curr_key, last_curr_key]`,
            // just for the sake of simplicity.
            let mut need_extend_leftward = false;
            let mut need_extend_rightward = false;
            for key in [first_curr_key, last_curr_key] {
                if key.is_smallest() {
                    need_extend_leftward = true;
                } else if key.is_largest() {
                    need_extend_rightward = true;
                }
            }
            if need_extend_leftward || need_extend_rightward {
                return Err((need_extend_leftward, need_extend_rightward));
            }
        }

        // From now on we definitely have two normal `curr_key`s.

        if first_curr_key > last_curr_key {
            // Note that we cannot move the this check before the above block, because for example,
            // if the range cache contains `[Smallest, 5, Largest]`, and the delta contains only
            // `Delete 5`, the frame is `RANGE BETWEEN CURRENT ROW AND CURRENT ROW`, then
            // `first_curr_key` will be `Largest`, `last_curr_key` will be `Smallest`, in this case
            // there may be some other entries with order value `5` in the table, which should be
            // *affected*.
            return Ok(vec![]);
        }

        let range_frame_logical_boundary = calc_logical_boundary_for_range_frames(
            &self.range_frames,
            first_curr_key.as_normal_expect(),
            last_curr_key.as_normal_expect(),
        );

        let first_frame_start = if self.start_is_unbounded || first_curr_key == first_key {
            // If the frame start is unbounded, or, the first curr key is the first key, then the first key
            // always need to be included in the affected range.
            first_key
        } else {
            let mut key = find_frame_start_for_rows_frame(
                &self.super_rows_frame_bounds,
                part_with_delta,
                first_curr_key,
            );

            if let Some((logical_first_start, _)) = range_frame_logical_boundary.as_ref() {
                let logical_boundary = logical_first_start.as_normal_expect(); // otherwise should go `end_is_unbounded` branch
                let new_key = find_left_for_range_frames(
                    &self.range_frames,
                    part_with_delta,
                    logical_boundary,
                    cache_key_pk_len,
                );
                key = std::cmp::min(key, new_key);
            }

            key
        };
        assert!(first_frame_start <= first_curr_key);

        let last_frame_end = if self.end_is_unbounded || last_curr_key == last_key {
            // similar to `first_frame_start`
            last_key
        } else {
            let mut key = find_frame_end_for_rows_frame(
                &self.super_rows_frame_bounds,
                part_with_delta,
                last_curr_key,
            );

            if let Some((_, logical_last_end)) = range_frame_logical_boundary.as_ref() {
                let logical_boundary = logical_last_end.as_normal_expect(); // otherwise should go `end_is_unbounded` branch
                let new_key = find_right_for_range_frames(
                    &self.range_frames,
                    part_with_delta,
                    logical_boundary,
                    cache_key_pk_len,
                );
                key = std::cmp::max(key, new_key);
            }

            key
        };
        assert!(last_frame_end >= last_curr_key);

        let mut need_extend_leftward = false;
        let mut need_extend_rightward = false;
        for key in [
            first_curr_key,
            last_curr_key,
            first_frame_start,
            last_frame_end,
        ] {
            if key.is_smallest() {
                need_extend_leftward = true;
            } else if key.is_largest() {
                need_extend_rightward = true;
            }
        }

        if need_extend_leftward || need_extend_rightward {
            Err((need_extend_leftward, need_extend_rightward))
        } else {
            Ok(vec![AffectedRange::new(
                first_frame_start,
                first_curr_key,
                last_curr_key,
                last_frame_end,
            )])
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

        tracing::trace!(partition=?self.deduped_part_key, "loading the whole partition into cache");

        let mut new_cache = PartitionCache::new(); // shouldn't use `new_empty_partition_cache` here because we don't want sentinels
        let sub_range: &(Bound<OwnedRow>, Bound<OwnedRow>) = &(Bound::Unbounded, Bound::Unbounded);
        let table_iter = table
            .iter_with_prefix(self.deduped_part_key, sub_range, PrefetchOptions::default())
            .await?;

        #[for_await]
        for row in table_iter {
            let row: OwnedRow = row?.into_owned_row();
            new_cache.insert(self.row_conv.row_to_state_key(&row)?.into(), row);
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
                partition=?self.deduped_part_key,
                cache_first=?cache_real_first_key,
                cache_last=?cache_real_last_key,
                range=?range,
                "modified range is completely non-overlapping with the cached range, re-initializing the cache"
            );
            *self.range_cache = new_empty_partition_cache();
        }

        if self.cache_real_len() == 0 {
            // no normal entry in the cache, just load the given range
            let table_sub_range = (
                Bound::Included(self.row_conv.state_key_to_table_sub_pk(range.start())?),
                Bound::Included(self.row_conv.state_key_to_table_sub_pk(range.end())?),
            );
            tracing::debug!(
                partition=?self.deduped_part_key,
                table_sub_range=?table_sub_range,
                "cache is empty, just loading the given range"
            );
            return self
                .extend_cache_by_range_inner(table, table_sub_range)
                .await;
        }

        let cache_real_first_key = self
            .cache_real_first_key()
            .expect("cache real len is not 0");
        if self.cache_left_is_sentinel() && *range.start() < cache_real_first_key {
            // extend leftward only if there's smallest sentinel
            let table_sub_range = (
                Bound::Included(self.row_conv.state_key_to_table_sub_pk(range.start())?),
                Bound::Excluded(
                    self.row_conv
                        .state_key_to_table_sub_pk(cache_real_first_key)?,
                ),
            );
            tracing::trace!(
                partition=?self.deduped_part_key,
                table_sub_range=?table_sub_range,
                "loading the left half of given range"
            );
            self.extend_cache_by_range_inner(table, table_sub_range)
                .await?;
        }

        let cache_real_last_key = self.cache_real_last_key().expect("cache real len is not 0");
        if self.cache_right_is_sentinel() && *range.end() > cache_real_last_key {
            // extend rightward only if there's largest sentinel
            let table_sub_range = (
                Bound::Excluded(
                    self.row_conv
                        .state_key_to_table_sub_pk(cache_real_last_key)?,
                ),
                Bound::Included(self.row_conv.state_key_to_table_sub_pk(range.end())?),
            );
            tracing::trace!(
                partition=?self.deduped_part_key,
                table_sub_range=?table_sub_range,
                "loading the right half of given range"
            );
            self.extend_cache_by_range_inner(table, table_sub_range)
                .await?;
        }

        // prefetch rows before the start of the range
        self.extend_cache_leftward_by_n(table, range.start())
            .await?;

        // prefetch rows after the end of the range
        self.extend_cache_rightward_by_n(table, range.end()).await
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

    async fn extend_cache_by_range_inner(
        &mut self,
        table: &StateTable<S>,
        table_sub_range: (Bound<impl Row>, Bound<impl Row>),
    ) -> StreamExecutorResult<()> {
        let stream = table
            .iter_with_prefix(
                self.deduped_part_key,
                &table_sub_range,
                PrefetchOptions::default(),
            )
            .await?;

        #[for_await]
        for row in stream {
            let row: OwnedRow = row?.into_owned_row();
            let key = self.row_conv.row_to_state_key(&row)?;
            self.range_cache.insert(CacheKey::from(key), row);
        }

        Ok(())
    }

    async fn extend_cache_leftward_by_n_inner(
        &mut self,
        table: &StateTable<S>,
        range_to_exclusive: &StateKey,
    ) -> StreamExecutorResult<()> {
        let mut n_extended = 0usize;
        {
            let sub_range = (
                Bound::<OwnedRow>::Unbounded,
                Bound::Excluded(
                    self.row_conv
                        .state_key_to_table_sub_pk(range_to_exclusive)?,
                ),
            );
            let rev_stream = table
                .rev_iter_with_prefix(
                    self.deduped_part_key,
                    &sub_range,
                    PrefetchOptions::default(),
                )
                .await?;

            #[for_await]
            for row in rev_stream {
                let row: OwnedRow = row?.into_owned_row();

                let key = self.row_conv.row_to_state_key(&row)?;
                self.range_cache.insert(CacheKey::from(key), row);

                n_extended += 1;
                if n_extended == MAGIC_BATCH_SIZE {
                    break;
                }
            }
        }

        if n_extended < MAGIC_BATCH_SIZE && self.cache_real_len() > 0 {
            // we reached the beginning of this partition in the table
            self.range_cache.remove(&CacheKey::Smallest);
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
            let sub_range = (
                Bound::Excluded(
                    self.row_conv
                        .state_key_to_table_sub_pk(range_from_exclusive)?,
                ),
                Bound::<OwnedRow>::Unbounded,
            );
            let stream = table
                .iter_with_prefix(
                    self.deduped_part_key,
                    &sub_range,
                    PrefetchOptions::default(),
                )
                .await?;

            #[for_await]
            for row in stream {
                let row: OwnedRow = row?.into_owned_row();

                let key = self.row_conv.row_to_state_key(&row)?;
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
}
