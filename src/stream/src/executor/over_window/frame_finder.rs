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

//! Helper functions for finding affected ranges from over window range cache for a
//! given set of changes (delta).

use std::ops::Bound;

use delta_btree_map::{CursorWithDelta, DeltaBTreeMap};
use itertools::Itertools;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{Datum, Sentinelled, ToDatumRef};
use risingwave_common::util::memcmp_encoding;
use risingwave_common::util::sort_util::cmp_datum;
use risingwave_expr::window_function::{FrameBound, RangeFrameBounds, RowsFrameBounds, StateKey};

use super::over_partition::CacheKey;

// -------------------------- ↓ PUBLIC INTERFACE ↓ --------------------------

/// Merge several `ROWS` frames into one super frame. The returned super frame is
/// guaranteed to be *canonical*, which means that the `CURRENT ROW` is always
/// included in the returned frame.
pub(super) fn merge_rows_frames(rows_frames: &[&RowsFrameBounds]) -> RowsFrameBounds {
    if rows_frames.is_empty() {
        // When there's no `ROWS` frame, for simplicity, we don't return `None`. Instead,
        // we return `ROWS BETWEEN CURRENT ROW AND CURRENT ROW`, which is the implicit
        // frame for all selected upstream columns.
        //
        // For example, the following two queries are equivalent:
        //
        // ```sql
        // SELECT a, b, sum(c) OVER (...) FROM t;
        // SELECT
        //   first_value(a) OVER (... ROWS BETWEEN CURRENT ROW AND CURRENT ROW),
        //   first_value(b) OVER (... ROWS BETWEEN CURRENT ROW AND CURRENT ROW),
        //   sum(c) OVER (...)
        // FROM t;
        // ```
        //
        // See https://risingwave.com/blog/risingwave-window-functions-the-art-of-sliding-and-the-aesthetics-of-symmetry/
        // for more details.
        return RowsFrameBounds {
            start: FrameBound::CurrentRow,
            end: FrameBound::CurrentRow,
        };
    }

    let none_as_max_cmp = |x: &Option<usize>, y: &Option<usize>| match (x, y) {
        // None means unbounded, which should be the largest.
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (Some(_), None) => std::cmp::Ordering::Less,
        (Some(x), Some(y)) => x.cmp(y),
    };

    // Note that the following two both use `max_by`, unlike when handling `RANGE` frames,
    // because here for `ROWS` frames, offsets are unsigned. We convert all pure preceding/
    // following `ROWS` frames into one containing the `CURRENT ROW`.
    let start = rows_frames
        .iter()
        .map(|bounds| bounds.n_preceding_rows())
        .max_by(none_as_max_cmp)
        .unwrap();
    let end = rows_frames
        .iter()
        .map(|bounds| bounds.n_following_rows())
        .max_by(none_as_max_cmp)
        .unwrap();

    RowsFrameBounds {
        start: start
            .map(FrameBound::Preceding) // may produce Preceding(0), but doesn't matter
            .unwrap_or(FrameBound::UnboundedPreceding),
        end: end
            .map(FrameBound::Following) // may produce Following(0), but doesn't matter
            .unwrap_or(FrameBound::UnboundedFollowing),
    }
}

/// For a canonical `ROWS` frame, given a key in delta, find the cache key
/// corresponding to the CURRENT ROW of the first frame that contains the given
/// key.
///
/// ## Example
///
/// - Frame: `ROWS BETWEEN ? AND 2 FOLLOWING`
/// - Cache: `[1, 2, 5]`
/// - Delta: `[Delete 5]`
///
/// For delta key `5`, this function will return `1` as the *first curr key*.
///
/// More examples can be found in the comment inside [`find_curr_for_rows_frame`].
pub(super) fn find_first_curr_for_rows_frame<'cache>(
    frame_bounds: &RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    delta_key: &'cache CacheKey,
) -> &'cache CacheKey {
    find_curr_for_rows_frame::<true /* LEFT */>(frame_bounds, part_with_delta, delta_key)
}

/// For a canonical `ROWS` frame, given a key in delta, find the cache key
/// corresponding to the CURRENT ROW of the last frame that contains the given
/// key.
///
/// This is the symmetric function of [`find_first_curr_for_rows_frame`].
pub(super) fn find_last_curr_for_rows_frame<'cache>(
    frame_bounds: &RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    delta_key: &'cache CacheKey,
) -> &'cache CacheKey {
    find_curr_for_rows_frame::<false /* RIGHT */>(frame_bounds, part_with_delta, delta_key)
}

/// For a canonical `ROWS` frame, given a key in `part_with_delta` corresponding
/// to some CURRENT ROW, find the cache key corresponding to the start row in
/// that frame.
pub(super) fn find_frame_start_for_rows_frame<'cache>(
    frame_bounds: &RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    curr_key: &'cache CacheKey,
) -> &'cache CacheKey {
    find_boundary_for_rows_frame::<true /* LEFT */>(frame_bounds, part_with_delta, curr_key)
}

/// For a canonical `ROWS` frame, given a key in `part_with_delta` corresponding
/// to some CURRENT ROW, find the cache key corresponding to the end row in that
/// frame.
///
/// This is the symmetric function of [`find_frame_start_for_rows_frame`].
pub(super) fn find_frame_end_for_rows_frame<'cache>(
    frame_bounds: &RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    curr_key: &'cache CacheKey,
) -> &'cache CacheKey {
    find_boundary_for_rows_frame::<false /* RIGHT */>(frame_bounds, part_with_delta, curr_key)
}

/// Given the first and last key in delta, calculate the order values of the first
/// and the last frames logically affected by some `RANGE` frames.
pub(super) fn calc_logical_curr_for_range_frames(
    range_frames: &[RangeFrameBounds],
    delta_first_key: &StateKey,
    delta_last_key: &StateKey,
) -> Option<(Sentinelled<Datum>, Sentinelled<Datum>)> {
    calc_logical_ord_for_range_frames(
        range_frames,
        delta_first_key,
        delta_last_key,
        |bounds, v| bounds.first_curr_of(v),
        |bounds, v| bounds.last_curr_of(v),
    )
}

/// Given the curr keys of the first and the last affected frames, calculate the order
/// values of the logical start row of the first frame and the logical end row of the
/// last frame.
pub(super) fn calc_logical_boundary_for_range_frames(
    range_frames: &[RangeFrameBounds],
    first_curr_key: &StateKey,
    last_curr_key: &StateKey,
) -> Option<(Sentinelled<Datum>, Sentinelled<Datum>)> {
    calc_logical_ord_for_range_frames(
        range_frames,
        first_curr_key,
        last_curr_key,
        |bounds, v| bounds.frame_start_of(v),
        |bounds, v| bounds.frame_end_of(v),
    )
}

/// Given a left logical order value (e.g. first curr order value, first delta order value),
/// find the most closed cache key in `part_with_delta`. Ideally this function returns
/// the smallest key that is larger than or equal to the given logical order (using `lower_bound`).
pub(super) fn find_left_for_range_frames<'cache>(
    range_frames: &[RangeFrameBounds],
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    logical_order_value: impl ToDatumRef,
    cache_key_pk_len: usize, // this is dirty but we have no better choice
) -> &'cache CacheKey {
    find_for_range_frames::<true /* LEFT */>(
        range_frames,
        part_with_delta,
        logical_order_value,
        cache_key_pk_len,
    )
}

/// Given a right logical order value (e.g. last curr order value, last delta order value),
/// find the most closed cache key in `part_with_delta`. Ideally this function returns
/// the largest key that is smaller than or equal to the given logical order (using `lower_bound`).
pub(super) fn find_right_for_range_frames<'cache>(
    range_frames: &[RangeFrameBounds],
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    logical_order_value: impl ToDatumRef,
    cache_key_pk_len: usize, // this is dirty but we have no better choice
) -> &'cache CacheKey {
    find_for_range_frames::<false /* RIGHT */>(
        range_frames,
        part_with_delta,
        logical_order_value,
        cache_key_pk_len,
    )
}

// -------------------------- ↑ PUBLIC INTERFACE ↑ --------------------------

fn find_curr_for_rows_frame<'cache, const LEFT: bool>(
    frame_bounds: &RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    delta_key: &'cache CacheKey,
) -> &'cache CacheKey {
    debug_assert!(frame_bounds.is_canonical());
    if LEFT {
        debug_assert!(
            !frame_bounds.end.is_unbounded_following(),
            "no need to call this function whenever any frame end is unbounded"
        );
    } else {
        debug_assert!(
            !frame_bounds.start.is_unbounded_preceding(),
            "no need to call this function whenever any frame start is unbounded"
        );
    }
    debug_assert!(
        part_with_delta.first_key().is_some(),
        "must have something in the range cache after applying delta"
    );

    // Let's think about the following algorithm with the following cases in mind.
    // Insertions are relatively easy, so we only give the deletion examples.
    // The two directions are symmetrical, we only consider the left direction in
    // the following description.
    //
    // ## Background
    //
    // *Sm* means smallest sentinel node in the cache
    // *SM* means largest sentinel node in the cache
    //
    // Before calling this function, all entries within range covered by delta should
    // have been loaded into the cache.
    //
    // ## Cases
    //
    // Frame: ROWS BETWEEN ? AND 2 FOLLOWING
    // Delta: Delete 5
    // Cache + Delta:
    //   [1, 2, 6] -> 1
    //   [1, 6] -> 1
    //   [6, 7, 8] -> 6, not precise but won't do too much harm
    //   [1, 2] -> 1
    //   [1] -> 1
    //   [Sm, 6] -> Sm
    //   [Sm, 1] -> Sm
    //   [1, 2, SM] -> 1
    //   [1, SM] -> 1
    //   [Sm, 1, SM] -> Sm
    //   [Sm, SM] -> Sm
    //
    // Frame: ROWS BETWEEN ? AND CURRENT ROW
    // Delta: Delete 5
    // Cache + Delta:
    //   [1, 2, 6] -> 6, not precise but won't do too much harm
    //   [1, 2] -> 2, not precise but won't do too much harm
    //   [1, 2, SM] -> SM, not precise but won't do too much harm
    //   [Sm, SM] -> SM, not precise but won't do too much harm
    //
    // Frame: ROWS BETWEEN ? AND 2 PRECEDING
    // This will be treated as if it's `ROWS BETWEEN ? AND CURRENT ROW`.

    let mut cursor = if LEFT {
        part_with_delta.lower_bound(Bound::Included(delta_key))
    } else {
        part_with_delta.upper_bound(Bound::Included(delta_key))
    };
    let pointed_key = |cursor: CursorWithDelta<'cache, CacheKey, OwnedRow>| {
        if LEFT {
            cursor.peek_next().map(|(k, _)| k)
        } else {
            cursor.peek_prev().map(|(k, _)| k)
        }
    };

    let n_rows_to_move = if LEFT {
        frame_bounds.n_following_rows().unwrap()
    } else {
        frame_bounds.n_preceding_rows().unwrap()
    };

    if n_rows_to_move == 0 {
        return pointed_key(cursor)
            .or_else(|| {
                if LEFT {
                    part_with_delta.last_key()
                } else {
                    part_with_delta.first_key()
                }
            })
            .unwrap();
    }

    for _ in 0..n_rows_to_move {
        let res = if LEFT { cursor.prev() } else { cursor.next() };
        if res.is_none() {
            // we reach the end
            break;
        }
    }

    // We always have a valid key here, because `part_with_delta` must not be empty,
    // and `n_rows_to_move` is always larger than 0 when we reach here.
    pointed_key(cursor).unwrap()
}

fn find_boundary_for_rows_frame<'cache, const LEFT: bool>(
    frame_bounds: &RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    curr_key: &'cache CacheKey,
) -> &'cache CacheKey {
    debug_assert!(frame_bounds.is_canonical());
    if LEFT {
        debug_assert!(
            !frame_bounds.start.is_unbounded_preceding(),
            "no need to call this function whenever any frame start is unbounded"
        );
    } else {
        debug_assert!(
            !frame_bounds.end.is_unbounded_following(),
            "no need to call this function whenever any frame end is unbounded"
        );
    }

    // Now things are easier than in `find_curr_for_rows_frame`, because we already
    // have `curr_key` which definitely exists in the `part_with_delta`. We just find
    // the cursor pointing to it and move the cursor to frame boundary.

    let mut cursor = if LEFT {
        part_with_delta.before(curr_key).unwrap()
    } else {
        part_with_delta.after(curr_key).unwrap()
    };
    let pointed_key = |cursor: CursorWithDelta<'cache, CacheKey, OwnedRow>| {
        if LEFT {
            cursor.peek_next().map(|(k, _)| k)
        } else {
            cursor.peek_prev().map(|(k, _)| k)
        }
    };

    let n_rows_to_move = if LEFT {
        frame_bounds.n_preceding_rows().unwrap()
    } else {
        frame_bounds.n_following_rows().unwrap()
    };

    for _ in 0..n_rows_to_move {
        let res = if LEFT { cursor.prev() } else { cursor.next() };
        if res.is_none() {
            // we reach the end
            break;
        }
    }

    // We always have a valid key here, because `cursor` must point to a valid key
    // at the beginning.
    pointed_key(cursor).unwrap()
}

/// Given a pair of left and right state keys, calculate the leftmost (smallest) and rightmost
/// (largest) order values after the two given `offset_fn`s are applied, for all range frames.
///
/// A more vivid description may be: Given a pair of left and right keys, this function pushes
/// the keys leftward and rightward respectively according to the given `offset_fn`s.
///
/// This is not very understandable but we have to extract the code to a function to avoid
/// repeating. Check [`calc_logical_curr_for_range_frames`] and [`calc_logical_boundary_for_range_frames`]
/// if you cannot understand the purpose of this function.
fn calc_logical_ord_for_range_frames(
    range_frames: &[RangeFrameBounds],
    left_key: &StateKey,
    right_key: &StateKey,
    left_offset_fn: impl Fn(&RangeFrameBounds, &Datum) -> Sentinelled<Datum>,
    right_offset_fn: impl Fn(&RangeFrameBounds, &Datum) -> Sentinelled<Datum>,
) -> Option<(Sentinelled<Datum>, Sentinelled<Datum>)> {
    if range_frames.is_empty() {
        return None;
    }

    let (data_type, order_type) = range_frames
        .iter()
        .map(|bounds| (&bounds.order_data_type, bounds.order_type))
        .all_equal_value()
        .unwrap();

    let datum_cmp = |a: &Datum, b: &Datum| cmp_datum(a, b, order_type);

    let left_given_ord = memcmp_encoding::decode_value(data_type, &left_key.order_key, order_type)
        .expect("no reason to fail because we just encoded it in memory");
    let right_given_ord =
        memcmp_encoding::decode_value(data_type, &right_key.order_key, order_type)
            .expect("no reason to fail because we just encoded it in memory");

    let logical_left_offset_ord = {
        let mut order_value = None;
        for bounds in range_frames {
            let new_order_value = left_offset_fn(bounds, &left_given_ord);
            order_value = match (order_value, new_order_value) {
                (None, any_new) => Some(any_new),
                (Some(old), new) => Some(std::cmp::min_by(old, new, |x, y| x.cmp_by(y, datum_cmp))),
            };
            if !order_value.as_ref().unwrap().is_normal() {
                // already unbounded
                assert!(
                    order_value.as_ref().unwrap().is_smallest(),
                    "left order value should never be `Largest`"
                );
                break;
            }
        }
        order_value.expect("# of range frames > 0")
    };

    let logical_right_offset_ord = {
        let mut order_value = None;
        for bounds in range_frames {
            let new_order_value = right_offset_fn(bounds, &right_given_ord);
            order_value = match (order_value, new_order_value) {
                (None, any_new) => Some(any_new),
                (Some(old), new) => Some(std::cmp::max_by(old, new, |x, y| x.cmp_by(y, datum_cmp))),
            };
            if !order_value.as_ref().unwrap().is_normal() {
                // already unbounded
                assert!(
                    order_value.as_ref().unwrap().is_largest(),
                    "right order value should never be `Smallest`"
                );
                break;
            }
        }
        order_value.expect("# of range frames > 0")
    };

    Some((logical_left_offset_ord, logical_right_offset_ord))
}

fn find_for_range_frames<'cache, const LEFT: bool>(
    range_frames: &[RangeFrameBounds],
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    logical_order_value: impl ToDatumRef,
    cache_key_pk_len: usize,
) -> &'cache CacheKey {
    debug_assert!(
        part_with_delta.first_key().is_some(),
        "must have something in the range cache after applying delta"
    );

    let order_type = range_frames
        .iter()
        .map(|bounds| bounds.order_type)
        .all_equal_value()
        .unwrap();

    let search_key = Sentinelled::Normal(StateKey {
        order_key: memcmp_encoding::encode_value(logical_order_value, order_type)
            .expect("the data type is simple, should succeed"),
        pk: if LEFT {
            OwnedRow::empty() // empty row is minimal
        } else {
            OwnedRow::new(vec![None; cache_key_pk_len]) // all-NULL row is maximal in default order
        }
        .into(),
    });

    if LEFT {
        let cursor = part_with_delta.lower_bound(Bound::Included(&search_key));
        if let Some((prev_key, _)) = cursor.peek_prev()
            && prev_key.is_smallest()
        {
            // If the found lower bound of search key is right behind a smallest sentinel,
            // we don't know if there's any other rows with the same order key in the state
            // table but not in cache. We should conservatively return the sentinel key as
            // the curr key.
            prev_key
        } else {
            // If there's nothing on the left, it simply means that the search key is larger
            // than any existing key. Returning the last key in this case does no harm. Especially,
            // if the last key is largest sentinel, the caller should extend the cache rightward
            // to get possible entries with the same order value into the cache.
            cursor
                .peek_next()
                .map(|(k, _)| k)
                .or_else(|| part_with_delta.last_key())
                .unwrap()
        }
    } else {
        let cursor = part_with_delta.upper_bound(Bound::Included(&search_key));
        if let Some((next_key, _)) = cursor.peek_next()
            && next_key.is_largest()
        {
            next_key
        } else {
            cursor
                .peek_prev()
                .map(|(k, _)| k)
                .or_else(|| part_with_delta.first_key())
                .unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use delta_btree_map::Change;
    use risingwave_common::types::{ScalarImpl, Sentinelled};
    use risingwave_expr::window_function::FrameBound::*;
    use risingwave_expr::window_function::{RowsFrameBounds, StateKey};

    use super::*;

    #[test]
    fn test_merge_rows_frame() {
        fn assert_equivalent(bounds1: RowsFrameBounds, bounds2: RowsFrameBounds) {
            assert_eq!(bounds1.start.to_offset(), bounds2.start.to_offset());
            assert_eq!(bounds1.end.to_offset(), bounds2.end.to_offset());
        }

        assert_equivalent(
            merge_rows_frames(&[]),
            RowsFrameBounds {
                start: CurrentRow,
                end: CurrentRow,
            },
        );

        let frames = [
            &RowsFrameBounds {
                start: Preceding(3),
                end: Preceding(2),
            },
            &RowsFrameBounds {
                start: Preceding(1),
                end: Preceding(4),
            },
        ];
        assert_equivalent(
            merge_rows_frames(&frames),
            RowsFrameBounds {
                start: Preceding(4),
                end: CurrentRow,
            },
        );

        let frames = [
            &RowsFrameBounds {
                start: Preceding(3),
                end: Following(2),
            },
            &RowsFrameBounds {
                start: Preceding(2),
                end: Following(3),
            },
        ];
        assert_equivalent(
            merge_rows_frames(&frames),
            RowsFrameBounds {
                start: Preceding(3),
                end: Following(3),
            },
        );

        let frames = [
            &RowsFrameBounds {
                start: UnboundedPreceding,
                end: Following(2),
            },
            &RowsFrameBounds {
                start: Preceding(2),
                end: UnboundedFollowing,
            },
        ];
        assert_equivalent(
            merge_rows_frames(&frames),
            RowsFrameBounds {
                start: UnboundedPreceding,
                end: UnboundedFollowing,
            },
        );

        let frames = [
            &RowsFrameBounds {
                start: UnboundedPreceding,
                end: Following(2),
            },
            &RowsFrameBounds {
                start: Following(5),
                end: Following(2),
            },
        ];
        assert_equivalent(
            merge_rows_frames(&frames),
            RowsFrameBounds {
                start: UnboundedPreceding,
                end: Following(5),
            },
        );
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
                                pk: OwnedRow::new(vec![Some(ScalarImpl::from($pk))]).into(),
                            },
                        ),
                        // value row doesn't matter here
                        OwnedRow::empty(),
                    );
                )*
                cache
            }
        };
        ($ord_type:expr, [..., $( ( $ord:literal, $pk:literal ) ),* , ...]) => {
            {
                let mut cache = create_cache!($ord_type, [$( ( $ord, $pk ) ),*]);
                cache.insert(CacheKey::Smallest, OwnedRow::empty().into());
                cache.insert(CacheKey::Largest, OwnedRow::empty().into());
                cache
            }
        };
        ($ord_type:expr, [..., $( ( $ord:literal, $pk:literal ) ),*]) => {
            {
                let mut cache = create_cache!($ord_type, [$( ( $ord, $pk ) ),*]);
                cache.insert(CacheKey::Smallest, OwnedRow::empty().into());
                cache
            }
        };
        ($ord_type:expr, [$( ( $ord:literal, $pk:literal ) ),* , ...]) => {
            {
                let mut cache = create_cache!($ord_type, [$( ( $ord, $pk ) ),*]);
                cache.insert(CacheKey::Largest, OwnedRow::empty().into());
                cache
            }
        };
        ($ord_type:expr, [$( ( $ord:literal, $pk:literal ) ),*]) => {
            {
                #[allow(unused_mut)]
                let mut cache = BTreeMap::new();
                $(
                    cache.insert(
                        CacheKey::Normal(
                            StateKey {
                                order_key: memcmp_encoding::encode_value(
                                    Some(ScalarImpl::from($ord)),
                                    $ord_type,
                                ).unwrap(),
                                pk: OwnedRow::new(vec![Some(ScalarImpl::from($pk))]).into(),
                            },
                        ),
                        // value row doesn't matter here
                        OwnedRow::empty(),
                    );
                )*
                cache
            }
        }
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
        ($( ( $pk:literal, $change:ident ) ),+ $(,)?) => {
            {
                let mut delta = BTreeMap::new();
                $(
                    delta.insert(
                        CacheKey::Normal(
                            StateKey {
                                // order key doesn't matter here
                                order_key: vec![].into(),
                                pk: OwnedRow::new(vec![Some(ScalarImpl::from($pk))]).into(),
                            },
                        ),
                        // value row doesn't matter here
                        create_change!( $change ),
                    );
                )*
                delta
            }
        };
        ($ord_type:expr, [ $( ( $ord:literal, $pk:literal, $change:ident ) ),+ $(,)? ]) => {
            {
                let mut delta = BTreeMap::new();
                $(
                    delta.insert(
                        CacheKey::Normal(
                            StateKey {
                                order_key: memcmp_encoding::encode_value(
                                    Some(ScalarImpl::from($ord)),
                                    $ord_type,
                                ).unwrap(),
                                pk: OwnedRow::new(vec![Some(ScalarImpl::from($pk))]).into(),
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

    mod rows_frame_tests {
        use super::*;

        fn assert_cache_key_eq(given: &CacheKey, expected: impl Into<ScalarImpl>) {
            assert_eq!(
                given.as_normal_expect().pk.0,
                OwnedRow::new(vec![Some(expected.into())])
            )
        }

        #[test]
        fn test_insert_delta_only() {
            let cache = create_cache!();
            let delta = create_delta!((1, Insert), (2, Insert), (3, Insert));
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);
            let delta_first_key = delta.first_key_value().unwrap().0;
            let delta_last_key = delta.last_key_value().unwrap().0;

            let bounds = RowsFrameBounds {
                start: Preceding(2),
                end: CurrentRow,
            };

            let first_curr_key =
                find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
            let last_curr_key =
                find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
            assert_cache_key_eq(first_curr_key, 1);
            assert_cache_key_eq(last_curr_key, 3);

            let first_frame_start =
                find_frame_start_for_rows_frame(&bounds, part_with_delta, first_curr_key);
            let last_frame_end =
                find_frame_end_for_rows_frame(&bounds, part_with_delta, last_curr_key);
            assert_cache_key_eq(first_frame_start, 1);
            assert_cache_key_eq(last_frame_end, 3);
        }

        #[test]
        fn test_simple() {
            let cache = create_cache!(1, 2, 3, 4, 5, 6);
            let delta = create_delta!((2, Insert), (3, Delete));
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);
            let delta_first_key = delta.first_key_value().unwrap().0;
            let delta_last_key = delta.last_key_value().unwrap().0;

            {
                let bounds = RowsFrameBounds {
                    start: Preceding(2),
                    end: CurrentRow,
                };
                let first_curr_key =
                    find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
                let last_curr_key =
                    find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
                assert_cache_key_eq(first_curr_key, 2);
                assert_cache_key_eq(last_curr_key, 5);

                let first_frame_start =
                    find_frame_start_for_rows_frame(&bounds, part_with_delta, first_curr_key);
                let last_frame_end =
                    find_frame_end_for_rows_frame(&bounds, part_with_delta, last_curr_key);
                assert_cache_key_eq(first_frame_start, 1);
                assert_cache_key_eq(last_frame_end, 5);
            }

            {
                let bounds = RowsFrameBounds {
                    start: Preceding(1),
                    end: Following(2),
                };
                let first_curr_key =
                    find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
                let last_curr_key =
                    find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
                assert_cache_key_eq(first_curr_key, 1);
                assert_cache_key_eq(last_curr_key, 4);

                let first_frame_start =
                    find_frame_start_for_rows_frame(&bounds, part_with_delta, first_curr_key);
                let last_frame_end =
                    find_frame_end_for_rows_frame(&bounds, part_with_delta, last_curr_key);
                assert_cache_key_eq(first_frame_start, 1);
                assert_cache_key_eq(last_frame_end, 6);
            }

            {
                let bounds = RowsFrameBounds {
                    start: CurrentRow,
                    end: Following(2),
                };
                let first_curr_key =
                    find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
                let last_curr_key =
                    find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
                assert_cache_key_eq(first_curr_key, 1);
                assert_cache_key_eq(last_curr_key, 2);

                let first_frame_start =
                    find_frame_start_for_rows_frame(&bounds, part_with_delta, first_curr_key);
                let last_frame_end =
                    find_frame_end_for_rows_frame(&bounds, part_with_delta, last_curr_key);
                assert_cache_key_eq(first_frame_start, 1);
                assert_cache_key_eq(last_frame_end, 5);
            }
        }

        #[test]
        fn test_lag_corner_case() {
            let cache = create_cache!(1, 2, 3, 4, 5, 6);
            let delta = create_delta!((1, Delete), (2, Delete), (3, Delete));
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);
            let delta_first_key = delta.first_key_value().unwrap().0;
            let delta_last_key = delta.last_key_value().unwrap().0;

            let bounds = RowsFrameBounds {
                start: Preceding(1),
                end: CurrentRow,
            };

            let first_curr_key =
                find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
            let last_curr_key =
                find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
            assert_cache_key_eq(first_curr_key, 4);
            assert_cache_key_eq(last_curr_key, 4);

            let first_frame_start =
                find_frame_start_for_rows_frame(&bounds, part_with_delta, first_curr_key);
            let last_frame_end =
                find_frame_end_for_rows_frame(&bounds, part_with_delta, last_curr_key);
            assert_cache_key_eq(first_frame_start, 4);
            assert_cache_key_eq(last_frame_end, 4);
        }

        #[test]
        fn test_lead_corner_case() {
            let cache = create_cache!(1, 2, 3, 4, 5, 6);
            let delta = create_delta!((4, Delete), (5, Delete), (6, Delete));
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);
            let delta_first_key = delta.first_key_value().unwrap().0;
            let delta_last_key = delta.last_key_value().unwrap().0;

            let bounds = RowsFrameBounds {
                start: CurrentRow,
                end: Following(1),
            };

            let first_curr_key =
                find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
            let last_curr_key =
                find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
            assert_cache_key_eq(first_curr_key, 3);
            assert_cache_key_eq(last_curr_key, 3);

            let first_frame_start =
                find_frame_start_for_rows_frame(&bounds, part_with_delta, first_curr_key);
            let last_frame_end =
                find_frame_end_for_rows_frame(&bounds, part_with_delta, last_curr_key);
            assert_cache_key_eq(first_frame_start, 3);
            assert_cache_key_eq(last_frame_end, 3);
        }

        #[test]
        fn test_lag_lead_offset_0_corner_case_1() {
            let cache = create_cache!(1, 2, 3, 4);
            let delta = create_delta!((2, Delete), (3, Delete));
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);
            let delta_first_key = delta.first_key_value().unwrap().0;
            let delta_last_key = delta.last_key_value().unwrap().0;

            let bounds = RowsFrameBounds {
                start: CurrentRow,
                end: CurrentRow,
            };

            let first_curr_key =
                find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
            let last_curr_key =
                find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
            assert_cache_key_eq(first_curr_key, 4);
            assert_cache_key_eq(last_curr_key, 1);

            // first_curr_key > last_curr_key, should not continue to find frame start/end
        }

        #[test]
        fn test_lag_lead_offset_0_corner_case_2() {
            // Note this case is false-positive, but it does very little harm.

            let cache = create_cache!(1, 2, 3, 4);
            let delta = create_delta!((2, Delete), (3, Delete), (4, Delete));
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);
            let delta_first_key = delta.first_key_value().unwrap().0;
            let delta_last_key = delta.last_key_value().unwrap().0;

            let bounds = RowsFrameBounds {
                start: CurrentRow,
                end: CurrentRow,
            };

            let first_curr_key =
                find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
            let last_curr_key =
                find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
            assert_cache_key_eq(first_curr_key, 1);
            assert_cache_key_eq(last_curr_key, 1);

            let first_frame_start =
                find_frame_start_for_rows_frame(&bounds, part_with_delta, first_curr_key);
            let last_frame_end =
                find_frame_end_for_rows_frame(&bounds, part_with_delta, last_curr_key);
            assert_cache_key_eq(first_frame_start, 1);
            assert_cache_key_eq(last_frame_end, 1);
        }

        #[test]
        fn test_lag_lead_offset_0_corner_case_3() {
            let cache = create_cache!(1, 2, 3, 4, 5);
            let delta = create_delta!((2, Delete), (3, Insert), (4, Delete));
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);
            let delta_first_key = delta.first_key_value().unwrap().0;
            let delta_last_key = delta.last_key_value().unwrap().0;

            let bounds = RowsFrameBounds {
                start: CurrentRow,
                end: CurrentRow,
            };

            let first_curr_key =
                find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
            let last_curr_key =
                find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
            assert_cache_key_eq(first_curr_key, 3);
            assert_cache_key_eq(last_curr_key, 3);

            let first_frame_start =
                find_frame_start_for_rows_frame(&bounds, part_with_delta, first_curr_key);
            let last_frame_end =
                find_frame_end_for_rows_frame(&bounds, part_with_delta, last_curr_key);
            assert_cache_key_eq(first_frame_start, 3);
            assert_cache_key_eq(last_frame_end, 3);
        }

        #[test]
        fn test_empty_with_sentinels() {
            let cache: BTreeMap<Sentinelled<StateKey>, OwnedRow> = create_cache!(..., , ...);
            let delta = create_delta!((1, Insert), (2, Insert));
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);
            let delta_first_key = delta.first_key_value().unwrap().0;
            let delta_last_key = delta.last_key_value().unwrap().0;

            {
                let bounds = RowsFrameBounds {
                    start: CurrentRow,
                    end: CurrentRow,
                };
                let first_curr_key =
                    find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
                let last_curr_key =
                    find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
                assert_cache_key_eq(first_curr_key, 1);
                assert_cache_key_eq(last_curr_key, 2);

                let first_frame_start =
                    find_frame_start_for_rows_frame(&bounds, part_with_delta, first_curr_key);
                let last_frame_end =
                    find_frame_end_for_rows_frame(&bounds, part_with_delta, last_curr_key);
                assert_cache_key_eq(first_frame_start, 1);
                assert_cache_key_eq(last_frame_end, 2);
            }

            {
                let bounds = RowsFrameBounds {
                    start: Preceding(1),
                    end: CurrentRow,
                };
                let first_curr_key =
                    find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
                let last_curr_key =
                    find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
                assert_cache_key_eq(first_curr_key, 1);
                assert!(last_curr_key.is_largest());

                // reaches sentinel, should not continue to find frame start/end
            }

            {
                let bounds = RowsFrameBounds {
                    start: CurrentRow,
                    end: Following(3),
                };

                let first_curr_key =
                    find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
                let last_curr_key =
                    find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
                assert!(first_curr_key.is_smallest());
                assert_cache_key_eq(last_curr_key, 2);

                // reaches sentinel, should not continue to find frame start/end
            }
        }

        #[test]
        fn test_with_left_sentinel() {
            let cache = create_cache!(..., 2, 4, 5, 8);
            let delta = create_delta!((3, Insert), (4, Insert), (8, Delete));
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);
            let delta_first_key = delta.first_key_value().unwrap().0;
            let delta_last_key = delta.last_key_value().unwrap().0;

            {
                let bounds = RowsFrameBounds {
                    start: CurrentRow,
                    end: Following(1),
                };
                let first_curr_key =
                    find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
                let last_curr_key =
                    find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
                assert_cache_key_eq(first_curr_key, 2);
                assert_cache_key_eq(last_curr_key, 5);

                let first_frame_start =
                    find_frame_start_for_rows_frame(&bounds, part_with_delta, first_curr_key);
                let last_frame_end =
                    find_frame_end_for_rows_frame(&bounds, part_with_delta, last_curr_key);
                assert_cache_key_eq(first_frame_start, 2);
                assert_cache_key_eq(last_frame_end, 5);
            }

            {
                let bounds = RowsFrameBounds {
                    start: Preceding(1),
                    end: Following(1),
                };
                let first_curr_key =
                    find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
                let last_curr_key =
                    find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
                assert_cache_key_eq(first_curr_key, 2);
                assert_cache_key_eq(last_curr_key, 5);

                let first_frame_start =
                    find_frame_start_for_rows_frame(&bounds, part_with_delta, first_curr_key);
                let last_frame_end =
                    find_frame_end_for_rows_frame(&bounds, part_with_delta, last_curr_key);
                assert!(first_frame_start.is_smallest());
                assert_cache_key_eq(last_frame_end, 5);
            }
        }

        #[test]
        fn test_with_right_sentinel() {
            let cache = create_cache!(1, 2, 4, 5, 8, ...);
            let delta = create_delta!((3, Insert), (4, Insert), (5, Delete));
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);
            let delta_first_key = delta.first_key_value().unwrap().0;
            let delta_last_key = delta.last_key_value().unwrap().0;

            {
                let bounds = RowsFrameBounds {
                    start: Preceding(1),
                    end: CurrentRow,
                };
                let first_curr_key =
                    find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
                let last_curr_key =
                    find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
                assert_cache_key_eq(first_curr_key, 3);
                assert_cache_key_eq(last_curr_key, 8);

                let first_frame_start =
                    find_frame_start_for_rows_frame(&bounds, part_with_delta, first_curr_key);
                let last_frame_end =
                    find_frame_end_for_rows_frame(&bounds, part_with_delta, last_curr_key);
                assert_cache_key_eq(first_frame_start, 2);
                assert_cache_key_eq(last_frame_end, 8);
            }

            {
                let bounds = RowsFrameBounds {
                    start: Preceding(1),
                    end: Following(1),
                };
                let first_curr_key =
                    find_first_curr_for_rows_frame(&bounds, part_with_delta, delta_first_key);
                let last_curr_key =
                    find_last_curr_for_rows_frame(&bounds, part_with_delta, delta_last_key);
                assert_cache_key_eq(first_curr_key, 2);
                assert_cache_key_eq(last_curr_key, 8);

                let first_frame_start =
                    find_frame_start_for_rows_frame(&bounds, part_with_delta, first_curr_key);
                let last_frame_end =
                    find_frame_end_for_rows_frame(&bounds, part_with_delta, last_curr_key);
                assert_cache_key_eq(first_frame_start, 1);
                assert!(last_frame_end.is_largest());
            }
        }
    }

    mod range_frame_tests {
        use risingwave_common::types::{DataType, Interval, data_types};
        use risingwave_common::util::sort_util::OrderType;
        use risingwave_expr::window_function::RangeFrameOffset;

        use super::*;

        fn create_range_frame<T>(
            order_data_type: DataType,
            order_type: OrderType,
            start: FrameBound<T>,
            end: FrameBound<T>,
        ) -> RangeFrameBounds
        where
            T: Into<ScalarImpl>,
        {
            let offset_data_type = match &order_data_type {
                t @ data_types::range_frame_numeric!() => t.clone(),
                data_types::range_frame_datetime!() => DataType::Interval,
                _ => unreachable!(),
            };

            let map_fn = |x: T| {
                RangeFrameOffset::new_for_test(x.into(), &order_data_type, &offset_data_type)
            };
            let start = start.map(map_fn);
            let end = end.map(map_fn);

            RangeFrameBounds {
                order_data_type,
                order_type,
                offset_data_type,
                start,
                end,
            }
        }

        #[test]
        fn test_calc_logical_for_int64_asc() {
            let order_data_type = DataType::Int64;
            let order_type = OrderType::ascending();

            let range_frames = [
                create_range_frame(
                    order_data_type.clone(),
                    order_type,
                    Preceding(3i64),
                    Preceding(2i64),
                ),
                create_range_frame(
                    order_data_type.clone(),
                    order_type,
                    Preceding(1i64),
                    Following(2i64),
                ),
            ];

            let ord_key_1 = StateKey {
                order_key: memcmp_encoding::encode_value(Some(ScalarImpl::Int64(1)), order_type)
                    .unwrap(),
                pk: OwnedRow::empty().into(),
            };
            let ord_key_2 = StateKey {
                order_key: memcmp_encoding::encode_value(Some(ScalarImpl::Int64(3)), order_type)
                    .unwrap(),
                pk: OwnedRow::empty().into(),
            };

            let (logical_first_curr, logical_last_curr) =
                calc_logical_curr_for_range_frames(&range_frames, &ord_key_1, &ord_key_2).unwrap();
            assert_eq!(
                logical_first_curr.as_normal_expect(),
                &Some(ScalarImpl::Int64(-1))
            );
            assert_eq!(
                logical_last_curr.as_normal_expect(),
                &Some(ScalarImpl::Int64(6))
            );

            let (first_start, last_end) =
                calc_logical_boundary_for_range_frames(&range_frames, &ord_key_1, &ord_key_2)
                    .unwrap();
            assert_eq!(first_start.as_normal_expect(), &Some(ScalarImpl::Int64(-2)));
            assert_eq!(last_end.as_normal_expect(), &Some(ScalarImpl::Int64(5)));
        }

        #[test]
        fn test_calc_logical_for_timestamp_desc_nulls_first() {
            let order_data_type = DataType::Timestamp;
            let order_type = OrderType::descending_nulls_first();

            let range_frames = [create_range_frame(
                order_data_type.clone(),
                order_type,
                Preceding(Interval::from_month_day_usec(1, 2, 3 * 1000 * 1000)),
                Following(Interval::from_month_day_usec(0, 1, 0)),
            )];

            let ord_key_1 = StateKey {
                order_key: memcmp_encoding::encode_value(
                    Some(ScalarImpl::Timestamp(
                        "2024-01-28 00:30:00".parse().unwrap(),
                    )),
                    order_type,
                )
                .unwrap(),
                pk: OwnedRow::empty().into(),
            };
            let ord_key_2 = StateKey {
                order_key: memcmp_encoding::encode_value(
                    Some(ScalarImpl::Timestamp(
                        "2024-01-26 15:47:00".parse().unwrap(),
                    )),
                    order_type,
                )
                .unwrap(),
                pk: OwnedRow::empty().into(),
            };

            let (logical_first_curr, logical_last_curr) =
                calc_logical_curr_for_range_frames(&range_frames, &ord_key_1, &ord_key_2).unwrap();
            assert_eq!(
                logical_first_curr.as_normal_expect(),
                &Some(ScalarImpl::Timestamp(
                    "2024-01-29 00:30:00".parse().unwrap()
                ))
            );
            assert_eq!(
                logical_last_curr.as_normal_expect(),
                &Some(ScalarImpl::Timestamp(
                    "2023-12-24 15:46:57".parse().unwrap()
                ))
            );

            let (first_start, last_end) =
                calc_logical_boundary_for_range_frames(&range_frames, &ord_key_1, &ord_key_2)
                    .unwrap();
            assert_eq!(
                first_start.as_normal_expect(),
                &Some(ScalarImpl::Timestamp(
                    "2024-03-01 00:30:03".parse().unwrap()
                ))
            );
            assert_eq!(
                last_end.as_normal_expect(),
                &Some(ScalarImpl::Timestamp(
                    "2024-01-25 15:47:00".parse().unwrap()
                ))
            );
        }

        fn assert_find_left_right_result_eq(
            order_data_type: DataType,
            order_type: OrderType,
            part_with_delta: DeltaBTreeMap<'_, CacheKey, OwnedRow>,
            logical_order_value: ScalarImpl,
            expected_left: Sentinelled<ScalarImpl>,
            expected_right: Sentinelled<ScalarImpl>,
        ) {
            let range_frames = if matches!(order_data_type, DataType::Int32) {
                [create_range_frame(
                    order_data_type.clone(),
                    order_type,
                    Preceding(0), // this doesn't matter here
                    Following(0), // this doesn't matter here
                )]
            } else {
                panic!()
            };
            let logical_order_value = Some(logical_order_value);
            let cache_key_pk_len = 1;

            let find_left_res = find_left_for_range_frames(
                &range_frames,
                part_with_delta,
                &logical_order_value,
                cache_key_pk_len,
            )
            .clone();
            assert_eq!(
                find_left_res.map(|x| x.pk.0.into_iter().next().unwrap().unwrap()),
                expected_left
            );

            let find_right_res = find_right_for_range_frames(
                &range_frames,
                part_with_delta,
                &logical_order_value,
                cache_key_pk_len,
            )
            .clone();
            assert_eq!(
                find_right_res.map(|x| x.pk.0.into_iter().next().unwrap().unwrap()),
                expected_right
            );
        }

        #[test]
        fn test_insert_delta_only() {
            let order_data_type = DataType::Int32;
            let order_type = OrderType::ascending();

            let cache = create_cache!();
            let delta = create_delta!(
                order_type,
                [
                    (1, 1, Insert),
                    (1, 11, Insert),
                    (3, 3, Insert),
                    (5, 5, Insert)
                ]
            );
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(2),
                ScalarImpl::from(3).into(),
                ScalarImpl::from(11).into(),
            );

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(5),
                ScalarImpl::from(5).into(),
                ScalarImpl::from(5).into(),
            );

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(6),
                ScalarImpl::from(5).into(),
                ScalarImpl::from(5).into(),
            );
        }

        #[test]
        fn test_simple() {
            let order_data_type = DataType::Int32;
            let order_type = OrderType::ascending();

            let cache = create_cache!(order_type, [(2, 2), (3, 3), (4, 4), (5, 5), (6, 6)]);
            let delta = create_delta!(
                order_type,
                [(2, 2, Insert), (2, 22, Insert), (3, 3, Delete)]
            );
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(0),
                ScalarImpl::from(2).into(),
                ScalarImpl::from(2).into(),
            );

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(2),
                ScalarImpl::from(2).into(),
                ScalarImpl::from(22).into(),
            );

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(3),
                ScalarImpl::from(4).into(),
                ScalarImpl::from(22).into(),
            );
        }

        #[test]
        fn test_empty_with_sentinels() {
            let order_data_type = DataType::Int32;
            let order_type = OrderType::ascending();

            let cache = create_cache!(order_type, [..., , ...]);
            let delta = create_delta!(order_type, [(1, 1, Insert), (2, 2, Insert)]);
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(0),
                Sentinelled::Smallest,
                Sentinelled::Smallest,
            );

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(1),
                Sentinelled::Smallest,
                ScalarImpl::from(1).into(),
            );

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(2),
                ScalarImpl::from(2).into(),
                Sentinelled::Largest,
            );

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(3),
                Sentinelled::Largest,
                Sentinelled::Largest,
            );
        }

        #[test]
        fn test_with_left_sentinels() {
            let order_data_type = DataType::Int32;
            let order_type = OrderType::ascending();

            let cache = create_cache!(order_type, [..., (2, 2), (4, 4), (5, 5)]);
            let delta = create_delta!(
                order_type,
                [
                    (1, 1, Insert),
                    (2, 2, Insert),
                    (4, 44, Insert),
                    (5, 5, Delete)
                ]
            );
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(1),
                Sentinelled::Smallest,
                ScalarImpl::from(1).into(),
            );

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(4),
                ScalarImpl::from(4).into(),
                ScalarImpl::from(44).into(),
            );

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(5),
                ScalarImpl::from(44).into(),
                ScalarImpl::from(44).into(),
            );
        }

        #[test]
        fn test_with_right_sentinel() {
            let order_data_type = DataType::Int32;
            let order_type = OrderType::ascending();

            let cache = create_cache!(order_type, [(2, 2), (4, 4), (5, 5), ...]);
            let delta = create_delta!(
                order_type,
                [
                    (1, 1, Insert),
                    (2, 2, Insert),
                    (4, 44, Insert),
                    (5, 5, Delete)
                ]
            );
            let part_with_delta = DeltaBTreeMap::new(&cache, &delta);

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(1),
                ScalarImpl::from(1).into(),
                ScalarImpl::from(1).into(),
            );

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(4),
                ScalarImpl::from(4).into(),
                Sentinelled::Largest,
            );

            assert_find_left_right_result_eq(
                order_data_type.clone(),
                order_type,
                part_with_delta,
                ScalarImpl::from(5),
                Sentinelled::Largest,
                Sentinelled::Largest,
            );
        }
    }
}
