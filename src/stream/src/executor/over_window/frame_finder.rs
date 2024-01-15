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

//! Helper functions for finding affected ranges from over window range cache for a
//! given set of changes (delta).

use std::ops::Bound;

use delta_btree_map::DeltaBTreeMap;
use risingwave_common::row::OwnedRow;
use risingwave_expr::window_function::{FrameBound, RowsFrameBounds};

use super::over_partition::CacheKey;

// -------------------------- ↓ PUBLIC INTERFACE ↓ --------------------------

/// Merge several `ROWS` frames into one super frame. The returned super frame is
/// ensured to not be a pure preceding/following frame, which means that `CURRENT ROW`
/// is always included in the returned frame.
pub(super) fn merge_rows_frames(rows_frames: &[&RowsFrameBounds]) -> RowsFrameBounds {
    if rows_frames.is_empty() {
        // this can simplify our implementation
        return RowsFrameBounds {
            start: FrameBound::CurrentRow,
            end: FrameBound::CurrentRow,
        };
    }

    let none_as_max_cmp = |x: &Option<usize>, y: &Option<usize>| match (x, y) {
        // None should be maximum (unbounded)
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
        .map(|bounds| bounds.start.n_preceding_rows())
        .max_by(none_as_max_cmp)
        .unwrap();
    let end = rows_frames
        .iter()
        .map(|bounds| bounds.end.n_following_rows())
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

/// For a `ROWS` frame, given a key in delta, find the cache key corresponding to
/// the CURRENT ROW of the first frame that contains the given key.
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
    frame_bounds: &'_ RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    delta_key: &'cache CacheKey,
) -> &'cache CacheKey {
    find_curr_for_rows_frame::<true>(frame_bounds, part_with_delta, delta_key)
}

/// For a `ROWS` frame, given a key in delta, find the cache key corresponding to
/// the CURRENT ROW of the last frame that contains the given key.
///
/// This is the symmetric function of [`find_first_curr_for_rows_frame`].
pub(super) fn find_last_curr_for_rows_frame<'cache>(
    frame_bounds: &'_ RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    delta_key: &'cache CacheKey,
) -> &'cache CacheKey {
    find_curr_for_rows_frame::<false>(frame_bounds, part_with_delta, delta_key)
}

/// For a `ROWS` frame, given a key in `part_with_delta` corresponding to some
/// CURRENT ROW, find the cache key corresponding to the start row in that frame.
pub(super) fn find_frame_start_for_rows_frame<'cache>(
    frame_bounds: &'_ RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    curr_key: &'cache CacheKey,
) -> &'cache CacheKey {
    find_boundary_for_rows_frame::<true>(frame_bounds, part_with_delta, curr_key)
}

/// For a `ROWS` frame, given a key in `part_with_delta` corresponding to some
/// CURRENT ROW, find the cache key corresponding to the end row in that frame.
///
/// This is the symmetric function of [`find_frame_start_for_rows_frame`].
pub(super) fn find_frame_end_for_rows_frame<'cache>(
    frame_bounds: &'_ RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    curr_key: &'cache CacheKey,
) -> &'cache CacheKey {
    find_boundary_for_rows_frame::<false>(frame_bounds, part_with_delta, curr_key)
}

// -------------------------- ↑ PUBLIC INTERFACE ↑ --------------------------

fn find_curr_for_rows_frame<'cache, const LEFT: bool>(
    frame_bounds: &'_ RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    delta_key: &'cache CacheKey,
) -> &'cache CacheKey {
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
    let n_rows_to_move = if LEFT {
        frame_bounds.end.n_following_rows().unwrap()
    } else {
        frame_bounds.start.n_preceding_rows().unwrap()
    };

    if n_rows_to_move == 0 {
        return cursor
            .key()
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
        // Note that we have to move before check, to handle situation where the
        // cursor is at ghost position at first.
        if LEFT {
            cursor.move_prev();
        } else {
            cursor.move_next();
        }
        if cursor.position().is_ghost() {
            break;
        }
    }
    cursor
        .key()
        .or_else(|| {
            // Note the difference between this with the `n_rows_to_move == 0` case.
            if LEFT {
                part_with_delta.first_key()
            } else {
                part_with_delta.last_key()
            }
        })
        .unwrap()
}

fn find_boundary_for_rows_frame<'cache, const LEFT: bool>(
    frame_bounds: &'_ RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    curr_key: &'cache CacheKey,
) -> &'cache CacheKey {
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

    let mut cursor = part_with_delta.find(curr_key).unwrap();
    assert!(!cursor.position().is_ghost());

    let n_rows_to_move = if LEFT {
        frame_bounds.start.n_preceding_rows().unwrap()
    } else {
        frame_bounds.end.n_following_rows().unwrap()
    };

    for _ in 0..n_rows_to_move {
        if LEFT {
            cursor.move_prev();
        } else {
            cursor.move_next();
        }
        if cursor.position().is_ghost() {
            break;
        }
    }
    cursor
        .key()
        .or_else(|| {
            if LEFT {
                part_with_delta.first_key()
            } else {
                part_with_delta.last_key()
            }
        })
        .unwrap()
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_find_first_curr_for_rows_frame() {
        // TODO(): need some ut
    }

    #[test]
    fn test_find_frame_start_for_rows_frame() {
        // TODO(): need some ut
    }
}
