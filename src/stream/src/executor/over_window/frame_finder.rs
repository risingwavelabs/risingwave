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
/// guaranteed to be *canonical*, which means that the `CURRENT ROW` is always
/// included in the returned frame.
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
        .flat_map(|bounds| [&bounds.start, &bounds.end])
        .map(FrameBound::n_preceding_rows)
        .max_by(none_as_max_cmp)
        .unwrap();
    let end = rows_frames
        .iter()
        .flat_map(|bounds| [&bounds.start, &bounds.end])
        .map(FrameBound::n_following_rows)
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
    frame_bounds: &'_ RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    delta_key: &'cache CacheKey,
) -> &'cache CacheKey {
    find_curr_for_rows_frame::<true>(frame_bounds, part_with_delta, delta_key)
}

/// For a canonical `ROWS` frame, given a key in delta, find the cache key
/// corresponding to the CURRENT ROW of the last frame that contains the given
/// key.
///
/// This is the symmetric function of [`find_first_curr_for_rows_frame`].
pub(super) fn find_last_curr_for_rows_frame<'cache>(
    frame_bounds: &'_ RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    delta_key: &'cache CacheKey,
) -> &'cache CacheKey {
    find_curr_for_rows_frame::<false>(frame_bounds, part_with_delta, delta_key)
}

/// For a canonical `ROWS` frame, given a key in `part_with_delta` corresponding
/// to some CURRENT ROW, find the cache key corresponding to the start row in
/// that frame.
pub(super) fn find_frame_start_for_rows_frame<'cache>(
    frame_bounds: &'_ RowsFrameBounds,
    part_with_delta: DeltaBTreeMap<'cache, CacheKey, OwnedRow>,
    curr_key: &'cache CacheKey,
) -> &'cache CacheKey {
    find_boundary_for_rows_frame::<true>(frame_bounds, part_with_delta, curr_key)
}

/// For a canonical `ROWS` frame, given a key in `part_with_delta` corresponding
/// to some CURRENT ROW, find the cache key corresponding to the end row in that
/// frame.
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
    use risingwave_expr::window_function::{FrameBound, RowsFrameBounds};

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
                start: FrameBound::CurrentRow,
                end: FrameBound::CurrentRow,
            },
        );

        let frames = [
            &RowsFrameBounds {
                start: FrameBound::Preceding(3),
                end: FrameBound::Preceding(2),
            },
            &RowsFrameBounds {
                start: FrameBound::Preceding(1),
                end: FrameBound::Preceding(4),
            },
        ];
        assert_equivalent(
            merge_rows_frames(&frames),
            RowsFrameBounds {
                start: FrameBound::Preceding(4),
                end: FrameBound::CurrentRow,
            },
        );

        let frames = [
            &RowsFrameBounds {
                start: FrameBound::Preceding(3),
                end: FrameBound::Following(2),
            },
            &RowsFrameBounds {
                start: FrameBound::Preceding(2),
                end: FrameBound::Following(3),
            },
        ];
        assert_equivalent(
            merge_rows_frames(&frames),
            RowsFrameBounds {
                start: FrameBound::Preceding(3),
                end: FrameBound::Following(3),
            },
        );

        let frames = [
            &RowsFrameBounds {
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::Following(2),
            },
            &RowsFrameBounds {
                start: FrameBound::Preceding(2),
                end: FrameBound::UnboundedFollowing,
            },
        ];
        assert_equivalent(
            merge_rows_frames(&frames),
            RowsFrameBounds {
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::UnboundedFollowing,
            },
        );

        let frames = [
            &RowsFrameBounds {
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::Following(2),
            },
            &RowsFrameBounds {
                start: FrameBound::Following(5),
                end: FrameBound::Following(2),
            },
        ];
        assert_equivalent(
            merge_rows_frames(&frames),
            RowsFrameBounds {
                start: FrameBound::UnboundedPreceding,
                end: FrameBound::Following(5),
            },
        );
    }

    mod rows_frame_tests {
        use std::collections::BTreeMap;

        use delta_btree_map::Change;
        use risingwave_common::types::{ScalarImpl, Sentinelled};
        use risingwave_expr::window_function::FrameBound::*;
        use risingwave_expr::window_function::StateKey;

        use super::*;

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
            ($(( $pk:literal, $change:ident )),+ $(,)?) => {
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
}
