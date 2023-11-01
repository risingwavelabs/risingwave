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

use std::collections::VecDeque;
use std::ops::Range;

use risingwave_common::array::Op;
use smallvec::{smallvec, SmallVec};

use crate::window_function::{Frame, FrameBounds, FrameExclusion};

struct Entry<K: Ord, V> {
    key: K,
    value: V,
}

// TODO(rc): May be a good idea to extract this into a separate crate.
/// A common sliding window buffer.
pub struct WindowBuffer<K: Ord, V: Clone> {
    frame: Frame,
    buffer: VecDeque<Entry<K, V>>,
    curr_idx: usize,
    left_idx: usize,       // inclusive, note this can be > `curr_idx`
    right_excl_idx: usize, // exclusive, note this can be <= `curr_idx`
    curr_delta: Option<Vec<(Op, V)>>,
}

/// Note: A window frame can be pure preceding, pure following, or acrossing the _current row_.
pub struct CurrWindow<'a, K> {
    pub key: Option<&'a K>,
    pub preceding_saturated: bool,
    pub following_saturated: bool,
}

impl<K: Ord, V: Clone> WindowBuffer<K, V> {
    pub fn new(frame: Frame, enable_delta: bool) -> Self {
        assert!(frame.bounds.is_valid());
        if enable_delta {
            // TODO(rc): currently only support `FrameExclusion::NoOthers` for delta
            assert!(frame.exclusion.is_no_others());
        }
        Self {
            frame,
            buffer: Default::default(),
            curr_idx: 0,
            left_idx: 0,
            right_excl_idx: 0,
            curr_delta: if enable_delta {
                Some(Default::default())
            } else {
                None
            },
        }
    }

    fn preceding_saturated(&self) -> bool {
        self.curr_key().is_some()
            && match &self.frame.bounds {
                FrameBounds::Rows(start, _) => {
                    let start_off = start.to_offset();
                    if let Some(start_off) = start_off {
                        if start_off >= 0 {
                            true // pure following frame, always preceding-saturated
                        } else {
                            // FIXME(rc): Clippy rule `clippy::nonminimal_bool` is misreporting that
                            // the following can be simplified.
                            #[allow(clippy::nonminimal_bool)]
                            {
                                assert!(self.curr_idx >= self.left_idx);
                            }
                            self.curr_idx - self.left_idx >= start_off.unsigned_abs()
                        }
                    } else {
                        false // unbounded frame start, never preceding-saturated
                    }
                }
            }
    }

    fn following_saturated(&self) -> bool {
        self.curr_key().is_some()
            && match &self.frame.bounds {
                FrameBounds::Rows(_, end) => {
                    let end_off = end.to_offset();
                    if let Some(end_off) = end_off {
                        if end_off <= 0 {
                            true // pure preceding frame, always following-saturated
                        } else {
                            // FIXME(rc): Ditto.
                            #[allow(clippy::nonminimal_bool)]
                            {
                                assert!(self.right_excl_idx > 0);
                                assert!(self.right_excl_idx > self.curr_idx);
                                assert!(self.right_excl_idx <= self.buffer.len());
                            }
                            self.right_excl_idx - 1 - self.curr_idx >= end_off as usize
                        }
                    } else {
                        false // unbounded frame end, never following-saturated
                    }
                }
            }
    }

    /// Get the key part of the current row.
    pub fn curr_key(&self) -> Option<&K> {
        self.buffer.get(self.curr_idx).map(|Entry { key, .. }| key)
    }

    /// Get the current window info.
    pub fn curr_window(&self) -> CurrWindow<'_, K> {
        CurrWindow {
            key: self.curr_key(),
            preceding_saturated: self.preceding_saturated(),
            following_saturated: self.following_saturated(),
        }
    }

    fn curr_window_outer(&self) -> Range<usize> {
        self.left_idx..self.right_excl_idx
    }

    fn curr_window_exclusion(&self) -> Range<usize> {
        // TODO(rc): should intersect with `curr_window_outer` to be more accurate
        match self.frame.exclusion {
            FrameExclusion::CurrentRow => self.curr_idx..self.curr_idx + 1,
            FrameExclusion::NoOthers => self.curr_idx..self.curr_idx,
        }
    }

    fn curr_window_ranges(&self) -> (Range<usize>, Range<usize>) {
        let selection = self.curr_window_outer();
        let exclusion = self.curr_window_exclusion();
        range_except(selection, exclusion)
    }

    /// Iterate over values in the current window.
    pub fn curr_window_values(&self) -> impl Iterator<Item = &V> {
        assert!(self.left_idx <= self.right_excl_idx);
        assert!(self.right_excl_idx <= self.buffer.len());

        let (left, right) = self.curr_window_ranges();
        self.buffer
            .range(left)
            .chain(self.buffer.range(right))
            .map(|Entry { value, .. }| value)
    }

    /// Consume the delta of values comparing the current window to the previous window.
    /// The delta is not guaranteed to be sorted, especially when frame exclusion is not `NoOthers`.
    pub fn consume_curr_window_values_delta(&mut self) -> impl Iterator<Item = (Op, V)> + '_ {
        self.curr_delta
            .as_mut()
            .expect("delta mode should be enabled")
            .drain(..)
    }

    fn recalculate_left_right(&mut self) {
        // TODO(rc): For the sake of simplicity, we just recalculate the left and right indices from
        // `curr_idx`, rather than trying to update them incrementally. The complexity is O(n) for
        // `Frame::Range` where n is the length of the buffer, for now it doesn't matter.

        if self.buffer.is_empty() {
            self.left_idx = 0;
            self.right_excl_idx = 0;
        }

        match &self.frame.bounds {
            FrameBounds::Rows(start, end) => {
                let start_off = start.to_offset();
                let end_off = end.to_offset();
                if let Some(start_off) = start_off {
                    let logical_left_idx = self.curr_idx as isize + start_off;
                    if logical_left_idx >= 0 {
                        self.left_idx = std::cmp::min(logical_left_idx as usize, self.buffer.len());
                    } else {
                        self.left_idx = 0;
                    }
                } else {
                    // unbounded start
                    self.left_idx = 0;
                }
                if let Some(end_off) = end_off {
                    let logical_right_excl_idx = self.curr_idx as isize + end_off + 1;
                    if logical_right_excl_idx >= 0 {
                        self.right_excl_idx =
                            std::cmp::min(logical_right_excl_idx as usize, self.buffer.len());
                    } else {
                        self.right_excl_idx = 0;
                    }
                } else {
                    // unbounded end
                    self.right_excl_idx = self.buffer.len();
                }
            }
        }
    }

    fn maintain_delta(&mut self, old_outer: Range<usize>, new_outer: Range<usize>) {
        debug_assert!(self.frame.exclusion.is_no_others());

        let (outer_removed, outer_added) = range_diff(old_outer.clone(), new_outer.clone());
        let delta = self.curr_delta.as_mut().unwrap();
        for idx in outer_removed.iter().cloned().flatten() {
            delta.push((Op::Delete, self.buffer[idx].value.clone()));
        }
        for idx in outer_added.iter().cloned().flatten() {
            delta.push((Op::Insert, self.buffer[idx].value.clone()));
        }
    }

    /// Append a key-value pair to the buffer.
    pub fn append(&mut self, key: K, value: V) {
        let old_outer = self.curr_window_outer();

        self.buffer.push_back(Entry { key, value });
        self.recalculate_left_right();

        if self.curr_delta.is_some() {
            self.maintain_delta(old_outer, self.curr_window_outer());
        }
    }

    /// Get the smallest key that is still kept in the buffer.
    /// Returns `None` if there's nothing yet.
    pub fn smallest_key(&self) -> Option<&K> {
        self.buffer.front().map(|Entry { key, .. }| key)
    }

    /// Slide the current window forward.
    /// Returns the keys that are removed from the buffer.
    pub fn slide(&mut self) -> impl Iterator<Item = (K, V)> + '_ {
        let old_outer = self.curr_window_outer();

        self.curr_idx += 1;
        self.recalculate_left_right();

        if self.curr_delta.is_some() {
            self.maintain_delta(old_outer, self.curr_window_outer());
        }

        let min_needed_idx = std::cmp::min(self.left_idx, self.curr_idx);
        self.curr_idx -= min_needed_idx;
        self.left_idx -= min_needed_idx;
        self.right_excl_idx -= min_needed_idx;
        self.buffer
            .drain(0..min_needed_idx)
            .map(|Entry { key, value }| (key, value))
    }
}

/// Calculate range (A - B), the result might be the union of two ranges when B is totally included
/// in the A.
fn range_except(a: Range<usize>, b: Range<usize>) -> (Range<usize>, Range<usize>) {
    #[allow(clippy::if_same_then_else)] // for better readability
    if a.is_empty() {
        (0..0, 0..0)
    } else if b.is_empty() {
        (a, 0..0)
    } else if a.end <= b.start || b.end <= a.start {
        // a: [   )
        // b:        [   )
        // or
        // a:        [   )
        // b: [   )
        (a, 0..0)
    } else if b.start <= a.start && a.end <= b.end {
        // a:  [   )
        // b: [       )
        (0..0, 0..0)
    } else if a.start < b.start && b.end < a.end {
        // a: [       )
        // b:   [   )
        (a.start..b.start, b.end..a.end)
    } else if a.end <= b.end {
        // a: [   )
        // b:   [   )
        (a.start..b.start, 0..0)
    } else if b.start <= a.start {
        // a:   [   )
        // b: [   )
        (b.end..a.end, 0..0)
    } else {
        unreachable!()
    }
}

/// Calculate the difference of two ranges A and B, return (removed ranges, added ranges).
/// Note this is quite different from [`range_except`].
#[allow(clippy::type_complexity)] // looks complex but it's not
fn range_diff(
    a: Range<usize>,
    b: Range<usize>,
) -> (SmallVec<[Range<usize>; 2]>, SmallVec<[Range<usize>; 2]>) {
    if a.start == b.start {
        match a.end.cmp(&b.end) {
            std::cmp::Ordering::Equal => {
                // a: [   )
                // b: [   )
                (smallvec![], smallvec![])
            }
            std::cmp::Ordering::Less => {
                // a: [   )
                // b: [     )
                (smallvec![], smallvec![a.end..b.end])
            }
            std::cmp::Ordering::Greater => {
                // a: [     )
                // b: [   )
                (smallvec![b.end..a.end], smallvec![])
            }
        }
    } else if a.end == b.end {
        debug_assert!(a.start != b.start);
        if a.start < b.start {
            // a: [     )
            // b:   [   )
            (smallvec![a.start..b.start], smallvec![])
        } else {
            // a:   [   )
            // b: [     )
            (smallvec![], smallvec![b.start..a.start])
        }
    } else {
        debug_assert!(a.start != b.start && a.end != b.end);
        if a.end <= b.start || b.end <= a.start {
            // a: [   )
            // b:     [  [   )
            // or
            // a:       [   )
            // b: [   ) )
            (smallvec![a], smallvec![b])
        } else if b.start < a.start && a.end < b.end {
            // a:  [   )
            // b: [       )
            (smallvec![], smallvec![b.start..a.start, a.end..b.end])
        } else if a.start < b.start && b.end < a.end {
            // a: [       )
            // b:   [   )
            (smallvec![a.start..b.start, b.end..a.end], smallvec![])
        } else if a.end < b.end {
            // a: [   )
            // b:   [   )
            (smallvec![a.start..b.start], smallvec![a.end..b.end])
        } else {
            // a:   [   )
            // b: [   )
            (smallvec![b.end..a.end], smallvec![b.start..a.start])
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use itertools::Itertools;

    use super::*;
    use crate::window_function::{Frame, FrameBound};

    #[test]
    fn test_range_diff() {
        fn test(
            a: Range<usize>,
            b: Range<usize>,
            expected_removed: impl IntoIterator<Item = usize>,
            expected_added: impl IntoIterator<Item = usize>,
        ) {
            let (removed, added) = range_diff(a, b);
            let removed_set = removed.into_iter().flatten().collect::<HashSet<_>>();
            let added_set = added.into_iter().flatten().collect::<HashSet<_>>();
            let expected_removed_set = expected_removed.into_iter().collect::<HashSet<_>>();
            let expected_added_set = expected_added.into_iter().collect::<HashSet<_>>();
            assert_eq!(removed_set, expected_removed_set);
            assert_eq!(added_set, expected_added_set);
        }

        test(0..0, 0..0, [], []);
        test(0..1, 0..1, [], []);
        test(0..1, 0..2, [], [1]);
        test(0..2, 0..1, [1], []);
        test(0..2, 1..2, [0], []);
        test(1..2, 0..2, [], [0]);
        test(0..1, 1..2, [0], [1]);
        test(0..1, 2..3, [0], [2]);
        test(1..2, 0..1, [1], [0]);
        test(2..3, 0..1, [2], [0]);
        test(0..3, 1..2, [0, 2], []);
        test(1..2, 0..3, [], [0, 2]);
        test(0..3, 2..4, [0, 1], [3]);
        test(2..4, 0..3, [3], [0, 1]);
    }

    #[test]
    fn test_rows_frame_unbounded_preceding_to_current_row() {
        let mut buffer = WindowBuffer::new(
            Frame::rows(FrameBound::UnboundedPreceding, FrameBound::CurrentRow),
            true,
        );

        let window = buffer.curr_window();
        assert!(window.key.is_none());
        assert!(!window.preceding_saturated);
        assert!(!window.following_saturated);
        buffer.append(1, "hello");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(!window.preceding_saturated); // unbounded preceding is never saturated
        assert!(window.following_saturated);
        buffer.append(2, "world");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(!window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["hello"]
        );
        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert!(removed_keys.is_empty()); // unbouded preceding, nothing can ever be removed
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&2));
        assert!(!window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(buffer.smallest_key(), Some(&1));
    }

    #[test]
    fn test_rows_frame_preceding_to_current_row() {
        let mut buffer = WindowBuffer::new(
            Frame::rows(FrameBound::Preceding(1), FrameBound::CurrentRow),
            true,
        );

        let window = buffer.curr_window();
        assert!(window.key.is_none());
        assert!(!window.preceding_saturated);
        assert!(!window.following_saturated);
        buffer.append(1, "hello");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(!window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["hello"]
        );
        buffer.append(2, "world");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(!window.preceding_saturated);
        assert!(window.following_saturated);
        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert!(removed_keys.is_empty());
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&2));
        assert!(window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(buffer.smallest_key(), Some(&1));
        buffer.append(3, "!");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&2));
        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert_eq!(removed_keys, vec![1]);
    }

    #[test]
    fn test_rows_frame_preceding_to_preceding() {
        let mut buffer = WindowBuffer::new(
            Frame::rows(FrameBound::Preceding(2), FrameBound::Preceding(1)),
            true,
        );

        buffer.append(1, "RisingWave");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(!window.preceding_saturated);
        assert!(window.following_saturated);
        assert!(buffer.curr_window_values().collect_vec().is_empty());
        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert!(removed_keys.is_empty());
        assert_eq!(buffer.smallest_key(), Some(&1));
        buffer.append(2, "is the best");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&2));
        assert!(!window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["RisingWave"]
        );
        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert!(removed_keys.is_empty());
        assert_eq!(buffer.smallest_key(), Some(&1));
        buffer.append(3, "streaming platform");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&3));
        assert!(window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["RisingWave", "is the best"]
        );
        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert_eq!(removed_keys, vec![1]);
        assert_eq!(buffer.smallest_key(), Some(&2));
    }

    #[test]
    fn test_rows_frame_current_row_to_unbounded_following() {
        let mut buffer = WindowBuffer::new(
            Frame::rows(FrameBound::CurrentRow, FrameBound::UnboundedFollowing),
            true,
        );

        buffer.append(1, "RisingWave");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(window.preceding_saturated);
        assert!(!window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["RisingWave"]
        );
        buffer.append(2, "is the best");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(window.preceding_saturated);
        assert!(!window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["RisingWave", "is the best"]
        );
        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert_eq!(removed_keys, vec![1]);
        assert_eq!(buffer.smallest_key(), Some(&2));
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&2));
        assert!(window.preceding_saturated);
        assert!(!window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["is the best"]
        );
    }

    #[test]
    fn test_rows_frame_current_row_to_following() {
        let mut buffer = WindowBuffer::new(
            Frame::rows(FrameBound::CurrentRow, FrameBound::Following(1)),
            true,
        );

        buffer.append(1, "RisingWave");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(window.preceding_saturated);
        assert!(!window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["RisingWave"]
        );
        buffer.append(2, "is the best");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["RisingWave", "is the best"]
        );
        buffer.append(3, "streaming platform");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["RisingWave", "is the best"]
        );
        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert_eq!(removed_keys, vec![1]);
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&2));
        assert!(window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["is the best", "streaming platform"]
        );
    }

    #[test]
    fn test_rows_frame_following_to_following() {
        let mut buffer = WindowBuffer::new(
            Frame::rows(FrameBound::Following(1), FrameBound::Following(2)),
            true,
        );

        buffer.append(1, "RisingWave");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(window.preceding_saturated);
        assert!(!window.following_saturated);
        assert!(buffer.curr_window_values().collect_vec().is_empty());
        buffer.append(2, "is the best");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(window.preceding_saturated);
        assert!(!window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["is the best"]
        );
        buffer.append(3, "streaming platform");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["is the best", "streaming platform"]
        );
        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert_eq!(removed_keys, vec![1]);
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&2));
        assert!(window.preceding_saturated);
        assert!(!window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["streaming platform"]
        );
    }

    #[test]
    fn test_rows_frame_exclude_current_row() {
        let mut buffer = WindowBuffer::new(
            Frame::rows_with_exclusion(
                FrameBound::UnboundedPreceding,
                FrameBound::CurrentRow,
                FrameExclusion::CurrentRow,
            ),
            false,
        );

        buffer.append(1, "hello");
        assert!(buffer
            .curr_window_values()
            .cloned()
            .collect_vec()
            .is_empty());
        buffer.append(2, "world");
        let _ = buffer.slide();
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["hello"]
        );
    }
}
