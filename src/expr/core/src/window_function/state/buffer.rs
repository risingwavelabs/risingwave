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

use std::collections::VecDeque;
use std::ops::Range;

use educe::Educe;
use risingwave_common::array::Op;

use super::range_utils::range_except;
use crate::window_function::state::range_utils::range_diff;
use crate::window_function::{FrameExclusion, RowsFrameBounds};

/// A common sliding window buffer.
pub(super) struct WindowBuffer<W: WindowImpl> {
    window_impl: W,
    frame_exclusion: FrameExclusion,
    buffer: VecDeque<Entry<W::Key, W::Value>>,
    curr_idx: usize,
    left_idx: usize,       // inclusive, note this can be > `curr_idx`
    right_excl_idx: usize, // exclusive, note this can be <= `curr_idx`
    curr_delta: Option<Vec<(Op, W::Value)>>,
}

struct Entry<K: Ord, V> {
    key: K,
    value: V,
}

/// Note: A window frame can be pure preceding, pure following, or acrossing the _current row_.
pub(super) struct CurrWindow<'a, K> {
    pub key: Option<&'a K>,

    // XXX(rc): Maybe will be used in the future, let's keep it for now.
    #[cfg_attr(not(test), expect(dead_code))]
    /// The preceding half, if any, of the current window is saturated.
    pub preceding_saturated: bool,
    /// The following half, if any, of the current window is saturated.
    pub following_saturated: bool,
}

impl<W: WindowImpl> WindowBuffer<W> {
    pub fn new(window_impl: W, frame_exclusion: FrameExclusion, enable_delta: bool) -> Self {
        if enable_delta {
            // TODO(rc): currently only support `FrameExclusion::NoOthers` for delta
            assert!(frame_exclusion.is_no_others());
        }

        Self {
            window_impl,
            frame_exclusion,
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

    /// Get the smallest key that is still kept in the buffer.
    /// Returns `None` if there's nothing yet.
    pub fn smallest_key(&self) -> Option<&W::Key> {
        self.buffer.front().map(|Entry { key, .. }| key)
    }

    /// Get the key part of the current row.
    pub fn curr_key(&self) -> Option<&W::Key> {
        self.buffer.get(self.curr_idx).map(|Entry { key, .. }| key)
    }

    /// Get the current window info.
    pub fn curr_window(&self) -> CurrWindow<'_, W::Key> {
        let buffer_ref = BufferRef {
            buffer: &self.buffer,
            curr_idx: self.curr_idx,
            left_idx: self.left_idx,
            right_excl_idx: self.right_excl_idx,
        };
        CurrWindow {
            key: self.curr_key(),
            preceding_saturated: self.window_impl.preceding_saturated(buffer_ref),
            following_saturated: self.window_impl.following_saturated(buffer_ref),
        }
    }

    fn curr_window_outer(&self) -> Range<usize> {
        self.left_idx..self.right_excl_idx
    }

    fn curr_window_exclusion(&self) -> Range<usize> {
        // TODO(rc): should intersect with `curr_window_outer` to be more accurate
        match self.frame_exclusion {
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
    pub fn curr_window_values(&self) -> impl Iterator<Item = &W::Value> {
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
    pub fn consume_curr_window_values_delta(
        &mut self,
    ) -> impl Iterator<Item = (Op, W::Value)> + '_ {
        self.curr_delta
            .as_mut()
            .expect("delta mode should be enabled")
            .drain(..)
    }

    /// Append a key-value pair to the buffer.
    pub fn append(&mut self, key: W::Key, value: W::Value) {
        let old_outer = self.curr_window_outer();

        self.buffer.push_back(Entry { key, value });
        self.recalculate_left_right();

        if self.curr_delta.is_some() {
            self.maintain_delta(old_outer, self.curr_window_outer());
        }
    }

    /// Slide the current window forward.
    /// Returns the keys that are removed from the buffer.
    pub fn slide(&mut self) -> impl Iterator<Item = (W::Key, W::Value)> + '_ {
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

    fn maintain_delta(&mut self, old_outer: Range<usize>, new_outer: Range<usize>) {
        debug_assert!(self.frame_exclusion.is_no_others());

        let (outer_removed, outer_added) = range_diff(old_outer.clone(), new_outer.clone());
        let delta = self.curr_delta.as_mut().unwrap();
        for idx in outer_removed.iter().cloned().flatten() {
            delta.push((Op::Delete, self.buffer[idx].value.clone()));
        }
        for idx in outer_added.iter().cloned().flatten() {
            delta.push((Op::Insert, self.buffer[idx].value.clone()));
        }
    }

    fn recalculate_left_right(&mut self) {
        let buffer_ref = BufferRefMut {
            buffer: &self.buffer,
            curr_idx: &mut self.curr_idx,
            left_idx: &mut self.left_idx,
            right_excl_idx: &mut self.right_excl_idx,
        };
        self.window_impl.recalculate_left_right(buffer_ref);
    }
}

#[derive(Educe)]
#[educe(Clone, Copy)]
pub(super) struct BufferRef<'a, K: Ord, V: Clone> {
    buffer: &'a VecDeque<Entry<K, V>>,
    curr_idx: usize,
    left_idx: usize,
    right_excl_idx: usize,
}

pub(super) struct BufferRefMut<'a, K: Ord, V: Clone> {
    buffer: &'a VecDeque<Entry<K, V>>,
    curr_idx: &'a mut usize,
    left_idx: &'a mut usize,
    right_excl_idx: &'a mut usize,
}

pub(super) trait WindowImpl {
    type Key: Ord;
    type Value: Clone;

    fn preceding_saturated(&self, buffer_ref: BufferRef<'_, Self::Key, Self::Value>) -> bool;
    fn following_saturated(&self, buffer_ref: BufferRef<'_, Self::Key, Self::Value>) -> bool;
    fn recalculate_left_right(&self, buffer_ref: BufferRefMut<'_, Self::Key, Self::Value>);
}

pub(super) struct RowsWindow<K: Ord, V: Clone> {
    frame_bounds: RowsFrameBounds,
    _phantom: std::marker::PhantomData<K>,
    _phantom2: std::marker::PhantomData<V>,
}

impl<K: Ord, V: Clone> RowsWindow<K, V> {
    pub fn new(frame_bounds: RowsFrameBounds) -> Self {
        Self {
            frame_bounds,
            _phantom: std::marker::PhantomData,
            _phantom2: std::marker::PhantomData,
        }
    }
}

impl<K: Ord, V: Clone> WindowImpl for RowsWindow<K, V> {
    type Key = K;
    type Value = V;

    fn preceding_saturated(&self, buffer_ref: BufferRef<'_, Self::Key, Self::Value>) -> bool {
        buffer_ref.curr_idx < buffer_ref.buffer.len() && {
            let start_off = self.frame_bounds.start.to_offset();
            if let Some(start_off) = start_off {
                if start_off >= 0 {
                    true // pure following frame, always preceding-saturated
                } else {
                    // FIXME(rc): Clippy rule `clippy::nonminimal_bool` is misreporting that
                    // the following can be simplified.
                    #[allow(clippy::nonminimal_bool)]
                    {
                        assert!(buffer_ref.curr_idx >= buffer_ref.left_idx);
                    }
                    buffer_ref.curr_idx - buffer_ref.left_idx >= start_off.unsigned_abs()
                }
            } else {
                false // unbounded frame start, never preceding-saturated
            }
        }
    }

    fn following_saturated(&self, buffer_ref: BufferRef<'_, Self::Key, Self::Value>) -> bool {
        buffer_ref.curr_idx < buffer_ref.buffer.len() && {
            let end_off = self.frame_bounds.end.to_offset();
            if let Some(end_off) = end_off {
                if end_off <= 0 {
                    true // pure preceding frame, always following-saturated
                } else {
                    // FIXME(rc): Ditto.
                    #[allow(clippy::nonminimal_bool)]
                    {
                        assert!(buffer_ref.right_excl_idx > 0);
                        assert!(buffer_ref.right_excl_idx > buffer_ref.curr_idx);
                        assert!(buffer_ref.right_excl_idx <= buffer_ref.buffer.len());
                    }
                    buffer_ref.right_excl_idx - 1 - buffer_ref.curr_idx >= end_off as usize
                }
            } else {
                false // unbounded frame end, never following-saturated
            }
        }
    }

    fn recalculate_left_right(&self, buffer_ref: BufferRefMut<'_, Self::Key, Self::Value>) {
        if buffer_ref.buffer.is_empty() {
            *buffer_ref.left_idx = 0;
            *buffer_ref.right_excl_idx = 0;
        }

        let start_off = self.frame_bounds.start.to_offset();
        let end_off = self.frame_bounds.end.to_offset();
        if let Some(start_off) = start_off {
            let logical_left_idx = *buffer_ref.curr_idx as isize + start_off;
            if logical_left_idx >= 0 {
                *buffer_ref.left_idx =
                    std::cmp::min(logical_left_idx as usize, buffer_ref.buffer.len());
            } else {
                *buffer_ref.left_idx = 0;
            }
        } else {
            // unbounded start
            *buffer_ref.left_idx = 0;
        }
        if let Some(end_off) = end_off {
            let logical_right_excl_idx = *buffer_ref.curr_idx as isize + end_off + 1;
            if logical_right_excl_idx >= 0 {
                *buffer_ref.right_excl_idx =
                    std::cmp::min(logical_right_excl_idx as usize, buffer_ref.buffer.len());
            } else {
                *buffer_ref.right_excl_idx = 0;
            }
        } else {
            // unbounded end
            *buffer_ref.right_excl_idx = buffer_ref.buffer.len();
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::window_function::FrameBound::{
        CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding,
    };

    #[test]
    fn test_rows_frame_unbounded_preceding_to_current_row() {
        let mut buffer = WindowBuffer::<RowsWindow<_, _>>::new(
            RowsWindow::new(RowsFrameBounds {
                start: UnboundedPreceding,
                end: CurrentRow,
            }),
            FrameExclusion::NoOthers,
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
        let mut buffer = WindowBuffer::<RowsWindow<_, _>>::new(
            RowsWindow::new(RowsFrameBounds {
                start: Preceding(1),
                end: CurrentRow,
            }),
            FrameExclusion::NoOthers,
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
        let mut buffer = WindowBuffer::<RowsWindow<_, _>>::new(
            RowsWindow::new(RowsFrameBounds {
                start: Preceding(2),
                end: Preceding(1),
            }),
            FrameExclusion::NoOthers,
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
        let mut buffer = WindowBuffer::<RowsWindow<_, _>>::new(
            RowsWindow::new(RowsFrameBounds {
                start: CurrentRow,
                end: UnboundedFollowing,
            }),
            FrameExclusion::NoOthers,
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
        let mut buffer = WindowBuffer::<RowsWindow<_, _>>::new(
            RowsWindow::new(RowsFrameBounds {
                start: CurrentRow,
                end: Following(1),
            }),
            FrameExclusion::NoOthers,
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
        let mut buffer = WindowBuffer::<RowsWindow<_, _>>::new(
            RowsWindow::new(RowsFrameBounds {
                start: Following(1),
                end: Following(2),
            }),
            FrameExclusion::NoOthers,
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
        let mut buffer = WindowBuffer::<RowsWindow<_, _>>::new(
            RowsWindow::new(RowsFrameBounds {
                start: UnboundedPreceding,
                end: CurrentRow,
            }),
            FrameExclusion::CurrentRow,
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
