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

use either::Either;
use risingwave_expr::function::window::{Frame, FrameBounds};

/// Actually with `VecDeque` as internal buffer, we don't need split key and value here. Just in
/// case we want to switch to `BTreeMap` later, so that the general version of `OverWindow` executor
/// can reuse this.
struct Entry<K: Ord, V> {
    key: K,
    value: V,
}

// TODO(rc): May be a good idea to extract this into a separate crate.
/// A common sliding window buffer for streaming input.
pub struct StreamWindowBuffer<K: Ord, V> {
    frame: Frame,
    buffer: VecDeque<Entry<K, V>>,
    curr_idx: usize,
    left_idx: usize,       // inclusive, note this can be > `curr_idx`
    right_excl_idx: usize, // exclusive, note this can be <= `curr_idx`
}

/// Two simple properties of a window frame:
/// 1. If the following half of the window is saturated, the current window is ready.
/// 2. If the preceding half of the window is saturated, the first several entries can be removed
/// when sliding.
///
/// Note: A window frame can be pure preceding, pure following, or across the _current row_.
pub struct CurrWindow<'a, K> {
    pub key: Option<&'a K>,
    pub preceding_saturated: bool,
    pub following_saturated: bool,
}

impl<K: Ord, V> StreamWindowBuffer<K, V> {
    pub fn new(frame: Frame) -> Self {
        assert!(frame.bounds.is_valid());
        Self {
            frame,
            buffer: Default::default(),
            curr_idx: 0,
            left_idx: 0,
            right_excl_idx: 0,
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
                            // assert!(self.curr_idx >= self.left_idx);
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
                            // assert!(self.right_excl_idx > 0);
                            // assert!(self.right_excl_idx > self.curr_idx);
                            // assert!(self.right_excl_idx <= self.buffer.len());
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

    /// Iterate over values in the current window.
    pub fn curr_window_values(&self) -> impl ExactSizeIterator<Item = &V> {
        assert!(self.left_idx <= self.right_excl_idx);
        assert!(self.right_excl_idx <= self.buffer.len());
        if self.left_idx == self.right_excl_idx {
            Either::Left(std::iter::empty())
        } else {
            Either::Right(
                self.buffer
                    .range(self.left_idx..self.right_excl_idx)
                    .map(|Entry { value, .. }| value),
            )
        }
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

    /// Append a key-value pair to the buffer.
    pub fn append(&mut self, key: K, value: V) {
        self.buffer.push_back(Entry { key, value });
        self.recalculate_left_right()
    }

    /// Get the smallest key that is still kept in the buffer.
    /// Returns `None` if there's nothing yet.
    pub fn smallest_key(&self) -> Option<&K> {
        self.buffer.front().map(|Entry { key, .. }| key)
    }

    /// Slide the current window forward.
    /// Returns the keys that are removed from the buffer.
    pub fn slide(&mut self) -> impl Iterator<Item = K> + '_ {
        self.curr_idx += 1;
        self.recalculate_left_right();
        let min_needed_idx = std::cmp::min(self.left_idx, self.curr_idx);
        self.curr_idx -= min_needed_idx;
        self.left_idx -= min_needed_idx;
        self.right_excl_idx -= min_needed_idx;
        self.buffer
            .drain(0..min_needed_idx)
            .map(|Entry { key, .. }| key)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_expr::function::window::{Frame, FrameBound};

    use super::*;

    #[test]
    fn test_rows_frame_unbounded_preceding_to_current_row() {
        let mut buffer = StreamWindowBuffer::new(Frame::rows(
            FrameBound::UnboundedPreceding,
            FrameBound::CurrentRow,
        ));

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

        let removed_keys = buffer.slide().collect_vec();
        assert!(removed_keys.is_empty()); // unbouded preceding, nothing can ever be removed
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&2));
        assert!(!window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(buffer.smallest_key(), Some(&1));
    }

    #[test]
    fn test_rows_frame_preceding_to_current_row() {
        let mut buffer = StreamWindowBuffer::new(Frame::rows(
            FrameBound::Preceding(1),
            FrameBound::CurrentRow,
        ));

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

        let removed_keys = buffer.slide().collect_vec();
        assert!(removed_keys.is_empty());
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&2));
        assert!(window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(buffer.smallest_key(), Some(&1));

        buffer.append(3, "!");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&2));
        let removed_keys = buffer.slide().collect_vec();
        assert_eq!(removed_keys, vec![1]);
    }

    #[test]
    fn test_rows_frame_preceding_to_preceding() {
        let mut buffer = StreamWindowBuffer::new(Frame::rows(
            FrameBound::Preceding(2),
            FrameBound::Preceding(1),
        ));

        buffer.append(1, "RisingWave");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&1));
        assert!(!window.preceding_saturated);
        assert!(window.following_saturated);
        assert!(buffer.curr_window_values().collect_vec().is_empty());

        let removed_keys = buffer.slide().collect_vec();
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

        let removed_keys = buffer.slide().collect_vec();
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

        let removed_keys = buffer.slide().collect_vec();
        assert_eq!(removed_keys, vec![1]);
        assert_eq!(buffer.smallest_key(), Some(&2));
    }

    #[test]
    fn test_rows_frame_current_row_to_unbounded_following() {
        let mut buffer = StreamWindowBuffer::new(Frame::rows(
            FrameBound::CurrentRow,
            FrameBound::UnboundedFollowing,
        ));

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

        let removed_keys = buffer.slide().collect_vec();
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
        let mut buffer = StreamWindowBuffer::new(Frame::rows(
            FrameBound::CurrentRow,
            FrameBound::Following(1),
        ));

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

        let removed_keys = buffer.slide().collect_vec();
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
        let mut buffer = StreamWindowBuffer::new(Frame::rows(
            FrameBound::Following(1),
            FrameBound::Following(2),
        ));

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

        let removed_keys = buffer.slide().collect_vec();
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
}
