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

use std::collections::VecDeque;
use std::ops::Range;

use educe::Educe;
use risingwave_common::array::Op;
use risingwave_common::types::Sentinelled;
use risingwave_common::util::memcmp_encoding::{self, MemcmpEncoded};
use risingwave_expr::window_function::{
    FrameExclusion, RangeFrameBounds, RowsFrameBounds, SessionFrameBounds, StateKey,
};

use super::range_utils::{range_diff, range_except};

/// A common sliding window buffer.
pub(super) struct WindowBuffer<W: WindowImpl> {
    window_impl: W,
    frame_exclusion: FrameExclusion,
    buffer: VecDeque<Entry<W::Key, W::Value>>,
    curr_idx: usize,
    left_idx: usize,       // inclusive, note this can be > `curr_idx`
    right_excl_idx: usize, // exclusive, note this can be <= `curr_idx` and even <= `left_idx`
    curr_delta: Option<Vec<(Op, W::Value)>>,
}

/// A key-value pair in the buffer.
struct Entry<K: Ord, V> {
    key: K,
    value: V,
}

/// Note: A window frame can be pure preceding, pure following, or acrossing the _current row_.
pub(super) struct CurrWindow<'a, K> {
    pub key: Option<&'a K>,

    // XXX(rc): Maybe will be used in the future, let's keep it for now.
    #[cfg_attr(not(test), expect(dead_code))]
    /// The preceding half of the current window is saturated.
    pub preceding_saturated: bool,
    /// The following half of the current window is saturated.
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
        let window = BufferRef {
            buffer: &self.buffer,
            curr_idx: self.curr_idx,
            left_idx: self.left_idx,
            right_excl_idx: self.right_excl_idx,
        };
        CurrWindow {
            key: self.curr_key(),
            preceding_saturated: self.window_impl.preceding_saturated(window),
            following_saturated: self.window_impl.following_saturated(window),
        }
    }

    fn curr_window_outer(&self) -> Range<usize> {
        self.left_idx..std::cmp::max(self.right_excl_idx, self.left_idx)
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
    pub fn curr_window_values(&self) -> impl DoubleEndedIterator<Item = &W::Value> {
        assert!(self.left_idx <= self.buffer.len()); // `left_idx` can be the same as `buffer.len()` when the buffer is empty
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
        self.recalculate_left_right(RecalculateHint::Append);

        if self.curr_delta.is_some() {
            self.maintain_delta(old_outer, self.curr_window_outer());
        }
    }

    /// Slide the current window forward.
    /// Returns the keys that are removed from the buffer.
    pub fn slide(&mut self) -> impl Iterator<Item = (W::Key, W::Value)> + '_ {
        let old_outer = self.curr_window_outer();

        self.curr_idx += 1;
        self.recalculate_left_right(RecalculateHint::Slide);

        if self.curr_delta.is_some() {
            self.maintain_delta(old_outer, self.curr_window_outer());
        }

        let min_needed_idx = [self.left_idx, self.curr_idx, self.right_excl_idx]
            .iter()
            .min()
            .copied()
            .unwrap();
        self.curr_idx -= min_needed_idx;
        self.left_idx -= min_needed_idx;
        self.right_excl_idx -= min_needed_idx;

        self.window_impl.shift_indices(min_needed_idx);

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

    fn recalculate_left_right(&mut self, hint: RecalculateHint) {
        let window = BufferRefMut {
            buffer: &self.buffer,
            curr_idx: &mut self.curr_idx,
            left_idx: &mut self.left_idx,
            right_excl_idx: &mut self.right_excl_idx,
        };
        self.window_impl.recalculate_left_right(window, hint);
    }
}

/// Wraps a reference to the buffer and some indices, to be used by [`WindowImpl`]s.
#[derive(Educe)]
#[educe(Clone, Copy)]
pub(super) struct BufferRef<'a, K: Ord, V: Clone> {
    buffer: &'a VecDeque<Entry<K, V>>,
    curr_idx: usize,
    left_idx: usize,
    right_excl_idx: usize,
}

/// Wraps a reference to the buffer and some mutable indices, to be used by [`WindowImpl`]s.
pub(super) struct BufferRefMut<'a, K: Ord, V: Clone> {
    buffer: &'a VecDeque<Entry<K, V>>,
    curr_idx: &'a mut usize,
    left_idx: &'a mut usize,
    right_excl_idx: &'a mut usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RecalculateHint {
    Append,
    Slide,
}

/// A trait for sliding window implementations. This trait is used by [`WindowBuffer`] to
/// determine the status of current window and how to slide the window.
pub(super) trait WindowImpl {
    type Key: Ord;
    type Value: Clone;

    /// Whether the preceding half of the current window is saturated.
    /// By "saturated" we mean that every row that is possible to be in the preceding half of the
    /// current window is already in the buffer.
    fn preceding_saturated(&self, window: BufferRef<'_, Self::Key, Self::Value>) -> bool;

    /// Whether the following half of the current window is saturated.
    fn following_saturated(&self, window: BufferRef<'_, Self::Key, Self::Value>) -> bool;

    /// Recalculate the left and right indices of the current window, according to the latest
    /// `curr_idx` and the hint.
    fn recalculate_left_right(
        &mut self,
        window: BufferRefMut<'_, Self::Key, Self::Value>,
        hint: RecalculateHint,
    );

    /// Shift the indices stored by the [`WindowImpl`] by `n` positions. This should be called
    /// after evicting rows from the buffer.
    fn shift_indices(&mut self, n: usize);
}

/// The sliding window implementation for `ROWS` frames.
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

    fn preceding_saturated(&self, window: BufferRef<'_, Self::Key, Self::Value>) -> bool {
        window.curr_idx < window.buffer.len() && {
            let start_off = self.frame_bounds.start.to_offset();
            if let Some(start_off) = start_off {
                if start_off >= 0 {
                    true // pure following frame, always preceding-saturated
                } else {
                    // FIXME(rc): Clippy rule `clippy::nonminimal_bool` is misreporting that
                    // the following can be simplified.
                    #[allow(clippy::nonminimal_bool)]
                    {
                        assert!(window.curr_idx >= window.left_idx);
                    }
                    window.curr_idx - window.left_idx >= start_off.unsigned_abs()
                }
            } else {
                false // unbounded frame start, never preceding-saturated
            }
        }
    }

    fn following_saturated(&self, window: BufferRef<'_, Self::Key, Self::Value>) -> bool {
        window.curr_idx < window.buffer.len() && {
            let end_off = self.frame_bounds.end.to_offset();
            if let Some(end_off) = end_off {
                if end_off <= 0 {
                    true // pure preceding frame, always following-saturated
                } else {
                    // FIXME(rc): Ditto.
                    #[allow(clippy::nonminimal_bool)]
                    {
                        assert!(window.right_excl_idx > 0);
                        assert!(window.right_excl_idx > window.curr_idx);
                        assert!(window.right_excl_idx <= window.buffer.len());
                    }
                    window.right_excl_idx - 1 - window.curr_idx >= end_off as usize
                }
            } else {
                false // unbounded frame end, never following-saturated
            }
        }
    }

    fn recalculate_left_right(
        &mut self,
        window: BufferRefMut<'_, Self::Key, Self::Value>,
        _hint: RecalculateHint,
    ) {
        if window.buffer.is_empty() {
            *window.left_idx = 0;
            *window.right_excl_idx = 0;
        }

        let start_off = self.frame_bounds.start.to_offset();
        let end_off = self.frame_bounds.end.to_offset();
        if let Some(start_off) = start_off {
            let logical_left_idx = *window.curr_idx as isize + start_off;
            if logical_left_idx >= 0 {
                *window.left_idx = std::cmp::min(logical_left_idx as usize, window.buffer.len());
            } else {
                *window.left_idx = 0;
            }
        } else {
            // unbounded start
            *window.left_idx = 0;
        }
        if let Some(end_off) = end_off {
            let logical_right_excl_idx = *window.curr_idx as isize + end_off + 1;
            if logical_right_excl_idx >= 0 {
                *window.right_excl_idx =
                    std::cmp::min(logical_right_excl_idx as usize, window.buffer.len());
            } else {
                *window.right_excl_idx = 0;
            }
        } else {
            // unbounded end
            *window.right_excl_idx = window.buffer.len();
        }
    }

    fn shift_indices(&mut self, _n: usize) {}
}

/// The sliding window implementation for `RANGE` frames.
pub(super) struct RangeWindow<V: Clone> {
    frame_bounds: RangeFrameBounds,
    _phantom: std::marker::PhantomData<V>,
}

impl<V: Clone> RangeWindow<V> {
    pub fn new(frame_bounds: RangeFrameBounds) -> Self {
        Self {
            frame_bounds,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V: Clone> WindowImpl for RangeWindow<V> {
    type Key = StateKey;
    type Value = V;

    fn preceding_saturated(&self, window: BufferRef<'_, Self::Key, Self::Value>) -> bool {
        window.curr_idx < window.buffer.len() && {
            // XXX(rc): It seems that preceding saturation is not important, may remove later.
            true
        }
    }

    fn following_saturated(&self, window: BufferRef<'_, Self::Key, Self::Value>) -> bool {
        window.curr_idx < window.buffer.len()
            && {
                // Left OK? (note that `left_idx` can be greater than `right_idx`)
                // The following line checks whether the left value is the last one in the buffer.
                // Here we adopt a conservative approach, which means we assume the next future value
                // is likely to be the same as the last value in the current window, in which case
                // we can't say the current window is saturated.
                window.left_idx < window.buffer.len() /* non-zero */ - 1
            }
            && {
                // Right OK? Ditto.
                window.right_excl_idx < window.buffer.len()
            }
    }

    fn recalculate_left_right(
        &mut self,
        window: BufferRefMut<'_, Self::Key, Self::Value>,
        _hint: RecalculateHint,
    ) {
        if window.buffer.is_empty() {
            *window.left_idx = 0;
            *window.right_excl_idx = 0;
        }

        let Some(entry) = window.buffer.get(*window.curr_idx) else {
            // If the current index has been moved to a future position, we can't touch anything
            // because the next coming key may equal to the previous one which means the left and
            // right indices will be the same.
            return;
        };
        let curr_key = &entry.key;

        let curr_order_value = memcmp_encoding::decode_value(
            &self.frame_bounds.order_data_type,
            &curr_key.order_key,
            self.frame_bounds.order_type,
        )
        .expect("no reason to fail here because we just encoded it in memory");

        match self.frame_bounds.frame_start_of(&curr_order_value) {
            Sentinelled::Smallest => {
                // unbounded frame start
                assert_eq!(
                    *window.left_idx, 0,
                    "for unbounded start, left index should always be 0"
                );
            }
            Sentinelled::Normal(value) => {
                // bounded, find the start position
                let value_enc = memcmp_encoding::encode_value(value, self.frame_bounds.order_type)
                    .expect("no reason to fail here");
                *window.left_idx = window
                    .buffer
                    .partition_point(|elem| elem.key.order_key < value_enc);
            }
            Sentinelled::Largest => unreachable!("frame start never be UNBOUNDED FOLLOWING"),
        }

        match self.frame_bounds.frame_end_of(curr_order_value) {
            Sentinelled::Largest => {
                // unbounded frame end
                *window.right_excl_idx = window.buffer.len();
            }
            Sentinelled::Normal(value) => {
                // bounded, find the end position
                let value_enc = memcmp_encoding::encode_value(value, self.frame_bounds.order_type)
                    .expect("no reason to fail here");
                *window.right_excl_idx = window
                    .buffer
                    .partition_point(|elem| elem.key.order_key <= value_enc);
            }
            Sentinelled::Smallest => unreachable!("frame end never be UNBOUNDED PRECEDING"),
        }
    }

    fn shift_indices(&mut self, _n: usize) {}
}

pub(super) struct SessionWindow<V: Clone> {
    frame_bounds: SessionFrameBounds,
    /// The latest session is the rightmost session in the buffer, which is updated during appending.
    latest_session: Option<LatestSession>,
    /// The sizes of recognized but not consumed sessions in the buffer. It's updated during appending.
    /// The first element, if any, should be the size of the "current session window". When sliding,
    /// the front should be popped.
    recognized_session_sizes: VecDeque<usize>,
    _phantom: std::marker::PhantomData<V>,
}

#[derive(Debug)]
struct LatestSession {
    /// The starting index of the latest session.
    start_idx: usize,

    /// Minimal next start means the minimal order value that can start a new session.
    /// If a row has an order value less than this, it should be in the current session.
    minimal_next_start: MemcmpEncoded,
}

impl<V: Clone> SessionWindow<V> {
    pub fn new(frame_bounds: SessionFrameBounds) -> Self {
        Self {
            frame_bounds,
            latest_session: None,
            recognized_session_sizes: Default::default(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<V: Clone> WindowImpl for SessionWindow<V> {
    type Key = StateKey;
    type Value = V;

    fn preceding_saturated(&self, window: BufferRef<'_, Self::Key, Self::Value>) -> bool {
        window.curr_idx < window.buffer.len() && {
            // XXX(rc): It seems that preceding saturation is not important, may remove later.
            true
        }
    }

    fn following_saturated(&self, window: BufferRef<'_, Self::Key, Self::Value>) -> bool {
        window.curr_idx < window.buffer.len() && {
            // For session window, `left_idx` is always smaller than `right_excl_idx`.
            assert!(window.left_idx <= window.curr_idx);
            assert!(window.curr_idx < window.right_excl_idx);

            // The following expression checks whether the current window is the latest session.
            // If it is, we don't think it's saturated because the next row may be still in the
            // same session. Otherwise, we can safely say it's saturated.
            self.latest_session
                .as_ref()
                .is_some_and(|LatestSession { start_idx, .. }| window.left_idx < *start_idx)
        }
    }

    fn recalculate_left_right(
        &mut self,
        window: BufferRefMut<'_, Self::Key, Self::Value>,
        hint: RecalculateHint,
    ) {
        // Terms:
        // - Session: A continuous range of rows among any two of which the difference of order values
        //   is less than the session gap. This is a concept on the whole stream. Sessions are recognized
        //   during appending.
        // - Current window: The range of rows that are represented by the indices in `window`. It is a
        //   status of the `WindowBuffer`. If the current window happens to be the last session in the
        //   buffer, it will be updated during appending. Otherwise it will only be updated during sliding.

        match hint {
            RecalculateHint::Append => {
                assert!(!window.buffer.is_empty()); // because we just appended a row
                let appended_idx = window.buffer.len() - 1;
                let appended_key = &window.buffer[appended_idx].key;

                let minimal_next_start_of_appended = self.frame_bounds.minimal_next_start_of(
                    memcmp_encoding::decode_value(
                        &self.frame_bounds.order_data_type,
                        &appended_key.order_key,
                        self.frame_bounds.order_type,
                    )
                    .expect("no reason to fail here because we just encoded it in memory"),
                );
                let minimal_next_start_enc_of_appended = memcmp_encoding::encode_value(
                    minimal_next_start_of_appended,
                    self.frame_bounds.order_type,
                )
                .expect("no reason to fail here");

                if let Some(&mut LatestSession {
                    ref start_idx,
                    ref mut minimal_next_start,
                }) = self.latest_session.as_mut()
                {
                    if &appended_key.order_key >= minimal_next_start {
                        // the appended row starts a new session
                        self.recognized_session_sizes
                            .push_back(appended_idx - start_idx);
                        self.latest_session = Some(LatestSession {
                            start_idx: appended_idx,
                            minimal_next_start: minimal_next_start_enc_of_appended,
                        });
                        // no need to update the current window because it's now corresponding
                        // to some previous session
                    } else {
                        // the appended row belongs to the latest session
                        *minimal_next_start = minimal_next_start_enc_of_appended;

                        if *start_idx == *window.left_idx {
                            // the current window is the latest session, we should extend it
                            *window.right_excl_idx = appended_idx + 1;
                        }
                    }
                } else {
                    // no session yet, the current window should be empty
                    let left_idx = *window.left_idx;
                    let curr_idx = *window.curr_idx;
                    let old_right_excl_idx = *window.right_excl_idx;
                    assert_eq!(left_idx, curr_idx);
                    assert_eq!(left_idx, old_right_excl_idx);
                    assert_eq!(old_right_excl_idx, window.buffer.len() - 1);

                    // now we put the first row into the current window
                    *window.right_excl_idx = window.buffer.len();

                    // and start to recognize the latest session
                    self.latest_session = Some(LatestSession {
                        start_idx: left_idx,
                        minimal_next_start: minimal_next_start_enc_of_appended,
                    });
                }
            }
            RecalculateHint::Slide => {
                let old_left_idx = *window.left_idx;
                let new_curr_idx = *window.curr_idx;
                let old_right_excl_idx = *window.right_excl_idx;

                if new_curr_idx < old_right_excl_idx {
                    // the current row is still in the current session window, no need to slide
                } else {
                    let old_session_size = self.recognized_session_sizes.pop_front();
                    let next_session_size = self.recognized_session_sizes.front().copied();

                    if let Some(old_session_size) = old_session_size {
                        assert_eq!(old_session_size, old_right_excl_idx - old_left_idx);

                        // slide the window to the next session
                        if let Some(next_session_size) = next_session_size {
                            // the next session is fully recognized, so we know the ending index
                            *window.left_idx = old_right_excl_idx;
                            *window.right_excl_idx = old_right_excl_idx + next_session_size;
                        } else {
                            // the next session is still in recognition, so we end the window at the end of buffer
                            *window.left_idx = old_right_excl_idx;
                            *window.right_excl_idx = window.buffer.len();
                        }
                    } else {
                        // no recognized session yet, meaning the current window is the last session in the buffer
                        assert_eq!(old_right_excl_idx, window.buffer.len());
                        *window.left_idx = old_right_excl_idx;
                        *window.right_excl_idx = old_right_excl_idx;
                        self.latest_session = None;
                    }
                }
            }
        }
    }

    fn shift_indices(&mut self, n: usize) {
        if let Some(LatestSession { start_idx, .. }) = self.latest_session.as_mut() {
            *start_idx -= n;
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::row::OwnedRow;
    use risingwave_common::types::{DataType, ScalarImpl};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_expr::window_function::FrameBound::{
        CurrentRow, Following, Preceding, UnboundedFollowing, UnboundedPreceding,
    };
    use risingwave_expr::window_function::SessionFrameGap;

    use super::*;

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
        assert!(
            buffer
                .curr_window_values()
                .cloned()
                .collect_vec()
                .is_empty()
        );
        buffer.append(2, "world");
        let _ = buffer.slide();
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["hello"]
        );
    }

    #[test]
    fn test_session_frame() {
        let order_data_type = DataType::Int64;
        let order_type = OrderType::ascending();
        let gap_data_type = DataType::Int64;

        let mut buffer = WindowBuffer::<SessionWindow<_>>::new(
            SessionWindow::new(SessionFrameBounds {
                order_data_type: order_data_type.clone(),
                order_type,
                gap_data_type: gap_data_type.clone(),
                gap: SessionFrameGap::new_for_test(
                    ScalarImpl::Int64(5),
                    &order_data_type,
                    &gap_data_type,
                ),
            }),
            FrameExclusion::NoOthers,
            true,
        );

        let key = |key: i64| -> StateKey {
            StateKey {
                order_key: memcmp_encoding::encode_value(Some(ScalarImpl::from(key)), order_type)
                    .unwrap(),
                pk: OwnedRow::empty().into(),
            }
        };

        assert!(buffer.curr_key().is_none());

        buffer.append(key(1), "hello");
        buffer.append(key(3), "session");
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&key(1)));
        assert!(window.preceding_saturated);
        assert!(!window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["hello", "session"]
        );

        buffer.append(key(8), "window"); // start a new session
        let window = buffer.curr_window();
        assert!(window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["hello", "session"]
        );

        buffer.append(key(15), "and");
        buffer.append(key(16), "world");
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["hello", "session"]
        );

        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert!(removed_keys.is_empty());
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&key(3)));
        assert!(window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["hello", "session"]
        );

        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert_eq!(removed_keys, vec![key(1), key(3)]);
        assert_eq!(buffer.smallest_key(), Some(&key(8)));
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&key(8)));
        assert!(window.preceding_saturated);
        assert!(window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["window"]
        );

        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert_eq!(removed_keys, vec![key(8)]);
        assert_eq!(buffer.smallest_key(), Some(&key(15)));
        let window = buffer.curr_window();
        assert_eq!(window.key, Some(&key(15)));
        assert!(window.preceding_saturated);
        assert!(!window.following_saturated);
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["and", "world"]
        );

        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert!(removed_keys.is_empty());
        assert_eq!(buffer.curr_key(), Some(&key(16)));
        assert_eq!(
            buffer.curr_window_values().cloned().collect_vec(),
            vec!["and", "world"]
        );

        let removed_keys = buffer.slide().map(|(k, _)| k).collect_vec();
        assert_eq!(removed_keys, vec![key(15), key(16)]);
        assert!(buffer.curr_key().is_none());
        assert!(
            buffer
                .curr_window_values()
                .cloned()
                .collect_vec()
                .is_empty()
        );
    }
}
