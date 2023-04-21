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

use risingwave_expr::function::window::Frame;

/// Actually with `VecDeque` as internal buffer, we don't need split key and value here. Just in
/// case we want to switch to `BTreeMap` later, so that the general version of `OverWindow` executor
/// can reuse this.
struct Entry<K: Ord, V> {
    key: K,
    value: V,
}

pub(super) struct StreamWindowBuffer<K: Ord, V> {
    frame: Frame,
    buffer: VecDeque<Entry<K, V>>,
    curr_idx: usize,
    right_idx: usize, // inclusive, note this can be less than `curr_idx`
}

const LEFT_IDX: usize = 0;

/// Two simple properties of a window frame:
/// 1. If the following half of the window is saturated, the current window is ready.
/// 2. If the preceding half of the window is saturated, the first several entries can be removed
/// when sliding.
///
/// Note: A window frame can be pure preceding, pure following, or across the _current row_.
pub(super) struct CurrWindow<'a, K> {
    pub key: Option<&'a K>,
    pub preceding_saturated: bool,
    pub following_saturated: bool,
}

impl<K> CurrWindow<'_, K> {
    pub fn is_ready(&self) -> bool {
        self.following_saturated
    }
}

impl<K: Ord, V> StreamWindowBuffer<K, V> {
    pub fn new(frame: Frame) -> Self {
        assert!(frame.is_valid());
        let right_idx = match &frame {
            Frame::Rows(start, end) => {
                let start_off = start.to_offset();
                let end_off = end
                    .to_offset()
                    .expect("frame end cannot be unbounded in streaming mode");
                if let Some(start_off) = start_off {
                    assert!(start_off <= end_off);
                }
                if end_off <= 0 {
                    0
                } else {
                    end_off as usize
                }
            }
        };
        Self {
            frame,
            buffer: Default::default(),
            curr_idx: 0,
            right_idx,
        }
    }

    fn preceding_saturated(&self) -> bool {
        self.curr_idx < self.buffer.len()
            && match &self.frame {
                Frame::Rows(start, _) => {
                    let start_off = start.to_offset();
                    if let Some(start_off) = start_off {
                        self.curr_idx - LEFT_IDX >= start_off.unsigned_abs()
                    } else {
                        // unbounded frame start, never be saturated
                        false
                    }
                }
            }
    }

    fn following_saturated(&self) -> bool {
        std::cmp::max(self.right_idx, self.curr_idx) < self.buffer.len()
    }

    /// Append a key value pair to the buffer.
    pub fn append(&mut self, key: K, value: V) {
        self.buffer.push_back(Entry { key, value });
    }

    /// Get the current window info.
    pub fn curr_window(&self) -> CurrWindow<'_, K> {
        CurrWindow {
            key: self.buffer.get(self.curr_idx).map(|Entry { key, .. }| key),
            preceding_saturated: self.preceding_saturated(),
            following_saturated: self.following_saturated(),
        }
    }

    /// Iterate over the values in the current window.
    /// Panics if the current window is not ready.
    pub fn curr_window_values(&self) -> impl ExactSizeIterator<Item = &V> {
        assert!(self.curr_window().is_ready());
        self.buffer
            .range(LEFT_IDX..=self.right_idx)
            .map(|Entry { value, .. }| value)
    }

    /// Get the smallest key that is still kept in the buffer.
    /// Returns `None` if there's nothing yet.
    pub fn smallest_key(&self) -> Option<&K> {
        self.buffer.front().map(|Entry { key, .. }| key)
    }

    /// Slide the current window forward.
    /// Returns the keys that are removed from the buffer.
    pub fn slide(&mut self) -> impl Iterator<Item = K> + '_ {
        let preceding_saturated = self.preceding_saturated();
        let del_range = match &self.frame {
            Frame::Rows(_, _) => {
                if !preceding_saturated {
                    self.curr_idx += 1;
                    self.right_idx += 1;
                    0..0 // nothing can be removed
                } else {
                    0..1 // can delete the first entry
                }
            }
        };
        self.buffer.drain(del_range).map(|Entry { key, .. }| key)
    }
}

#[cfg(test)]
mod tests {
    // TODO(rc): need to add some unit tests
}
