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

struct Entry<K: Ord, V> {
    key: K,
    value: V,
}

pub(super) struct WindowBuffer<K: Ord, V> {
    frame: Frame,
    buffer: VecDeque<Entry<K, V>>,
    curr_idx: usize,
    right_idx: usize, // inclusive, note this can be less than `curr_idx`
}

const LEFT_IDX: usize = 0;

pub(super) struct CurrWindow<'a, K> {
    pub key: Option<&'a K>,
    pub preceding_saturated: bool,
    pub following_saturated: bool,
}

impl<K: Ord, V> WindowBuffer<K, V> {
    pub fn new(frame: Frame) -> Self {
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
                        self.curr_idx - LEFT_IDX >= start_off.abs() as usize
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
    /// Returns true if the key belongs to the current window.
    pub fn append(&mut self, key: K, value: V) -> bool {
        let following_not_saturated = !self.following_saturated();
        self.buffer.push_back(Entry { key, value });
        following_not_saturated
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
    pub fn curr_window_values(&self) -> impl Iterator<Item = &V> {
        self.buffer
            .iter()
            .skip(LEFT_IDX)
            .take(self.right_idx - LEFT_IDX + 1)
            .map(|Entry { value, .. }| value)
    }

    /// Get the left most value of the current window.
    /// Returns `None` if there's nothing yet.
    pub fn curr_window_left(&self) -> Option<(&K, &V)> {
        self.buffer
            .get(LEFT_IDX)
            .map(|Entry { key, value }| (key, value))
    }

    /// Slide the current window forward.
    /// Returns the keys that are removed from the buffer.
    /// Panics if the current window is not following-saturated.
    pub fn slide(&mut self) -> impl Iterator<Item = K> + '_ {
        let preceding_saturated = self.preceding_saturated();
        let del_range = match &self.frame {
            Frame::Rows(_, _) => {
                if !preceding_saturated {
                    self.curr_idx += 1;
                    self.right_idx += 1;
                    0..=0
                } else {
                    0..=1
                }
            }
        };
        self.buffer.drain(del_range).map(|Entry { key, .. }| key)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test() {
        // TODO()
    }
}
