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

use risingwave_common::must_match;
use risingwave_common::types::Datum;
use smallvec::SmallVec;

use super::{StateKey, StateOutput, StatePos, WindowState};
use crate::executor::over_window::call::Frame;
use crate::executor::over_window::state::StateEvictHint;

struct BufferEntry(StateKey, Datum);

pub(super) struct LagState {
    offset: usize,
    // TODO(rc): may move buffer maintanence to a common structure.
    buffer: VecDeque<BufferEntry>,
    curr_idx: usize,
}

impl LagState {
    pub fn new(frame: &Frame) -> Self {
        let offset = must_match!(frame, Frame::Offset(offset) if *offset < 0 => -offset as usize);
        Self {
            offset,
            buffer: Default::default(),
            curr_idx: 0,
        }
    }
}

impl WindowState for LagState {
    fn append(&mut self, key: StateKey, args: SmallVec<[Datum; 2]>) {
        self.buffer
            .push_back(BufferEntry(key, args.into_iter().next().unwrap()));
    }

    fn curr_window(&self) -> StatePos<'_> {
        // 2 cases:
        // 1. `curr_index` is in the buffer, then it's ready.
        // 2. `curr_index` is not in the buffer, then it points to the next input row, which
        //    means `curr_index == buffer.len()`.
        let curr_key = self
            .buffer
            .get(self.curr_idx)
            .map(|BufferEntry(key, _)| key);
        StatePos {
            key: curr_key,
            is_ready: curr_key.is_some(),
        }
    }

    fn slide(&mut self) -> StateOutput {
        debug_assert!(self.curr_window().is_ready);
        if self.curr_idx < self.offset {
            // the ready window doesn't have enough preceding rows, just return NULL
            self.curr_idx += 1;
            StateOutput {
                return_value: None,
                evict_hint: StateEvictHint::CannotEvict(self.buffer.front().unwrap().0.clone()),
            }
        } else {
            // in the other case, the first entry in buffer is always the `lag(offset)` row
            assert_eq!(self.curr_idx, self.offset);
            let BufferEntry(key, value) = self.buffer.pop_front().unwrap();
            StateOutput {
                return_value: value,
                evict_hint: StateEvictHint::CanEvict(std::iter::once(key).collect()),
            }
        }
    }
}
