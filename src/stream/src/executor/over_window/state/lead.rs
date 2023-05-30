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

use risingwave_common::estimate_size::EstimateSize;
use risingwave_common::must_match;
use risingwave_common::types::Datum;
use risingwave_expr::function::window::{Frame, FrameBound, FrameBounds};
use smallvec::SmallVec;

use super::{EstimatedVecDeque, StateKey, StateOutput, StatePos, WindowState};
use crate::executor::over_window::state::StateEvictHint;
use crate::executor::StreamExecutorResult;

struct BufferEntry(StateKey, Datum);

impl EstimateSize for BufferEntry {
    fn estimated_heap_size(&self) -> usize {
        self.0.estimated_heap_size() + self.1.estimated_heap_size()
    }
}

#[derive(EstimateSize)]
pub(super) struct LeadState {
    offset: usize,
    buffer: EstimatedVecDeque<BufferEntry>,
}

impl LeadState {
    pub fn new(frame: &Frame) -> Self {
        let offset = must_match!(&frame.bounds, FrameBounds::Rows(FrameBound::CurrentRow, FrameBound::Following(offset)) => *offset);
        Self {
            offset,
            buffer: Default::default(),
        }
    }
}

impl WindowState for LeadState {
    fn append(&mut self, key: StateKey, args: SmallVec<[Datum; 2]>) {
        self.buffer
            .push_back(BufferEntry(key, args.into_iter().next().unwrap()));
    }

    fn curr_window(&self) -> StatePos<'_> {
        let curr_key = self.buffer.front().map(|BufferEntry(key, _)| key);
        StatePos {
            key: curr_key,
            is_ready: self.buffer.len() > self.offset,
        }
    }

    fn output(&mut self) -> StreamExecutorResult<StateOutput> {
        debug_assert!(self.curr_window().is_ready);
        let lead_value = self.buffer[self.offset].1.clone();
        let BufferEntry(key, _) = self.buffer.pop_front().unwrap();
        Ok(StateOutput {
            return_value: lead_value,
            evict_hint: StateEvictHint::CanEvict(std::iter::once(key).collect()),
        })
    }
}

#[cfg(test)]
mod tests {
    // TODO(rc): need to add some unit tests
}
