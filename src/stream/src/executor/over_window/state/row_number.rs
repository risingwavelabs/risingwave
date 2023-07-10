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
use risingwave_common::types::Datum;
use risingwave_expr::function::window::WindowFuncCall;
use smallvec::SmallVec;

use super::{EstimatedVecDeque, StateEvictHint, StateKey, StatePos, WindowState};
use crate::executor::StreamExecutorResult;

#[derive(EstimateSize)]
pub(super) struct RowNumberState {
    first_key: Option<StateKey>,
    buffer: EstimatedVecDeque<StateKey>,
    curr_row_number: i64,
}

impl RowNumberState {
    pub fn new(_call: &WindowFuncCall) -> Self {
        Self {
            first_key: None,
            buffer: Default::default(),
            curr_row_number: 1,
        }
    }
}

impl WindowState for RowNumberState {
    fn append(&mut self, key: StateKey, _args: SmallVec<[Datum; 2]>) {
        if self.first_key.is_none() {
            self.first_key = Some(key.clone());
        }
        self.buffer.push_back(key);
    }

    fn curr_window(&self) -> StatePos<'_> {
        let curr_key = self.buffer.front();
        StatePos {
            key: curr_key,
            is_ready: curr_key.is_some(),
        }
    }

    fn curr_output(&self) -> StreamExecutorResult<Datum> {
        Ok(Some(self.curr_row_number.into()))
    }

    fn slide_forward(&mut self) -> StateEvictHint {
        self.curr_row_number += 1;
        self.buffer
            .pop_front()
            .expect("should not slide forward when the current window is not ready");
        StateEvictHint::CannotEvict(
            self.first_key
                .clone()
                .expect("should have appended some rows"),
        )
    }
}
