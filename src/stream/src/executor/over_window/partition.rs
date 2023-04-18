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

use itertools::Itertools;
use risingwave_expr::function::window::WindowFuncCall;

use super::state::{create_window_state, StateKey, WindowState};
use crate::executor::StreamExecutorResult;

pub(super) struct Partition {
    pub states: Vec<Box<dyn WindowState + Send>>,
}

impl Partition {
    pub fn new(calls: &[WindowFuncCall]) -> StreamExecutorResult<Self> {
        let states = calls.iter().map(create_window_state).try_collect()?;
        Ok(Self { states })
    }

    pub fn is_aligned(&self) -> bool {
        if self.states.is_empty() {
            true
        } else {
            self.states
                .iter()
                .map(|state| state.curr_window().key)
                .all_equal()
        }
    }

    pub fn is_ready(&self) -> bool {
        debug_assert!(self.is_aligned());
        self.states.iter().all(|state| state.curr_window().is_ready)
    }

    pub fn curr_window_key(&self) -> Option<&StateKey> {
        debug_assert!(self.is_aligned());
        self.states
            .first()
            .and_then(|state| state.curr_window().key)
    }
}
