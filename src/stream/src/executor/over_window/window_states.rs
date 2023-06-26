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

use std::ops::{Deref, DerefMut};

use itertools::Itertools;
use risingwave_common::types::Datum;

use super::state::{StateEvictHint, StateKey, WindowState};
use crate::executor::StreamExecutorResult;

pub(super) struct WindowStates(Vec<Box<dyn WindowState + Send>>);

impl WindowStates {
    pub fn new(states: Vec<Box<dyn WindowState + Send>>) -> Self {
        assert!(!states.is_empty());
        Self(states)
    }

    /// Check if all windows are aligned.
    pub fn are_aligned(&self) -> bool {
        self.0
            .iter()
            .map(|state| state.curr_window().key)
            .all_equal()
    }

    /// Get the key of current windows.
    pub fn curr_key(&self) -> Option<&StateKey> {
        debug_assert!(self.are_aligned());
        self.0.first().and_then(|state| state.curr_window().key)
    }

    /// Check is all windows are ready.
    pub fn are_ready(&self) -> bool {
        debug_assert!(self.are_aligned());
        self.0.iter().all(|state| state.curr_window().is_ready)
    }

    /// Get the current output of all windows.
    pub fn curr_output(&self) -> StreamExecutorResult<Vec<Datum>> {
        debug_assert!(self.are_aligned());
        self.0.iter().map(|state| state.curr_output()).try_collect()
    }

    /// Slide all windows forward and collect the evict hints.
    pub fn slide_forward(&mut self) -> StateEvictHint {
        debug_assert!(self.are_aligned());
        self.0
            .iter_mut()
            .map(|state| state.slide_forward())
            .reduce(StateEvictHint::merge)
            .expect("# of evict hints = # of window states")
    }

    /// Slide all windows forward, ignoring the evict hints.
    pub fn just_slide_forward(&mut self) {
        debug_assert!(self.are_aligned());
        self.0
            .iter_mut()
            .for_each(|state| _ = state.slide_forward());
    }
}

impl Deref for WindowStates {
    type Target = Vec<Box<dyn WindowState + Send>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WindowStates {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
