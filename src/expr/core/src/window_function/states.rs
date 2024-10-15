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

use std::ops::{Deref, DerefMut};

use itertools::Itertools;
use risingwave_common::types::Datum;

use super::state::{StateEvictHint, StateKey, WindowState};
use crate::Result;

pub struct WindowStates(Vec<Box<dyn WindowState + Send + Sync>>);

impl WindowStates {
    pub fn new(states: Vec<Box<dyn WindowState + Send + Sync>>) -> Self {
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

    /// Slide all windows forward and collect the output and evict hints.
    pub fn slide(&mut self) -> Result<(Vec<Datum>, StateEvictHint)> {
        debug_assert!(self.are_aligned());
        let mut output = Vec::with_capacity(self.0.len());
        let mut evict_hint: Option<StateEvictHint> = None;
        for state in &mut self.0 {
            let (x_output, x_evict) = state.slide()?;
            output.push(x_output);
            evict_hint = match evict_hint {
                Some(evict_hint) => Some(evict_hint.merge(x_evict)),
                None => Some(x_evict),
            };
        }
        Ok((
            output,
            evict_hint.expect("# of evict hints = # of window states"),
        ))
    }

    /// Slide all windows forward and collect the output, ignoring the evict hints.
    pub fn slide_no_evict_hint(&mut self) -> Result<Vec<Datum>> {
        debug_assert!(self.are_aligned());
        let mut output = Vec::with_capacity(self.0.len());
        for state in &mut self.0 {
            let (x_output, _) = state.slide()?;
            output.push(x_output);
        }
        Ok(output)
    }

    /// Slide all windows forward, ignoring the output and evict hints.
    pub fn just_slide(&mut self) -> Result<()> {
        debug_assert!(self.are_aligned());
        for state in &mut self.0 {
            state.slide_no_output()?;
        }
        Ok(())
    }

    /// Slide all windows forward, until the current key is `curr_key`, ignoring the output and evict hints.
    /// After this method, `self.curr_key() == Some(curr_key)`.
    /// `curr_key` must exist in the `WindowStates`.
    pub fn just_slide_to(&mut self, curr_key: &StateKey) -> Result<()> {
        // TODO(rc): with the knowledge of the old output, we can "jump" to the `curr_key` directly for some window function kind
        while self.curr_key() != Some(curr_key) {
            self.just_slide()?;
        }
        Ok(())
    }
}

impl Deref for WindowStates {
    type Target = Vec<Box<dyn WindowState + Send + Sync>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for WindowStates {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
