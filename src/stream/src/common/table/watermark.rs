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

/// Strategy to decide when to do state cleaning.
pub trait StateCleanStrategy: Default {
    /// Trigger when a epoch is committed.
    fn tick(&mut self);

    /// Whether to apply the state cleaning watermark.
    ///
    /// Returns true to indicate that state cleaning should be applied.
    fn apply(&mut self) -> bool;
}

/// No delay, apply watermark to clean state immediately.
#[derive(Default, Debug)]
pub struct EagerClean;

impl StateCleanStrategy for EagerClean {
    fn tick(&mut self) {}

    fn apply(&mut self) -> bool {
        true
    }
}

/// Delay the state cleaning by a specified epoch period.
/// The strategy reduced the delete-range calls to storage.
#[derive(Default, Debug)]
pub struct LazyCleanByEpoch<const PERIOD: usize> {
    /// number of epochs since the last time we did state cleaning by watermark.
    buffered_epochs_cnt: usize,
}

impl<const PERIOD: usize> StateCleanStrategy for LazyCleanByEpoch<PERIOD> {
    fn tick(&mut self) {
        self.buffered_epochs_cnt += 1;
    }

    fn apply(&mut self) -> bool {
        if self.buffered_epochs_cnt >= PERIOD {
            self.buffered_epochs_cnt = 0;
            true
        } else {
            false
        }
    }
}
