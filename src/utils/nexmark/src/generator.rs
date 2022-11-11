// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::config::{GeneratorConfig, NexmarkConfig};
use crate::event::Event;

/// Nexmark event generator.
#[derive(Clone, Debug)]
pub struct EventGenerator {
    config: GeneratorConfig,
    events_so_far: u64,
    wall_clock_base_time: u64,
}

impl EventGenerator {
    /// Create a new generator.
    pub fn new(config: NexmarkConfig, events_so_far: u64, wall_clock_base_time: u64) -> Self {
        EventGenerator {
            config: config.into(),
            events_so_far,
            wall_clock_base_time,
        }
    }

    /// Return the number of events so far.
    pub const fn events_so_far(&self) -> u64 {
        self.events_so_far
    }

    /// Return the wall clock base time in ms.
    pub const fn wall_clock_base_time(&self) -> u64 {
        self.wall_clock_base_time
    }
}

impl Iterator for EventGenerator {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        let (event, new_wall_clock_base_time) = Event::new(
            self.events_so_far as usize,
            &self.config,
            self.wall_clock_base_time,
        );
        self.wall_clock_base_time = new_wall_clock_base_time;
        self.events_so_far += 1;
        Some(event)
    }
}
