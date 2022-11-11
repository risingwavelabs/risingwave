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

#[derive(Clone, Debug)]
pub struct EventGenerator {
    pub config: GeneratorConfig,
    pub events_so_far: u64,
    pub wall_clock_base_time: usize,
}

impl EventGenerator {
    pub fn new(config: NexmarkConfig, events_so_far: u64, wall_clock_base_time: usize) -> Self {
        EventGenerator {
            config: config.into(),
            events_so_far,
            wall_clock_base_time,
        }
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
