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

use std::time::Duration;

use crate::config::{GeneratorConfig, NexmarkConfig};
use crate::event::*;

/// Nexmark event generator.
#[derive(Default, Clone, Debug)]
pub struct EventGenerator {
    cfg: GeneratorConfig,
    events_so_far: u64,
    /// If Some, only the specified type of events will be generated.
    type_filter: Option<EventType>,
    elapsed_ms: u64,
}

impl EventGenerator {
    /// Create a new generator.
    pub fn new(config: NexmarkConfig) -> Self {
        EventGenerator {
            cfg: config.into(),
            events_so_far: 0,
            type_filter: None,
            elapsed_ms: 0,
        }
    }

    /// Set the start event offset.
    pub fn with_events_so_far(mut self, events_so_far: u64) -> Self {
        self.events_so_far = events_so_far;
        self
    }

    /// Set the type of events to generate.
    pub fn with_type_filter(mut self, type_: EventType) -> Self {
        self.type_filter = Some(type_);
        self
    }

    /// Return the number of events so far.
    pub fn events_so_far(&self) -> u64 {
        self.events_so_far
    }

    /// Returns the amount of time elapsed since the base time to the last event timestamp.
    pub fn elapsed(&self) -> Duration {
        Duration::from_millis(self.elapsed_ms)
    }
}

impl Iterator for EventGenerator {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        loop {
            let event_number = self.cfg.next_adjusted_event(self.events_so_far as usize);
            let event_type = self.cfg.event_type(event_number);
            self.events_so_far += 1;

            if matches!(self.type_filter, Some(t) if t != event_type) {
                continue;
            }
            let event = Event::new(event_number, &self.cfg);
            self.elapsed_ms = event.timestamp() - self.cfg.base_time;
            return Some(event);
        }
    }
}
