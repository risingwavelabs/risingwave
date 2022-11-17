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
use crate::event::*;

/// Nexmark event generator.
#[derive(Default, Clone, Debug)]
pub struct EventGenerator {
    cfg: GeneratorConfig,
    /// The current offset of event.
    offset: u64,
    /// Each iteration the offset is incremented by this step.
    step: u64,
    /// If Some, only the specified type of events will be generated.
    type_filter: Option<EventType>,
}

impl EventGenerator {
    /// Create a new generator.
    pub fn new(config: NexmarkConfig) -> Self {
        EventGenerator {
            cfg: config.into(),
            offset: 0,
            step: 1,
            type_filter: None,
        }
    }

    /// Set the start event offset. Default: 0.
    pub fn with_offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    /// Set the step. Default: 1.
    pub fn with_step(mut self, step: u64) -> Self {
        assert_ne!(step, 0);
        self.step = step;
        self
    }

    /// Set the type of events to generate. Default: all.
    pub fn with_type_filter(mut self, type_: EventType) -> Self {
        self.type_filter = Some(type_);
        self
    }

    /// Return the current offset of the generator.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Return the timestamp of current offset event.
    pub fn timestamp(&self) -> u64 {
        let event_number = self.cfg.next_adjusted_event(self.offset as usize);
        self.cfg.event_timestamp(event_number)
    }
}

impl Iterator for EventGenerator {
    type Item = (u64, Event);

    fn next(&mut self) -> Option<(u64, Event)> {
        loop {
            let offset = self.offset;
            let event_number = self.cfg.next_adjusted_event(offset as usize);
            let event_type = self.cfg.event_type(event_number);
            self.offset += self.step;

            if matches!(self.type_filter, Some(t) if t != event_type) {
                continue;
            }
            let event = Event::new(event_number, &self.cfg);
            return Some((offset, event));
        }
    }
}
