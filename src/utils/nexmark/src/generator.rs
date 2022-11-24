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
    ///
    /// If `type_filter` is set, this is the offset of the certain type.
    /// Otherwise, this is the offset of all events.
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
        self.cfg.event_timestamp(self.event_number())
    }

    /// Return the global offset of all type of events.
    pub fn global_offset(&self) -> u64 {
        match self.type_filter {
            Some(EventType::Person) => {
                (self.offset as usize / self.cfg.person_proportion
                    * self.cfg.proportion_denominator
                    + self.offset as usize % self.cfg.person_proportion) as u64
            }
            Some(EventType::Auction) => {
                (self.offset as usize / self.cfg.auction_proportion
                    * self.cfg.proportion_denominator
                    + self.cfg.person_proportion
                    + self.offset as usize % self.cfg.auction_proportion) as u64
            }
            Some(EventType::Bid) => {
                (self.offset as usize / self.cfg.bid_proportion * self.cfg.proportion_denominator
                    + self.cfg.person_proportion
                    + self.cfg.auction_proportion
                    + self.offset as usize % self.cfg.bid_proportion) as u64
            }
            None => self.offset,
        }
    }

    fn event_number(&self) -> usize {
        match self.type_filter {
            Some(_) => self.cfg.first_event_number + self.global_offset() as usize,
            None => self.cfg.next_adjusted_event(self.offset as usize),
        }
    }
}

impl Iterator for EventGenerator {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        let event = Event::new(self.event_number(), &self.cfg);
        self.offset += self.step;
        Some(event)
    }
}
