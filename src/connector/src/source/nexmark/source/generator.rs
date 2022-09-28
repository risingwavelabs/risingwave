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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_async_stream::try_stream;
use risingwave_common::bail;

use crate::source::nexmark::config::NexmarkConfig;
use crate::source::nexmark::source::event::{Event, EventType};
use crate::source::nexmark::source::message::NexmarkMessage;
use crate::source::{SourceMessage, SplitId};

#[derive(Clone, Debug)]
pub struct NexmarkEventGenerator {
    pub events_so_far: u64,
    pub event_num: i64,
    pub config: NexmarkConfig,
    pub wall_clock_base_time: usize,
    pub split_index: i32,
    pub split_num: i32,
    pub split_id: SplitId,
    pub event_type: EventType,
    pub use_real_time: bool,
    pub min_event_gap_in_ns: u64,
    pub max_chunk_size: u64,
}

impl NexmarkEventGenerator {
    #[try_stream(ok = SourceMessage, error = anyhow::Error)]
    pub async fn into_stream(mut self) {
        if self.split_num == 0 {
            bail!("NexmarkEventGenerator is not ready");
        }
        let mut last_event = None;
        loop {
            let mut num_event = 0;
            let old_events_so_far = self.events_so_far;

            // Get unix timestamp in milliseconds
            let current_timestamp_ms = if self.use_real_time {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
            } else {
                0
            };

            if let Some(event) = last_event.take() {
                num_event += 1;
                yield NexmarkMessage::new(self.split_id.clone(), self.events_so_far as u64, event)
                    .into();
            }

            while num_event < self.max_chunk_size {
                if self.event_num > 0 && self.events_so_far >= self.event_num as u64 {
                    break;
                }

                let (event, new_wall_clock_base_time) = Event::new(
                    self.events_so_far as usize,
                    &self.config,
                    self.wall_clock_base_time,
                );

                self.wall_clock_base_time = new_wall_clock_base_time;
                self.events_so_far += 1;

                if event.event_type() != self.event_type
                    || self.events_so_far % self.split_num as u64 != self.split_index as u64
                {
                    continue;
                }

                // When the generated timestamp is larger then current timestamp, if its the first
                // event, sleep and continue. Otherwise, directly return.
                if self.use_real_time && current_timestamp_ms < new_wall_clock_base_time as u64 {
                    tokio::time::sleep(Duration::from_millis(
                        new_wall_clock_base_time as u64 - current_timestamp_ms,
                    ))
                    .await;

                    last_event = Some(event);
                    break;
                }

                num_event += 1;
                yield NexmarkMessage::new(self.split_id.clone(), self.events_so_far as u64, event)
                    .into();
            }

            if !self.use_real_time && self.min_event_gap_in_ns > 0 {
                tokio::time::sleep(Duration::from_nanos(
                    (self.events_so_far - old_events_so_far) as u64 * self.min_event_gap_in_ns,
                ))
                .await;
            }
        }
    }
}
