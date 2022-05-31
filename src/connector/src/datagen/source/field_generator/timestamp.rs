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

use anyhow::Result;
use chrono::prelude::*;
use chrono::Duration;
use parse_duration::parse;
use rand::{thread_rng, Rng};
use serde_json::{json, Value};

use super::DEFAULT_MAX_PAST;

pub struct TimestampField {
    max_past: Duration,
    local_now: NaiveDateTime,
}

impl TimestampField {
    pub fn new(max_past_option: Option<String>) -> Result<Self> {
        let local_now = Local::now().naive_local();
        // std duration
        let max_past = if let Some(max_past_option) = max_past_option {
            parse(&max_past_option)?
        } else {
            // default max_past = 1 day
            DEFAULT_MAX_PAST
        };
        Ok(Self {
            // convert to chrono::Duration
            max_past: chrono::Duration::from_std(max_past)?,
            local_now,
        })
    }

    pub fn generate(&mut self) -> Value {
        let seconds = self.max_past.num_seconds();
        let mut rng = thread_rng();
        let max_seconds = rng.gen_range(0..=seconds);
        let res = self.local_now - Duration::seconds(max_seconds);
        json!(res.to_string())
    }
}
