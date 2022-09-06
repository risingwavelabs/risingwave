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
use humantime::parse_duration;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{json, Value};

use super::DEFAULT_MAX_PAST;
use crate::types::{Datum, NaiveDateTimeWrapper, Scalar};

pub struct TimestampField {
    max_past: Duration,
    local_now: NaiveDateTime,
    seed: u64,
}

impl TimestampField {
    pub fn new(max_past_option: Option<String>, seed: u64) -> Result<Self> {
        let local_now = Local::now().naive_local();
        // std duration
        let max_past = if let Some(max_past_option) = max_past_option {
            parse_duration(&max_past_option)?
        } else {
            // default max_past = 1 day
            DEFAULT_MAX_PAST
        };
        Ok(Self {
            // convert to chrono::Duration
            max_past: chrono::Duration::from_std(max_past)?,
            local_now,
            seed,
        })
    }

    pub fn generate(&mut self, offset: u64) -> Value {
        let seconds = self.max_past.num_seconds();
        let mut rng = StdRng::seed_from_u64(offset ^ self.seed);
        let max_seconds = rng.gen_range(0..=seconds);
        let res = self.local_now - Duration::seconds(max_seconds);
        json!(res.to_string())
    }

    pub fn generate_datum(&mut self, offset: u64) -> Datum {
        let seconds = self.max_past.num_seconds();
        let mut rng = StdRng::seed_from_u64(offset ^ self.seed);
        let max_seconds = rng.gen_range(0..=seconds);
        let res = self.local_now - Duration::seconds(max_seconds);
        Some(NaiveDateTimeWrapper::new(res).to_scalar_value())
    }
}
