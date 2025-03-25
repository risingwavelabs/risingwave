// Copyright 2025 RisingWave Labs
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

use anyhow::Result;
use chrono::prelude::*;
use chrono::{Duration, DurationRound};
use humantime::parse_duration;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::Value;
use tracing::debug;

use super::DEFAULT_MAX_PAST;
use crate::types::{Datum, Scalar, Timestamp, Timestamptz};

pub struct ChronoField<T: ChronoFieldInner> {
    max_past: Duration,
    absolute_base: Option<T>,
    seed: u64,
}

impl<T: ChronoFieldInner> ChronoField<T> {
    pub fn new(
        base: Option<DateTime<FixedOffset>>,
        max_past_option: Option<String>,
        max_past_mode: Option<String>,
        seed: u64,
    ) -> Result<Self> {
        let local_now = match max_past_mode.as_deref() {
            Some("relative") => None,
            _ => Some(T::from_now()),
        };

        let max_past = if let Some(max_past_option) = max_past_option {
            parse_duration(&max_past_option)?
        } else {
            // default max_past = 1 day
            DEFAULT_MAX_PAST
        };
        debug!(?local_now, ?max_past, "parse timestamp field option");
        Ok(Self {
            // convert to chrono::Duration
            max_past: chrono::Duration::from_std(max_past)?,
            absolute_base: base.map(T::from_base).or(local_now),
            seed,
        })
    }

    fn generate_data(&mut self, offset: u64) -> T {
        let milliseconds = self.max_past.num_milliseconds();
        let mut rng = StdRng::seed_from_u64(offset ^ self.seed);
        let max_milliseconds = rng.random_range(0..=milliseconds);
        let base = match self.absolute_base {
            Some(base) => base,
            None => T::from_now(),
        };
        base.minus(Duration::milliseconds(max_milliseconds))
    }

    pub fn generate(&mut self, offset: u64) -> Value {
        self.generate_data(offset).to_json()
    }

    pub fn generate_datum(&mut self, offset: u64) -> Datum {
        Some(self.generate_data(offset).to_scalar_value())
    }
}

pub trait ChronoFieldInner: std::fmt::Debug + Copy + Scalar {
    fn from_now() -> Self;
    fn from_base(base: DateTime<FixedOffset>) -> Self;
    fn minus(&self, duration: Duration) -> Self;
    fn to_json(&self) -> Value;
}

impl ChronoFieldInner for Timestamp {
    fn from_now() -> Self {
        Timestamp::new(
            Local::now()
                .naive_local()
                .duration_round(Duration::microseconds(1))
                .unwrap(),
        )
    }

    fn from_base(base: DateTime<FixedOffset>) -> Self {
        Timestamp::new(base.naive_local())
    }

    fn minus(&self, duration: Duration) -> Self {
        Timestamp::new(self.0 - duration)
    }

    fn to_json(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl ChronoFieldInner for Timestamptz {
    fn from_now() -> Self {
        Timestamptz::from(
            Utc::now()
                .duration_round(Duration::microseconds(1))
                .unwrap(),
        )
    }

    fn from_base(base: DateTime<FixedOffset>) -> Self {
        Timestamptz::from(base)
    }

    fn minus(&self, duration: Duration) -> Self {
        Timestamptz::from(self.to_datetime_utc() - duration)
    }

    fn to_json(&self) -> Value {
        Value::String(self.to_string())
    }
}
