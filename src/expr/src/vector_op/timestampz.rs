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

use chrono::{TimeZone, Utc};
use chrono_tz::Tz;
use num_traits::ToPrimitive;
use risingwave_common::types::{NaiveDateTimeWrapper, OrderedF64};

use crate::{ExprError, Result};

/// Just a wrapper to reuse the `map_err` logic.
#[inline(always)]
fn parse_time_zone(time_zone: &str) -> Result<Tz> {
    Tz::from_str_insensitive(time_zone).map_err(|e| ExprError::InvalidParam {
        name: "time_zone",
        reason: e,
    })
}

#[inline(always)]
pub fn f64_sec_to_timestampz(elem: OrderedF64) -> Result<i64> {
    // TODO(#4515): handle +/- infinity
    (elem * 1e6)
        .round() // TODO(#5576): should round to even
        .to_i64()
        .ok_or(ExprError::NumericOutOfRange)
}

#[inline(always)]
pub fn timestamp_at_time_zone(input: NaiveDateTimeWrapper, time_zone: &str) -> Result<i64> {
    let time_zone = parse_time_zone(time_zone)?;
    let instant_local = input
        .0
        .and_local_timezone(time_zone)
        .latest()
        .ok_or_else(|| ExprError::InvalidParam {
            name: "local timestamp",
            reason: format!(
                "fail to interpret local timestamp \"{}\" in time zone \"{}\"",
                input, time_zone
            ),
        })?;
    let usec = instant_local.timestamp_micros();
    Ok(usec)
}

#[inline(always)]
pub fn timestampz_at_time_zone(input: i64, time_zone: &str) -> Result<NaiveDateTimeWrapper> {
    let time_zone = parse_time_zone(time_zone)?;
    let instant_utc = Utc.timestamp(input / 1_000_000, (input % 1_000_000 * 1000) as u32);
    let instant_local = instant_utc.with_timezone(&time_zone);
    let naive = instant_local.naive_local();
    Ok(NaiveDateTimeWrapper(naive))
}
