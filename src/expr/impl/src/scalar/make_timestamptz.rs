// Copyright 2023 RisingWave Labs
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

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use risingwave_common::types::{FloatExt, Timestamp, Timestamptz, F64};
use risingwave_expr::expr_context::TIME_ZONE;
use risingwave_expr::{capture_context, function, ExprError, Result};

use crate::scalar::timestamptz::timestamp_at_time_zone;

// year int, month int, day int, hour int, min int, sec double precision
#[function("make_timestamptz(int4, int4, int4, int4, int4, float8) -> timestamptz")]
pub fn make_timestamptz(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: F64,
) -> Result<Timestamptz> {
    make_timestamptz_impl_captured(year, month, day, hour, min, sec)
}

// year int, month int, day int, hour int, min int, sec double precision, timezone text
#[function("make_timestamptz(int4, int4, int4, int4, int4, float8, varchar) -> timestamptz")]
pub fn make_timestamptz_with_time_zone(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: F64,
    time_zone: &str,
) -> Result<Timestamptz> {
    make_timestamptz_impl(time_zone, year, month, day, hour, min, sec)
}

#[capture_context(TIME_ZONE)]
fn make_timestamptz_impl(
    time_zone: &str,
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: F64,
) -> Result<Timestamptz> {
    if !sec.is_finite() || sec.0.is_sign_negative() {
        return Err(ExprError::InvalidParam {
            name: "sec",
            reason: "invalid sec".into(),
        });
    }
    let sec_u32 = sec.0.trunc() as u32;
    let microsecond_u32 = ((sec.0 - sec.0.trunc()) * 1_000_000.0).round_ties_even() as u32;
    let naive_date_time = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(year, month as u32, day as u32).ok_or_else(|| {
            ExprError::InvalidParam {
                name: "year, month, day",
                reason: "invalid date".into(),
            }
        })?,
        NaiveTime::from_hms_micro_opt(hour as u32, min as u32, sec_u32, microsecond_u32)
            .ok_or_else(|| ExprError::InvalidParam {
                name: "hour, min, sec",
                reason: "invalid time".into(),
            })?,
    );

    timestamp_at_time_zone(Timestamp(naive_date_time), time_zone)
}
