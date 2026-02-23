// Copyright 2024 RisingWave Labs
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
use risingwave_common::types::{Date, F64, FloatExt, Interval, Time, Timestamp, Timestamptz};
use risingwave_expr::expr_context::TIME_ZONE;
use risingwave_expr::{ExprError, Result, capture_context, function};

use crate::scalar::timestamptz::timestamp_at_time_zone;

pub fn make_naive_date(mut year: i32, month: i32, day: i32) -> Result<NaiveDate> {
    if year == 0 {
        return Err(ExprError::InvalidParam {
            name: "year, month, day",
            reason: format!("invalid date: {}-{}-{}", year, month, day).into(),
        });
    }
    if year < 0 {
        year += 1
    }
    NaiveDate::from_ymd_opt(year, month as u32, day as u32).ok_or_else(|| ExprError::InvalidParam {
        name: "year, month, day",
        reason: format!("invalid date: {}-{}-{}", year, month, day).into(),
    })
}

fn make_naive_time(hour: i32, min: i32, sec: F64) -> Result<NaiveTime> {
    if !sec.is_finite() || sec.0.is_sign_negative() {
        return Err(ExprError::InvalidParam {
            name: "sec",
            reason: format!("invalid sec: {}", sec).into(),
        });
    }
    let sec_u32 = sec.0.trunc() as u32;
    let nanosecond_u32 = ((sec.0 - sec.0.trunc()) * 1_000_000_000.0).round_ties_even() as u32;
    NaiveTime::from_hms_nano_opt(hour as u32, min as u32, sec_u32, nanosecond_u32).ok_or_else(
        || ExprError::InvalidParam {
            name: "hour, min, sec",
            reason: format!("invalid time: {}:{}:{}", hour, min, sec).into(),
        },
    )
}

// year int, month int, day int
#[function("make_date(int4, int4, int4) -> date")]
pub fn make_date(year: i32, month: i32, day: i32) -> Result<Date> {
    Ok(Date(make_naive_date(year, month, day)?))
}

// years int, months int, weeks int, days int, hours int, mins int, secs double precision
#[function("make_interval(int4, int4, int4, int4, int4, int4, float8) -> interval")]
pub fn make_interval(
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
    hours: i32,
    mins: i32,
    secs: F64,
) -> Result<Interval> {
    if !secs.is_finite() {
        return Err(ExprError::InvalidParam {
            name: "secs",
            reason: format!("invalid secs: {}", secs).into(),
        });
    }

    let total_months = years
        .checked_mul(12)
        .and_then(|v| v.checked_add(months))
        .ok_or(ExprError::NumericOutOfRange)?;

    let total_days = weeks
        .checked_mul(7)
        .and_then(|v| v.checked_add(days))
        .ok_or(ExprError::NumericOutOfRange)?;

    let hours_usecs = (hours as i64)
        .checked_mul(3600)
        .and_then(|v| v.checked_mul(1_000_000))
        .ok_or(ExprError::NumericOutOfRange)?;
    let mins_usecs = (mins as i64)
        .checked_mul(60)
        .and_then(|v| v.checked_mul(1_000_000))
        .ok_or(ExprError::NumericOutOfRange)?;

    let secs_usecs_f64 = (secs.0 * 1_000_000.0).round_ties_even();
    if !secs_usecs_f64.is_finite() || secs_usecs_f64 < i64::MIN as f64 || secs_usecs_f64 > i64::MAX as f64 {
        return Err(ExprError::NumericOutOfRange);
    }
    let secs_usecs = secs_usecs_f64 as i64;

    let total_usecs = hours_usecs
        .checked_add(mins_usecs)
        .and_then(|v| v.checked_add(secs_usecs))
        .ok_or(ExprError::NumericOutOfRange)?;

    Ok(Interval::from_month_day_usec(total_months, total_days, total_usecs))
}

// hour int, min int, sec double precision
#[function("make_time(int4, int4, float8) -> time")]
pub fn make_time(hour: i32, min: i32, sec: F64) -> Result<Time> {
    Ok(Time(make_naive_time(hour, min, sec)?))
}

// year int, month int, day int, hour int, min int, sec double precision
#[function("make_timestamp(int4, int4, int4, int4, int4, float8) -> timestamp")]
pub fn make_timestamp(
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    min: i32,
    sec: F64,
) -> Result<Timestamp> {
    Ok(Timestamp(NaiveDateTime::new(
        make_naive_date(year, month, day)?,
        make_naive_time(hour, min, sec)?,
    )))
}

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
    let naive_date_time = NaiveDateTime::new(
        make_naive_date(year, month, day)?,
        make_naive_time(hour, min, sec)?,
    );
    timestamp_at_time_zone(Timestamp(naive_date_time), time_zone)
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use risingwave_common::types::{Date, Interval, Timestamp};

    use super::make_interval;

    #[test]
    fn test_naive_date_and_time() {
        let year = -1973;
        let month = 2;
        let day = 2;
        let hour = 12;
        let min = 34;
        let sec: f64 = 56.789;
        let naive_date = NaiveDate::from_ymd_opt(year, month as u32, day as u32).unwrap();
        let naive_time = NaiveTime::from_hms_micro_opt(
            hour as u32,
            min as u32,
            sec.trunc() as u32,
            ((sec - sec.trunc()) * 1_000_000.0).round() as u32,
        )
        .unwrap();
        assert_eq!(naive_date.to_string(), String::from("-1973-02-02"));
        let date = Date(naive_date);
        assert_eq!(date.to_string(), String::from("1974-02-02 BC"));
        assert_eq!(naive_time.to_string(), String::from("12:34:56.789"));
        let date_time = Timestamp(NaiveDateTime::new(naive_date, naive_time));
        assert_eq!(
            date_time.to_string(),
            String::from("1974-02-02 12:34:56.789 BC")
        );
    }

    #[test]
    fn test_make_interval_days() {
        assert_eq!(
            make_interval(0, 0, 0, 10, 0, 0, 0.0.into()).unwrap(),
            Interval::from_month_day_usec(0, 10, 0)
        );
        assert_eq!(
            make_interval(0, 0, 0, -3, 0, 0, 0.0.into()).unwrap(),
            Interval::from_month_day_usec(0, -3, 0)
        );
    }

    #[test]
    fn test_make_interval_mixed_fields() {
        assert_eq!(
            make_interval(1, 2, 1, 3, 4, 5, 6.5.into()).unwrap(),
            Interval::from_month_day_usec(14, 10, ((4 * 3600 + 5 * 60 + 6) * 1_000_000 + 500_000) as i64)
        );
    }
}
