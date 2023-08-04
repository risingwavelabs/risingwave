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
use snafu::{ResultExt, Snafu};
use speedate::{Date as SpeedDate, DateTime as SpeedDateTime, Time as SpeedTime};
use strum::EnumMessage;

use crate::types::{Date, Time, Timestamp, Timestamptz};

type Result<T> = std::result::Result<T, DateTimeCastError>;

/// Wrapper for `speedate::ParseError` to implement `Error` trait.
#[derive(Snafu, Debug)]
#[snafu(display("{}", inner.get_documentation().unwrap()))]
pub struct SpeedateParseError {
    inner: speedate::ParseError,
}

impl From<speedate::ParseError> for SpeedateParseError {
    fn from(value: speedate::ParseError) -> Self {
        Self { inner: value }
    }
}

#[derive(Snafu, Debug)]
pub enum DateTimeCastError {
    #[snafu(display("can't cast `{from}` to date (expected format is YYYY-MM-DD)"))]
    DateParse {
        from: Box<str>,
        #[snafu(source(from(speedate::ParseError, Into::into)), provide(false))]
        source: SpeedateParseError,
    },

    #[snafu(display(
        "can't cast `{from}` to time (expected format is HH:MM:SS[.D+{{up to 6 digits}}][Z] or HH:MM)",
    ))]
    TimeParse {
        from: Box<str>,
        #[snafu(source(from(speedate::ParseError, Into::into)), provide(false))]
        source: SpeedateParseError,
    },

    #[snafu(display(
        "can't cast `{from}` to timestamp (expected format is YYYY-MM-DD HH:MM:SS[.D+{{up to 6 digits}}] or YYYY-MM-DD HH:MM or YYYY-MM-DD or ISO 8601 format)",
    ))]
    TimestampParse {
        from: Box<str>,
        #[snafu(source(from(speedate::ParseError, Into::into)), provide(false))]
        source: SpeedateParseError,
    },

    #[snafu(display("can't cast integer `{from}` to timestamp"))]
    TimestampFromInt { from: i64 },
}

impl From<DateTimeCastError> for crate::error::RwError {
    fn from(value: DateTimeCastError) -> Self {
        crate::error::ErrorCode::ExprError(value.into()).into()
    }
}

pub fn str_to_date(elem: &str) -> Result<Date> {
    Ok(Date::new(parse_naive_date(elem)?))
}

pub fn str_to_time(elem: &str) -> Result<Time> {
    Ok(Time::new(parse_naive_time(elem)?))
}

pub fn str_to_timestamp(elem: &str) -> Result<Timestamp> {
    Ok(Timestamp::new(parse_naive_datetime(elem)?))
}

#[inline]
pub fn parse_naive_date(s: &str) -> Result<NaiveDate> {
    let res = SpeedDate::parse_str(s).context(DateParseSnafu { from: s })?;
    Ok(Date::from_ymd_uncheck(res.year as i32, res.month as u32, res.day as u32).0)
}

#[inline]
pub fn parse_naive_time(s: &str) -> Result<NaiveTime> {
    let s_without_zone = s.trim_end_matches('Z');
    let res = SpeedTime::parse_str(s_without_zone).context(TimeParseSnafu { from: s })?;
    Ok(Time::from_hms_micro_uncheck(
        res.hour as u32,
        res.minute as u32,
        res.second as u32,
        res.microsecond,
    )
    .0)
}

#[inline]
pub fn parse_naive_datetime(s: &str) -> Result<NaiveDateTime> {
    if let Ok(res) = SpeedDateTime::parse_str(s) {
        Ok(Date::from_ymd_uncheck(
            res.date.year as i32,
            res.date.month as u32,
            res.date.day as u32,
        )
        .and_hms_micro_uncheck(
            res.time.hour as u32,
            res.time.minute as u32,
            res.time.second as u32,
            res.time.microsecond,
        )
        .0)
    } else {
        let res = SpeedDate::parse_str(s).context(TimestampParseSnafu { from: s })?;
        Ok(
            Date::from_ymd_uncheck(res.year as i32, res.month as u32, res.day as u32)
                .and_hms_micro_uncheck(0, 0, 0, 0)
                .0,
        )
    }
}

/// Converts UNIX epoch time to timestamptz.
///
/// The input UNIX epoch time is interpreted as follows:
///
/// - [0, 1e11) are assumed to be in seconds.
/// - [1e11, 1e14) are assumed to be in milliseconds.
/// - [1e14, 1e17) are assumed to be in microseconds.
/// - [1e17, upper) are assumed to be in nanoseconds.
///
/// This would cause no problem for timestamp in [1973-03-03 09:46:40, 5138-11-16 09:46:40).
#[inline]
pub fn i64_to_timestamptz(t: i64) -> Result<Timestamptz> {
    const E11: i64 = 100_000_000_000;
    const E14: i64 = 100_000_000_000_000;
    const E17: i64 = 100_000_000_000_000_000;
    match t {
        0..E11 => Ok(Timestamptz::from_secs(t).unwrap()), // s
        E11..E14 => Ok(Timestamptz::from_millis(t).unwrap()), // ms
        E14..E17 => Ok(Timestamptz::from_micros(t)),      // us
        E17.. => Ok(Timestamptz::from_micros(t / 1000)),  // ns
        _ => TimestampFromIntSnafu { from: t }.fail(),
    }
}

/// Converts UNIX epoch time to timestamp.
///
/// The input UNIX epoch time is interpreted as follows:
///
/// - [0, 1e11) are assumed to be in seconds.
/// - [1e11, 1e14) are assumed to be in milliseconds.
/// - [1e14, 1e17) are assumed to be in microseconds.
/// - [1e17, upper) are assumed to be in nanoseconds.
///
/// This would cause no problem for timestamp in [1973-03-03 09:46:40, 5138-11-16 09:46:40).
///
/// # Example
/// ```
/// # use risingwave_common::cast::i64_to_timestamp;
/// assert_eq!(
///     i64_to_timestamp(1_666_666_666).unwrap().to_string(),
///     "2022-10-25 02:57:46"
/// );
/// assert_eq!(
///     i64_to_timestamp(1_666_666_666_666).unwrap().to_string(),
///     "2022-10-25 02:57:46.666"
/// );
/// assert_eq!(
///     i64_to_timestamp(1_666_666_666_666_666).unwrap().to_string(),
///     "2022-10-25 02:57:46.666666"
/// );
/// assert_eq!(
///     i64_to_timestamp(1_666_666_666_666_666_666)
///         .unwrap()
///         .to_string(),
///     // note that we only support microseconds precision
///     "2022-10-25 02:57:46.666666"
/// );
/// ```
#[inline]
pub fn i64_to_timestamp(t: i64) -> Result<Timestamp> {
    let tz = i64_to_timestamptz(t)?;
    Ok(Timestamp::from_timestamp_uncheck(
        tz.timestamp(),
        tz.timestamp_subsec_nanos(),
    ))
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;

    #[test]
    fn parse_str() {
        str_to_timestamp("1999-01-08 04:02").unwrap();
        str_to_timestamp("1999-01-08 04:05:06").unwrap();
        assert_eq!(
            str_to_timestamp("2022-08-03T10:34:02Z").unwrap(),
            str_to_timestamp("2022-08-03 10:34:02").unwrap()
        );
        str_to_date("1999-01-08").unwrap();
        str_to_time("04:05").unwrap();
        str_to_time("04:05:06").unwrap();

        assert_matches!(
            str_to_timestamp("1999-01-08 04:05:06AA").unwrap_err(),
            DateTimeCastError::TimestampParse { .. }
        );
        assert_matches!(
            str_to_date("1999-01-08AA").unwrap_err(),
            DateTimeCastError::DateParse { .. }
        );
        assert_matches!(
            str_to_time("AA04:05:06").unwrap_err(),
            DateTimeCastError::TimeParse { .. }
        );
    }
}
