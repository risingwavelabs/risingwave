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

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use speedate::{Date as SpeedDate, DateTime as SpeedDateTime, Time as SpeedTime};

use crate::types::{Date, Time, Timestamp};

type Result<T> = std::result::Result<T, String>;

pub const PARSE_ERROR_STR_WITH_TIME_ZONE_TO_TIMESTAMPTZ: &str = concat!(
    "Can't cast string to timestamp with time zone (expected format is YYYY-MM-DD HH:MM:SS[.D+{up to 6 digits}] followed by +hh:mm or literal Z)"
    , "\nFor example: '2021-04-01 00:00:00+00:00'"
);
pub const PARSE_ERROR_STR_TO_TIMESTAMP: &str = "Can't cast string to timestamp (expected format is YYYY-MM-DD HH:MM:SS[.D+{up to 6 digits}] or YYYY-MM-DD HH:MM or YYYY-MM-DD or ISO 8601 format)";
pub const PARSE_ERROR_STR_TO_TIME: &str =
    "Can't cast string to time (expected format is HH:MM:SS[.D+{up to 6 digits}][Z] or HH:MM)";
pub const PARSE_ERROR_STR_TO_DATE: &str =
    "Can't cast string to date (expected format is YYYY-MM-DD)";
pub const PARSE_ERROR_STR_TO_BYTEA: &str = "Invalid Bytea syntax";

const ERROR_INT_TO_TIMESTAMP: &str = "Can't cast negative integer to timestamp";

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
    let res = SpeedDate::parse_str(s).map_err(|_| PARSE_ERROR_STR_TO_DATE.to_string())?;
    Ok(Date::from_ymd_uncheck(res.year as i32, res.month as u32, res.day as u32).0)
}

#[inline]
pub fn parse_naive_time(s: &str) -> Result<NaiveTime> {
    let s_without_zone = s.trim_end_matches('Z');
    let res =
        SpeedTime::parse_str(s_without_zone).map_err(|_| PARSE_ERROR_STR_TO_TIME.to_string())?;
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
        let res = SpeedDate::parse_str(s).map_err(|_| PARSE_ERROR_STR_TO_TIMESTAMP.to_string())?;
        Ok(
            Date::from_ymd_uncheck(res.year as i32, res.month as u32, res.day as u32)
                .and_hms_micro_uncheck(0, 0, 0, 0)
                .0,
        )
    }
}

#[inline(always)]
pub fn str_with_time_zone_to_timestamptz(elem: &str) -> Result<i64> {
    elem.parse::<DateTime<Utc>>()
        .map(|ret| ret.timestamp_micros())
        .map_err(|_| PARSE_ERROR_STR_WITH_TIME_ZONE_TO_TIMESTAMPTZ.to_string())
}

/// Converts UNIX epoch time to timestamp in microseconds.
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
pub fn i64_to_timestamptz(t: i64) -> Result<i64> {
    const E11: i64 = 100_000_000_000;
    const E14: i64 = 100_000_000_000_000;
    const E17: i64 = 100_000_000_000_000_000;
    match t {
        0..E11 => Ok(t * 1_000_000), // s
        E11..E14 => Ok(t * 1_000),   // ms
        E14..E17 => Ok(t),           // us
        E17.. => Ok(t / 1_000),      // ns
        _ => Err(ERROR_INT_TO_TIMESTAMP.to_string()),
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
    let us = i64_to_timestamptz(t)?;
    Ok(Timestamp::from_timestamp_uncheck(
        us / 1_000_000,
        (us % 1_000_000) as u32 * 1000,
    ))
}

// #[function("cast(varchar) -> bytea")]
pub fn str_to_bytea(elem: &str) -> Result<Box<[u8]>> {
    // Padded with whitespace str is not allowed.
    if elem.starts_with(' ') && elem.trim().starts_with("\\x") {
        Err(PARSE_ERROR_STR_TO_BYTEA.to_string())
    } else if let Some(remainder) = elem.strip_prefix(r"\x") {
        Ok(parse_bytes_hex(remainder)?.into())
    } else {
        Ok(parse_bytes_traditional(elem)?.into())
    }
}

// Refer to Materialize: https://github.com/MaterializeInc/materialize/blob/1766ab3978bc90abf75eb9b1fbadfcc95eca1993/src/repr/src/strconv.rs#L623
pub fn parse_bytes_hex(s: &str) -> Result<Vec<u8>> {
    // Can't use `hex::decode` here, as it doesn't tolerate whitespace
    // between encoded bytes.

    let decode_nibble = |b| match b {
        b'a'..=b'f' => Ok::<u8, String>(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        b'0'..=b'9' => Ok(b - b'0'),
        _ => Err(PARSE_ERROR_STR_TO_BYTEA.to_string()),
    };

    let mut buf = vec![];
    let mut nibbles = s.as_bytes().iter().copied();
    while let Some(n) = nibbles.next() {
        if let b' ' | b'\n' | b'\t' | b'\r' = n {
            continue;
        }
        let n = decode_nibble(n)?;
        let n2 = match nibbles.next() {
            None => return Err(PARSE_ERROR_STR_TO_BYTEA.to_string()),
            Some(n2) => decode_nibble(n2)?,
        };
        buf.push((n << 4) | n2);
    }
    Ok(buf)
}

// Refer to https://github.com/MaterializeInc/materialize/blob/1766ab3978bc90abf75eb9b1fbadfcc95eca1993/src/repr/src/strconv.rs#L650
pub fn parse_bytes_traditional(s: &str) -> Result<Vec<u8>> {
    // Bytes are interpreted literally, save for the special escape sequences
    // "\\", which represents a single backslash, and "\NNN", where each N
    // is an octal digit, which represents the byte whose octal value is NNN.
    let mut out = Vec::new();
    let mut bytes = s.as_bytes().iter().fuse();
    while let Some(&b) = bytes.next() {
        if b != b'\\' {
            out.push(b);
            continue;
        }
        match bytes.next() {
            None => return Err(PARSE_ERROR_STR_TO_BYTEA.to_string()),
            Some(b'\\') => out.push(b'\\'),
            b => match (b, bytes.next(), bytes.next()) {
                (Some(d2 @ b'0'..=b'3'), Some(d1 @ b'0'..=b'7'), Some(d0 @ b'0'..=b'7')) => {
                    out.push(((d2 - b'0') << 6) + ((d1 - b'0') << 3) + (d0 - b'0'));
                }
                _ => return Err(PARSE_ERROR_STR_TO_BYTEA.to_string()),
            },
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_str() {
        assert_eq!(
            str_with_time_zone_to_timestamptz("2022-08-03 10:34:02Z").unwrap(),
            str_with_time_zone_to_timestamptz("2022-08-03 02:34:02-08:00").unwrap()
        );
        str_to_timestamp("1999-01-08 04:02").unwrap();
        str_to_timestamp("1999-01-08 04:05:06").unwrap();
        assert_eq!(
            str_to_timestamp("2022-08-03T10:34:02Z").unwrap(),
            str_to_timestamp("2022-08-03 10:34:02").unwrap()
        );
        str_to_date("1999-01-08").unwrap();
        str_to_time("04:05").unwrap();
        str_to_time("04:05:06").unwrap();

        assert_eq!(
            str_with_time_zone_to_timestamptz("1999-01-08 04:05:06").unwrap_err(),
            PARSE_ERROR_STR_WITH_TIME_ZONE_TO_TIMESTAMPTZ.to_string()
        );
        assert_eq!(
            str_to_timestamp("1999-01-08 04:05:06AA").unwrap_err(),
            PARSE_ERROR_STR_TO_TIMESTAMP.to_string()
        );
        assert_eq!(
            str_to_date("1999-01-08AA").unwrap_err(),
            PARSE_ERROR_STR_TO_DATE.to_string()
        );
        assert_eq!(
            str_to_time("AA04:05:06").unwrap_err(),
            PARSE_ERROR_STR_TO_TIME.to_string()
        );
    }

    #[test]
    fn test_bytea() {
        assert_eq!(str_to_bytea("fgo").unwrap().as_ref().to_text(), r"\x66676f");
        assert_eq!(
            str_to_bytea(r"\xDeadBeef").unwrap().as_ref().to_text(),
            r"\xdeadbeef"
        );
        assert_eq!(
            str_to_bytea("12CD").unwrap().as_ref().to_text(),
            r"\x31324344"
        );
        assert_eq!(
            str_to_bytea("1234").unwrap().as_ref().to_text(),
            r"\x31323334"
        );
        assert_eq!(
            str_to_bytea(r"\x12CD").unwrap().as_ref().to_text(),
            r"\x12cd"
        );
        assert_eq!(
            str_to_bytea(r"\x De Ad Be Ef ").unwrap().as_ref().to_text(),
            r"\xdeadbeef"
        );
        assert_eq!(
            str_to_bytea("x De Ad Be Ef ").unwrap().as_ref().to_text(),
            r"\x7820446520416420426520456620"
        );
        assert_eq!(
            str_to_bytea(r"De\\123dBeEf").unwrap().as_ref().to_text(),
            r"\x44655c3132336442654566"
        );
        assert_eq!(
            str_to_bytea(r"De\123dBeEf").unwrap().as_ref().to_text(),
            r"\x4465536442654566"
        );
        assert_eq!(
            str_to_bytea(r"De\\000dBeEf").unwrap().as_ref().to_text(),
            r"\x44655c3030306442654566"
        );
    }
}
