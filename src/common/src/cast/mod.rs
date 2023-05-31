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

/// Refer to PostgreSQL's implementation <https://github.com/postgres/postgres/blob/5cb54fc310fb84287cbdc74533f3420490a2f63a/src/backend/utils/adt/varlena.c#L276-L288>
pub fn str_to_bytea(elem: &str) -> Result<Box<[u8]>> {
    if let Some(remainder) = elem.strip_prefix(r"\x") {
        Ok(parse_bytes_hex(remainder)?.into())
    } else {
        Ok(parse_bytes_traditional(elem)?.into())
    }
}

/// Ref: <https://docs.rs/hex/0.4.3/src/hex/lib.rs.html#175-185>
fn get_hex(c: u8) -> Result<u8> {
    match c {
        b'A'..=b'F' => Ok(c - b'A' + 10),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'0'..=b'9' => Ok(c - b'0'),
        _ => Err(format!("invalid hexadecimal digit: \"{}\"", c as char)),
    }
}

/// Refer to <https://www.postgresql.org/docs/current/datatype-binary.html#id-1.5.7.12.10> for specification.
pub fn parse_bytes_hex(s: &str) -> Result<Vec<u8>> {
    let mut res = Vec::with_capacity(s.len() / 2);

    let mut bytes = s.bytes();
    while let Some(c) = bytes.next() {
        // white spaces are tolerated
        if c == b' ' || c == b'\n' || c == b'\t' || c == b'\r' {
            continue;
        }
        let v1 = get_hex(c)?;

        match bytes.next() {
            Some(c) => {
                let v2 = get_hex(c)?;
                res.push((v1 << 4) | v2);
            }
            None => return Err("invalid hexadecimal data: odd number of digits".to_string()),
        }
    }

    Ok(res)
}

/// Refer to <https://www.postgresql.org/docs/current/datatype-binary.html#id-1.5.7.12.10> for specification.
#[expect(clippy::identity_op)]
pub fn parse_bytes_traditional(s: &str) -> Result<Vec<u8>> {
    let bytes = s.as_bytes();

    let mut capacity = 0;
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'\\' {
            i += 1;
        } else if (bytes[i + 0] == b'\\')
            && (bytes[i + 1] >= b'0' && bytes[i + 1] <= b'3')
            && (bytes[i + 2] >= b'0' && bytes[i + 2] <= b'7')
            && (bytes[i + 3] >= b'0' && bytes[i + 3] <= b'7')
        {
            i += 4;
        } else if (bytes[i + 0] == b'\\') && (bytes[i + 1] == b'\\') {
            i += 2;
        } else {
            // one backslash, not followed by another or ### valid octal
            return Err("invalid input syntax for type bytea".to_string());
        }
        capacity += 1;
    }

    let mut res = Vec::with_capacity(capacity);
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'\\' {
            res.push(bytes[i]);
            i += 1;
        } else if (bytes[i + 0] == b'\\')
            && (bytes[i + 1] >= b'0' && bytes[i + 1] <= b'3')
            && (bytes[i + 2] >= b'0' && bytes[i + 2] <= b'7')
            && (bytes[i + 3] >= b'0' && bytes[i + 3] <= b'7')
        {
            res.push(
                ((bytes[i + 1] - b'0') << 6) + ((bytes[i + 2] - b'0') << 3) + (bytes[i + 3] - b'0'),
            );
            i += 4;
        } else if (bytes[i + 0] == b'\\') && (bytes[i + 1] == b'\\') {
            res.push(b'\\');
            i += 2;
        } else {
            // one backslash, not followed by another or ### valid octal
            return Err("invalid input syntax for type bytea".to_string());
        }
    }

    Ok(res)
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
        use crate::types::ToText;
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
