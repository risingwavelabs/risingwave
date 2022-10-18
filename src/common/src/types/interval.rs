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

use std::cmp::Ordering;
use std::fmt::{Display, Formatter, Write as _};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::ops::{Add, Neg, Sub};

use anyhow::anyhow;
use byteorder::{BigEndian, WriteBytesExt};
use bytes::BytesMut;
use num_traits::{CheckedAdd, CheckedSub, Zero};
use risingwave_pb::data::IntervalUnit as IntervalUnitProto;
use smallvec::SmallVec;

use super::ops::IsNegative;
use super::*;
use crate::error::{ErrorCode, Result, RwError};

/// Every interval can be represented by a `IntervalUnit`.
/// Note that the difference between Interval and Instant.
/// For example, `5 yrs 1 month 25 days 23:22:57` is a interval (Can be interpreted by Interval Unit
/// with month = 61, days = 25, seconds = (57 + 23 * 3600 + 22 * 60) * 1000),
/// `1970-01-01 04:05:06` is a Instant or Timestamp
/// One month may contain 28/31 days. One day may contain 23/25 hours.
/// This internals is learned from PG:
/// <https://www.postgresql.org/docs/9.1/datatype-datetime.html#:~:text=field%20is%20negative.-,Internally,-interval%20values%20are>
#[derive(Debug, Clone, Copy, Default)]
pub struct IntervalUnit {
    months: i32,
    days: i32,
    ms: i64,
}

const DAY_MS: i64 = 86400000;
const MONTH_MS: i64 = 30 * DAY_MS;

impl IntervalUnit {
    pub fn new(months: i32, days: i32, ms: i64) -> Self {
        IntervalUnit { months, days, ms }
    }

    pub fn get_days(&self) -> i32 {
        self.days
    }

    pub fn get_months(&self) -> i32 {
        self.months
    }

    pub fn get_years(&self) -> i32 {
        self.months / 12
    }

    pub fn get_ms(&self) -> i64 {
        self.ms
    }

    pub fn get_ms_of_day(&self) -> u64 {
        self.ms.rem_euclid(DAY_MS) as u64
    }

    pub fn from_protobuf_bytes(bytes: &[u8], ty: IntervalType) -> ArrayResult<Self> {
        // TODO: remove IntervalType later.
        match ty {
            // the unit is months
            Year | YearToMonth | Month => {
                let bytes = bytes
                    .try_into()
                    .map_err(|e| anyhow!("Failed to deserialize i32: {:?}", e))?;
                let mouths = i32::from_be_bytes(bytes);
                Ok(IntervalUnit::from_month(mouths))
            }
            // the unit is ms
            Day | DayToHour | DayToMinute | DayToSecond | Hour | HourToMinute | HourToSecond
            | Minute | MinuteToSecond | Second => {
                let bytes = bytes
                    .try_into()
                    .map_err(|e| anyhow!("Failed to deserialize i64: {:?}", e))?;
                let ms = i64::from_be_bytes(bytes);
                Ok(IntervalUnit::from_millis(ms))
            }
            Unspecified => {
                // Invalid means the interval is from the new frontend.
                // TODO: make this default path later.
                let mut cursor = Cursor::new(bytes);
                read_interval_unit(&mut cursor)
            }
        }
    }

    /// Justify interval, convert 1 month to 30 days and 86400 ms to 1 day.
    /// If day is positive, complement the ms negative value.
    /// These rules only use in interval comparison.
    pub fn justify_interval(&mut self) {
        let total_ms = self.total_ms();
        *self = Self {
            months: 0,
            days: (total_ms / DAY_MS) as i32,
            ms: total_ms % DAY_MS,
        }
    }

    pub fn justified(&self) -> Self {
        let mut interval = *self;
        interval.justify_interval();
        interval
    }

    #[must_use]
    pub fn negative(&self) -> Self {
        IntervalUnit {
            months: -self.months,
            days: -self.days,
            ms: -self.ms,
        }
    }

    #[must_use]
    pub fn from_total_ms(ms: i64) -> Self {
        let mut remaining_ms = ms;
        let months = remaining_ms / MONTH_MS;
        remaining_ms -= months * MONTH_MS;
        let days = remaining_ms / DAY_MS;
        remaining_ms -= days * DAY_MS;
        IntervalUnit {
            months: (months as i32),
            days: (days as i32),
            ms: remaining_ms,
        }
    }

    pub fn total_ms(&self) -> i64 {
        self.months as i64 * MONTH_MS + self.days as i64 * DAY_MS + self.ms
    }

    #[must_use]
    pub fn from_ymd(year: i32, month: i32, days: i32) -> Self {
        let months = year * 12 + month;
        let days = days;
        let ms = 0;
        IntervalUnit { months, days, ms }
    }

    #[must_use]
    pub fn from_month(months: i32) -> Self {
        IntervalUnit {
            months,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn from_days(days: i32) -> Self {
        Self {
            days,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn from_millis(ms: i64) -> Self {
        Self {
            ms,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn from_minutes(minutes: i64) -> Self {
        Self {
            ms: 1000 * 60 * minutes,
            ..Default::default()
        }
    }

    pub fn to_protobuf_owned(self) -> Vec<u8> {
        let buf = BytesMut::with_capacity(16);
        let mut writer = buf.writer();
        self.to_protobuf(&mut writer).unwrap();
        writer.into_inner().to_vec()
    }

    pub fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output.write_i32::<BigEndian>(self.months)?;
        output.write_i32::<BigEndian>(self.days)?;
        output.write_i64::<BigEndian>(self.ms)?;
        Ok(16)
    }

    /// Multiple [`IntervalUnit`] by an integer with overflow check.
    pub fn checked_mul_int<I>(&self, rhs: I) -> Option<Self>
    where
        I: TryInto<i32>,
    {
        let rhs = rhs.try_into().ok()?;
        let months = self.months.checked_mul(rhs)?;
        let days = self.days.checked_mul(rhs)?;
        let ms = self.ms.checked_mul(rhs as i64)?;

        Some(IntervalUnit { months, days, ms })
    }

    /// Divides [`IntervalUnit`] by an integer/float with zero check.
    pub fn div_float<I>(&self, rhs: I) -> Option<Self>
    where
        I: TryInto<OrderedF64>,
    {
        let rhs = rhs.try_into().ok()?;
        let rhs = rhs.0;

        if rhs == 0.0 {
            return None;
        }

        let ms = self.as_ms_i64();
        Some(IntervalUnit::from_total_ms((ms as f64 / rhs).round() as i64))
    }

    fn as_ms_i64(&self) -> i64 {
        self.months as i64 * MONTH_MS + self.days as i64 * DAY_MS + self.ms
    }

    /// times [`IntervalUnit`] with an integer/float.
    pub fn mul_float<I>(&self, rhs: I) -> Option<Self>
    where
        I: TryInto<OrderedF64>,
    {
        let rhs = rhs.try_into().ok()?;
        let rhs = rhs.0;

        let ms = self.as_ms_i64();
        Some(IntervalUnit::from_total_ms((ms as f64 * rhs).round() as i64))
    }

    /// Performs an exact division, returns [`None`] if for any unit, lhs % rhs != 0.
    pub fn exact_div(&self, rhs: &Self) -> Option<i64> {
        let mut res = None;
        let mut check_unit = |l: i64, r: i64| {
            if l == 0 && r == 0 {
                return Some(());
            }
            if l != 0 && r == 0 {
                return None;
            }
            if l % r != 0 {
                return None;
            }
            let new_res = l / r;
            if let Some(old_res) = res {
                if old_res != new_res {
                    return None;
                }
            } else {
                res = Some(new_res);
            }

            Some(())
        };

        check_unit(self.months as i64, rhs.months as i64)?;
        check_unit(self.days as i64, rhs.days as i64)?;
        check_unit(self.ms, rhs.ms)?;

        res
    }

    /// Checks if [`IntervalUnit`] is positive.
    pub fn is_positive(&self) -> bool {
        self > &Self::new(0, 0, 0)
    }
}

impl Serialize for IntervalUnit {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let IntervalUnit { months, days, ms } = self.justified();
        // serialize the `IntervalUnit` as a tuple
        (months, days, ms).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for IntervalUnit {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (months, days, ms) = <(i32, i32, i64)>::deserialize(deserializer)?;
        Ok(Self { months, days, ms })
    }
}

#[expect(clippy::from_over_into)]
impl Into<IntervalUnitProto> for IntervalUnit {
    fn into(self) -> IntervalUnitProto {
        IntervalUnitProto {
            months: self.months,
            days: self.days,
            ms: self.ms,
        }
    }
}

impl From<&'_ IntervalUnitProto> for IntervalUnit {
    fn from(p: &'_ IntervalUnitProto) -> Self {
        Self {
            months: p.months,
            days: p.days,
            ms: p.ms,
        }
    }
}

impl From<NaiveTimeWrapper> for IntervalUnit {
    fn from(time: NaiveTimeWrapper) -> Self {
        let mut ms: i64 = (time.0.num_seconds_from_midnight() * 1000) as i64;
        ms += (time.0.nanosecond() / 1_000_000) as i64;
        Self {
            months: 0,
            days: 0,
            ms,
        }
    }
}

impl Add for IntervalUnit {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        let months = self.months + rhs.months;
        let days = self.days + rhs.days;
        let ms = self.ms + rhs.ms;
        IntervalUnit { months, days, ms }
    }
}

impl PartialOrd for IntervalUnit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.eq(other) {
            Some(Ordering::Equal)
        } else {
            let diff = *self - *other;
            let days = (diff.months * 30 + diff.days) as i64;
            Some((days * DAY_MS + diff.ms).cmp(&0))
        }
    }
}

impl Hash for IntervalUnit {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let interval = self.justified();
        interval.months.hash(state);
        interval.ms.hash(state);
        interval.days.hash(state);
    }
}

impl PartialEq for IntervalUnit {
    fn eq(&self, other: &Self) -> bool {
        let interval = self.justified();
        let other = other.justified();
        interval.days == other.days && interval.ms == other.ms
    }
}

impl Eq for IntervalUnit {}

impl Ord for IntervalUnit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl CheckedAdd for IntervalUnit {
    fn checked_add(&self, other: &Self) -> Option<Self> {
        let months = self.months.checked_add(other.months)?;
        let days = self.days.checked_add(other.days)?;
        let ms = self.ms.checked_add(other.ms)?;
        Some(IntervalUnit { months, days, ms })
    }
}

impl Sub for IntervalUnit {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        let months = self.months - rhs.months;
        let days = self.days - rhs.days;
        let ms = self.ms - rhs.ms;
        IntervalUnit { months, days, ms }
    }
}

impl CheckedSub for IntervalUnit {
    fn checked_sub(&self, other: &Self) -> Option<Self> {
        let months = self.months.checked_sub(other.months)?;
        let days = self.days.checked_sub(other.days)?;
        let ms = self.ms.checked_sub(other.ms)?;
        Some(IntervalUnit { months, days, ms })
    }
}

impl Zero for IntervalUnit {
    fn zero() -> Self {
        Self::new(0, 0, 0)
    }

    fn is_zero(&self) -> bool {
        self.months == 0 && self.days == 0 && self.ms == 0
    }
}

impl IsNegative for IntervalUnit {
    fn is_negative(&self) -> bool {
        let i = self.justified();
        i.months < 0 || (i.months == 0 && i.days < 0) || (i.months == 0 && i.days == 0 && i.ms < 0)
    }
}

impl Neg for IntervalUnit {
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            months: -self.months,
            days: -self.days,
            ms: -self.ms,
        }
    }
}

impl Display for IntervalUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let years = self.months / 12;
        let months = self.months % 12;
        let days = self.days;
        let hours = self.ms / 1000 / 3600;
        let minutes = (self.ms / 1000 / 60) % 60;
        let seconds = self.ms % 60000 / 1000;
        let mut secs_fract = self.ms % 1000;
        let mut v = SmallVec::<[String; 4]>::new();
        if years == 1 {
            v.push(format!("{years} year"));
        } else if years != 0 {
            v.push(format!("{years} years"));
        }
        if months == 1 {
            v.push(format!("{months} mon"));
        } else if months != 0 {
            v.push(format!("{months} mons"));
        }
        if days == 1 {
            v.push(format!("{days} day"));
        } else if days != 0 {
            v.push(format!("{days} days"));
        }
        let mut format_time = format!("{hours:0>2}:{minutes:0>2}:{seconds:0>2}");
        if secs_fract != 0 {
            write!(format_time, ".{:03}", secs_fract)?;
            while secs_fract % 10 == 0 {
                secs_fract /= 10;
                format_time.pop();
            }
        }
        v.push(format_time);
        Display::fmt(&v.join(" "), f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DateTimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl FromStr for DateTimeField {
    type Err = RwError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "years" | "year" | "yrs" | "yr" | "y" => Ok(Self::Year),
            "days" | "day" | "d" => Ok(Self::Day),
            "hours" | "hour" | "hrs" | "hr" | "h" => Ok(Self::Hour),
            "minutes" | "minute" | "mins" | "min" | "m" => Ok(Self::Minute),
            "months" | "month" | "mons" | "mon" => Ok(Self::Month),
            "seconds" | "second" | "secs" | "sec" | "s" => Ok(Self::Second),
            _ => Err(ErrorCode::InvalidInputSyntax(format!("unknown unit {}", s)).into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum TimeStrToken {
    Second(OrderedF64),
    Num(i64),
    TimeUnit(DateTimeField),
}

fn parse_interval(s: &str) -> Result<Vec<TimeStrToken>> {
    let s = s.trim();
    let mut tokens = Vec::new();
    let mut num_buf = "".to_string();
    let mut char_buf = "".to_string();
    let mut hour_min_sec = Vec::new();
    for (i, c) in s.chars().enumerate() {
        match c {
            '-' => {
                num_buf.push(c);
            }
            '.' => {
                num_buf.push(c);
            }
            c if c.is_ascii_digit() => {
                convert_unit(&mut char_buf, &mut tokens)?;
                num_buf.push(c);
            }
            c if c.is_ascii_alphabetic() => {
                convert_digit(&mut num_buf, &mut tokens)?;
                char_buf.push(c);
            }
            chr if chr.is_ascii_whitespace() => {
                convert_unit(&mut char_buf, &mut tokens)?;
                convert_digit(&mut num_buf, &mut tokens)?;
            }
            ':' => {
                // there must be a digit before the ':'
                if num_buf.is_empty() {
                    return Err(ErrorCode::InvalidInputSyntax(format!(
                        "invalid interval format: {}",
                        s
                    ))
                    .into());
                }
                hour_min_sec.push(num_buf.clone());
                num_buf.clear();
            }
            _ => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "Invalid character at offset {} in {}: {:?}. Only support digit or alphabetic now",
                    i,s, c
                ))
                .into());
            }
        };
    }
    if !hour_min_sec.is_empty() {
        if !num_buf.is_empty() {
            hour_min_sec.push(num_buf.clone());
            num_buf.clear();
        }
    } else {
        convert_digit(&mut num_buf, &mut tokens)?;
    }
    convert_unit(&mut char_buf, &mut tokens)?;
    convert_hms(&mut hour_min_sec, &mut tokens)?;

    Ok(tokens)
}

fn convert_digit(c: &mut String, t: &mut Vec<TimeStrToken>) -> Result<()> {
    if !c.is_empty() {
        match c.parse::<i64>() {
            Ok(num) => {
                t.push(TimeStrToken::Num(num));
            }
            Err(_) => {
                return Err(
                    ErrorCode::InvalidInputSyntax(format!("Invalid interval: {}", c)).into(),
                );
            }
        }
        c.clear();
    }
    Ok(())
}

fn convert_unit(c: &mut String, t: &mut Vec<TimeStrToken>) -> Result<()> {
    if !c.is_empty() {
        t.push(TimeStrToken::TimeUnit(c.parse()?));
        c.clear();
    }
    Ok(())
}

/// convert `hour_min_sec` format
/// e.g.
/// c = ["1", "2", "3"], c will be convert to:
/// [`TimeStrToken::Num(1)`, `TimeStrToken::TimeUnit(DateTimeField::Hour)`,
///  `TimeStrToken::Num(2)`, `TimeStrToken::TimeUnit(DateTimeField::Minute)`,
///  `TimeStrToken::Second("3")`, `TimeStrToken::TimeUnit(DateTimeField::Second)`]
fn convert_hms(c: &mut Vec<String>, t: &mut Vec<TimeStrToken>) -> Result<()> {
    if c.len() > 3 {
        return Err(ErrorCode::InvalidInputSyntax(format!("Invalid interval: {:?}", c)).into());
    }
    for (i, s) in c.iter().enumerate() {
        match i {
            0 => {
                t.push(TimeStrToken::Num(s.parse().map_err(|_| {
                    ErrorCode::InternalError(format!("Invalid interval: {}", c[0]))
                })?));
                t.push(TimeStrToken::TimeUnit(DateTimeField::Hour))
            }
            1 => {
                t.push(TimeStrToken::Num(s.parse().map_err(|_| {
                    ErrorCode::InternalError(format!("Invalid interval: {}", c[0]))
                })?));
                t.push(TimeStrToken::TimeUnit(DateTimeField::Minute))
            }
            2 => {
                t.push(TimeStrToken::Second(s.parse().map_err(|_| {
                    ErrorCode::InternalError(format!("Invalid interval: {}", c[0]))
                })?));
                t.push(TimeStrToken::TimeUnit(DateTimeField::Second))
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

impl IntervalUnit {
    fn parse_sql_standard(s: &str, leading_field: DateTimeField) -> Result<Self> {
        use DateTimeField::*;
        let tokens = parse_interval(s)?;
        // Todo: support more syntax
        if tokens.len() > 1 {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "(standard sql format) Can't support syntax of interval {}.",
                &s
            ))
            .into());
        }
        let num = match tokens.get(0) {
            Some(TimeStrToken::Num(num)) => *num,
            _ => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "(standard sql format)Invalid interval {}.",
                    &s
                ))
                .into());
            }
        };

        (|| match leading_field {
            Year => {
                let months = num.checked_mul(12)?;
                Some(IntervalUnit::from_month(months as i32))
            }
            Month => Some(IntervalUnit::from_month(num as i32)),
            Day => Some(IntervalUnit::from_days(num as i32)),
            Hour => {
                let ms = num.checked_mul(3600 * 1000)?;
                Some(IntervalUnit::from_millis(ms))
            }
            Minute => {
                let ms = num.checked_mul(60 * 1000)?;
                Some(IntervalUnit::from_millis(ms))
            }
            Second => {
                let ms = num.checked_mul(1000)?;
                Some(IntervalUnit::from_millis(ms))
            }
        })()
        .ok_or_else(|| ErrorCode::InvalidInputSyntax(format!("Invalid interval {}.", s)).into())
    }

    fn parse_postgres(s: &str) -> Result<Self> {
        use DateTimeField::*;
        let mut tokens = parse_interval(s)?;
        if tokens.len()%2!=0 && let Some(TimeStrToken::Num(_)) = tokens.last() {
            tokens.push(TimeStrToken::TimeUnit(DateTimeField::Second));
        }
        if tokens.len() % 2 != 0 {
            return Err(ErrorCode::InvalidInputSyntax(format!("Invalid interval {}.", &s)).into());
        }
        let mut token_iter = tokens.into_iter();
        let mut result = IntervalUnit::new(0, 0, 0);
        while let Some(num) = token_iter.next() && let Some(interval_unit) = token_iter.next() {
            match (num, interval_unit) {
                (TimeStrToken::Num(num), TimeStrToken::TimeUnit(interval_unit)) => {
                    result = result + (|| match interval_unit {
                        Year => {
                            let months = num.checked_mul(12)?;
                            Some(IntervalUnit::from_month(months as i32))
                        }
                        Month => Some(IntervalUnit::from_month(num as i32)),
                        Day => Some(IntervalUnit::from_days(num as i32)),
                        Hour => {
                            let ms = num.checked_mul(3600 * 1000)?;
                            Some(IntervalUnit::from_millis(ms))
                        }
                        Minute => {
                            let ms = num.checked_mul(60 * 1000)?;
                            Some(IntervalUnit::from_millis(ms))
                        }
                        Second => {
                            let ms = num.checked_mul(1000)?;
                            Some(IntervalUnit::from_millis(ms))
                        }
                    })()
                    .ok_or_else(|| ErrorCode::InvalidInputSyntax(format!("Invalid interval {}.", s)))?;
                }
                (TimeStrToken::Second(second), TimeStrToken::TimeUnit(interval_unit)) => {
                    result = result + (|| match interval_unit {
                        Second => {
                            // TODO: IntervalUnit only support millisecond precision so the part smaller than millisecond will be truncated.
                            if second < OrderedF64::from(0.001) {
                                return None;
                            }
                            let ms = (second * 1000_f64).round() as i64;
                            Some(IntervalUnit::from_millis(ms))
                        }
                        _ => None,
                    })()
                    .ok_or_else(|| ErrorCode::InvalidInputSyntax(format!("Invalid interval {}.", s)))?;
                }
                _ => {
                    return Err(ErrorCode::InvalidInputSyntax(format!("Invalid interval {}.", &s)).into());
                }
            }
        }
        Ok(result)
    }

    pub fn parse_with_fields(s: &str, leading_field: Option<DateTimeField>) -> Result<Self> {
        if let Some(leading_field) = leading_field {
            Self::parse_sql_standard(s, leading_field)
        } else {
            Self::parse_postgres(s)
        }
    }
}

impl FromStr for IntervalUnit {
    type Err = RwError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse_with_fields(s, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::ordered_float::OrderedFloat;

    #[test]
    fn test_parse() {
        let interval = "1 year 2 months 3 days 00:00:01"
            .parse::<IntervalUnit>()
            .unwrap();
        assert_eq!(
            interval,
            IntervalUnit::from_month(14)
                + IntervalUnit::from_days(3)
                + IntervalUnit::from_millis(1000)
        );

        let interval = "1 year 2 months 3 days 00:00:00.001"
            .parse::<IntervalUnit>()
            .unwrap();
        assert_eq!(
            interval,
            IntervalUnit::from_month(14)
                + IntervalUnit::from_days(3)
                + IntervalUnit::from_millis(1)
        );

        let interval = "1 year 2 months 3 days 00:59:59.005"
            .parse::<IntervalUnit>()
            .unwrap();
        assert_eq!(
            interval,
            IntervalUnit::from_month(14)
                + IntervalUnit::from_days(3)
                + IntervalUnit::from_minutes(59)
                + IntervalUnit::from_millis(59000)
                + IntervalUnit::from_millis(5)
        );

        let interval = "1 year 2 months 3 days 01".parse::<IntervalUnit>().unwrap();
        assert_eq!(
            interval,
            IntervalUnit::from_month(14)
                + IntervalUnit::from_days(3)
                + IntervalUnit::from_millis(1000)
        );

        let interval = "1 year 2 months 3 days 1:".parse::<IntervalUnit>().unwrap();
        assert_eq!(
            interval,
            IntervalUnit::from_month(14)
                + IntervalUnit::from_days(3)
                + IntervalUnit::from_minutes(60)
        );

        let interval = "1 year 2 months 3 days 1:2"
            .parse::<IntervalUnit>()
            .unwrap();
        assert_eq!(
            interval,
            IntervalUnit::from_month(14)
                + IntervalUnit::from_days(3)
                + IntervalUnit::from_minutes(62)
        );

        let interval = "1 year 2 months 3 days 1:2:"
            .parse::<IntervalUnit>()
            .unwrap();
        assert_eq!(
            interval,
            IntervalUnit::from_month(14)
                + IntervalUnit::from_days(3)
                + IntervalUnit::from_minutes(62)
        );
    }

    #[test]
    fn test_to_string() {
        let interval =
            IntervalUnit::new(-14, 3, 11 * 3600 * 1000 + 45 * 60 * 1000 + 14 * 1000 + 233);
        assert_eq!(interval.to_string(), "-1 years -2 mons 3 days 11:45:14.233");
    }

    #[test]
    fn test_exact_div() {
        let cases = [
            ((14, 6, 6), (14, 6, 6), Some(1)),
            ((0, 0, 0), (0, 0, 0), None),
            ((0, 0, 0), (1, 0, 0), Some(0)),
            ((1, 1, 1), (0, 0, 0), None),
            ((1, 1, 1), (1, 0, 0), None),
            ((10, 0, 0), (1, 0, 0), Some(10)),
            ((10, 0, 0), (4, 0, 0), None),
            ((0, 24, 0), (4, 0, 0), None),
            ((6, 8, 9), (3, 1, 3), None),
            ((6, 8, 12), (3, 4, 6), Some(2)),
        ];

        for (lhs, rhs, expected) in cases {
            let lhs = IntervalUnit::new(lhs.0, lhs.1, lhs.2 as i64);
            let rhs = IntervalUnit::new(rhs.0, rhs.1, rhs.2 as i64);
            let result = std::panic::catch_unwind(|| {
                let actual = lhs.exact_div(&rhs);
                assert_eq!(actual, expected);
            });
            if result.is_err() {
                println!("Failed on {}.exact_div({})", lhs, rhs);
                break;
            }
        }
    }

    #[test]
    fn test_div_float() {
        let cases_int = [
            ((10, 8, 6), 2, Some((5, 4, 3))),
            ((1, 2, 33), 3, Some((0, 10, 57600011))),
            ((1, 0, 11), 10, Some((0, 3, 1))),
            ((5, 6, 7), 0, None),
        ];

        let cases_float = [
            ((10, 8, 6), 2.0f32, Some((5, 4, 3))),
            ((1, 2, 33), 3.0f32, Some((0, 10, 57600011))),
            ((10, 15, 100), 2.5f32, Some((4, 6, 40))),
            ((5, 6, 7), 0.0f32, None),
        ];

        for (lhs, rhs, expected) in cases_int {
            let lhs = IntervalUnit::new(lhs.0, lhs.1, lhs.2 as i64);
            let expected = expected.map(|x| IntervalUnit::new(x.0, x.1, x.2 as i64));

            let actual = lhs.div_float(rhs as i16);
            assert_eq!(actual, expected);

            let actual = lhs.div_float(rhs);
            assert_eq!(actual, expected);

            let actual = lhs.div_float(rhs as i64);
            assert_eq!(actual, expected);
        }

        for (lhs, rhs, expected) in cases_float {
            let lhs = IntervalUnit::new(lhs.0, lhs.1, lhs.2 as i64);
            let expected = expected.map(|x| IntervalUnit::new(x.0, x.1, x.2 as i64));

            let actual = lhs.div_float(OrderedFloat::<f32>(rhs));
            assert_eq!(actual, expected);

            let actual = lhs.div_float(OrderedFloat::<f64>(rhs as f64));
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        let a = IntervalUnit::new(123, 456, 789);
        a.serialize(&mut serializer).unwrap();
        let buf = serializer.into_inner();
        let mut deserializer = memcomparable::Deserializer::new(&buf[..]);
        assert_eq!(IntervalUnit::deserialize(&mut deserializer).unwrap(), a);
    }

    #[test]
    fn test_memcomparable() {
        let cases = [
            ((1, 2, 3), (4, 5, 6), Ordering::Less),
            ((0, 31, 0), (1, 0, 0), Ordering::Greater),
            ((1, 0, 0), (0, 0, MONTH_MS + 1), Ordering::Less),
            ((0, 1, 0), (0, 0, DAY_MS + 1), Ordering::Less),
            ((2, 3, 4), (1, 2, 4 + DAY_MS + MONTH_MS), Ordering::Equal),
        ];

        for ((lhs_months, lhs_days, lhs_ms), (rhs_months, rhs_days, rhs_ms), order) in cases {
            let lhs = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                IntervalUnit::new(lhs_months, lhs_days, lhs_ms)
                    .serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            let rhs = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                IntervalUnit::new(rhs_months, rhs_days, rhs_ms)
                    .serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            assert_eq!(lhs.cmp(&rhs), order)
        }
    }
}
