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

use std::hash::Hash;
use std::io::Write;

use bytes::{Bytes, BytesMut};
use chrono::{Datelike, Days, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Weekday};
use postgres_types::{ToSql, Type};
use thiserror::Error;

use super::to_binary::ToBinary;
use super::to_text::ToText;
use super::{CheckedAdd, DataType, Interval};
use crate::array::ArrayResult;

/// The same as `NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce()`.
/// Minus this magic number to store the number of days since 1970-01-01.
pub const UNIX_EPOCH_DAYS: i32 = 719_163;
const LEAP_DAYS: &[i32] = &[0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
const NORMAL_DAYS: &[i32] = &[0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

macro_rules! impl_chrono_wrapper {
    ($variant_name:ident, $chrono:ty) => {
        #[derive(
            Clone,
            Copy,
            Debug,
            Default,
            PartialEq,
            Eq,
            PartialOrd,
            Ord,
            Hash,
            parse_display::Display,
        )]
        #[repr(transparent)]
        pub struct $variant_name(pub $chrono);

        impl $variant_name {
            pub fn new(data: $chrono) -> Self {
                $variant_name(data)
            }
        }

        impl std::str::FromStr for $variant_name {
            type Err = chrono::ParseError;

            fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
                Ok($variant_name(s.parse()?))
            }
        }

        impl From<$chrono> for $variant_name {
            fn from(data: $chrono) -> Self {
                $variant_name(data)
            }
        }
    };
}

impl_chrono_wrapper!(Date, NaiveDate);
impl_chrono_wrapper!(Timestamp, NaiveDateTime);
impl_chrono_wrapper!(Time, NaiveTime);

#[derive(Copy, Clone, Debug, Error)]
enum InvalidParamsErrorKind {
    #[error("Invalid date: days: {days}")]
    Date { days: i32 },
    #[error("Invalid time: secs: {secs}, nanoseconds: {nsecs}")]
    Time { secs: u32, nsecs: u32 },
    #[error("Invalid datetime: seconds: {secs}, nanoseconds: {nsecs}")]
    DateTime { secs: i64, nsecs: u32 },
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct InvalidParamsError(#[from] InvalidParamsErrorKind);

impl InvalidParamsError {
    pub fn date(days: i32) -> Self {
        InvalidParamsErrorKind::Date { days }.into()
    }

    pub fn time(secs: u32, nsecs: u32) -> Self {
        InvalidParamsErrorKind::Time { secs, nsecs }.into()
    }

    pub fn datetime(secs: i64, nsecs: u32) -> Self {
        InvalidParamsErrorKind::DateTime { secs, nsecs }.into()
    }
}

type Result<T> = std::result::Result<T, InvalidParamsError>;

impl ToText for Date {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        match ty {
            super::DataType::Date => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl ToText for Time {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        match ty {
            super::DataType::Time => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl ToText for Timestamp {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        match ty {
            super::DataType::Timestamp => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl ToBinary for Date {
    fn to_binary_with_type(&self, ty: &DataType) -> crate::error::Result<Option<Bytes>> {
        match ty {
            super::DataType::Date => {
                let mut output = BytesMut::new();
                self.0.to_sql(&Type::ANY, &mut output).unwrap();
                Ok(Some(output.freeze()))
            }
            _ => unreachable!(),
        }
    }
}

impl ToBinary for Time {
    fn to_binary_with_type(&self, ty: &DataType) -> crate::error::Result<Option<Bytes>> {
        match ty {
            super::DataType::Time => {
                let mut output = BytesMut::new();
                self.0.to_sql(&Type::ANY, &mut output).unwrap();
                Ok(Some(output.freeze()))
            }
            _ => unreachable!(),
        }
    }
}

impl ToBinary for Timestamp {
    fn to_binary_with_type(&self, ty: &DataType) -> crate::error::Result<Option<Bytes>> {
        match ty {
            super::DataType::Timestamp => {
                let mut output = BytesMut::new();
                self.0.to_sql(&Type::ANY, &mut output).unwrap();
                Ok(Some(output.freeze()))
            }
            _ => unreachable!(),
        }
    }
}

impl Date {
    pub fn with_days(days: i32) -> Result<Self> {
        Ok(Date::new(
            NaiveDate::from_num_days_from_ce_opt(days)
                .ok_or_else(|| InvalidParamsError::date(days))?,
        ))
    }

    pub fn with_days_since_unix_epoch(days: i32) -> Result<Self> {
        Ok(Date::new(
            NaiveDate::from_num_days_from_ce_opt(days)
                .ok_or_else(|| InvalidParamsError::date(days))?
                .checked_add_days(Days::new(UNIX_EPOCH_DAYS as u64))
                .ok_or_else(|| InvalidParamsError::date(days))?,
        ))
    }

    pub fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output
            .write(&(self.0.num_days_from_ce()).to_be_bytes())
            .map_err(Into::into)
    }

    pub fn from_ymd_uncheck(year: i32, month: u32, day: u32) -> Self {
        Self::new(NaiveDate::from_ymd_opt(year, month, day).unwrap())
    }

    pub fn from_num_days_from_ce_uncheck(days: i32) -> Self {
        Self::with_days(days).unwrap()
    }

    pub fn and_hms_uncheck(self, hour: u32, min: u32, sec: u32) -> Timestamp {
        self.and_hms_micro_uncheck(hour, min, sec, 0)
    }

    pub fn and_hms_micro_uncheck(self, hour: u32, min: u32, sec: u32, micro: u32) -> Timestamp {
        Timestamp::new(
            self.0
                .and_time(Time::from_hms_micro_uncheck(hour, min, sec, micro).0),
        )
    }
}

impl Time {
    pub fn with_secs_nano(secs: u32, nano: u32) -> Result<Self> {
        Ok(Time::new(
            NaiveTime::from_num_seconds_from_midnight_opt(secs, nano)
                .ok_or_else(|| InvalidParamsError::time(secs, nano))?,
        ))
    }

    pub fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output
            .write(
                &(self.0.num_seconds_from_midnight() as u64 * 1_000_000_000
                    + self.0.nanosecond() as u64)
                    .to_be_bytes(),
            )
            .map_err(Into::into)
    }

    pub fn with_nano(nano: u64) -> Result<Self> {
        let secs = (nano / 1_000_000_000) as u32;
        let nano = (nano % 1_000_000_000) as u32;
        Self::with_secs_nano(secs, nano).map_err(Into::into)
    }

    pub fn with_milli(milli: u32) -> Result<Self> {
        let secs = milli / 1_000;
        let nano = (milli % 1_000) * 1_000_000;
        Self::with_secs_nano(secs, nano)
    }

    pub fn from_hms_uncheck(hour: u32, min: u32, sec: u32) -> Self {
        Self::from_hms_nano_uncheck(hour, min, sec, 0)
    }

    pub fn from_hms_micro_uncheck(hour: u32, min: u32, sec: u32, micro: u32) -> Self {
        Self::new(NaiveTime::from_hms_micro_opt(hour, min, sec, micro).unwrap())
    }

    pub fn from_hms_nano_uncheck(hour: u32, min: u32, sec: u32, nano: u32) -> Self {
        Self::new(NaiveTime::from_hms_nano_opt(hour, min, sec, nano).unwrap())
    }

    pub fn from_num_seconds_from_midnight_uncheck(secs: u32, nano: u32) -> Self {
        Self::new(NaiveTime::from_num_seconds_from_midnight_opt(secs, nano).unwrap())
    }
}

impl Timestamp {
    pub fn with_secs_nsecs(secs: i64, nsecs: u32) -> Result<Self> {
        Ok(Timestamp::new({
            NaiveDateTime::from_timestamp_opt(secs, nsecs)
                .ok_or_else(|| InvalidParamsError::datetime(secs, nsecs))?
        }))
    }

    /// Although `Timestamp` takes 12 bytes, we drop 4 bytes in protobuf encoding.
    pub fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output
            .write(&(self.0.timestamp_micros()).to_be_bytes())
            .map_err(Into::into)
    }

    pub fn with_macros(timestamp_micros: i64) -> Result<Self> {
        let secs = timestamp_micros.div_euclid(1_000_000);
        let nsecs = timestamp_micros.rem_euclid(1_000_000) * 1000;
        Self::with_secs_nsecs(secs, nsecs as u32)
    }

    pub fn from_timestamp_uncheck(secs: i64, nsecs: u32) -> Self {
        Self::new(NaiveDateTime::from_timestamp_opt(secs, nsecs).unwrap())
    }

    /// Truncate the timestamp to the precision of microseconds.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_micros().to_string(),
    ///     "2001-05-16 20:38:40.123456"
    /// );
    /// ```
    pub fn truncate_micros(self) -> Self {
        Self::new(
            self.0
                .with_nanosecond(self.0.nanosecond() / 1000 * 1000)
                .unwrap(),
        )
    }

    /// Truncate the timestamp to the precision of milliseconds.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_millis().to_string(),
    ///     "2001-05-16 20:38:40.123"
    /// );
    /// ```
    pub fn truncate_millis(self) -> Self {
        Self::new(
            self.0
                .with_nanosecond(self.0.nanosecond() / 1_000_000 * 1_000_000)
                .unwrap(),
        )
    }

    /// Truncate the timestamp to the precision of seconds.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_second().to_string(),
    ///     "2001-05-16 20:38:40"
    /// );
    /// ```
    pub fn truncate_second(self) -> Self {
        Self::new(self.0.with_nanosecond(0).unwrap())
    }

    /// Truncate the timestamp to the precision of minutes.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_minute().to_string(),
    ///     "2001-05-16 20:38:00"
    /// );
    /// ```
    pub fn truncate_minute(self) -> Self {
        Date::new(self.0.date()).and_hms_uncheck(self.0.hour(), self.0.minute(), 0)
    }

    /// Truncate the timestamp to the precision of hours.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_hour().to_string(),
    ///     "2001-05-16 20:00:00"
    /// );
    /// ```
    pub fn truncate_hour(self) -> Self {
        Date::new(self.0.date()).and_hms_uncheck(self.0.hour(), 0, 0)
    }

    /// Truncate the timestamp to the precision of days.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_day().to_string(),
    ///     "2001-05-16 00:00:00"
    /// );
    /// ```
    pub fn truncate_day(self) -> Self {
        Date::new(self.0.date()).into()
    }

    /// Truncate the timestamp to the precision of weeks.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_week().to_string(),
    ///     "2001-05-14 00:00:00"
    /// );
    /// ```
    pub fn truncate_week(self) -> Self {
        Date::new(self.0.date().week(Weekday::Mon).first_day()).into()
    }

    /// Truncate the timestamp to the precision of months.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_month().to_string(),
    ///     "2001-05-01 00:00:00"
    /// );
    /// ```
    pub fn truncate_month(self) -> Self {
        Date::new(self.0.date().with_day(1).unwrap()).into()
    }

    /// Truncate the timestamp to the precision of quarters.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_quarter().to_string(),
    ///     "2001-04-01 00:00:00"
    /// );
    /// ```
    pub fn truncate_quarter(self) -> Self {
        Date::from_ymd_uncheck(self.0.year(), self.0.month0() / 3 * 3 + 1, 1).into()
    }

    /// Truncate the timestamp to the precision of years.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_year().to_string(),
    ///     "2001-01-01 00:00:00"
    /// );
    /// ```
    pub fn truncate_year(self) -> Self {
        Date::from_ymd_uncheck(self.0.year(), 1, 1).into()
    }

    /// Truncate the timestamp to the precision of decades.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "2001-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_decade().to_string(),
    ///     "2000-01-01 00:00:00"
    /// );
    /// ```
    pub fn truncate_decade(self) -> Self {
        Date::from_ymd_uncheck(self.0.year() / 10 * 10, 1, 1).into()
    }

    /// Truncate the timestamp to the precision of centuries.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "3202-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_century().to_string(),
    ///     "3201-01-01 00:00:00"
    /// );
    /// ```
    pub fn truncate_century(self) -> Self {
        Date::from_ymd_uncheck((self.0.year() - 1) / 100 * 100 + 1, 1, 1).into()
    }

    /// Truncate the timestamp to the precision of millenniums.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::Timestamp;
    /// let ts = "3202-05-16T20:38:40.123456789".parse().unwrap();
    /// assert_eq!(
    ///     Timestamp::new(ts).truncate_millennium().to_string(),
    ///     "3001-01-01 00:00:00"
    /// );
    /// ```
    pub fn truncate_millennium(self) -> Self {
        Date::from_ymd_uncheck((self.0.year() - 1) / 1000 * 1000 + 1, 1, 1).into()
    }
}

impl From<Date> for Timestamp {
    fn from(date: Date) -> Self {
        date.and_hms_uncheck(0, 0, 0)
    }
}

/// return the days of the `year-month`
fn get_mouth_days(year: i32, month: usize) -> i32 {
    if is_leap_year(year) {
        LEAP_DAYS[month]
    } else {
        NORMAL_DAYS[month]
    }
}

fn is_leap_year(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

impl CheckedAdd<Interval> for Timestamp {
    type Output = Timestamp;

    fn checked_add(self, rhs: Interval) -> Option<Timestamp> {
        let mut date = self.0.date();
        if rhs.months() != 0 {
            // NaiveDate don't support add months. We need calculate manually
            let mut day = date.day() as i32;
            let mut month = date.month() as i32;
            let mut year = date.year();
            // Calculate the number of year in this interval
            let interval_months = rhs.months();
            let year_diff = interval_months / 12;
            year += year_diff;

            // Calculate the number of month in this interval except the added year
            // The range of month_diff is (-12, 12) (The month is negative when the interval is
            // negative)
            let month_diff = interval_months - year_diff * 12;
            // The range of new month is (-12, 24) ( original month:[1, 12] + month_diff:(-12, 12) )
            month += month_diff;
            // Process the overflow months
            if month > 12 {
                year += 1;
                month -= 12;
            } else if month <= 0 {
                year -= 1;
                month += 12;
            }

            // Fix the days after changing date.
            // For example, 1970.1.31 + 1 month = 1970.2.28
            day = day.min(get_mouth_days(year, month as usize));
            date = NaiveDate::from_ymd_opt(year, month as u32, day as u32)?;
        }
        let mut datetime = NaiveDateTime::new(date, self.0.time());
        datetime = datetime.checked_add_signed(Duration::days(rhs.days().into()))?;
        datetime = datetime.checked_add_signed(Duration::microseconds(rhs.usecs()))?;

        Some(Timestamp::new(datetime))
    }
}
