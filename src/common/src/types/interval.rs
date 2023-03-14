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

use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::ops::{Add, Neg, Sub};

use byteorder::{BigEndian, NetworkEndian, ReadBytesExt, WriteBytesExt};
use bytes::BytesMut;
use num_traits::{CheckedAdd, CheckedNeg, CheckedSub, Zero};
use postgres_types::{to_sql_checked, FromSql};
use risingwave_pb::data::IntervalUnit as IntervalUnitProto;

use super::ops::IsNegative;
use super::to_binary::ToBinary;
use super::*;
use crate::error::{ErrorCode, Result, RwError};

/// Every interval can be represented by a `IntervalUnit`.
/// Note that the difference between Interval and Instant.
/// For example, `5 yrs 1 month 25 days 23:22:57` is a interval (Can be interpreted by Interval Unit
/// with months = 61, days = 25, usecs = (57 + 23 * 3600 + 22 * 60) * 1000000),
/// `1970-01-01 04:05:06` is a Instant or Timestamp
/// One month may contain 28/31 days. One day may contain 23/25 hours.
/// This internals is learned from PG:
/// <https://www.postgresql.org/docs/9.1/datatype-datetime.html#:~:text=field%20is%20negative.-,Internally,-interval%20values%20are>
#[derive(Debug, Clone, Copy, Default)]
pub struct IntervalUnit {
    months: i32,
    days: i32,
    usecs: i64,
}

const USECS_PER_SEC: i64 = 1_000_000;
const USECS_PER_DAY: i64 = 86400 * USECS_PER_SEC;
const USECS_PER_MONTH: i64 = 30 * USECS_PER_DAY;

impl IntervalUnit {
    /// Smallest interval value.
    pub const MIN: Self = Self {
        months: i32::MIN,
        days: i32::MIN,
        usecs: i64::MIN,
    };

    pub fn from_month_day_usec(months: i32, days: i32, usecs: i64) -> Self {
        IntervalUnit {
            months,
            days,
            usecs,
        }
    }

    pub fn get_days(&self) -> i32 {
        self.days
    }

    pub fn get_months(&self) -> i32 {
        self.months
    }

    pub fn get_usecs(&self) -> i64 {
        self.usecs
    }

    pub fn get_usecs_of_day(&self) -> u64 {
        self.usecs.rem_euclid(USECS_PER_DAY) as u64
    }

    /// Justify interval, convert 24 hours to 1 day and 30 days to 1 month.
    /// Also makes signs of all fields to be the same.
    ///
    /// <https://github.com/postgres/postgres/blob/REL_15_2/src/backend/utils/adt/timestamp.c#L2740>
    pub fn justify_interval(&self) -> Option<Self> {
        let mut v = *self;
        if v.days > 0 && v.usecs > 0 || v.days < 0 && v.usecs < 0 {
            v.months = v.months.checked_add(v.days / 30)?;
            v.days %= 30;
        }

        v.days += (v.usecs / USECS_PER_DAY) as i32;
        v.usecs %= USECS_PER_DAY;

        v.months = v.months.checked_add(v.days / 30)?;
        v.days %= 30;

        if v.months > 0 && (v.days < 0 || v.days == 0 && v.usecs < 0) {
            v.days += 30;
            v.months -= 1;
        } else if v.months < 0 && (v.days > 0 || v.days == 0 && v.usecs > 0) {
            v.days -= 30;
            v.months += 1;
        }

        if v.days > 0 && v.usecs < 0 {
            v.usecs += USECS_PER_DAY;
            v.days -= 1;
        } else if v.days < 0 && v.usecs > 0 {
            v.usecs -= USECS_PER_DAY;
            v.days += 1;
        }

        Some(v)
    }

    #[deprecated]
    fn from_total_usecs(usecs: i64) -> Self {
        let mut remaining_usecs = usecs;
        let months = remaining_usecs / USECS_PER_MONTH;
        remaining_usecs -= months * USECS_PER_MONTH;
        let days = remaining_usecs / USECS_PER_DAY;
        remaining_usecs -= days * USECS_PER_DAY;
        IntervalUnit {
            months: (months as i32),
            days: (days as i32),
            usecs: remaining_usecs,
        }
    }

    pub fn to_protobuf<T: Write>(self, output: &mut T) -> ArrayResult<usize> {
        output.write_i32::<BigEndian>(self.months)?;
        output.write_i32::<BigEndian>(self.days)?;
        output.write_i64::<BigEndian>(self.usecs)?;
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
        let usecs = self.usecs.checked_mul(rhs as i64)?;

        Some(IntervalUnit {
            months,
            days,
            usecs,
        })
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

        #[expect(deprecated)]
        let usecs = self.as_usecs_i64();
        #[expect(deprecated)]
        Some(IntervalUnit::from_total_usecs(
            (usecs as f64 / rhs).round() as i64
        ))
    }

    #[deprecated]
    fn as_usecs_i64(&self) -> i64 {
        self.months as i64 * USECS_PER_MONTH + self.days as i64 * USECS_PER_DAY + self.usecs
    }

    /// times [`IntervalUnit`] with an integer/float.
    pub fn mul_float<I>(&self, rhs: I) -> Option<Self>
    where
        I: TryInto<OrderedF64>,
    {
        let rhs = rhs.try_into().ok()?;
        let rhs = rhs.0;

        #[expect(deprecated)]
        let usecs = self.as_usecs_i64();
        #[expect(deprecated)]
        Some(IntervalUnit::from_total_usecs(
            (usecs as f64 * rhs).round() as i64
        ))
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
        check_unit(self.usecs, rhs.usecs)?;

        res
    }

    /// Checks if [`IntervalUnit`] is positive.
    pub fn is_positive(&self) -> bool {
        self > &Self::from_month_day_usec(0, 0, 0)
    }

    /// Truncate the interval to the precision of milliseconds.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::IntervalUnit;
    /// let interval: IntervalUnit = "5 years 1 mon 25 days 23:22:57.123".parse().unwrap();
    /// assert_eq!(
    ///     interval.truncate_millis().to_string(),
    ///     "5 years 1 mon 25 days 23:22:57.123"
    /// );
    /// ```
    pub const fn truncate_millis(self) -> Self {
        IntervalUnit {
            months: self.months,
            days: self.days,
            usecs: self.usecs / 1000 * 1000,
        }
    }

    /// Truncate the interval to the precision of seconds.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::IntervalUnit;
    /// let interval: IntervalUnit = "5 years 1 mon 25 days 23:22:57.123".parse().unwrap();
    /// assert_eq!(
    ///     interval.truncate_second().to_string(),
    ///     "5 years 1 mon 25 days 23:22:57"
    /// );
    /// ```
    pub const fn truncate_second(self) -> Self {
        IntervalUnit {
            months: self.months,
            days: self.days,
            usecs: self.usecs / USECS_PER_SEC * USECS_PER_SEC,
        }
    }

    /// Truncate the interval to the precision of minutes.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::IntervalUnit;
    /// let interval: IntervalUnit = "5 years 1 mon 25 days 23:22:57.123".parse().unwrap();
    /// assert_eq!(
    ///     interval.truncate_minute().to_string(),
    ///     "5 years 1 mon 25 days 23:22:00"
    /// );
    /// ```
    pub const fn truncate_minute(self) -> Self {
        IntervalUnit {
            months: self.months,
            days: self.days,
            usecs: self.usecs / USECS_PER_SEC / 60 * USECS_PER_SEC * 60,
        }
    }

    /// Truncate the interval to the precision of hours.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::IntervalUnit;
    /// let interval: IntervalUnit = "5 years 1 mon 25 days 23:22:57.123".parse().unwrap();
    /// assert_eq!(
    ///     interval.truncate_hour().to_string(),
    ///     "5 years 1 mon 25 days 23:00:00"
    /// );
    /// ```
    pub const fn truncate_hour(self) -> Self {
        IntervalUnit {
            months: self.months,
            days: self.days,
            usecs: self.usecs / USECS_PER_SEC / 60 / 60 * USECS_PER_SEC * 60 * 60,
        }
    }

    /// Truncate the interval to the precision of days.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::IntervalUnit;
    /// let interval: IntervalUnit = "5 years 1 mon 25 days 23:22:57.123".parse().unwrap();
    /// assert_eq!(interval.truncate_day().to_string(), "5 years 1 mon 25 days");
    /// ```
    pub const fn truncate_day(self) -> Self {
        IntervalUnit {
            months: self.months,
            days: self.days,
            usecs: 0,
        }
    }

    /// Truncate the interval to the precision of months.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::IntervalUnit;
    /// let interval: IntervalUnit = "5 years 1 mon 25 days 23:22:57.123".parse().unwrap();
    /// assert_eq!(interval.truncate_month().to_string(), "5 years 1 mon");
    /// ```
    pub const fn truncate_month(self) -> Self {
        IntervalUnit {
            months: self.months,
            days: 0,
            usecs: 0,
        }
    }

    /// Truncate the interval to the precision of quarters.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::IntervalUnit;
    /// let interval: IntervalUnit = "5 years 1 mon 25 days 23:22:57.123".parse().unwrap();
    /// assert_eq!(interval.truncate_quarter().to_string(), "5 years");
    /// ```
    pub const fn truncate_quarter(self) -> Self {
        IntervalUnit {
            months: self.months / 3 * 3,
            days: 0,
            usecs: 0,
        }
    }

    /// Truncate the interval to the precision of years.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::IntervalUnit;
    /// let interval: IntervalUnit = "5 years 1 mon 25 days 23:22:57.123".parse().unwrap();
    /// assert_eq!(interval.truncate_year().to_string(), "5 years");
    /// ```
    pub const fn truncate_year(self) -> Self {
        IntervalUnit {
            months: self.months / 12 * 12,
            days: 0,
            usecs: 0,
        }
    }

    /// Truncate the interval to the precision of decades.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::IntervalUnit;
    /// let interval: IntervalUnit = "15 years 1 mon 25 days 23:22:57.123".parse().unwrap();
    /// assert_eq!(interval.truncate_decade().to_string(), "10 years");
    /// ```
    pub const fn truncate_decade(self) -> Self {
        IntervalUnit {
            months: self.months / 12 / 10 * 12 * 10,
            days: 0,
            usecs: 0,
        }
    }

    /// Truncate the interval to the precision of centuries.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::IntervalUnit;
    /// let interval: IntervalUnit = "115 years 1 mon 25 days 23:22:57.123".parse().unwrap();
    /// assert_eq!(interval.truncate_century().to_string(), "100 years");
    /// ```
    pub const fn truncate_century(self) -> Self {
        IntervalUnit {
            months: self.months / 12 / 100 * 12 * 100,
            days: 0,
            usecs: 0,
        }
    }

    /// Truncate the interval to the precision of millenniums.
    ///
    /// # Example
    /// ```
    /// # use risingwave_common::types::IntervalUnit;
    /// let interval: IntervalUnit = "1115 years 1 mon 25 days 23:22:57.123".parse().unwrap();
    /// assert_eq!(interval.truncate_millennium().to_string(), "1000 years");
    /// ```
    pub const fn truncate_millennium(self) -> Self {
        IntervalUnit {
            months: self.months / 12 / 1000 * 12 * 1000,
            days: 0,
            usecs: 0,
        }
    }
}

/// A separate mod so that `use types::*` or `use interval::*` does not `use IntervalUnitTestExt` by
/// accident.
pub mod test_utils {
    use super::*;

    /// These constructors may panic when value out of bound. Only use in tests with known input.
    pub trait IntervalUnitTestExt {
        fn from_ymd(year: i32, month: i32, days: i32) -> Self;
        fn from_month(months: i32) -> Self;
        fn from_days(days: i32) -> Self;
        fn from_millis(ms: i64) -> Self;
        fn from_minutes(minutes: i64) -> Self;
    }

    impl IntervalUnitTestExt for IntervalUnit {
        #[must_use]
        fn from_ymd(year: i32, month: i32, days: i32) -> Self {
            let months = year * 12 + month;
            let days = days;
            let usecs = 0;
            IntervalUnit {
                months,
                days,
                usecs,
            }
        }

        #[must_use]
        fn from_month(months: i32) -> Self {
            IntervalUnit {
                months,
                ..Default::default()
            }
        }

        #[must_use]
        fn from_days(days: i32) -> Self {
            Self {
                days,
                ..Default::default()
            }
        }

        #[must_use]
        fn from_millis(ms: i64) -> Self {
            Self {
                usecs: ms * 1000,
                ..Default::default()
            }
        }

        #[must_use]
        fn from_minutes(minutes: i64) -> Self {
            Self {
                usecs: USECS_PER_SEC * 60 * minutes,
                ..Default::default()
            }
        }
    }
}

/// Wrapper so that `Debug for IntervalUnitDisplay` would use the concise format of `Display for
/// IntervalUnit`.
#[derive(Clone, Copy)]
pub struct IntervalUnitDisplay<'a> {
    pub core: &'a IntervalUnit,
}

impl std::fmt::Display for IntervalUnitDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (self as &dyn std::fmt::Debug).fmt(f)
    }
}

impl std::fmt::Debug for IntervalUnitDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.core)
    }
}

/// <https://github.com/postgres/postgres/blob/REL_15_2/src/backend/utils/adt/timestamp.c#L2384>
///
/// Do NOT make this `pub` as the assumption of 1 month = 30 days and 1 day = 24 hours does not
/// always hold in other places.
#[derive(PartialEq, Eq, Hash, PartialOrd, Ord)]
struct IntervalCmpValue(i128);

impl From<IntervalUnit> for IntervalCmpValue {
    fn from(value: IntervalUnit) -> Self {
        let days = (value.days as i64) + 30i64 * (value.months as i64);
        let usecs = (value.usecs as i128) + (USECS_PER_DAY as i128) * (days as i128);
        Self(usecs)
    }
}

impl Ord for IntervalUnit {
    fn cmp(&self, other: &Self) -> Ordering {
        IntervalCmpValue::from(*self).cmp(&(*other).into())
    }
}

impl PartialOrd for IntervalUnit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for IntervalUnit {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for IntervalUnit {}

impl Hash for IntervalUnit {
    fn hash<H: Hasher>(&self, state: &mut H) {
        IntervalCmpValue::from(*self).hash(state);
    }
}

/// Loss of information during the process due to `justify`. Only intended for memcomparable
/// encoding.
impl Serialize for IntervalUnit {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let cmp_value = IntervalCmpValue::from(*self);
        // split i128 as (i64, u64), which is equivalent
        (
            (cmp_value.0 >> 64) as i64,
            cmp_value.0 as u64, // truncate to get the lower part
        )
            .serialize(serializer)
    }
}

impl IntervalCmpValue {
    fn as_justified(&self) -> Option<IntervalUnit> {
        let usecs = (self.0 % (USECS_PER_DAY as i128)) as i64;
        let remaining_days = self.0 / (USECS_PER_DAY as i128);
        let days = (remaining_days % 30) as i32;
        let months = (remaining_days / 30).try_into().ok()?;
        Some(IntervalUnit::from_month_day_usec(months, days, usecs))
    }

    fn as_alternate(&self) -> Option<IntervalUnit> {
        match self.0.cmp(&0) {
            Ordering::Equal => Some(IntervalUnit::from_month_day_usec(0, 0, 0)),
            Ordering::Greater => {
                let remaining_usecs = self.0;
                let mut usecs = (remaining_usecs % (USECS_PER_DAY as i128)) as i64;
                let mut remaining_days = remaining_usecs / (USECS_PER_DAY as i128);
                let extra_days = ((i64::MAX - usecs) / USECS_PER_DAY)
                    .min(remaining_days.try_into().unwrap_or(i64::MAX));
                usecs += extra_days * USECS_PER_DAY;
                remaining_days -= extra_days as i128;

                let mut days = (remaining_days % 30) as i32;
                let mut remaining_months = remaining_days / 30;
                let extra_months =
                    ((i32::MAX - days) / 30).min(remaining_months.try_into().unwrap_or(i32::MAX));
                days += extra_months * 30;
                remaining_months -= extra_months as i128;

                let months = remaining_months.try_into().ok()?;
                Some(IntervalUnit::from_month_day_usec(months, days, usecs))
            }
            Ordering::Less => {
                let remaining_usecs = self.0;
                let mut usecs = (remaining_usecs % (USECS_PER_DAY as i128)) as i64;
                let mut remaining_days = remaining_usecs / (USECS_PER_DAY as i128);
                let extra_days = ((i64::MIN - usecs) / USECS_PER_DAY)
                    .max(remaining_days.try_into().unwrap_or(i64::MIN));
                usecs += extra_days * USECS_PER_DAY;
                remaining_days -= extra_days as i128;

                let mut days = (remaining_days % 30) as i32;
                let mut remaining_months = remaining_days / 30;
                let extra_months =
                    ((i32::MIN - days) / 30).max(remaining_months.try_into().unwrap_or(i32::MIN));
                days += extra_months * 30;
                remaining_months -= extra_months as i128;

                let months = remaining_months.try_into().ok()?;
                Some(IntervalUnit::from_month_day_usec(months, days, usecs))
            }
        }
    }
}

impl<'de> Deserialize<'de> for IntervalUnit {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let (hi, lo) = <(i64, u64)>::deserialize(deserializer)?;
        let cmp_value = IntervalCmpValue(((hi as i128) << 64) | (lo as i128));
        let interval = cmp_value
            .as_justified()
            .or_else(|| cmp_value.as_alternate());
        interval.ok_or_else(|| {
            use serde::de::Error as _;
            D::Error::custom("memcomparable deserialize interval overflow")
        })
    }
}

impl crate::hash::HashKeySerDe<'_> for IntervalUnit {
    type S = [u8; 16];

    fn serialize(self) -> Self::S {
        let cmp_value = IntervalCmpValue::from(self);
        cmp_value.0.to_ne_bytes()
    }

    fn deserialize<R: std::io::Read>(source: &mut R) -> Self {
        let value = Self::read_fixed_size_bytes::<R, 16>(source);
        let cmp_value = IntervalCmpValue(i128::from_ne_bytes(value));
        cmp_value
            .as_justified()
            .or_else(|| cmp_value.as_alternate())
            .expect("HashKey deserialize interval overflow")
    }
}

/// Duplicated logic only used by `HopWindow`. See #8452.
#[expect(clippy::from_over_into)]
impl Into<IntervalUnitProto> for IntervalUnit {
    fn into(self) -> IntervalUnitProto {
        IntervalUnitProto {
            months: self.months,
            days: self.days,
            usecs: self.usecs,
        }
    }
}

impl From<&'_ IntervalUnitProto> for IntervalUnit {
    fn from(p: &'_ IntervalUnitProto) -> Self {
        Self {
            months: p.months,
            days: p.days,
            usecs: p.usecs,
        }
    }
}

impl From<NaiveTimeWrapper> for IntervalUnit {
    fn from(time: NaiveTimeWrapper) -> Self {
        let mut usecs: i64 = (time.0.num_seconds_from_midnight() as i64) * USECS_PER_SEC;
        usecs += (time.0.nanosecond() / 1000) as i64;
        Self {
            months: 0,
            days: 0,
            usecs,
        }
    }
}

impl Add for IntervalUnit {
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        let months = self.months + rhs.months;
        let days = self.days + rhs.days;
        let usecs = self.usecs + rhs.usecs;
        IntervalUnit {
            months,
            days,
            usecs,
        }
    }
}

impl CheckedNeg for IntervalUnit {
    fn checked_neg(&self) -> Option<Self> {
        let months = self.months.checked_neg()?;
        let days = self.days.checked_neg()?;
        let usecs = self.usecs.checked_neg()?;
        Some(IntervalUnit {
            months,
            days,
            usecs,
        })
    }
}

impl CheckedAdd for IntervalUnit {
    fn checked_add(&self, other: &Self) -> Option<Self> {
        let months = self.months.checked_add(other.months)?;
        let days = self.days.checked_add(other.days)?;
        let usecs = self.usecs.checked_add(other.usecs)?;
        Some(IntervalUnit {
            months,
            days,
            usecs,
        })
    }
}

impl Sub for IntervalUnit {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        let months = self.months - rhs.months;
        let days = self.days - rhs.days;
        let usecs = self.usecs - rhs.usecs;
        IntervalUnit {
            months,
            days,
            usecs,
        }
    }
}

impl CheckedSub for IntervalUnit {
    fn checked_sub(&self, other: &Self) -> Option<Self> {
        let months = self.months.checked_sub(other.months)?;
        let days = self.days.checked_sub(other.days)?;
        let usecs = self.usecs.checked_sub(other.usecs)?;
        Some(IntervalUnit {
            months,
            days,
            usecs,
        })
    }
}

impl Zero for IntervalUnit {
    fn zero() -> Self {
        Self::from_month_day_usec(0, 0, 0)
    }

    fn is_zero(&self) -> bool {
        self.months == 0 && self.days == 0 && self.usecs == 0
    }
}

impl IsNegative for IntervalUnit {
    fn is_negative(&self) -> bool {
        self < &Self::from_month_day_usec(0, 0, 0)
    }
}

impl Neg for IntervalUnit {
    type Output = Self;

    fn neg(self) -> Self {
        Self {
            months: -self.months,
            days: -self.days,
            usecs: -self.usecs,
        }
    }
}

impl ToText for crate::types::IntervalUnit {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{self}")
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        match ty {
            DataType::Interval => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl Display for IntervalUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let years = self.months / 12;
        let months = self.months % 12;
        let days = self.days;
        let mut space = false;
        let mut write = |arg: std::fmt::Arguments<'_>| {
            if space {
                write!(f, " ")?;
            }
            write!(f, "{arg}")?;
            space = true;
            Ok(())
        };
        if years == 1 {
            write(format_args!("{years} year"))?;
        } else if years != 0 {
            write(format_args!("{years} years"))?;
        }
        if months == 1 {
            write(format_args!("{months} mon"))?;
        } else if months != 0 {
            write(format_args!("{months} mons"))?;
        }
        if days == 1 {
            write(format_args!("{days} day"))?;
        } else if days != 0 {
            write(format_args!("{days} days"))?;
        }
        if self.usecs != 0 || self.months == 0 && self.days == 0 {
            let usecs = self.usecs.abs();
            let ms = usecs / 1000;
            let hours = ms / 1000 / 3600;
            let minutes = (ms / 1000 / 60) % 60;
            let seconds = ms % 60000 / 1000;
            let secs_fract = usecs % USECS_PER_SEC;

            if self.usecs < 0 {
                write(format_args!("-{hours:0>2}:{minutes:0>2}:{seconds:0>2}"))?;
            } else {
                write(format_args!("{hours:0>2}:{minutes:0>2}:{seconds:0>2}"))?;
            }
            if secs_fract != 0 {
                let mut buf = [0u8; 7];
                write!(buf.as_mut_slice(), ".{:06}", secs_fract).unwrap();
                write!(
                    f,
                    "{}",
                    std::str::from_utf8(&buf).unwrap().trim_end_matches('0')
                )?;
            }
        }
        Ok(())
    }
}

impl ToSql for IntervalUnit {
    to_sql_checked!();

    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> std::result::Result<IsNull, Box<dyn Error + 'static + Send + Sync>> {
        // refer: https://github.com/postgres/postgres/blob/517bf2d91/src/backend/utils/adt/timestamp.c#L1008
        out.put_i64(self.usecs);
        out.put_i32(self.days);
        out.put_i32(self.months);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INTERVAL)
    }
}

impl<'a> FromSql<'a> for IntervalUnit {
    fn from_sql(
        _: &Type,
        mut raw: &'a [u8],
    ) -> std::result::Result<IntervalUnit, Box<dyn Error + Sync + Send>> {
        let usecs = raw.read_i64::<NetworkEndian>()?;
        let days = raw.read_i32::<NetworkEndian>()?;
        let months = raw.read_i32::<NetworkEndian>()?;
        Ok(IntervalUnit::from_month_day_usec(months, days, usecs))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INTERVAL)
    }
}

impl ToBinary for IntervalUnit {
    fn to_binary_with_type(&self, ty: &DataType) -> Result<Option<Bytes>> {
        match ty {
            DataType::Interval => {
                let mut output = BytesMut::new();
                self.to_sql(&Type::ANY, &mut output).unwrap();
                Ok(Some(output.freeze()))
            }
            _ => unreachable!(),
        }
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
                Some(IntervalUnit::from_month_day_usec(months as i32, 0, 0))
            }
            Month => Some(IntervalUnit::from_month_day_usec(num as i32, 0, 0)),
            Day => Some(IntervalUnit::from_month_day_usec(0, num as i32, 0)),
            Hour => {
                let usecs = num.checked_mul(3600 * USECS_PER_SEC)?;
                Some(IntervalUnit::from_month_day_usec(0, 0, usecs))
            }
            Minute => {
                let usecs = num.checked_mul(60 * USECS_PER_SEC)?;
                Some(IntervalUnit::from_month_day_usec(0, 0, usecs))
            }
            Second => {
                let usecs = num.checked_mul(USECS_PER_SEC)?;
                Some(IntervalUnit::from_month_day_usec(0, 0, usecs))
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
        let mut result = IntervalUnit::from_month_day_usec(0, 0, 0);
        while let Some(num) = token_iter.next() && let Some(interval_unit) = token_iter.next() {
            match (num, interval_unit) {
                (TimeStrToken::Num(num), TimeStrToken::TimeUnit(interval_unit)) => {
                    result = result + (|| match interval_unit {
                        Year => {
                            let months = num.checked_mul(12)?;
                            Some(IntervalUnit::from_month_day_usec(months as i32, 0, 0))
                        }
                        Month => Some(IntervalUnit::from_month_day_usec(num as i32, 0, 0)),
                        Day => Some(IntervalUnit::from_month_day_usec(0, num as i32, 0)),
                        Hour => {
                            let usecs = num.checked_mul(3600 * USECS_PER_SEC)?;
                            Some(IntervalUnit::from_month_day_usec(0, 0, usecs))
                        }
                        Minute => {
                            let usecs = num.checked_mul(60 * USECS_PER_SEC)?;
                            Some(IntervalUnit::from_month_day_usec(0, 0, usecs))
                        }
                        Second => {
                            let usecs = num.checked_mul(USECS_PER_SEC)?;
                            Some(IntervalUnit::from_month_day_usec(0, 0, usecs))
                        }
                    })()
                    .ok_or_else(|| ErrorCode::InvalidInputSyntax(format!("Invalid interval {}.", s)))?;
                }
                (TimeStrToken::Second(second), TimeStrToken::TimeUnit(interval_unit)) => {
                    result = result + match interval_unit {
                        Second => {
                            // If unsatisfied precision is passed as input, we should not return None (Error).
                            let usecs = (second.into_inner() * (USECS_PER_SEC as f64)).round() as i64;
                            Some(IntervalUnit::from_month_day_usec(0, 0, usecs))
                        }
                        _ => None,
                    }
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
    use interval::test_utils::IntervalUnitTestExt;

    use super::*;
    use crate::types::ordered_float::OrderedFloat;

    #[test]
    fn test_parse() {
        let interval = "04:00:00".parse::<IntervalUnit>().unwrap();
        assert_eq!(interval, IntervalUnit::from_millis(4 * 3600 * 1000));

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
        assert_eq!(
            IntervalUnit::from_month_day_usec(
                -14,
                3,
                (11 * 3600 + 45 * 60 + 14) * USECS_PER_SEC + 233
            )
            .to_string(),
            "-1 years -2 mons 3 days 11:45:14.000233"
        );
        assert_eq!(
            IntervalUnit::from_month_day_usec(-14, 3, 0).to_string(),
            "-1 years -2 mons 3 days"
        );
        assert_eq!(IntervalUnit::default().to_string(), "00:00:00");
        assert_eq!(
            IntervalUnit::from_month_day_usec(
                -14,
                3,
                -((11 * 3600 + 45 * 60 + 14) * USECS_PER_SEC + 233)
            )
            .to_string(),
            "-1 years -2 mons 3 days -11:45:14.000233"
        );
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
            let lhs = IntervalUnit::from_month_day_usec(lhs.0, lhs.1, lhs.2 as i64);
            let rhs = IntervalUnit::from_month_day_usec(rhs.0, rhs.1, rhs.2 as i64);
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
            ((1, 2, 33), 3, Some((0, 10, 57600000011i64))),
            ((1, 0, 11), 10, Some((0, 3, 1))),
            ((5, 6, 7), 0, None),
        ];

        let cases_float = [
            ((10, 8, 6), 2.0f32, Some((5, 4, 3))),
            ((1, 2, 33), 3.0f32, Some((0, 10, 57600000011i64))),
            ((10, 15, 100), 2.5f32, Some((4, 6, 40))),
            ((5, 6, 7), 0.0f32, None),
        ];

        for (lhs, rhs, expected) in cases_int {
            let lhs = IntervalUnit::from_month_day_usec(lhs.0, lhs.1, lhs.2 as i64);
            let expected = expected.map(|x| IntervalUnit::from_month_day_usec(x.0, x.1, x.2));

            let actual = lhs.div_float(rhs as i16);
            assert_eq!(actual, expected);

            let actual = lhs.div_float(rhs);
            assert_eq!(actual, expected);

            let actual = lhs.div_float(rhs as i64);
            assert_eq!(actual, expected);
        }

        for (lhs, rhs, expected) in cases_float {
            let lhs = IntervalUnit::from_month_day_usec(lhs.0, lhs.1, lhs.2 as i64);
            let expected = expected.map(|x| IntervalUnit::from_month_day_usec(x.0, x.1, x.2));

            let actual = lhs.div_float(OrderedFloat::<f32>(rhs));
            assert_eq!(actual, expected);

            let actual = lhs.div_float(OrderedFloat::<f64>(rhs as f64));
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn test_serialize_deserialize() {
        let mut serializer = memcomparable::Serializer::new(vec![]);
        let a = IntervalUnit::from_month_day_usec(123, 456, 789);
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
            ((1, 0, 0), (0, 0, USECS_PER_MONTH + 1), Ordering::Less),
            ((0, 1, 0), (0, 0, USECS_PER_DAY + 1), Ordering::Less),
            (
                (2, 3, 4),
                (1, 2, 4 + USECS_PER_DAY + USECS_PER_MONTH),
                Ordering::Equal,
            ),
        ];

        for ((lhs_months, lhs_days, lhs_usecs), (rhs_months, rhs_days, rhs_usecs), order) in cases {
            let lhs = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                IntervalUnit::from_month_day_usec(lhs_months, lhs_days, lhs_usecs)
                    .serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            let rhs = {
                let mut serializer = memcomparable::Serializer::new(vec![]);
                IntervalUnit::from_month_day_usec(rhs_months, rhs_days, rhs_usecs)
                    .serialize(&mut serializer)
                    .unwrap();
                serializer.into_inner()
            };
            assert_eq!(lhs.cmp(&rhs), order)
        }
    }
}
