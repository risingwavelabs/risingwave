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

use chrono::{Datelike, Timelike};
use risingwave_common::types::{Date, Decimal, Time, Timestamp};
use risingwave_expr_macro::function;

use crate::{ExprError, Result};

fn extract_time<T>(time: T, unit: &str) -> Option<Decimal>
where
    T: Timelike,
{
    Some(match unit {
        "HOUR" => time.hour().into(),
        "MINUTE" => time.minute().into(),
        "SECOND" => time.second().into(),
        _ => return None,
    })
}

fn extract_date<T>(date: T, unit: &str) -> Option<Decimal>
where
    T: Datelike,
{
    Some(match unit {
        "DAY" => date.day().into(),
        "MONTH" => date.month().into(),
        "YEAR" => date.year().into(),
        // Sun = 0 and Sat = 6
        "DOW" => date.weekday().num_days_from_sunday().into(),
        "DOY" => date.ordinal().into(),
        _ => return None,
    })
}

fn invalid_unit(name: &'static str, unit: &str) -> ExprError {
    ExprError::InvalidParam {
        name,
        reason: format!("\"{unit}\" not recognized or supported"),
    }
}

#[function("extract(varchar, date) -> decimal")]
pub fn extract_from_date(unit: &str, date: Date) -> Result<Decimal> {
    extract_date(date.0, unit).ok_or_else(|| invalid_unit("date unit", unit))
}

#[function("extract(varchar, timestamp) -> decimal")]
pub fn extract_from_timestamp(unit: &str, timestamp: Timestamp) -> Result<Decimal> {
    let time = timestamp.0;

    extract_date(time, unit)
        .or_else(|| extract_time(time, unit))
        .ok_or_else(|| invalid_unit("timestamp unit", unit))
}

#[function("extract(varchar, timestamptz) -> decimal")]
pub fn extract_from_timestamptz(unit: &str, usecs: i64) -> Result<Decimal> {
    match unit {
        "EPOCH" => Ok(Decimal::from(usecs) / 1_000_000.into()),
        // TODO(#5826): all other units depend on implicit session TimeZone
        _ => Err(invalid_unit("timestamp with time zone units", unit)),
    }
}

#[function("extract(varchar, time) -> decimal")]
pub fn extract_from_time(unit: &str, time: Time) -> Result<Decimal> {
    extract_time(time.0, unit).ok_or_else(|| invalid_unit("time unit", unit))
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveDateTime};

    use super::*;

    #[test]
    fn test_date() {
        let date = Date::new(NaiveDate::parse_from_str("2021-11-22", "%Y-%m-%d").unwrap());
        assert_eq!(extract_from_date("DAY", date).unwrap(), 22.into());
        assert_eq!(extract_from_date("MONTH", date).unwrap(), 11.into());
        assert_eq!(extract_from_date("YEAR", date).unwrap(), 2021.into());
        assert_eq!(extract_from_date("DOW", date).unwrap(), 1.into());
        assert_eq!(extract_from_date("DOY", date).unwrap(), 326.into());
    }

    #[test]
    fn test_time() {
        let time = Timestamp::new(
            NaiveDateTime::parse_from_str("2021-11-22 12:4:2", "%Y-%m-%d %H:%M:%S").unwrap(),
        );
        assert_eq!(extract_from_timestamp("HOUR", time).unwrap(), 12.into());
        assert_eq!(extract_from_timestamp("MINUTE", time).unwrap(), 4.into());
        assert_eq!(extract_from_timestamp("SECOND", time).unwrap(), 2.into());
    }
}
