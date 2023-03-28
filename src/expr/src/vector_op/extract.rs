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

use chrono::{Datelike, NaiveDate, NaiveTime, Timelike};
use risingwave_common::types::{Date, Decimal, Time, Timestamp};
use risingwave_expr_macro::function;

use crate::{ExprError, Result};

fn extract_time(time: NaiveTime, unit: &str) -> Option<Decimal> {
    Some(match unit {
        "HOUR" | "hour" => time.hour().into(),
        "MINUTE" | "minute" => time.minute().into(),
        "SECOND" | "second" => time.second().into(),
        "MILLISECOND" | "millisecond" => {
            (time.second() * 1_000 + time.nanosecond() / 1_000_000).into()
        }
        "MICROSECOND" | "microsecond" => {
            (time.second() * 1_000_000 + time.nanosecond() / 1_000).into()
        }
        "EPOCH" | "epoch" => (time.num_seconds_from_midnight() as f64
            + time.nanosecond() as f64 / 1_000_000_000.0)
            .into(),
        _ => return None,
    })
}

fn extract_date(date: NaiveDate, unit: &str) -> Option<Decimal> {
    Some(match unit {
        "MILLENNIUM" | "millennium" => ((date.year() - 1) / 1000 + 1).into(),
        "CENTURY" | "century" => ((date.year() - 1) / 100 + 1).into(),
        "DECADE" | "decade" => (date.year() / 10).into(),
        "YEAR" | "year" => date.year().into(),
        "ISOYEAR" | "isoyear" => date.iso_week().year().into(),
        "QUARTER" | "quarter" => ((date.month() - 1) / 3 + 1).into(),
        "MONTH" | "month" => date.month().into(),
        "WEEK" | "week" => date.iso_week().week().into(),
        "DAY" | "day" => date.day().into(),
        "DOY" | "doy" => date.ordinal().into(),
        "DOW" | "dow" => date.weekday().num_days_from_sunday().into(),
        "ISODOW" | "isodow" => date.weekday().number_from_monday().into(),
        "EPOCH" | "epoch" => date.and_time(NaiveTime::default()).timestamp().into(),
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
    if matches!(unit, "EPOCH" | "epoch") {
        let epoch =
            timestamp.0.timestamp() as f64 + timestamp.0.nanosecond() as f64 / 1_000_000_000.0;
        return Ok(epoch.into());
    }
    extract_date(timestamp.0.date(), unit)
        .or_else(|| extract_time(timestamp.0.time(), unit))
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
        assert_eq!(extract_from_date("MILLENNIUM", date).unwrap(), 3.into());
        assert_eq!(extract_from_date("CENTURY", date).unwrap(), 21.into());
        assert_eq!(extract_from_date("DECADE", date).unwrap(), 202.into());
        assert_eq!(extract_from_date("ISOYEAR", date).unwrap(), 2021.into());
        assert_eq!(extract_from_date("QUARTER", date).unwrap(), 4.into());
        assert_eq!(extract_from_date("WEEK", date).unwrap(), 47.into());
        assert_eq!(extract_from_date("ISODOW", date).unwrap(), 1.into());
        assert_eq!(extract_from_date("EPOCH", date).unwrap(), 1637539200.into());
    }

    #[test]
    fn test_timestamp() {
        let ts = Timestamp::new(
            NaiveDateTime::parse_from_str("2021-11-22 12:4:2.575401000", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
        );
        assert_eq!(extract_from_timestamp("HOUR", ts).unwrap(), 12.into());
        assert_eq!(extract_from_timestamp("MINUTE", ts).unwrap(), 4.into());
        assert_eq!(extract_from_timestamp("SECOND", ts).unwrap(), 2.into());
        assert_eq!(
            extract_from_timestamp("MILLISECOND", ts).unwrap(),
            2575.into()
        );
        assert_eq!(
            extract_from_timestamp("MICROSECOND", ts).unwrap(),
            2575401.into()
        );
        assert_eq!(
            extract_from_timestamp("EPOCH", ts).unwrap(),
            1637582642.575401.into()
        );
    }
}
