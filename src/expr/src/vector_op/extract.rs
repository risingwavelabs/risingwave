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

use chrono::{Datelike, NaiveTime, Timelike};
use risingwave_common::types::{Date, Decimal, Time, Timestamp, F64};
use risingwave_expr_macro::function;

use crate::{ExprError, Result};

fn extract_date(date: impl Datelike, unit: &str) -> Option<Decimal> {
    Some(if unit.eq_ignore_ascii_case("MILLENNIUM") {
        ((date.year() - 1) / 1000 + 1).into()
    } else if unit.eq_ignore_ascii_case("CENTURY") {
        ((date.year() - 1) / 100 + 1).into()
    } else if unit.eq_ignore_ascii_case("DECADE") {
        (date.year() / 10).into()
    } else if unit.eq_ignore_ascii_case("YEAR") {
        date.year().into()
    } else if unit.eq_ignore_ascii_case("ISOYEAR") {
        date.iso_week().year().into()
    } else if unit.eq_ignore_ascii_case("QUARTER") {
        ((date.month() - 1) / 3 + 1).into()
    } else if unit.eq_ignore_ascii_case("MONTH") {
        date.month().into()
    } else if unit.eq_ignore_ascii_case("WEEK") {
        date.iso_week().week().into()
    } else if unit.eq_ignore_ascii_case("DAY") {
        date.day().into()
    } else if unit.eq_ignore_ascii_case("DOY") {
        date.ordinal().into()
    } else if unit.eq_ignore_ascii_case("DOW") {
        date.weekday().num_days_from_sunday().into()
    } else if unit.eq_ignore_ascii_case("ISODOW") {
        date.weekday().number_from_monday().into()
    } else {
        return None;
    })
}

fn extract_time(time: impl Timelike, unit: &str) -> Option<Decimal> {
    let nanoseconds = || time.second() as u64 * 1_000_000_000 + time.nanosecond() as u64;
    Some(if unit.eq_ignore_ascii_case("hour") {
        time.hour().into()
    } else if unit.eq_ignore_ascii_case("minute") {
        time.minute().into()
    } else if unit.eq_ignore_ascii_case("second") {
        Decimal::from_i128_with_scale(nanoseconds() as i128, 9)
    } else if unit.eq_ignore_ascii_case("millisecond") {
        Decimal::from_i128_with_scale(nanoseconds() as i128, 6)
    } else if unit.eq_ignore_ascii_case("microsecond") {
        Decimal::from_i128_with_scale(nanoseconds() as i128, 3)
    } else {
        return None;
    })
}

#[function("extract(varchar, date) -> decimal")]
pub fn extract_from_date(unit: &str, date: Date) -> Result<Decimal> {
    if unit.eq_ignore_ascii_case("epoch") {
        let epoch = date.0.and_time(NaiveTime::default()).timestamp();
        return Ok(epoch.into());
    } else if unit.eq_ignore_ascii_case("julian") {
        const UNIX_EPOCH_DAY: i32 = 719_163;
        let julian = date.0.num_days_from_ce() - UNIX_EPOCH_DAY + 2_440_588;
        return Ok(julian.into());
    };
    extract_date(date.0, unit).ok_or_else(|| invalid_unit("date unit", unit))
}

#[function("extract(varchar, time) -> decimal")]
pub fn extract_from_time(unit: &str, time: Time) -> Result<Decimal> {
    extract_time(time.0, unit).ok_or_else(|| invalid_unit("time unit", unit))
}

#[function("extract(varchar, timestamp) -> decimal")]
pub fn extract_from_timestamp(unit: &str, timestamp: Timestamp) -> Result<Decimal> {
    if unit.eq_ignore_ascii_case("epoch") {
        let epoch = Decimal::from_i128_with_scale(timestamp.0.timestamp_nanos() as i128, 9);
        return Ok(epoch);
    } else if unit.eq_ignore_ascii_case("julian") {
        let epoch = Decimal::from_i128_with_scale(timestamp.0.timestamp_nanos() as i128, 9);
        return Ok(epoch / (24 * 60 * 60).into() + 2_440_588.into());
    };
    extract_date(timestamp.0, unit)
        .or_else(|| extract_time(timestamp.0, unit))
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

#[function("date_part(varchar, date) -> float64")]
pub fn date_part_from_date(unit: &str, date: Date) -> Result<F64> {
    // date_part of date manually cast to timestamp
    // https://github.com/postgres/postgres/blob/REL_15_2/src/backend/catalog/system_functions.sql#L123
    extract_from_timestamp(unit, date.into()).map(|d| d.into())
}

#[function("date_part(varchar, time) -> float64")]
pub fn date_part_from_time(unit: &str, time: Time) -> Result<F64> {
    extract_from_time(unit, time).map(|d| d.into())
}

#[function("date_part(varchar, timestamp) -> float64")]
pub fn date_part_from_timestamp(unit: &str, timestamp: Timestamp) -> Result<F64> {
    extract_from_timestamp(unit, timestamp).map(|d| d.into())
}

fn invalid_unit(name: &'static str, unit: &str) -> ExprError {
    ExprError::InvalidParam {
        name,
        reason: format!("\"{unit}\" not recognized or supported"),
    }
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
        assert_eq!(extract_from_date("JULIAN", date).unwrap(), 2_459_541.into());
    }

    #[test]
    fn test_timestamp() {
        let ts = Timestamp::new(
            NaiveDateTime::parse_from_str("2021-11-22 12:4:2.575401000", "%Y-%m-%d %H:%M:%S%.f")
                .unwrap(),
        );
        let extract = extract_from_timestamp;
        assert_eq!(extract("MILLENNIUM", ts).unwrap(), 3.into());
        assert_eq!(extract("CENTURY", ts).unwrap(), 21.into());
        assert_eq!(extract("DECADE", ts).unwrap(), 202.into());
        assert_eq!(extract("ISOYEAR", ts).unwrap(), 2021.into());
        assert_eq!(extract("YEAR", ts).unwrap(), 2021.into());
        assert_eq!(extract("QUARTER", ts).unwrap(), 4.into());
        assert_eq!(extract("MONTH", ts).unwrap(), 11.into());
        assert_eq!(extract("WEEK", ts).unwrap(), 47.into());
        assert_eq!(extract("DAY", ts).unwrap(), 22.into());
        assert_eq!(extract("DOW", ts).unwrap(), 1.into());
        assert_eq!(extract("ISODOW", ts).unwrap(), 1.into());
        assert_eq!(extract("DOY", ts).unwrap(), 326.into());
        assert_eq!(extract("HOUR", ts).unwrap(), 12.into());
        assert_eq!(extract("MINUTE", ts).unwrap(), 4.into());
        assert_eq!(extract("SECOND", ts).unwrap(), 2.575401.into());
        assert_eq!(extract("MILLISECOND", ts).unwrap(), 2575.401.into());
        assert_eq!(extract("MICROSECOND", ts).unwrap(), 2575401.into());
        assert_eq!(extract("EPOCH", ts).unwrap(), 1637582642.575401.into());
        assert_eq!(
            extract("JULIAN", ts).unwrap(),
            "2459541.5028075856597222222222".parse().unwrap()
        );
    }
}
