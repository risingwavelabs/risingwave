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
use risingwave_common::types::{Decimal, Timestamp, F64};
use risingwave_expr_macro::function;

use crate::{ExprError, Result};

#[function("extract(varchar, timestamp) -> decimal")]
pub fn extract_from_timestamp(unit: &str, timestamp: Timestamp) -> Result<Decimal> {
    let ts = timestamp.0;
    let nanoseconds = || ts.second() as u64 * 1_000_000_000 + ts.nanosecond() as u64;
    Ok(if unit.eq_ignore_ascii_case("MILLENNIUM") {
        ((ts.year() - 1) / 1000 + 1).into()
    } else if unit.eq_ignore_ascii_case("CENTURY") {
        ((ts.year() - 1) / 100 + 1).into()
    } else if unit.eq_ignore_ascii_case("DECADE") {
        (ts.year() / 10).into()
    } else if unit.eq_ignore_ascii_case("YEAR") {
        ts.year().into()
    } else if unit.eq_ignore_ascii_case("ISOYEAR") {
        ts.iso_week().year().into()
    } else if unit.eq_ignore_ascii_case("QUARTER") {
        ((ts.month() - 1) / 3 + 1).into()
    } else if unit.eq_ignore_ascii_case("MONTH") {
        ts.month().into()
    } else if unit.eq_ignore_ascii_case("WEEK") {
        ts.iso_week().week().into()
    } else if unit.eq_ignore_ascii_case("DAY") {
        ts.day().into()
    } else if unit.eq_ignore_ascii_case("DOY") {
        ts.ordinal().into()
    } else if unit.eq_ignore_ascii_case("DOW") {
        ts.weekday().num_days_from_sunday().into()
    } else if unit.eq_ignore_ascii_case("ISODOW") {
        ts.weekday().number_from_monday().into()
    } else if unit.eq_ignore_ascii_case("hour") {
        ts.hour().into()
    } else if unit.eq_ignore_ascii_case("minute") {
        ts.minute().into()
    } else if unit.eq_ignore_ascii_case("second") {
        Decimal::from_i128_with_scale(nanoseconds() as i128, 9)
    } else if unit.eq_ignore_ascii_case("millisecond") {
        Decimal::from_i128_with_scale(nanoseconds() as i128, 6)
    } else if unit.eq_ignore_ascii_case("microsecond") {
        Decimal::from_i128_with_scale(nanoseconds() as i128, 3)
    } else if unit.eq_ignore_ascii_case("epoch") {
        Decimal::from_i128_with_scale(ts.timestamp_nanos() as i128, 9)
    } else {
        return Err(invalid_unit("timestamp unit", unit));
    })
}

#[function("extract(varchar, timestamptz) -> decimal")]
pub fn extract_from_timestamptz(unit: &str, usecs: i64) -> Result<Decimal> {
    match unit {
        "EPOCH" => Ok(Decimal::from(usecs) / 1_000_000.into()),
        // TODO(#5826): all other units depend on implicit session TimeZone
        _ => Err(invalid_unit("timestamp with time zone units", unit)),
    }
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
    use chrono::NaiveDateTime;

    use super::*;

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
    }
}
