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

use chrono::{Datelike, Timelike};
use risingwave_common::types::{Decimal, NaiveDateTimeWrapper, NaiveDateWrapper};

use crate::{bail, Result};

fn extract_time<T>(time: T, time_unit: &str) -> Result<Decimal>
where
    T: Timelike,
{
    match time_unit {
        "HOUR" => Ok(time.hour().into()),
        "MINUTE" => Ok(time.minute().into()),
        "SECOND" => Ok(time.second().into()),
        _ => bail!("Unsupported time unit {} in extract function", time_unit),
    }
}

fn extract_date<T>(date: T, time_unit: &str) -> Result<Decimal>
where
    T: Datelike,
{
    match time_unit {
        "DAY" => Ok(date.day().into()),
        "MONTH" => Ok(date.month().into()),
        "YEAR" => Ok(date.year().into()),
        // Sun = 0 and Sat = 6
        "DOW" => Ok(date.weekday().num_days_from_sunday().into()),
        "DOY" => Ok(date.ordinal().into()),
        _ => bail!("Unsupported time unit {} in extract function", time_unit),
    }
}

pub fn extract_from_date(time_unit: &str, date: NaiveDateWrapper) -> Result<Decimal> {
    extract_date(date.0, time_unit)
}

pub fn extract_from_timestamp(time_unit: &str, timestamp: NaiveDateTimeWrapper) -> Result<Decimal> {
    let time = timestamp.0;
    let mut res = extract_date(time, time_unit);
    if res.is_err() {
        res = extract_time(time, time_unit);
    }
    res
}

pub fn extract_from_timestampz(time_unit: &str, usecs: i64) -> Result<Decimal> {
    match time_unit {
        "EPOCH" => Ok(Decimal::from(usecs) / 1_000_000.into()),
        // TODO(#5826): all other units depend on implicit session TimeZone
        _ => bail!(
            "Unsupported timestamp with time zone unit {} in extract function",
            time_unit
        ),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveDateTime};

    use super::*;

    #[test]
    fn test_date() {
        let date =
            NaiveDateWrapper::new(NaiveDate::parse_from_str("2021-11-22", "%Y-%m-%d").unwrap());
        assert_eq!(extract_from_date("DAY", date).unwrap(), 22.into());
        assert_eq!(extract_from_date("MONTH", date).unwrap(), 11.into());
        assert_eq!(extract_from_date("YEAR", date).unwrap(), 2021.into());
        assert_eq!(extract_from_date("DOW", date).unwrap(), 1.into());
        assert_eq!(extract_from_date("DOY", date).unwrap(), 326.into());
    }

    #[test]
    fn test_time() {
        let time = NaiveDateTimeWrapper::new(
            NaiveDateTime::parse_from_str("2021-11-22 12:4:2", "%Y-%m-%d %H:%M:%S").unwrap(),
        );
        assert_eq!(extract_from_timestamp("HOUR", time).unwrap(), 12.into());
        assert_eq!(extract_from_timestamp("MINUTE", time).unwrap(), 4.into());
        assert_eq!(extract_from_timestamp("SECOND", time).unwrap(), 2.into());
    }
}
