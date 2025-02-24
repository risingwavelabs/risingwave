// Copyright 2025 RisingWave Labs
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

use chrono::format::Parsed;
use risingwave_common::types::{Date, Timestamp, Timestamptz};
use risingwave_expr::{ExprError, Result, function};

use super::timestamptz::{timestamp_at_time_zone, timestamptz_at_time_zone};
use super::to_char::ChronoPattern;

/// Parse the input string with the given chrono pattern.
#[inline(always)]
fn parse(s: &str, tmpl: &ChronoPattern) -> Result<Parsed> {
    let mut parsed = Parsed::new();
    chrono::format::parse(&mut parsed, s, tmpl.borrow_dependent().iter())?;

    // chrono will only assign the default value for seconds/nanoseconds fields, and raise an error
    // for other ones. We should specify the default value manually.

    // If year is omitted, the default value should be 0001 BC.
    if parsed.year.is_none()
        && parsed.year_div_100.is_none()
        && parsed.year_mod_100.is_none()
        && parsed.isoyear.is_none()
        && parsed.isoyear_div_100.is_none()
        && parsed.isoyear_mod_100.is_none()
    {
        parsed.set_year(-1).unwrap();
    }

    // If the month is omitted, the default value should be 1 (January).
    if parsed.month.is_none()
        && parsed.week_from_mon.is_none()
        && parsed.week_from_sun.is_none()
        && parsed.isoweek.is_none()
    {
        parsed.set_month(1).unwrap();
    }

    // If the day is omitted, the default value should be 1.
    if parsed.day.is_none() && parsed.ordinal.is_none() {
        parsed.set_day(1).unwrap();
    }

    // The default value should be AM.
    parsed.hour_div_12.get_or_insert(0);

    // The default time should be 00:00.
    parsed.hour_mod_12.get_or_insert(0);
    parsed.minute.get_or_insert(0);

    // Seconds and nanoseconds can be omitted, so we don't need to assign default value for them.

    Ok(parsed)
}

#[function(
    "char_to_timestamptz(varchar, varchar) -> timestamp",
    prebuild = "ChronoPattern::compile($1)",
    deprecated
)]
pub fn to_timestamp_legacy(s: &str, tmpl: &ChronoPattern) -> Result<Timestamp> {
    let parsed = parse(s, tmpl)?;
    match parsed.offset {
        None => Ok(parsed.to_naive_datetime_with_offset(0)?.into()),
        // If the parsed result is a physical instant, return its reading in UTC.
        // This decision was arbitrary and we are just being backward compatible here.
        Some(_) => timestamptz_at_time_zone(parsed.to_datetime()?.into(), "UTC"),
    }
}

#[function(
    "char_to_timestamptz(varchar, varchar, varchar) -> timestamptz",
    prebuild = "ChronoPattern::compile($1)"
)]
pub fn to_timestamp(s: &str, timezone: &str, tmpl: &ChronoPattern) -> Result<Timestamptz> {
    let parsed = parse(s, tmpl)?;
    Ok(match parsed.offset {
        Some(_) => parsed.to_datetime()?.into(),
        // If the parsed result lacks offset info, interpret it in the implicit session time zone.
        None => timestamp_at_time_zone(parsed.to_naive_datetime_with_offset(0)?.into(), timezone)?,
    })
}

#[function("char_to_timestamptz(varchar, varchar) -> timestamptz", rewritten)]
fn _to_timestamp1() {}

#[function(
    "char_to_date(varchar, varchar) -> date",
    prebuild = "ChronoPattern::compile($1)"
)]
pub fn to_date(s: &str, tmpl: &ChronoPattern) -> Result<Date> {
    let mut parsed = parse(s, tmpl)?;
    if let Some(year) = &mut parsed.year
        && *year < 0
    {
        *year += 1;
    }
    Ok(parsed.to_naive_date()?.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_timestamp_legacy() {
        // This legacy expr can no longer be build by frontend, so we test its backward compatible
        // behavior in unit tests rather than e2e slt.
        for (input, format, expected) in [
            (
                "2020-02-03 12:34:56",
                "yyyy-mm-dd hh24:mi:ss",
                "2020-02-03 12:34:56",
            ),
            (
                "2020-02-03 12:34:56+03:00",
                "yyyy-mm-dd hh24:mi:ss tzh:tzm",
                "2020-02-03 09:34:56",
            ),
        ] {
            let actual = to_timestamp_legacy(input, &ChronoPattern::compile(format)).unwrap();
            assert_eq!(actual.to_string(), expected);
        }
    }
}
