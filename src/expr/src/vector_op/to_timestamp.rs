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

use chrono::format::Parsed;
use risingwave_common::types::Timestamp;

// use risingwave_expr_macro::function;
use super::to_char::{compile_pattern_to_chrono, ChronoPattern};
use crate::Result;

#[inline(always)]
pub fn to_timestamp_const_tmpl(s: &str, tmpl: &ChronoPattern) -> Result<Timestamp> {
    let mut parsed = Parsed::new();
    chrono::format::parse(&mut parsed, s, tmpl.borrow_items().iter())?;

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

    Ok(Timestamp(
        parsed.to_naive_datetime_with_offset(0)?,
    ))
}

// #[function("to_timestamp(varchar, varchar) -> timestamp")]
pub fn to_timestamp(s: &str, tmpl: &str) -> Result<Timestamp> {
    let pattern = compile_pattern_to_chrono(tmpl);
    to_timestamp_const_tmpl(s, &pattern)
}
