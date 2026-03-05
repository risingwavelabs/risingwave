// Copyright 2024 RisingWave Labs
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

use risingwave_common::types::{F64, Interval};
use risingwave_expr::{Result, function};

/// Creates an interval from integer and float values.
/// PostgreSQL compatible function with named parameters support.
///
/// Signature: make_interval(years => int, months => int, weeks => int,
///                          days => int, hours => int, mins => int, secs => float8)
/// All parameters default to 0.
#[function("make_interval(int4, int4, int4, int4, int4, int4, float8) -> interval")]
pub fn make_interval(
    years: i32,
    months: i32,
    weeks: i32,
    days: i32,
    hours: i32,
    mins: i32,
    secs: F64,
) -> Result<Interval> {
    // Convert years to months (1 year = 12 months)
    let total_months = months
        .checked_add(
            years
                .checked_mul(12)
                .ok_or_else(|| risingwave_expr::ExprError::NumericOutOfRange)?,
        )
        .ok_or_else(|| risingwave_expr::ExprError::NumericOutOfRange)?;

    // Convert weeks to days (1 week = 7 days)
    let total_days = days
        .checked_add(
            weeks
                .checked_mul(7)
                .ok_or_else(|| risingwave_expr::ExprError::NumericOutOfRange)?,
        )
        .ok_or_else(|| risingwave_expr::ExprError::NumericOutOfRange)?;

    // Convert hours, mins, secs to microseconds
    let hours_us = (hours as i64)
        .checked_mul(3_600_000_000)
        .ok_or_else(|| risingwave_expr::ExprError::NumericOutOfRange)?;

    let mins_us = (mins as i64)
        .checked_mul(60_000_000)
        .ok_or_else(|| risingwave_expr::ExprError::NumericOutOfRange)?;

    let secs_us = (secs.0 * 1_000_000.0).round() as i64;

    let total_us = hours_us
        .checked_add(mins_us)
        .ok_or_else(|| risingwave_expr::ExprError::NumericOutOfRange)?
        .checked_add(secs_us)
        .ok_or_else(|| risingwave_expr::ExprError::NumericOutOfRange)?;

    Ok(Interval::from_month_day_usec(
        total_months,
        total_days,
        total_us,
    ))
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{F64, Interval};

    use super::*;

    #[test]
    fn test_make_interval_all_params() {
        // make_interval(1, 2, 3, 4, 5, 6, 7.5)
        // 1 year 2 months 25 days (3 weeks + 4 days) 05:06:07.5
        let result = make_interval(1, 2, 3, 4, 5, 6, F64::from(7.5)).unwrap();
        assert_eq!(result.months(), 14); // 1 year * 12 + 2 months
        assert_eq!(result.days(), 25); // 3 weeks * 7 + 4 days
        assert_eq!(result.usecs(), 18_367_500); // 5*3600 + 6*60 + 7.5 seconds in microseconds
    }

    #[test]
    fn test_make_interval_days_only() {
        // make_interval(days => 12)
        let result = make_interval(0, 0, 0, 12, 0, 0, F64::from(0.0)).unwrap();
        assert_eq!(result.months(), 0);
        assert_eq!(result.days(), 12);
        assert_eq!(result.usecs(), 0);
    }

    #[test]
    fn test_make_interval_mixed() {
        // make_interval(months => 10, days => 5, mins => 47)
        let result = make_interval(0, 10, 0, 5, 0, 47, F64::from(0.0)).unwrap();
        assert_eq!(result.months(), 10);
        assert_eq!(result.days(), 5);
        assert_eq!(result.usecs(), 2_820_000_000); // 47 * 60 * 1_000_000
    }

    #[test]
    fn test_make_interval_weeks_conversion() {
        // make_interval(weeks => 2)
        let result = make_interval(0, 0, 2, 0, 0, 0, F64::from(0.0)).unwrap();
        assert_eq!(result.months(), 0);
        assert_eq!(result.days(), 14); // 2 weeks = 14 days
        assert_eq!(result.usecs(), 0);
    }

    #[test]
    fn test_make_interval_fractional_seconds() {
        // make_interval(secs => 1.5)
        let result = make_interval(0, 0, 0, 0, 0, 0, F64::from(1.5)).unwrap();
        assert_eq!(result.months(), 0);
        assert_eq!(result.days(), 0);
        assert_eq!(result.usecs(), 1_500_000); // 1.5 seconds in microseconds
    }
}
