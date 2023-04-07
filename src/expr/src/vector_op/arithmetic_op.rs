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

use std::convert::TryInto;
use std::fmt::Debug;

use chrono::{Duration, NaiveDateTime};
use num_traits::{CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, Float, Signed, Zero};
use risingwave_common::types::{CheckedAdd, Date, Decimal, Interval, Time, Timestamp, F64};
use risingwave_expr_macro::function;
use rust_decimal::MathematicalOps;

use crate::{ExprError, Result};

#[function("add(*number, *number) -> auto")]
#[function("add(interval, interval) -> interval")]
#[function("add(int256, int256) -> int256")]
pub fn general_add<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: CheckedAdd<Output = T3>,
{
    general_atm(l, r, |a, b| {
        a.checked_add(b).ok_or(ExprError::NumericOutOfRange)
    })
}

#[function("subtract(*number, *number) -> auto")]
#[function("subtract(interval, interval) -> interval")]
#[function("subtract(int256, int256) -> int256")]
pub fn general_sub<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: CheckedSub,
{
    general_atm(l, r, |a, b| {
        a.checked_sub(&b).ok_or(ExprError::NumericOutOfRange)
    })
}

#[function("multiply(*number, *number) -> auto")]
#[function("multiply(int256, int256) -> int256")]
pub fn general_mul<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: CheckedMul,
{
    general_atm(l, r, |a, b| {
        a.checked_mul(&b).ok_or(ExprError::NumericOutOfRange)
    })
}

#[function("divide(*number, *number) -> auto")]
#[function("divide(int256, int256) -> int256")]
pub fn general_div<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: CheckedDiv + Zero,
{
    general_atm(l, r, |a, b| {
        a.checked_div(&b).ok_or_else(|| {
            if b.is_zero() {
                ExprError::DivisionByZero
            } else {
                ExprError::NumericOutOfRange
            }
        })
    })
}

#[function("modulus(*number, *number) -> auto")]
#[function("modulus(int256, int256) -> int256")]
pub fn general_mod<T1, T2, T3>(l: T1, r: T2) -> Result<T3>
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    T3: CheckedRem,
{
    general_atm(l, r, |a, b| {
        a.checked_rem(&b).ok_or(ExprError::NumericOutOfRange)
    })
}

#[function("neg(int16) -> int16")]
#[function("neg(int32) -> int32")]
#[function("neg(int64) -> int64")]
#[function("neg(float32) -> float32")]
#[function("neg(float64) -> float64")]
#[function("neg(decimal) -> decimal")]
pub fn general_neg<T1: CheckedNeg>(expr: T1) -> Result<T1> {
    expr.checked_neg().ok_or(ExprError::NumericOutOfRange)
}

#[function("neg(int256) -> int256")]
pub fn int256_neg<TRef, T>(expr: TRef) -> Result<T>
where
    TRef: Into<T> + Debug,
    T: CheckedNeg,
{
    expr.into()
        .checked_neg()
        .ok_or(ExprError::NumericOutOfRange)
}

#[function("abs(int16) -> int16")]
#[function("abs(int32) -> int32")]
#[function("abs(int64) -> int64")]
#[function("abs(float32) -> float32")]
#[function("abs(float64) -> float64")]
pub fn general_abs<T1: Signed + CheckedNeg>(expr: T1) -> Result<T1> {
    if expr.is_negative() {
        general_neg(expr)
    } else {
        Ok(expr)
    }
}

#[function("abs(decimal) -> decimal")]
pub fn decimal_abs(decimal: Decimal) -> Result<Decimal> {
    Ok(Decimal::abs(&decimal))
}

#[function("pow(float64, float64) -> float64")]
pub fn pow_f64(l: F64, r: F64) -> Result<F64> {
    let res = l.powf(r);
    if res.is_infinite() {
        Err(ExprError::NumericOutOfRange)
    } else {
        Ok(res)
    }
}

#[inline(always)]
fn general_atm<T1, T2, T3, F>(l: T1, r: T2, atm: F) -> Result<T3>
where
    T1: Into<T3> + Debug,
    T2: Into<T3> + Debug,
    F: FnOnce(T3, T3) -> Result<T3>,
{
    atm(l.into(), r.into())
}

#[function("subtract(timestamp, timestamp) -> interval")]
pub fn timestamp_timestamp_sub(l: Timestamp, r: Timestamp) -> Result<Interval> {
    let tmp = l.0 - r.0; // this does not overflow or underflow
    let days = tmp.num_days();
    let usecs = (tmp - Duration::days(tmp.num_days()))
        .num_microseconds()
        .ok_or_else(|| ExprError::NumericOutOfRange)?;
    Ok(Interval::from_month_day_usec(0, days as i32, usecs))
}

#[function("subtract(date, date) -> int32")]
pub fn date_date_sub(l: Date, r: Date) -> Result<i32> {
    Ok((l.0 - r.0).num_days() as i32) // this does not overflow or underflow
}

#[function("add(interval, timestamp) -> timestamp")]
pub fn interval_timestamp_add(l: Interval, r: Timestamp) -> Result<Timestamp> {
    r.checked_add(l).ok_or(ExprError::NumericOutOfRange)
}

#[function("add(interval, date) -> timestamp")]
pub fn interval_date_add(l: Interval, r: Date) -> Result<Timestamp> {
    interval_timestamp_add(l, r.into())
}

#[function("add(interval, time) -> time")]
pub fn interval_time_add(l: Interval, r: Time) -> Result<Time> {
    time_interval_add(r, l)
}

#[function("add(date, interval) -> timestamp")]
pub fn date_interval_add(l: Date, r: Interval) -> Result<Timestamp> {
    interval_date_add(r, l)
}

#[function("subtract(date, interval) -> timestamp")]
pub fn date_interval_sub(l: Date, r: Interval) -> Result<Timestamp> {
    // TODO: implement `checked_sub` for `Timestamp` to handle the edge case of negation
    // overflowing.
    interval_date_add(r.checked_neg().ok_or(ExprError::NumericOutOfRange)?, l)
}

#[function("add(date, int32) -> date")]
pub fn date_int_add(l: Date, r: i32) -> Result<Date> {
    let date = l.0;
    let date_wrapper = date
        .checked_add_signed(chrono::Duration::days(r as i64))
        .map(Date::new);

    date_wrapper.ok_or(ExprError::NumericOutOfRange)
}

#[function("add(int32, date) -> date")]
pub fn int_date_add(l: i32, r: Date) -> Result<Date> {
    date_int_add(r, l)
}

#[function("subtract(date, int32) -> date")]
pub fn date_int_sub(l: Date, r: i32) -> Result<Date> {
    let date = l.0;
    let date_wrapper = date
        .checked_sub_signed(chrono::Duration::days(r as i64))
        .map(Date::new);

    date_wrapper.ok_or(ExprError::NumericOutOfRange)
}

#[function("add(timestamp, interval) -> timestamp")]
pub fn timestamp_interval_add(l: Timestamp, r: Interval) -> Result<Timestamp> {
    interval_timestamp_add(r, l)
}

#[function("subtract(timestamp, interval) -> timestamp")]
pub fn timestamp_interval_sub(l: Timestamp, r: Interval) -> Result<Timestamp> {
    interval_timestamp_add(r.checked_neg().ok_or(ExprError::NumericOutOfRange)?, l)
}

#[function("add(timestamptz, interval) -> timestamptz")]
pub fn timestamptz_interval_add(l: i64, r: Interval) -> Result<i64> {
    timestamptz_interval_inner(l, r, i64::checked_add)
}

#[function("subtract(timestamptz, interval) -> timestamptz")]
pub fn timestamptz_interval_sub(l: i64, r: Interval) -> Result<i64> {
    timestamptz_interval_inner(l, r, i64::checked_sub)
}

#[function("add(interval, timestamptz) -> timestamptz")]
pub fn interval_timestamptz_add(l: Interval, r: i64) -> Result<i64> {
    timestamptz_interval_add(r, l)
}

#[inline(always)]
fn timestamptz_interval_inner(l: i64, r: Interval, f: fn(i64, i64) -> Option<i64>) -> Result<i64> {
    // Without session TimeZone, we cannot add month/day in local time. See #5826.
    if r.months() != 0 || r.days() != 0 {
        return Err(ExprError::UnsupportedFunction(
            "timestamp with time zone +/- interval of days".into(),
        ));
    }

    let result: Option<i64> = try {
        let delta_usecs = r.usecs();
        f(l, delta_usecs)?
    };

    result.ok_or(ExprError::NumericOutOfRange)
}

#[function("multiply(interval, *int) -> interval")]
pub fn interval_int_mul(l: Interval, r: impl TryInto<i32> + Debug) -> Result<Interval> {
    l.checked_mul_int(r).ok_or(ExprError::NumericOutOfRange)
}

#[function("multiply(*int, interval) -> interval")]
pub fn int_interval_mul(l: impl TryInto<i32> + Debug, r: Interval) -> Result<Interval> {
    interval_int_mul(r, l)
}

#[function("add(date, time) -> timestamp")]
pub fn date_time_add(l: Date, r: Time) -> Result<Timestamp> {
    Ok(Timestamp::new(NaiveDateTime::new(l.0, r.0)))
}

#[function("add(time, date) -> timestamp")]
pub fn time_date_add(l: Time, r: Date) -> Result<Timestamp> {
    date_time_add(r, l)
}

#[function("subtract(time, time) -> interval")]
pub fn time_time_sub(l: Time, r: Time) -> Result<Interval> {
    let tmp = l.0 - r.0; // this does not overflow or underflow
    let usecs = tmp
        .num_microseconds()
        .ok_or_else(|| ExprError::NumericOutOfRange)?;
    Ok(Interval::from_month_day_usec(0, 0, usecs))
}

#[function("subtract(time, interval) -> time")]
pub fn time_interval_sub(l: Time, r: Interval) -> Result<Time> {
    let time = l.0;
    let (new_time, ignored) = time.overflowing_sub_signed(Duration::microseconds(r.usecs()));
    if ignored == 0 {
        Ok(Time::new(new_time))
    } else {
        Err(ExprError::NumericOutOfRange)
    }
}

#[function("add(time, interval) -> time")]
pub fn time_interval_add(l: Time, r: Interval) -> Result<Time> {
    let time = l.0;
    let (new_time, ignored) = time.overflowing_add_signed(Duration::microseconds(r.usecs()));
    if ignored == 0 {
        Ok(Time::new(new_time))
    } else {
        Err(ExprError::NumericOutOfRange)
    }
}

#[function("divide(interval, *number) -> interval")]
pub fn interval_float_div<T2>(l: Interval, r: T2) -> Result<Interval>
where
    T2: TryInto<F64> + Debug,
{
    l.div_float(r).ok_or(ExprError::NumericOutOfRange)
}

#[function("multiply(interval, float32) -> interval")]
#[function("multiply(interval, float64) -> interval")]
#[function("multiply(interval, decimal) -> interval")]
pub fn interval_float_mul<T2>(l: Interval, r: T2) -> Result<Interval>
where
    T2: TryInto<F64> + Debug,
{
    l.mul_float(r).ok_or(ExprError::NumericOutOfRange)
}

#[function("multiply(float32, interval) -> interval")]
#[function("multiply(float64, interval) -> interval")]
#[function("multiply(decimal, interval) -> interval")]
pub fn float_interval_mul<T1>(l: T1, r: Interval) -> Result<Interval>
where
    T1: TryInto<F64> + Debug,
{
    r.mul_float(l).ok_or(ExprError::NumericOutOfRange)
}

#[function("sqrt(float64) -> float64")]
pub fn sqrt_f64(expr: F64) -> Result<F64> {
    if expr < F64::from(0.0) {
        return Err(ExprError::InvalidParam {
            name: "sqrt input",
            reason: "input cannot be negative value".to_string(),
        });
    }
    // Edge cases: nan, inf, negative zero should return itself.
    match expr.is_nan() || expr == f64::INFINITY || expr.is_negative() {
        true => Ok(expr),
        false => Ok(expr.sqrt()),
    }
}

#[function("sqrt(decimal) -> decimal")]
pub fn sqrt_decimal(expr: Decimal) -> Result<Decimal> {
    match expr {
        Decimal::NaN | Decimal::PositiveInf => Ok(expr),
        Decimal::Normalized(value) => match value.sqrt() {
            Some(res) => Ok(Decimal::from(res)),
            None => Err(ExprError::InvalidParam {
                name: "sqrt input",
                reason: "input cannot be negative value".to_string(),
            }),
        },
        Decimal::NegativeInf => Err(ExprError::InvalidParam {
            name: "sqrt input",
            reason: "input cannot be negative value".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::types::{Date, Decimal, Interval, Timestamp, F32, F64};

    use super::*;

    #[test]
    fn test() {
        assert_eq!(
            general_add::<_, _, Decimal>(Decimal::from_str("1").unwrap(), 1i32).unwrap(),
            Decimal::from_str("2").unwrap()
        );
    }

    #[test]
    fn test_arithmetic() {
        assert_eq!(
            general_add::<Decimal, i32, Decimal>(dec("1.0"), 1).unwrap(),
            dec("2.0")
        );
        assert_eq!(
            general_sub::<Decimal, i32, Decimal>(dec("1.0"), 2).unwrap(),
            dec("-1.0")
        );
        assert_eq!(
            general_mul::<Decimal, i32, Decimal>(dec("1.0"), 2).unwrap(),
            dec("2.0")
        );
        assert_eq!(
            general_div::<Decimal, i32, Decimal>(dec("2.0"), 2).unwrap(),
            dec("1.0")
        );
        assert_eq!(
            general_mod::<Decimal, i32, Decimal>(dec("2.0"), 2).unwrap(),
            dec("0")
        );
        assert_eq!(general_neg::<Decimal>(dec("1.0")).unwrap(), dec("-1.0"));
        assert_eq!(general_add::<i16, i32, i32>(1i16, 1i32).unwrap(), 2i32);
        assert_eq!(general_sub::<i16, i32, i32>(1i16, 1i32).unwrap(), 0i32);
        assert_eq!(general_mul::<i16, i32, i32>(1i16, 1i32).unwrap(), 1i32);
        assert_eq!(general_div::<i16, i32, i32>(1i16, 1i32).unwrap(), 1i32);
        assert_eq!(general_mod::<i16, i32, i32>(1i16, 1i32).unwrap(), 0i32);
        assert_eq!(general_neg::<i16>(1i16).unwrap(), -1i16);

        assert_eq!(
            general_add::<Decimal, f32, Decimal>(dec("1.0"), -1f32).unwrap(),
            dec("0.0")
        );
        assert_eq!(
            general_sub::<Decimal, f32, Decimal>(dec("1.0"), 1f32).unwrap(),
            dec("0.0")
        );
        assert_eq!(
            general_div::<Decimal, f32, Decimal>(dec("0.0"), 1f32).unwrap(),
            dec("0.0")
        );
        assert_eq!(
            general_mul::<Decimal, f32, Decimal>(dec("0.0"), 1f32).unwrap(),
            dec("0.0")
        );
        assert_eq!(
            general_mod::<Decimal, f32, Decimal>(dec("0.0"), 1f32).unwrap(),
            dec("0.0")
        );
        assert!(general_add::<i32, F32, F64>(-1i32, 1f32.into())
            .unwrap()
            .is_zero());
        assert!(general_sub::<i32, F32, F64>(1i32, 1f32.into())
            .unwrap()
            .is_zero());
        assert!(general_mul::<i32, F32, F64>(0i32, 1f32.into())
            .unwrap()
            .is_zero());
        assert!(general_div::<i32, F32, F64>(0i32, 1f32.into())
            .unwrap()
            .is_zero());
        assert_eq!(general_neg::<F32>(1f32.into()).unwrap(), F32::from(-1f32));
        assert_eq!(
            date_interval_add(Date::from_ymd_uncheck(1994, 1, 1), Interval::from_month(12))
                .unwrap(),
            Timestamp::new(
                NaiveDateTime::parse_from_str("1995-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        );
        assert_eq!(
            interval_date_add(Interval::from_month(12), Date::from_ymd_uncheck(1994, 1, 1))
                .unwrap(),
            Timestamp::new(
                NaiveDateTime::parse_from_str("1995-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        );
        assert_eq!(
            date_interval_sub(Date::from_ymd_uncheck(1994, 1, 1), Interval::from_month(12))
                .unwrap(),
            Timestamp::new(
                NaiveDateTime::parse_from_str("1993-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        );
        assert_eq!(sqrt_f64(F64::from(25.00)).unwrap(), F64::from(5.0));
        assert_eq!(
            sqrt_f64(F64::from(107)).unwrap(),
            F64::from(10.344080432788601)
        );
        assert_eq!(
            sqrt_f64(F64::from(12.234567)).unwrap(),
            F64::from(3.4977945908815173)
        );
        assert!(sqrt_f64(F64::from(-25.00)).is_err());
        // sqrt edge cases.
        assert_eq!(sqrt_f64(F64::from(f64::NAN)).unwrap(), F64::from(f64::NAN));
        assert_eq!(
            sqrt_f64(F64::from(f64::neg_zero())).unwrap(),
            F64::from(f64::neg_zero())
        );
        assert_eq!(
            sqrt_f64(F64::from(f64::INFINITY)).unwrap(),
            F64::from(f64::INFINITY)
        );
        assert!(sqrt_f64(F64::from(f64::NEG_INFINITY)).is_err());
        assert_eq!(sqrt_decimal(dec("25.0")).unwrap(), dec("5.0"));
        assert_eq!(
            sqrt_decimal(dec("107")).unwrap(),
            dec("10.344080432788600469738599442")
        );
        assert_eq!(
            sqrt_decimal(dec("12.234567")).unwrap(),
            dec("3.4977945908815171589625746860")
        );
        assert!(sqrt_decimal(dec("-25.0")).is_err());
        assert_eq!(sqrt_decimal(dec("nan")).unwrap(), dec("nan"));
        assert_eq!(sqrt_decimal(dec("inf")).unwrap(), dec("inf"));
        assert_eq!(sqrt_decimal(dec("-0")).unwrap(), dec("-0"));
        assert!(sqrt_decimal(dec("-inf")).is_err());
    }

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }
}
