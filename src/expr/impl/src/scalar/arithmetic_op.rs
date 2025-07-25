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

use std::fmt::Debug;

use chrono::{Duration, NaiveDateTime};
use num_traits::{CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, Zero};
use risingwave_common::types::{
    CheckedAdd, Date, Decimal, F64, FloatExt, Interval, IsNegative, Time, Timestamp,
};
use risingwave_expr::{ExprError, Result, function};
use rust_decimal::MathematicalOps;

#[function("add(*int, *int) -> auto")]
#[function("add(decimal, decimal) -> auto")]
#[function("add(*float, *float) -> auto")]
#[function("add(interval, interval) -> interval")]
#[function("add(int256, int256) -> int256")]
#[function("add(uint256, uint256) -> uint256")]
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

#[function("subtract(*int, *int) -> auto")]
#[function("subtract(decimal, decimal) -> auto")]
#[function("subtract(*float, *float) -> auto")]
#[function("subtract(interval, interval) -> interval")]
#[function("subtract(int256, int256) -> int256")]
#[function("subtract(uint256, uint256) -> uint256")]
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

#[function("multiply(*int, *int) -> auto")]
#[function("multiply(decimal, decimal) -> auto")]
#[function("multiply(*float, *float) -> auto")]
#[function("multiply(int256, int256) -> int256")]
#[function("multiply(uint256, uint256) -> uint256")]
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

#[function("divide(*int, *int) -> auto")]
#[function("divide(decimal, decimal) -> auto")]
#[function("divide(*float, *float) -> auto")]
#[function("divide(int256, int256) -> int256")]
#[function("divide(int256, float8) -> float8")]
#[function("divide(int256, *int) -> int256")]
#[function("divide(uint256, uint256) -> uint256")]
#[function("divide(uint256, float8) -> float8")]
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

use risingwave_common::types::UInt256;

// Addition operators for uint256 with signed integers
#[function("add(uint256, int2) -> uint256")]
#[function("add(uint256, int4) -> uint256")]
#[function("add(uint256, int8) -> uint256")]
pub fn uint256_add_int<L, T>(l: L, r: T) -> Result<UInt256>
where
    L: Into<UInt256>,
    T: TryInto<UInt256> + Debug,
    T::Error: Debug,
{
    let l_uint256 = l.into();
    let r_uint256 = r.try_into()
        .map_err(|_| ExprError::CastOutOfRange("uint256"))?;
    l_uint256.checked_add(r_uint256)
        .ok_or(ExprError::NumericOutOfRange)
}

#[function("add(int2, uint256) -> uint256")]
#[function("add(int4, uint256) -> uint256")]
#[function("add(int8, uint256) -> uint256")]
pub fn int_add_uint256<T, R>(l: T, r: R) -> Result<UInt256>
where
    T: TryInto<UInt256> + Debug,
    T::Error: Debug,
    R: Into<UInt256>,
{
    let l_uint256 = l.try_into()
        .map_err(|_| ExprError::CastOutOfRange("uint256"))?;
    let r_uint256 = r.into();
    l_uint256.checked_add(r_uint256)
        .ok_or(ExprError::NumericOutOfRange)
}

// Subtraction operators for uint256 with signed integers
#[function("subtract(uint256, int2) -> uint256")]
#[function("subtract(uint256, int4) -> uint256")]
#[function("subtract(uint256, int8) -> uint256")]
pub fn uint256_sub_int<L, T>(l: L, r: T) -> Result<UInt256>
where
    L: Into<UInt256>,
    T: TryInto<UInt256> + Debug,
    T::Error: Debug,
{
    let l_uint256 = l.into();
    let r_uint256 = r.try_into()
        .map_err(|_| ExprError::CastOutOfRange("uint256"))?;
    l_uint256.checked_sub(&r_uint256)
        .ok_or(ExprError::NumericOutOfRange)
}

#[function("subtract(int2, uint256) -> uint256")]
#[function("subtract(int4, uint256) -> uint256")]
#[function("subtract(int8, uint256) -> uint256")]
pub fn int_sub_uint256<T, R>(l: T, r: R) -> Result<UInt256>
where
    T: TryInto<UInt256> + Debug,
    T::Error: Debug,
    R: Into<UInt256>,
{
    let l_uint256 = l.try_into()
        .map_err(|_| ExprError::CastOutOfRange("uint256"))?;
    let r_uint256 = r.into();
    l_uint256.checked_sub(&r_uint256)
        .ok_or(ExprError::NumericOutOfRange)
}

// Multiplication operators for uint256 with signed integers
#[function("multiply(uint256, int2) -> uint256")]
#[function("multiply(uint256, int4) -> uint256")]
#[function("multiply(uint256, int8) -> uint256")]
pub fn uint256_mul_int<L, T>(l: L, r: T) -> Result<UInt256>
where
    L: Into<UInt256>,
    T: TryInto<UInt256> + Debug,
    T::Error: Debug,
{
    let l_uint256 = l.into();
    let r_uint256 = r.try_into()
        .map_err(|_| ExprError::CastOutOfRange("uint256"))?;
    l_uint256.checked_mul(&r_uint256)
        .ok_or(ExprError::NumericOutOfRange)
}

#[function("multiply(int2, uint256) -> uint256")]
#[function("multiply(int4, uint256) -> uint256")]
#[function("multiply(int8, uint256) -> uint256")]
pub fn int_mul_uint256<T, R>(l: T, r: R) -> Result<UInt256>
where
    T: TryInto<UInt256> + Debug,
    T::Error: Debug,
    R: Into<UInt256>,
{
    let l_uint256 = l.try_into()
        .map_err(|_| ExprError::CastOutOfRange("uint256"))?;
    let r_uint256 = r.into();
    l_uint256.checked_mul(&r_uint256)
        .ok_or(ExprError::NumericOutOfRange)
}

#[function("divide(uint256, int2) -> uint256")]
#[function("divide(uint256, int4) -> uint256")]  
#[function("divide(uint256, int8) -> uint256")]
pub fn uint256_div_int<L, T>(l: L, r: T) -> Result<UInt256>
where
    L: Into<UInt256>,
    T: TryInto<UInt256> + Zero + Debug,
    T::Error: Debug,
{
    if r.is_zero() {
        return Err(ExprError::DivisionByZero);
    }
    let l_uint256 = l.into();
    let r_uint256 = r.try_into()
        .map_err(|_| ExprError::CastOutOfRange("uint256"))?;
    l_uint256.checked_div(&r_uint256)
        .ok_or(ExprError::NumericOutOfRange)
}

#[function("divide(int2, uint256) -> uint256")]
#[function("divide(int4, uint256) -> uint256")]
#[function("divide(int8, uint256) -> uint256")]
pub fn int_div_uint256<T, R>(l: T, r: R) -> Result<UInt256>
where
    T: TryInto<UInt256> + Debug,
    T::Error: Debug,
    R: Into<UInt256>,
{
    let r_uint256 = r.into();
    if r_uint256.is_zero() {
        return Err(ExprError::DivisionByZero);
    }
    let l_uint256 = l.try_into()
        .map_err(|_| ExprError::CastOutOfRange("uint256"))?;
    l_uint256.checked_div(&r_uint256)
        .ok_or(ExprError::NumericOutOfRange)
}

// Modulus operators for uint256 with signed integers
#[function("modulus(uint256, int2) -> uint256")]
#[function("modulus(uint256, int4) -> uint256")]
#[function("modulus(uint256, int8) -> uint256")]
pub fn uint256_mod_int<L, T>(l: L, r: T) -> Result<UInt256>
where
    L: Into<UInt256>,
    T: TryInto<UInt256> + Zero + Debug,
    T::Error: Debug,
{
    if r.is_zero() {
        return Err(ExprError::DivisionByZero);
    }
    let l_uint256 = l.into();
    let r_uint256 = r.try_into()
        .map_err(|_| ExprError::CastOutOfRange("uint256"))?;
    l_uint256.checked_rem(&r_uint256)
        .ok_or(ExprError::NumericOutOfRange)
}

#[function("modulus(int2, uint256) -> uint256")]
#[function("modulus(int4, uint256) -> uint256")]
#[function("modulus(int8, uint256) -> uint256")]
pub fn int_mod_uint256<T, R>(l: T, r: R) -> Result<UInt256>
where
    T: TryInto<UInt256> + Debug,
    T::Error: Debug,
    R: Into<UInt256>,
{
    let r_uint256 = r.into();
    if r_uint256.is_zero() {
        return Err(ExprError::DivisionByZero);
    }
    let l_uint256 = l.try_into()
        .map_err(|_| ExprError::CastOutOfRange("uint256"))?;
    l_uint256.checked_rem(&r_uint256)
        .ok_or(ExprError::NumericOutOfRange)
}

#[function("modulus(*int, *int) -> auto")]
#[function("modulus(decimal, decimal) -> auto")]
#[function("modulus(int256, int256) -> int256")]
#[function("modulus(uint256, uint256) -> uint256")]
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

#[function("neg(*int) -> auto")]
#[function("neg(*float) -> auto")]
#[function("neg(decimal) -> decimal")]
pub fn general_neg<T1: CheckedNeg>(expr: T1) -> Result<T1> {
    expr.checked_neg().ok_or(ExprError::NumericOutOfRange)
}

#[function("neg(int256) -> int256")]
pub fn int256_neg<TRef, T>(expr: TRef) -> Result<T>
where
    TRef: Into<T> + Debug,
    T: CheckedNeg + Debug,
{
    expr.into()
        .checked_neg()
        .ok_or(ExprError::NumericOutOfRange)
}

#[function("neg(uint256) -> uint256")]
pub fn uint256_neg<TRef, T>(expr: TRef) -> Result<T>
where
    TRef: Into<T> + Debug,
    T: Zero + Debug,
{
    // For UInt256, negation is only valid for zero (result is zero)
    // Any other value would result in a negative number, which is invalid for unsigned
    let value = expr.into();
    if value.is_zero() {
        Ok(value)
    } else {
        Err(ExprError::NumericOutOfRange)
    }
}

#[function("abs(*int) -> auto")]
pub fn general_abs<T1: IsNegative + CheckedNeg>(expr: T1) -> Result<T1> {
    if expr.is_negative() {
        general_neg(expr)
    } else {
        Ok(expr)
    }
}

#[function("abs(*float) -> auto")]
pub fn float_abs<F: num_traits::Float, T1: FloatExt<F>>(expr: T1) -> T1 {
    expr.abs()
}

#[function("abs(int256) -> int256")]
pub fn int256_abs<TRef, T>(expr: TRef) -> Result<T>
where
    TRef: Into<T> + Debug,
    T: IsNegative + CheckedNeg + Debug,
{
    let expr = expr.into();
    if expr.is_negative() {
        int256_neg(expr)
    } else {
        Ok(expr)
    }
}

#[function("abs(uint256) -> uint256")]
pub fn uint256_abs<TRef, T>(expr: TRef) -> Result<T>
where
    TRef: Into<T> + Debug,
{
    // UInt256 is always non-negative, so abs is identity
    Ok(expr.into())
}

#[function("abs(decimal) -> decimal")]
pub fn decimal_abs(decimal: Decimal) -> Decimal {
    Decimal::abs(&decimal)
}

fn err_pow_zero_negative() -> ExprError {
    ExprError::InvalidParam {
        name: "rhs",
        reason: "zero raised to a negative power is undefined".into(),
    }
}
fn err_pow_negative_fract() -> ExprError {
    ExprError::InvalidParam {
        name: "rhs",
        reason: "a negative number raised to a non-integer power yields a complex result".into(),
    }
}

#[function("pow(float8, float8) -> float8")]
pub fn pow_f64(l: F64, r: F64) -> Result<F64> {
    if l.is_zero() && r.0 < 0.0 {
        return Err(err_pow_zero_negative());
    }
    if l.0 < 0.0 && (r.is_finite() && !r.fract().is_zero()) {
        return Err(err_pow_negative_fract());
    }
    let res = l.powf(r);
    if res.is_infinite() && l.is_finite() && r.is_finite() {
        return Err(ExprError::NumericOverflow);
    }
    if res.is_zero() && l.is_finite() && r.is_finite() && !l.is_zero() {
        return Err(ExprError::NumericUnderflow);
    }

    Ok(res)
}

#[function("pow(decimal, decimal) -> decimal")]
pub fn pow_decimal(l: Decimal, r: Decimal) -> Result<Decimal> {
    use risingwave_common::types::DecimalPowError as PowError;

    l.checked_powd(&r).map_err(|e| match e {
        PowError::ZeroNegative => err_pow_zero_negative(),
        PowError::NegativeFract => err_pow_negative_fract(),
        PowError::Overflow => ExprError::NumericOverflow,
    })
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

#[function("subtract(date, date) -> int4")]
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

#[function("add(date, int4) -> date")]
pub fn date_int_add(l: Date, r: i32) -> Result<Date> {
    let date = l.0;
    let date_wrapper = date
        .checked_add_signed(chrono::Duration::days(r as i64))
        .map(Date::new);

    date_wrapper.ok_or(ExprError::NumericOutOfRange)
}

#[function("add(int4, date) -> date")]
pub fn int_date_add(l: i32, r: Date) -> Result<Date> {
    date_int_add(r, l)
}

#[function("subtract(date, int4) -> date")]
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

#[function("divide(interval, *int) -> interval")]
#[function("divide(interval, decimal) -> interval")]
#[function("divide(interval, *float) -> interval")]
pub fn interval_float_div<T2>(l: Interval, r: T2) -> Result<Interval>
where
    T2: TryInto<F64> + Debug,
{
    l.div_float(r).ok_or(ExprError::NumericOutOfRange)
}

#[function("multiply(interval, float4) -> interval")]
#[function("multiply(interval, float8) -> interval")]
#[function("multiply(interval, decimal) -> interval")]
pub fn interval_float_mul<T2>(l: Interval, r: T2) -> Result<Interval>
where
    T2: TryInto<F64> + Debug,
{
    l.mul_float(r).ok_or(ExprError::NumericOutOfRange)
}

#[function("multiply(float4, interval) -> interval")]
#[function("multiply(float8, interval) -> interval")]
#[function("multiply(decimal, interval) -> interval")]
pub fn float_interval_mul<T1>(l: T1, r: Interval) -> Result<Interval>
where
    T1: TryInto<F64> + Debug,
{
    r.mul_float(l).ok_or(ExprError::NumericOutOfRange)
}

#[function("sqrt(float8) -> float8")]
pub fn sqrt_f64(expr: F64) -> Result<F64> {
    if expr < F64::from(0.0) {
        return Err(ExprError::InvalidParam {
            name: "sqrt input",
            reason: "input cannot be negative value".into(),
        });
    }
    // Edge cases: nan, inf, negative zero should return itself.
    match expr.is_nan() || expr == f64::INFINITY || expr == -0.0 {
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
                reason: "input cannot be negative value".into(),
            }),
        },
        Decimal::NegativeInf => Err(ExprError::InvalidParam {
            name: "sqrt input",
            reason: "input cannot be negative value".into(),
        }),
    }
}

#[function("cbrt(float8) -> float8")]
pub fn cbrt_f64(expr: F64) -> F64 {
    expr.cbrt()
}

#[function("sign(float8) -> float8")]
pub fn sign_f64(input: F64) -> F64 {
    match input.0.partial_cmp(&0.) {
        Some(std::cmp::Ordering::Less) => (-1).into(),
        Some(std::cmp::Ordering::Equal) => 0.into(),
        Some(std::cmp::Ordering::Greater) => 1.into(),
        None => 0.into(),
    }
}

#[function("sign(decimal) -> decimal")]
pub fn sign_dec(input: Decimal) -> Decimal {
    input.sign()
}

#[function("scale(decimal) -> int4")]
pub fn decimal_scale(d: Decimal) -> Option<i32> {
    d.scale()
}

#[function("min_scale(decimal) -> int4")]
pub fn decimal_min_scale(d: Decimal) -> Option<i32> {
    d.normalize().scale()
}

#[function("trim_scale(decimal) -> decimal")]
pub fn decimal_trim_scale(d: Decimal) -> Decimal {
    d.normalize()
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use num_traits::Float;
    use risingwave_common::types::test_utils::IntervalTestExt;
    use risingwave_common::types::{F32, Int256, Int256Ref, Scalar};

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

        assert!(
            general_add::<i32, F32, F64>(-1i32, 1f32.into())
                .unwrap()
                .is_zero()
        );
        assert!(
            general_sub::<i32, F32, F64>(1i32, 1f32.into())
                .unwrap()
                .is_zero()
        );
        assert!(
            general_mul::<i32, F32, F64>(0i32, 1f32.into())
                .unwrap()
                .is_zero()
        );
        assert!(
            general_div::<i32, F32, F64>(0i32, 1f32.into())
                .unwrap()
                .is_zero()
        );
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

    #[test]
    fn test_arithmetic_int256() {
        let tuples = vec![
            (0, 1, "0"),
            (0, -1, "0"),
            (1, 1, "1"),
            (1, -1, "-1"),
            (1, 2, "0.5"),
            (1, -2, "-0.5"),
            (9007199254740991i64, 2, "4503599627370495.5"),
        ];

        for (i, j, k) in tuples {
            let lhs = Int256::from(i);
            let rhs = F64::from(j);
            let res = F64::from_str(k).unwrap();
            assert_eq!(
                general_div::<Int256Ref<'_>, F64, F64>(lhs.as_scalar_ref(), rhs).unwrap(),
                res,
            );
        }
    }

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }
}
