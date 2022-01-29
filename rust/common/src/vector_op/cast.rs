use core::convert::From;
use std::any::type_name;
use std::str::FromStr;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use num_traits::ToPrimitive;

use crate::error::ErrorCode::{InternalError, InvalidInputSyntax, ParseError};
use crate::error::{Result, RwError};
use crate::types::{
    Decimal, NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper, OrderedF32, OrderedF64,
};

/// The same as `NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce()`.
/// Minus this magic number to store the number of days since 1970-01-01.
pub const UNIX_EPOCH_DAYS: i32 = 719_163;

/// String literals for bool type.
///
/// See [`https://www.postgresql.org/docs/9.5/datatype-boolean.html`]
const TRUE_BOOL_LITERALS: [&str; 9] = ["true", "tru", "tr", "t", "on", "1", "yes", "ye", "y"];
const FALSE_BOOL_LITERALS: [&str; 10] = [
    "false", "fals", "fal", "fa", "f", "off", "of", "0", "no", "n",
];

#[inline(always)]
pub fn num_up<T, R>(n: T) -> Result<R>
where
    T: Into<R>,
{
    Ok(n.into())
}

#[inline(always)]
pub fn float_up(n: OrderedF32) -> Result<OrderedF64> {
    Ok((n.0 as f64).into())
}

/// Cast between different precision and scale.
/// Eg. Decimal(10,5) -> Decimal(20,10)
/// Currently no-op.
#[inline(always)]
pub fn dec_to_dec(n: Decimal) -> Result<Decimal> {
    Ok(n)
}

/// Cast between different precision/length.
/// Eg. Char(5) -> Char(10)
/// Currently no-op. TODO: implement padding and overflow check (#2137)
#[inline(always)]
pub fn str_to_str(n: &str) -> Result<String> {
    Ok(n.into())
}

#[inline(always)]
pub fn str_to_date(elem: &str) -> Result<NaiveDateWrapper> {
    Ok(NaiveDateWrapper::new(
        NaiveDate::parse_from_str(elem, "%Y-%m-%d")
            .map_err(|e| RwError::from(ParseError(Box::new(e))))?,
    ))
}

#[inline(always)]
pub fn str_to_decimal(elem: &str) -> Result<Decimal> {
    Decimal::from_str(elem).map_err(|e| RwError::from(ParseError(Box::new(e))))
}

#[inline(always)]
pub fn str_to_time(elem: &str) -> Result<NaiveTimeWrapper> {
    Ok(NaiveTimeWrapper::new(
        NaiveTime::parse_from_str(elem, "%H:%M:%S")
            .map_err(|e| RwError::from(ParseError(Box::new(e))))?,
    ))
}

#[inline(always)]
pub fn str_to_timestamp(elem: &str) -> Result<NaiveDateTimeWrapper> {
    Ok(NaiveDateTimeWrapper::new(
        NaiveDateTime::parse_from_str(elem, "%Y-%m-%d %H:%M:%S")
            .map_err(|e| RwError::from(ParseError(Box::new(e))))?,
    ))
}

#[inline(always)]
pub fn str_to_timestampz(elem: &str) -> Result<i64> {
    DateTime::parse_from_str(elem, "%Y-%m-%d %H:%M:%S %:z")
        .map(|ret| ret.timestamp_nanos() / 1000)
        .map_err(|e| RwError::from(ParseError(Box::new(e))))
}

#[inline(always)]
pub fn str_to_real(elem: &str) -> Result<OrderedF32> {
    elem.parse()
        .map_err(|e| RwError::from(ParseError(Box::new(e))))
}

#[inline(always)]
pub fn str_to_double(elem: &str) -> Result<OrderedF64> {
    elem.parse()
        .map_err(|e| RwError::from(ParseError(Box::new(e))))
}
#[inline(always)]
pub fn str_to_i16(elem: &str) -> Result<i16> {
    elem.parse()
        .map_err(|e| RwError::from(ParseError(Box::new(e))))
}
#[inline(always)]
pub fn str_to_i32(elem: &str) -> Result<i32> {
    elem.parse()
        .map_err(|e| RwError::from(ParseError(Box::new(e))))
}
#[inline(always)]
pub fn str_to_i64(elem: &str) -> Result<i64> {
    elem.parse()
        .map_err(|e| RwError::from(ParseError(Box::new(e))))
}

#[inline(always)]
pub fn date_to_timestamp(elem: NaiveDateWrapper) -> Result<NaiveDateTimeWrapper> {
    Ok(NaiveDateTimeWrapper::new(elem.0.and_hms(0, 0, 0)))
}

// Due to the orphan rule, some data can't implement TryFrom trait for basic type.
// We can only use ToPrimitive Trait
#[inline(always)]
pub fn to_i16<T1>(elem: T1) -> Result<i16>
where
    T1: ToPrimitive + std::fmt::Debug,
{
    elem.to_i16()
        .ok_or_else(|| RwError::from(InternalError(format!("Can't cast {:?} to i16", elem))))
}

#[inline(always)]
pub fn to_i32<T1>(elem: T1) -> Result<i32>
where
    T1: ToPrimitive + std::fmt::Debug,
{
    elem.to_i32()
        .ok_or_else(|| RwError::from(InternalError(format!("Can't cast {:?} to i32", elem))))
}

#[inline(always)]
pub fn to_i64<T1>(elem: T1) -> Result<i64>
where
    T1: ToPrimitive + std::fmt::Debug,
{
    elem.to_i64()
        .ok_or_else(|| RwError::from(InternalError(format!("Can't cast {:?} to i64", elem))))
}

// In postgresSql, the behavior of casting decimal to integer is rounding.
// We should write them separately
#[inline(always)]
pub fn deci_to_i16(elem: Decimal) -> Result<i16> {
    to_i16(elem.round_dp(0))
}

#[inline(always)]
pub fn deci_to_i32(elem: Decimal) -> Result<i32> {
    to_i32(elem.round_dp(0))
}

#[inline(always)]
pub fn deci_to_i64(elem: Decimal) -> Result<i64> {
    to_i64(elem.round_dp(0))
}

#[inline(always)]
pub fn general_cast<T1, T2>(elem: T1) -> Result<T2>
where
    T1: TryInto<T2> + std::fmt::Debug + Copy,
{
    elem.try_into().map_err(|_| {
        RwError::from(InternalError(format!(
            "Can't cast {:?} to {:?}",
            &elem,
            type_name::<T2>()
        )))
    })
}

#[inline(always)]
pub fn str_to_bool(input: &str) -> Result<bool> {
    let trimmed_input = input.trim();
    if TRUE_BOOL_LITERALS
        .iter()
        .any(|s| s.eq_ignore_ascii_case(trimmed_input))
    {
        Ok(true)
    } else if FALSE_BOOL_LITERALS
        .iter()
        .any(|s| trimmed_input.eq_ignore_ascii_case(*s))
    {
        Ok(false)
    } else {
        Err(InvalidInputSyntax("boolean".to_string(), input.to_string()).into())
    }
}

#[inline(always)]
pub fn bool_to_str(input: bool) -> Result<String> {
    match input {
        true => Ok("true".into()),
        false => Ok("false".into()),
    }
}
