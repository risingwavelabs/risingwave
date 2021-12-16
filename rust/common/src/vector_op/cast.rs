use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use rust_decimal::Decimal;

use crate::error::ErrorCode::{InvalidInputSyntax, ParseError};
use crate::error::{Result, RwError};
use crate::types::{OrderedF32, OrderedF64};

// The same as NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce().
// Minus this magic number to store the number of days since 1970-01-01.
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

#[inline(always)]
pub fn str_to_date(elem: &str) -> Result<i32> {
    NaiveDate::parse_from_str(elem, "%Y-%m-%d")
        .map(|ret| ret.num_days_from_ce() - UNIX_EPOCH_DAYS)
        .map_err(|e| RwError::from(ParseError(e)))
}

#[inline(always)]
pub fn str_to_time(elem: &str) -> Result<i64> {
    NaiveTime::parse_from_str(elem, "%H:%M:%S")
        // FIXME: add support for precision in microseconds.
        .map(|ret| ret.num_seconds_from_midnight() as i64 * 1000 * 1000)
        .map_err(|e| RwError::from(ParseError(e)))
}

#[inline(always)]
pub fn str_to_timestamp(elem: &str) -> Result<i64> {
    NaiveDateTime::parse_from_str(elem, "%Y-%m-%d %H:%M:%S")
        .map(|ret| ret.timestamp_nanos() / 1000)
        .map_err(|e| RwError::from(ParseError(e)))
}

#[inline(always)]
pub fn str_to_timestampz(elem: &str) -> Result<i64> {
    DateTime::parse_from_str(elem, "%Y-%m-%d %H:%M:%S %:z")
        .map(|ret| ret.timestamp_nanos() / 1000)
        .map_err(|e| RwError::from(ParseError(e)))
}

#[inline(always)]
pub fn date_to_timestamp(elem: i32) -> Result<i64> {
    Ok((elem as i64) * 24 * 60 * 60 * 1000 * 1000)
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
