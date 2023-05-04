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

use std::any::type_name;
use std::fmt::{Debug, Write};
use std::str::FromStr;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures_util::FutureExt;
use itertools::Itertools;
use num_traits::ToPrimitive;
use risingwave_common::array::{
    JsonbRef, ListArray, ListRef, ListValue, StructArray, StructRef, StructValue, Utf8Array,
};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::num256::Int256;
use risingwave_common::types::struct_type::StructType;
use risingwave_common::types::to_text::ToText;
use risingwave_common::types::{
    DataType, Date, Decimal, Interval, ScalarImpl, Time, Timestamp, F32, F64,
};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr_macro::{build_function, function};
use risingwave_pb::expr::expr_node::PbType;
use speedate::{Date as SpeedDate, DateTime as SpeedDateTime, Time as SpeedTime};

use crate::expr::template::UnaryExpression;
use crate::expr::{build, BoxedExpression, Expression, InputRefExpression};
use crate::{ExprError, Result};

/// String literals for bool type.
///
/// See [`https://www.postgresql.org/docs/9.5/datatype-boolean.html`]
const TRUE_BOOL_LITERALS: [&str; 9] = ["true", "tru", "tr", "t", "on", "1", "yes", "ye", "y"];
const FALSE_BOOL_LITERALS: [&str; 10] = [
    "false", "fals", "fal", "fa", "f", "off", "of", "0", "no", "n",
];
const ERROR_INT_TO_TIMESTAMP: &str = "Can't cast negative integer to timestamp";
const PARSE_ERROR_STR_WITH_TIME_ZONE_TO_TIMESTAMPTZ: &str = concat!(
    "Can't cast string to timestamp with time zone (expected format is YYYY-MM-DD HH:MM:SS[.D+{up to 6 digits}] followed by +hh:mm or literal Z)"
    , "\nFor example: '2021-04-01 00:00:00+00:00'"
);
const PARSE_ERROR_STR_TO_TIMESTAMP: &str = "Can't cast string to timestamp (expected format is YYYY-MM-DD HH:MM:SS[.D+{up to 6 digits}] or YYYY-MM-DD HH:MM or YYYY-MM-DD or ISO 8601 format)";
const PARSE_ERROR_STR_TO_TIME: &str =
    "Can't cast string to time (expected format is HH:MM:SS[.D+{up to 6 digits}] or HH:MM)";
const PARSE_ERROR_STR_TO_DATE: &str = "Can't cast string to date (expected format is YYYY-MM-DD)";
const PARSE_ERROR_STR_TO_BYTEA: &str = "Invalid Bytea syntax";

#[function("cast(varchar) -> date")]
pub fn str_to_date(elem: &str) -> Result<Date> {
    Ok(Date::new(parse_naive_date(elem)?))
}

#[function("cast(varchar) -> time")]
pub fn str_to_time(elem: &str) -> Result<Time> {
    Ok(Time::new(parse_naive_time(elem)?))
}

#[function("cast(varchar) -> timestamp")]
pub fn str_to_timestamp(elem: &str) -> Result<Timestamp> {
    Ok(Timestamp::new(parse_naive_datetime(elem)?))
}

#[inline]
fn parse_naive_datetime(s: &str) -> Result<NaiveDateTime> {
    if let Ok(res) = SpeedDateTime::parse_str(s) {
        Ok(Date::from_ymd_uncheck(
            res.date.year as i32,
            res.date.month as u32,
            res.date.day as u32,
        )
        .and_hms_micro_uncheck(
            res.time.hour as u32,
            res.time.minute as u32,
            res.time.second as u32,
            res.time.microsecond,
        )
        .0)
    } else {
        let res = SpeedDate::parse_str(s)
            .map_err(|_| ExprError::Parse(PARSE_ERROR_STR_TO_TIMESTAMP.into()))?;
        Ok(
            Date::from_ymd_uncheck(res.year as i32, res.month as u32, res.day as u32)
                .and_hms_micro_uncheck(0, 0, 0, 0)
                .0,
        )
    }
}

/// Converts UNIX epoch time to timestamp.
///
/// The input UNIX epoch time is interpreted as follows:
///
/// - [0, 1e11) are assumed to be in seconds.
/// - [1e11, 1e14) are assumed to be in milliseconds.
/// - [1e14, 1e17) are assumed to be in microseconds.
/// - [1e17, upper) are assumed to be in nanoseconds.
///
/// This would cause no problem for timestamp in [1973-03-03 09:46:40, 5138-11-16 09:46:40).
///
/// # Example
/// ```
/// # use risingwave_expr::vector_op::cast::i64_to_timestamp;
/// assert_eq!(
///     i64_to_timestamp(1_666_666_666).unwrap().to_string(),
///     "2022-10-25 02:57:46"
/// );
/// assert_eq!(
///     i64_to_timestamp(1_666_666_666_666).unwrap().to_string(),
///     "2022-10-25 02:57:46.666"
/// );
/// assert_eq!(
///     i64_to_timestamp(1_666_666_666_666_666).unwrap().to_string(),
///     "2022-10-25 02:57:46.666666"
/// );
/// assert_eq!(
///     i64_to_timestamp(1_666_666_666_666_666_666)
///         .unwrap()
///         .to_string(),
///     // note that we only support microseconds precision
///     "2022-10-25 02:57:46.666666"
/// );
/// ```
#[inline]
pub fn i64_to_timestamp(t: i64) -> Result<Timestamp> {
    let us = i64_to_timestamptz(t)?;
    Ok(Timestamp::from_timestamp_uncheck(
        us / 1_000_000,
        (us % 1_000_000) as u32 * 1000,
    ))
}

#[inline]
fn parse_naive_date(s: &str) -> Result<NaiveDate> {
    let res =
        SpeedDate::parse_str(s).map_err(|_| ExprError::Parse(PARSE_ERROR_STR_TO_DATE.into()))?;
    Ok(Date::from_ymd_uncheck(res.year as i32, res.month as u32, res.day as u32).0)
}

#[inline]
fn parse_naive_time(s: &str) -> Result<NaiveTime> {
    let res =
        SpeedTime::parse_str(s).map_err(|_| ExprError::Parse(PARSE_ERROR_STR_TO_TIME.into()))?;
    Ok(Time::from_hms_micro_uncheck(
        res.hour as u32,
        res.minute as u32,
        res.second as u32,
        res.microsecond,
    )
    .0)
}

#[inline(always)]
pub fn str_with_time_zone_to_timestamptz(elem: &str) -> Result<i64> {
    elem.parse::<DateTime<Utc>>()
        .map(|ret| ret.timestamp_micros())
        .map_err(|_| ExprError::Parse(PARSE_ERROR_STR_WITH_TIME_ZONE_TO_TIMESTAMPTZ.into()))
}

/// Converts UNIX epoch time to timestamp in microseconds.
///
/// The input UNIX epoch time is interpreted as follows:
///
/// - [0, 1e11) are assumed to be in seconds.
/// - [1e11, 1e14) are assumed to be in milliseconds.
/// - [1e14, 1e17) are assumed to be in microseconds.
/// - [1e17, upper) are assumed to be in nanoseconds.
///
/// This would cause no problem for timestamp in [1973-03-03 09:46:40, 5138-11-16 09:46:40).
#[inline]
pub fn i64_to_timestamptz(t: i64) -> Result<i64> {
    const E11: i64 = 100_000_000_000;
    const E14: i64 = 100_000_000_000_000;
    const E17: i64 = 100_000_000_000_000_000;
    match t {
        0..E11 => Ok(t * 1_000_000), // s
        E11..E14 => Ok(t * 1_000),   // ms
        E14..E17 => Ok(t),           // us
        E17.. => Ok(t / 1_000),      // ns
        _ => Err(ExprError::Parse(ERROR_INT_TO_TIMESTAMP.into())),
    }
}

#[function("cast(varchar) -> bytea")]
pub fn str_to_bytea(elem: &str) -> Result<Box<[u8]>> {
    // Padded with whitespace str is not allowed.
    if elem.starts_with(' ') && elem.trim().starts_with("\\x") {
        Err(ExprError::Parse(PARSE_ERROR_STR_TO_BYTEA.into()))
    } else if let Some(remainder) = elem.strip_prefix(r"\x") {
        Ok(parse_bytes_hex(remainder)?.into())
    } else {
        Ok(parse_bytes_traditional(elem)?.into())
    }
}

// Refer to Materialize: https://github.com/MaterializeInc/materialize/blob/1766ab3978bc90abf75eb9b1fbadfcc95eca1993/src/repr/src/strconv.rs#L623
pub fn parse_bytes_hex(s: &str) -> Result<Vec<u8>> {
    // Can't use `hex::decode` here, as it doesn't tolerate whitespace
    // between encoded bytes.

    let decode_nibble = |b| match b {
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        b'0'..=b'9' => Ok(b - b'0'),
        _ => Err(ExprError::Parse(PARSE_ERROR_STR_TO_BYTEA.into())),
    };

    let mut buf = vec![];
    let mut nibbles = s.as_bytes().iter().copied();
    while let Some(n) = nibbles.next() {
        if let b' ' | b'\n' | b'\t' | b'\r' = n {
            continue;
        }
        let n = decode_nibble(n)?;
        let n2 = match nibbles.next() {
            None => return Err(ExprError::Parse(PARSE_ERROR_STR_TO_BYTEA.into())),
            Some(n2) => decode_nibble(n2)?,
        };
        buf.push((n << 4) | n2);
    }
    Ok(buf)
}

// Refer to https://github.com/MaterializeInc/materialize/blob/1766ab3978bc90abf75eb9b1fbadfcc95eca1993/src/repr/src/strconv.rs#L650
pub fn parse_bytes_traditional(s: &str) -> Result<Vec<u8>> {
    // Bytes are interpreted literally, save for the special escape sequences
    // "\\", which represents a single backslash, and "\NNN", where each N
    // is an octal digit, which represents the byte whose octal value is NNN.
    let mut out = Vec::new();
    let mut bytes = s.as_bytes().iter().fuse();
    while let Some(&b) = bytes.next() {
        if b != b'\\' {
            out.push(b);
            continue;
        }
        match bytes.next() {
            None => return Err(ExprError::Parse(PARSE_ERROR_STR_TO_BYTEA.into())),
            Some(b'\\') => out.push(b'\\'),
            b => match (b, bytes.next(), bytes.next()) {
                (Some(d2 @ b'0'..=b'3'), Some(d1 @ b'0'..=b'7'), Some(d0 @ b'0'..=b'7')) => {
                    out.push(((d2 - b'0') << 6) + ((d1 - b'0') << 3) + (d0 - b'0'));
                }
                _ => return Err(ExprError::Parse(PARSE_ERROR_STR_TO_BYTEA.into())),
            },
        }
    }
    Ok(out)
}

#[function("cast(varchar) -> *int")]
#[function("cast(varchar) -> *numeric")]
#[function("cast(varchar) -> *float")]
#[function("cast(varchar) -> int256")]
#[function("cast(varchar) -> interval")]
#[function("cast(varchar) -> jsonb")]
pub fn str_parse<T>(elem: &str) -> Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    elem.trim()
        .parse()
        .map_err(|_| ExprError::Parse(type_name::<T>().into()))
}

// Define the cast function to primitive types.
//
// Due to the orphan rule, some data can't implement `TryFrom` trait for basic type.
// We can only use [`ToPrimitive`] trait.
//
// Note: this might be lossy according to the docs from [`ToPrimitive`]:
// > On the other hand, conversions with possible precision loss or truncation
// are admitted, like an `f32` with a decimal part to an integer type, or
// even a large `f64` saturating to `f32` infinity.

#[function("cast(float32) -> int16")]
#[function("cast(float64) -> int16")]
pub fn to_i16<T: ToPrimitive + Debug>(elem: T) -> Result<i16> {
    elem.to_i16().ok_or(ExprError::CastOutOfRange("i16"))
}

#[function("cast(float32) -> int32")]
#[function("cast(float64) -> int32")]
pub fn to_i32<T: ToPrimitive + Debug>(elem: T) -> Result<i32> {
    elem.to_i32().ok_or(ExprError::CastOutOfRange("i32"))
}

#[function("cast(float32) -> int64")]
#[function("cast(float64) -> int64")]
pub fn to_i64<T: ToPrimitive + Debug>(elem: T) -> Result<i64> {
    elem.to_i64().ok_or(ExprError::CastOutOfRange("i64"))
}

#[function("cast(int32) -> float32")]
#[function("cast(int64) -> float32")]
#[function("cast(float64) -> float32")]
#[function("cast(decimal) -> float32")]
pub fn to_f32<T: ToPrimitive + Debug>(elem: T) -> Result<F32> {
    elem.to_f32()
        .map(Into::into)
        .ok_or(ExprError::CastOutOfRange("f32"))
}

#[function("cast(int16) -> int256")]
#[function("cast(int32) -> int256")]
#[function("cast(int64) -> int256")]
pub fn to_int256<T: TryInto<Int256>>(elem: T) -> Result<Int256> {
    elem.try_into()
        .map_err(|_| ExprError::CastOutOfRange("int256"))
}

#[function("cast(decimal) -> float64")]
pub fn to_f64<T: ToPrimitive + Debug>(elem: T) -> Result<F64> {
    elem.to_f64()
        .map(Into::into)
        .ok_or(ExprError::CastOutOfRange("f64"))
}

// In postgresSql, the behavior of casting decimal to integer is rounding.
// We should write them separately
#[function("cast(decimal) -> int16")]
pub fn dec_to_i16(elem: Decimal) -> Result<i16> {
    to_i16(elem.round_dp(0))
}

#[function("cast(decimal) -> int32")]
pub fn dec_to_i32(elem: Decimal) -> Result<i32> {
    to_i32(elem.round_dp(0))
}

#[function("cast(decimal) -> int64")]
pub fn dec_to_i64(elem: Decimal) -> Result<i64> {
    to_i64(elem.round_dp(0))
}

#[function("cast(jsonb) -> boolean")]
pub fn jsonb_to_bool(v: JsonbRef<'_>) -> Result<bool> {
    v.as_bool().map_err(|e| ExprError::Parse(e.into()))
}

#[function("cast(jsonb) -> decimal")]
pub fn jsonb_to_dec(v: JsonbRef<'_>) -> Result<Decimal> {
    v.as_number()
        .map_err(|e| ExprError::Parse(e.into()))?
        .try_into()
        .map_err(|_| ExprError::NumericOutOfRange)
}

/// Similar to and an result of [`define_cast_to_primitive`] macro above.
/// If that was implemented as a trait to cast from `f64`, this could also call them via trait
/// rather than macro.
///
/// Note that PostgreSQL casts JSON numbers from arbitrary precision `numeric` but we use `f64`.
/// This is less powerful but still meets RFC 8259 interoperability.
macro_rules! define_jsonb_to_number {
    ($ty:ty, $sig:literal) => {
        define_jsonb_to_number! { $ty, $ty, $sig }
    };
    ($ty:ty, $wrapper_ty:ty, $sig:literal) => {
        paste::paste! {
            #[function($sig)]
            pub fn [<jsonb_to_ $ty>](v: JsonbRef<'_>) -> Result<$wrapper_ty> {
                v.as_number().map_err(|e| ExprError::Parse(e.into())).and_then([<to_ $ty>])
            }
        }
    };
}
define_jsonb_to_number! { i16, "cast(jsonb) -> int16" }
define_jsonb_to_number! { i32, "cast(jsonb) -> int32" }
define_jsonb_to_number! { i64, "cast(jsonb) -> int64" }
define_jsonb_to_number! { f32, F32, "cast(jsonb) -> float32" }
define_jsonb_to_number! { f64, F64, "cast(jsonb) -> float64" }

/// In `PostgreSQL`, casting from timestamp to date discards the time part.
#[function("cast(timestamp) -> date")]
pub fn timestamp_to_date(elem: Timestamp) -> Date {
    Date(elem.0.date())
}

/// In `PostgreSQL`, casting from timestamp to time discards the date part.
#[function("cast(timestamp) -> time")]
pub fn timestamp_to_time(elem: Timestamp) -> Time {
    Time(elem.0.time())
}

/// In `PostgreSQL`, casting from interval to time discards the days part.
#[function("cast(interval) -> time")]
pub fn interval_to_time(elem: Interval) -> Time {
    let usecs = elem.usecs_of_day();
    let secs = (usecs / 1_000_000) as u32;
    let nano = (usecs % 1_000_000 * 1000) as u32;
    Time::from_num_seconds_from_midnight_uncheck(secs, nano)
}

#[function("cast(boolean) -> int32")]
#[function("cast(int32) -> int16")]
#[function("cast(int64) -> int16")]
#[function("cast(int64) -> int32")]
#[function("cast(int64) -> float64")]
#[function("cast(float32) -> decimal")]
#[function("cast(float64) -> decimal")]
pub fn try_cast<T1, T2>(elem: T1) -> Result<T2>
where
    T1: TryInto<T2> + std::fmt::Debug + Copy,
    <T1 as TryInto<T2>>::Error: std::fmt::Display,
{
    elem.try_into()
        .map_err(|_| ExprError::CastOutOfRange(std::any::type_name::<T2>()))
}

#[function("cast(int16) -> int32")]
#[function("cast(int16) -> int64")]
#[function("cast(int16) -> float32")]
#[function("cast(int16) -> float64")]
#[function("cast(int16) -> decimal")]
#[function("cast(int32) -> int64")]
#[function("cast(int32) -> float64")]
#[function("cast(int32) -> decimal")]
#[function("cast(int64) -> decimal")]
#[function("cast(float32) -> float64")]
#[function("cast(date) -> timestamp")]
#[function("cast(time) -> interval")]
#[function("cast(varchar) -> varchar")]
#[function("cast(int256) -> float64")]
pub fn cast<T1, T2>(elem: T1) -> T2
where
    T1: Into<T2>,
{
    elem.into()
}

#[function("cast(varchar) -> boolean")]
pub fn str_to_bool(input: &str) -> Result<bool> {
    let trimmed_input = input.trim();
    if TRUE_BOOL_LITERALS
        .iter()
        .any(|s| s.eq_ignore_ascii_case(trimmed_input))
    {
        Ok(true)
    } else if FALSE_BOOL_LITERALS
        .iter()
        .any(|s| trimmed_input.eq_ignore_ascii_case(s))
    {
        Ok(false)
    } else {
        Err(ExprError::Parse("Invalid bool".into()))
    }
}

#[function("cast(int32) -> boolean")]
pub fn int32_to_bool(input: i32) -> Result<bool> {
    Ok(input != 0)
}

// For most of the types, cast them to varchar is similar to return their text format.
// So we use this function to cast type to varchar.
#[function("cast(*int) -> varchar")]
#[function("cast(*numeric) -> varchar")]
#[function("cast(*float) -> varchar")]
#[function("cast(int256) -> varchar")]
#[function("cast(time) -> varchar")]
#[function("cast(date) -> varchar")]
#[function("cast(interval) -> varchar")]
#[function("cast(timestamp) -> varchar")]
#[function("cast(jsonb) -> varchar")]
#[function("cast(list) -> varchar")]
pub fn general_to_text(elem: impl ToText, mut writer: &mut dyn Write) -> Result<()> {
    elem.write(&mut writer).unwrap();
    Ok(())
}

#[function("cast(boolean) -> varchar")]
pub fn bool_to_varchar(input: bool, writer: &mut dyn Write) -> Result<()> {
    writer
        .write_str(if input { "true" } else { "false" })
        .unwrap();
    Ok(())
}

/// `bool_out` is different from `general_to_string<bool>` to produce a single char. `PostgreSQL`
/// uses different variants of bool-to-string in different situations.
#[function("bool_out(boolean) -> varchar")]
pub fn bool_out(input: bool, writer: &mut dyn Write) -> Result<()> {
    writer.write_str(if input { "t" } else { "f" }).unwrap();
    Ok(())
}

/// A lite version of casting from string to target type. Used by frontend to handle types that have
/// to be created by casting.
///
/// For example, the user can input `1` or `true` directly, but they have to use
/// `'2022-01-01'::date`.
pub fn literal_parsing(
    t: &DataType,
    s: &str,
) -> std::result::Result<ScalarImpl, Option<ExprError>> {
    let scalar = match t {
        DataType::Boolean => str_to_bool(s)?.into(),
        DataType::Int16 => str_parse::<i16>(s)?.into(),
        DataType::Int32 => str_parse::<i32>(s)?.into(),
        DataType::Int64 => str_parse::<i64>(s)?.into(),
        DataType::Int256 => str_parse::<Int256>(s)?.into(),
        DataType::Serial => return Err(None),
        DataType::Decimal => str_parse::<Decimal>(s)?.into(),
        DataType::Float32 => str_parse::<F32>(s)?.into(),
        DataType::Float64 => str_parse::<F64>(s)?.into(),
        DataType::Varchar => return Err(None),
        DataType::Date => str_to_date(s)?.into(),
        DataType::Timestamp => str_to_timestamp(s)?.into(),
        // We only handle the case with timezone here, and leave the implicit session timezone case
        // for later phase.
        DataType::Timestamptz => str_with_time_zone_to_timestamptz(s)?.into(),
        DataType::Time => str_to_time(s)?.into(),
        DataType::Interval => str_parse::<Interval>(s)?.into(),
        // Not processing list or struct literal right now. Leave it for later phase (normal backend
        // evaluation).
        DataType::List { .. } => return Err(None),
        DataType::Struct(_) => return Err(None),
        DataType::Jsonb => return Err(None),
        DataType::Bytea => str_to_bytea(s)?.into(),
    };
    Ok(scalar)
}

// TODO(nanderstabel): optimize for multidimensional List. Depth can be given as a parameter to this
// function.
/// Takes a string input in the form of a comma-separated list enclosed in braces, and returns a
/// vector of strings containing the list items.
///
/// # Examples
/// - "{1, 2, 3}" => ["1", "2", "3"]
/// - "{1, {2, 3}}" => ["1", "{2, 3}"]
fn unnest(input: &str) -> Result<Vec<&str>> {
    let trimmed = input.trim();
    if !trimmed.starts_with('{') || !trimmed.ends_with('}') {
        return Err(ExprError::Parse("Input must be braced".into()));
    }
    let trimmed = &trimmed[1..trimmed.len() - 1];

    let mut items = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in trimmed.chars().enumerate() {
        match c {
            '{' => depth += 1,
            '}' => depth -= 1,
            ',' if depth == 0 => {
                let item = trimmed[start..i].trim();
                items.push(item);
                start = i + 1;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(ExprError::Parse("Unbalanced braces".into()));
    }
    let last = trimmed[start..].trim();
    if !last.is_empty() {
        items.push(last);
    }
    Ok(items)
}

#[build_function("cast(varchar) -> list")]
fn build_cast_str_to_list(
    return_type: DataType,
    children: Vec<BoxedExpression>,
) -> Result<BoxedExpression> {
    let elem_type = match &return_type {
        DataType::List { datatype } => (**datatype).clone(),
        _ => panic!("expected list type"),
    };
    let child = children.into_iter().next().unwrap();
    Ok(Box::new(UnaryExpression::<Utf8Array, ListArray, _>::new(
        child,
        return_type,
        move |x| str_to_list(x, &elem_type),
    )))
}

fn str_to_list(input: &str, target_elem_type: &DataType) -> Result<ListValue> {
    let cast = build(
        PbType::Cast,
        target_elem_type.clone(),
        vec![InputRefExpression::new(DataType::Varchar, 0).boxed()],
    )
    .unwrap();
    let mut values = vec![];
    for item in unnest(input)? {
        let v = cast
            .eval_row(&OwnedRow::new(vec![Some(item.to_string().into())])) // TODO: optimize
            .now_or_never()
            .unwrap()?;
        values.push(v);
    }
    Ok(ListValue::new(values))
}

#[build_function("cast(list) -> list")]
fn build_cast_list_to_list(
    return_type: DataType,
    children: Vec<BoxedExpression>,
) -> Result<BoxedExpression> {
    let child = children.into_iter().next().unwrap();
    let source_elem_type = match child.return_type() {
        DataType::List { datatype } => (*datatype).clone(),
        _ => panic!("expected list type"),
    };
    let target_elem_type = match &return_type {
        DataType::List { datatype } => (**datatype).clone(),
        _ => panic!("expected list type"),
    };
    Ok(Box::new(UnaryExpression::<ListArray, ListArray, _>::new(
        child,
        return_type,
        move |x| list_cast(x, &source_elem_type, &target_elem_type),
    )))
}

/// Cast array with `source_elem_type` into array with `target_elem_type` by casting each element.
fn list_cast(
    input: ListRef<'_>,
    source_elem_type: &DataType,
    target_elem_type: &DataType,
) -> Result<ListValue> {
    let cast = build(
        PbType::Cast,
        target_elem_type.clone(),
        vec![InputRefExpression::new(source_elem_type.clone(), 0).boxed()],
    )
    .unwrap();
    let elements = input.iter_elems_ref();
    let mut values = Vec::with_capacity(elements.len());
    for item in elements {
        let v = cast
            .eval_row(&OwnedRow::new(vec![item.map(|s| s.into_scalar_impl())])) // TODO: optimize
            .now_or_never()
            .unwrap()?;
        values.push(v);
    }
    Ok(ListValue::new(values))
}

#[build_function("cast(struct) -> struct")]
fn build_cast_struct_to_struct(
    return_type: DataType,
    children: Vec<BoxedExpression>,
) -> Result<BoxedExpression> {
    let child = children.into_iter().next().unwrap();
    let source_elem_type = match child.return_type() {
        DataType::Struct(s) => (*s).clone(),
        _ => panic!("expected struct type"),
    };
    let target_elem_type = match &return_type {
        DataType::Struct(s) => (**s).clone(),
        _ => panic!("expected struct type"),
    };
    Ok(Box::new(
        UnaryExpression::<StructArray, StructArray, _>::new(child, return_type, move |x| {
            struct_cast(x, &source_elem_type, &target_elem_type)
        }),
    ))
}

/// Cast struct of `source_elem_type` to `target_elem_type` by casting each element.
fn struct_cast(
    input: StructRef<'_>,
    source_elem_type: &StructType,
    target_elem_type: &StructType,
) -> Result<StructValue> {
    let fields = (input.iter_fields_ref())
        .zip_eq_fast(source_elem_type.fields.iter())
        .zip_eq_fast(target_elem_type.fields.iter())
        .map(|((datum_ref, source_field_type), target_field_type)| {
            if source_field_type == target_field_type {
                return Ok(datum_ref.map(|scalar_ref| scalar_ref.into_scalar_impl()));
            }
            let cast = build(
                PbType::Cast,
                target_field_type.clone(),
                vec![InputRefExpression::new(source_field_type.clone(), 0).boxed()],
            )
            .unwrap();
            let value = match datum_ref {
                Some(scalar_ref) => cast
                    .eval_row(&OwnedRow::new(vec![Some(scalar_ref.into_scalar_impl())]))
                    .now_or_never()
                    .unwrap()?,
                None => None,
            };
            Ok(value) as Result<_>
        })
        .try_collect()?;
    Ok(StructValue::new(fields))
}

#[cfg(test)]
mod tests {

    use num_traits::FromPrimitive;
    use risingwave_common::types::Scalar;

    use super::*;

    #[test]
    fn parse_str() {
        assert_eq!(
            str_with_time_zone_to_timestamptz("2022-08-03 10:34:02Z").unwrap(),
            str_with_time_zone_to_timestamptz("2022-08-03 02:34:02-08:00").unwrap()
        );
        str_to_timestamp("1999-01-08 04:02").unwrap();
        str_to_timestamp("1999-01-08 04:05:06").unwrap();
        assert_eq!(
            str_to_timestamp("2022-08-03T10:34:02Z").unwrap(),
            str_to_timestamp("2022-08-03 10:34:02").unwrap()
        );
        str_to_date("1999-01-08").unwrap();
        str_to_time("04:05").unwrap();
        str_to_time("04:05:06").unwrap();

        assert_eq!(
            str_with_time_zone_to_timestamptz("1999-01-08 04:05:06")
                .unwrap_err()
                .to_string(),
            ExprError::Parse(PARSE_ERROR_STR_WITH_TIME_ZONE_TO_TIMESTAMPTZ.into()).to_string()
        );
        assert_eq!(
            str_to_timestamp("1999-01-08 04:05:06AA")
                .unwrap_err()
                .to_string(),
            ExprError::Parse(PARSE_ERROR_STR_TO_TIMESTAMP.into()).to_string()
        );
        assert_eq!(
            str_to_date("1999-01-08AA").unwrap_err().to_string(),
            "Parse error: Can't cast string to date (expected format is YYYY-MM-DD)".to_string()
        );
        assert_eq!(
            str_to_time("AA04:05:06").unwrap_err().to_string(),
            ExprError::Parse(PARSE_ERROR_STR_TO_TIME.into()).to_string()
        );
    }

    #[test]
    fn integer_cast_to_bool() {
        use super::*;
        assert!(int32_to_bool(32).unwrap());
        assert!(int32_to_bool(-32).unwrap());
        assert!(!int32_to_bool(0).unwrap());
    }

    #[test]
    fn number_to_string() {
        use super::*;

        macro_rules! test {
            ($fn:ident($value:expr), $right:literal) => {
                let mut writer = String::new();
                $fn($value, &mut writer).unwrap();
                assert_eq!(writer, $right);
            };
        }

        test!(bool_to_varchar(true), "true");
        test!(bool_to_varchar(true), "true");
        test!(bool_to_varchar(false), "false");

        test!(general_to_text(32), "32");
        test!(general_to_text(-32), "-32");
        test!(general_to_text(i32::MIN), "-2147483648");
        test!(general_to_text(i32::MAX), "2147483647");

        test!(general_to_text(i16::MIN), "-32768");
        test!(general_to_text(i16::MAX), "32767");

        test!(general_to_text(i64::MIN), "-9223372036854775808");
        test!(general_to_text(i64::MAX), "9223372036854775807");

        test!(general_to_text(F64::from(32.12)), "32.12");
        test!(general_to_text(F64::from(-32.14)), "-32.14");

        test!(general_to_text(F32::from(32.12_f32)), "32.12");
        test!(general_to_text(F32::from(-32.14_f32)), "-32.14");

        test!(general_to_text(Decimal::from_f64(1.222).unwrap()), "1.222");

        test!(general_to_text(Decimal::NaN), "NaN");
    }

    #[test]
    fn temporal_cast() {
        assert_eq!(
            timestamp_to_date(str_to_timestamp("1999-01-08 04:02").unwrap()),
            str_to_date("1999-01-08").unwrap(),
        );
        assert_eq!(
            timestamp_to_time(str_to_timestamp("1999-01-08 04:02").unwrap()),
            str_to_time("04:02").unwrap(),
        );
        assert_eq!(
            interval_to_time(Interval::from_month_day_usec(1, 2, 61000003)),
            str_to_time("00:01:01.000003").unwrap(),
        );
        assert_eq!(
            interval_to_time(Interval::from_month_day_usec(0, 0, -61000003)),
            str_to_time("23:58:58.999997").unwrap(),
        );
    }

    #[test]
    fn test_unnest() {
        assert_eq!(unnest("{ }").unwrap(), vec![] as Vec<String>);
        assert_eq!(
            unnest("{1, 2, 3}").unwrap(),
            vec!["1".to_string(), "2".to_string(), "3".to_string()]
        );
        assert_eq!(
            unnest("{{1, 2, 3}, {4, 5, 6}}").unwrap(),
            vec!["{1, 2, 3}".to_string(), "{4, 5, 6}".to_string()]
        );
        assert_eq!(
            unnest("{{{1, 2, 3}}, {{4, 5, 6}}}").unwrap(),
            vec!["{{1, 2, 3}}".to_string(), "{{4, 5, 6}}".to_string()]
        );
        assert_eq!(
            unnest("{{{1, 2, 3}, {4, 5, 6}}}").unwrap(),
            vec!["{{1, 2, 3}, {4, 5, 6}}".to_string()]
        );
        assert_eq!(
            unnest("{{{aa, bb, cc}, {dd, ee, ff}}}").unwrap(),
            vec!["{{aa, bb, cc}, {dd, ee, ff}}".to_string()]
        );
    }

    #[test]
    fn test_str_to_list() {
        // Empty List
        assert_eq!(
            str_to_list("{}", &DataType::Int32).unwrap(),
            ListValue::new(vec![])
        );

        let list123 = ListValue::new(vec![
            Some(1.to_scalar_value()),
            Some(2.to_scalar_value()),
            Some(3.to_scalar_value()),
        ]);

        // Single List
        assert_eq!(str_to_list("{1, 2, 3}", &DataType::Int32).unwrap(), list123);

        // Nested List
        let nested_list123 = ListValue::new(vec![Some(ScalarImpl::List(list123))]);
        assert_eq!(
            str_to_list(
                "{{1, 2, 3}}",
                &DataType::List {
                    datatype: Box::new(DataType::Int32)
                }
            )
            .unwrap(),
            nested_list123
        );

        let nested_list445566 = ListValue::new(vec![Some(ScalarImpl::List(ListValue::new(vec![
            Some(44.to_scalar_value()),
            Some(55.to_scalar_value()),
            Some(66.to_scalar_value()),
        ])))]);

        let double_nested_list123_445566 = ListValue::new(vec![
            Some(ScalarImpl::List(nested_list123.clone())),
            Some(ScalarImpl::List(nested_list445566.clone())),
        ]);

        // Double nested List
        assert_eq!(
            str_to_list(
                "{{{1, 2, 3}}, {{44, 55, 66}}}",
                &DataType::List {
                    datatype: Box::new(DataType::List {
                        datatype: Box::new(DataType::Int32)
                    })
                }
            )
            .unwrap(),
            double_nested_list123_445566
        );

        // Cast previous double nested lists to double nested varchar lists
        let double_nested_varchar_list123_445566 = ListValue::new(vec![
            Some(ScalarImpl::List(
                list_cast(
                    ListRef::ValueRef {
                        val: &nested_list123,
                    },
                    &DataType::List {
                        datatype: Box::new(DataType::Int32),
                    },
                    &DataType::List {
                        datatype: Box::new(DataType::Varchar),
                    },
                )
                .unwrap(),
            )),
            Some(ScalarImpl::List(
                list_cast(
                    ListRef::ValueRef {
                        val: &nested_list445566,
                    },
                    &DataType::List {
                        datatype: Box::new(DataType::Int32),
                    },
                    &DataType::List {
                        datatype: Box::new(DataType::Varchar),
                    },
                )
                .unwrap(),
            )),
        ]);

        // Double nested Varchar List
        assert_eq!(
            str_to_list(
                "{{{1, 2, 3}}, {{44, 55, 66}}}",
                &DataType::List {
                    datatype: Box::new(DataType::List {
                        datatype: Box::new(DataType::Varchar)
                    })
                }
            )
            .unwrap(),
            double_nested_varchar_list123_445566
        );
    }

    #[test]
    fn test_invalid_str_to_list() {
        // Unbalanced input
        assert!(str_to_list("{{}", &DataType::Int32).is_err());
        assert!(str_to_list("{}}", &DataType::Int32).is_err());
        assert!(str_to_list("{{1, 2, 3}, {4, 5, 6}", &DataType::Int32).is_err());
        assert!(str_to_list("{{1, 2, 3}, 4, 5, 6}}", &DataType::Int32).is_err());
    }

    #[test]
    fn test_bytea() {
        assert_eq!(str_to_bytea("fgo").unwrap().as_ref().to_text(), r"\x66676f");
        assert_eq!(
            str_to_bytea(r"\xDeadBeef").unwrap().as_ref().to_text(),
            r"\xdeadbeef"
        );
        assert_eq!(
            str_to_bytea("12CD").unwrap().as_ref().to_text(),
            r"\x31324344"
        );
        assert_eq!(
            str_to_bytea("1234").unwrap().as_ref().to_text(),
            r"\x31323334"
        );
        assert_eq!(
            str_to_bytea(r"\x12CD").unwrap().as_ref().to_text(),
            r"\x12cd"
        );
        assert_eq!(
            str_to_bytea(r"\x De Ad Be Ef ").unwrap().as_ref().to_text(),
            r"\xdeadbeef"
        );
        assert_eq!(
            str_to_bytea("x De Ad Be Ef ").unwrap().as_ref().to_text(),
            r"\x7820446520416420426520456620"
        );
        assert_eq!(
            str_to_bytea(r"De\\123dBeEf").unwrap().as_ref().to_text(),
            r"\x44655c3132336442654566"
        );
        assert_eq!(
            str_to_bytea(r"De\123dBeEf").unwrap().as_ref().to_text(),
            r"\x4465536442654566"
        );
        assert_eq!(
            str_to_bytea(r"De\\000dBeEf").unwrap().as_ref().to_text(),
            r"\x44655c3030306442654566"
        );
    }

    #[test]
    fn test_struct_cast() {
        assert_eq!(
            struct_cast(
                StructValue::new(vec![
                    Some("1".into()),
                    Some(F32::from(0.0).to_scalar_value()),
                ])
                .as_scalar_ref(),
                &StructType::new(vec![
                    (DataType::Varchar, "a".to_string()),
                    (DataType::Float32, "b".to_string()),
                ]),
                &StructType::new(vec![
                    (DataType::Int32, "a".to_string()),
                    (DataType::Int32, "b".to_string()),
                ])
            )
            .unwrap(),
            StructValue::new(vec![
                Some(1i32.to_scalar_value()),
                Some(0i32.to_scalar_value()),
            ])
        );
    }

    #[test]
    fn test_str_to_timestamp() {
        let str1 = "0001-11-15 07:35:40.999999";
        let timestamp1 = str_to_timestamp(str1).unwrap();
        assert_eq!(timestamp1.0.timestamp_micros(), -62108094259000001);

        let str2 = "1969-12-31 23:59:59.999999";
        let timestamp2 = str_to_timestamp(str2).unwrap();
        assert_eq!(timestamp2.0.timestamp_micros(), -1);
    }

    #[test]
    fn test_timestamp() {
        assert_eq!(
            try_cast::<_, Timestamp>(Date::from_ymd_uncheck(1994, 1, 1)).unwrap(),
            Timestamp::new(
                NaiveDateTime::parse_from_str("1994-1-1 0:0:0", "%Y-%m-%d %H:%M:%S").unwrap()
            )
        )
    }
}
