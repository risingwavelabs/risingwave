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
use std::fmt::Write;
use std::str::FromStr;

use chrono::{DateTime, Days, Duration, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use itertools::Itertools;
use num_traits::ToPrimitive;
use risingwave_common::array::{Array, JsonbRef, ListRef, ListValue, StructRef, StructValue};
use risingwave_common::types::struct_type::StructType;
use risingwave_common::types::to_text::ToText;
use risingwave_common::types::{
    DataType, Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper,
    OrderedF32, OrderedF64, Scalar, ScalarImpl, ScalarRefImpl,
};
use risingwave_common::util::iter_util::ZipEqFast;
use speedate::{Date as SpeedDate, DateTime as SpeedDateTime, Time as SpeedTime};

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

#[inline(always)]
pub fn i32_to_date(d: i32) -> Result<NaiveDateWrapper> {
    Ok(NaiveDateWrapper::new(
        NaiveDate::from_num_days_from_ce_opt(d).unwrap() + Days::new(719_163),
    ))
}

#[inline(always)]
pub fn str_to_date(elem: &str) -> Result<NaiveDateWrapper> {
    Ok(NaiveDateWrapper::new(parse_naive_date(elem)?))
}

#[inline(always)]
pub fn i64_to_time(t: i64) -> Result<NaiveTimeWrapper> {
    let (time, _) = NaiveTime::from_num_seconds_from_midnight_opt(0, 0).unwrap().overflowing_add_signed(Duration::microseconds(t));
    Ok(NaiveTimeWrapper::new(time))
}

#[inline(always)]
pub fn str_to_time(elem: &str) -> Result<NaiveTimeWrapper> {
    Ok(NaiveTimeWrapper::new(parse_naive_time(elem)?))
}

#[inline(always)]
pub fn str_to_timestamp(elem: &str) -> Result<NaiveDateTimeWrapper> {
    Ok(NaiveDateTimeWrapper::new(parse_naive_datetime(elem)?))
}

#[inline]
fn parse_naive_datetime(s: &str) -> Result<NaiveDateTime> {
    if let Ok(res) = SpeedDateTime::parse_str(s) {
        Ok(NaiveDateWrapper::from_ymd_uncheck(
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
            NaiveDateWrapper::from_ymd_uncheck(res.year as i32, res.month as u32, res.day as u32)
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
pub fn i64_to_timestamp(t: i64) -> Result<NaiveDateTimeWrapper> {
    let us = i64_to_timestamptz(t)?;
    Ok(NaiveDateTimeWrapper::from_timestamp_uncheck(
        us / 1_000_000,
        (us % 1_000_000) as u32 * 1000,
    ))
}

#[inline]
fn parse_naive_date(s: &str) -> Result<NaiveDate> {
    let res =
        SpeedDate::parse_str(s).map_err(|_| ExprError::Parse(PARSE_ERROR_STR_TO_DATE.into()))?;
    Ok(NaiveDateWrapper::from_ymd_uncheck(res.year as i32, res.month as u32, res.day as u32).0)
}

#[inline]
fn parse_naive_time(s: &str) -> Result<NaiveTime> {
    let res =
        SpeedTime::parse_str(s).map_err(|_| ExprError::Parse(PARSE_ERROR_STR_TO_TIME.into()))?;
    Ok(NaiveTimeWrapper::from_hms_micro_uncheck(
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

#[inline(always)]
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

#[inline(always)]
pub fn str_parse<T>(elem: &str) -> Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    elem.trim()
        .parse()
        .map_err(|_| ExprError::Parse(type_name::<T>().into()))
}

/// Define the cast function to primitive types.
///
/// Due to the orphan rule, some data can't implement `TryFrom` trait for basic type.
/// We can only use [`ToPrimitive`] trait.
///
/// Note: this might be lossy according to the docs from [`ToPrimitive`]:
/// > On the other hand, conversions with possible precision loss or truncation
/// are admitted, like an `f32` with a decimal part to an integer type, or
/// even a large `f64` saturating to `f32` infinity.
macro_rules! define_cast_to_primitive {
    ($ty:ty) => {
        define_cast_to_primitive! { $ty, $ty }
    };
    ($ty:ty, $wrapper_ty:ty) => {
        paste::paste! {
            #[inline(always)]
            pub fn [<to_ $ty>]<T>(elem: T) -> Result<$wrapper_ty>
            where
                T: ToPrimitive + std::fmt::Debug,
            {
                elem.[<to_ $ty>]()
                    .ok_or_else(|| {
                        ExprError::CastOutOfRange(
                            std::any::type_name::<$ty>()
                        )
                    })
                    .map(Into::into)
            }
        }
    };
}

define_cast_to_primitive! { i16 }
define_cast_to_primitive! { i32 }
define_cast_to_primitive! { i64 }
define_cast_to_primitive! { f32, OrderedF32 }
define_cast_to_primitive! { f64, OrderedF64 }

// In postgresSql, the behavior of casting decimal to integer is rounding.
// We should write them separately
#[inline(always)]
pub fn dec_to_i16(elem: Decimal) -> Result<i16> {
    to_i16(elem.round_dp(0))
}

#[inline(always)]
pub fn dec_to_i32(elem: Decimal) -> Result<i32> {
    to_i32(elem.round_dp(0))
}

#[inline(always)]
pub fn dec_to_i64(elem: Decimal) -> Result<i64> {
    to_i64(elem.round_dp(0))
}

#[inline(always)]
pub fn jsonb_to_bool(v: JsonbRef<'_>) -> Result<bool> {
    v.as_bool().map_err(|e| ExprError::Parse(e.into()))
}

#[inline(always)]
pub fn jsonb_to_dec(v: JsonbRef<'_>) -> Result<Decimal> {
    v.as_number()
        .map_err(|e| ExprError::Parse(e.into()))
        .map(Into::into)
}

/// Similar to and an result of [`define_cast_to_primitive`] macro above.
/// If that was implemented as a trait to cast from `f64`, this could also call them via trait
/// rather than macro.
///
/// Note that PostgreSQL casts JSON numbers from arbitrary precision `numeric` but we use `f64`.
/// This is less powerful but still meets RFC 8259 interoperability.
macro_rules! define_jsonb_to_number {
    ($ty:ty) => {
        define_jsonb_to_number! { $ty, $ty }
    };
    ($ty:ty, $wrapper_ty:ty) => {
        paste::paste! {
            #[inline(always)]
            pub fn [<jsonb_to_ $ty>](v: JsonbRef<'_>) -> Result<$wrapper_ty> {
                v.as_number().map_err(|e| ExprError::Parse(e.into())).and_then([<to_ $ty>])
            }
        }
    };
}
define_jsonb_to_number! { i16 }
define_jsonb_to_number! { i32 }
define_jsonb_to_number! { i64 }
define_jsonb_to_number! { f32, OrderedF32 }
define_jsonb_to_number! { f64, OrderedF64 }

/// In `PostgreSQL`, casting from timestamp to date discards the time part.
#[inline(always)]
pub fn timestamp_to_date(elem: NaiveDateTimeWrapper) -> NaiveDateWrapper {
    NaiveDateWrapper(elem.0.date())
}

/// In `PostgreSQL`, casting from timestamp to time discards the date part.
#[inline(always)]
pub fn timestamp_to_time(elem: NaiveDateTimeWrapper) -> NaiveTimeWrapper {
    NaiveTimeWrapper(elem.0.time())
}

/// In `PostgreSQL`, casting from interval to time discards the days part.
#[inline(always)]
pub fn interval_to_time(elem: IntervalUnit) -> NaiveTimeWrapper {
    let ms = elem.get_ms_of_day();
    let secs = (ms / 1000) as u32;
    let nano = (ms % 1000 * 1_000_000) as u32;
    NaiveTimeWrapper::from_num_seconds_from_midnight_uncheck(secs, nano)
}

#[inline(always)]
pub fn try_cast<T1, T2>(elem: T1) -> Result<T2>
where
    T1: TryInto<T2> + std::fmt::Debug + Copy,
    <T1 as TryInto<T2>>::Error: std::fmt::Display,
{
    elem.try_into()
        .map_err(|_| ExprError::CastOutOfRange(std::any::type_name::<T2>()))
}

#[inline(always)]
pub fn cast<T1, T2>(elem: T1) -> T2
where
    T1: Into<T2>,
{
    elem.into()
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
        .any(|s| trimmed_input.eq_ignore_ascii_case(s))
    {
        Ok(false)
    } else {
        Err(ExprError::Parse("Invalid bool".into()))
    }
}

pub fn int32_to_bool(input: i32) -> Result<bool> {
    Ok(input != 0)
}

// For most of the types, cast them to varchar is similar to return their text format.
// So we use this function to cast type to varchar.
pub fn general_to_text(elem: impl ToText, mut writer: &mut dyn Write) -> Result<()> {
    elem.write(&mut writer).unwrap();
    Ok(())
}

pub fn bool_to_varchar(input: bool, writer: &mut dyn Write) -> Result<()> {
    writer
        .write_str(if input { "true" } else { "false" })
        .unwrap();
    Ok(())
}

/// `bool_out` is different from `general_to_string<bool>` to produce a single char. `PostgreSQL`
/// uses different variants of bool-to-string in different situations.
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
        DataType::Decimal => str_parse::<Decimal>(s)?.into(),
        DataType::Float32 => str_parse::<OrderedF32>(s)?.into(),
        DataType::Float64 => str_parse::<OrderedF64>(s)?.into(),
        DataType::Varchar => return Err(None),
        DataType::Date => str_to_date(s)?.into(),
        DataType::Timestamp => str_to_timestamp(s)?.into(),
        // We only handle the case with timezone here, and leave the implicit session timezone case
        // for later phase.
        DataType::Timestamptz => str_with_time_zone_to_timestamptz(s)?.into(),
        DataType::Time => str_to_time(s)?.into(),
        DataType::Interval => str_parse::<IntervalUnit>(s)?.into(),
        // Not processing list or struct literal right now. Leave it for later phase (normal backend
        // evaluation).
        DataType::List { .. } => return Err(None),
        DataType::Struct(_) => return Err(None),
        DataType::Jsonb => return Err(None),
        DataType::Bytea => str_to_bytea(s)?.into(),
    };
    Ok(scalar)
}

/// It accepts a macro whose input is `{ $input:ident, $cast:ident, $func:expr }` tuples
///
/// * `$input`: input type
/// * `$cast`: The cast type in that the operation will calculate
/// * `$func`: The scalar function for expression, it's a generic function and specialized by the
///   type of `$input, $cast`
/// * `$infallible`: Whether the cast is infallible
#[macro_export]
macro_rules! for_all_cast_variants {
    ($macro:ident) => {
        $macro! {
            { varchar, date, str_to_date, false },
            { varchar, time, str_to_time, false },
            { varchar, interval, str_parse, false },
            { varchar, timestamp, str_to_timestamp, false },
            { varchar, int16, str_parse, false },
            { varchar, int32, str_parse, false },
            { varchar, int64, str_parse, false },
            { varchar, float32, str_parse, false },
            { varchar, float64, str_parse, false },
            { varchar, decimal, str_parse, false },
            { varchar, boolean, str_to_bool, false },
            { varchar, bytea, str_to_bytea, false },
            { varchar, jsonb, str_parse, false },
            // `str_to_list` requires `target_elem_type` and is handled elsewhere

            { boolean, varchar, bool_to_varchar, false },
            { int16, varchar, general_to_text, false },
            { int32, varchar, general_to_text, false },
            { int64, varchar, general_to_text, false },
            { float32, varchar, general_to_text, false },
            { float64, varchar, general_to_text, false },
            { decimal, varchar, general_to_text, false },
            { time, varchar, general_to_text, false },
            { interval, varchar, general_to_text, false },
            { date, varchar, general_to_text, false },
            { timestamp, varchar, general_to_text, false },
            { jsonb, varchar, |x, w| general_to_text(x, w), false },
            { list, varchar, |x, w| general_to_text(x, w), false },

            { jsonb, boolean, jsonb_to_bool, false },
            { jsonb, int16, jsonb_to_i16, false },
            { jsonb, int32, jsonb_to_i32, false },
            { jsonb, int64, jsonb_to_i64, false },
            { jsonb, decimal, jsonb_to_dec, false },
            { jsonb, float32, jsonb_to_f32, false },
            { jsonb, float64, jsonb_to_f64, false },

            { boolean, int32, try_cast, false },
            { int32, boolean, int32_to_bool, false },

            { int16, int32, cast::<i16, i32>, true },
            { int16, int64, cast::<i16, i64>, true },
            { int16, float32, cast::<i16, OrderedF32>, true },
            { int16, float64, cast::<i16, OrderedF64>, true },
            { int16, decimal, cast::<i16, Decimal>, true },
            { int32, int16, try_cast, false },
            { int32, int64, cast::<i32, i64>, true },
            { int32, float32, to_f32, false }, // lossy
            { int32, float64, cast::<i32, OrderedF64>, true },
            { int32, decimal, cast::<i32, Decimal>, true },
            { int64, int16, try_cast, false },
            { int64, int32, try_cast, false },
            { int64, float32, to_f32, false }, // lossy
            { int64, float64, to_f64, false }, // lossy
            { int64, decimal, cast::<i64, Decimal>, true },

            { float32, float64, cast::<OrderedF32, OrderedF64>, true },
            { float32, decimal, cast::<OrderedF32, Decimal>, true },
            { float32, int16, to_i16, false },
            { float32, int32, to_i32, false },
            { float32, int64, to_i64, false },
            { float64, decimal, cast::<OrderedF64, Decimal>, true },
            { float64, int16, to_i16, false },
            { float64, int32, to_i32, false },
            { float64, int64, to_i64, false },
            { float64, float32, to_f32, false }, // lossy

            { decimal, int16, dec_to_i16, false },
            { decimal, int32, dec_to_i32, false },
            { decimal, int64, dec_to_i64, false },
            { decimal, float32, to_f32, false },
            { decimal, float64, to_f64, false },

            { date, timestamp, cast::<NaiveDateWrapper, NaiveDateTimeWrapper>, true },
            { time, interval, cast::<NaiveTimeWrapper, IntervalUnit>, true },
            { timestamp, date, timestamp_to_date, true },
            { timestamp, time, timestamp_to_time, true },
            { interval, time, interval_to_time, true }
        }
    };
}

// TODO(nanderstabel): optimize for multidimensional List. Depth can be given as a parameter to this
// function.
fn unnest(input: &str) -> Result<Vec<String>> {
    // Trim input
    let trimmed = input.trim();

    let mut chars = trimmed.chars();
    if chars.next() != Some('{') || chars.next_back() != Some('}') {
        return Err(ExprError::Parse("Input must be braced".into()));
    }

    let mut items = Vec::new();
    while let Some(c) = chars.next() {
        match c {
            '{' => {
                let mut string = String::from(c);
                let mut depth = 1;
                while depth != 0 {
                    let c = match chars.next() {
                        Some(c) => {
                            if c == '{' {
                                depth += 1;
                            } else if c == '}' {
                                depth -= 1;
                            }
                            c
                        }
                        None => {
                            return Err(ExprError::Parse(
                                "Missing closing brace '}}' character".into(),
                            ))
                        }
                    };
                    string.push(c);
                }
                items.push(string);
            }
            '}' => {
                return Err(ExprError::Parse(
                    "Unexpected closing brace '}}' character".into(),
                ))
            }
            ',' => {}
            c if c.is_whitespace() => {}
            c => items.push(format!(
                "{}{}",
                c,
                chars.take_while_ref(|&c| c != ',').collect::<String>()
            )),
        }
    }
    Ok(items)
}

#[inline(always)]
pub fn str_to_list(input: &str, target_elem_type: &DataType) -> Result<ListValue> {
    // Return a new ListValue.
    // For each &str in the comma separated input a ScalarRefImpl is initialized which in turn
    // is cast into the target DataType. If the target DataType is of type Varchar, then
    // no casting is needed.
    Ok(ListValue::new(
        unnest(input)?
            .iter()
            .map(|s| {
                Some(ScalarRefImpl::Utf8(s.trim()))
                    .map(|scalar_ref| match target_elem_type {
                        DataType::Varchar => Ok(scalar_ref.into_scalar_impl()),
                        _ => scalar_cast(scalar_ref, &DataType::Varchar, target_elem_type),
                    })
                    .transpose()
            })
            .try_collect()?,
    ))
}

/// Cast array with `source_elem_type` into array with `target_elem_type` by casting each element.
///
/// TODO: `.map(scalar_cast)` is not a preferred pattern and we should avoid it if possible.
pub fn list_cast(
    input: ListRef<'_>,
    source_elem_type: &DataType,
    target_elem_type: &DataType,
) -> Result<ListValue> {
    Ok(ListValue::new(
        input
            .values_ref()
            .into_iter()
            .map(|datum_ref| {
                datum_ref
                    .map(|scalar_ref| scalar_cast(scalar_ref, source_elem_type, target_elem_type))
                    .transpose()
            })
            .try_collect()?,
    ))
}

/// Cast struct of `source_elem_type` to `target_elem_type` by casting each element.
pub fn struct_cast(
    input: StructRef<'_>,
    source_elem_type: &StructType,
    target_elem_type: &StructType,
) -> Result<StructValue> {
    Ok(StructValue::new(
        input
            .fields_ref()
            .into_iter()
            .zip_eq_fast(source_elem_type.fields.iter())
            .zip_eq_fast(target_elem_type.fields.iter())
            .map(|((datum_ref, source_elem_type), target_elem_type)| {
                if source_elem_type == target_elem_type {
                    return Ok(datum_ref.map(|scalar_ref| scalar_ref.into_scalar_impl()));
                }
                datum_ref
                    .map(|scalar_ref| scalar_cast(scalar_ref, source_elem_type, target_elem_type))
                    .transpose()
            })
            .try_collect()?,
    ))
}

/// Cast scalar ref with `source_type` into owned scalar with `target_type`. This function forms a
/// mutual recursion with `list_cast` so that we can cast nested lists (e.g., varchar[][] to
/// int[][]).
fn scalar_cast(
    source: ScalarRefImpl<'_>,
    source_type: &DataType,
    target_type: &DataType,
) -> Result<ScalarImpl> {
    use crate::expr::data_types::*;

    match (source_type, target_type) {
        (DataType::Struct(source_type), DataType::Struct(target_type)) => {
            Ok(struct_cast(source.try_into()?, source_type, target_type)?.to_scalar_value())
        }
        (
            DataType::List {
                datatype: source_elem_type,
            },
            DataType::List {
                datatype: target_elem_type,
            },
        ) => list_cast(source.try_into()?, source_elem_type, target_elem_type)
            .map(Scalar::to_scalar_value),
        (
            DataType::Varchar,
            DataType::List {
                datatype: target_elem_type,
            },
        ) => str_to_list(source.try_into()?, target_elem_type).map(Scalar::to_scalar_value),
        (source_type, target_type) => {
            macro_rules! gen_cast_impl {
                ($( { $input:ident, $cast:ident, $func:expr, $infallible:ident } ),*) => {
                    match (source_type, target_type) {
                        $(
                            ($input! { type_match_pattern }, $cast! { type_match_pattern }) => gen_cast_impl!(arm: $input, $cast, $func, $infallible),
                        )*
                        _ => {
                            return Err(ExprError::UnsupportedCast(source_type.clone(), target_type.clone()));
                        }
                    }
                };
                (arm: $input:ident, varchar, $func:expr, false) => {
                    {
                        let source: <$input! { type_array } as Array>::RefItem<'_> = source.try_into()?;
                        let mut writer = String::new();
                        let target: Result<()> = $func(source, &mut writer);
                        target.map(|_| Scalar::to_scalar_value(writer.into_boxed_str()))
                    }
                };
                (arm: $input:ident, $cast:ident, $func:expr, false) => {
                    {
                        let source: <$input! { type_array } as Array>::RefItem<'_> = source.try_into()?;
                        let target: Result<<$cast! { type_array } as Array>::OwnedItem> = $func(source);
                        target.map(Scalar::to_scalar_value)
                    }
                };
                (arm: $input:ident, $cast:ident, $func:expr, true) => {
                    {
                        let source: <$input! { type_array } as Array>::RefItem<'_> = source.try_into()?;
                        let target: Result<<$cast! { type_array } as Array>::OwnedItem> = Ok($func(source));
                        target.map(Scalar::to_scalar_value)
                    }
                };
            }
            for_all_cast_variants!(gen_cast_impl)
        }
    }
}

#[cfg(test)]
mod tests {

    use num_traits::FromPrimitive;

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

        test!(general_to_text(OrderedF64::from(32.12)), "32.12");
        test!(general_to_text(OrderedF64::from(-32.14)), "-32.14");

        test!(general_to_text(OrderedF32::from(32.12_f32)), "32.12");
        test!(general_to_text(OrderedF32::from(-32.14_f32)), "-32.14");

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
            interval_to_time(IntervalUnit::new(1, 2, 61003)),
            str_to_time("00:01:01.003").unwrap(),
        );
        assert_eq!(
            interval_to_time(IntervalUnit::new(0, 0, -61003)),
            str_to_time("23:58:58.997").unwrap(),
        );
    }

    #[test]
    fn test_unnest() {
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
                    Some(OrderedF32::from(0.0).to_scalar_value()),
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
}
