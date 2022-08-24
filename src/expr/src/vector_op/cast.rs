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

use std::any::type_name;
use std::str::FromStr;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};
use num_traits::ToPrimitive;
use risingwave_common::array::{Array, ListRef, ListValue};
use risingwave_common::types::{
    DataType, Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper,
    OrderedF32, OrderedF64, Scalar, ScalarImpl, ScalarRefImpl,
};

use crate::{ExprError, Result};

/// String literals for bool type.
///
/// See [`https://www.postgresql.org/docs/9.5/datatype-boolean.html`]
const TRUE_BOOL_LITERALS: [&str; 9] = ["true", "tru", "tr", "t", "on", "1", "yes", "ye", "y"];
const FALSE_BOOL_LITERALS: [&str; 10] = [
    "false", "fals", "fal", "fa", "f", "off", "of", "0", "no", "n",
];
const PARSE_ERROR_STR_TO_TIMESTAMP: &str = "Can't cast string to timestamp (expected format is YYYY-MM-DD HH:MM:SS[.MS] or YYYY-MM-DD HH:MM or YYYY-MM-DD or ISO 8601 format)";
const PARSE_ERROR_STR_TO_TIME: &str =
    "Can't cast string to time (expected format is HH:MM:SS[.MS] or HH:MM)";
const PARSE_ERROR_STR_TO_DATE: &str = "Can't cast string to date (expected format is YYYY-MM-DD)";

#[inline(always)]
pub fn str_to_date(elem: &str) -> Result<NaiveDateWrapper> {
    Ok(NaiveDateWrapper::new(
        NaiveDate::parse_from_str(elem, "%Y-%m-%d")
            .map_err(|_| ExprError::Parse(PARSE_ERROR_STR_TO_DATE))?,
    ))
}

#[inline(always)]
pub fn str_to_time(elem: &str) -> Result<NaiveTimeWrapper> {
    if let Ok(time) = NaiveTime::parse_from_str(elem, "%H:%M:%S%.f") {
        return Ok(NaiveTimeWrapper::new(time));
    }
    if let Ok(time) = NaiveTime::parse_from_str(elem, "%H:%M") {
        return Ok(NaiveTimeWrapper::new(time));
    }
    Err(ExprError::Parse(PARSE_ERROR_STR_TO_TIME))
}

#[inline(always)]
pub fn str_to_timestamp(elem: &str) -> Result<NaiveDateTimeWrapper> {
    if let Ok(timestamp) = NaiveDateTime::parse_from_str(elem, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(NaiveDateTimeWrapper::new(timestamp));
    }
    if let Ok(timestamp) = NaiveDateTime::parse_from_str(elem, "%Y-%m-%d %H:%M") {
        return Ok(NaiveDateTimeWrapper::new(timestamp));
    }
    if let Ok(timestamp) = NaiveDateTime::parse_from_str(elem, "%+") {
        // ISO 8601 format
        return Ok(NaiveDateTimeWrapper::new(timestamp));
    }
    if let Ok(date) = NaiveDate::parse_from_str(elem, "%Y-%m-%d") {
        return Ok(NaiveDateTimeWrapper::new(date.and_hms(0, 0, 0)));
    }
    Err(ExprError::Parse(PARSE_ERROR_STR_TO_TIMESTAMP))
}

#[inline(always)]
pub fn str_to_timestampz(elem: &str) -> Result<i64> {
    DateTime::parse_from_str(elem, "%Y-%m-%d %H:%M:%S %:z")
        .map(|ret| ret.timestamp_nanos() / 1000)
        .map_err(|_| ExprError::Parse(PARSE_ERROR_STR_TO_TIMESTAMP))
}

#[inline(always)]
pub fn timestampz_to_utc_string(elem: i64) -> Result<String> {
    // Just a meaningful representation as placeholder. The real implementation depends on TimeZone
    // from session. See #3552.
    Ok(Utc.timestamp_nanos(elem * 1000).to_rfc3339())
}

#[inline(always)]
pub fn str_parse<T>(elem: &str) -> Result<T>
where
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    elem.parse()
        .map_err(|_| ExprError::Cast(type_name::<str>(), type_name::<T>()))
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
                        ExprError::Cast(
                            std::any::type_name::<T>(),
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

/// In `PostgreSQL`, casting from timestamp to date discards the time part.
#[inline(always)]
pub fn timestamp_to_date(elem: NaiveDateTimeWrapper) -> Result<NaiveDateWrapper> {
    Ok(NaiveDateWrapper(elem.0.date()))
}

/// In `PostgreSQL`, casting from timestamp to time discards the date part.
#[inline(always)]
pub fn timestamp_to_time(elem: NaiveDateTimeWrapper) -> Result<NaiveTimeWrapper> {
    Ok(NaiveTimeWrapper(elem.0.time()))
}

/// In `PostgreSQL`, casting from interval to time discards the days part.
#[inline(always)]
pub fn interval_to_time(elem: IntervalUnit) -> Result<NaiveTimeWrapper> {
    let ms = elem.get_ms_of_day();
    let secs = (ms / 1000) as u32;
    let nano = (ms % 1000 * 1_000_000) as u32;
    Ok(NaiveTimeWrapper(NaiveTime::from_num_seconds_from_midnight(
        secs, nano,
    )))
}

#[inline(always)]
pub fn general_cast<T1, T2>(elem: T1) -> Result<T2>
where
    T1: TryInto<T2> + std::fmt::Debug + Copy,
    <T1 as TryInto<T2>>::Error: std::fmt::Display,
{
    elem.try_into()
        .map_err(|_| ExprError::Cast(std::any::type_name::<T1>(), std::any::type_name::<T2>()))
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
        Err(ExprError::Parse("Invalid bool"))
    }
}

pub fn int32_to_bool(input: i32) -> Result<bool> {
    Ok(input != 0)
}

pub fn general_to_string<T: std::fmt::Display>(elem: T) -> Result<String> {
    Ok(elem.to_string())
}

/// `bool_out` is different from `general_to_string<bool>` to produce a single char. `PostgreSQL`
/// uses different variants of bool-to-string in different situations.
pub fn bool_out(input: bool) -> Result<String> {
    Ok(if input { "t".into() } else { "f".into() })
}

/// This macro helps to cast individual scalars.
macro_rules! gen_cast_impl {
    ([$source:expr, $source_ty:expr, $target_ty:expr], $( { $input:ident, $cast:ident, $func:expr } ),* $(,)?) => {
        match ($source_ty, $target_ty) {
            $(
                ($input! { type_match_pattern }, $cast! { type_match_pattern }) => {
                    let source: <$input! { type_array } as Array>::RefItem<'_> = $source.try_into()?;
                    let target: Result<<$cast! { type_array } as Array>::OwnedItem> = $func(source);
                    target.map(Scalar::to_scalar_value)
                }
            )*
            _ => {
                return Err(ExprError::Cast2($source_ty.clone(), $target_ty.clone()));
            }
        }
    };
}

#[macro_export]
macro_rules! for_each_cast {
    ($macro:ident, $($x:tt, )* ) => {
        $macro! {
            [$($x),*],

            { varchar, date, str_to_date },
            { varchar, time, str_to_time },
            { varchar, interval, str_parse },
            { varchar, timestamp, str_to_timestamp },
            { varchar, timestampz, str_to_timestampz },
            { varchar, int16, str_parse },
            { varchar, int32, str_parse },
            { varchar, int64, str_parse },
            { varchar, float32, str_parse },
            { varchar, float64, str_parse },
            { varchar, decimal, str_parse },
            { varchar, boolean, str_to_bool },

            { boolean, varchar, general_to_string },
            { int16, varchar, general_to_string },
            { int32, varchar, general_to_string },
            { int64, varchar, general_to_string },
            { float32, varchar, general_to_string },
            { float64, varchar, general_to_string },
            { decimal, varchar, general_to_string },
            { time, varchar, general_to_string },
            { interval, varchar, general_to_string },
            { date, varchar, general_to_string },
            { timestamp, varchar, general_to_string },
            { timestampz, varchar, timestampz_to_utc_string },

            { boolean, int32, general_cast },
            { int32, boolean, int32_to_bool },

            { int16, int32, general_cast },
            { int16, int64, general_cast },
            { int16, float32, general_cast },
            { int16, float64, general_cast },
            { int16, decimal, general_cast },
            { int32, int16, general_cast },
            { int32, int64, general_cast },
            { int32, float32, to_f32 }, // lossy
            { int32, float64, general_cast },
            { int32, decimal, general_cast },
            { int64, int16, general_cast },
            { int64, int32, general_cast },
            { int64, float32, to_f32 }, // lossy
            { int64, float64, to_f64 }, // lossy
            { int64, decimal, general_cast },

            { float32, float64, general_cast },
            { float32, decimal, general_cast },
            { float32, int16, to_i16 },
            { float32, int32, to_i32 },
            { float32, int64, to_i64 },
            { float64, decimal, general_cast },
            { float64, int16, to_i16 },
            { float64, int32, to_i32 },
            { float64, int64, to_i64 },
            { float64, float32, to_f32 }, // lossy

            { decimal, int16, dec_to_i16 },
            { decimal, int32, dec_to_i32 },
            { decimal, int64, dec_to_i64 },
            { decimal, float32, to_f32 },
            { decimal, float64, to_f64 },

            { date, timestamp, general_cast },
            { time, interval, general_cast },
            { timestamp, date, timestamp_to_date },
            { timestamp, time, timestamp_to_time },
            { interval, time, interval_to_time },
        }
    };
}

#[inline(always)]
pub fn str_to_list(input: &str, target_elem_type: &DataType) -> Result<ListValue> {
    // Trim input
    let trimmed = input.trim();

    // Ensure input string is correctly braced.
    let mut chars = trimmed.chars();
    risingwave_common::ensure!(
        chars.next() == Some('{'),
        "First character should be left brace '{{'"
    );
    risingwave_common::ensure!(
        chars.next_back() == Some('}'),
        "Last character should be right brace '}}'"
    );

    // Return a new ListValue.
    // For each &str in the comma separated input a ScalarRefImpl is initialized which in turn is
    // cast into the target DataType. If the target DataType is of type Varchar, then no casting is
    // needed.
    Ok(ListValue::new(
        chars
            .as_str()
            .split(',')
            .map(|s| {
                Some(ScalarRefImpl::Utf8(s.trim()))
                    .map(|scalar_ref| {
                        if target_elem_type == &DataType::Varchar {
                            Ok(scalar_ref.into_scalar_impl())
                        } else {
                            scalar_cast(scalar_ref, &DataType::Varchar, target_elem_type)
                        }
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
    input: ListRef,
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

/// Cast scalar ref with `source_type` into owned scalar with `target_type`. This function forms a
/// mutual recursion with `list_cast` so that we can cast nested lists (e.g., varchar[][] to
/// int[][]).
fn scalar_cast(
    source: ScalarRefImpl,
    source_type: &DataType,
    target_type: &DataType,
) -> Result<ScalarImpl> {
    use crate::expr::data_types::*;

    match (source_type, target_type) {
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
            for_each_cast!(gen_cast_impl, source, source_type, target_type,)
        }
    }
}

#[cfg(test)]
mod tests {
    use num_traits::FromPrimitive;

    use super::*;

    #[test]
    fn parse_str() {
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
            str_to_timestamp("1999-01-08 04:05:06AA")
                .unwrap_err()
                .to_string(),
            ExprError::Parse(PARSE_ERROR_STR_TO_TIMESTAMP).to_string()
        );
        assert_eq!(
            str_to_date("1999-01-08AA").unwrap_err().to_string(),
            "Parse error: Can't cast string to date (expected format is YYYY-MM-DD)".to_string()
        );
        assert_eq!(
            str_to_time("AA04:05:06").unwrap_err().to_string(),
            ExprError::Parse(PARSE_ERROR_STR_TO_TIME).to_string()
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

        assert_eq!(general_to_string(true).unwrap(), "true");
        assert_eq!(general_to_string(false).unwrap(), "false");

        assert_eq!(general_to_string(32).unwrap(), "32");
        assert_eq!(general_to_string(-32).unwrap(), "-32");
        assert_eq!(general_to_string(i32::MIN).unwrap(), "-2147483648");
        assert_eq!(general_to_string(i32::MAX).unwrap(), "2147483647");

        assert_eq!(general_to_string(i16::MIN).unwrap(), "-32768");
        assert_eq!(general_to_string(i16::MAX).unwrap(), "32767");

        assert_eq!(general_to_string(i64::MIN).unwrap(), "-9223372036854775808");
        assert_eq!(general_to_string(i64::MAX).unwrap(), "9223372036854775807");

        assert_eq!(general_to_string(32.12).unwrap(), "32.12");
        assert_eq!(general_to_string(-32.14).unwrap(), "-32.14");

        assert_eq!(general_to_string(32.12_f32).unwrap(), "32.12");
        assert_eq!(general_to_string(-32.14_f32).unwrap(), "-32.14");

        assert_eq!(
            general_to_string(Decimal::from_f64(1.222).unwrap()).unwrap(),
            "1.222"
        );

        assert_eq!(general_to_string(Decimal::NaN).unwrap(), "NaN");
    }

    #[test]
    fn temporal_cast() {
        assert_eq!(
            timestamp_to_date(str_to_timestamp("1999-01-08 04:02").unwrap()).unwrap(),
            str_to_date("1999-01-08").unwrap(),
        );
        assert_eq!(
            timestamp_to_time(str_to_timestamp("1999-01-08 04:02").unwrap()).unwrap(),
            str_to_time("04:02").unwrap(),
        );
        assert_eq!(
            interval_to_time(IntervalUnit::new(1, 2, 61003)).unwrap(),
            str_to_time("00:01:01.003").unwrap(),
        );
        assert_eq!(
            interval_to_time(IntervalUnit::new(0, 0, -61003)).unwrap(),
            str_to_time("23:58:58.997").unwrap(),
        );
    }
}
