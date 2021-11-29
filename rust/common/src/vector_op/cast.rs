// TODO: delete vector_op. use new vectorized
use crate::array::{Array, ArrayBuilder, ArrayImpl, ArrayRef, I32Array, I64Array};
use crate::error::ErrorCode::InvalidInputSyntax;
use crate::error::{ErrorCode::ParseError, Result, RwError};
use crate::types::{DataTypeKind, DataTypeRef, Scalar};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

pub fn vec_cast(
    un_casted_arr: ArrayRef,
    src_type: DataTypeRef,
    dst_type: DataTypeRef,
) -> Result<ArrayImpl> {
    match (src_type.data_type_kind(), dst_type.data_type_kind()) {
        (DataTypeKind::Char, DataTypeKind::Date) => {
            vector_cast_op(un_casted_arr.as_utf8(), str_to_date).map(|arr: I32Array| arr.into())
        }
        (DataTypeKind::Char, DataTypeKind::Time) => {
            vector_cast_op(un_casted_arr.as_utf8(), str_to_time).map(|arr: I64Array| arr.into())
        }
        (DataTypeKind::Char, DataTypeKind::Timestamp) => {
            vector_cast_op(un_casted_arr.as_utf8(), str_to_timestamp)
                .map(|arr: I64Array| arr.into())
        }
        (DataTypeKind::Char, DataTypeKind::Timestampz) => {
            vector_cast_op(un_casted_arr.as_utf8(), str_to_timestampz)
                .map(|arr: I64Array| arr.into())
        }

        (other_src_type, other_dst_type) => todo!(
            "cast from {:?} to {:?} is not implemented",
            other_src_type,
            other_dst_type
        ),
    }
}

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

fn vector_cast_op<'a, A1, A2, F>(a: &'a A1, f: F) -> Result<A2>
where
    A1: Array,
    A2: Array,
    F: Fn(A1::RefItem<'a>) -> Result<A2::OwnedItem>,
{
    let mut builder = A2::Builder::new(a.len())?;
    for elem in a.iter() {
        if let Some(x) = elem {
            let casted = f(x)?;
            builder.append(Some(casted.as_scalar_ref()))?;
        } else {
            builder.append(None)?;
        }
    }
    builder.finish()
}
