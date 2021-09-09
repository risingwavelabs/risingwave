use crate::array2::{Array, ArrayBuilder, ArrayImpl, ArrayRef, I32Array, UTF8Array};
use crate::error::{Result, RwError};
use crate::types::{DataTypeKind, DataTypeRef, Scalar};
use chrono::{Datelike, NaiveDate};

pub fn vec_cast(
    un_casted_arr: ArrayRef,
    src_type: DataTypeRef,
    dst_type: DataTypeRef,
) -> Result<ArrayImpl> {
    match (src_type.data_type_kind(), dst_type.data_type_kind()) {
        (DataTypeKind::Char, DataTypeKind::Date) => {
            vector_cast_str_to_date(un_casted_arr.as_utf8())
        }

        (other_src_type, other_dst_type) => todo!(
            "cast from {:?} to {:?} is not implemented",
            other_src_type,
            other_dst_type
        ),
    }
    .map(|arr| arr.into())
}

// The same as NaiveDate::from_ymd(1970, 1, 1).num_days_from_ce().
// Minus this magic number to store the number of days since 1970-01-01.
const UNIX_EPOCH_DAYS: i32 = 719_163;

#[inline(always)]
fn str_to_date(elem: &str) -> Result<i32> {
    NaiveDate::parse_from_str(elem, "%Y-%m-%d")
        .map(|ret| ret.num_days_from_ce() - UNIX_EPOCH_DAYS)
        .map_err(RwError::from)
}

pub fn vector_cast_str_to_date(arr: &UTF8Array) -> Result<I32Array> {
    vector_cast_op(arr, str_to_date)
}

pub fn vector_cast_op<'a, A1, A2, F>(a: &'a A1, f: F) -> Result<A2>
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
