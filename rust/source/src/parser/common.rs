use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataTypeKind, Decimal, ScalarImpl, ScalarRef};
use risingwave_common::vector_op::cast::str_to_date;
use serde_json::Value;

use crate::SourceColumnDesc;

macro_rules! make_ScalarImpl {
    ($x:expr, $y:expr) => {
        match $x {
            Some(v) => return Ok($y(v)),
            None => return Err(RwError::from(InternalError("json parse error".to_string()))),
        }
    };
}

pub(crate) fn json_parse_value(
    column: &SourceColumnDesc,
    value: Option<&Value>,
) -> Result<ScalarImpl> {
    match column.data_type.data_type_kind() {
        DataTypeKind::Boolean => {
            make_ScalarImpl!(value.and_then(|v| v.as_bool()), |x| ScalarImpl::Bool(
                x as bool
            ))
        }
        DataTypeKind::Int16 => {
            make_ScalarImpl!(value.and_then(|v| v.as_i64()), |x| ScalarImpl::Int16(
                x as i16
            ))
        }
        DataTypeKind::Int32 => {
            make_ScalarImpl!(value.and_then(|v| v.as_i64()), |x| ScalarImpl::Int32(
                x as i32
            ))
        }
        DataTypeKind::Int64 => {
            make_ScalarImpl!(value.and_then(|v| v.as_i64()), |x| ScalarImpl::Int64(
                x as i64
            ))
        }
        DataTypeKind::Float32 => {
            make_ScalarImpl!(value.and_then(|v| v.as_f64()), |v| ScalarImpl::Float32(
                (v as f32).into()
            ))
        }
        DataTypeKind::Float64 => {
            make_ScalarImpl!(
                value.and_then(|v| v.as_f64()),
                |v: f64| ScalarImpl::Float64(v.into())
            )
        }
        DataTypeKind::Decimal => {
            make_ScalarImpl!(value.and_then(|v| v.as_u64()), |v| ScalarImpl::Decimal(
                Decimal::from(v)
            ))
        }
        DataTypeKind::Char | DataTypeKind::Varchar => make_ScalarImpl!(
            value.and_then(|v| v.as_str()),
            |v: &str| ScalarImpl::Utf8(v.to_owned_scalar())
        ),
        DataTypeKind::Date => match value.and_then(|v| v.as_str()) {
            None => Err(RwError::from(InternalError("parse error".to_string()))),
            Some(date_str) => match str_to_date(date_str) {
                Ok(date) => Ok(ScalarImpl::Int32(date as i32)),
                Err(e) => Err(e),
            },
        },
        _ => unimplemented!(),
    }
}
