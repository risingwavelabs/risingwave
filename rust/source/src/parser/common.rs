use risingwave_common::types::{DataTypeKind, ScalarImpl, ScalarRef};
use rust_decimal::Decimal;
use serde_json::Value;

use crate::SourceColumnDesc;

pub(crate) fn json_parse_value(
    column: &SourceColumnDesc,
    value: Option<&Value>,
) -> Option<ScalarImpl> {
    match column.data_type.data_type_kind() {
        DataTypeKind::Boolean => value
            .and_then(|v| v.as_bool())
            .map(|v| ScalarImpl::Bool(v as bool)),
        DataTypeKind::Int16 => value
            .and_then(|v| v.as_i64())
            .map(|v| ScalarImpl::Int16(v as i16)),
        DataTypeKind::Int32 => value
            .and_then(|v| v.as_i64())
            .map(|v| ScalarImpl::Int32(v as i32)),
        DataTypeKind::Int64 => value
            .and_then(|v| v.as_i64())
            .map(|v| ScalarImpl::Int64(v as i64)),
        DataTypeKind::Float32 => value
            .and_then(|v| v.as_f64())
            .map(|v| ScalarImpl::Float32((v as f32).into())),
        DataTypeKind::Float64 => value
            .and_then(|v| v.as_f64())
            .map(|v| ScalarImpl::Float64(v.into())),
        DataTypeKind::Decimal => value
            .and_then(|v| v.as_u64())
            .map(|v| ScalarImpl::Decimal(Decimal::from(v))),
        DataTypeKind::Char | DataTypeKind::Varchar => value
            .and_then(|v| v.as_str())
            .map(|v| ScalarImpl::Utf8(v.to_owned_scalar())),
        _ => unimplemented!(),
    }
}
