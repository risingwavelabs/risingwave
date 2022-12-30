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

use anyhow::{anyhow, Result};
use itertools::Itertools;
use num_traits::FromPrimitive;
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::types::{DataType, Datum, Decimal, ScalarImpl};
use risingwave_expr::vector_op::cast::{
    i64_to_timestamp, i64_to_timestamptz, str_to_date, str_to_time, str_to_timestamp,
    str_to_timestamptz,
};
#[cfg(not(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
)))]
use serde_json::Value;
#[cfg(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
))]
use simd_json::{value::StaticNode, BorrowedValue, ValueAccess};

use crate::{ensure_int, ensure_str};

#[cfg(not(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
)))]
fn do_parse_json_value(dtype: &DataType, v: &Value) -> Result<ScalarImpl> {
    use crate::ensure_float;
    let v = match dtype {
        DataType::Boolean => v.as_bool().ok_or_else(|| anyhow!("expect bool"))?.into(),
        DataType::Int16 => ScalarImpl::Int16(
            ensure_int!(v, i16)
                .try_into()
                .map_err(|e| anyhow!("expect i16: {}", e))?,
        ),
        DataType::Int32 => ScalarImpl::Int32(
            ensure_int!(v, i32)
                .try_into()
                .map_err(|e| anyhow!("expect i32: {}", e))?,
        ),
        DataType::Int64 => ensure_int!(v, i64).into(),
        DataType::Float32 => ScalarImpl::Float32((ensure_float!(v, f32) as f32).into()),
        DataType::Float64 => ScalarImpl::Float64((ensure_float!(v, f64)).into()),
        // FIXME: decimal should have more precision than f64
        DataType::Decimal => Decimal::from_f64(ensure_float!(v, Decimal))
            .ok_or_else(|| anyhow!("expect decimal"))?
            .into(),
        DataType::Bytea => ensure_str!(v, "bytea").to_string().into(),
        DataType::Varchar => ensure_str!(v, "varchar").to_string().into(),
        DataType::Date => str_to_date(ensure_str!(v, "date"))?.into(),
        DataType::Time => str_to_time(ensure_str!(v, "time"))?.into(),
        DataType::Timestamp => match v {
            Value::String(s) => str_to_timestamp(s)?.into(),
            Value::Number(_n) => i64_to_timestamp(ensure_int!(v, i64))?.into(),
            _ => anyhow::bail!("expect timestamp, but found {v}"),
        },
        DataType::Timestamptz => match v {
            Value::String(s) => str_to_timestamptz(s)?.into(),
            Value::Number(_n) => i64_to_timestamptz(ensure_int!(v, i64))?.into(),
            _ => anyhow::bail!("expect timestamptz, but found {v}"),
        },
        DataType::Struct(struct_type_info) => {
            let fields = struct_type_info
                .field_names
                .iter()
                .zip_eq(struct_type_info.fields.iter())
                .map(|field| json_parse_value(field.1, v.get(field.0)))
                .collect::<Result<Vec<Datum>>>()?;
            ScalarImpl::Struct(StructValue::new(fields))
        }
        DataType::List {
            datatype: item_type,
        } => {
            if let Value::Array(values) = v {
                let values = values
                    .iter()
                    .map(|v| json_parse_value(item_type, Some(v)))
                    .collect::<Result<Vec<Datum>>>()?;
                ScalarImpl::List(ListValue::new(values))
            } else {
                let err_msg = format!(
                    "json parse error.type incompatible, dtype {:?}, value {:?}",
                    dtype, v
                );
                return Err(anyhow!(err_msg));
            }
        }
        DataType::Interval => unimplemented!(),
    };
    Ok(v)
}

#[cfg(not(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
)))]
#[inline]
pub(crate) fn json_parse_value(dtype: &DataType, value: Option<&Value>) -> Result<Datum> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(do_parse_json_value(dtype, v).map_err(|e| {
            anyhow!("failed to parse type '{}' from json: {}", dtype, e)
        })?)),
    }
}

#[cfg(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
))]
fn do_parse_simd_json_value(dtype: &DataType, v: &BorrowedValue<'_>) -> Result<ScalarImpl> {
    use crate::simd_json_ensure_float;

    let v = match dtype {
        DataType::Boolean => v.as_bool().ok_or_else(|| anyhow!("expect bool"))?.into(),
        DataType::Int16 => ScalarImpl::Int16(
            ensure_int!(v, i16)
                .try_into()
                .map_err(|e| anyhow!("expect i16: {}", e))?,
        ),
        DataType::Int32 => ScalarImpl::Int32(
            ensure_int!(v, i32)
                .try_into()
                .map_err(|e| anyhow!("expect i32: {}", e))?,
        ),
        DataType::Int64 => ensure_int!(v, i64).into(),
        DataType::Float32 => ScalarImpl::Float32((simd_json_ensure_float!(v, f32) as f32).into()),
        DataType::Float64 => ScalarImpl::Float64((simd_json_ensure_float!(v, f64)).into()),
        // FIXME: decimal should have more precision than f64
        DataType::Decimal => Decimal::from_f64(simd_json_ensure_float!(v, Decimal))
            .ok_or_else(|| anyhow!("expect decimal"))?
            .into(),
        DataType::Varchar => ensure_str!(v, "varchar").to_string().into(),
        DataType::Bytea => ensure_str!(v, "bytea").to_string().into(),
        DataType::Date => str_to_date(ensure_str!(v, "date"))?.into(),
        DataType::Time => str_to_time(ensure_str!(v, "time"))?.into(),
        DataType::Timestamp => match v {
            BorrowedValue::String(s) => str_to_timestamp(s)?.into(),
            BorrowedValue::Static(_) => i64_to_timestamp(ensure_int!(v, i64))?.into(),
            _ => anyhow::bail!("expect timestamp, but found {v}"),
        },
        DataType::Timestamptz => match v {
            BorrowedValue::String(s) => str_to_timestamptz(s)?.into(),
            BorrowedValue::Static(_) => i64_to_timestamptz(ensure_int!(v, i64))?.into(),
            _ => anyhow::bail!("expect timestamptz, but found {v}"),
        },
        DataType::Struct(struct_type_info) => {
            let fields = struct_type_info
                .field_names
                .iter()
                .zip_eq(struct_type_info.fields.iter())
                .map(|field| simd_json_parse_value(field.1, v.get(field.0.as_str())))
                .collect::<Result<Vec<Datum>>>()?;
            ScalarImpl::Struct(StructValue::new(fields))
        }
        DataType::List {
            datatype: item_type,
        } => {
            if let BorrowedValue::Array(values) = v {
                let values = values
                    .iter()
                    .map(|v| simd_json_parse_value(item_type, Some(v)))
                    .collect::<Result<Vec<Datum>>>()?;
                ScalarImpl::List(ListValue::new(values))
            } else {
                let err_msg = format!(
                    "json parse error.type incompatible, dtype {:?}, value {:?}",
                    dtype, v
                );
                return Err(anyhow!(err_msg));
            }
        }
        DataType::Interval => unimplemented!(),
    };
    Ok(v)
}

#[cfg(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
))]
#[inline]
pub(crate) fn simd_json_parse_value(
    // column: &ColumnDesc,
    dtype: &DataType,
    value: Option<&BorrowedValue<'_>>,
) -> Result<Datum> {
    match value {
        None | Some(BorrowedValue::Static(StaticNode::Null)) => Ok(None),
        Some(v) => Ok(Some(do_parse_simd_json_value(dtype, v).map_err(|e| {
            anyhow!("failed to parse type '{}' from json: {}", dtype, e)
        })?)),
    }
}
