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
use risingwave_common::types::{DataType, Datum, Decimal, ScalarImpl, ScalarRef};
use risingwave_expr::vector_op::cast::{str_to_date, str_to_time, str_to_timestamp};
use risingwave_expr::vector_op::to_timestamp::to_timestamp;
use serde_json::Value;
#[cfg(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
))]
use simd_json::{value::StaticNode, BorrowedValue, ValueAccess};

macro_rules! ensure_float {
    ($v:ident, $t:ty) => {
        $v.as_f64()
            .ok_or_else(|| anyhow!(concat!("expect ", stringify!($t), ", but found {}"), $v))?
    };
}

macro_rules! simd_json_ensure_float {
    ($v:ident, $t:ty) => {
        $v.cast_f64()
            .ok_or_else(|| anyhow!(concat!("expect ", stringify!($t), ", but found {}"), $v))?
    };
}

macro_rules! ensure_int {
    ($v:ident, $t:ty) => {
        $v.as_i64()
            .ok_or_else(|| anyhow!(concat!("expect ", stringify!($t), ", but found {}"), $v))?
    };
}

macro_rules! ensure_str {
    ($v:ident, $t:literal) => {
        $v.as_str()
            .ok_or_else(|| anyhow!(concat!("expect ", $t, ", but found {}"), $v))?
    };
}

fn do_parse_json_value(dtype: &DataType, v: &Value) -> Result<ScalarImpl> {
    dbg!(&v);
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
        DataType::Float64 => ScalarImpl::Float64((ensure_float!(v, f64) as f64).into()),
        // FIXME: decimal should have more precision than f64
        DataType::Decimal => Decimal::from_f64(ensure_float!(v, Decimal))
            .ok_or_else(|| anyhow!("expect decimal"))?
            .into(),
        DataType::Varchar => ensure_str!(v, "varchar").to_string().into(),
        DataType::Date => str_to_date(ensure_str!(v, "date"))?.into(),
        DataType::Time => str_to_time(ensure_str!(v, "time"))?.into(),
        DataType::Timestamp => str_to_timestamp(ensure_str!(v, "timestamp"))?.into(),
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
        DataType::Timestampz => unimplemented!(),
        DataType::Interval => unimplemented!(),
    };
    Ok(v)
}

#[inline]
pub(crate) fn json_parse_value(dtype: &DataType, value: Option<&Value>) -> Result<Datum> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(v) => {
            if v.is_i64() && *dtype == DataType::Timestamp {
                Ok(Some(ScalarImpl::NaiveDateTime(
                    to_timestamp(v.as_i64().unwrap()).map_err(|e| {
                        anyhow!("failed to parse type '{}' from json: {}", dtype, e)
                    })?,
                )))
            } else {
                Ok(Some(do_parse_json_value(dtype, v).map_err(|e| {
                    anyhow!("failed to parse type '{}' from json: {}", dtype, e)
                })?))
            }
        }
    }
}

#[cfg(any(
    target_feature = "sse4.2",
    target_feature = "avx2",
    target_feature = "neon",
    target_feature = "simd128"
))]
fn do_parse_simd_json_value(dtype: &DataType, v: &BorrowedValue<'_>) -> Result<ScalarImpl> {
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
        DataType::Float64 => ScalarImpl::Float64((simd_json_ensure_float!(v, f64) as f64).into()),
        // FIXME: decimal should have more precision than f64
        DataType::Decimal => Decimal::from_f64(simd_json_ensure_float!(v, Decimal))
            .ok_or_else(|| anyhow!("expect decimal"))?
            .into(),
        DataType::Varchar => ensure_str!(v, "varchar").to_string().into(),
        DataType::Date => str_to_date(ensure_str!(v, "date"))?.into(),
        DataType::Time => str_to_time(ensure_str!(v, "time"))?.into(),
        DataType::Timestamp => str_to_timestamp(ensure_str!(v, "timestamp"))?.into(),
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
        DataType::Timestampz => unimplemented!(),
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
