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

use anyhow::{anyhow, Result};
use num_traits::FromPrimitive;
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::types::{DataType, Datum, Decimal, ScalarImpl};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::vector_op::cast::{
    i32_to_date, i64_to_time, i64_to_timestamp, i64_to_timestamptz, str_to_date, str_to_time,
    str_to_timestamp, str_with_time_zone_to_timestamptz,
};
use simd_json::value::StaticNode;
use simd_json::{BorrowedValue, ValueAccess};

use crate::{ensure_i32, ensure_int, ensure_str, simd_json_ensure_float};

fn do_parse_simd_json_value(dtype: &DataType, v: &BorrowedValue<'_>) -> Result<ScalarImpl> {
    let v = match dtype {
        DataType::Boolean => match v {
            BorrowedValue::Static(StaticNode::Bool(_)) => {
                v.as_bool().ok_or_else(|| anyhow!("expect bool"))?.into()
            }
            BorrowedValue::Static(StaticNode::I64(i)) => match i {
                0i64 => ScalarImpl::Bool(false),
                1i64 => ScalarImpl::Bool(true),
                _ => anyhow::bail!("expect bool, but found {v}"),
            },
            _ => anyhow::bail!("expect bool, but found {v}"),
        },
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
        DataType::Float32 => {
            let scalar_val = ScalarImpl::Float32((simd_json_ensure_float!(v, f32) as f32).into());
            if let ScalarImpl::Float32(f) = scalar_val {
                if f.is_infinite() {
                    anyhow::bail!("{v} is out of range for type f32");
                }
            }
            scalar_val
        }
        DataType::Float64 => {
            let scalar_val = ScalarImpl::Float64((simd_json_ensure_float!(v, f64)).into());
            if let ScalarImpl::Float64(f) = scalar_val {
                if f.is_infinite() {
                    anyhow::bail!("{v} is out of range for type f64");
                }
            }
            scalar_val
        }
        // FIXME: decimal should have more precision than f64
        DataType::Decimal => Decimal::from_f64(simd_json_ensure_float!(v, Decimal))
            .ok_or_else(|| anyhow!("expect decimal"))?
            .into(),
        DataType::Varchar => ensure_str!(v, "varchar").to_string().into(),
        DataType::Bytea => ensure_str!(v, "bytea").to_string().into(),
        DataType::Date => match v {
            BorrowedValue::String(_) => str_to_date(ensure_str!(v, "date"))?.into(),
            BorrowedValue::Static(_) => i32_to_date(ensure_i32!(v, i32))?.into(),
            _ => anyhow::bail!("expect date, but found {v}"),
        },
        DataType::Time => match v {
            BorrowedValue::String(_) => str_to_time(ensure_str!(v, "time"))?.into(),
            BorrowedValue::Static(_) => i64_to_time(ensure_int!(v, i64))?.into(),
            _ => anyhow::bail!("expect time, but found {v}"),
        },
        DataType::Timestamp => match v {
            BorrowedValue::String(s) => str_to_timestamp(s)?.into(),
            BorrowedValue::Static(_) => i64_to_timestamp(ensure_int!(v, i64))?.into(),
            _ => anyhow::bail!("expect timestamp, but found {v}"),
        },
        DataType::Timestamptz => match v {
            BorrowedValue::String(s) => str_with_time_zone_to_timestamptz(s)?.into(),
            BorrowedValue::Static(_) => i64_to_timestamptz(ensure_int!(v, i64))?.into(),
            _ => anyhow::bail!("expect timestamptz, but found {v}"),
        },
        DataType::Jsonb => {
            let v: serde_json::Value = v.clone().try_into()?;
            #[expect(clippy::disallowed_methods)]
            ScalarImpl::Jsonb(risingwave_common::array::JsonbVal::from_serde(v))
        }
        DataType::Struct(struct_type_info) => {
            let fields = struct_type_info
                .field_names
                .iter()
                .zip_eq_fast(struct_type_info.fields.iter())
                .map(|field| {
                    simd_json_parse_value(field.1, v.get(field.0.to_ascii_lowercase().as_str()))
                })
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
