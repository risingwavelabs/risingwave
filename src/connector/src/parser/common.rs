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

use std::borrow::Cow;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use base64::Engine as _;
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::cast::{
    i64_to_timestamp, i64_to_timestamptz, str_to_bytea, str_to_date, str_to_time, str_to_timestamp,
    str_with_time_zone_to_timestamptz,
};
use risingwave_common::types::{
    DataType, Date, Datum, Decimal, Int256, Interval, JsonbVal, ScalarImpl, Time,
};
use simd_json::value::StaticNode;
use simd_json::{BorrowedValue, ValueAccess};

use crate::source::SourceFormat;
use crate::{
    ensure_i16, ensure_i32, ensure_i64, ensure_rust_type, ensure_str, simd_json_ensure_float,
};
pub(crate) fn json_object_smart_get_value<'a, 'b>(
    v: &'b simd_json::BorrowedValue<'a>,
    key: Cow<'b, str>,
) -> Option<&'b BorrowedValue<'a>> {
    let obj = v.as_object()?;
    if obj.contains_key(key.as_ref()) {
        return obj.get(key.as_ref());
    }
    for (k, v) in obj {
        if k.eq_ignore_ascii_case(key.as_ref()) {
            return Some(v);
        }
    }
    None
}

pub(crate) fn do_parse_simd_json_value(
    format: &SourceFormat,
    dtype: &DataType,
    v: &BorrowedValue<'_>,
) -> Result<ScalarImpl> {
    let v = match (dtype, format) {
        (DataType::Boolean, SourceFormat::CanalJson) => (ensure_rust_type!(v, i16) != 0).into(),
        (DataType::Boolean, _) => match v {
            BorrowedValue::Static(StaticNode::Bool(b)) => (*b).into(),
            // debezium converts bool to int, false -> 0, true -> 1, for mysql and postgres
            BorrowedValue::Static(v) => match v.as_i64() {
                Some(0i64) => ScalarImpl::Bool(false),
                Some(1i64) => ScalarImpl::Bool(true),
                _ => anyhow::bail!("expect bool, but found {v}"),
            },
            _ => anyhow::bail!("expect bool, but found {v}"),
        },
        (DataType::Int16, SourceFormat::CanalJson) => ensure_rust_type!(v, i16).into(),
        (DataType::Int16, _) => ensure_i16!(v, i16).into(),
        (DataType::Int32, SourceFormat::CanalJson) => ensure_rust_type!(v, i32).into(),
        (DataType::Int32, _) => ensure_i32!(v, i32).into(),
        (DataType::Int64, SourceFormat::CanalJson) => ensure_rust_type!(v, i64).into(),
        (DataType::Int64, _) => ensure_i64!(v, i64).into(),
        (DataType::Int256, _) => Int256::from_str(ensure_str!(v, "quoted int256"))?.into(),
        (DataType::Serial, _) => anyhow::bail!("serial should not be parsed"),
        // if the value is too large, str parsing to f32 will fail
        (DataType::Float32, SourceFormat::CanalJson) => ensure_rust_type!(v, f32).into(),
        // when f32 overflows, the value is converted to `inf` which is inappropriate
        (DataType::Float32, _) => {
            let scalar_val = ScalarImpl::Float32((simd_json_ensure_float!(v, f32) as f32).into());
            if let ScalarImpl::Float32(f) = scalar_val {
                if f.0.is_infinite() {
                    anyhow::bail!("{v} is out of range for type f32");
                }
            }
            scalar_val
        }
        (DataType::Float64, SourceFormat::CanalJson) => ensure_rust_type!(v, f64).into(),
        (DataType::Float64, _) => simd_json_ensure_float!(v, f64).into(),
        (DataType::Decimal, SourceFormat::CanalJson) => Decimal::from_str(ensure_str!(v, "string"))
            .map_err(|_| anyhow!("parse decimal from string err {}", v))?
            .into(),
        // FIXME: decimal should have more precision than f64
        (DataType::Decimal, _) => Decimal::try_from(simd_json_ensure_float!(v, Decimal))
            .map_err(|_| anyhow!("expect decimal"))?
            .into(),
        (DataType::Varchar, _) => ensure_str!(v, "varchar").to_string().into(),
        (DataType::Bytea, _) => match format {
            // debezium converts postgres bytea to base64 format
            SourceFormat::DebeziumJson => ScalarImpl::Bytea(
                base64::engine::general_purpose::STANDARD
                    .decode(ensure_str!(v, "bytea"))
                    .map_err(|e| anyhow!(e))?
                    .into(),
            ),
            _ => ScalarImpl::Bytea(str_to_bytea(ensure_str!(v, "bytea")).map_err(|e| anyhow!(e))?),
        },
        (DataType::Date, _) => match v {
            BorrowedValue::String(s) => str_to_date(s).map_err(|e| anyhow!(e))?.into(),
            BorrowedValue::Static(_) => {
                // debezium converts date to i32 for mysql and postgres
                Date::with_days_since_unix_epoch(ensure_i32!(v, i32))?.into()
            }
            _ => anyhow::bail!("expect date, but found {v}"),
        },
        (DataType::Time, _) => {
            match v {
                BorrowedValue::String(s) => str_to_time(s).map_err(|e| anyhow!(e))?.into(),
                BorrowedValue::Static(_) => {
                    match format {
                        SourceFormat::DebeziumJson => {
                            // debezium converts time to i64 for mysql and postgres in microseconds
                            Time::with_micro(ensure_i64!(v, i64).try_into().map_err(|_| {
                                anyhow!("cannot cast i64 to time, value out of range")
                            })?)?
                            .into()
                        }
                        _ => Time::with_milli(ensure_i64!(v, i64).try_into().map_err(|_| {
                            anyhow!("cannot cast i64 to time, value out of range")
                        })?)?
                        .into(),
                    }
                }
                _ => anyhow::bail!("expect time, but found {v}"),
            }
        }
        (DataType::Timestamp, _) => match v {
            BorrowedValue::String(s) => str_to_timestamp(s).map_err(|e| anyhow!(e))?.into(),
            BorrowedValue::Static(_) => i64_to_timestamp(ensure_i64!(v, i64))
                .map_err(|e| anyhow!(e))?
                .into(),
            _ => anyhow::bail!("expect timestamp, but found {v}"),
        },
        (DataType::Timestamptz, _) => match v {
            BorrowedValue::String(s) => str_with_time_zone_to_timestamptz(s)
                .map_err(|e| anyhow!(e))?
                .into(),
            BorrowedValue::Static(_) => i64_to_timestamptz(ensure_i64!(v, i64))
                .map_err(|e| anyhow!(e))?
                .into(),
            _ => anyhow::bail!("expect timestamptz, but found {v}"),
        },
        (DataType::Jsonb, _) => {
            // jsonb will be output as a string in debezium format
            if *format == SourceFormat::DebeziumJson {
                ScalarImpl::Jsonb(JsonbVal::from_str(ensure_str!(v, "jsonb"))?)
            } else {
                let v: serde_json::Value = v.clone().try_into()?;
                #[expect(clippy::disallowed_methods)]
                ScalarImpl::Jsonb(JsonbVal::from_serde(v))
            }
        }
        (DataType::Struct(struct_type_info), _) => {
            let fields: Vec<Option<ScalarImpl>> = struct_type_info
                .name_types()
                .map(|(name, ty)| {
                    simd_json_parse_value(format, ty, json_object_smart_get_value(v, name.into()))
                })
                .collect::<Result<Vec<Datum>>>()?;
            ScalarImpl::Struct(StructValue::new(fields))
        }
        (DataType::List(item_type), _) => {
            if let BorrowedValue::Array(values) = v {
                let values = values
                    .iter()
                    .map(|v| simd_json_parse_value(format, item_type, Some(v)))
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
        (DataType::Interval, _) => match format {
            SourceFormat::DebeziumJson => {
                ScalarImpl::Interval(Interval::from_iso_8601(ensure_str!(v, "interval"))?)
            }
            _ => unimplemented!(),
        },
    };
    Ok(v)
}

#[inline]
pub(crate) fn simd_json_parse_value(
    // column: &ColumnDesc,
    format: &SourceFormat,
    dtype: &DataType,
    value: Option<&BorrowedValue<'_>>,
) -> Result<Datum> {
    match value {
        None | Some(BorrowedValue::Static(StaticNode::Null)) => Ok(None),
        Some(v) => Ok(Some(do_parse_simd_json_value(format, dtype, v).map_err(
            |e| anyhow!("failed to parse type '{}' from json: {}", dtype, e),
        )?)),
    }
}
