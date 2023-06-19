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

use apache_avro::types::Value;
use apache_avro::{Decimal as AvroDecimal, Schema};
use chrono::Datelike;
use itertools::Itertools;
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::{DataType, Date, Datum};
use risingwave_pb::plan_common::ColumnDesc;

use crate::parser::unified::avro::AvroParseOptions;

const RW_DECIMAL_MAX_PRECISION: usize = 28;
const DBZ_VARIABLE_SCALE_DECIMAL_NAME: &str = "VariableScaleDecimal";
const DBZ_VARIABLE_SCALE_DECIMAL_NAMESPACE: &str = "io.debezium.data";

pub(crate) fn avro_field_to_column_desc(
    name: &str,
    schema: &Schema,
    index: &mut i32,
) -> Result<ColumnDesc> {
    let data_type = avro_type_mapping(schema)?;
    match schema {
        Schema::Record {
            name: schema_name,
            fields,
            ..
        } => {
            let vec_column = fields
                .iter()
                .map(|f| avro_field_to_column_desc(&f.name, &f.schema, index))
                .collect::<Result<Vec<_>>>()?;
            *index += 1;
            Ok(ColumnDesc {
                column_type: Some(data_type.to_protobuf()),
                column_id: *index,
                name: name.to_owned(),
                field_descs: vec_column,
                type_name: schema_name.to_string(),
                generated_or_default_column: None,
            })
        }
        _ => {
            *index += 1;
            Ok(ColumnDesc {
                column_type: Some(data_type.to_protobuf()),
                column_id: *index,
                name: name.to_owned(),
                ..Default::default()
            })
        }
    }
}

fn avro_type_mapping(schema: &Schema) -> Result<DataType> {
    let data_type = match schema {
        Schema::String => DataType::Varchar,
        Schema::Int => DataType::Int32,
        Schema::Long => DataType::Int64,
        Schema::Boolean => DataType::Boolean,
        Schema::Float => DataType::Float32,
        Schema::Double => DataType::Float64,
        Schema::Decimal { precision, .. } => {
            if precision > &RW_DECIMAL_MAX_PRECISION {
                tracing::warn!(
                    "RisingWave supports decimal precision up to {}, but got {}. Will truncate.",
                    RW_DECIMAL_MAX_PRECISION,
                    precision
                );
            }
            DataType::Decimal
        }
        Schema::Date => DataType::Date,
        Schema::TimestampMillis => DataType::Timestamptz,
        Schema::TimestampMicros => DataType::Timestamptz,
        Schema::Duration => DataType::Interval,
        Schema::Bytes => DataType::Bytea,
        Schema::Enum { .. } => DataType::Varchar,
        Schema::Record { fields, name, .. } => {
            if name.name == DBZ_VARIABLE_SCALE_DECIMAL_NAME
                && name.namespace == Some(DBZ_VARIABLE_SCALE_DECIMAL_NAMESPACE.into())
            {
                return Ok(DataType::Decimal);
            }

            let struct_fields = fields
                .iter()
                .map(|f| avro_type_mapping(&f.schema))
                .collect::<Result<Vec<_>>>()?;
            let struct_names = fields.iter().map(|f| f.name.clone()).collect_vec();
            DataType::new_struct(struct_fields, struct_names)
        }
        Schema::Array(item_schema) => {
            let item_type = avro_type_mapping(item_schema.as_ref())?;
            DataType::List(Box::new(item_type))
        }
        Schema::Union(union_schema) => {
            let nested_schema = union_schema
                .variants()
                .iter()
                .find_or_first(|s| **s != Schema::Null)
                .ok_or_else(|| {
                    RwError::from(InternalError(format!(
                        "unsupported type in Avro: {:?}",
                        union_schema
                    )))
                })?;

            avro_type_mapping(nested_schema)?
        }
        _ => {
            return Err(RwError::from(InternalError(format!(
                "unsupported type in Avro: {:?}",
                schema
            ))));
        }
    };

    Ok(data_type)
}

pub(crate) fn get_field_from_avro_value<'a>(
    avro_value: &'a Value,
    field_name: &str,
) -> Result<&'a Value> {
    match avro_value {
        Value::Record(fields) => fields
            .iter()
            .find(|val| val.0.eq(field_name))
            .map(|entry| &entry.1)
            .ok_or_else(|| {
                RwError::from(ProtocolError(format!(
                    "field {} not found in debezium event",
                    field_name
                )))
            }),
        Value::Union(_, boxed_value) => get_field_from_avro_value(boxed_value.as_ref(), field_name),
        _ => Err(RwError::from(ProtocolError(format!(
            "avro parse unexpected field {}",
            field_name
        )))),
    }
}

pub(crate) fn avro_decimal_to_rust_decimal(
    avro_decimal: AvroDecimal,
    _precision: usize,
    scale: usize,
) -> Result<rust_decimal::Decimal> {
    let negative = !avro_decimal.is_positive();
    let bytes = avro_decimal.to_vec_unsigned();

    let (lo, mid, hi) = extract_decimal(bytes);
    Ok(rust_decimal::Decimal::from_parts(
        lo,
        mid,
        hi,
        negative,
        scale as u32,
    ))
}

pub(crate) fn extract_decimal(bytes: Vec<u8>) -> (u32, u32, u32) {
    match bytes.len() {
        len @ 0..=4 => {
            let mut pad = vec![0; 4 - len];
            pad.extend_from_slice(&bytes);
            let lo = u32::from_be_bytes(pad.try_into().unwrap());
            (lo, 0, 0)
        }
        len @ 5..=8 => {
            let mid = u32::from_be_bytes(bytes[..4].to_owned().try_into().unwrap());
            let mut pad = vec![0; 8 - len];
            pad.extend_from_slice(&bytes[4..]);
            let lo = u32::from_be_bytes(pad.try_into().unwrap());
            (lo, mid, 0)
        }
        len @ 9..=12 => {
            let hi = u32::from_be_bytes(bytes[..4].to_owned().try_into().unwrap());
            let mid = u32::from_be_bytes(bytes[4..8].to_owned().try_into().unwrap());
            let mut pad = vec![0; 12 - len];
            pad.extend_from_slice(&bytes[8..]);
            let lo = u32::from_be_bytes(pad.try_into().unwrap());
            (lo, mid, hi)
        }
        _ => unreachable!(),
    }
}

pub(crate) fn unix_epoch_days() -> i32 {
    Date::from_ymd_uncheck(1970, 1, 1).0.num_days_from_ce()
}

// extract inner filed/item schema of record/array/union
pub(crate) fn extract_inner_field_schema<'a>(
    schema: &'a Schema,
    name: Option<&'a str>,
) -> Result<&'a Schema> {
    match schema {
        Schema::Record { fields, lookup, .. } => {
            let name = name.ok_or_else(|| {
                RwError::from(InternalError(
                    "no name provided for a field in record".to_owned(),
                ))
            })?;
            let index = lookup.get(name).ok_or_else(|| {
                RwError::from(InternalError(format!(
                    "No filed named {} in record {:?}",
                    name, schema
                )))
            })?;
            let field = fields.get(*index).ok_or_else(|| {
                RwError::from(InternalError(format!(
                    "illegal avro record schema {:?}",
                    schema
                )))
            })?;
            Ok(&field.schema)
        }
        Schema::Array(schema) => Ok(schema),
        Schema::Union(union_schema) => {
            let inner_schema = union_schema
                .variants()
                .iter()
                .find(|s| **s != Schema::Null)
                .ok_or_else(|| {
                    RwError::from(InternalError(format!(
                        "illegal avro record schema {:?}",
                        union_schema
                    )))
                })?;
            Ok(inner_schema)
        }
        _ => Err(RwError::from(InternalError(
            "avro schema is not a record or array".to_owned(),
        ))),
    }
}

/// Convert Avro value to datum.For now, support the following [Avro type](https://avro.apache.org/docs/current/spec.html).
///  - boolean
///  - int : i32
///  - long: i64
///  - float: f32
///  - double: f64
///  - string: String
///  - Date (the number of days from the unix epoch, 1970-1-1 UTC)
///  - Timestamp (the number of milliseconds from the unix epoch,  1970-1-1 00:00:00.000 UTC)
#[inline]
pub(crate) fn from_avro_value(
    value: Value,
    value_schema: &Schema,
    shape: &DataType,
) -> Result<Datum> {
    AvroParseOptions {
        schema: Some(value_schema),
        relax_numeric: true,
    }
    .parse(&value, Some(shape))
    .map_err(|err| RwError::from(InternalError(format!("{:?}", err))))
}

#[cfg(test)]
mod tests {
    use risingwave_common::types::{ScalarImpl, Timestamp};

    use super::*;
    #[test]
    fn test_convert_decimal() {
        // 280
        let v = vec![1, 24];
        let avro_decimal = AvroDecimal::from(v);
        let rust_decimal = avro_decimal_to_rust_decimal(avro_decimal, 28, 0).unwrap();
        assert_eq!(rust_decimal, rust_decimal::Decimal::from(280));

        // 28.1
        let v = vec![1, 25];
        let avro_decimal = AvroDecimal::from(v);
        let rust_decimal = avro_decimal_to_rust_decimal(avro_decimal, 28, 1).unwrap();
        assert_eq!(rust_decimal, rust_decimal::Decimal::try_from(28.1).unwrap());
    }
    #[test]
    fn test_avro_timestamp_micros() {
        let v1 = Value::TimestampMicros(1620000000000);
        let v2 = Value::TimestampMillis(1620000000);
        let value_schema1 = Schema::TimestampMicros;
        let value_schema2 = Schema::TimestampMillis;
        let datum1 = from_avro_value(v1, &value_schema1, &DataType::Timestamp).unwrap();
        let datum2 = from_avro_value(v2, &value_schema2, &DataType::Timestamp).unwrap();
        assert_eq!(
            datum1,
            Some(ScalarImpl::Timestamp(Timestamp::new(
                "2021-05-03T00:00:00".parse().unwrap()
            )))
        );
        assert_eq!(
            datum2,
            Some(ScalarImpl::Timestamp(Timestamp::new(
                "2021-05-03T00:00:00".parse().unwrap()
            )))
        );
    }
}
