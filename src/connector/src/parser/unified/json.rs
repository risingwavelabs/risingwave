use std::str::FromStr;

use risingwave_common::array::StructValue;
use risingwave_common::cast::{
    i64_to_timestamp, i64_to_timestamptz, str_to_date, str_to_time, str_to_timestamp,
    str_with_time_zone_to_timestamptz,
};
use risingwave_common::types::{DataType, Date, Datum, Decimal, Interval, ScalarImpl, Time};
use risingwave_common::util::iter_util::ZipEqFast;
use simd_json::{BorrowedValue, TryTypeError, ValueAccess, ValueType};

use super::{AccessError, AccessResult};
use crate::parser::common::json_object_smart_get_value;

enum ByteaHandling {
    Standard,
    // debezium converts postgres bytea to base64 format
    Debezium,
}

enum TimeHandling {
    Milli,
    Micro,
}
struct JsonParseOptions {
    bytea_handling: ByteaHandling,
    time_handling: TimeHandling,
}

impl JsonParseOptions {
    fn parse(&self, value: &BorrowedValue<'_>, shape: &DataType) -> AccessResult {
        let create_error = || AccessError::TypeError {
            expected: shape.to_string(),
            got: value.value_type().to_string(),
            value: value.to_string(),
        };

        let v: ScalarImpl = match (shape, value.value_type()) {
            (_, ValueType::Null) => return Ok(None),
            (DataType::Boolean, ValueType::Bool) => value.as_bool().unwrap().into(),
            (
                DataType::Boolean,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) if matches!(value.as_i64(), Some(0i64) | Some(1i64)) => {
                (value.as_i64() == Some(1i64)).into()
            }

            (
                DataType::Int16,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value.try_as_i16()?.into(),
            (
                DataType::Int32,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value.try_as_i32()?.into(),
            (
                DataType::Int64,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value.try_as_i64()?.into(),

            (
                DataType::Float32,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => (value.try_as_i64()? as f32).into(),
            (DataType::Float32, ValueType::F64) => value.try_as_f32()?.into(),

            (
                DataType::Float64,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => (value.try_as_i64()? as f64).into(),
            (DataType::Float32, ValueType::F64) => value.try_as_f64()?.into(),

            (
                DataType::Decimal,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value.try_as_i64()?.into(),

            (DataType::Decimal, ValueType::F64) => Decimal::try_from(value.try_as_f64()?)
                .map_err(|_| create_error())?
                .into(),

            (DataType::Decimal, ValueType::String) => ScalarImpl::Decimal(
                Decimal::from_str(value.as_str().unwrap()).map_err(|err| create_error())?,
            ),

            (
                DataType::Date,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => Date::with_days_since_unix_epoch(value.try_as_i32()?)
                .map_err(|_| create_error())?
                .into(),
            (DataType::Date, ValueType::String) => str_to_date(value.as_str().unwrap())
                .map_err(|_| create_error())?
                .into(),

            (DataType::Varchar, ValueType::String) => value.as_str().unwrap().into(),

            (DataType::Time, ValueType::String) => str_to_time(value.as_str().unwrap())
                .map_err(|_| create_error())?
                .into(),
            (
                DataType::Time,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value
                .as_i64()
                .map(|i| match self.time_handling {
                    TimeHandling::Milli => Time::with_milli(i as u32),
                    TimeHandling::Micro => Time::with_micro(i as u64),
                })
                .unwrap()
                .map_err(|_| create_error())?
                .into(),

            (DataType::Timestamp, ValueType::String) => str_to_timestamp(value.as_str().unwrap())
                .map_err(|_| create_error())?
                .into(),
            (
                DataType::Timestamp,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => i64_to_timestamp(value.as_i64().unwrap())
                .map_err(|_| create_error())?
                .into(),

            (DataType::Timestamptz, ValueType::String) => {
                str_with_time_zone_to_timestamptz(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into()
            }
            (
                DataType::Timestamptz,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => i64_to_timestamptz(value.as_i64().unwrap())
                .map_err(|_| create_error())?
                .into(),

            (DataType::Interval, ValueType::String) => {
                Interval::from_iso_8601(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into()
            }
            (DataType::Struct(struct_type_info), ValueType::Object) => StructValue::new(
                struct_type_info
                    .field_names
                    .iter()
                    .zip_eq_fast(struct_type_info.fields.iter())
                    .map(|(field_name, field_type)| {
                        self.parse(
                            json_object_smart_get_value(value, field_name.into())
                                .unwrap_or(&BorrowedValue::Static(simd_json::StaticNode::Null)),
                            field_type,
                        )
                    })
                    .collect::<Result<_,_>>()?,
            ).into(),
            (DataType::List(_), ValueType::Null) => todo!(),
            (DataType::List(_), ValueType::Bool) => todo!(),
            (DataType::List(_), ValueType::I64) => todo!(),
            (DataType::List(_), ValueType::I128) => todo!(),
            (DataType::List(_), ValueType::U64) => todo!(),
            (DataType::List(_), ValueType::U128) => todo!(),
            (DataType::List(_), ValueType::F64) => todo!(),
            (DataType::List(_), ValueType::String) => todo!(),
            (DataType::List(_), ValueType::Array) => todo!(),
            (DataType::List(_), ValueType::Object) => todo!(),
            (DataType::List(_), ValueType::Extended(_)) => todo!(),
            (DataType::List(_), ValueType::Custom(_)) => todo!(),
            (DataType::Bytea, ValueType::Null) => todo!(),
            (DataType::Bytea, ValueType::Bool) => todo!(),
            (DataType::Bytea, ValueType::I64) => todo!(),
            (DataType::Bytea, ValueType::I128) => todo!(),
            (DataType::Bytea, ValueType::U64) => todo!(),
            (DataType::Bytea, ValueType::U128) => todo!(),
            (DataType::Bytea, ValueType::F64) => todo!(),
            (DataType::Bytea, ValueType::String) => todo!(),
            (DataType::Bytea, ValueType::Array) => todo!(),
            (DataType::Bytea, ValueType::Object) => todo!(),
            (DataType::Bytea, ValueType::Extended(_)) => todo!(),
            (DataType::Bytea, ValueType::Custom(_)) => todo!(),
            (DataType::Jsonb, ValueType::Null) => todo!(),
            (DataType::Jsonb, ValueType::Bool) => todo!(),
            (DataType::Jsonb, ValueType::I64) => todo!(),
            (DataType::Jsonb, ValueType::I128) => todo!(),
            (DataType::Jsonb, ValueType::U64) => todo!(),
            (DataType::Jsonb, ValueType::U128) => todo!(),
            (DataType::Jsonb, ValueType::F64) => todo!(),
            (DataType::Jsonb, ValueType::String) => todo!(),
            (DataType::Jsonb, ValueType::Array) => todo!(),
            (DataType::Jsonb, ValueType::Object) => todo!(),
            (DataType::Jsonb, ValueType::Extended(_)) => todo!(),
            (DataType::Jsonb, ValueType::Custom(_)) => todo!(),
            (DataType::Serial, ValueType::Null) => todo!(),
            (DataType::Serial, ValueType::Bool) => todo!(),
            (DataType::Serial, ValueType::I64) => todo!(),
            (DataType::Serial, ValueType::I128) => todo!(),
            (DataType::Serial, ValueType::U64) => todo!(),
            (DataType::Serial, ValueType::U128) => todo!(),
            (DataType::Serial, ValueType::F64) => todo!(),
            (DataType::Serial, ValueType::String) => todo!(),
            (DataType::Serial, ValueType::Array) => todo!(),
            (DataType::Serial, ValueType::Object) => todo!(),
            (DataType::Serial, ValueType::Extended(_)) => todo!(),
            (DataType::Serial, ValueType::Custom(_)) => todo!(),
            (DataType::Int256, ValueType::Null) => todo!(),
            (DataType::Int256, ValueType::Bool) => todo!(),
            (DataType::Int256, ValueType::I64) => todo!(),
            (DataType::Int256, ValueType::I128) => todo!(),
            (DataType::Int256, ValueType::U64) => todo!(),
            (DataType::Int256, ValueType::U128) => todo!(),
            (DataType::Int256, ValueType::F64) => todo!(),
            (DataType::Int256, ValueType::String) => todo!(),
            (DataType::Int256, ValueType::Array) => todo!(),
            (DataType::Int256, ValueType::Object) => todo!(),
            (DataType::Int256, ValueType::Extended(_)) => todo!(),
            (DataType::Int256, ValueType::Custom(_)) => todo!(),
            (expected, got) => Err(create_error())?,
        };
        Ok(Some(v))
    }
}

impl From<TryTypeError> for AccessError {
    fn from(value: TryTypeError) -> Self {
        AccessError::TypeError {
            expected: value.expected.to_string(),
            got: value.expected.to_string(),
            value: Default::default(),
        }
    }
}
