use std::str::FromStr;

use base64::Engine;
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::cast::{
    i64_to_timestamp, i64_to_timestamptz, str_to_bytea, str_to_date, str_to_time, str_to_timestamp,
    str_with_time_zone_to_timestamptz,
};
use risingwave_common::types::{
    DataType, Date, Datum, Decimal, Int256, Interval, JsonbVal, ScalarImpl, Time,
};
use risingwave_common::util::iter_util::ZipEqFast;
use simd_json::{BorrowedValue, TryTypeError, ValueAccess, ValueType};

use super::{Access, AccessError, AccessResult};
use crate::parser::common::json_object_smart_get_value;

pub enum ByteaHandling {
    Standard,
    // debezium converts postgres bytea to base64 format
    Base64,
}

pub enum TimeHandling {
    Milli,
    Micro,
}

pub enum JsonValueHandling {
    AsValue,
    AsString,
}

pub struct JsonParseOptions {
    pub bytea_handling: ByteaHandling,
    pub time_handling: TimeHandling,
    pub json_value_handling: JsonValueHandling,
}

impl Default for JsonParseOptions {
    fn default() -> Self {
        Self {
            bytea_handling: ByteaHandling::Standard,
            time_handling: TimeHandling::Micro,
            json_value_handling: JsonValueHandling::AsValue,
        }
    }
}

impl JsonParseOptions {
    pub const DEBEZIUM: JsonParseOptions = JsonParseOptions {
        bytea_handling: ByteaHandling::Base64,
        time_handling: TimeHandling::Micro,
        json_value_handling: JsonValueHandling::AsString,
    };

    pub fn parse(&self, value: &BorrowedValue<'_>, shape: &DataType) -> AccessResult {
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
                    .collect::<Result<_, _>>()?,
            )
            .into(),
            (DataType::List(item_type), ValueType::Array) => ListValue::new(
                value
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| self.parse(v, item_type))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .into(),
            (DataType::Bytea, ValueType::String) => match self.bytea_handling {
                ByteaHandling::Standard => str_to_bytea(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into(),
                ByteaHandling::Base64 => base64::engine::general_purpose::STANDARD
                    .decode(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into_boxed_slice()
                    .into(),
            },
            (DataType::Jsonb, ValueType::String)
                if matches!(self.json_value_handling, JsonValueHandling::AsString) =>
            {
                JsonbVal::from_str(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into()
            }
            (DataType::Jsonb, _)
                if matches!(self.json_value_handling, JsonValueHandling::AsValue) =>
            {
                JsonbVal::from_serde(value.clone().try_into().map_err(|_| create_error())?).into()
            }

            (
                DataType::Int64,
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => Int256::from(value.try_as_i64()?).into(),

            (DataType::Int256, ValueType::String) => Int256::from_str(value.as_str().unwrap())
                .map_err(|_| create_error())?
                .into(),

            (expected, got) => Err(create_error())?,
        };
        Ok(Some(v))
    }
}

// pub struct JsonAccess<'a> {

// }

impl Access for JsonParseOptions {
    fn access(&self, path: &[&str], shape: DataType) -> AccessResult {
        todo!()
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
