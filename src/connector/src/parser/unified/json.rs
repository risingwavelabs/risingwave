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

use std::str::FromStr;

use base64::Engine;
use itertools::Itertools;
use risingwave_common::array::{ListValue, StructValue};
use risingwave_common::cast::{
    i64_to_timestamp, i64_to_timestamptz, str_to_bytea, str_to_date, str_to_time, str_to_timestamp,
    str_with_time_zone_to_timestamptz,
};
use risingwave_common::types::{
    DataType, Date, Decimal, Int256, Interval, JsonbVal, ScalarImpl, Time,
};
use risingwave_common::util::iter_util::ZipEqFast;
use simd_json::{BorrowedValue, TryTypeError, ValueAccess, ValueType};

use super::{Access, AccessError, AccessResult};
use crate::parser::common::json_object_smart_get_value;
#[derive(Clone, Debug)]
pub enum ByteaHandling {
    Standard,
    // debezium converts postgres bytea to base64 format
    Base64,
}
#[derive(Clone, Debug)]
pub enum TimeHandling {
    Milli,
    Micro,
}
#[derive(Clone, Debug)]
pub enum JsonValueHandling {
    AsValue,
    AsString,
}
#[derive(Clone, Debug)]
pub enum NumericHandling {
    Strict,
    // should integer be parsed to float
    Relax {
        // should "3.14" be parsed to 3.14 in float
        string_parsing: bool,
    },
}
#[derive(Clone, Debug)]
pub enum BooleanHandling {
    Strict,
    // should integer 1,0 be parsed to boolean (debezium)
    Relax {
        // should "True" "False" be parsed to true or false in boolean
        string_parsing: bool,
        // should string "1" "0" be paesed to boolean (cannal + mysql)
        string_integer_parsing: bool,
    },
}

#[derive(Clone, Debug)]
pub struct JsonParseOptions {
    pub bytea_handling: ByteaHandling,
    pub time_handling: TimeHandling,
    pub json_value_handling: JsonValueHandling,
    pub numeric_handling: NumericHandling,
    pub boolean_handing: BooleanHandling,
    pub ignoring_keycase: bool,
}

impl Default for JsonParseOptions {
    fn default() -> Self {
        Self::DEFAULT.clone()
    }
}

impl JsonParseOptions {
    pub const CANAL: JsonParseOptions = JsonParseOptions {
        bytea_handling: ByteaHandling::Standard,
        time_handling: TimeHandling::Micro,
        json_value_handling: JsonValueHandling::AsValue,
        numeric_handling: NumericHandling::Relax {
            string_parsing: true,
        },
        boolean_handing: BooleanHandling::Relax {
            string_parsing: true,
            string_integer_parsing: true,
        },
        ignoring_keycase: true,
    };
    pub const DEBEZIUM: JsonParseOptions = JsonParseOptions {
        bytea_handling: ByteaHandling::Base64,
        time_handling: TimeHandling::Micro,
        json_value_handling: JsonValueHandling::AsString,
        numeric_handling: NumericHandling::Relax {
            string_parsing: false,
        },
        boolean_handing: BooleanHandling::Relax {
            string_parsing: false,
            string_integer_parsing: false,
        },
        ignoring_keycase: true,
    };
    pub const DEFAULT: JsonParseOptions = JsonParseOptions {
        bytea_handling: ByteaHandling::Standard,
        time_handling: TimeHandling::Micro,
        json_value_handling: JsonValueHandling::AsValue,
        numeric_handling: NumericHandling::Relax {
            string_parsing: false,
        },
        boolean_handing: BooleanHandling::Strict,
        ignoring_keycase: true,
    };

    pub fn parse(
        &self,
        value: &BorrowedValue<'_>,
        type_expected: Option<&DataType>,
    ) -> AccessResult {
        let create_error = || AccessError::TypeError {
            expected: format!("{:?}", type_expected),
            got: value.value_type().to_string(),
            value: value.to_string(),
        };
        let v: ScalarImpl = match (type_expected, value.value_type()) {
            (_, ValueType::Null) => return Ok(None),
            // ---- Boolean -----
            (Some(DataType::Boolean) | None, ValueType::Bool) => value.as_bool().unwrap().into(),

            (
                Some(DataType::Boolean),
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) if matches!(self.boolean_handing, BooleanHandling::Relax { .. })
                && matches!(value.as_i64(), Some(0i64) | Some(1i64)) =>
            {
                (value.as_i64() == Some(1i64)).into()
            }

            (Some(DataType::Boolean), ValueType::String)
                if matches!(
                    self.boolean_handing,
                    BooleanHandling::Relax {
                        string_parsing: true,
                        ..
                    }
                ) =>
            {
                match value.as_str().unwrap().to_lowercase().as_str() {
                    "true" => true.into(),
                    "false" => false.into(),
                    c @ ("1" | "0")
                        if matches!(
                            self.boolean_handing,
                            BooleanHandling::Relax {
                                string_parsing: true,
                                string_integer_parsing: true
                            }
                        ) =>
                    {
                        if c == "1" {
                            true.into()
                        } else {
                            false.into()
                        }
                    }
                    _ => Err(create_error())?,
                }
            }
            // ---- Int16 -----
            (
                Some(DataType::Int16),
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value.try_as_i16()?.into(),

            (Some(DataType::Int16), ValueType::String)
                if matches!(
                    self.numeric_handling,
                    NumericHandling::Relax {
                        string_parsing: true
                    }
                ) =>
            {
                value
                    .as_str()
                    .unwrap()
                    .parse::<i16>()
                    .map_err(|_| create_error())?
                    .into()
            }
            // ---- Int32 -----
            (
                Some(DataType::Int32),
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value.try_as_i32()?.into(),

            (Some(DataType::Int32), ValueType::String)
                if matches!(
                    self.numeric_handling,
                    NumericHandling::Relax {
                        string_parsing: true
                    }
                ) =>
            {
                value
                    .as_str()
                    .unwrap()
                    .parse::<i32>()
                    .map_err(|_| create_error())?
                    .into()
            }
            // ---- Int64 -----
            (None, ValueType::I64 | ValueType::U64) => value.try_as_i64()?.into(),
            (
                Some(DataType::Int64),
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => value.try_as_i64()?.into(),

            (Some(DataType::Int64), ValueType::String)
                if matches!(
                    self.numeric_handling,
                    NumericHandling::Relax {
                        string_parsing: true
                    }
                ) =>
            {
                value
                    .as_str()
                    .unwrap()
                    .parse::<i64>()
                    .map_err(|_| create_error())?
                    .into()
            }
            // ---- Float32 -----
            (
                Some(DataType::Float32),
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) if matches!(self.numeric_handling, NumericHandling::Relax { .. }) => {
                (value.try_as_i64()? as f32).into()
            }
            (Some(DataType::Float32), ValueType::String)
                if matches!(
                    self.numeric_handling,
                    NumericHandling::Relax {
                        string_parsing: true
                    }
                ) =>
            {
                value
                    .as_str()
                    .unwrap()
                    .parse::<f32>()
                    .map_err(|_| create_error())?
                    .into()
            }
            (Some(DataType::Float32), ValueType::F64) => value.try_as_f32()?.into(),
            // ---- Float64 -----
            (
                Some(DataType::Float64),
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) if matches!(self.numeric_handling, NumericHandling::Relax { .. }) => {
                (value.try_as_i64()? as f64).into()
            }
            (Some(DataType::Float64), ValueType::String)
                if matches!(
                    self.numeric_handling,
                    NumericHandling::Relax {
                        string_parsing: true
                    }
                ) =>
            {
                value
                    .as_str()
                    .unwrap()
                    .parse::<f64>()
                    .map_err(|_| create_error())?
                    .into()
            }
            (Some(DataType::Float64) | None, ValueType::F64) => value.try_as_f64()?.into(),
            // ---- Decimal -----
            (Some(DataType::Decimal) | None, ValueType::I128 | ValueType::U128) => {
                Decimal::from_str(&value.try_as_i128()?.to_string())
                    .map_err(|_| create_error())?
                    .into()
            }
            (Some(DataType::Decimal), ValueType::I64 | ValueType::U64) => {
                Decimal::from(value.try_as_i64()?).into()
            }

            (Some(DataType::Decimal), ValueType::F64) => Decimal::try_from(value.try_as_f64()?)
                .map_err(|_| create_error())?
                .into(),

            (Some(DataType::Decimal), ValueType::String) => ScalarImpl::Decimal(
                Decimal::from_str(value.as_str().unwrap()).map_err(|_err| create_error())?,
            ),
            // ---- Date -----
            (
                Some(DataType::Date),
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => Date::with_days_since_unix_epoch(value.try_as_i32()?)
                .map_err(|_| create_error())?
                .into(),
            (Some(DataType::Date), ValueType::String) => str_to_date(value.as_str().unwrap())
                .map_err(|_| create_error())?
                .into(),
            // ---- Varchar -----
            (Some(DataType::Varchar) | None, ValueType::String) => value.as_str().unwrap().into(),
            // ---- Time -----
            (Some(DataType::Time), ValueType::String) => str_to_time(value.as_str().unwrap())
                .map_err(|_| create_error())?
                .into(),
            (
                Some(DataType::Time),
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
            // ---- Timestamp -----
            (Some(DataType::Timestamp), ValueType::String) => {
                str_to_timestamp(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into()
            }
            (
                Some(DataType::Timestamp),
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => i64_to_timestamp(value.as_i64().unwrap())
                .map_err(|_| create_error())?
                .into(),
            // ---- Timestamptz -----
            (Some(DataType::Timestamptz), ValueType::String) => {
                str_with_time_zone_to_timestamptz(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into()
            }
            (
                Some(DataType::Timestamptz),
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => i64_to_timestamptz(value.as_i64().unwrap())
                .map_err(|_| create_error())?
                .into(),
            // ---- Interval -----
            (Some(DataType::Interval), ValueType::String) => {
                Interval::from_iso_8601(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into()
            }
            // ---- Struct -----
            (Some(DataType::Struct(struct_type_info)), ValueType::Object) => StructValue::new(
                struct_type_info
                    .names()
                    .zip_eq_fast(struct_type_info.types())
                    .map(|(field_name, field_type)| {
                        self.parse(
                            json_object_smart_get_value(value, field_name.into())
                                .unwrap_or(&BorrowedValue::Static(simd_json::StaticNode::Null)),
                            Some(field_type),
                        )
                    })
                    .collect::<Result<_, _>>()?,
            )
            .into(),

            (None, ValueType::Object) => StructValue::new(
                value
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(_field_name, field_value)| self.parse(field_value, None))
                    .collect::<Result<_, _>>()?,
            )
            .into(),
            // ---- List -----
            (Some(DataType::List(item_type)), ValueType::Array) => ListValue::new(
                value
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| self.parse(v, Some(item_type)))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .into(),
            (None, ValueType::Array) => ListValue::new(
                value
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| self.parse(v, None))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .into(),
            // ---- Bytea -----
            (Some(DataType::Bytea), ValueType::String) => match self.bytea_handling {
                ByteaHandling::Standard => str_to_bytea(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into(),
                ByteaHandling::Base64 => base64::engine::general_purpose::STANDARD
                    .decode(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into_boxed_slice()
                    .into(),
            },
            // ---- Jsonb -----
            (Some(DataType::Jsonb), ValueType::String)
                if matches!(self.json_value_handling, JsonValueHandling::AsString) =>
            {
                JsonbVal::from_str(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into()
            }
            (Some(DataType::Jsonb), _)
                if matches!(self.json_value_handling, JsonValueHandling::AsValue) =>
            {
                let value: serde_json::Value =
                    value.clone().try_into().map_err(|_| create_error())?;
                JsonbVal::from(value).into()
            }
            // ---- Int256 -----
            (
                Some(DataType::Int256),
                ValueType::I64 | ValueType::I128 | ValueType::U64 | ValueType::U128,
            ) => Int256::from(value.try_as_i64()?).into(),

            (Some(DataType::Int256), ValueType::String) => {
                Int256::from_str(value.as_str().unwrap())
                    .map_err(|_| create_error())?
                    .into()
            }

            (_expected, _got) => Err(create_error())?,
        };
        Ok(Some(v))
    }
}

pub struct JsonAccess<'a, 'b> {
    value: BorrowedValue<'b>,
    options: &'a JsonParseOptions,
}

impl<'a, 'b> JsonAccess<'a, 'b> {
    pub fn new_with_options(value: BorrowedValue<'b>, options: &'a JsonParseOptions) -> Self {
        Self { value, options }
    }

    pub fn new(value: BorrowedValue<'b>) -> Self {
        Self::new_with_options(value, &JsonParseOptions::DEFAULT)
    }
}

impl<'a, 'b> Access for JsonAccess<'a, 'b>
where
    'a: 'b,
{
    fn access(&self, path: &[&str], type_expected: Option<&DataType>) -> AccessResult {
        let mut value = &self.value;
        for (idx, key) in path.iter().enumerate() {
            if let Some(sub_value) = if self.options.ignoring_keycase {
                json_object_smart_get_value(value, (*key).into())
            } else {
                value.get(*key)
            } {
                value = sub_value;
            } else {
                Err(AccessError::Undefined {
                    name: key.to_string(),
                    path: path.iter().take(idx).join("."),
                })?;
            }
        }

        self.options.parse(value, type_expected)
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
