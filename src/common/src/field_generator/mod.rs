// Copyright 2025 RisingWave Labs
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

mod numeric;
mod timestamp;
mod varchar;

use std::time::Duration;

// TODO(error-handling): use a new error type
use anyhow::{Result, anyhow};
use chrono::{DateTime, FixedOffset};
pub use numeric::*;
use serde_json::Value;
pub use timestamp::*;
pub use varchar::*;

use crate::array::{ListValue, StructValue};
use crate::types::{DataType, Datum, ScalarImpl, Timestamp, Timestamptz};

pub const DEFAULT_MIN: i16 = i16::MIN;
pub const DEFAULT_MAX: i16 = i16::MAX;
pub const DEFAULT_START: i16 = 0;
pub const DEFAULT_END: i16 = i16::MAX;

/// default max past for `TimestampField` = 1 day
pub const DEFAULT_MAX_PAST: Duration = Duration::from_secs(60 * 60 * 24);

/// default length for `VarcharField` = 10
pub const DEFAULT_LENGTH: usize = 10;

/// fields that can be randomly generated impl this trait
pub trait NumericFieldRandomGenerator {
    fn new(min: Option<String>, max: Option<String>, seed: u64) -> Result<Self>
    where
        Self: Sized;

    fn generate(&mut self, offset: u64) -> Value;

    fn generate_datum(&mut self, offset: u64) -> Datum;
}

/// fields that can be continuously generated impl this trait
pub trait NumericFieldSequenceGenerator {
    fn new(
        start: Option<String>,
        end: Option<String>,
        offset: u64,
        step: u64,
        event_offset: u64,
    ) -> Result<Self>
    where
        Self: Sized;

    fn generate(&mut self) -> Value;

    fn generate_datum(&mut self) -> Datum;
}

/// the way that datagen create the field data. such as 'sequence' or 'random'.
#[derive(Default)]
pub enum FieldKind {
    Sequence,
    #[default]
    Random,
}

pub enum VarcharProperty {
    RandomVariableLength,
    RandomFixedLength(Option<usize>),
    Constant,
}

pub enum FieldGeneratorImpl {
    I16Sequence(I16SequenceField),
    I32Sequence(I32SequenceField),
    I64Sequence(I64SequenceField),
    F32Sequence(F32SequenceField),
    F64Sequence(F64SequenceField),
    I16Random(I16RandomField),
    I32Random(I32RandomField),
    I64Random(I64RandomField),
    F32Random(F32RandomField),
    F64Random(F64RandomField),
    VarcharRandomVariableLength(VarcharRandomVariableLengthField),
    VarcharRandomFixedLength(VarcharRandomFixedLengthField),
    VarcharConstant,
    Timestamp(ChronoField<Timestamp>),
    Timestamptz(ChronoField<Timestamptz>),
    Struct(Vec<(String, FieldGeneratorImpl)>),
    List(Box<FieldGeneratorImpl>, usize),
}

impl FieldGeneratorImpl {
    pub fn with_number_sequence(
        data_type: DataType,
        start: Option<String>,
        end: Option<String>,
        split_index: u64,
        split_num: u64,
        offset: u64,
    ) -> Result<Self> {
        match data_type {
            DataType::Int16 => Ok(FieldGeneratorImpl::I16Sequence(I16SequenceField::new(
                start,
                end,
                split_index,
                split_num,
                offset,
            )?)),
            DataType::Int32 => Ok(FieldGeneratorImpl::I32Sequence(I32SequenceField::new(
                start,
                end,
                split_index,
                split_num,
                offset,
            )?)),
            DataType::Int64 => Ok(FieldGeneratorImpl::I64Sequence(I64SequenceField::new(
                start,
                end,
                split_index,
                split_num,
                offset,
            )?)),
            DataType::Float32 => Ok(FieldGeneratorImpl::F32Sequence(F32SequenceField::new(
                start,
                end,
                split_index,
                split_num,
                offset,
            )?)),
            DataType::Float64 => Ok(FieldGeneratorImpl::F64Sequence(F64SequenceField::new(
                start,
                end,
                split_index,
                split_num,
                offset,
            )?)),
            _ => Err(anyhow!("unimplemented field generator {}", data_type)),
        }
    }

    pub fn with_number_random(
        data_type: DataType,
        min: Option<String>,
        max: Option<String>,
        seed: u64,
    ) -> Result<Self> {
        match data_type {
            DataType::Int16 => Ok(FieldGeneratorImpl::I16Random(I16RandomField::new(
                min, max, seed,
            )?)),
            DataType::Int32 => Ok(FieldGeneratorImpl::I32Random(I32RandomField::new(
                min, max, seed,
            )?)),
            DataType::Int64 => Ok(FieldGeneratorImpl::I64Random(I64RandomField::new(
                min, max, seed,
            )?)),
            DataType::Float32 => Ok(FieldGeneratorImpl::F32Random(F32RandomField::new(
                min, max, seed,
            )?)),
            DataType::Float64 => Ok(FieldGeneratorImpl::F64Random(F64RandomField::new(
                min, max, seed,
            )?)),
            _ => Err(anyhow!("unimplemented field generator {}", data_type)),
        }
    }

    pub fn with_timestamp(
        base: Option<DateTime<FixedOffset>>,
        max_past: Option<String>,
        max_past_mode: Option<String>,
        seed: u64,
    ) -> Result<Self> {
        Ok(FieldGeneratorImpl::Timestamp(ChronoField::new(
            base,
            max_past,
            max_past_mode,
            seed,
        )?))
    }

    pub fn with_timestamptz(
        base: Option<DateTime<FixedOffset>>,
        max_past: Option<String>,
        max_past_mode: Option<String>,
        seed: u64,
    ) -> Result<Self> {
        Ok(FieldGeneratorImpl::Timestamptz(ChronoField::new(
            base,
            max_past,
            max_past_mode,
            seed,
        )?))
    }

    pub fn with_varchar(varchar_property: &VarcharProperty, seed: u64) -> Self {
        match varchar_property {
            VarcharProperty::RandomFixedLength(length_option) => {
                FieldGeneratorImpl::VarcharRandomFixedLength(VarcharRandomFixedLengthField::new(
                    length_option,
                    seed,
                ))
            }
            VarcharProperty::RandomVariableLength => {
                FieldGeneratorImpl::VarcharRandomVariableLength(
                    VarcharRandomVariableLengthField::new(seed),
                )
            }
            VarcharProperty::Constant => FieldGeneratorImpl::VarcharConstant,
        }
    }

    pub fn with_struct_fields(fields: Vec<(String, FieldGeneratorImpl)>) -> Result<Self> {
        Ok(FieldGeneratorImpl::Struct(fields))
    }

    pub fn with_list(field: FieldGeneratorImpl, length_option: Option<String>) -> Result<Self> {
        let list_length = if let Some(length_option) = length_option {
            length_option.parse::<usize>()?
        } else {
            DEFAULT_LENGTH
        };
        Ok(FieldGeneratorImpl::List(Box::new(field), list_length))
    }

    pub fn generate_json(&mut self, offset: u64) -> Value {
        match self {
            FieldGeneratorImpl::I16Sequence(f) => f.generate(),
            FieldGeneratorImpl::I32Sequence(f) => f.generate(),
            FieldGeneratorImpl::I64Sequence(f) => f.generate(),
            FieldGeneratorImpl::F32Sequence(f) => f.generate(),
            FieldGeneratorImpl::F64Sequence(f) => f.generate(),
            FieldGeneratorImpl::I16Random(f) => f.generate(offset),
            FieldGeneratorImpl::I32Random(f) => f.generate(offset),
            FieldGeneratorImpl::I64Random(f) => f.generate(offset),
            FieldGeneratorImpl::F32Random(f) => f.generate(offset),
            FieldGeneratorImpl::F64Random(f) => f.generate(offset),
            FieldGeneratorImpl::VarcharRandomFixedLength(f) => f.generate(offset),
            FieldGeneratorImpl::VarcharRandomVariableLength(f) => f.generate(offset),
            FieldGeneratorImpl::VarcharConstant => VarcharConstant::generate_json(),
            FieldGeneratorImpl::Timestamp(f) => f.generate(offset),
            FieldGeneratorImpl::Timestamptz(f) => f.generate(offset),
            FieldGeneratorImpl::Struct(fields) => {
                let map = fields
                    .iter_mut()
                    .map(|(name, r#gen)| (name.clone(), r#gen.generate_json(offset)))
                    .collect();
                Value::Object(map)
            }
            FieldGeneratorImpl::List(field, list_length) => {
                let vec = (0..*list_length)
                    .map(|_| field.generate_json(offset))
                    .collect::<Vec<_>>();
                Value::Array(vec)
            }
        }
    }

    pub fn generate_datum(&mut self, offset: u64) -> Datum {
        match self {
            FieldGeneratorImpl::I16Sequence(f) => f.generate_datum(),
            FieldGeneratorImpl::I32Sequence(f) => f.generate_datum(),
            FieldGeneratorImpl::I64Sequence(f) => f.generate_datum(),
            FieldGeneratorImpl::F32Sequence(f) => f.generate_datum(),
            FieldGeneratorImpl::F64Sequence(f) => f.generate_datum(),
            FieldGeneratorImpl::I16Random(f) => f.generate_datum(offset),
            FieldGeneratorImpl::I32Random(f) => f.generate_datum(offset),
            FieldGeneratorImpl::I64Random(f) => f.generate_datum(offset),
            FieldGeneratorImpl::F32Random(f) => f.generate_datum(offset),
            FieldGeneratorImpl::F64Random(f) => f.generate_datum(offset),
            FieldGeneratorImpl::VarcharRandomFixedLength(f) => f.generate_datum(offset),
            FieldGeneratorImpl::VarcharRandomVariableLength(f) => f.generate_datum(offset),
            FieldGeneratorImpl::VarcharConstant => VarcharConstant::generate_datum(),
            FieldGeneratorImpl::Timestamp(f) => f.generate_datum(offset),
            FieldGeneratorImpl::Timestamptz(f) => f.generate_datum(offset),
            FieldGeneratorImpl::Struct(fields) => {
                let data = fields
                    .iter_mut()
                    .map(|(_, r#gen)| r#gen.generate_datum(offset))
                    .collect();
                Some(ScalarImpl::Struct(StructValue::new(data)))
            }
            FieldGeneratorImpl::List(field, list_length) => {
                Some(ScalarImpl::List(ListValue::from_datum_iter(
                    &field.data_type(),
                    std::iter::repeat_with(|| field.generate_datum(offset)).take(*list_length),
                )))
            }
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Self::I16Sequence(_) => DataType::Int16,
            Self::I32Sequence(_) => DataType::Int32,
            Self::I64Sequence(_) => DataType::Int64,
            Self::F32Sequence(_) => DataType::Float32,
            Self::F64Sequence(_) => DataType::Float64,
            Self::I16Random(_) => DataType::Int16,
            Self::I32Random(_) => DataType::Int32,
            Self::I64Random(_) => DataType::Int64,
            Self::F32Random(_) => DataType::Float32,
            Self::F64Random(_) => DataType::Float64,
            Self::VarcharRandomFixedLength(_) => DataType::Varchar,
            Self::VarcharRandomVariableLength(_) => DataType::Varchar,
            Self::VarcharConstant => DataType::Varchar,
            Self::Timestamp(_) => DataType::Timestamp,
            Self::Timestamptz(_) => DataType::Timestamptz,
            Self::Struct(_) => todo!("data_type for struct"),
            Self::List(inner, _) => DataType::List(Box::new(inner.data_type())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_sequence() {
        let split_num = 4;
        let mut i32_fields = vec![];
        for split_index in 0..split_num {
            i32_fields.push(
                FieldGeneratorImpl::with_number_sequence(
                    DataType::Int32,
                    Some("1".to_owned()),
                    Some("20".to_owned()),
                    split_index,
                    split_num,
                    0,
                )
                .unwrap(),
            );
        }

        for step in 0..5 {
            for (index, i32_field) in i32_fields.iter_mut().enumerate() {
                let value = i32_field.generate_json(0);
                assert!(value.is_number());
                let num = value.as_u64();
                let expected_num = split_num * step + 1 + index as u64;
                assert_eq!(expected_num, num.unwrap());
            }
        }
    }

    #[test]
    fn test_random_generate() {
        let seed = 1234;
        for data_type in [
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Varchar,
            DataType::Timestamp,
            DataType::Timestamptz,
        ] {
            let mut generator = match data_type {
                DataType::Varchar => FieldGeneratorImpl::with_varchar(
                    &VarcharProperty::RandomFixedLength(None),
                    seed,
                ),
                DataType::Timestamp => {
                    FieldGeneratorImpl::with_timestamp(None, None, None, seed).unwrap()
                }
                DataType::Timestamptz => {
                    FieldGeneratorImpl::with_timestamptz(None, None, None, seed).unwrap()
                }
                _ => FieldGeneratorImpl::with_number_random(data_type, None, None, seed).unwrap(),
            };

            let val1 = generator.generate_json(1);
            let val2 = generator.generate_json(2);

            assert_ne!(val1, val2);

            let val1_new = generator.generate_json(1);
            let val2_new = generator.generate_json(2);

            assert_eq!(val1_new, val1);
            assert_eq!(val2_new, val2);

            let datum1 = generator.generate_datum(5);
            let datum2 = generator.generate_datum(7);

            assert_ne!(datum1, datum2);

            let datum1_new = generator.generate_datum(5);
            let datum2_new = generator.generate_datum(7);

            assert_eq!(datum1_new, datum1);
            assert_eq!(datum2_new, datum2);
        }
    }

    #[test]
    fn test_deterministic_timestamp() {
        let seed = 1234;
        let base_time: DateTime<FixedOffset> =
            DateTime::parse_from_rfc3339("2020-01-01T00:00:00+00:00").unwrap();
        let mut generator =
            FieldGeneratorImpl::with_timestamp(Some(base_time), None, None, seed).unwrap();
        let val1 = generator.generate_json(1);
        let val2 = generator.generate_json(1);
        assert_eq!(val1, val2);
    }
}
