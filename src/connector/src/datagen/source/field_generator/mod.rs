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

mod numeric;
mod timestamp;
mod varchar;

use std::time::Duration;

use anyhow::Result;
pub use numeric::*;
use risingwave_common::types::DataType;
use serde_json::Value;
pub use timestamp::*;
pub use varchar::*;

pub const DEFAULT_MIN: i16 = i16::MIN;
pub const DEFAULT_MAX: i16 = i16::MAX;
pub const DEFAULT_START: i16 = 0;
pub const DEFAULT_END: i16 = i16::MAX;

// default max_past for TimestampField =  1 day
pub const DEFAULT_MAX_PAST: Duration = Duration::from_secs(60 * 60 * 24);

// default length for VarcharField = 10
pub const DEFAULT_LENGTH: usize = 10;

/// fields that can be continuously or randomly generated impl this trait
/// such as i32, float, double
pub trait NumericFieldGenerator {
    fn with_sequence(min: Option<String>, max: Option<String>,split_index:i32,split_num:i32) -> Result<Self>
    where
        Self: Sized;
    fn with_random(start: Option<String>, end: Option<String>) -> Result<Self>
    where
        Self: Sized;
    fn generate(&mut self) -> Value;
}

/// the way that datagen create the field data. such as 'sequence' or 'random'.
pub enum FieldKind {
    Sequence,
    Random,
}

impl Default for FieldKind {
    fn default() -> Self {
        FieldKind::Random
    }
}

pub enum FieldGeneratorImpl {
    I16(I16Field),
    I32(I32Field),
    I64(I64Field),
    F32(F32Field),
    F64(F64Field),
    Varchar(VarcharField),
    Timestamp(TimestampField),
}

impl FieldGeneratorImpl {
    pub fn new(
        data_type: DataType,
        kind: FieldKind,
        first_arg: Option<String>,
        second_arg: Option<String>,
        split_index:i32,
        split_num:i32,
    ) -> Result<Self> {
        match kind {
            // todo(d2lark) use macro to simplify the code later
            FieldKind::Random => match data_type {
                DataType::Int16 => Ok(FieldGeneratorImpl::I16(I16Field::with_random(
                    first_arg, second_arg,
                )?)),
                DataType::Int32 => Ok(FieldGeneratorImpl::I32(I32Field::with_random(
                    first_arg, second_arg,
                )?)),
                DataType::Int64 => Ok(FieldGeneratorImpl::I64(I64Field::with_random(
                    first_arg, second_arg,
                )?)),
                DataType::Float32 => Ok(FieldGeneratorImpl::F32(F32Field::with_random(
                    first_arg, second_arg,
                )?)),
                DataType::Float64 => Ok(FieldGeneratorImpl::F64(F64Field::with_random(
                    first_arg, second_arg,
                )?)),
                DataType::Varchar => Ok(FieldGeneratorImpl::Varchar(VarcharField::new(first_arg)?)),
                DataType::Timestamp => Ok(FieldGeneratorImpl::Timestamp(TimestampField::new(
                    first_arg,
                )?)),
                _ => unimplemented!(),
            },
            FieldKind::Sequence => match data_type {
                DataType::Int16 => Ok(FieldGeneratorImpl::I16(I16Field::with_sequence(
                    first_arg, second_arg,split_index,split_num
                )?)),
                DataType::Int32 => Ok(FieldGeneratorImpl::I32(I32Field::with_sequence(
                    first_arg, second_arg,split_index,split_num
                )?)),
                DataType::Int64 => Ok(FieldGeneratorImpl::I64(I64Field::with_sequence(
                    first_arg, second_arg,split_index,split_num
                )?)),
                DataType::Float32 => Ok(FieldGeneratorImpl::F32(F32Field::with_sequence(
                    first_arg, second_arg,split_index,split_num
                )?)),
                DataType::Float64 => Ok(FieldGeneratorImpl::F64(F64Field::with_sequence(
                    first_arg, second_arg,split_index,split_num
                )?)),
                _ => unimplemented!(),
            },
        }
    }

    pub fn generate(&mut self) -> Value {
        match self {
            FieldGeneratorImpl::I16(f) => f.generate(),
            FieldGeneratorImpl::I32(f) => f.generate(),
            FieldGeneratorImpl::I64(f) => f.generate(),
            FieldGeneratorImpl::F32(f) => f.generate(),
            FieldGeneratorImpl::F64(f) => f.generate(),
            FieldGeneratorImpl::Varchar(f) => f.generate(),
            FieldGeneratorImpl::Timestamp(f) => f.generate(),
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
        for split_index in 0..4{
            i32_fields.push(            FieldGeneratorImpl::new(
                DataType::Int32,
                FieldKind::Sequence,
                Some("1".to_string()),
                Some("20".to_string()),
                split_index,
                split_num
            )
            .unwrap());
        }

        for step in 0..5{
            for (index,i32_field)  in i32_fields.iter_mut().enumerate(){
                let value = i32_field.generate();
                assert!(value.is_number());
                let num = value.as_i64();
                let expected_num = 4*step+1+index as i64;
                assert_eq!(expected_num,num.unwrap());
            }
        }
    }
}
