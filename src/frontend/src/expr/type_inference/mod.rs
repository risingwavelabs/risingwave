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

//! This type inference is just to infer the return type of function calls, and make sure the
//! functionCall expressions have same input type requirement and return type definition as backend.
use risingwave_common::types::DataType;

mod cast;
mod func;
pub use cast::{
    align_types, cast_map_array, cast_ok, cast_ok_base, least_restrictive, CastContext,
};
pub use func::{func_sig_map, infer_type, FuncSign};

/// `DataTypeName` is designed for type derivation here. In other scenarios,
/// use `DataType` instead.
#[derive(Debug, Ord, PartialOrd, Clone, PartialEq, Eq, Hash, Copy)]
pub enum DataTypeName {
    Boolean,
    Int16,
    Int32,
    Int64,
    Decimal,
    Float32,
    Float64,
    Varchar,
    Date,
    Timestamp,
    Timestampz,
    Time,
    Interval,
    Struct,
    List,
}

impl From<&DataType> for DataTypeName {
    fn from(ty: &DataType) -> Self {
        match ty {
            DataType::Boolean => DataTypeName::Boolean,
            DataType::Int16 => DataTypeName::Int16,
            DataType::Int32 => DataTypeName::Int32,
            DataType::Int64 => DataTypeName::Int64,
            DataType::Decimal => DataTypeName::Decimal,
            DataType::Float32 => DataTypeName::Float32,
            DataType::Float64 => DataTypeName::Float64,
            DataType::Varchar => DataTypeName::Varchar,
            DataType::Date => DataTypeName::Date,
            DataType::Timestamp => DataTypeName::Timestamp,
            DataType::Timestampz => DataTypeName::Timestampz,
            DataType::Time => DataTypeName::Time,
            DataType::Interval => DataTypeName::Interval,
            DataType::Struct { .. } => DataTypeName::Struct,
            DataType::List { .. } => DataTypeName::List,
        }
    }
}

impl From<DataType> for DataTypeName {
    fn from(ty: DataType) -> Self {
        (&ty).into()
    }
}

impl From<DataTypeName> for DataType {
    fn from(type_name: DataTypeName) -> Self {
        match type_name {
            DataTypeName::Boolean => DataType::Boolean,
            DataTypeName::Int16 => DataType::Int16,
            DataTypeName::Int32 => DataType::Int32,
            DataTypeName::Int64 => DataType::Int64,
            DataTypeName::Decimal => DataType::Decimal,
            DataTypeName::Float32 => DataType::Float32,
            DataTypeName::Float64 => DataType::Float64,
            DataTypeName::Varchar => DataType::Varchar,
            DataTypeName::Date => DataType::Date,
            DataTypeName::Timestamp => DataType::Timestamp,
            DataTypeName::Timestampz => DataType::Timestampz,
            DataTypeName::Time => DataType::Time,
            DataTypeName::Interval => DataType::Interval,
            DataTypeName::Struct | DataTypeName::List => {
                panic!("Functions returning struct or list can not be inferred. Please use `FunctionCall::new_unchecked`.")
            }
        }
    }
}
