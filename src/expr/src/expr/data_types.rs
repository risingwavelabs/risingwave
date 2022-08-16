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

//! Macros containing all necessary information for a logical type.
//!
//! Each type macro will call the `$macro` with multiple parameters:
//! * Patterns when being used in pattern match. e.g., `DataType::Decimal { .. }`.
//! * Array types. e.g., `DecimalArray`.
//!
//! To understand how this datatype macros work, we write them in pseudo code:
//!
//! ```ignore
//! fn boolean<T>(f: impl Fn(DataType, Array) -> T) -> T {
//!     f(DataType::Boolean, BoolArray)
//! }
//!
//! fn type_array(_: DataType, a: Array) -> Array {
//!     a
//! }
//!
//! boolean(type_array) // result = BoolArray
//! ```
//!
//! Due to Rust's macro expand order, using this pattern could help the macro expand in an expected
//! way.

#[macro_export]
macro_rules! boolean {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Boolean,
            risingwave_common::array::BoolArray
        }
    };
}

pub(crate) use boolean;

#[macro_export]
macro_rules! struct_type {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Struct{ .. },
            risingwave_common::array::StructArray
        }
    };
}

pub(crate) use struct_type;

#[macro_export]
macro_rules! list {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::List{ .. },
            risingwave_common::array::ListArray
        }
    };
}

pub(crate) use list;

#[macro_export]
macro_rules! int16 {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Int16,
            risingwave_common::array::I16Array
        }
    };
}

pub(crate) use int16;

#[macro_export]
macro_rules! int32 {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Int32,
            risingwave_common::array::I32Array
        }
    };
}

pub(crate) use int32;

#[macro_export]
macro_rules! int64 {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Int64,
            risingwave_common::array::I64Array
        }
    };
}

pub(crate) use int64;

#[macro_export]
macro_rules! float32 {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Float32,
            risingwave_common::array::F32Array
        }
    };
}

pub(crate) use float32;

#[macro_export]
macro_rules! float64 {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Float64,
            risingwave_common::array::F64Array
        }
    };
}

pub(crate) use float64;

#[macro_export]
macro_rules! decimal {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Decimal { .. },
            risingwave_common::array::DecimalArray
        }
    };
}

pub(crate) use decimal;

#[macro_export]
macro_rules! date {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Date,
            risingwave_common::array::NaiveDateArray
        }
    };
}

pub(crate) use date;

#[macro_export]
macro_rules! varchar {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Varchar,
            risingwave_common::array::Utf8Array
        }
    };
}

pub(crate) use varchar;

#[macro_export]
macro_rules! time {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Time,
            risingwave_common::array::NaiveTimeArray
        }
    };
}

pub(crate) use time;

#[macro_export]
macro_rules! timestamp {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Timestamp,
            risingwave_common::array::NaiveDateTimeArray
        }
    };
}

pub(crate) use timestamp;

#[macro_export]
macro_rules! timestampz {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Timestampz,
            risingwave_common::array::I64Array
        }
    };
}

pub(crate) use timestampz;

#[macro_export]
macro_rules! interval {
    ($macro:ident) => {
        $macro! {
            risingwave_common::types::DataType::Interval,
            risingwave_common::array::IntervalArray
        }
    };
}

pub(crate) use interval;

/// Get the type match pattern out of the type macro. e.g., `DataType::Decimal { .. }`.
#[macro_export]
macro_rules! type_match_pattern {
    ($match_pattern:pat, $array:ty) => {
        $match_pattern
    };
}

pub(crate) use type_match_pattern;

/// Get the array type out of the type macro. e.g., `Int32Array`.
#[macro_export]
macro_rules! type_array {
    ($match_pattern:pat, $array:ty) => {
        $array
    };
}

pub(crate) use type_array;
