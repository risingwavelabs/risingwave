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
//
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
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Boolean,
            $crate::array::BoolArray
        }
    };
}

pub(crate) use boolean;

#[macro_export]
macro_rules! int16 {
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Int16,
            $crate::array::I16Array
        }
    };
}

pub(crate) use int16;

#[macro_export]
macro_rules! int32 {
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Int32,
            $crate::array::I32Array
        }
    };
}

pub(crate) use int32;

#[macro_export]
macro_rules! int64 {
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Int64,
            $crate::array::I64Array
        }
    };
}

pub(crate) use int64;

#[macro_export]
macro_rules! float32 {
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Float32,
            $crate::array::F32Array
        }
    };
}

pub(crate) use float32;

#[macro_export]
macro_rules! float64 {
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Float64,
            $crate::array::F64Array
        }
    };
}

pub(crate) use float64;

#[macro_export]
macro_rules! decimal {
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Decimal { .. },
            $crate::array::DecimalArray
        }
    };
}

pub(crate) use decimal;

#[macro_export]
macro_rules! date {
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Date,
            $crate::array::NaiveDateArray
        }
    };
}

pub(crate) use date;

#[macro_export]
macro_rules! char {
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Char,
            $crate::array::Utf8Array
        }
    };
}

pub(crate) use char;

#[macro_export]
macro_rules! varchar {
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Varchar,
            $crate::array::Utf8Array
        }
    };
}

pub(crate) use varchar;

#[macro_export]
macro_rules! time {
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Time,
            $crate::array::NaiveTimeArray
        }
    };
}

#[allow(unused_imports)]
pub(crate) use time;

#[macro_export]
macro_rules! timestamp {
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Timestamp,
            $crate::array::NaiveDateTimeArray
        }
    };
}

pub(crate) use timestamp;

#[macro_export]
macro_rules! timestampz {
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Timestampz,
            $crate::array::I64Array
        }
    };
}

pub(crate) use timestampz;

#[macro_export]
macro_rules! interval {
    ($macro:tt) => {
        $macro! {
            $crate::types::DataType::Interval,
            $crate::array::IntervalArray
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
