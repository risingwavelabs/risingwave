//! Macros containing all necessary information for a logical type.
//!
//! Each type macro will call the `$macro` with multiple parameters:
//! * Patterns when being used in pattern match. e.g., `DataTypeKind::Decimal { .. }`.
//! * Array types. e.g., `DecimalArray`.
//!
//! To understand how this datatype macros work, we write them in pseudo code:
//!
//! ```ignore
//! fn boolean<T>(f: impl Fn(DataTypeKind, Array) -> T) -> T {
//!   f(DataTypeKind::Boolean, BoolArray)
//! }
//!
//! fn type_array(_: DataTypeKind, a: Array) -> Array {
//!   a
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
          $crate::types::DataTypeKind::Boolean,
          $crate::array::BoolArray
        }
    };
}

pub(crate) use boolean;

#[macro_export]
macro_rules! int16 {
    ($macro:tt) => {
        $macro! {
          $crate::types::DataTypeKind::Int16,
          $crate::array::I16Array
        }
    };
}

pub(crate) use int16;

#[macro_export]
macro_rules! int32 {
    ($macro:tt) => {
        $macro! {
          $crate::types::DataTypeKind::Int32,
          $crate::array::I32Array
        }
    };
}

pub(crate) use int32;

#[macro_export]
macro_rules! int64 {
    ($macro:tt) => {
        $macro! {
          $crate::types::DataTypeKind::Int64,
          $crate::array::I64Array
        }
    };
}

pub(crate) use int64;

#[macro_export]
macro_rules! float32 {
    ($macro:tt) => {
        $macro! {
          $crate::types::DataTypeKind::Float32,
          $crate::array::F32Array
        }
    };
}

pub(crate) use float32;

#[macro_export]
macro_rules! float64 {
    ($macro:tt) => {
        $macro! {
          $crate::types::DataTypeKind::Float64,
          $crate::array::F64Array
        }
    };
}

pub(crate) use float64;

#[macro_export]
macro_rules! decimal {
    ($macro:tt) => {
        $macro! {
          $crate::types::DataTypeKind::Decimal { .. },
          $crate::array::DecimalArray
        }
    };
}

pub(crate) use decimal;

#[macro_export]
macro_rules! date {
    ($macro:tt) => {
        $macro! {
          $crate::types::DataTypeKind::Date,
          $crate::array::NaiveDateArray
        }
    };
}

pub(crate) use date;

#[macro_export]
macro_rules! char {
    ($macro:tt) => {
        $macro! {
          $crate::types::DataTypeKind::Char,
          $crate::array::Utf8Array
        }
    };
}

pub(crate) use char;

#[macro_export]
macro_rules! varchar {
    ($macro:tt) => {
        $macro! {
          $crate::types::DataTypeKind::Varchar,
          $crate::array::Utf8Array
        }
    };
}

pub(crate) use varchar;

#[macro_export]
macro_rules! time {
    ($macro:tt) => {
        $macro! {
          $crate::types::DataTypeKind::Time,
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
          $crate::types::DataTypeKind::Timestamp,
          $crate::array::NaiveDateTimeArray
        }
    };
}

pub(crate) use timestamp;

#[macro_export]
macro_rules! timestampz {
    ($macro:tt) => {
        $macro! {
          $crate::types::DataTypeKind::Timestampz,
          $crate::array::I64Array
        }
    };
}

pub(crate) use timestampz;

#[macro_export]
macro_rules! interval {
    ($macro:tt) => {
        $macro! {
          $crate::types::DataTypeKind::Interval,
          $crate::array::IntervalArray
        }
    };
}

pub(crate) use interval;

/// Get the type match pattern out of the type macro. e.g., `DataTypeKind::Decimal { .. }`.
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
