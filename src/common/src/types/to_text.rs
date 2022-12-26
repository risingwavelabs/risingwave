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

use std::fmt::{Result, Write};
use std::num::FpCategory;

use chrono::{TimeZone, Utc};

use super::{DataType, DatumRef, ScalarRefImpl};

// Used to convert ScalarRef to text format
pub trait ToText {
    /// Write the text to the writer.
    fn fmt(&self, f: &mut dyn Write) -> Result;

    fn to_text_with_type(&self, ty: &DataType) -> String;

    /// `to_text` is a special version of `to_text_with_type`, it convert the scalar to default type
    /// text. E.g. for Int64, it will convert to text as a Int64 type.
    /// We should prefer to use `to_text_with_type` because it's more clear and readable.
    ///
    /// Following is the relationship between scalar and default type:
    /// - `ScalarRefImpl::Int16` -> `DataType::Int16`
    /// - `ScalarRefImpl::Int32` -> `DataType::Int32`
    /// - `ScalarRefImpl::Int64` -> `DataType::Int64`
    /// - `ScalarRefImpl::Float32` -> `DataType::Float32`
    /// - `ScalarRefImpl::Float64` -> `DataType::Float64`
    /// - `ScalarRefImpl::Decimal` -> `DataType::Decimal`
    /// - `ScalarRefImpl::Boolean` -> `DataType::Boolean`
    /// - `ScalarRefImpl::Utf8` -> `DataType::Varchar`
    /// - `ScalarRefImpl::Bytea` -> `DataType::Bytea`
    /// - `ScalarRefImpl::NaiveDate` -> `DataType::Date`
    /// - `ScalarRefImpl::NaiveTime` -> `DataType::Time`
    /// - `ScalarRefImpl::NaiveDateTime` -> `DataType::Timestamp`
    /// - `ScalarRefImpl::Interval` -> `DataType::Interval`
    /// - `ScalarRefImpl::List` -> `DataType::List`
    /// - `ScalarRefImpl::Struct` -> `DataType::Struct`
    ///
    /// Exception:
    /// The scalar of `DataType::Timestampz` is the `ScalarRefImpl::Int64`.
    fn to_text(&self) -> String {
        let mut s = String::new();
        self.fmt(&mut s).unwrap();
        s
    }
}

macro_rules! implement_using_to_string {
    ($({ $scalar_type:ty , $data_type:ident} ),*) => {
        $(
            impl ToText for $scalar_type {
                fn fmt(&self, f: &mut dyn Write) -> Result {
                    write!(f, "{self}")
                }
                fn to_text_with_type(&self, ty: &DataType) -> String {
                    match ty {
                        DataType::$data_type => self.to_text(),
                        _ => unreachable!(),
                    }
                }
            }
        )*
    };
}

macro_rules! implement_using_itoa {
    ($({ $scalar_type:ty , $data_type:ident} ),*) => {
        $(
            impl ToText for $scalar_type {
                fn fmt(&self, f: &mut dyn Write) -> Result {
                    write!(f, "{}", itoa::Buffer::new().format(*self))
                }
                fn to_text_with_type(&self, ty:&DataType) -> String {
                    match ty {
                        DataType::$data_type => self.to_text(),
                        _ => unreachable!(),
                    }
                }
            }
        )*
    };
}

implement_using_to_string! {
    { String ,Varchar },
    { &str ,Varchar}
}

implement_using_itoa! {
    { i16 ,Int16},
    { i32 ,Int32}
}

macro_rules! implement_using_ryu {
    ($({ $scalar_type:ty, $data_type:ident } ),*) => {
            $(
            impl ToText for $scalar_type {
                fn fmt(&self, f: &mut dyn Write) -> Result {
                    match self.classify() {
                        FpCategory::Infinite if self.is_sign_negative() => write!(f, "-Infinity"),
                        FpCategory::Infinite => write!(f, "Infinity"),
                        FpCategory::Zero if self.is_sign_negative() => write!(f, "-0"),
                        FpCategory::Nan => write!(f, "NaN"),
                        _ => {
                            let mut buf = ryu::Buffer::new();
                            let mut s = buf.format_finite(self.0);
                            if let Some(trimmed) = s.strip_suffix(".0") {
                                s = trimmed;
                            }
                            if let Some(mut idx) = s.as_bytes().iter().position(|x| *x == b'e') {
                                write!(f, "{}", &s[..=idx])?;
                                if s.as_bytes().get(idx + 1) != Some(&b'-') {
                                    write!(f, "+")?;
                                } else {
                                    idx += 1;
                                    // idx at '-'
                                }
                                if idx + 1 == s.len() {
                                    write!(f, "0")?;
                                } else {
                                    write!(f, "{}", &s[idx + 1..])?;
                                }
                            } else {
                                write!(f, "{}", s)?;
                            }
                            Ok(())
                        }
                    }
                }
                fn to_text_with_type(&self, ty: &DataType) -> String {
                    match ty {
                        DataType::$data_type => self.to_text(),
                        _ => unreachable!(),
                    }
                }
            }
        )*
    };
}

implement_using_ryu! {
    { crate::types::OrderedF32, Float32 },
    { crate::types::OrderedF64, Float64 }
}

impl ToText for i64 {
    fn fmt(&self, f: &mut dyn Write) -> Result {
        write!(f, "{self}")
    }

    fn to_text_with_type(&self, ty: &DataType) -> String {
        match ty {
            DataType::Int64 => self.to_text(),
            DataType::Timestampz => {
                // Just a meaningful representation as placeholder. The real implementation depends
                // on TimeZone from session. See #3552.
                let secs = self.div_euclid(1_000_000);
                let nsecs = self.rem_euclid(1_000_000) * 1000;
                let instant = Utc.timestamp_opt(secs, nsecs as u32).unwrap();
                // PostgreSQL uses a space rather than `T` to separate the date and time.
                // https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-OUTPUT
                instant.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string()
            }
            _ => unreachable!(),
        }
    }
}

impl ToText for bool {
    fn fmt(&self, f: &mut dyn Write) -> Result {
        if *self {
            write!(f, "t")
        } else {
            write!(f, "f")
        }
    }

    fn to_text_with_type(&self, ty: &DataType) -> String {
        match ty {
            DataType::Boolean => self.to_text(),
            _ => unreachable!(),
        }
    }
}

impl ToText for &[u8] {
    fn fmt(&self, f: &mut dyn Write) -> Result {
        write!(f, "\\x{}", hex::encode(self))
    }

    fn to_text_with_type(&self, ty: &DataType) -> String {
        match ty {
            DataType::Bytea => self.to_text(),
            _ => unreachable!(),
        }
    }
}

impl ToText for ScalarRefImpl<'_> {
    fn fmt(&self, f: &mut dyn Write) -> Result {
        match self {
            ScalarRefImpl::Bool(v) => v.fmt(f),
            ScalarRefImpl::Int16(v) => v.fmt(f),
            ScalarRefImpl::Int32(v) => v.fmt(f),
            ScalarRefImpl::Int64(v) => v.fmt(f),
            ScalarRefImpl::Float32(v) => v.fmt(f),
            ScalarRefImpl::Float64(v) => v.fmt(f),
            ScalarRefImpl::Decimal(v) => v.fmt(f),
            ScalarRefImpl::Interval(v) => v.fmt(f),
            ScalarRefImpl::NaiveDate(v) => v.fmt(f),
            ScalarRefImpl::NaiveTime(v) => v.fmt(f),
            ScalarRefImpl::NaiveDateTime(v) => v.fmt(f),
            ScalarRefImpl::List(v) => v.fmt(f),
            ScalarRefImpl::Struct(v) => v.fmt(f),
            ScalarRefImpl::Utf8(v) => v.fmt(f),
            ScalarRefImpl::Bytea(v) => v.fmt(f),
        }
    }

    fn to_text_with_type(&self, ty: &DataType) -> String {
        match self {
            ScalarRefImpl::Bool(b) => b.to_text_with_type(ty),
            ScalarRefImpl::Int16(i) => i.to_text_with_type(ty),
            ScalarRefImpl::Int32(i) => i.to_text_with_type(ty),
            ScalarRefImpl::Int64(i) => i.to_text_with_type(ty),
            ScalarRefImpl::Float32(f) => f.to_text_with_type(ty),
            ScalarRefImpl::Float64(f) => f.to_text_with_type(ty),
            ScalarRefImpl::Decimal(d) => d.to_text_with_type(ty),
            ScalarRefImpl::Interval(i) => i.to_text_with_type(ty),
            ScalarRefImpl::NaiveDate(d) => d.to_text_with_type(ty),
            ScalarRefImpl::NaiveTime(t) => t.to_text_with_type(ty),
            ScalarRefImpl::NaiveDateTime(dt) => dt.to_text_with_type(ty),
            ScalarRefImpl::List(l) => l.to_text_with_type(ty),
            ScalarRefImpl::Struct(s) => s.to_text_with_type(ty),
            ScalarRefImpl::Utf8(v) => v.to_text_with_type(ty),
            ScalarRefImpl::Bytea(v) => v.to_text_with_type(ty),
        }
    }
}

impl ToText for DatumRef<'_> {
    fn fmt(&self, f: &mut dyn Write) -> Result {
        match self {
            Some(data) => data.fmt(f),
            None => write!(f, "NULL"),
        }
    }

    fn to_text_with_type(&self, ty: &DataType) -> String {
        match self {
            Some(data) => data.to_text_with_type(ty),
            None => "NULL".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::ordered_float::OrderedFloat;
    use crate::types::to_text::ToText;

    #[test]
    fn test_float_to_text() {
        // f64 -> text.
        let ret: OrderedFloat<f64> = OrderedFloat::<f64>::from(1.234567890123456);
        tracing::info!("ret: {}", ret.to_text());
        assert_eq!("1.234567890123456".to_string(), ret.to_text());

        // f32 -> text.
        let ret: OrderedFloat<f32> = OrderedFloat::<f32>::from(1.234567);
        assert_eq!("1.234567".to_string(), ret.to_text());
    }
}
