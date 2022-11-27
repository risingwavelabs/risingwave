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

use std::fmt::Write;

use super::{DatumRef, ScalarRefImpl};

// Used to convert ScalarRef to text format
pub trait ToText {
    fn to_text(&self) -> String;
}

macro_rules! implement_using_to_string {
    ($({ $scalar_type:ty } ),*) => {
        $(
            impl ToText for $scalar_type {
                fn to_text(&self) -> String {
                    self.to_string()
                }
            }
        )*
    };
}

macro_rules! implement_using_itoa {
    ($({ $scalar_type:ty } ),*) => {
        $(
            impl ToText for $scalar_type {
                fn to_text(&self) -> String {
                    itoa::Buffer::new().format(*self).to_owned()
                }
            }
        )*
    };
}

implement_using_to_string! {
    { String },
    { &str }
}

implement_using_itoa! {
    { i16 },
    { i32 },
    { i64 }
}

impl ToText for crate::types::OrderedF32 {
    fn to_text(&self) -> String {
        match self.to_f32() {
            Some(v) => {
                if v.is_infinite() {
                    if v.is_sign_positive() {
                        "Infinity".to_owned()
                    } else {
                        "-Infinity".to_owned()
                    }
                } else {
                    let mut buffer = ryu::Buffer::new();
                    buffer.format(v).to_owned()
                }
            }
            None => "NaN".to_owned(),
        }
    }
}

impl ToText for crate::types::OrderedF64 {
    fn to_text(&self) -> String {
        match self.to_f64() {
            Some(v) => {
                if v.is_infinite() {
                    if v.is_sign_positive() {
                        "Infinity".to_owned()
                    } else {
                        "-Infinity".to_owned()
                    }
                } else {
                    let mut buffer = ryu::Buffer::new();
                    buffer.format(v).to_owned()
                }
            }
            None => "NaN".to_owned(),
        }
    }
}

impl ToText for bool {
    fn to_text(&self) -> String {
        if *self {
            "t".to_string()
        } else {
            "f".to_string()
        }
    }
}

/// Convert bytes in `Bytea` type to String.
pub fn format_bytes(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(2 * bytes.len());
    write!(s, "\\x{}", hex::encode(bytes)).unwrap();
    s
}

impl ToText for ScalarRefImpl<'_> {
    fn to_text(&self) -> String {
        match self {
            ScalarRefImpl::Bool(b) => b.to_text(),
            ScalarRefImpl::Int16(i) => i.to_text(),
            ScalarRefImpl::Int32(i) => i.to_text(),
            ScalarRefImpl::Int64(i) => i.to_text(),
            ScalarRefImpl::Float32(f) => f.to_text(),
            ScalarRefImpl::Float64(f) => f.to_text(),
            ScalarRefImpl::Decimal(d) => d.to_text(),
            ScalarRefImpl::Interval(i) => i.to_text(),
            ScalarRefImpl::NaiveDate(d) => d.to_text(),
            ScalarRefImpl::NaiveTime(t) => t.to_text(),
            ScalarRefImpl::NaiveDateTime(dt) => dt.to_text(),
            ScalarRefImpl::List(l) => l.to_text(),
            ScalarRefImpl::Struct(s) => s.to_text(),
            ScalarRefImpl::Utf8(v) => v.to_text(),
            ScalarRefImpl::Bytea(v) => format_bytes(v),
        }
    }
}

impl ToText for DatumRef<'_> {
    fn to_text(&self) -> String {
        match self {
            Some(data) => data.to_text(),
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
        assert_eq!("1.234567890123456".to_text(), ret.to_text());

        // f32 -> text.
        let ret: OrderedFloat<f32> = OrderedFloat::<f32>::from(1.234567);
        assert_eq!("1.234567".to_string(), ret.to_text());
    }
}
