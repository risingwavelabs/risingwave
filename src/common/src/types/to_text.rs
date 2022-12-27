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

use super::{
    DatumRef, Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper,
    ScalarRefImpl,
};
use crate::for_all_scalar_variants;

// Used to convert ScalarRef to text format
pub trait ToText {
    /// Write the text to the writer.
    fn write<W: Write>(&self, f: &mut W) -> Result;

    fn to_text(&self) -> String {
        let mut s = String::new();
        self.write(&mut s).unwrap();
        s
    }
}

macro_rules! implement_using_display {
    ($($scalar_type:ty),*) => {
        $(
            impl ToText for $scalar_type {
                fn write<W: Write>(&self, f: &mut W) -> Result {
                    write!(f, "{self}")
                }
            }
        )*
    };
}
implement_using_display! { String, &str, Decimal, IntervalUnit, NaiveDateWrapper, NaiveTimeWrapper, NaiveDateTimeWrapper }

macro_rules! implement_using_itoa {
    ($($type:ty),*) => {
        $(
            impl ToText for $type {
                fn write<W: Write>(&self, f: &mut W) -> Result {
                    write!(f, "{}", itoa::Buffer::new().format(*self))
                }
            }
        )*
    };
}
implement_using_itoa! { i16, i32, i64 }

macro_rules! implement_using_ryu {
    ($({ $scalar_type:ty, $data_type:ident } ),*) => {
            $(
            impl ToText for $scalar_type {
                fn write<W: Write>(&self, f: &mut W) -> Result {
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
                                idx += 1;
                                write!(f, "{}", &s[..idx])?;
                                if s.as_bytes()[idx] == b'-' {
                                    write!(f, "-")?;
                                    idx += 1;
                                } else {
                                    write!(f, "+")?;
                                }
                                if idx + 1 == s.len() {
                                    write!(f, "0")?;
                                }
                                write!(f, "{}", &s[idx..])?;
                            } else {
                                write!(f, "{}", s)?;
                            }
                            Ok(())
                        }
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

impl ToText for bool {
    fn write<W: Write>(&self, f: &mut W) -> Result {
        if *self {
            write!(f, "t")
        } else {
            write!(f, "f")
        }
    }
}

impl ToText for &[u8] {
    fn write<W: Write>(&self, f: &mut W) -> Result {
        write!(f, "\\x{}", hex::encode(self))
    }
}

macro_rules! impl_totext_for_scalar {
    ($({ $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty }),*) => {
        impl ToText for ScalarRefImpl<'_> {
            fn write<W: Write>(&self, f: &mut W) -> Result {
                match self {
                    $(ScalarRefImpl::$variant_name(v) => v.write(f),)*
                }
            }
        }
    };
}
for_all_scalar_variants! { impl_totext_for_scalar }

impl ToText for DatumRef<'_> {
    fn write<W: Write>(&self, f: &mut W) -> Result {
        match self {
            Some(data) => data.write(f),
            None => write!(f, "NULL"),
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
