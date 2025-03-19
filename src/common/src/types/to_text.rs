// Copyright 2025 RisingWave Labs
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

use std::fmt::{Result, Write};
use std::num::FpCategory;

use super::{DataType, DatumRef, ScalarRefImpl};
use crate::dispatch_scalar_ref_variants;

/// Converts `ScalarRef` to pgwire "TEXT" format.
///
/// ## Relationship with casting to varchar
///
/// For most types, this is also the implementation for casting to varchar, but there are exceptions.
/// e.g., The TEXT format for boolean is `t` / `f` while they cast to varchar `true` / `false`.
/// - <https://github.com/postgres/postgres/blob/REL_16_3/src/include/catalog/pg_cast.dat#L438-L439>
/// - <https://www.postgresql.org/docs/16/sql-createcast.html#:~:text=A%20small%20number%20of%20the%20built%2Din%20types%20do%20indeed%20have%20different%20behaviors%20for%20conversions%2C%20mostly%20because%20of%20requirements%20of%20the%20SQL%20standard>
///
/// ## Relationship with `ToString`/`Display`
///
/// For some types, the implementation diverge from Rust's standard `ToString`/`Display`,
/// to match PostgreSQL's representation.
///
/// ---
///
/// FIXME: `ToText` should depend on a lot of other stuff
/// but we have not implemented them yet: timezone, date style, interval style, bytea output, etc
pub trait ToText {
    /// Write the text to the writer *regardless* of its data type
    ///
    /// See `ToText::to_text` for more details.
    fn write<W: Write>(&self, f: &mut W) -> Result;

    /// Write the text to the writer according to its data type
    fn write_with_type<W: Write>(&self, _ty: &DataType, f: &mut W) -> Result;

    /// Convert to text according to its data type
    fn to_text_with_type(&self, ty: &DataType) -> String {
        let mut s = String::new();
        self.write_with_type(ty, &mut s).unwrap();
        s
    }

    /// `to_text` is a special version of `to_text_with_type`, it convert the scalar to default type
    /// text. E.g. for Int64, it will convert to text as a Int64 type.
    /// We should prefer to use `to_text_with_type` because it's more clear and readable.
    ///
    /// Note: currently the `DataType` param is actually unnecessary.
    /// Previously, Timestamptz is also represented as int64, and we need the data type to distinguish them.
    /// Now we have 1-1 mapping, and it happens to be the case that PostgreSQL default `ToText` format does
    /// not need additional metadata like field names contained in `DataType`.
    fn to_text(&self) -> String {
        let mut s = String::new();
        self.write(&mut s).unwrap();
        s
    }
}

macro_rules! implement_using_to_string {
    ($({ $scalar_type:ty , $data_type:ident} ),*) => {
        $(
            impl ToText for $scalar_type {
                fn write<W: Write>(&self, f: &mut W) -> Result {
                    write!(f, "{self}")
                }
                fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
                    match ty {
                        DataType::$data_type => self.write(f),
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
                fn write<W: Write>(&self, f: &mut W) -> Result {
                    write!(f, "{}", itoa::Buffer::new().format(*self))
                }
                fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
                    match ty {
                        DataType::$data_type => self.write(f),
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
    { i16, Int16 },
    { i32, Int32 },
    { i64, Int64 }
}

macro_rules! implement_using_ryu {
    ($({ $scalar_type:ty, $data_type:ident } ),*) => {
            $(
            impl ToText for $scalar_type {
                fn write<W: Write>(&self, f: &mut W) -> Result {
                    let inner = self.0;
                    match inner.classify() {
                        FpCategory::Infinite if inner.is_sign_negative() => write!(f, "-Infinity"),
                        FpCategory::Infinite => write!(f, "Infinity"),
                        FpCategory::Zero if inner.is_sign_negative() => write!(f, "-0"),
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
                fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
                    match ty {
                        DataType::$data_type => self.write(f),
                        _ => unreachable!(),
                    }
                }
            }
        )*
    };
}

implement_using_ryu! {
    { crate::types::F32, Float32 },
    { crate::types::F64, Float64 }
}

impl ToText for bool {
    fn write<W: Write>(&self, f: &mut W) -> Result {
        if *self {
            write!(f, "t")
        } else {
            write!(f, "f")
        }
    }

    fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
        match ty {
            DataType::Boolean => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl ToText for &[u8] {
    fn write<W: Write>(&self, f: &mut W) -> Result {
        write!(f, "\\x{}", hex::encode(self))
    }

    fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
        match ty {
            DataType::Bytea => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl ToText for ScalarRefImpl<'_> {
    fn write<W: Write>(&self, f: &mut W) -> Result {
        dispatch_scalar_ref_variants!(self, v, { v.write(f) })
    }

    fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
        dispatch_scalar_ref_variants!(self, v, { v.write_with_type(ty, f) })
    }
}

impl ToText for DatumRef<'_> {
    fn write<W: Write>(&self, f: &mut W) -> Result {
        match self {
            Some(data) => data.write(f),
            None => write!(f, "NULL"),
        }
    }

    fn write_with_type<W: Write>(&self, ty: &DataType, f: &mut W) -> Result {
        match self {
            Some(data) => data.write_with_type(ty, f),
            None => write!(f, "NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::ToText;
    use crate::types::ordered_float::OrderedFloat;

    #[test]
    fn test_float_to_text() {
        // f64 -> text.
        let ret: OrderedFloat<f64> = OrderedFloat::<f64>::from(1.234567890123456);
        tracing::info!("ret: {}", ret.to_text());
        assert_eq!("1.234567890123456".to_owned(), ret.to_text());

        // f32 -> text.
        let ret: OrderedFloat<f32> = OrderedFloat::<f32>::from(1.234567);
        assert_eq!("1.234567".to_owned(), ret.to_text());
    }
}
