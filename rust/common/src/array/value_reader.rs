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

use std::io::Cursor;
use std::str::{from_utf8, FromStr};

use byteorder::{BigEndian, ReadBytesExt};

use crate::array::{
    Array, ArrayBuilder, DecimalArrayBuilder, PrimitiveArrayItemType, Utf8ArrayBuilder,
};
use crate::error::ErrorCode::InternalError;
use crate::error::{ErrorCode, Result, RwError};
use crate::types::{Decimal, OrderedF32, OrderedF64};

/// Reads an encoded buffer into a value.
pub trait PrimitiveValueReader<T: PrimitiveArrayItemType> {
    fn read(cur: &mut Cursor<&[u8]>) -> Result<T>;
}

pub struct I16ValueReader {}
pub struct I32ValueReader {}
pub struct I64ValueReader {}
pub struct F32ValueReader {}
pub struct F64ValueReader {}

macro_rules! impl_numeric_value_reader {
    ($value_type:ty, $value_reader:ty, $read_fn:ident) => {
        impl PrimitiveValueReader<$value_type> for $value_reader {
            fn read(cur: &mut Cursor<&[u8]>) -> Result<$value_type> {
                cur.$read_fn::<BigEndian>().map(Into::into).map_err(|e| {
                    RwError::from(ErrorCode::InternalError(format!(
                        "Failed to read value from buffer: {}",
                        e
                    )))
                })
            }
        }
    };
}

impl_numeric_value_reader!(i16, I16ValueReader, read_i16);
impl_numeric_value_reader!(i32, I32ValueReader, read_i32);
impl_numeric_value_reader!(i64, I64ValueReader, read_i64);
impl_numeric_value_reader!(OrderedF32, F32ValueReader, read_f32);
impl_numeric_value_reader!(OrderedF64, F64ValueReader, read_f64);

pub trait VarSizedValueReader<AB: ArrayBuilder> {
    fn read(buf: &[u8]) -> Result<<<AB as ArrayBuilder>::ArrayType as Array>::RefItem<'_>>;
}

pub struct Utf8ValueReader {}

impl VarSizedValueReader<Utf8ArrayBuilder> for Utf8ValueReader {
    fn read(buf: &[u8]) -> Result<&str> {
        from_utf8(buf).map_err(|e| {
            RwError::from(InternalError(format!(
                "failed to read utf8 string from bytes: {}",
                e
            )))
        })
    }
}

pub struct DecimalValueReader {}

impl VarSizedValueReader<DecimalArrayBuilder> for DecimalValueReader {
    fn read(buf: &[u8]) -> Result<Decimal> {
        Decimal::from_str(Utf8ValueReader::read(buf)?).map_err(|e| {
            RwError::from(InternalError(format!(
                "failed to read decimal from string: {}",
                e
            )))
        })
    }
}
