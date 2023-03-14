// Copyright 2023 RisingWave Labs
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

use std::io::Cursor;
use std::str::from_utf8;

use byteorder::{BigEndian, ReadBytesExt};

use super::ArrayResult;
use crate::array::{
    ArrayBuilder, BytesArrayBuilder, JsonbArrayBuilder, PrimitiveArrayItemType, Serial,
    Utf8ArrayBuilder,
};
use crate::types::{Decimal, OrderedF32, OrderedF64};

/// Reads an encoded buffer into a value.
pub trait PrimitiveValueReader<T: PrimitiveArrayItemType> {
    fn read(cur: &mut Cursor<&[u8]>) -> ArrayResult<T>;
}

pub struct I16ValueReader;
pub struct I32ValueReader;
pub struct I64ValueReader;
pub struct SerialValueReader;
pub struct F32ValueReader;
pub struct F64ValueReader;

macro_rules! impl_numeric_value_reader {
    ($value_type:ty, $value_reader:ty, $read_fn:ident) => {
        impl PrimitiveValueReader<$value_type> for $value_reader {
            fn read(cur: &mut Cursor<&[u8]>) -> ArrayResult<$value_type> {
                match cur.$read_fn::<BigEndian>() {
                    Ok(v) => Ok(v.into()),
                    Err(e) => bail!("Failed to read value from buffer: {}", e),
                }
            }
        }
    };
}

impl_numeric_value_reader!(i16, I16ValueReader, read_i16);
impl_numeric_value_reader!(i32, I32ValueReader, read_i32);
impl_numeric_value_reader!(i64, I64ValueReader, read_i64);
impl_numeric_value_reader!(Serial, SerialValueReader, read_i64);
impl_numeric_value_reader!(OrderedF32, F32ValueReader, read_f32);
impl_numeric_value_reader!(OrderedF64, F64ValueReader, read_f64);

pub struct DecimalValueReader;

impl PrimitiveValueReader<Decimal> for DecimalValueReader {
    fn read(cur: &mut Cursor<&[u8]>) -> ArrayResult<Decimal> {
        Decimal::from_protobuf(cur)
    }
}

pub trait VarSizedValueReader<AB: ArrayBuilder> {
    fn read(buf: &[u8], builder: &mut AB) -> ArrayResult<()>;
}

pub struct Utf8ValueReader;

impl VarSizedValueReader<Utf8ArrayBuilder> for Utf8ValueReader {
    fn read(buf: &[u8], builder: &mut Utf8ArrayBuilder) -> ArrayResult<()> {
        let s = match from_utf8(buf) {
            Ok(s) => s,
            Err(e) => bail!("failed to read utf8 string from bytes: {}", e),
        };
        builder.append(Some(s));
        Ok(())
    }
}

pub struct BytesValueReader;

impl VarSizedValueReader<BytesArrayBuilder> for BytesValueReader {
    fn read(buf: &[u8], builder: &mut BytesArrayBuilder) -> ArrayResult<()> {
        builder.append(Some(buf));
        Ok(())
    }
}

pub struct JsonbValueReader;

impl VarSizedValueReader<JsonbArrayBuilder> for JsonbValueReader {
    fn read(buf: &[u8], builder: &mut JsonbArrayBuilder) -> ArrayResult<()> {
        let Some(v) = super::JsonbVal::value_deserialize(buf) else {
            bail!("failed to read jsonb from bytes");
        };
        builder.append_move(v);
        Ok(())
    }
}
