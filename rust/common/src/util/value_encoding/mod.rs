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

//! Value encoding is an encoding format which converts the data into a binary form (not
//! memcomparable).

use bytes::Buf;
use serde::Deserialize;

use crate::error::{ErrorCode, Result, RwError};
use crate::types::{
    deserialize_datum_from, deserialize_datum_not_null_from, serialize_datum_into,
    serialize_datum_not_null_into, DataType, Datum, Decimal, ScalarImpl,
};

/// Serialize datum into cell bytes (Not order guarantee, used in value encoding).
pub fn serialize_cell(cell: &Datum) -> Result<Vec<u8>> {
    let mut serializer = value_encoding::Serializer::new(vec![]);
    if let Some(ScalarImpl::Decimal(decimal)) = cell {
        return serialize_decimal(decimal);
    }
    serialize_datum_into(cell, serializer.memcom_ser())?;
    Ok(serializer.into_inner())
}

/// Serialize datum cannot be null into cell bytes.
pub fn serialize_cell_not_null(cell: &Datum) -> Result<Vec<u8>> {
    let mut serializer = value_encoding::Serializer::new(vec![]);
    if let Some(ScalarImpl::Decimal(decimal)) = cell {
        return serialize_decimal(decimal);
    }
    serialize_datum_not_null_into(cell, serializer.memcom_ser())?;
    Ok(serializer.into_inner())
}

/// Deserialize cell bytes into datum (Not order guarantee, used in value decoding).
pub fn deserialize_cell(
    deserializer: &mut value_encoding::Deserializer<impl Buf>,
    ty: &DataType,
) -> Result<Datum> {
    match ty {
        &DataType::Decimal => deserialize_decimal(deserializer),
        _ => Ok(deserialize_datum_from(ty, deserializer.memcom_de())?),
    }
}

/// Deserialize cell bytes which cannot be null into datum.
pub fn deserialize_cell_not_null(
    deserializer: &mut value_encoding::Deserializer<impl Buf>,
    ty: DataType,
) -> Result<Datum> {
    match ty {
        DataType::Decimal => deserialize_decimal(deserializer),
        _ => Ok(deserialize_datum_not_null_from(
            ty,
            deserializer.memcom_de(),
        )?),
    }
}

fn serialize_decimal(decimal: &Decimal) -> Result<Vec<u8>> {
    let (mut mantissa, mut scale) = decimal.mantissa_scale_for_serialization();
    if mantissa < 0 {
        mantissa = -mantissa;
        // We use the most significant bit of `scale` to denote whether decimal is negative or not.
        scale += 1 << 7;
    }
    let mut byte_array = vec![1, scale];
    while mantissa != 0 {
        let byte = (mantissa % 100) as u8;
        byte_array.push(byte);
        mantissa /= 100;
    }
    // Add 100 marker for the end of decimal (cuz `byte` always can not be 100 in above loop).
    byte_array.push(100);
    Ok(byte_array)
}

fn deserialize_decimal(deserializer: &mut value_encoding::Deserializer<impl Buf>) -> Result<Datum> {
    // None denotes NULL which is a valid value while Err means invalid encoding.
    let null_tag = u8::deserialize(&mut *deserializer.memcom_de())?;
    match null_tag {
        0 => {
            return Ok(None);
        }
        1 => {}
        _ => {
            return Err(RwError::from(ErrorCode::InternalError(format!(
                "Invalid null tag: {}",
                null_tag
            ))));
        }
    }
    let bytes = deserializer.read_decimal_v2()?;
    let mut scale = bytes[0];
    let neg = if (scale & 1 << 7) > 0 {
        scale &= !(1 << 7);
        true
    } else {
        false
    };
    let mut mantissa: i128 = 0;
    for (exp, byte) in bytes.iter().skip(1).enumerate() {
        mantissa += (*byte as i128) * 100i128.pow(exp as u32);
    }
    if neg {
        mantissa = -mantissa;
    }
    Ok(Some(ScalarImpl::Decimal(Decimal::from_i128_with_scale(
        mantissa,
        scale as u32,
    ))))
}
