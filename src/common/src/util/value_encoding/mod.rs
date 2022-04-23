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

use bytes::{Buf, BufMut};
use chrono::{Datelike, Timelike};
use memcomparable;

use crate::error::{ErrorCode, Result, RwError};
use crate::types::{
    DataType, Datum, Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper,
    NaiveTimeWrapper, OrderedF32, OrderedF64, ScalarImpl, ScalarRefImpl,
};

/// Serialize datum into cell bytes (Not order guarantee, used in value encoding).
pub fn serialize_cell(cell: &Datum) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = vec![];
    if let Some(datum) = cell {
        buf.put_u8(1);
        serialize_datum(datum.as_scalar_ref_impl(), &mut buf)
    } else {
        buf.put_u8(0);
    }
    Ok(buf)
}

/// Serialize datum cannot be null into cell bytes.
pub fn serialize_cell_not_null(cell: &Datum) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = vec![];
    serialize_datum(
        cell.as_ref()
            .expect("datum cannot be null")
            .as_scalar_ref_impl(),
        &mut buf,
    );
    Ok(buf)
}

/// Deserialize cell bytes into datum (Not order guarantee, used in value decoding).
pub fn deserialize_cell<B: Buf>(data: &mut B, ty: &DataType) -> Result<Datum> {
    let null_tag = data.get_u8();
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
    Ok(deserialize_datum(ty, data)?)
}

/// Deserialize cell bytes which cannot be null into datum.
pub fn deserialize_cell_not_null<B: Buf>(data: &mut B, ty: &DataType) -> Result<Datum> {
    Ok(deserialize_datum(ty, data)?)
}

fn serialize_datum(value: ScalarRefImpl, buf: &mut Vec<u8>) {
    match value {
        ScalarRefImpl::Int16(v) => buf.put_i16_le(v),
        ScalarRefImpl::Int32(v) => buf.put_i32_le(v),
        ScalarRefImpl::Int64(v) => buf.put_i64_le(v),
        ScalarRefImpl::Float32(v) => buf.put_f32_le(v.into_inner()),
        ScalarRefImpl::Float64(v) => buf.put_f64_le(v.into_inner()),
        ScalarRefImpl::Utf8(v) => serialize_str(v.as_bytes(), buf),
        ScalarRefImpl::Bool(v) => buf.put_u8(v as u8),
        ScalarRefImpl::Decimal(v) => serialize_decimal(&v, buf),
        ScalarRefImpl::Interval(v) => serialize_interval(&v, buf),
        ScalarRefImpl::NaiveDate(v) => serialize_naivedate(v.0.num_days_from_ce(), buf),
        ScalarRefImpl::NaiveDateTime(v) => {
            serialize_naivedatetime(v.0.timestamp(), v.0.timestamp_subsec_nanos(), buf)
        }
        ScalarRefImpl::NaiveTime(v) => {
            serialize_naivetime(v.0.num_seconds_from_midnight(), v.0.nanosecond(), buf)
        }
        _ => {
            panic!("Type is unable to be serialized.")
        }
    }
}

fn serialize_str(bytes: &[u8], buf: &mut Vec<u8>) {
    buf.put_u32_le(bytes.len() as u32);
    buf.put_slice(bytes);
}

fn serialize_interval(interval: &IntervalUnit, buf: &mut Vec<u8>) {
    buf.put_i32_le(interval.get_months());
    buf.put_i32_le(interval.get_days());
    buf.put_i64_le(interval.get_ms());
}

fn serialize_naivedate(days: i32, buf: &mut Vec<u8>) {
    buf.put_i32_le(days);
}

fn serialize_naivedatetime(secs: i64, nsecs: u32, buf: &mut Vec<u8>) {
    buf.put_i64_le(secs);
    buf.put_u32_le(nsecs);
}

fn serialize_naivetime(secs: u32, nano: u32, buf: &mut Vec<u8>) {
    buf.put_u32_le(secs);
    buf.put_u32_le(nano);
}

fn deserialize_datum<B: Buf>(ty: &DataType, data: &mut B) -> memcomparable::Result<Datum> {
    Ok(Some(match *ty {
        DataType::Int16 => ScalarImpl::Int16(data.get_i16_le()),
        DataType::Int32 => ScalarImpl::Int32(data.get_i32_le()),
        DataType::Int64 => ScalarImpl::Int64(data.get_i64_le()),
        DataType::Float32 => ScalarImpl::Float32(OrderedF32::from(data.get_f32_le())),
        DataType::Float64 => ScalarImpl::Float64(OrderedF64::from(data.get_f64_le())),
        DataType::Varchar => ScalarImpl::Utf8(deserialize_str(data)?),
        DataType::Boolean => ScalarImpl::Bool(deserialize_bool(data)?),
        DataType::Decimal => ScalarImpl::Decimal(deserialize_decimal(data)?),
        DataType::Interval => ScalarImpl::Interval(deserialize_interval(data)?),
        DataType::Time => ScalarImpl::NaiveTime(deserialize_naivetime(data)?),
        DataType::Timestamp => ScalarImpl::NaiveDateTime(deserialize_naivedatetime(data)?),
        DataType::Timestampz => ScalarImpl::Int64(data.get_i64_le()),
        DataType::Date => ScalarImpl::NaiveDate(deserialize_naivedate(data)?),
        _ => {
            panic!("Type is unable to be deserialized.")
        }
    }))
}

fn deserialize_str<B: Buf>(data: &mut B) -> memcomparable::Result<String> {
    let len = data.get_u32_le();
    let mut bytes = vec![0; len as usize];
    data.copy_to_slice(&mut bytes);
    Ok(String::from_utf8(bytes)?)
}

fn deserialize_bool<B: Buf>(data: &mut B) -> memcomparable::Result<bool> {
    match data.get_u8() {
        1 => Ok(true),
        0 => Ok(false),
        value => Err(memcomparable::Error::InvalidBoolEncoding(value)),
    }
}

fn deserialize_interval<B: Buf>(data: &mut B) -> memcomparable::Result<IntervalUnit> {
    let months = data.get_i32_le();
    let days = data.get_i32_le();
    let ms = data.get_i64_le();
    Ok(IntervalUnit::new(months, days, ms))
}

fn deserialize_naivetime<B: Buf>(data: &mut B) -> memcomparable::Result<NaiveTimeWrapper> {
    let secs = data.get_u32_le();
    let nano = data.get_u32_le();
    NaiveTimeWrapper::new_with_secs_nano(secs, nano)
}

fn deserialize_naivedatetime<B: Buf>(data: &mut B) -> memcomparable::Result<NaiveDateTimeWrapper> {
    let secs = data.get_i64_le();
    let nsecs = data.get_u32_le();
    NaiveDateTimeWrapper::new_with_secs_nsecs(secs, nsecs)
}

fn deserialize_naivedate<B: Buf>(data: &mut B) -> memcomparable::Result<NaiveDateWrapper> {
    let days = data.get_i32_le();
    NaiveDateWrapper::new_with_days(days)
}

fn serialize_decimal(decimal: &Decimal, byte_array: &mut Vec<u8>) {
    let (mut mantissa, mut scale) = decimal.mantissa_scale_for_serialization();
    if mantissa < 0 {
        mantissa = -mantissa;
        // We use the most significant bit of `scale` to denote whether decimal is negative or not.
        scale += 1 << 7;
    }
    byte_array.push(scale);
    while mantissa != 0 {
        let byte = (mantissa % 100) as u8;
        byte_array.push(byte);
        mantissa /= 100;
    }
    // Add 100 marker for the end of decimal (cuz `byte` always can not be 100 in above loop).
    byte_array.push(100);
}

fn read_decimal<B: Buf>(data: &mut B) -> memcomparable::Result<Vec<u8>> {
    let flag = data.get_u8();
    let mut byte_array = vec![flag];
    loop {
        let byte = data.get_u8();
        if byte == 100 {
            break;
        }
        byte_array.push(byte);
    }
    Ok(byte_array)
}

fn deserialize_decimal<B: Buf>(data: &mut B) -> memcomparable::Result<Decimal> {
    let bytes = read_decimal(data)?;
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
    Ok(Decimal::from_i128_with_scale(mantissa, scale as u32))
}
