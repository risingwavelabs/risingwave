// Copyright 2023 Singularity Data
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

//! Value encoding is an encoding format which converts the data into a binary form (not
//! memcomparable).

use std::collections::HashMap;

use bytes::{Buf, BufMut};
use chrono::{Datelike, Timelike};
use itertools::Itertools;

use crate::array::{ListRef, ListValue, StructRef, StructValue};
use crate::catalog::ColumnId;
use crate::row::Row;
use crate::types::struct_type::StructType;
use crate::types::{
    DataType, Datum, Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper,
    NaiveTimeWrapper, OrderedF32, OrderedF64, ScalarImpl, ScalarRefImpl, ToDatumRef,
};

pub mod error;
use error::ValueEncodingError;

pub type Result<T> = std::result::Result<T, ValueEncodingError>;

#[derive(Clone, Copy)]
enum Width {
    Mid(u8),
    Large(u16),
    Extra(u32),
}

enum Offset {
    Mid(Vec<u8>),
    Large(Vec<u16>),
    Extra(Vec<u32>),
}

pub struct RowEncoding {
    flag: u8,
    non_null_datum_nums: Width,
    non_null_column_ids: Vec<ColumnId>,
    offsets: Offset,
    buf: Vec<u8>,
}

impl RowEncoding {
    pub fn new() -> Self {
        RowEncoding {
            flag: 0b_10000000,
            non_null_datum_nums: Width::Mid(0),
            non_null_column_ids: vec![],
            offsets: Offset::Mid(vec![]),
            buf: vec![],
        }
    }

    pub fn set_width(&mut self, non_null_column_nums: usize) {
        self.non_null_datum_nums = match non_null_column_nums {
            n if n <= u8::MAX as usize => {
                self.flag |= 0b0100;
                Width::Mid(n as u8)
            }
            n if n <= u16::MAX as usize => {
                self.flag |= 0b1000;
                Width::Large(n as u16)
            }
            n if n <= u32::MAX as usize => {
                self.flag |= 0b1100;
                Width::Extra(n as u32)
            }
            _ => unreachable!("the number of columns exceeds u32"),
        }
    }

    pub fn set_big(&mut self, maybe_offset: Vec<usize>, max_offset: usize) {
        self.offsets = match max_offset {
            n if n <= u8::MAX as usize => {
                self.flag |= 0b01;
                Offset::Mid(maybe_offset.into_iter().map(|m| m as u8).collect_vec())
            }
            n if n <= u16::MAX as usize => {
                self.flag |= 0b10;
                Offset::Large(maybe_offset.into_iter().map(|m| m as u16).collect_vec())
            }
            n if n <= u32::MAX as usize => {
                self.flag |= 0b11;
                Offset::Extra(maybe_offset.into_iter().map(|m| m as u32).collect_vec())
            }
            _ => unreachable!("encoding length exceeds u32"),
        }
    }

    pub fn encode(
        &mut self,
        datum_refs: impl Iterator<Item = impl ToDatumRef>,
        column_ids: Vec<ColumnId>,
    ) {
        assert!(
            self.buf.is_empty() && self.non_null_column_ids.is_empty(),
            "should not encode one RowEncoding object multiple times."
        );
        let mut maybe_offset = vec![];
        for (datum, &id) in datum_refs.zip_eq(column_ids.iter()) {
            if let Some(v) = datum.to_datum_ref() {
                maybe_offset.push(self.buf.len());
                self.non_null_column_ids.push(id);
                serialize_scalar(v, &mut self.buf);
            }            
        }
        let max_offset = *maybe_offset
            .last()
            .expect("should encode at least one column");
        self.set_width(self.non_null_column_ids.len());
        self.set_big(maybe_offset, max_offset);
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut row_bytes = vec![];
        row_bytes.put_u8(self.flag);
        serialize_width(self.non_null_datum_nums, &mut row_bytes);
        for id in self.non_null_column_ids.iter() {
            row_bytes.put_i32_le(id.get_id());
        }
        serialize_offsets(&self.offsets, &mut row_bytes);
        row_bytes.extend(self.buf.iter());

        row_bytes
    }
}

pub fn serialize_row_column_aware(column_ids: Vec<ColumnId>, row: impl Row) -> Vec<u8> {
    let encoding = encode_datums(column_ids, row.iter());
    encoding.serialize()
}

pub fn encode_datums(
    column_ids: Vec<ColumnId>,
    datum_refs: impl Iterator<Item = impl ToDatumRef>,
) -> RowEncoding {
    let mut encoding = RowEncoding::new();
    // encoding.update_non_null_datum_nums(column_ids);
    encoding.encode(datum_refs, column_ids);
    encoding
}

pub fn decode(
    need_columns: &[ColumnId],
    schema: &[DataType],
    mut encoded_bytes: &[u8],
) -> Vec<Datum> {
    let flag = encoded_bytes.get_u8();
    let nums_bytes = match flag & 0b1100 {
        0b0100 => 1,
        0b1000 => 2,
        0b1100 => 4,
        _ => unreachable!("flag's WW bits corrupted"),
    };
    let offset_bytes = match flag & 0b11 {
        0b01 => 1,
        0b10 => 2,
        0b11 => 4,
        _ => unreachable!("flag's BB bits corrupted"),
    };
    let non_null_datum_nums = deserialize_width(nums_bytes, &mut encoded_bytes);
    let id_to_index = (0..non_null_datum_nums)
        .into_iter()
        .fold(HashMap::new(), |mut map, i| {
            map.insert(ColumnId::new(encoded_bytes.get_i32_le()), i);
            map
        });
    // let column_id_start_idx = 1 + nums_bytes;
    let offsets_start_idx = 0;
    // let mut one_offset = &encoded_bytes[offsets_start_idx..offsets_start_idx+4];
    // let mut another_offset = &encoded_bytes[offsets_start_idx+4..offsets_start_idx+8];
    // let one_offset_val = one_offset.get_i32_le();
    let data_start_idx = offsets_start_idx + non_null_datum_nums * offset_bytes;
    let mut datums = vec![];
    datums.reserve(schema.len());
    for (id, datatype) in need_columns.iter().zip_eq(schema.iter()) {
        if let Some(&index) = id_to_index.get(id) {
            // let base_offset_idx = offsets_start_idx + 4 * index;
            let mut this_offset_slice = &encoded_bytes[index..(index + offset_bytes)];
            let this_offset = deserialize_width(offset_bytes, &mut this_offset_slice);
            let data = if index + 1 < data_start_idx {
                let mut next_offset_slice =
                    &encoded_bytes[(index + offset_bytes)..(index + 2 * offset_bytes)];
                let next_offset = deserialize_width(offset_bytes, &mut next_offset_slice);
                let mut data_slice = &encoded_bytes
                    [(data_start_idx + this_offset)..(data_start_idx + next_offset)];
                deserialize_value(datatype, &mut data_slice)
            } else {
                let mut data_slice = &encoded_bytes[(data_start_idx + this_offset)..];
                deserialize_value(datatype, &mut data_slice)
            };
            if let Ok(d) = data {
                datums.push(Some(d));
            } else {
                unreachable!("decode error");
            }
        } else {
            datums.push(None);
        }
    }

    datums
}

/// Serialize a datum into bytes and return (Not order guarantee, used in value encoding).
pub fn serialize_datum(cell: impl ToDatumRef) -> Vec<u8> {
    let mut buf: Vec<u8> = vec![];
    serialize_datum_into(cell, &mut buf);
    buf
}

/// Serialize a datum into bytes (Not order guarantee, used in value encoding).
pub fn serialize_datum_into(datum_ref: impl ToDatumRef, buf: &mut impl BufMut) {
    if let Some(d) = datum_ref.to_datum_ref() {
        buf.put_u8(1);
        serialize_scalar(d, buf)
    } else {
        buf.put_u8(0);
    }
}

/// Deserialize bytes into a datum (Not order guarantee, used in value encoding).
pub fn deserialize_datum(mut data: impl Buf, ty: &DataType) -> Result<Datum> {
    inner_deserialize_datum(&mut data, ty)
}

// prevent recursive use of &mut
#[inline(always)]
fn inner_deserialize_datum(data: &mut impl Buf, ty: &DataType) -> Result<Datum> {
    let null_tag = data.get_u8();
    match null_tag {
        0 => Ok(None),
        1 => Some(deserialize_value(ty, data)).transpose(),
        _ => Err(ValueEncodingError::InvalidTagEncoding(null_tag)),
    }
}

fn serialize_scalar(value: ScalarRefImpl<'_>, buf: &mut impl BufMut) {
    match value {
        ScalarRefImpl::Int16(v) => buf.put_i16_le(v),
        ScalarRefImpl::Int32(v) => buf.put_i32_le(v),
        ScalarRefImpl::Int64(v) => buf.put_i64_le(v),
        ScalarRefImpl::Float32(v) => buf.put_f32_le(v.into_inner()),
        ScalarRefImpl::Float64(v) => buf.put_f64_le(v.into_inner()),
        ScalarRefImpl::Utf8(v) => serialize_str(v.as_bytes(), buf),
        ScalarRefImpl::Bytea(v) => serialize_str(v, buf),
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
        ScalarRefImpl::Struct(s) => serialize_struct(s, buf),
        ScalarRefImpl::List(v) => serialize_list(v, buf),
    }
}

fn serialize_width(width: Width, buf: &mut impl BufMut) {
    match width {
        Width::Mid(w) => buf.put_u8(w),
        Width::Large(w) => buf.put_u16_le(w),
        Width::Extra(w) => buf.put_u32_le(w),
    }
}

fn serialize_offsets(offset: &Offset, buf: &mut impl BufMut) {
    match offset {
        Offset::Mid(v) => {
            for offset in v.iter() {
                buf.put_u8(*offset);
            }
        }
        Offset::Large(v) => {
            for offset in v.iter() {
                buf.put_u16_le(*offset);
            }
        }
        Offset::Extra(v) => {
            for offset in v.iter() {
                buf.put_u32_le(*offset);
            }
        }
    }
}

fn serialize_struct(value: StructRef<'_>, buf: &mut impl BufMut) {
    value
        .fields_ref()
        .iter()
        .map(|field_value| {
            serialize_datum_into(*field_value, buf);
        })
        .collect_vec();
}

fn serialize_list(value: ListRef<'_>, buf: &mut impl BufMut) {
    let values_ref = value.values_ref();
    buf.put_u32_le(values_ref.len() as u32);

    values_ref
        .iter()
        .map(|field_value| {
            serialize_datum_into(*field_value, buf);
        })
        .collect_vec();
}

fn serialize_str(bytes: &[u8], buf: &mut impl BufMut) {
    buf.put_u32_le(bytes.len() as u32);
    buf.put_slice(bytes);
}

fn serialize_interval(interval: &IntervalUnit, buf: &mut impl BufMut) {
    buf.put_i32_le(interval.get_months());
    buf.put_i32_le(interval.get_days());
    buf.put_i64_le(interval.get_ms());
}

fn serialize_naivedate(days: i32, buf: &mut impl BufMut) {
    buf.put_i32_le(days);
}

fn serialize_naivedatetime(secs: i64, nsecs: u32, buf: &mut impl BufMut) {
    buf.put_i64_le(secs);
    buf.put_u32_le(nsecs);
}

fn serialize_naivetime(secs: u32, nano: u32, buf: &mut impl BufMut) {
    buf.put_u32_le(secs);
    buf.put_u32_le(nano);
}

fn serialize_decimal(decimal: &Decimal, buf: &mut impl BufMut) {
    buf.put_slice(&decimal.unordered_serialize());
}

fn deserialize_value(ty: &DataType, data: &mut impl Buf) -> Result<ScalarImpl> {
    Ok(match ty {
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
        DataType::Timestamptz => ScalarImpl::Int64(data.get_i64_le()),
        DataType::Date => ScalarImpl::NaiveDate(deserialize_naivedate(data)?),
        DataType::Struct(struct_def) => deserialize_struct(struct_def, data)?,
        DataType::Bytea => ScalarImpl::Bytea(deserialize_bytea(data).into()),
        DataType::List {
            datatype: item_type,
        } => deserialize_list(item_type, data)?,
    })
}

fn deserialize_width(len: usize, data: &mut impl Buf) -> usize {
    match len {
        1 => data.get_u8() as usize,
        2 => data.get_u16_le() as usize,
        4 => data.get_u32_le() as usize,
        _ => unreachable!("Width's len should be either 1, 2, or 4"),
    }
}

fn deserialize_struct(struct_def: &StructType, data: &mut impl Buf) -> Result<ScalarImpl> {
    let num_fields = struct_def.fields.len();
    let mut field_values = Vec::with_capacity(num_fields);
    for field_type in &struct_def.fields {
        field_values.push(inner_deserialize_datum(data, field_type)?);
    }

    Ok(ScalarImpl::Struct(StructValue::new(field_values)))
}

fn deserialize_list(item_type: &DataType, data: &mut impl Buf) -> Result<ScalarImpl> {
    let len = data.get_u32_le();
    let mut values = Vec::with_capacity(len as usize);
    for _ in 0..len {
        values.push(inner_deserialize_datum(data, item_type)?);
    }
    Ok(ScalarImpl::List(ListValue::new(values)))
}

fn deserialize_str(data: &mut impl Buf) -> Result<Box<str>> {
    let len = data.get_u32_le();
    let mut bytes = vec![0; len as usize];
    data.copy_to_slice(&mut bytes);
    String::from_utf8(bytes)
        .map(String::into_boxed_str)
        .map_err(ValueEncodingError::InvalidUtf8)
}

fn deserialize_bytea(data: &mut impl Buf) -> Vec<u8> {
    let len = data.get_u32_le();
    let mut bytes = vec![0; len as usize];
    data.copy_to_slice(&mut bytes);
    bytes
}

fn deserialize_bool(data: &mut impl Buf) -> Result<bool> {
    match data.get_u8() {
        1 => Ok(true),
        0 => Ok(false),
        value => Err(ValueEncodingError::InvalidBoolEncoding(value)),
    }
}

fn deserialize_interval(data: &mut impl Buf) -> Result<IntervalUnit> {
    let months = data.get_i32_le();
    let days = data.get_i32_le();
    let ms = data.get_i64_le();
    Ok(IntervalUnit::new(months, days, ms))
}

fn deserialize_naivetime(data: &mut impl Buf) -> Result<NaiveTimeWrapper> {
    let secs = data.get_u32_le();
    let nano = data.get_u32_le();
    NaiveTimeWrapper::with_secs_nano_value(secs, nano)
}

fn deserialize_naivedatetime(data: &mut impl Buf) -> Result<NaiveDateTimeWrapper> {
    let secs = data.get_i64_le();
    let nsecs = data.get_u32_le();
    NaiveDateTimeWrapper::with_secs_nsecs_value(secs, nsecs)
}

fn deserialize_naivedate(data: &mut impl Buf) -> Result<NaiveDateWrapper> {
    let days = data.get_i32_le();
    NaiveDateWrapper::with_days_value(days)
}

fn deserialize_decimal(data: &mut impl Buf) -> Result<Decimal> {
    let mut bytes = [0; 16];
    data.copy_to_slice(&mut bytes);
    Ok(Decimal::unordered_deserialize(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnId;
    use crate::row::OwnedRow;
    use crate::types::ScalarImpl::*;

    #[test]
    fn test_row_encoding() {
        let column_ids = vec![ColumnId::new(0), ColumnId::new(1)];
        let row1 = OwnedRow::new(vec![Some(Int16(5)), Some(Utf8("abc".into()))]);
        let row2 = OwnedRow::new(vec![Some(Int16(5)), Some(Utf8("abd".into()))]);
        let row3 = OwnedRow::new(vec![Some(Int16(6)), Some(Utf8("abc".into()))]);
        let rows = vec![row1, row2, row3];
        let mut array = vec![];
        for row in &rows {
            let row_bytes = serialize_row_column_aware(column_ids.clone(), row);
            array.push(row_bytes);
        }
        let zero_le_bytes = 0_i32.to_le_bytes();
        let one_le_bytes = 1_i32.to_le_bytes();
        assert_eq!(
            array[0],
            [
                0b10000101,       // flag mid WW mid BB
                2,                // column nums
                zero_le_bytes[0], // start id 0
                zero_le_bytes[1],
                zero_le_bytes[2],
                zero_le_bytes[3],
                one_le_bytes[0], // start id 1
                one_le_bytes[1],
                one_le_bytes[2],
                one_le_bytes[3],
                0, // offset0: 0
                3, // offset1: 3
                1, // i16: 5
                5,
                0, // str: abc
                1,
                3,
                0,
                0,
                0,
                b'a',
                b'b',
                b'c'
            ]
        );
    }
    #[test]
    fn test_row_decoding() {
        let column_ids = vec![ColumnId::new(0), ColumnId::new(1)];
        let row1 = OwnedRow::new(vec![Some(Int16(5)), Some(Utf8("abc".into()))]);
        let mut row_bytes = serialize_row_column_aware(column_ids.clone(), row1.clone());
        let data_types = vec![DataType::Int16, DataType::Varchar];
        let decoded = decode(&column_ids[..], &data_types[..], &mut row_bytes[..]);
        assert_eq!(decoded, vec![Some(Int16(5)), Some(Utf8("abc".into()))]);
        let data_types1 = vec![
            DataType::Varchar,
            DataType::Int16,
            DataType::Date,
        ];
        let mut row_bytes1 = serialize_row_column_aware(column_ids.clone(), row1);
        let decoded1 = decode(
            &[
                ColumnId::new(1),
                ColumnId::new(5),
                ColumnId::new(6),
            ],
            &data_types1[..],
            &mut row_bytes1[..],
        );
        assert_eq!(
            decoded1,
            vec![Some(Utf8("abc".into())), None, None]
        );
    }
}
