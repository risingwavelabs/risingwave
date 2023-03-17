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

//! Value encoding is an encoding format which converts the data into a binary form (not
//! memcomparable).

use std::marker::{Send, Sync};
use std::sync::Arc;

use bytes::{Buf, BufMut};
use chrono::{Datelike, Timelike};
use either::{for_both, Either};
use enum_as_inner::EnumAsInner;
use itertools::Itertools;

use crate::array::{serial_array, JsonbVal, ListRef, ListValue, StructRef, StructValue};
use crate::catalog::ColumnId;
use crate::row::{Row, RowDeserializer as BasicDeserializer};
use crate::types::struct_type::StructType;
use crate::types::{
    DataType, Datum, Decimal, IntervalUnit, NaiveDateTimeWrapper, NaiveDateWrapper,
    NaiveTimeWrapper, OrderedF32, OrderedF64, ScalarImpl, ScalarRefImpl, ToDatumRef,
};

pub mod error;
use error::ValueEncodingError;
use serial_array::Serial;

use self::column_aware_row_encoding::ColumnAwareSerde;
pub mod column_aware_row_encoding;

pub type Result<T> = std::result::Result<T, ValueEncodingError>;

/// The kind of all possible `ValueRowSerde`.
#[derive(EnumAsInner)]
pub enum ValueRowSerdeKind {
    /// For `BasicSerde`, the value is encoded with value-encoding.
    Basic,
    /// For `ColumnAwareSerde`, the value is encoded with column-aware row encoding.
    ColumnAware,
}

/// Part of `ValueRowSerde` that implements `serialize` a `Row` into bytes
pub trait ValueRowSerializer: Clone {
    fn serialize(&self, row: impl Row) -> Vec<u8>;
}

/// Part of `ValueRowSerde` that implements `deserialize` bytes into a `Row`
pub trait ValueRowDeserializer: Clone {
    fn deserialize(&self, encoded_bytes: &[u8]) -> Result<Vec<Datum>>;
}

/// Part of `ValueRowSerde` that implements `new` a serde given `column_ids` and `schema`
pub trait ValueRowSerdeNew: Clone {
    fn new(column_ids: &[ColumnId], schema: Arc<[DataType]>) -> Self;
}

/// The compound trait used in `StateTableInner`, implemented by `BasicSerde` and `ColumnAwareSerde`
pub trait ValueRowSerde:
    ValueRowSerializer + ValueRowDeserializer + ValueRowSerdeNew + Sync + Send + 'static
{
    fn kind(&self) -> ValueRowSerdeKind;
}

/// The type-erased `ValueRowSerde`, used for simplifying the code.
#[derive(Clone)]
pub struct EitherSerde(Either<BasicSerde, ColumnAwareSerde>);

impl From<BasicSerde> for EitherSerde {
    fn from(value: BasicSerde) -> Self {
        Self(Either::Left(value))
    }
}
impl From<ColumnAwareSerde> for EitherSerde {
    fn from(value: ColumnAwareSerde) -> Self {
        Self(Either::Right(value))
    }
}

impl ValueRowSerializer for EitherSerde {
    fn serialize(&self, row: impl Row) -> Vec<u8> {
        for_both!(&self.0, s => s.serialize(row))
    }
}

impl ValueRowDeserializer for EitherSerde {
    fn deserialize(&self, encoded_bytes: &[u8]) -> Result<Vec<Datum>> {
        for_both!(&self.0, s => s.deserialize(encoded_bytes))
    }
}

impl ValueRowSerdeNew for EitherSerde {
    fn new(_column_ids: &[ColumnId], _schema: Arc<[DataType]>) -> EitherSerde {
        unreachable!("should construct manually")
    }
}

impl ValueRowSerde for EitherSerde {
    fn kind(&self) -> ValueRowSerdeKind {
        for_both!(&self.0, s => s.kind())
    }
}

/// Wrap of the original `Row` serializing function
#[derive(Clone)]
pub struct BasicSerializer;

impl ValueRowSerializer for BasicSerializer {
    fn serialize(&self, row: impl Row) -> Vec<u8> {
        let mut buf = vec![];
        for datum in row.iter() {
            serialize_datum_into(datum, &mut buf);
        }
        buf
    }
}

impl ValueRowDeserializer for BasicDeserializer {
    fn deserialize(&self, encoded_bytes: &[u8]) -> Result<Vec<Datum>> {
        Ok(self.deserialize(encoded_bytes)?.into_inner())
    }
}

/// Wrap of the original `Row` serializing and deserializing function
#[derive(Clone)]
pub struct BasicSerde {
    serializer: BasicSerializer,
    deserializer: BasicDeserializer,
}

impl ValueRowSerdeNew for BasicSerde {
    fn new(_column_ids: &[ColumnId], schema: Arc<[DataType]>) -> BasicSerde {
        BasicSerde {
            serializer: BasicSerializer {},
            deserializer: BasicDeserializer::new(schema.as_ref().to_owned()),
        }
    }
}

impl ValueRowSerializer for BasicSerde {
    fn serialize(&self, row: impl Row) -> Vec<u8> {
        self.serializer.serialize(row)
    }
}

impl ValueRowDeserializer for BasicSerde {
    fn deserialize(&self, encoded_bytes: &[u8]) -> Result<Vec<Datum>> {
        Ok(self.deserializer.deserialize(encoded_bytes)?.into_inner())
    }
}

impl ValueRowSerde for BasicSerde {
    fn kind(&self) -> ValueRowSerdeKind {
        ValueRowSerdeKind::Basic
    }
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
        ScalarRefImpl::Serial(v) => buf.put_i64_le(v.into_inner()),
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
        ScalarRefImpl::Jsonb(v) => serialize_str(&v.value_serialize(), buf),
        ScalarRefImpl::Struct(s) => serialize_struct(s, buf),
        ScalarRefImpl::List(v) => serialize_list(v, buf),
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
    buf.put_i64_le(interval.get_usecs());
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
        DataType::Serial => ScalarImpl::Serial(Serial::from(data.get_i64_le())),
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
        DataType::Jsonb => ScalarImpl::Jsonb(
            JsonbVal::value_deserialize(&deserialize_bytea(data))
                .ok_or(ValueEncodingError::InvalidJsonbEncoding)?,
        ),
        DataType::Struct(struct_def) => deserialize_struct(struct_def, data)?,
        DataType::Bytea => ScalarImpl::Bytea(deserialize_bytea(data).into()),
        DataType::List {
            datatype: item_type,
        } => deserialize_list(item_type, data)?,
    })
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
    let usecs = data.get_i64_le();
    Ok(IntervalUnit::from_month_day_usec(months, days, usecs))
}

fn deserialize_naivetime(data: &mut impl Buf) -> Result<NaiveTimeWrapper> {
    let secs = data.get_u32_le();
    let nano = data.get_u32_le();
    NaiveTimeWrapper::with_secs_nano(secs, nano)
        .map_err(|_e| ValueEncodingError::InvalidNaiveTimeEncoding(secs, nano))
}

fn deserialize_naivedatetime(data: &mut impl Buf) -> Result<NaiveDateTimeWrapper> {
    let secs = data.get_i64_le();
    let nsecs = data.get_u32_le();
    NaiveDateTimeWrapper::with_secs_nsecs(secs, nsecs)
        .map_err(|_e| ValueEncodingError::InvalidNaiveDateTimeEncoding(secs, nsecs))
}

fn deserialize_naivedate(data: &mut impl Buf) -> Result<NaiveDateWrapper> {
    let days = data.get_i32_le();
    NaiveDateWrapper::with_days(days)
        .map_err(|_e| ValueEncodingError::InvalidNaiveDateEncoding(days))
}

fn deserialize_decimal(data: &mut impl Buf) -> Result<Decimal> {
    let mut bytes = [0; 16];
    data.copy_to_slice(&mut bytes);
    Ok(Decimal::unordered_deserialize(bytes))
}
