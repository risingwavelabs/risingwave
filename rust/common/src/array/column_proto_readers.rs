use std::io::{Cursor, Read};

use byteorder::{BigEndian, ReadBytesExt};
use paste::paste;
use risingwave_pb::data::Array as ProstArray;

use crate::array::value_reader::{PrimitiveValueReader, VarSizedValueReader};
use crate::array::{
    ArrayBuilder, ArrayImpl, ArrayMeta, BoolArrayBuilder, NaiveDateArrayBuilder,
    NaiveDateTimeArrayBuilder, NaiveTimeArrayBuilder, PrimitiveArrayBuilder,
    PrimitiveArrayItemType};
use crate::buffer::Bitmap;
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::types::{NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper};

// TODO: Use techniques like apache arrow flight RPC to eliminate deserialization.
// https://arrow.apache.org/docs/format/Flight.html

pub fn read_numeric_array<T: PrimitiveArrayItemType, R: PrimitiveValueReader<T>>(
    array: &ProstArray,
    cardinality: usize,
) -> Result<ArrayImpl> {
    ensure!(
        array.get_values().len() == 1,
        "Must have only 1 buffer in a numeric array"
    );

    let buf = array.get_values()[0].get_body().as_slice();
    let value_size = std::mem::size_of::<T>();
    ensure!(
        buf.len() % value_size == 0,
        "Unexpected memory layout of numeric array"
    );

    let mut builder = PrimitiveArrayBuilder::<T>::new(cardinality)?;
    let bitmap: Bitmap = array.get_null_bitmap()?.try_into()?;
    let mut cursor = Cursor::new(buf);
    for not_null in bitmap.iter() {
        if not_null {
            let v = R::read(&mut cursor)?;
            builder.append(Some(v))?;
        } else {
            builder.append(None)?;
        }
    }
    let arr = builder.finish()?;
    Ok(arr.into())
}

fn read_bool(cursor: &mut Cursor<&[u8]>) -> Result<bool> {
    let v = cursor
        .read_u8()
        .map_err(|e| InternalError(format!("Failed to read u8 from bool buffer: {}", e)))?;
    Ok(v != 0)
}

fn read_naivedate(cursor: &mut Cursor<&[u8]>) -> Result<NaiveDateWrapper> {
    match cursor.read_i32::<BigEndian>() {
        Ok(days) => NaiveDateWrapper::from_protobuf(days),
        Err(e) => Err(RwError::from(InternalError(format!(
            "Failed to read i32 from NaiveDate buffer: {}",
            e
        )))),
    }
}

fn read_naivetime(cursor: &mut Cursor<&[u8]>) -> Result<NaiveTimeWrapper> {
    match cursor.read_i64::<BigEndian>() {
        Ok(t) => NaiveTimeWrapper::from_protobuf(t),
        Err(e) => Err(RwError::from(InternalError(format!(
            "Failed to read i64 from NaiveTime buffer: {}",
            e
        )))),
    }
}

fn read_naivedatetime(cursor: &mut Cursor<&[u8]>) -> Result<NaiveDateTimeWrapper> {
    match cursor.read_i64::<BigEndian>() {
        Ok(t) => NaiveDateTimeWrapper::from_protobuf(t),
        Err(e) => Err(RwError::from(InternalError(format!(
            "Failed to read i64 from NaiveDateTime buffer: {}",
            e
        )))),
    }
}

macro_rules! read_one_value_array {
    ($({ $type:ident, $builder:ty }),*) => {
      paste! {
        $(
          pub fn [<read_ $type:lower _array>](array: &ProstArray, cardinality: usize) -> Result<ArrayImpl> {
            ensure!(
              array.get_values().len() == 1,
              "Must have only 1 buffer in a {} array", stringify!($type)
            );

            let buf = array.get_values()[0].get_body().as_slice();

            let mut builder = $builder::new(cardinality)?;
            let bitmap: Bitmap = array.get_null_bitmap()?.try_into()?;
            let mut cursor = Cursor::new(buf);
            for not_null in bitmap.iter() {
              if not_null {
                builder.append(Some([<read_ $type:lower>](&mut cursor)?))?;
              } else {
                builder.append(None)?;
              }
            }
            let arr = builder.finish()?;
            Ok(arr.into())
          }
        )*
      }
    };
}

read_one_value_array! {
    { bool, BoolArrayBuilder },
    { NaiveDate, NaiveDateArrayBuilder },
    { NaiveTime, NaiveTimeArrayBuilder },
    { NaiveDateTime, NaiveDateTimeArrayBuilder }
}

fn read_offset(offset_cursor: &mut Cursor<&[u8]>) -> Result<i64> {
    let offset = offset_cursor
        .read_i64::<BigEndian>()
        .map_err(|e| InternalError(format!("failed to read i64 from offset buffer: {}", e)))?;
    Ok(offset)
}

pub fn read_string_array<B: ArrayBuilder, R: VarSizedValueReader<B>>(
    array: &ProstArray,
    cardinality: usize,
) -> Result<ArrayImpl> {
    ensure!(
        array.get_values().len() == 2,
        "Must have exactly 2 buffers in a string array"
    );
    let offset_buff = array.get_values()[0].get_body().as_slice();
    let data_buf = array.get_values()[1].get_body().as_slice();

    let mut builder = B::new_with_meta(cardinality, ArrayMeta::Simple)?;
    let bitmap: Bitmap = array.get_null_bitmap()?.try_into()?;
    let mut offset_cursor = Cursor::new(offset_buff);
    let mut data_cursor = Cursor::new(data_buf);
    let mut prev_offset: i64 = -1;

    let mut buf = Vec::new();
    for not_null in bitmap.iter() {
        if not_null {
            if prev_offset < 0 {
                prev_offset = read_offset(&mut offset_cursor)?;
            }
            let offset = read_offset(&mut offset_cursor)?;
            let length = (offset - prev_offset) as usize;
            prev_offset = offset;
            buf.resize(length, Default::default());
            data_cursor.read_exact(buf.as_mut_slice()).map_err(|e| {
                InternalError(format!(
                    "failed to read str from data buffer: {} [length={}, offset={}]",
                    e, length, offset
                ))
            })?;
            let v = R::read(buf.as_slice())?;
            builder.append(Some(v))?;
        } else {
            builder.append(None)?;
        }
    }
    let arr = builder.finish()?;
    Ok(arr.into())
}
