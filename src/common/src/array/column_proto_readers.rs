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

use std::io::{Cursor, Read};

use anyhow::anyhow;
use byteorder::{BigEndian, ReadBytesExt};
use paste::paste;
use risingwave_pb::data::Array as ProstArray;

use crate::array::value_reader::{PrimitiveValueReader, VarSizedValueReader};
use crate::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayMeta, ArrayResult, BoolArray, IntervalArrayBuilder,
    NaiveDateArrayBuilder, NaiveDateTimeArrayBuilder, NaiveTimeArrayBuilder, PrimitiveArrayBuilder,
    PrimitiveArrayItemType,
};
use crate::buffer::Bitmap;
use crate::types::interval::IntervalUnit;
use crate::types::{NaiveDateTimeWrapper, NaiveDateWrapper, NaiveTimeWrapper};

// TODO: Use techniques like apache arrow flight RPC to eliminate deserialization.
// https://arrow.apache.org/docs/format/Flight.html

pub fn read_numeric_array<T: PrimitiveArrayItemType, R: PrimitiveValueReader<T>>(
    array: &ProstArray,
    cardinality: usize,
) -> ArrayResult<ArrayImpl> {
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

    let mut builder = PrimitiveArrayBuilder::<T>::new(cardinality);
    let bitmap: Bitmap = array.get_null_bitmap()?.into();
    let mut cursor = Cursor::new(buf);
    for not_null in bitmap.iter() {
        if not_null {
            let v = R::read(&mut cursor)?;
            builder.append(Some(v));
        } else {
            builder.append(None);
        }
    }
    let arr = builder.finish();
    Ok(arr.into())
}

pub fn read_bool_array(array: &ProstArray, cardinality: usize) -> ArrayResult<ArrayImpl> {
    ensure!(
        array.get_values().len() == 1,
        "Must have only 1 buffer in a bool array"
    );

    let data = (&array.get_values()[0]).into();
    let bitmap: Bitmap = array.get_null_bitmap()?.into();

    let arr = BoolArray::new(bitmap, data);
    assert_eq!(arr.len(), cardinality);

    Ok(arr.into())
}

fn read_naive_date(cursor: &mut Cursor<&[u8]>) -> ArrayResult<NaiveDateWrapper> {
    match cursor.read_i32::<BigEndian>() {
        Ok(days) => NaiveDateWrapper::from_protobuf(days),
        Err(e) => bail!("Failed to read i32 from NaiveDate buffer: {}", e),
    }
}

fn read_naive_time(cursor: &mut Cursor<&[u8]>) -> ArrayResult<NaiveTimeWrapper> {
    match cursor.read_u64::<BigEndian>() {
        Ok(t) => NaiveTimeWrapper::from_protobuf(t),
        Err(e) => bail!("Failed to read i64 from NaiveTime buffer: {}", e),
    }
}

fn read_naive_date_time(cursor: &mut Cursor<&[u8]>) -> ArrayResult<NaiveDateTimeWrapper> {
    match cursor.read_i64::<BigEndian>() {
        Ok(t) => NaiveDateTimeWrapper::from_protobuf(t),
        Err(e) => bail!("Failed to read i64 from NaiveDateTime buffer: {}", e),
    }
}

pub fn read_interval_unit(cursor: &mut Cursor<&[u8]>) -> ArrayResult<IntervalUnit> {
    let mut read = || {
        let months = cursor.read_i32::<BigEndian>()?;
        let days = cursor.read_i32::<BigEndian>()?;
        let ms = cursor.read_i64::<BigEndian>()?;

        Ok::<_, std::io::Error>(IntervalUnit::new(months, days, ms))
    };

    match read() {
        Ok(iu) => Ok(iu),
        Err(e) => bail!("Failed to read IntervalUnit from buffer: {}", e),
    }
}

macro_rules! read_one_value_array {
    ($({ $type:ident, $builder:ty }),*) => {
        paste! {
            $(
            pub fn [<read_ $type:snake _array>](array: &ProstArray, cardinality: usize) -> ArrayResult<ArrayImpl> {
                ensure!(
                    array.get_values().len() == 1,
                    "Must have only 1 buffer in a {} array", stringify!($type)
                );

                let buf = array.get_values()[0].get_body().as_slice();

                let mut builder = $builder::new(cardinality);
                let bitmap: Bitmap = array.get_null_bitmap()?.into();
                let mut cursor = Cursor::new(buf);
                for not_null in bitmap.iter() {
                    if not_null {
                        builder.append(Some([<read_ $type:snake>](&mut cursor)?));
                    } else {
                        builder.append(None);
                    }
                }
                let arr = builder.finish();
                Ok(arr.into())
            }
            )*
        }
    };
}

read_one_value_array! {
    { IntervalUnit, IntervalArrayBuilder },
    { NaiveDate, NaiveDateArrayBuilder },
    { NaiveTime, NaiveTimeArrayBuilder },
    { NaiveDateTime, NaiveDateTimeArrayBuilder }
}

fn read_offset(offset_cursor: &mut Cursor<&[u8]>) -> ArrayResult<i64> {
    match offset_cursor.read_i64::<BigEndian>() {
        Ok(offset) => Ok(offset),
        Err(e) => bail!("failed to read i64 from offset buffer: {}", e),
    }
}

pub fn read_string_array<B: ArrayBuilder, R: VarSizedValueReader<B>>(
    array: &ProstArray,
    cardinality: usize,
) -> ArrayResult<ArrayImpl> {
    ensure!(
        array.get_values().len() == 2,
        "Must have exactly 2 buffers in a string array"
    );
    let offset_buff = array.get_values()[0].get_body().as_slice();
    let data_buf = array.get_values()[1].get_body().as_slice();

    let mut builder = B::with_meta(cardinality, ArrayMeta::Simple);
    let bitmap: Bitmap = array.get_null_bitmap()?.into();
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
                anyhow!(
                    "failed to read str from data buffer: {} [length={}, offset={}]",
                    e,
                    length,
                    offset
                )
            })?;
            let v = R::read(buf.as_slice())?;
            builder.append(Some(v));
        } else {
            builder.append(None);
        }
    }
    let arr = builder.finish();
    Ok(arr.into())
}
