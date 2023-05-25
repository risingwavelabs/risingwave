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

use std::io::{Cursor, Read};

use anyhow::anyhow;
use byteorder::{BigEndian, ReadBytesExt};
use paste::paste;
use risingwave_pb::data::{PbArray, PbArrayType};

use super::*;
use crate::array::value_reader::{PrimitiveValueReader, VarSizedValueReader};
use crate::array::{
    Array, ArrayBuilder, ArrayImpl, ArrayResult, BoolArray, DateArrayBuilder, IntervalArrayBuilder,
    PrimitiveArrayBuilder, PrimitiveArrayItemType, TimeArrayBuilder, TimestampArrayBuilder,
};
use crate::buffer::Bitmap;
use crate::types::{Date, Interval, Time, Timestamp};

impl ArrayImpl {
    pub fn from_protobuf(array: &PbArray, cardinality: usize) -> ArrayResult<Self> {
        use crate::array::value_reader::*;
        let array = match array.array_type() {
            PbArrayType::Int16 => read_numeric_array::<i16, I16ValueReader>(array, cardinality)?,
            PbArrayType::Int32 => read_numeric_array::<i32, I32ValueReader>(array, cardinality)?,
            PbArrayType::Int64 => read_numeric_array::<i64, I64ValueReader>(array, cardinality)?,
            PbArrayType::Serial => {
                read_numeric_array::<Serial, SerialValueReader>(array, cardinality)?
            }
            PbArrayType::Float32 => read_numeric_array::<F32, F32ValueReader>(array, cardinality)?,
            PbArrayType::Float64 => read_numeric_array::<F64, F64ValueReader>(array, cardinality)?,
            PbArrayType::Bool => read_bool_array(array, cardinality)?,
            PbArrayType::Utf8 => {
                read_string_array::<Utf8ArrayBuilder, Utf8ValueReader>(array, cardinality)?
            }
            PbArrayType::Decimal => {
                read_numeric_array::<Decimal, DecimalValueReader>(array, cardinality)?
            }
            PbArrayType::Date => read_date_array(array, cardinality)?,
            PbArrayType::Time => read_time_array(array, cardinality)?,
            PbArrayType::Timestamp => read_timestamp_array(array, cardinality)?,
            PbArrayType::Interval => read_interval_array(array, cardinality)?,
            PbArrayType::Jsonb => {
                read_string_array::<JsonbArrayBuilder, JsonbValueReader>(array, cardinality)?
            }
            PbArrayType::Struct => StructArray::from_protobuf(array)?,
            PbArrayType::List => ListArray::from_protobuf(array)?,
            PbArrayType::Unspecified => unreachable!(),
            PbArrayType::Bytea => {
                read_string_array::<BytesArrayBuilder, BytesValueReader>(array, cardinality)?
            }
            PbArrayType::Int256 => Int256Array::from_protobuf(array, cardinality)?,
        };
        Ok(array)
    }
}

// TODO: Use techniques like apache arrow flight RPC to eliminate deserialization.
// https://arrow.apache.org/docs/format/Flight.html

fn read_numeric_array<T: PrimitiveArrayItemType, R: PrimitiveValueReader<T>>(
    array: &PbArray,
    cardinality: usize,
) -> ArrayResult<ArrayImpl> {
    ensure!(
        array.get_values().len() == 1,
        "Must have only 1 buffer in a numeric array"
    );

    let buf = array.get_values()[0].get_body().as_slice();

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
    ensure_eq!(arr.len(), cardinality);

    Ok(arr.into())
}

fn read_bool_array(array: &PbArray, cardinality: usize) -> ArrayResult<ArrayImpl> {
    ensure!(
        array.get_values().len() == 1,
        "Must have only 1 buffer in a bool array"
    );

    let data = (&array.get_values()[0]).into();
    let bitmap: Bitmap = array.get_null_bitmap()?.into();

    let arr = BoolArray::new(data, bitmap);
    ensure_eq!(arr.len(), cardinality);

    Ok(arr.into())
}

fn read_date(cursor: &mut Cursor<&[u8]>) -> ArrayResult<Date> {
    match cursor.read_i32::<BigEndian>() {
        Ok(days) => Date::with_days(days).map_err(|e| anyhow!(e).into()),
        Err(e) => bail!("Failed to read i32 from Date buffer: {}", e),
    }
}

fn read_time(cursor: &mut Cursor<&[u8]>) -> ArrayResult<Time> {
    match cursor.read_u64::<BigEndian>() {
        Ok(t) => Time::with_nano(t).map_err(|e| anyhow!(e).into()),
        Err(e) => bail!("Failed to read i64 from NaiveTime buffer: {}", e),
    }
}

fn read_timestamp(cursor: &mut Cursor<&[u8]>) -> ArrayResult<Timestamp> {
    cursor
        .read_i64::<BigEndian>()
        .map_err(|e| anyhow!("Failed to read i64 from Timestamp buffer: {}", e))
        .and_then(|t| Timestamp::with_macros(t).map_err(|e| anyhow!("{}", e)))
        .map_err(Into::into)
}

fn read_interval(cursor: &mut Cursor<&[u8]>) -> ArrayResult<Interval> {
    let mut read = || {
        let months = cursor.read_i32::<BigEndian>()?;
        let days = cursor.read_i32::<BigEndian>()?;
        let usecs = cursor.read_i64::<BigEndian>()?;

        Ok::<_, std::io::Error>(Interval::from_month_day_usec(months, days, usecs))
    };

    match read() {
        Ok(iu) => Ok(iu),
        Err(e) => bail!("Failed to read Interval from buffer: {}", e),
    }
}

macro_rules! read_one_value_array {
    ($({ $type:ident, $builder:ty }),*) => {
        paste! {
            $(
            fn [<read_ $type:snake _array>](array: &PbArray, cardinality: usize) -> ArrayResult<ArrayImpl> {
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
                ensure_eq!(arr.len(), cardinality);

                Ok(arr.into())
            }
            )*
        }
    };
}

read_one_value_array! {
    { Interval, IntervalArrayBuilder },
    { Date, DateArrayBuilder },
    { Time, TimeArrayBuilder },
    { Timestamp, TimestampArrayBuilder }
}

fn read_offset(offset_cursor: &mut Cursor<&[u8]>) -> ArrayResult<i64> {
    match offset_cursor.read_i64::<BigEndian>() {
        Ok(offset) => Ok(offset),
        Err(e) => bail!("failed to read i64 from offset buffer: {}", e),
    }
}

fn read_string_array<B: ArrayBuilder, R: VarSizedValueReader<B>>(
    array: &PbArray,
    cardinality: usize,
) -> ArrayResult<ArrayImpl> {
    ensure!(
        array.get_values().len() == 2,
        "Must have exactly 2 buffers in a string array"
    );
    let offset_buff = array.get_values()[0].get_body().as_slice();
    let data_buf = array.get_values()[1].get_body().as_slice();

    let mut builder = B::new(cardinality);
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
            R::read(buf.as_slice(), &mut builder)?;
        } else {
            builder.append(None);
        }
    }
    let arr = builder.finish();
    ensure_eq!(arr.len(), cardinality);

    Ok(arr.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{
        Array, ArrayBuilder, BoolArray, BoolArrayBuilder, DateArray, DateArrayBuilder,
        DecimalArray, DecimalArrayBuilder, I32Array, I32ArrayBuilder, TimeArray, TimeArrayBuilder,
        TimestampArray, TimestampArrayBuilder, Utf8Array, Utf8ArrayBuilder,
    };
    use crate::error::Result;
    use crate::types::{Date, Decimal, Time, Timestamp};

    // Convert a column to protobuf, then convert it back to column, and ensures the two are
    // identical.
    #[test]
    fn test_column_protobuf_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = I32ArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Some(i as i32));
            } else {
                builder.append(None);
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.len(), cardinality);
        let arr: &I32Array = new_col.as_int32();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(i as i32, x.unwrap());
            } else {
                assert!(x.is_none());
            }
        });
        Ok(())
    }

    #[test]
    fn test_bool_column_protobuf_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = BoolArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            match i % 3 {
                0 => builder.append(Some(false)),
                1 => builder.append(Some(true)),
                _ => builder.append(None),
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.len(), cardinality);
        let arr: &BoolArray = new_col.as_bool();
        arr.iter().enumerate().for_each(|(i, x)| match i % 3 {
            0 => assert_eq!(Some(false), x),
            1 => assert_eq!(Some(true), x),
            _ => assert_eq!(None, x),
        });
        Ok(())
    }

    #[test]
    fn test_utf8_column_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = Utf8ArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Some("abc"));
            } else {
                builder.append(None);
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        let arr: &Utf8Array = new_col.as_utf8();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!("abc", x.unwrap());
            } else {
                assert!(x.is_none());
            }
        });
        Ok(())
    }

    #[test]
    fn test_decimal_protobuf_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = DecimalArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Some(Decimal::from(i)));
            } else {
                builder.append(None);
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.len(), cardinality);
        let arr: &DecimalArray = new_col.as_decimal();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(Decimal::from(i), x.unwrap());
            } else {
                assert!(x.is_none());
            }
        });
        Ok(())
    }

    #[test]
    fn test_date_protobuf_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = DateArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Date::with_days(i as i32).ok());
            } else {
                builder.append(None);
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.len(), cardinality);
        let arr: &DateArray = new_col.as_date();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(Date::with_days(i as i32).ok().unwrap(), x.unwrap());
            } else {
                assert!(x.is_none());
            }
        });
        Ok(())
    }

    #[test]
    fn test_time_protobuf_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = TimeArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Time::with_secs_nano(i as u32, i as u32 * 1000).ok());
            } else {
                builder.append(None);
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.len(), cardinality);
        let arr: &TimeArray = new_col.as_time();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(
                    Time::with_secs_nano(i as u32, i as u32 * 1000)
                        .ok()
                        .unwrap(),
                    x.unwrap()
                );
            } else {
                assert!(x.is_none());
            }
        });
        Ok(())
    }

    #[test]
    fn test_timestamp_protobuf_conversion() -> Result<()> {
        let cardinality = 2048;
        let mut builder = TimestampArrayBuilder::new(cardinality);
        for i in 0..cardinality {
            if i % 2 == 0 {
                builder.append(Timestamp::with_secs_nsecs(i as i64, i as u32 * 1000).ok());
            } else {
                builder.append(None);
            }
        }
        let col: ArrayImpl = builder.finish().into();
        let new_col = ArrayImpl::from_protobuf(&col.to_protobuf(), cardinality).unwrap();
        assert_eq!(new_col.len(), cardinality);
        let arr: &TimestampArray = new_col.as_timestamp();
        arr.iter().enumerate().for_each(|(i, x)| {
            if i % 2 == 0 {
                assert_eq!(
                    Timestamp::with_secs_nsecs(i as i64, i as u32 * 1000)
                        .ok()
                        .unwrap(),
                    x.unwrap()
                );
            } else {
                assert!(x.is_none());
            }
        });
        Ok(())
    }
}
