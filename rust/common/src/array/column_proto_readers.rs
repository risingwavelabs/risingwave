use std::io::{Cursor, Read};
use std::sync::Arc;

use byteorder::{BigEndian, ReadBytesExt};
use risingwave_pb::data::Column as ProstColumn;

use crate::array::value_reader::{PrimitiveValueReader, VarSizedValueReader};
use crate::array::{
    ArrayBuilder, ArrayRef, BoolArrayBuilder, PrimitiveArrayBuilder, PrimitiveArrayItemType,
};
use crate::buffer::Bitmap;
use crate::error::ErrorCode::InternalError;
use crate::error::Result;

// TODO: Use techniques like apache arrow flight RPC to eliminate deserialization.
// https://arrow.apache.org/docs/format/Flight.html

pub fn read_numeric_column<T: PrimitiveArrayItemType, R: PrimitiveValueReader<T>>(
    col: &ProstColumn,
    cardinality: usize,
) -> Result<ArrayRef> {
    ensure!(
        col.get_values().len() == 1,
        "Must have only 1 buffer in a numeric column"
    );

    let buf = col.get_values()[0].get_body().as_slice();
    let value_size = std::mem::size_of::<T>();
    ensure!(
        buf.len() % value_size == 0,
        "Unexpected memory layout of numeric column"
    );

    let mut builder = PrimitiveArrayBuilder::<T>::new(cardinality)?;
    let bitmap: Bitmap = (col.get_null_bitmap()).try_into()?;
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
    Ok(Arc::new(arr.into()))
}

pub fn read_bool_column(col: &ProstColumn, cardinality: usize) -> Result<ArrayRef> {
    ensure!(
        col.get_values().len() == 1,
        "Must have only 1 buffer in a bool column"
    );

    let buf = col.get_values()[0].get_body().as_slice();

    let mut builder = BoolArrayBuilder::new(cardinality)?;
    let bitmap: Bitmap = col.get_null_bitmap().try_into()?;
    let mut cursor = Cursor::new(buf);
    for not_null in bitmap.iter() {
        if not_null {
            let v = cursor
                .read_u8()
                .map_err(|e| InternalError(format!("Failed to read u8 from bool buffer: {}", e)))?;
            builder.append(Some(v != 0))?;
        } else {
            builder.append(None)?;
        }
    }
    let arr = builder.finish()?;
    Ok(Arc::new(arr.into()))
}

fn read_offset(offset_cursor: &mut Cursor<&[u8]>) -> Result<i64> {
    let offset = offset_cursor
        .read_i64::<BigEndian>()
        .map_err(|e| InternalError(format!("failed to read i64 from offset buffer: {}", e)))?;
    Ok(offset)
}

pub fn read_string_column<B: ArrayBuilder, R: VarSizedValueReader<B>>(
    col: &ProstColumn,
    cardinality: usize,
) -> Result<ArrayRef> {
    ensure!(
        col.get_values().len() == 2,
        "Must have exactly 2 buffers in a string column"
    );
    let offset_buff = col.get_values()[0].get_body().as_slice();
    let data_buf = col.get_values()[1].get_body().as_slice();

    let mut builder = B::new(cardinality)?;
    let bitmap: Bitmap = (col.get_null_bitmap()).try_into()?;
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
    Ok(Arc::new(arr.into()))
}
