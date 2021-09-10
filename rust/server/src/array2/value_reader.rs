use crate::error::{ErrorCode, Result, RwError};

use crate::array2::PrimitiveArrayItemType;
use byteorder::{BigEndian, ReadBytesExt};
use risingwave_proto::data::Buffer;

/// Reads an encoded buffer into a value.
pub trait ValueReader<T: PrimitiveArrayItemType> {
    fn read(buf: &Buffer) -> Result<T>;
}

pub struct I16ValueReader {}
pub struct I32ValueReader {}
pub struct I64ValueReader {}
pub struct F32ValueReader {}
pub struct F64ValueReader {}

macro_rules! impl_numeric_value_reader {
    ($value_type:ty, $value_reader:ty,  $read_fn:ident) => {
        impl ValueReader<$value_type> for $value_reader {
            fn read(buf: &Buffer) -> Result<$value_type> {
                buf.get_body().$read_fn::<BigEndian>().map_err(|e| {
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
impl_numeric_value_reader!(f32, F32ValueReader, read_f32);
impl_numeric_value_reader!(f64, F64ValueReader, read_f64);
