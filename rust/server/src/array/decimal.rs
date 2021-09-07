use std::any::Any;
use std::convert::{TryFrom, TryInto};
use std::iter::Iterator;
use std::mem::size_of;
use std::sync::Arc;

use protobuf::well_known_types::Any as AnyProto;
use rust_decimal::prelude::Zero;
use rust_decimal::Decimal;

use risingwave_proto::data::Buffer as BufferProto;
use risingwave_proto::data::{Buffer_CompressionType, Column};

use crate::array::array_data::ArrayData;
use crate::array::{Array, ArrayBuilder, ArrayRef};
use crate::buffer::{Bitmap, Buffer};
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::Result;
use crate::error::RwError;
use crate::expr::Datum;
use crate::types::{DataType, DataTypeKind, DataTypeRef, DecimalType, MAX_PRECISION};
use crate::util::downcast_ref;

pub const DECIMAL_BYTES: usize = 16;

pub struct DecimalArray {
    data: ArrayData,
}

pub struct DecimalArrayBuilder {
    data_type: DataTypeRef,
    buffer: Vec<Decimal>,
    null_bitmap_buffer: Vec<bool>,
}

impl DecimalArray {
    fn new(data: ArrayData) -> Result<Self> {
        ensure!(data.data_type().data_type_kind() == DataTypeKind::Decimal);
        ensure!(data.buffers().len() == 1);
        ensure!(data.buffers()[0].len() >= (DECIMAL_BYTES * data.cardinality()));
        ensure!(data.buffers()[0].as_ptr().align_offset(DECIMAL_BYTES) == 0);
        Ok(Self { data })
    }

    fn inferred_from_values<T>(values: T) -> (u32, u32)
    where
        T: AsRef<[Option<Decimal>]>,
    {
        let precision = MAX_PRECISION;
        let scale = values.as_ref().iter().fold(0u32, |s, nd| match nd {
            Some(d) => s.max(d.scale()),
            None => s,
        });
        (precision, scale)
    }

    pub fn as_iter(&self) -> Result<impl Iterator<Item = Option<Decimal>> + '_> {
        DecimalIter::new(self)
    }

    // pub fn from_values<T>(input: T) -> Result<ArrayRef>
    // where
    //   T: AsRef<[Option<Decimal>]>,
    // {
    //   let (precision, scale) = DecimalArray::inferred_from_values(&input);
    //   let data_type = DecimalType::create(true, precision, scale)?;
    //   let mut boxed_builder =
    //     DataType::create_array_builder(data_type.clone(), input.as_ref().len())?;
    //   let builder: &mut DecimalArrayBuilder = downcast_mut(boxed_builder.as_mut())?;
    //
    //   for v in input.as_ref() {
    //     builder.append_value(*v)?;
    //   }
    //
    //   boxed_builder.finish()
    // }

    fn value_at(&self, idx: usize) -> Result<Option<Decimal>> {
        self.check_idx(idx)?;
        // Justification
        // We've already checked index before.
        Ok(unsafe { self.value_at_unchecked(idx) })
    }

    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<Decimal> {
        if self.is_null_unchecked(idx) {
            None
        } else {
            let st = DECIMAL_BYTES * idx;
            let ed = DECIMAL_BYTES * (idx + 1);
            let decimal_bytes: [u8; DECIMAL_BYTES] = self.data.buffer_at_unchecked(0).as_slice()
                [st..ed]
                .try_into()
                .expect("slice with incorrect length");
            Some(Decimal::deserialize(decimal_bytes))
        }
    }
}

struct DecimalIter<'a> {
    array: &'a DecimalArray,
    cur_pos: usize,
    end_pos: usize,
}

impl<'a> Iterator for DecimalIter<'a> {
    type Item = Option<Decimal>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_pos >= self.end_pos {
            None
        } else {
            let old_pos = self.cur_pos;
            self.cur_pos += 1;

            // Justification
            // We've already checked pos.
            unsafe { Some(self.array.value_at_unchecked(old_pos)) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end_pos - self.cur_pos;
        (remaining, Some(remaining))
    }
}

impl<'a> DecimalIter<'a> {
    fn new(array: &'a DecimalArray) -> Result<Self> {
        Ok(Self {
            array,
            cur_pos: 0,
            end_pos: array.len(),
        })
    }
}

impl TryFrom<ArrayData> for DecimalArray {
    type Error = RwError;

    fn try_from(data: ArrayData) -> Result<Self> {
        DecimalArray::new(data)
    }
}

impl AsRef<dyn Any> for DecimalArray {
    fn as_ref(&self) -> &dyn Any {
        self
    }
}

impl AsMut<dyn Any> for DecimalArray {
    fn as_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Array for DecimalArray {
    fn data_type(&self) -> &dyn DataType {
        self.data.data_type()
    }

    fn array_data(&self) -> &ArrayData {
        &self.data
    }

    fn to_protobuf(&self) -> Result<AnyProto> {
        let mut column = Column::new();
        let proto_data_type = self.data.data_type().to_protobuf()?;
        column.set_column_type(proto_data_type);
        if let Some(null_bitmap) = self.data.null_bitmap() {
            column.set_null_bitmap(null_bitmap.to_protobuf()?);
        }

        let (offset_value, data_value) = {
            let mut data_buffer = Vec::<u8>::new();
            let mut offset_buffer = Vec::<u8>::with_capacity(self.len() * size_of::<u32>());
            let mut offset = 0u32;

            for v in self.as_iter()? {
                match v {
                    Some(b) => {
                        let decimal_str = b.to_string();
                        data_buffer.extend_from_slice(decimal_str.as_bytes());
                        offset_buffer.extend_from_slice(&offset.to_be_bytes());
                        offset += decimal_str.len() as u32;
                    }
                    None => {
                        offset_buffer.extend_from_slice(&offset.to_be_bytes());
                    }
                }
            }
            offset_buffer.extend_from_slice(&offset.to_be_bytes());

            let mut data_value = BufferProto::new();
            data_value.set_compression(Buffer_CompressionType::NONE);
            data_value.set_body(data_buffer);

            let mut offset_value = BufferProto::new();
            offset_value.set_compression(Buffer_CompressionType::NONE);
            offset_value.set_body(offset_buffer);

            (offset_value, data_value)
        };

        column.set_values(
            protobuf::RepeatedField::<risingwave_proto::data::Buffer>::from_vec(vec![
                offset_value,
                data_value,
            ]),
        );

        AnyProto::pack(&column).map_err(|e| RwError::from(ProtobufError(e)))
    }
}

impl AsRef<dyn Any> for DecimalArrayBuilder {
    fn as_ref(&self) -> &dyn Any {
        self
    }
}

impl AsMut<dyn Any> for DecimalArrayBuilder {
    fn as_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl ArrayBuilder for DecimalArrayBuilder {
    fn append(&mut self, datum: &Datum) -> Result<()> {
        match datum {
            Datum::Decimal(v) => self.append_value(Some(*v)),
            _ => Err(InternalError(format!("Incorrect datum for decimal: {:?}", datum)).into()),
        }
    }

    fn append_array(&mut self, source: &dyn Array) -> Result<()> {
        let input: &DecimalArray = downcast_ref(source)?;
        for v in input.as_iter()? {
            self.append_value(v)?;
        }

        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<ArrayRef> {
        let cardinality = self.buffer.len();
        let mut output_buffer = Vec::<u8>::with_capacity(self.buffer.len() * DECIMAL_BYTES);
        for v in self.buffer {
            output_buffer.extend_from_slice(&v.serialize());
        }
        let data_buffer = Buffer::try_from(output_buffer)?;
        let null_bitmap = Bitmap::from_vec(self.null_bitmap_buffer)?;
        let array_data = ArrayData::builder()
            .data_type(self.data_type)
            .cardinality(cardinality)
            .null_count(0)
            .buffers(vec![data_buffer])
            .null_bitmap(null_bitmap)
            .build();

        DecimalArray::try_from(array_data).map(|arr| Arc::new(arr) as ArrayRef)
    }
}

impl DecimalArrayBuilder {
    pub fn new(data_type: Arc<DecimalType>, capacity: usize) -> Self {
        Self {
            data_type,
            buffer: Vec::with_capacity(capacity),
            null_bitmap_buffer: Vec::with_capacity(capacity),
        }
    }

    pub fn append_value(&mut self, value: Option<Decimal>) -> Result<()> {
        match value {
            Some(v) => {
                self.buffer.push(v);
                self.null_bitmap_buffer.push(true)
            }
            None => {
                self.buffer.push(Decimal::zero());
                self.null_bitmap_buffer.push(false);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    // #[test]
    // fn test_build_decimal_array() {
    //   let input = vec![Some(Decimal::new(123, 2)), None, Some(Decimal::new(321, 2))];
    //
    //   let result_array =
    //     DecimalArray::from_values(&input).expect("Failed to build decimal array from vec");
    //   let result_array: &DecimalArray = downcast_ref(&*result_array).expect("Not decimal array");
    //
    //   assert_eq!(
    //     input,
    //     result_array
    //       .as_iter()
    //       .expect("Failed to create decimal iterator")
    //       .collect::<Vec<Option<Decimal>>>()
    //   );
    // }
    //
    // #[test]
    // fn test_auto_infer_scale_from_values() {
    //   let input = vec![
    //     Some(Decimal::new(123, 2)),
    //     Some(Decimal::new(1234, 3)),
    //     Some(Decimal::new(12345, 4)),
    //   ];
    //   let result_array =
    //     DecimalArray::from_values(&input).expect("Failed to build decimal array from vec");
    //   let result_array: &DecimalArray = downcast_ref(&*result_array).expect("Not decimal array");
    //
    //   let data_type: &DecimalType = result_array
    //     .data
    //     .data_type()
    //     .as_any()
    //     .downcast_ref::<DecimalType>()
    //     .expect("Not decimal type");
    //
    //   assert_eq!(data_type.get_scale(), 4u32);
    // }
    //
    // #[test]
    // fn test_to_proto() {
    //   let input = vec![Some(Decimal::new(123, 2)), None, Some(Decimal::new(321, 2))];
    //
    //   let result_array =
    //     DecimalArray::from_values(&input).expect("Failed to build decimal array from vec");
    //   let result_proto = result_array
    //     .to_protobuf()
    //     .expect("Failed to convert to protobuf");
    //
    //   let result_proto: Column = result_proto
    //     .unpack()
    //     .expect("Failed to unpack")
    //     .expect("Failed to unwrap option");
    //
    //   assert_eq!(
    //     DataType_TypeName::DECIMAL,
    //     result_proto.get_column_type().get_type_name()
    //   );
    //
    //   assert_eq!(true, result_proto.get_column_type().get_is_nullable());
    //   assert_eq!(
    //     vec![1u8, 0u8, 1u8],
    //     result_proto.get_null_bitmap().get_body()[0..input.len()]
    //   );
    //
    //   assert_eq!(
    //     [
    //       0u32.to_be_bytes(),
    //       4u32.to_be_bytes(),
    //       4u32.to_be_bytes(),
    //       8u32.to_be_bytes()
    //     ]
    //     .concat(),
    //     result_proto.get_values()[0].get_body(),
    //   );
    //
    //   assert_eq!(
    //     ["1.23".as_bytes(), "3.21".as_bytes(),].concat(),
    //     result_proto.get_values()[1].get_body(),
    //   );
    //
    //   assert_eq!(
    //     Buffer_CompressionType::NONE,
    //     result_proto.get_values()[0].get_compression()
    //   );
    // }
}
