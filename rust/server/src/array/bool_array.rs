use std::any::Any;
use std::convert::TryFrom;
use std::iter::Iterator;
use std::sync::Arc;

use protobuf::well_known_types::Any as AnyProto;
use risingwave_proto::data::Buffer_CompressionType;
use risingwave_proto::data::DataType as DataTypeProto;
use risingwave_proto::data::DataType_TypeName;
use risingwave_proto::data::{Buffer as BufferProto, Column};

use crate::array::array_data::ArrayData;
use crate::array::{Array, ArrayBuilder, ArrayRef};
use crate::buffer::Bitmap;
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::Result;
use crate::error::RwError;
use crate::expr::Datum;
use crate::types::{BoolType, DataType, DataTypeKind, DataTypeRef};
use crate::util::{bit_util, downcast_mut, downcast_ref};

pub struct BoolArray {
    data: ArrayData,
}

pub struct BoolArrayBuilder {
    data_type: DataTypeRef,
    buffer: Vec<bool>,
    null_bitmap_buffer: Vec<bool>,
}

impl TryFrom<ArrayData> for BoolArray {
    type Error = RwError;

    fn try_from(data: ArrayData) -> Result<Self> {
        BoolArray::new(data)
    }
}

impl AsRef<dyn Any> for BoolArray {
    fn as_ref(&self) -> &dyn Any {
        self
    }
}

impl BoolArray {
    /// Constructor.
    fn new(data: ArrayData) -> Result<Self> {
        ensure!(data.data_type().data_type_kind() == DataTypeKind::Boolean);
        ensure!(data.buffers().len() == 1);
        ensure!(data.buffers()[0].len() >= Bitmap::num_of_bytes(data.cardinality()));

        Ok(Self { data })
    }

    pub fn as_iter(&self) -> Result<impl Iterator<Item = Option<bool>> + '_> {
        BoolIter::new(self)
    }

    pub fn from_values<T>(input: T) -> Result<ArrayRef>
    where
        T: AsRef<[Option<bool>]>,
    {
        let data_type = BoolType::create(true);
        let mut boxed_builder =
            DataType::create_array_builder(data_type.clone(), input.as_ref().len())?;
        let builder: &mut BoolArrayBuilder = downcast_mut(boxed_builder.as_mut())?;

        for v in input.as_ref() {
            builder.append_value(*v)?;
        }

        boxed_builder.finish()
    }

    fn value_at(&self, idx: usize) -> Result<Option<bool>> {
        self.check_idx(idx)?;
        // Justification
        // We've already checked index before.
        Ok(unsafe { self.value_at_unchecked(idx) })
    }

    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<bool> {
        if self.is_null_unchecked(idx) {
            None
        } else {
            Some(bit_util::get_bit_raw(
                self.data.buffer_at_unchecked(0).as_ptr(),
                idx,
            ))
        }
    }
}

impl AsMut<dyn Any> for BoolArray {
    fn as_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl Array for BoolArray {
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
        let values = {
            let mut output_buffer = Vec::<u8>::with_capacity(self.len());

            for v in self.as_iter()? {
                match v {
                    Some(b) => {
                        output_buffer.push(if b { 1u8 } else { 0u8 });
                    }
                    None => {
                        output_buffer.push(0u8);
                    }
                }
            }

            let mut values = BufferProto::new();
            values.set_compression(Buffer_CompressionType::NONE);
            values.set_body(output_buffer);

            values
        };

        column.mut_values().push(values);

        AnyProto::pack(&column).map_err(|e| RwError::from(ProtobufError(e)))
    }
}

impl BoolArrayBuilder {
    pub fn new(data_type: DataTypeRef, capacity: usize) -> Result<Self> {
        Ok(Self {
            data_type,
            buffer: Vec::with_capacity(capacity),
            null_bitmap_buffer: Vec::with_capacity(capacity),
        })
    }

    pub fn append_value(&mut self, value: Option<bool>) -> Result<()> {
        match value {
            Some(v) => {
                self.buffer.push(v);
                self.null_bitmap_buffer.push(true);
            }
            None => {
                self.buffer.push(false);
                self.null_bitmap_buffer.push(false);
            }
        }

        Ok(())
    }
}

impl AsRef<dyn Any> for BoolArrayBuilder {
    fn as_ref(&self) -> &dyn Any {
        self
    }
}

impl AsMut<dyn Any> for BoolArrayBuilder {
    fn as_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl ArrayBuilder for BoolArrayBuilder {
    fn append(&mut self, datum: &Datum) -> Result<()> {
        match datum {
            Datum::Bool(v) => self.append_value(Some(*v)),
            _ => Err(InternalError(format!("Incorrect datum for bool: {:?}", datum)).into()),
        }
    }

    fn append_array(&mut self, source: &dyn Array) -> Result<()> {
        let input: &BoolArray = downcast_ref(source)?;
        for v in input.as_iter()? {
            self.append_value(v)?;
        }

        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<ArrayRef> {
        let cardinality = self.buffer.len();
        let data_buffer = Bitmap::from_vec(self.buffer)?.bits;
        let null_bitmap = Bitmap::from_vec(self.null_bitmap_buffer)?;
        let array_data = ArrayData::builder()
            .data_type(self.data_type)
            .cardinality(cardinality)
            .null_count(0)
            .buffers(vec![data_buffer])
            .null_bitmap(null_bitmap)
            .build();

        BoolArray::try_from(array_data).map(|arr| Arc::new(arr) as ArrayRef)
    }
}

struct BoolIter<'a> {
    array: &'a BoolArray,
    cur_pos: usize,
    end_pos: usize,
}

impl<'a> Iterator for BoolIter<'a> {
    type Item = Option<bool>;

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

impl<'a> BoolIter<'a> {
    fn new(array: &'a BoolArray) -> Result<Self> {
        Ok(Self {
            array,
            cur_pos: 0,
            end_pos: array.len(),
        })
    }
}

impl<'a> TryFrom<&'a DataTypeProto> for BoolType {
    type Error = RwError;

    fn try_from(proto: &'a DataTypeProto) -> Result<Self> {
        ensure!(proto.get_type_name() == DataType_TypeName::BOOLEAN);
        Ok(BoolType::new(proto.get_is_nullable()))
    }
}

#[cfg(test)]
mod tests {
    use std::iter::Iterator;

    use risingwave_proto::data::{Buffer_CompressionType, Column, DataType_TypeName};

    use crate::array::BoolArray;
    use crate::util::downcast_ref;

    #[test]
    fn test_build_bool_array() {
        let input = vec![
            Some(true),
            Some(false),
            None,
            Some(false),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
            None,
            Some(false),
            Some(true),
            None,
            Some(true),
        ];

        let result_array =
            BoolArray::from_values(&input).expect("Failed to build bool array from vec");
        let result_array: &BoolArray = downcast_ref(&*result_array).expect("Not bool array");

        assert_eq!(
            input,
            result_array
                .as_iter()
                .expect("Failed to create bool iterator")
                .collect::<Vec<Option<bool>>>()
        );
    }

    #[test]
    fn test_to_proto() {
        let input = vec![Some(true), None, Some(false), Some(false), Some(true), None];

        let result_array =
            BoolArray::from_values(&input).expect("Failed to build bool array from vec");
        let result_proto = result_array
            .to_protobuf()
            .expect("Failed to convert to protobuf");

        let result_proto: Column = result_proto
            .unpack()
            .expect("Failed to unpack")
            .expect("Failed to unwrap option");

        assert_eq!(
            DataType_TypeName::BOOLEAN,
            result_proto.get_column_type().get_type_name()
        );

        assert_eq!(true, result_proto.get_column_type().get_is_nullable());
        assert_eq!(
            vec![1u8, 0u8, 1u8, 1u8, 1u8, 0u8],
            result_proto.get_null_bitmap().get_body()[0..input.len()]
        );

        assert_eq!(
            vec![1u8, 0u8, 0u8, 0u8, 1u8, 0u8],
            result_proto.get_values()[0].get_body()[0..input.len()]
        );
        assert_eq!(
            Buffer_CompressionType::NONE,
            result_proto.get_values()[0].get_compression()
        );
    }
}
