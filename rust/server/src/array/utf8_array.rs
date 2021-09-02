use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

use protobuf::well_known_types::Any as AnyProto;

use risingwave_proto::data::Buffer_CompressionType;
use risingwave_proto::data::{Buffer as BufferProto, Column};

use crate::array::array_data::ArrayData;
use crate::array::{Array, ArrayBuilder, ArrayRef};
use crate::buffer::{Bitmap, Buffer};
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::{Result, RwError};
use crate::expr::Datum;
use crate::types::{DataType, DataTypeKind, DataTypeRef, StringType};
use crate::util::{downcast_mut, downcast_ref};

use std::mem::{align_of, size_of};

pub(crate) struct UTF8Array {
    data: ArrayData,
}

impl UTF8Array {
    pub(crate) fn from_values<'a, T>(input: T, width: usize, kind: DataTypeKind) -> Result<ArrayRef>
    where
        T: AsRef<[Option<&'a str>]>,
    {
        let data_type = StringType::create(true, width, kind);

        let mut boxed_builder =
            DataType::create_array_builder(data_type.clone(), input.as_ref().len())?;
        let builder: &mut UTF8ArrayBuilder = downcast_mut(boxed_builder.as_mut())?;

        for v in input.as_ref() {
            builder.append_str(*v)?;
        }

        boxed_builder.finish()
    }
}

impl TryFrom<ArrayData> for UTF8Array {
    type Error = RwError;

    fn try_from(data: ArrayData) -> Result<Self> {
        UTF8Array::new(data)
    }
}

impl UTF8Array {
    fn new(data: ArrayData) -> Result<Self> {
        ensure!(
            data.data_type().data_type_kind() == DataTypeKind::Char
                || data.data_type().data_type_kind() == DataTypeKind::Varchar
        );

        // offset buffer and data buffer
        ensure!(data.buffers().len() == 2);

        let offset_buffer = data.buffer_at(0)?;
        let data_buffer = data.buffer_at(1)?;

        // offset buffer align check
        ensure!(offset_buffer.as_ptr().align_offset(align_of::<u32>()) == 0);

        // offset buffer size check
        ensure!(offset_buffer.len() >= (size_of::<u32>() * data.cardinality()));

        if data.cardinality() == 0 {
            ensure!(offset_buffer.len() == size_of::<u32>());
            ensure!(data_buffer.is_empty());
        } else {
            let offsets = offset_buffer.typed_data::<u32>();
            ensure!(!offsets.is_empty());
            let last_offset = *offsets.last().unwrap();
            ensure!(last_offset == data_buffer.len() as u32);
        }

        Ok(Self { data })
    }
}

pub(crate) struct UTF8ArrayBuilder {
    width: usize,
    data_type: DataTypeRef,
    data_buffer: Vec<u8>,
    offset_buffer: Vec<u32>,
    null_bitmap_buffer: Vec<bool>,
    null_count: usize,
}

impl AsRef<dyn Any> for UTF8ArrayBuilder {
    fn as_ref(&self) -> &dyn Any {
        self
    }
}

impl AsMut<dyn Any> for UTF8ArrayBuilder {
    fn as_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl UTF8ArrayBuilder {
    pub(crate) fn new(data_type: DataTypeRef, width: usize, capacity: usize) -> Result<Self> {
        let mut builder = Self {
            width,
            data_type,
            data_buffer: Vec::with_capacity(capacity * width),
            offset_buffer: Vec::with_capacity(capacity),
            null_bitmap_buffer: Vec::with_capacity(capacity),
            null_count: 0,
        };

        builder.offset_buffer.push(0);

        Ok(builder)
    }
}

impl UTF8ArrayBuilder {
    pub(crate) fn append_str(&mut self, value: Option<&str>) -> Result<()> {
        match value {
            Some(v) => {
                // TODO (peng) for now we don't care about column description for char(n)
                // ensure!(v.len() <= self.width);
                self.data_buffer.extend_from_slice(v.as_bytes());
                self.offset_buffer.push(self.data_buffer.len() as u32);
                self.null_bitmap_buffer.push(true);
                Ok(())
            }
            None => {
                self.offset_buffer.push(self.data_buffer.len() as u32);
                self.null_bitmap_buffer.push(false);
                self.null_count += 1;
                Ok(())
            }
        }
    }
}

impl ArrayBuilder for UTF8ArrayBuilder {
    fn append(&mut self, datum: &Datum) -> Result<()> {
        match datum {
            Datum::UTF8String(v) => self.append_str(Some(v)),
            _ => Err(InternalError(format!("Incorrect datum for string: {:?}", datum)).into()),
        }
    }

    fn append_array(&mut self, source: &dyn Array) -> crate::error::Result<()> {
        let input: &UTF8Array = downcast_ref(source)?;
        for v in input.as_iter()? {
            self.append_str(v)?;
        }

        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<ArrayRef> {
        let cardinality = self.offset_buffer.len() - 1;

        let offset_buffer = Buffer::from_slice(self.offset_buffer)?;
        let data_buffer = Buffer::from_slice(self.data_buffer)?;
        let null_bitmap = Bitmap::from_vec(self.null_bitmap_buffer)?;

        let array_data = ArrayData::builder()
            .data_type(self.data_type)
            .cardinality(cardinality)
            .null_count(self.null_count)
            .buffers(vec![offset_buffer, data_buffer])
            .null_bitmap(null_bitmap)
            .build();

        UTF8Array::try_from(array_data).map(|arr| Arc::new(arr) as ArrayRef)
    }
}

impl AsRef<dyn Any> for UTF8Array {
    fn as_ref(&self) -> &dyn Any {
        self
    }
}

impl AsMut<dyn Any> for UTF8Array {
    fn as_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl UTF8Array {
    pub fn value_at(&self, i: usize) -> Result<&str> {
        let offset_buffer = self.data.buffer_at(0)?;
        let data_buffer = self.data.buffer_at(1)?;
        let offsets = offset_buffer.typed_data::<u32>();

        let offset_start = *offsets
            .get(i)
            .ok_or_else(|| InternalError(format!("Index: {} overflow", i)))?;

        let offset_end = *offsets
            .get(i + 1)
            .ok_or_else(|| InternalError(format!("Index: {} overflow", i + 1)))?;

        let slice = &data_buffer.as_slice()[offset_start as usize..offset_end as usize];

        std::str::from_utf8(slice)
            .map_err(|e| InternalError(format!("Restore from utf8 string failed: {}", e)).into())
    }

    pub unsafe fn value_at_unchecked(&self, i: usize) -> &str {
        let offset_buffer = self.data.buffer_at_unchecked(0);
        let data_buffer = self.data.buffer_at_unchecked(1);
        let offsets = offset_buffer.typed_data::<u32>();
        let offset_start = *offsets.get_unchecked(i);
        let offset_end = *offsets.get_unchecked(i + 1);
        let slice = &data_buffer.as_slice()[offset_start as usize..offset_end as usize];
        std::str::from_utf8_unchecked(slice)
    }
}

impl Array for UTF8Array {
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

        let mut offset_buffer = Vec::<u8>::with_capacity(self.len() * size_of::<u32>());
        let mut data_buffer = Vec::<u8>::with_capacity(self.data.buffer_at(1)?.len());

        offset_buffer.extend_from_slice(0_u32.to_be_bytes().as_ref());

        for v in self.as_iter()? {
            if let Some(s) = v {
                data_buffer.extend_from_slice(s.as_bytes());
            }

            let curr_len = data_buffer.len() as u32;
            offset_buffer.extend_from_slice(curr_len.to_be_bytes().as_ref());
        }

        let values = [offset_buffer, data_buffer]
            .iter()
            .map(|b| {
                let mut values = BufferProto::new();
                values.set_compression(Buffer_CompressionType::NONE);
                values.set_body(b.as_slice().to_vec());
                values
            })
            .collect();

        column.set_values(values);
        column.set_cardinality(self.data.cardinality() as u32);

        AnyProto::pack(&column).map_err(|e| RwError::from(ProtobufError(e)))
    }
}

struct UTF8ArrayIter<'a> {
    array: &'a UTF8Array,
    cur_pos: usize,
    end_pos: usize,
}

impl<'a> Iterator for UTF8ArrayIter<'a> {
    type Item = Option<&'a str>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cur_pos >= self.end_pos {
            return None;
        }

        let prev_pos = self.cur_pos;
        self.cur_pos += 1;

        unsafe {
            if self.array.data.is_null_unchecked(prev_pos) {
                return Some(None);
            }

            Some(Some(self.array.value_at_unchecked(prev_pos)))
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.end_pos - self.cur_pos;
        (remaining, Some(remaining))
    }
}

impl UTF8Array {
    pub(crate) fn as_iter(&self) -> Result<impl Iterator<Item = Option<&str>> + '_> {
        UTF8ArrayIter::new(self)
    }
}

impl<'a> UTF8ArrayIter<'a> {
    fn new(array: &'a UTF8Array) -> Result<Self> {
        Ok(Self {
            array,
            cur_pos: 0,
            end_pos: array.len(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::array::{Array, UTF8Array};
    use crate::types::DataTypeKind;
    use crate::util::downcast_ref;
    use risingwave_proto::data::Column;
    use std::mem::{align_of, size_of};

    #[test]
    fn test_empty_utf8_array() {
        let input: Vec<Option<&str>> = vec![];

        let result_array = UTF8Array::from_values(&input, 10, DataTypeKind::Varchar)
            .expect("Failed to build string array from vec");

        let result_array: &UTF8Array = downcast_ref(&*result_array).expect("Not string array");

        assert_eq!(result_array.len(), 0);
        assert_eq!(result_array.data.buffers().len(), 2);
        assert_eq!(result_array.data.buffers()[0].len(), (size_of::<u32>() * 1));
        assert_eq!(result_array.data.buffers()[1].len(), 0);
    }

    #[test]
    fn test_build_utf8_array() {
        let input = vec![Some("abc"), Some("jkl"), None, Some("xyz")];

        let width = 8;
        let result_array = UTF8Array::from_values(&input, width, DataTypeKind::Varchar)
            .expect("Failed to build string array from vec");

        let result_array: &UTF8Array = downcast_ref(&*result_array).expect("Not string array");

        assert_eq!(result_array.len(), input.len());
        assert_eq!(result_array.array_data().buffers().len(), 2);

        let offset_buffer = unsafe { result_array.data.buffer_at_unchecked(0) };
        let data_buffer = unsafe { result_array.data.buffer_at_unchecked(1) };

        assert_eq!(offset_buffer.as_ptr().align_offset(align_of::<u32>()), 0);
        assert!(
            offset_buffer.len() >= (size_of::<u32>() * result_array.array_data().cardinality())
        );
        assert_eq!(
            offset_buffer.len(),
            size_of::<u32>() * (result_array.array_data().cardinality() + 1)
        );

        assert_eq!(
            data_buffer.len(),
            size_of::<u8>()
                * input
                    .iter()
                    .map(|s| s.as_ref().unwrap_or(&"").len())
                    .sum::<usize>()
        );

        let offsets = offset_buffer.typed_data::<u32>();
        assert!(offsets.len() >= 1);
        let last_offset = *offsets.last().unwrap();

        assert_eq!(last_offset, data_buffer.len() as u32);

        assert_eq!(
            input,
            result_array
                .as_iter()
                .expect("Failed to create string iterator")
                .collect::<Vec<Option<&str>>>()
        );
    }

    #[test]
    fn test_restore_utf8_array_from_proto() {
        let input = vec![Some("abc"), None, Some("xyz"), None];

        let width = 8;
        let result_array = UTF8Array::from_values(&input, width, DataTypeKind::Varchar)
            .expect("Failed to build string array from vec");

        let result_array: &UTF8Array = downcast_ref(&*result_array).expect("Not string array");

        let result_proto = result_array
            .to_protobuf()
            .expect("Failed to convert to protobuf");

        let result_proto: Column = result_proto
            .unpack()
            .expect("Failed to unpack")
            .expect("Failed to unwrap option");

        assert_eq!(
            vec![1u8, 0u8, 1u8, 0u8],
            result_proto.get_null_bitmap().get_body()[0..input.len()]
        );

        assert_eq!(input.len(), result_proto.get_cardinality() as usize);
    }
}
