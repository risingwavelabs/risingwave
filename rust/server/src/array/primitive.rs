use std::any::Any;
use std::convert::TryFrom;
use std::marker::PhantomData;
use std::mem::transmute;
use std::mem::{align_of, size_of};
use std::slice::from_raw_parts;
use std::sync::Arc;

use protobuf::well_known_types::Any as AnyProto;

use risingwave_proto::data::Buffer_CompressionType;
use risingwave_proto::data::{Buffer as BufferProto, Column};

use crate::array::array_data::ArrayData;
use crate::array::{Array, ArrayBuilder, ArrayRef};
use crate::buffer::{Bitmap, Buffer};
use crate::error::ErrorCode::ProtobufError;
use crate::error::Result;
use crate::error::RwError;
use crate::expr::Datum;
use crate::types::{DataType, DataTypeRef, NativeType, PrimitiveDataType};
use crate::util::downcast_ref;

/// A primitive array contains only one value buffer, and an optional bitmap buffer
pub struct PrimitiveArray<T: PrimitiveDataType> {
    data: ArrayData,
    _phantom: PhantomData<T>,
}

pub struct PrimitiveArrayBuilder<T: PrimitiveDataType> {
    data_type: DataTypeRef,
    buffer: Vec<T::N>,
    null_bitmap_buffer: Vec<bool>,
}

impl<T: PrimitiveDataType> PrimitiveArray<T> {
    pub fn as_slice(&self) -> &[T::N] {
        unsafe {
            from_raw_parts(
                transmute(self.data.buffers()[0].as_ptr()),
                self.data.cardinality(),
            )
        }
    }

    fn len(&self) -> usize {
        self.data.cardinality()
    }

    // pub fn from_slice<S>(input: S) -> Result<ArrayRef>
    // where
    //   S: AsRef<[T::N]>,
    // {
    //   let data_type = Arc::new(T::default());
    //   let mut boxed_array_builder = DataType::create_array_builder(data_type, input.as_ref().len())?;
    //   {
    //     let array_builder: &mut PrimitiveArrayBuilder<T> =
    //       downcast_mut(boxed_array_builder.as_mut())?;
    //
    //     for v in input.as_ref() {
    //       array_builder.append_value(Some(*v))?;
    //     }
    //   }
    //
    //   boxed_array_builder.finish()
    // }

    pub fn from_values<I>(input: I) -> Result<ArrayRef>
    where
        I: IntoIterator<Item = Option<T::N>>,
    {
        let data_type = T::default();
        let input = input.into_iter();

        // let mut boxed_array_builder = DataType::create_array_builder(data_type, input.size_hint().0)?;
        let mut array_builder =
            PrimitiveArrayBuilder::<T>::new(Arc::new(data_type), input.size_hint().0);

        for v in input {
            array_builder.append_value(v)?;
        }

        Box::new(array_builder).finish()
    }

    fn value_at(&self, idx: usize) -> Result<Option<T::N>> {
        self.check_idx(idx)?;

        // Justification
        // Already checked index.
        unsafe { Ok(self.value_at_unchecked(idx)) }
    }

    unsafe fn value_at_unchecked(&self, idx: usize) -> Option<T::N> {
        if self.is_null_unchecked(idx) {
            None
        } else {
            Some(self.as_slice()[idx])
        }
    }

    pub fn as_iter(&self) -> Result<impl Iterator<Item = Option<T::N>> + '_> {
        PrimitiveIter::new(self)
    }
}

impl<T: PrimitiveDataType> TryFrom<ArrayData> for PrimitiveArray<T> {
    type Error = RwError;

    fn try_from(data: ArrayData) -> Result<Self> {
        ensure!(T::DATA_TYPE_KIND == data.data_type().data_type_kind());
        ensure!(data.buffers().len() == 1);
        ensure!(data.buffers()[0].as_ptr().align_offset(align_of::<T::N>()) == 0);
        ensure!(data.buffers()[0].len() >= (size_of::<T::N>() * data.cardinality()));

        Ok(Self {
            data,
            _phantom: PhantomData,
        })
    }
}

impl<T: PrimitiveDataType> AsRef<dyn Any> for PrimitiveArray<T> {
    fn as_ref(&self) -> &dyn Any {
        self
    }
}

impl<T: PrimitiveDataType> AsMut<dyn Any> for PrimitiveArray<T> {
    fn as_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<T: PrimitiveDataType> Array for PrimitiveArray<T> {
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
            let mut output_buffer = Vec::<u8>::with_capacity(self.len() * size_of::<T::N>());

            for v in self.as_slice() {
                v.to_protobuf(&mut output_buffer)?;
            }

            let mut b = BufferProto::new();
            b.set_compression(Buffer_CompressionType::NONE);
            b.set_body(output_buffer);
            b
        };

        column.mut_values().push(values);

        AnyProto::pack(&column).map_err(|e| RwError::from(ProtobufError(e)))
    }
}

impl<T: PrimitiveDataType> AsRef<dyn Any> for PrimitiveArrayBuilder<T> {
    fn as_ref(&self) -> &dyn Any {
        self
    }
}

impl<T: PrimitiveDataType> AsMut<dyn Any> for PrimitiveArrayBuilder<T> {
    fn as_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl<T: PrimitiveDataType> ArrayBuilder for PrimitiveArrayBuilder<T> {
    fn append(&mut self, datum: &Datum) -> Result<()> {
        self.buffer.push(T::N::from_datum(datum)?);
        self.null_bitmap_buffer.push(true);
        Ok(())
    }

    fn append_array(&mut self, source: &dyn Array) -> Result<()> {
        let input: &PrimitiveArray<T> = downcast_ref(source)?;

        self.buffer.extend_from_slice(input.as_slice());
        if let Some(null_bitmap) = input.array_data().null_bitmap() {
            self.null_bitmap_buffer.extend(null_bitmap.iter());
        } else {
            for _ in 0..input.len() {
                self.null_bitmap_buffer.push(true);
            }
        }

        Ok(())
    }

    fn finish(self: Box<Self>) -> Result<ArrayRef> {
        let cardinality = self.buffer.len();
        let data_buffer = Buffer::from_slice(self.buffer)?;
        let null_bitmap = Bitmap::from_vec(self.null_bitmap_buffer)?;
        let array_data = ArrayData::builder()
            .data_type(self.data_type)
            .cardinality(cardinality)
            .null_count(0)
            .buffers(vec![data_buffer])
            .null_bitmap(null_bitmap)
            .build();

        PrimitiveArray::<T>::try_from(array_data).map(|arr| Arc::new(arr) as ArrayRef)
    }
}

impl<T: PrimitiveDataType> PrimitiveArrayBuilder<T> {
    pub fn new(data_type: Arc<T>, capacity: usize) -> Self {
        Self {
            data_type,
            buffer: Vec::with_capacity(capacity),
            null_bitmap_buffer: Vec::with_capacity(capacity),
        }
    }

    fn append_value_opt2(&mut self, value: T::N) -> Result<()> {
        self.buffer.push(value);
        self.null_bitmap_buffer.push(true);
        Ok(())
    }

    pub fn append_value(&mut self, value: Option<T::N>) -> Result<()> {
        match value {
            Some(v) => {
                self.buffer.push(v);
                self.null_bitmap_buffer.push(true);
            }
            None => {
                self.buffer.push(T::N::default());
                self.null_bitmap_buffer.push(false);
            }
        }

        Ok(())
    }
}

struct PrimitiveIter<'a, T: PrimitiveDataType> {
    array: &'a PrimitiveArray<T>,
    cur_pos: usize,
    end_pos: usize,
}

impl<'a, T: PrimitiveDataType> Iterator for PrimitiveIter<'a, T> {
    type Item = Option<T::N>;

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

impl<'a, T: PrimitiveDataType> PrimitiveIter<'a, T> {
    fn new(array: &'a PrimitiveArray<T>) -> Result<Self> {
        Ok(Self {
            array,
            cur_pos: 0,
            end_pos: array.len(),
        })
    }
}

impl<T: PrimitiveDataType> PrimitiveArray<T> {}
