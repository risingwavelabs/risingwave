use crate::array::array_data::ArrayData;
use crate::array::{Array, ArrayBuilder, ArrayRef};
use crate::buffer::{Bitmap, Buffer};
use crate::error::ErrorCode::{InternalError, ProtobufError};
use crate::error::Result;
use crate::error::RwError;
use crate::expr::Datum;
use crate::types::{DataType, DataTypeRef, NativeType, PrimitiveDataType};
use protobuf::well_known_types::Any as AnyProto;
use risingwave_proto::data::Buffer as BufferProto;
use risingwave_proto::data::{Buffer_CompressionType, ColumnCommon, FixedWidthColumn};
use std::any::Any;
use std::convert::TryFrom;
use std::marker::PhantomData;
use std::mem::transmute;
use std::mem::{align_of, size_of};
use std::slice::from_raw_parts;
use std::sync::Arc;

/// A primitive array contains only one value buffer, and an optional bitmap buffer
pub(crate) struct PrimitiveArray<T: PrimitiveDataType> {
    data: ArrayData,
    _phantom: PhantomData<T>,
}

pub(crate) struct PrimitiveArrayBuilder<T: PrimitiveDataType> {
    data_type: DataTypeRef,
    buffer: Vec<T::N>,
    null_bitmap_buffer: Vec<bool>,
}

impl<T: PrimitiveDataType> PrimitiveArray<T> {
    fn as_slice(&self) -> &[T::N] {
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

    pub(crate) fn from_slice<S>(input: S) -> Result<ArrayRef>
    where
        S: AsRef<[T::N]>,
    {
        let data_type = Arc::new(T::default());
        let mut array_builder = DataType::create_array_builder(data_type, input.as_ref().len())?;
        {
            let array_builder = array_builder
                .as_any_mut()
                .downcast_mut::<PrimitiveArrayBuilder<T>>()
                .ok_or_else(|| {
                    InternalError(format!(
                        "Failed to downcast input array builder: {:?}",
                        T::DATA_TYPE_KIND
                    ))
                })?;

            for v in input.as_ref() {
                array_builder.append_value(*v)?;
            }
        }

        array_builder.finish()
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

impl<T: PrimitiveDataType> Array for PrimitiveArray<T> {
    fn data_type(&self) -> &dyn DataType {
        self.data.data_type()
    }

    fn array_data(&self) -> &ArrayData {
        &self.data
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self as &dyn std::any::Any
    }

    fn to_protobuf(&self) -> Result<AnyProto> {
        let proto_data_type = self.data.data_type().to_protobuf()?;
        let mut column_common = ColumnCommon::new();
        column_common.set_column_type(proto_data_type);
        if let Some(null_bitmap) = self.data.null_bitmap() {
            column_common.set_null_bitmap(null_bitmap.to_protobuf()?);
        }

        let mut column = FixedWidthColumn::new();
        column.set_common_parts(column_common);

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

        column.set_value_width(size_of::<T::N>() as u64);
        column.set_values(values);

        AnyProto::pack(&column).map_err(|e| RwError::from(ProtobufError(e)))
    }
}

impl<T: PrimitiveDataType> ArrayBuilder for PrimitiveArrayBuilder<T> {
    fn append(&mut self, datum: &Datum) -> Result<()> {
        self.buffer.push(T::N::from_datum(datum)?);
        self.null_bitmap_buffer.push(true);
        Ok(())
    }

    fn append_array(&mut self, source: &dyn Array) -> Result<()> {
        let input = source
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                InternalError(format!(
                    "Can't append array {:?} into array builder {:?}",
                    &*self.data_type,
                    source.data_type()
                ))
            })?;

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

    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self as &mut dyn Any
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
    pub(crate) fn new(data_type: Arc<T>, capacity: usize) -> Self {
        Self {
            data_type,
            buffer: Vec::with_capacity(capacity),
            null_bitmap_buffer: Vec::with_capacity(capacity),
        }
    }

    fn append_value(&mut self, value: T::N) -> Result<()> {
        self.buffer.push(value);
        self.null_bitmap_buffer.push(true);
        Ok(())
    }
}
