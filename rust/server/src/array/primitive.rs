use crate::array::array_data::ArrayData;
use crate::array::{Array, ArrayBuilder, ArrayRef};
use crate::buffer::{Bitmap, Buffer};
use crate::error::Result;
use crate::expr::Datum;
use crate::types::{DataType, DataTypeRef, NativeType, PrimitiveDataType};
use std::marker::PhantomData;
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
    fn new(data: ArrayData) -> Self {
        Self {
            data,
            _phantom: PhantomData,
        }
    }
}

impl<T: PrimitiveDataType> Array for PrimitiveArray<T> {
    fn data_type(&self) -> &dyn DataType {
        self.data.data_type()
    }

    fn array_data(&self) -> &ArrayData {
        &self.data
    }
}

impl<T: PrimitiveDataType> ArrayBuilder for PrimitiveArrayBuilder<T> {
    fn append(&mut self, datum: &Datum) -> Result<()> {
        self.buffer.push(T::N::from_datum(datum)?);
        self.null_bitmap_buffer.push(true);
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

        let array = PrimitiveArray::<T>::new(array_data);
        Ok(Arc::new(array) as ArrayRef)
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
}
