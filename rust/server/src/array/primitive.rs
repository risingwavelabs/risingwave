use crate::array::array_data::ArrayData;
use crate::types::PrimitiveDataType;
use std::marker::PhantomData;

/// A primitive array contains only one value buffer, and an optional bitmap buffer
pub(crate) struct PrimitiveArray<T: PrimitiveDataType> {
    data: ArrayData,
    _phantom: PhantomData<T>,
}
