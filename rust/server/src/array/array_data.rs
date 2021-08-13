use crate::buffer::Bitmap;
use crate::buffer::Buffer;
use crate::types::{DataType, DataTypeRef};
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub(crate) struct ArrayData {
    data_type: DataTypeRef,
    cardinality: usize,
    null_count: usize,
    buffers: Vec<Buffer>,
    #[builder(default, setter(strip_option))]
    null_bitmap: Option<Bitmap>,
}

impl ArrayData {
    pub(crate) fn cardinality(&self) -> usize {
        self.cardinality
    }

    pub(crate) fn data_type(&self) -> &dyn DataType {
        &*self.data_type
    }

    pub(crate) fn null_bitmap(&self) -> Option<&Bitmap> {
        self.null_bitmap.as_ref()
    }

    pub(crate) fn buffers(&self) -> &[Buffer] {
        &self.buffers
    }
}
