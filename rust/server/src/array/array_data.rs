use crate::buffer::Bitmap;
use crate::buffer::Buffer;
use crate::types::DataTypeRef;

pub(crate) struct ArrayData {
    data_type: DataTypeRef,
    cardinality: usize,
    null_count: usize,
    buffers: Vec<Buffer>,
    bitmap: Option<Bitmap>,
}

impl ArrayData {
    pub(crate) fn cardinality(&self) -> usize {
        self.cardinality
    }
}
