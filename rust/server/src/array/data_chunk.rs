use crate::array::array::ArrayRef;
use crate::buffer::Bitmap;
use std::sync::Arc;

pub(crate) struct DataChunk {
    arrays: Vec<ArrayRef>,
    cardinality: usize,
    visibility: Option<Bitmap>,
}

impl DataChunk {
    pub(crate) fn cardinality(&self) -> usize {
        self.cardinality
    }
}

pub(crate) type DataChunkRef = Arc<DataChunk>;
