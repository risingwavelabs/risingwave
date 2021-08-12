use crate::array::array::ArrayRef;
use crate::buffer::Bitmap;
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub(crate) struct DataChunk {
    #[builder(default)]
    arrays: Vec<ArrayRef>,
    cardinality: usize,
    #[builder(default, setter(strip_option))]
    visibility: Option<Bitmap>,
}

impl DataChunk {
    pub(crate) fn cardinality(&self) -> usize {
        self.cardinality
    }

    pub(crate) fn array_at(&self, idx: usize) -> Result<ArrayRef> {
        self.arrays.get(idx).cloned().ok_or_else(|| {
            InternalError(format!(
                "Invalid array index: {}, chunk array count: {}",
                self.arrays.len(),
                idx
            ))
            .into()
        })
    }
}

/// Create an empty data chunk
impl Default for DataChunk {
    fn default() -> Self {
        DataChunk {
            arrays: Vec::new(),
            cardinality: 0,
            visibility: None,
        }
    }
}

pub(crate) type DataChunkRef = Arc<DataChunk>;
