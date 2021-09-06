use super::ArrayImpl;

use crate::buffer::Bitmap;
use crate::error::Result;
use risingwave_proto::data::DataChunk as DataChunkProto;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// `DataChunk` is a collection of arrays with visibility mask.
#[derive(TypedBuilder)]
pub struct DataChunk {
    #[builder(default)]
    pub(crate) arrays: Vec<ArrayImpl>,
    pub(crate) cardinality: usize,
    #[builder(default, setter(strip_option))]
    pub(crate) visibility: Option<Bitmap>,
}

impl DataChunk {
    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    pub fn visibility(&self) -> &Option<Bitmap> {
        &self.visibility
    }

    pub fn set_visibility(&mut self, visibility: Bitmap) {
        self.visibility = Some(visibility);
    }

    pub fn array_at(&self, idx: usize) -> &ArrayImpl {
        &self.arrays[idx]
    }

    pub fn to_protobuf(&self) -> Result<DataChunkProto> {
        unimplemented!()
    }
}

/// Create an empty data chunk
impl Default for DataChunk {
    fn default() -> Self {
        DataChunk {
            arrays: vec![],
            cardinality: 0,
            visibility: None,
        }
    }
}

pub type DataChunkRef = Arc<DataChunk>;
