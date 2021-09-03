use super::ArrayImpl;

use crate::buffer::Bitmap;
use crate::error::Result;
use risingwave_proto::data::DataChunk as DataChunkProto;
use smallvec::{smallvec, SmallVec};
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// `DataChunk` is a collection of arrays with visibility mask.
#[derive(TypedBuilder)]
pub struct DataChunk {
    #[builder(default)]
    arrays: SmallVec<[ArrayImpl; 16]>,
    cardinality: usize,
    #[builder(default, setter(strip_option))]
    visibility: Option<Bitmap>,
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
            arrays: smallvec![],
            cardinality: 0,
            visibility: None,
        }
    }
}

pub type DataChunkRef = Arc<DataChunk>;
