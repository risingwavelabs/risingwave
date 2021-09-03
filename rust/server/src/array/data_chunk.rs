use crate::array::array::ArrayRef;
use crate::buffer::Bitmap;
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use risingwave_proto::data::DataChunk as DataChunkProto;
use std::sync::Arc;
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct DataChunk {
    #[builder(default)]
    arrays: Vec<ArrayRef>,
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

    pub fn with_visibility(&self, visibility: Bitmap) -> Self {
        DataChunk {
            arrays: self.arrays.clone(),
            cardinality: self.cardinality,
            visibility: Some(visibility),
        }
    }

    pub fn array_at(&self, idx: usize) -> Result<ArrayRef> {
        self.arrays.get(idx).cloned().ok_or_else(|| {
            InternalError(format!(
                "Invalid array index: {}, chunk array count: {}",
                self.arrays.len(),
                idx
            ))
            .into()
        })
    }

    pub fn to_protobuf(&self) -> Result<DataChunkProto> {
        ensure!(self.visibility.is_none());
        let mut proto = DataChunkProto::new();
        proto.set_cardinality(self.cardinality as u32);
        for arr in &self.arrays {
            proto.mut_columns().push(arr.to_protobuf()?);
        }

        Ok(proto)
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

pub type DataChunkRef = Arc<DataChunk>;
