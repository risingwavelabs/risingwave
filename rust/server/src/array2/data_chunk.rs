use crate::array2::column::Column;

use crate::buffer::Bitmap;
use crate::error::ErrorCode::InternalError;
use crate::error::Result;

use risingwave_proto::data::DataChunk as DataChunkProto;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// `DataChunk` is a collection of arrays with visibility mask.
#[derive(TypedBuilder)]
pub struct DataChunk {
    /// Use Vec to be consistent with previous array::DataChunk
    #[builder(default)]
    pub(crate) columns: Vec<Column>,
    // pub(crate) arrays: Vec<Arc<ArrayImpl>>,
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

    pub fn with_visibility(self, visibility: Bitmap) -> Self {
        DataChunk {
            columns: self.columns,
            cardinality: self.cardinality,
            visibility: Some(visibility),
        }
    }

    pub fn set_visibility(&mut self, visibility: Bitmap) {
        self.visibility = Some(visibility);
    }

    pub fn column_at(&self, idx: usize) -> Result<Column> {
        self.columns.get(idx).cloned().ok_or_else(|| {
            InternalError(format!(
                "Invalid array index: {}, chunk array count: {}",
                self.columns.len(),
                idx
            ))
            .into()
        })
    }

    pub fn to_protobuf(&self) -> Result<DataChunkProto> {
        ensure!(self.visibility.is_none());
        let mut proto = DataChunkProto::new();
        proto.set_cardinality(self.cardinality as u32);
        for arr in &self.columns {
            proto.mut_columns().push(arr.to_protobuf()?);
        }

        Ok(proto)
    }
}

/// Create an empty data chunk
impl Default for DataChunk {
    fn default() -> Self {
        DataChunk {
            columns: vec![],
            cardinality: 0,
            visibility: None,
        }
    }
}

pub type DataChunkRef = Arc<DataChunk>;
