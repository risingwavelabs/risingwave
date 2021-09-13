use crate::array2::column::Column;

use crate::buffer::Bitmap;
use crate::error::ErrorCode::InternalError;
use crate::error::{ErrorCode, Result};
use protobuf::Message;
use risingwave_proto::data::{Column as ColumnProto, DataChunk as DataChunkProto};
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// `DataChunk` is a collection of arrays with visibility mask.
#[derive(Default, TypedBuilder)]
pub struct DataChunk {
    /// Use Vec to be consistent with previous array::DataChunk
    #[builder(default)]
    columns: Vec<Column>,
    // pub(crate) arrays: Vec<Arc<ArrayImpl>>,
    cardinality: usize,
    #[builder(default, setter(strip_option))]
    visibility: Option<Bitmap>,
}

impl DataChunk {
    pub fn new(columns: Vec<Column>, cardinality: usize, visibility: Option<Bitmap>) -> Self {
        DataChunk {
            columns,
            cardinality,
            visibility,
        }
    }

    pub fn destruct(self) -> (Vec<Column>, Option<Bitmap>) {
        (self.columns, self.visibility)
    }

    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    pub fn visibility(&self) -> &Option<Bitmap> {
        &self.visibility
    }

    pub fn with_visibility(&self, visibility: Bitmap) -> Self {
        DataChunk {
            columns: self.columns.clone(),
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

    /// `compact` will convert the chunk to compact format.
    /// Compact format means that `visibility == None`.
    pub fn compact(self) -> Result<Self> {
        match &self.visibility {
            None => Ok(self),
            Some(visibility) => {
                let cardinality = visibility
                    .iter()
                    .fold(0, |vis_cnt, vis| vis_cnt + vis as usize);
                let columns = self
                    .columns
                    .into_iter()
                    .map(|col| {
                        let array = col.array();
                        let data_type = col.data_type();
                        array
                            .compact(visibility, cardinality)
                            .map(|array| Column::new(Arc::new(array), data_type))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Self::builder()
                    .cardinality(cardinality)
                    .columns(columns)
                    .build())
            }
        }
    }

    pub fn from_protobuf(proto: &DataChunkProto) -> Result<Self> {
        let mut chunk = DataChunk {
            columns: vec![],
            cardinality: proto.get_cardinality() as usize,
            visibility: None,
        };

        for any_col in proto.get_columns() {
            let col = unpack_from_any!(any_col, ColumnProto);
            chunk
                .columns
                .push(Column::from_protobuf(col, chunk.cardinality)?);
        }
        Ok(chunk)
    }
}

pub type DataChunkRef = Arc<DataChunk>;
