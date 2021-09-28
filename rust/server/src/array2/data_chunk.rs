use crate::array2::column::Column;

use crate::array2::data_chunk_iter::{DataChunkIter, DataTuple};
use crate::array2::ArrayImpl;
use crate::buffer::Bitmap;
use crate::error::ErrorCode::InternalError;
use crate::error::{ErrorCode, Result, RwError};
use crate::util::hash_util::finalize_hashers;
use itertools::Itertools;
use protobuf::Message;
use risingwave_proto::data::{Column as ColumnProto, DataChunk as DataChunkProto};
use std::convert::TryFrom;
use std::hash::BuildHasher;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// `DataChunk` is a collection of arrays with visibility mask.
#[derive(Default, TypedBuilder)]
pub struct DataChunk {
    /// Use Vec to be consistent with previous array::DataChunk
    #[builder(default)]
    columns: Vec<Column>,
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

    pub fn dimension(&self) -> usize {
        self.columns.len()
    }

    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    // return the number of visible tuples
    pub fn capacity(&self) -> usize {
        if let Some(bitmap) = &self.visibility {
            bitmap
                .iter()
                .fold(0, |cnt, nullable| cnt + nullable as usize);
        }
        self.cardinality()
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

    pub fn get_visibility_ref(&self) -> Option<&Bitmap> {
        self.visibility.as_ref()
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

    /// `rechunk` creates a new vector of data chunk whose size is `each_size_limit`.
    /// When the total cardinality of all the chunks is not evenly divided by the `each_size_limit`,
    /// the last new chunk will be the remainder.
    /// TODO: All data are copied twice now. We could save this optimization for the future.
    pub fn rechunk(chunks: &[DataChunkRef], each_size_limit: usize) -> Result<Vec<DataChunk>> {
        assert!(each_size_limit > 0);
        if chunks.is_empty() {
            return Ok(Vec::new());
        }
        assert!(!chunks[0].columns.is_empty());

        let total_cardinality = chunks
            .iter()
            .map(|chunk| chunk.cardinality())
            .reduce(|x, y| x + y)
            .unwrap();
        let num_chunks = (total_cardinality + each_size_limit - 1) / each_size_limit;
        // for each of the column in all the data chunks, merge them together into one single column
        let mut new_arrays = Vec::with_capacity(chunks[0].columns.len());
        for (col_idx, column) in chunks[0].columns.iter().enumerate() {
            let mut array_builder = column
                .data_type()
                .create_array_builder(total_cardinality)
                .unwrap();
            for each_chunk in chunks.iter() {
                array_builder
                    .append_array(each_chunk.column_at(col_idx).unwrap().array_ref())
                    .unwrap();
            }
            new_arrays.push(array_builder.finish().unwrap());
        }

        let mut new_chunks = Vec::with_capacity(num_chunks);
        for chunk_idx in 0..num_chunks {
            let mut new_columns = Vec::with_capacity(chunks[0].columns.len());
            let start_idx = each_size_limit * chunk_idx;
            let end_idx = std::cmp::min(
                each_size_limit * (chunk_idx + 1) - 1,
                new_arrays[0].len() - 1,
            );
            for (new_array, col) in new_arrays.iter().zip(chunks[0].columns.iter()) {
                let column = Column::new(
                    Arc::new(new_array.get_continuous_sub_array(start_idx, end_idx)),
                    col.data_type(),
                );
                new_columns.push(column);
            }
            let cardinality = end_idx - start_idx + 1;
            let new_chunk = DataChunk::builder()
                .cardinality(cardinality)
                .columns(new_columns)
                .build();
            new_chunks.push(new_chunk);
        }
        Ok(new_chunks)
    }
    pub fn get_hash_values<H: BuildHasher>(
        &self,
        keys: &[usize],
        hasher_builder: H,
    ) -> Result<Vec<u64>> {
        let mut states = Vec::with_capacity(self.cardinality());
        states.resize_with(self.cardinality(), || hasher_builder.build_hasher());
        for key in keys {
            let array = self.column_at(*key)?.array();
            array.hash_vec(&mut states);
        }
        Ok(finalize_hashers(&mut states))
    }

    /// Iterate for each row. The iterator will return all tuples (include visible and invisible).
    pub fn iter(&self) -> DataChunkIter<'_> {
        DataChunkIter::new(self)
    }

    // The overflow should be checked by up layer.
    pub fn row_at(&self, pos: usize) -> DataTuple<'_> {
        let mut row = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            row.push(column.array_ref().value_at(pos));
        }
        DataTuple::new(row)
    }
}

impl TryFrom<Vec<Column>> for DataChunk {
    type Error = RwError;

    fn try_from(columns: Vec<Column>) -> Result<Self> {
        ensure!(!columns.is_empty(), "Columns can't be empty!");
        ensure!(
            columns
                .iter()
                .map(Column::array_ref)
                .map(ArrayImpl::len)
                .all_equal(),
            "Not all columns length same!"
        );

        let cardinality = columns[0].array_ref().len();
        Ok(Self {
            columns,
            cardinality,
            visibility: None,
        })
    }
}

pub type DataChunkRef = Arc<DataChunk>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array2::*;
    use crate::types::Int32Type;

    #[test]
    fn test_rechunk() {
        let num_chunks = 10;
        let chunk_size = 60;
        let mut chunks = vec![];
        for chunk_idx in 0..num_chunks {
            let mut builder = PrimitiveArrayBuilder::<i32>::new(0).unwrap();
            for i in chunk_size * chunk_idx..chunk_size * (chunk_idx + 1) {
                builder.append(Some(i as i32)).unwrap();
            }
            let chunk = DataChunk::builder()
                .cardinality(chunk_size)
                .columns(vec![Column::new(
                    Arc::new(builder.finish().unwrap().into()),
                    Arc::new(Int32Type::new(false)),
                )])
                .build();
            chunks.push(Arc::new(chunk));
        }

        let total_card = num_chunks * chunk_size;
        let new_chunk_size = 80;
        let chunk_sizes = vec![80, 80, 80, 80, 80, 80, 80, 40];
        let new_chunks = DataChunk::rechunk(&chunks, new_chunk_size).unwrap();
        assert_eq!(new_chunks.len(), chunk_sizes.len());
        // check cardinality
        for (idx, chunk_size) in chunk_sizes.iter().enumerate() {
            assert_eq!(*chunk_size, new_chunks[idx].cardinality());
        }

        let mut chunk_idx = 0;
        let mut cur_idx = 0;
        for val in 0..total_card {
            if cur_idx >= chunk_sizes[chunk_idx] {
                cur_idx = 0;
                chunk_idx += 1;
            }
            assert_eq!(
                new_chunks[chunk_idx]
                    .column_at(0)
                    .unwrap()
                    .array()
                    .as_int32()
                    .value_at(cur_idx)
                    .unwrap(),
                val as i32
            );
            cur_idx += 1;
        }
    }

    #[test]
    fn test_chunk_iter() {
        let num_of_columns: usize = 2;
        let length = 5;
        let mut columns = vec![];
        for i in 0..num_of_columns {
            let mut builder = PrimitiveArrayBuilder::<i32>::new(length).unwrap();
            for _ in 0..length {
                builder.append(Some(i as i32)).unwrap();
            }
            let arr = builder.finish().unwrap();
            columns.push(Column::new(
                Arc::new(arr.into()),
                Arc::new(Int32Type::new(false)),
            ))
        }
        let chunk: DataChunk = DataChunk::builder()
            .cardinality(length)
            .columns(columns)
            .build();
        for row in chunk.iter() {
            for i in 0..num_of_columns {
                let val = row.value_at(i).unwrap();
                assert_eq!(val.into_int32(), i as i32);
            }
        }
    }
}
