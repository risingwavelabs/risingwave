use crate::array::column::Column;
use crate::array::data_chunk_iter::{DataChunkRefIter, RowRef};
use crate::array::{ArrayBuilderImpl, ArrayImpl};
use crate::buffer::Bitmap;
use crate::error::ErrorCode::InternalError;
use crate::error::{ErrorCode, Result, RwError};
use crate::unpack_from_any;
use crate::util::hash_util::finalize_hashers;
use itertools::Itertools;
use protobuf::Message;
use risingwave_proto::data::{Column as ColumnProto, DataChunk as DataChunkProto};
use std::convert::TryFrom;
use std::hash::BuildHasher;
use std::sync::Arc;
use typed_builder::TypedBuilder;

/// `DataChunk` is a collection of arrays with visibility mask.
#[derive(Clone, Default, Debug, TypedBuilder)]
pub struct DataChunk {
    #[builder(default)]
    columns: Vec<Column>,
    #[builder(default, setter(strip_option))]
    visibility: Option<Bitmap>,
}

impl DataChunk {
    pub fn new(columns: Vec<Column>, visibility: Option<Bitmap>) -> Self {
        DataChunk {
            columns,
            visibility,
        }
    }

    pub fn into_parts(self) -> (Vec<Column>, Option<Bitmap>) {
        (self.columns, self.visibility)
    }

    pub fn dimension(&self) -> usize {
        self.columns.len()
    }

    /// return the number of visible tuples
    pub fn cardinality(&self) -> usize {
        if let Some(bitmap) = &self.visibility {
            bitmap.iter().map(|visible| visible as usize).sum()
        } else {
            self.capacity()
        }
    }

    /// return physical length of any chunk column
    pub fn capacity(&self) -> usize {
        self.columns
            .first()
            .map(|col| col.array_ref().len())
            .unwrap_or(0)
    }

    pub fn visibility(&self) -> &Option<Bitmap> {
        &self.visibility
    }

    pub fn with_visibility(&self, visibility: Bitmap) -> Self {
        DataChunk {
            columns: self.columns.clone(),
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

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn to_protobuf(&self) -> Result<DataChunkProto> {
        ensure!(self.visibility.is_none());
        let mut proto = DataChunkProto::new();
        proto.set_cardinality(self.cardinality() as u32);
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
                Ok(Self::builder().columns(columns).build())
            }
        }
    }

    pub fn from_protobuf(proto: &DataChunkProto) -> Result<Self> {
        let mut chunk = DataChunk {
            columns: vec![],
            visibility: None,
        };

        for any_col in proto.get_columns() {
            let col = unpack_from_any!(any_col, ColumnProto);
            let cardinality = proto.get_cardinality() as usize;
            chunk.columns.push(Column::from_protobuf(col, cardinality)?);
        }
        Ok(chunk)
    }

    /// `rechunk` creates a new vector of data chunk whose size is `each_size_limit`.
    /// When the total cardinality of all the chunks is not evenly divided by the `each_size_limit`,
    /// the last new chunk will be the remainder.
    /// Currently, `rechunk` would ignore visibility map. May or may not support it later depending
    /// on the demand
    pub fn rechunk(chunks: &[DataChunkRef], each_size_limit: usize) -> Result<Vec<DataChunk>> {
        assert!(each_size_limit > 0);
        // Corner case: one of the `chunks` may have 0 length
        // remove the chunks with zero physical length here,
        // or skip them in the loop below
        let chunks = chunks
            .iter()
            .filter(|chunk| chunk.capacity() != 0)
            .collect::<Vec<_>>();
        if chunks.is_empty() {
            return Ok(Vec::new());
        }
        assert!(!chunks[0].columns.is_empty());

        let column_types = chunks[0]
            .columns
            .iter()
            .map(|col| col.data_type())
            .collect::<Vec<_>>();

        let mut total_capacity = chunks
            .iter()
            .map(|chunk| chunk.capacity())
            .reduce(|x, y| x + y)
            .unwrap();
        let num_chunks = (total_capacity + each_size_limit - 1) / each_size_limit;

        // the idx of `chunks`
        let mut chunk_idx = 0;
        // the row idx of `chunks[chunk_idx]`
        let mut start_row_idx = 0;
        // how many rows does this new chunk need?
        let mut new_chunk_require = std::cmp::min(total_capacity, each_size_limit);
        let mut array_builders: Vec<ArrayBuilderImpl> = column_types
            .iter()
            .map(|col_type| col_type.create_array_builder(new_chunk_require))
            .try_collect()?;
        let mut new_chunks = Vec::with_capacity(num_chunks);
        while chunk_idx < chunks.len() {
            let capacity = chunks[chunk_idx].capacity();
            let num_rows_left = capacity - start_row_idx;
            let actual_acquire = std::cmp::min(new_chunk_require, num_rows_left);
            let end_row_idx = start_row_idx + actual_acquire - 1;
            array_builders
                .iter_mut()
                .zip(chunks[chunk_idx].columns())
                .try_for_each(|(builder, column)| {
                    let mut array_builder = column
                        .data_type_ref()
                        .create_array_builder(end_row_idx - start_row_idx + 1)?;
                    for row_idx in start_row_idx..=end_row_idx {
                        array_builder.append_datum_ref(column.array_ref().value_at(row_idx))?;
                    }
                    builder.append_array(&array_builder.finish()?)
                })?;
            // since `end_row_idx` is inclusive, exclude it for the next round.
            start_row_idx = end_row_idx + 1;
            // if the current `chunks[chunk_idx] is used up, move to the next one
            if start_row_idx == capacity {
                chunk_idx += 1;
                start_row_idx = 0;
            }
            new_chunk_require -= actual_acquire;
            total_capacity -= actual_acquire;
            // a new chunk receives enough rows, finalize it
            if new_chunk_require == 0 {
                let new_columns: Vec<Column> = array_builders
                    .drain(..)
                    .zip(column_types.iter())
                    .map(|(builder, col_type)| {
                        let array = builder.finish()?;
                        Ok::<_, RwError>(Column::new(Arc::new(array), col_type.clone()))
                    })
                    .try_collect()?;

                let data_chunk = DataChunk::builder().columns(new_columns).build();
                new_chunks.push(data_chunk);

                array_builders = column_types
                    .iter()
                    .map(|col_type| col_type.create_array_builder(new_chunk_require))
                    .try_collect()?;
                new_chunk_require = std::cmp::min(total_capacity, each_size_limit);
            }
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

    /// Get an iterator for visible rows.
    pub fn iter(&self) -> DataChunkRefIter<'_> {
        DataChunkRefIter::new(self)
    }

    /// Random access a tuple in a data chunk. Return in a row format.
    /// # Arguments
    /// * `pos` - Index of look up tuple
    /// * `RowRef` - Reference of data tuple
    /// * bool - whether this tuple is visible
    pub fn row_at(&self, pos: usize) -> Result<(RowRef<'_>, bool)> {
        let mut row = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            row.push(column.array_ref().value_at(pos));
        }
        let row = RowRef::new(row);
        let vis = match self.visibility.as_ref() {
            Some(bitmap) => bitmap.is_set(pos)?,
            None => true,
        };
        Ok((row, vis))
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

        Ok(Self {
            columns,
            visibility: None,
        })
    }
}

pub type DataChunkRef = Arc<DataChunk>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::*;
    use crate::types::Int32Type;

    #[test]
    fn test_rechunk() {
        let test_case = |num_chunks: usize, chunk_size: usize, new_chunk_size: usize| {
            let mut chunks = vec![];
            for chunk_idx in 0..num_chunks {
                let mut builder = PrimitiveArrayBuilder::<i32>::new(0).unwrap();
                for i in chunk_size * chunk_idx..chunk_size * (chunk_idx + 1) {
                    builder.append(Some(i as i32)).unwrap();
                }
                let chunk = DataChunk::builder()
                    .columns(vec![Column::new(
                        Arc::new(builder.finish().unwrap().into()),
                        Int32Type::create(false),
                    )])
                    .build();
                chunks.push(Arc::new(chunk));
            }

            let total_size = num_chunks * chunk_size;
            let num_full_new_chunk = total_size / new_chunk_size;
            let mut chunk_sizes = vec![new_chunk_size; num_full_new_chunk];
            let remainder = total_size % new_chunk_size;
            if remainder != 0 {
                chunk_sizes.push(remainder);
            }

            let new_chunks = DataChunk::rechunk(&chunks, new_chunk_size).unwrap();
            assert_eq!(new_chunks.len(), chunk_sizes.len());
            // check cardinality
            for (idx, chunk_size) in chunk_sizes.iter().enumerate() {
                assert_eq!(*chunk_size, new_chunks[idx].capacity());
            }

            let mut chunk_idx = 0;
            let mut cur_idx = 0;
            for val in 0..total_size {
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
        };

        test_case(0, 0, 1);
        test_case(0, 10, 1);
        test_case(10, 0, 1);
        test_case(1, 1, 6);
        test_case(1, 10, 11);
        test_case(2, 3, 6);
        test_case(5, 5, 6);
        test_case(10, 10, 7);
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
        let chunk: DataChunk = DataChunk::builder().columns(columns).build();
        for row in chunk.iter() {
            for i in 0..num_of_columns {
                let val = row.value_at(i).unwrap();
                assert_eq!(val.into_int32(), i as i32);
            }
        }
    }
}
