// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::convert::TryFrom;
use std::fmt;
use std::hash::BuildHasher;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_pb::data::DataChunk as ProstDataChunk;

use crate::array::column::Column;
use crate::array::data_chunk_iter::{DataChunkRefIter, Row, RowRef};
use crate::array::{ArrayBuilderImpl, ArrayImpl};
use crate::buffer::Bitmap;
use crate::error::{Result, RwError};
use crate::types::DataType;
use crate::util::hash_util::finalize_hashers;

pub struct DataChunkBuilder {
    columns: Vec<Column>,
    visibility: Option<Bitmap>,
}

impl DataChunkBuilder {
    fn new() -> Self {
        DataChunkBuilder {
            columns: vec![],
            visibility: None,
        }
    }

    pub fn columns(self, columns: Vec<Column>) -> DataChunkBuilder {
        DataChunkBuilder {
            columns,
            visibility: self.visibility,
        }
    }

    pub fn visibility(self, visibility: Bitmap) -> DataChunkBuilder {
        DataChunkBuilder {
            columns: self.columns,
            visibility: Some(visibility),
        }
    }

    pub fn build(self) -> DataChunk {
        DataChunk::new(self.columns, self.visibility)
    }
}

/// `DataChunk` is a collection of arrays with visibility mask.
#[derive(Clone, Default)]
pub struct DataChunk {
    columns: Vec<Column>,
    visibility: Option<Bitmap>,
    cardinality: usize,
}

impl DataChunk {
    pub fn new(columns: Vec<Column>, visibility: Option<Bitmap>) -> Self {
        let cardinality = if let Some(bitmap) = &visibility {
            // with visibility bitmap
            let card = bitmap.iter().map(|visible| visible as usize).sum();
            for column in &columns {
                assert_eq!(bitmap.len(), column.array_ref().len())
            }
            card
        } else if !columns.is_empty() {
            // without visibility bitmap
            let card = columns.first().unwrap().array_ref().len();
            for column in columns.iter().skip(1) {
                assert_eq!(card, column.array_ref().len())
            }
            card
        } else {
            // no data (dummy)
            0
        };

        DataChunk {
            columns,
            visibility,
            cardinality,
        }
    }

    /// `new_dummy` creates a data chunk without columns but only a cardinality.
    pub fn new_dummy(cardinality: usize) -> Self {
        DataChunk {
            columns: vec![],
            visibility: None,
            cardinality,
        }
    }

    /// Build a `DataChunk` with rows.
    pub fn from_rows(rows: &[Row], data_types: &[DataType]) -> Result<Self> {
        let mut array_builders = data_types
            .iter()
            .map(|data_type| data_type.create_array_builder(1))
            .collect::<Result<Vec<_>>>()?;

        for row in rows {
            for (datum, builder) in row.0.iter().zip_eq(array_builders.iter_mut()) {
                builder.append_datum(datum)?;
            }
        }

        let new_arrays = array_builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<Result<Vec<_>>>()?;

        let new_columns = new_arrays
            .into_iter()
            .map(|array_impl| Column::new(Arc::new(array_impl)))
            .collect::<Vec<_>>();
        Ok(DataChunk::new(new_columns, None))
    }

    /// Return the next visible row index on or after `row_idx`.
    pub fn next_visible_row_idx(&self, row_idx: usize) -> Option<usize> {
        match &self.visibility {
            Some(vis) => vis.next_set_bit(row_idx),
            None => {
                if row_idx < self.cardinality {
                    Some(row_idx)
                } else {
                    None
                }
            }
        }
    }

    pub fn builder() -> DataChunkBuilder {
        DataChunkBuilder::new()
    }

    pub fn into_parts(self) -> (Vec<Column>, Option<Bitmap>) {
        (self.columns, self.visibility)
    }

    pub fn dimension(&self) -> usize {
        self.columns.len()
    }

    /// `cardinality` returns the number of visible tuples
    pub fn cardinality(&self) -> usize {
        self.cardinality
    }

    /// `capacity` returns physical length of any chunk column
    pub fn capacity(&self) -> usize {
        if let Some(bitmap) = &self.visibility {
            bitmap.len()
        } else {
            self.cardinality
        }
    }

    pub fn visibility(&self) -> &Option<Bitmap> {
        &self.visibility
    }

    #[must_use]
    pub fn with_visibility(&self, visibility: Bitmap) -> Self {
        DataChunk::new(self.columns.clone(), Some(visibility))
    }

    pub fn get_visibility_ref(&self) -> Option<&Bitmap> {
        self.visibility.as_ref()
    }

    pub fn set_visibility(&mut self, visibility: Bitmap) {
        let card = visibility.iter().map(|visible| visible as usize).sum();
        for column in &self.columns {
            assert_eq!(visibility.len(), column.array_ref().len())
        }
        self.visibility = Some(visibility);
        self.cardinality = card;
    }

    pub fn column_at(&self, idx: usize) -> &Column {
        &self.columns[idx]
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn to_protobuf(&self) -> ProstDataChunk {
        assert!(
            self.visibility.is_none(),
            "must be compacted before transfer"
        );
        let mut proto = ProstDataChunk {
            cardinality: self.cardinality() as u32,
            columns: Default::default(),
        };
        let column_ref = &mut proto.columns;
        for arr in &self.columns {
            column_ref.push(arr.to_protobuf());
        }

        proto
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
                        array
                            .compact(visibility, cardinality)
                            .map(|array| Column::new(Arc::new(array)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(Self::builder().columns(columns).build())
            }
        }
    }

    pub fn from_protobuf(proto: &ProstDataChunk) -> Result<Self> {
        if proto.columns.is_empty() {
            // Dummy chunk, we should deserialize cardinality
            Ok(DataChunk::new_dummy(proto.cardinality as usize))
        } else {
            let mut columns = vec![];
            for any_col in proto.get_columns() {
                let cardinality = proto.get_cardinality() as usize;
                columns.push(Column::from_protobuf(any_col, cardinality)?);
            }

            let chunk = DataChunk::new(columns, None);
            Ok(chunk)
        }
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
        let mut array_builders: Vec<ArrayBuilderImpl> = chunks[0]
            .columns
            .iter()
            .map(|col| col.array_ref().create_builder(new_chunk_require))
            .try_collect()?;
        let mut new_chunks = Vec::with_capacity(num_chunks);
        while chunk_idx < chunks.len() {
            let capacity = chunks[chunk_idx].capacity();
            let num_rows_left = capacity - start_row_idx;
            let actual_acquire = std::cmp::min(new_chunk_require, num_rows_left);
            let end_row_idx = start_row_idx + actual_acquire - 1;
            array_builders
                .iter_mut()
                .zip_eq(chunks[chunk_idx].columns())
                .try_for_each(|(builder, column)| {
                    let mut array_builder = column
                        .array_ref()
                        .create_builder(end_row_idx - start_row_idx + 1)?;
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
                    .map(|builder| {
                        let array = builder.finish()?;
                        Ok::<_, RwError>(Column::new(Arc::new(array)))
                    })
                    .try_collect()?;

                array_builders = new_columns
                    .iter()
                    .map(|col_type| col_type.array_ref().create_builder(new_chunk_require))
                    .try_collect()?;

                let data_chunk = DataChunk::builder().columns(new_columns).build();
                new_chunks.push(data_chunk);

                new_chunk_require = std::cmp::min(total_capacity, each_size_limit);
            }
        }

        Ok(new_chunks)
    }

    pub fn get_hash_values<H: BuildHasher>(
        &self,
        column_idxes: &[usize],
        hasher_builder: H,
    ) -> Result<Vec<u64>> {
        let mut states = Vec::with_capacity(self.capacity());
        states.resize_with(self.capacity(), || hasher_builder.build_hasher());
        for column_idx in column_idxes {
            let array = self.column_at(*column_idx).array();
            array.hash_vec(&mut states[..]);
        }
        Ok(finalize_hashers(&mut states[..]))
    }

    /// Get an iterator for visible rows.
    pub fn rows(&self) -> DataChunkRefIter<'_> {
        DataChunkRefIter::new(self)
    }

    /// Random access a tuple in a data chunk. Return in a row format.
    /// # Arguments
    /// * `pos` - Index of look up tuple
    /// * `RowRef` - Reference of data tuple
    /// * bool - whether this tuple is visible
    pub fn row_at(&self, pos: usize) -> Result<(RowRef<'_>, bool)> {
        let row = self.row_at_unchecked_vis(pos);
        let vis = match self.visibility.as_ref() {
            Some(bitmap) => bitmap.is_set(pos)?,
            None => true,
        };
        Ok((row, vis))
    }

    /// Random access a tuple in a data chunk. Return in a row format.
    /// Note that this function do not return whether the row is visible.
    /// # Arguments
    /// * `pos` - Index of look up tuple
    pub fn row_at_unchecked_vis(&self, pos: usize) -> RowRef<'_> {
        let mut row = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            row.push(column.array_ref().value_at(pos));
        }
        RowRef::new(row)
    }

    /// `to_pretty_string` returns a table-like text representation of the `DataChunk`.
    pub fn to_pretty_string(&self) -> String {
        use prettytable::{format, Table};
        let mut table = Table::new();
        table.set_format(*format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
        for row in self.rows() {
            let cells = row
                .0
                .iter()
                .map(|v| {
                    match v {
                        None => "".to_owned(), // null
                        Some(scalar) => scalar.to_string(),
                    }
                })
                .collect();
            table.add_row(cells);
        }
        table.to_string()
    }
}

impl fmt::Debug for DataChunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DataChunk {{ cardinality = {}, capacity = {}, data = \n{} }}",
            self.cardinality(),
            self.capacity(),
            self.to_pretty_string()
        )
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

        Ok(DataChunk::new(columns, None))
    }
}

pub type DataChunkRef = Arc<DataChunk>;

#[cfg(test)]
mod tests {
    use crate::array::column::Column;
    use crate::array::*;
    use crate::{column, column_nonnull};

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
                    .columns(vec![Column::new(Arc::new(
                        builder.finish().unwrap().into(),
                    ))])
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
            columns.push(Column::new(Arc::new(arr.into())))
        }
        let chunk: DataChunk = DataChunk::builder().columns(columns).build();
        for row in chunk.rows() {
            for i in 0..num_of_columns {
                let val = row.value_at(i).unwrap();
                assert_eq!(val.into_int32(), i as i32);
            }
        }
    }

    #[test]
    fn test_to_pretty_string() {
        let chunk = DataChunk::new(
            vec![
                column_nonnull!(I64Array, [1, 2, 3, 4]),
                column!(I64Array, [Some(6), None, Some(7), None]),
            ],
            None,
        );
        assert_eq!(
            chunk.to_pretty_string(),
            "\
+---+---+
| 1 | 6 |
| 2 |   |
| 3 | 7 |
| 4 |   |
+---+---+
"
        );
    }

    #[test]
    fn test_no_column_chunk() {
        let chunk = DataChunk::new_dummy(10);
        assert_eq!(chunk.rows().count(), 10);

        let chunk_after_serde = DataChunk::from_protobuf(&chunk.to_protobuf()).unwrap();
        assert_eq!(chunk_after_serde.rows().count(), 10);
        assert_eq!(chunk_after_serde.cardinality(), 10);
    }
}
