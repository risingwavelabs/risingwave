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

use std::hash::BuildHasher;
use std::sync::Arc;
use std::{fmt, iter};

use auto_enums::auto_enum;
use itertools::Itertools;
use risingwave_pb::data::DataChunk as ProstDataChunk;

use super::ArrayResult;
use crate::array::column::Column;
use crate::array::data_chunk_iter::{Row, RowRef};
use crate::array::ArrayBuilderImpl;
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::hash::HashCode;
use crate::types::{DataType, NaiveDateTimeWrapper, ToOwnedDatum};
use crate::util::hash_util::finalize_hashers;
use crate::util::value_encoding::serialize_datum_ref;

/// `DataChunk` is a collection of arrays with visibility mask.
#[derive(Clone, PartialEq)]
pub struct DataChunk {
    columns: Vec<Column>,
    vis2: Vis,
}

/// `Vis` is a visibility bitmap of rows. When all rows are visible, it is considered compact and
/// is represented by a single cardinality number rather than that many of ones.
#[derive(Clone, PartialEq, Debug)]
pub enum Vis {
    Bitmap(Bitmap),
    Compact(usize), // equivalent to all ones of this size
}

impl From<Bitmap> for Vis {
    fn from(b: Bitmap) -> Self {
        Vis::Bitmap(b)
    }
}

impl From<usize> for Vis {
    fn from(c: usize) -> Self {
        Vis::Compact(c)
    }
}

impl From<&Vis> for Vis {
    fn from(vis: &Vis) -> Self {
        match vis {
            Vis::Bitmap(b) => b.clone().into(),
            Vis::Compact(c) => (*c).into(),
        }
    }
}

impl Vis {
    pub fn is_empty(&self) -> bool {
        match self {
            Vis::Bitmap(b) => b.is_empty(),
            Vis::Compact(c) => *c == 0,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Vis::Bitmap(b) => b.len(),
            Vis::Compact(c) => *c,
        }
    }

    /// # Panics
    /// Panics if `idx > len`.
    pub fn is_set(&self, idx: usize) -> bool {
        match self {
            Vis::Bitmap(b) => b.is_set(idx).unwrap(),
            Vis::Compact(c) => {
                assert!(idx <= *c);
                true
            }
        }
    }

    #[auto_enum(Iterator)]
    pub fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        match self {
            Vis::Bitmap(b) => b.iter(),
            Vis::Compact(c) => iter::repeat(true).take(*c),
        }
    }
}

impl DataChunk {
    /// Create a `DataChunk` with `columns` and visibility. The visibility can either be a `Bitmap`
    /// or a simple cardinality number.
    pub fn new<V: Into<Vis>>(columns: Vec<Column>, vis: V) -> Self {
        let vis = vis.into();
        let capacity = match &vis {
            Vis::Bitmap(b) => b.len(),
            Vis::Compact(c) => *c,
        };
        for column in &columns {
            assert_eq!(capacity, column.array_ref().len());
        }

        DataChunk { columns, vis2: vis }
    }

    /// `new_dummy` creates a data chunk without columns but only a cardinality.
    pub fn new_dummy(cardinality: usize) -> Self {
        DataChunk {
            columns: vec![],
            vis2: Vis::Compact(cardinality),
        }
    }

    /// Build a `DataChunk` with rows.
    pub fn from_rows(rows: &[Row], data_types: &[DataType]) -> ArrayResult<Self> {
        let mut array_builders = data_types
            .iter()
            .map(|data_type| data_type.create_array_builder(1))
            .collect::<Vec<_>>();

        for row in rows {
            for (datum, builder) in row.0.iter().zip_eq(array_builders.iter_mut()) {
                builder.append_datum(datum)?;
            }
        }

        let new_columns = array_builders
            .into_iter()
            .map(|builder| builder.finish())
            .map(|array_impl| Column::new(Arc::new(array_impl)))
            .collect::<Vec<_>>();
        Ok(DataChunk::new(new_columns, rows.len()))
    }

    /// Return the next visible row index on or after `row_idx`.
    pub fn next_visible_row_idx(&self, row_idx: usize) -> Option<usize> {
        match &self.vis2 {
            Vis::Bitmap(vis) => vis.next_set_bit(row_idx),
            Vis::Compact(cardinality) => {
                if row_idx < *cardinality {
                    Some(row_idx)
                } else {
                    None
                }
            }
        }
    }

    pub fn into_parts(self) -> (Vec<Column>, Vis) {
        (self.columns, self.vis2)
    }

    pub fn dimension(&self) -> usize {
        self.columns.len()
    }

    /// `cardinality` returns the number of visible tuples
    pub fn cardinality(&self) -> usize {
        match &self.vis2 {
            Vis::Bitmap(b) => b.num_high_bits(),
            Vis::Compact(len) => *len,
        }
    }

    /// `capacity` returns physical length of any chunk column
    pub fn capacity(&self) -> usize {
        match &self.vis2 {
            Vis::Bitmap(b) => b.len(),
            Vis::Compact(len) => *len,
        }
    }

    pub fn vis(&self) -> &Vis {
        &self.vis2
    }

    #[must_use]
    pub fn with_visibility(&self, visibility: Bitmap) -> Self {
        DataChunk::new(self.columns.clone(), visibility)
    }

    pub fn visibility(&self) -> Option<&Bitmap> {
        self.get_visibility_ref()
    }

    pub fn get_visibility_ref(&self) -> Option<&Bitmap> {
        match &self.vis2 {
            Vis::Bitmap(b) => Some(b),
            Vis::Compact(_) => None,
        }
    }

    pub fn set_visibility(&mut self, visibility: Bitmap) {
        for column in &self.columns {
            assert_eq!(visibility.len(), column.array_ref().len())
        }
        self.vis2 = Vis::Bitmap(visibility);
    }

    pub fn column_at(&self, idx: usize) -> &Column {
        &self.columns[idx]
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn to_protobuf(&self) -> ProstDataChunk {
        assert!(
            matches!(self.vis2, Vis::Compact(_)),
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
    pub fn compact(self) -> ArrayResult<Self> {
        match &self.vis2 {
            Vis::Compact(_) => Ok(self),
            Vis::Bitmap(visibility) => {
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
                    .collect::<ArrayResult<Vec<_>>>()?;
                Ok(Self::new(columns, cardinality))
            }
        }
    }

    pub fn from_protobuf(proto: &ProstDataChunk) -> ArrayResult<Self> {
        let mut columns = vec![];
        for any_col in proto.get_columns() {
            let cardinality = proto.get_cardinality() as usize;
            columns.push(Column::from_protobuf(any_col, cardinality)?);
        }

        let chunk = DataChunk::new(columns, proto.cardinality as usize);
        Ok(chunk)
    }

    /// `rechunk` creates a new vector of data chunk whose size is `each_size_limit`.
    /// When the total cardinality of all the chunks is not evenly divided by the `each_size_limit`,
    /// the last new chunk will be the remainder.
    ///
    /// Currently, `rechunk` would ignore visibility map. May or may not support it later depending
    /// on the demand
    pub fn rechunk(chunks: &[DataChunk], each_size_limit: usize) -> ArrayResult<Vec<DataChunk>> {
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
        let mut array_len = new_chunk_require;
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
                    builder.append_array(&array_builder.finish())
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
                    .map(|builder| builder.finish().into())
                    .collect();

                array_builders = new_columns
                    .iter()
                    .map(|col_type| col_type.array_ref().create_builder(new_chunk_require))
                    .try_collect()?;

                let data_chunk = DataChunk::new(new_columns, array_len);
                new_chunks.push(data_chunk);

                new_chunk_require = std::cmp::min(total_capacity, each_size_limit);
                array_len = new_chunk_require;
            }
        }

        Ok(new_chunks)
    }

    pub fn get_hash_values<H: BuildHasher>(
        &self,
        column_idxes: &[usize],
        hasher_builder: H,
    ) -> ArrayResult<Vec<HashCode>> {
        let mut states = Vec::with_capacity(self.capacity());
        states.resize_with(self.capacity(), || hasher_builder.build_hasher());
        for column_idx in column_idxes {
            let array = self.column_at(*column_idx).array();
            array.hash_vec(&mut states[..]);
        }
        Ok(finalize_hashers(&mut states[..])
            .into_iter()
            .map(|hash_code| hash_code.into())
            .collect_vec())
    }

    /// Random access a tuple in a data chunk. Return in a row format.
    /// # Arguments
    /// * `pos` - Index of look up tuple
    /// * `RowRef` - Reference of data tuple
    /// * bool - whether this tuple is visible
    pub fn row_at(&self, pos: usize) -> ArrayResult<(RowRef<'_>, bool)> {
        let row = self.row_at_unchecked_vis(pos);
        let vis = match &self.vis2 {
            Vis::Bitmap(bitmap) => bitmap.is_set(pos)?,
            Vis::Compact(_) => true,
        };
        Ok((row, vis))
    }

    /// Random access a tuple in a data chunk. Return in a row format.
    /// Note that this function do not return whether the row is visible.
    /// # Arguments
    /// * `pos` - Index of look up tuple
    pub fn row_at_unchecked_vis(&self, pos: usize) -> RowRef<'_> {
        RowRef::new(self, pos)
    }

    /// `to_pretty_string` returns a table-like text representation of the `DataChunk`.
    pub fn to_pretty_string(&self) -> String {
        use comfy_table::Table;
        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++\n");
        for row in self.rows() {
            let cells: Vec<_> = row
                .values()
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

    /// Reorder (and possibly remove) columns. e.g. if `column_mapping` is `[2, 1, 0]`, and
    /// the chunk contains column `[a, b, c]`, then the output will be
    /// `[c, b, a]`. If `column_mapping` is [2, 0], then the output will be `[c, a]`
    /// If the input mapping is identity mapping, no reorder will be performed.
    pub fn reorder_columns(self, column_mapping: &[usize]) -> Self {
        if column_mapping
            .iter()
            .copied()
            .eq((0..self.columns().len()).into_iter())
        {
            return self;
        }
        let mut new_columns = Vec::with_capacity(column_mapping.len());
        for &idx in column_mapping {
            new_columns.push(self.columns[idx].clone());
        }
        Self {
            columns: new_columns,
            ..self
        }
    }

    /// Serialize each rows into value encoding bytes.
    ///
    /// the returned vector's size is self.capacity() and for the invisible row will give a empty
    /// vec<u8>
    pub fn serialize(&self) -> Vec<Vec<u8>> {
        match &self.vis2 {
            Vis::Bitmap(vis) => {
                let rows_num = vis.len();
                let mut buffers = vec![vec![]; rows_num];
                for c in &self.columns {
                    let c = c.array_ref();
                    assert_eq!(c.len(), rows_num);
                    for (i, buffer) in buffers.iter_mut().enumerate() {
                        // SAFETY(value_at_unchecked): the idx is always in bound.
                        unsafe {
                            if vis.is_set_unchecked(i) {
                                serialize_datum_ref(&c.value_at_unchecked(i), buffer);
                            }
                        }
                    }
                }
                buffers
            }
            Vis::Compact(rows_num) => {
                let mut buffers = vec![vec![]; *rows_num];
                for c in &self.columns {
                    let c = c.array_ref();
                    assert_eq!(c.len(), *rows_num);
                    for (i, buffer) in buffers.iter_mut().enumerate() {
                        // SAFETY(value_at_unchecked): the idx is always in bound.
                        unsafe {
                            serialize_datum_ref(&c.value_at_unchecked(i), buffer);
                        }
                    }
                }
                buffers
            }
        }
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

/// Test utilities for [`DataChunk`].
pub trait DataChunkTestExt {
    /// Parse a chunk from string.
    ///
    /// # Format
    ///
    /// The first line is a header indicating the column types.
    /// The following lines indicate rows within the chunk.
    /// Each line starts with an operation followed by values.
    /// NULL values are represented as `.`.
    ///
    /// # Example
    /// ```
    /// use risingwave_common::array::{DataChunk, DataChunkTestExt};
    /// let chunk = DataChunk::from_pretty(
    ///     "I I I I      // type chars
    ///      2 5 . .      // '.' means NULL
    ///      2 5 2 6 D    // 'D' means deleted in visibility
    ///      . . 4 8      // ^ comments are ignored
    ///      . . 3 4",
    /// );
    ///
    /// // type chars:
    /// //     I: i64
    /// //     i: i32
    /// //     F: f64
    /// //     f: f32
    /// //     T: str
    /// //    TS: Timestamp
    /// ```
    fn from_pretty(s: &str) -> Self;

    /// Insert one invisible hole after every record.
    fn with_invisible_holes(self) -> Self
    where
        Self: Sized;

    /// Panic if the chunk is invalid.
    fn assert_valid(&self);
}

impl DataChunkTestExt for DataChunk {
    fn from_pretty(s: &str) -> Self {
        use crate::types::ScalarImpl;

        let mut lines = s.split('\n').filter(|l| !l.trim().is_empty());
        // initialize array builders from the first line
        let header = lines.next().unwrap().trim();
        let mut array_builders = header
            .split_ascii_whitespace()
            .take_while(|c| *c != "//")
            .map(|c| match c {
                "I" => DataType::Int64,
                "i" => DataType::Int32,
                "F" => DataType::Float64,
                "f" => DataType::Float32,
                "TS" => DataType::Timestamp,
                "T" => DataType::Varchar,
                _ => todo!("unsupported type: {c:?}"),
            })
            .map(|ty| ty.create_array_builder(1))
            .collect::<Vec<_>>();
        let mut visibility = vec![];
        for mut line in lines {
            line = line.trim();
            let mut token = line.split_ascii_whitespace();
            // allow `zip` since `token` may longer than `array_builders`
            #[allow(clippy::disallowed_methods)]
            for (builder, val_str) in array_builders.iter_mut().zip(&mut token) {
                let datum = match val_str {
                    "." => None,
                    s if matches!(builder, ArrayBuilderImpl::Int32(_)) => Some(ScalarImpl::Int32(
                        s.parse()
                            .map_err(|_| panic!("invalid int32: {s:?}"))
                            .unwrap(),
                    )),
                    s if matches!(builder, ArrayBuilderImpl::Int64(_)) => Some(ScalarImpl::Int64(
                        s.parse()
                            .map_err(|_| panic!("invalid int64: {s:?}"))
                            .unwrap(),
                    )),
                    s if matches!(builder, ArrayBuilderImpl::Float32(_)) => {
                        Some(ScalarImpl::Float32(
                            s.parse()
                                .map_err(|_| panic!("invalid float32: {s:?}"))
                                .unwrap(),
                        ))
                    }
                    s if matches!(builder, ArrayBuilderImpl::Float64(_)) => {
                        Some(ScalarImpl::Float64(
                            s.parse()
                                .map_err(|_| panic!("invalid float64: {s:?}"))
                                .unwrap(),
                        ))
                    }
                    s if matches!(builder, ArrayBuilderImpl::NaiveDateTime(_)) => {
                        Some(ScalarImpl::NaiveDateTime(NaiveDateTimeWrapper(
                            s.parse()
                                .map_err(|_| panic!("invalid datetime: {s:?}"))
                                .unwrap(),
                        )))
                    }
                    s if matches!(builder, ArrayBuilderImpl::Utf8(_)) => {
                        Some(ScalarImpl::Utf8(s.into()))
                    }
                    _ => panic!("invalid data type"),
                };
                builder
                    .append_datum(&datum)
                    .expect("failed to append datum");
            }
            let visible = match token.next() {
                None | Some("//") => true,
                Some("D") => false,
                Some(t) => panic!("invalid token: {t:?}"),
            };
            visibility.push(visible);
        }
        let columns = array_builders
            .into_iter()
            .map(|builder| Column::new(Arc::new(builder.finish())))
            .collect();
        let vis = if visibility.iter().all(|b| *b) {
            Vis::Compact(visibility.len())
        } else {
            Vis::Bitmap(Bitmap::from_iter(visibility))
        };
        let chunk = DataChunk::new(columns, vis);
        chunk.assert_valid();
        chunk
    }

    fn with_invisible_holes(self) -> Self
    where
        Self: Sized,
    {
        let (cols, vis) = self.into_parts();
        let n = vis.len();
        let mut new_vis = BitmapBuilder::with_capacity(n * 2);
        for b in vis.iter() {
            new_vis.append(b);
            new_vis.append(false);
        }
        let new_cols = cols
            .into_iter()
            .map(|col| {
                let arr = col.array_ref();
                let mut builder = arr.create_builder(n * 2).unwrap();
                for v in arr.iter() {
                    builder.append_datum(&v.to_owned_datum()).unwrap();
                    builder.append_null().unwrap();
                }

                Column::new(builder.finish().into())
            })
            .collect();
        let chunk = DataChunk::new(new_cols, Vis::Bitmap(new_vis.finish()));
        chunk.assert_valid();
        chunk
    }

    fn assert_valid(&self) {
        let cols = self.columns();
        let vis = &self.vis2;
        let n = vis.len();
        for col in cols.iter() {
            assert_eq!(col.array().len(), n);
        }
    }
}

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
                let mut builder = PrimitiveArrayBuilder::<i32>::new(0);
                for i in chunk_size * chunk_idx..chunk_size * (chunk_idx + 1) {
                    builder.append(Some(i as i32)).unwrap();
                }
                let chunk = DataChunk::new(
                    vec![Column::new(Arc::new(builder.finish().into()))],
                    chunk_size,
                );
                chunks.push(chunk);
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
            let mut builder = PrimitiveArrayBuilder::<i32>::new(length);
            for _ in 0..length {
                builder.append(Some(i as i32)).unwrap();
            }
            let arr = builder.finish();
            columns.push(Column::new(Arc::new(arr.into())))
        }
        let chunk: DataChunk = DataChunk::new(columns, length);
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
            4,
        );
        assert_eq!(
            chunk.to_pretty_string(),
            "\
+---+---+
| 1 | 6 |
| 2 |   |
| 3 | 7 |
| 4 |   |
+---+---+"
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
    #[test]
    fn reorder_columns() {
        let chunk = DataChunk::from_pretty(
            "I I I
             2 5 1
             4 9 2
             6 9 3",
        );
        let reorder = chunk.clone().reorder_columns(&[2, 1, 0]);
        assert_eq!(
            reorder,
            DataChunk::from_pretty(
                "I I I
             1 5 2
             2 9 4
             3 9 6",
            )
        );
        let reorder = chunk.clone().reorder_columns(&[2, 0]);
        assert_eq!(
            reorder,
            DataChunk::from_pretty(
                "I I
             1 2
             2 4
             3 6",
            )
        );
        let reorder = chunk.clone().reorder_columns(&[0, 1, 2]);
        assert_eq!(reorder, chunk);
        let reorder = chunk.reorder_columns(&[]);
        assert_eq!(reorder.cardinality(), 3);
    }
}
