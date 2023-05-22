// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::hash::BuildHasher;
use std::sync::Arc;
use std::{fmt, usize};

use bytes::Bytes;
use itertools::Itertools;
use risingwave_pb::data::PbDataChunk;

use super::{Array, ArrayImpl, ArrayRef, ArrayResult, StructArray, Vis};
use crate::array::data_chunk_iter::RowRef;
use crate::array::ArrayBuilderImpl;
use crate::buffer::{Bitmap, BitmapBuilder};
use crate::estimate_size::EstimateSize;
use crate::field_generator::{FieldGeneratorImpl, VarcharProperty};
use crate::hash::HashCode;
use crate::row::Row;
use crate::types::{DataType, DatumRef, StructType, ToOwnedDatum, ToText};
use crate::util::hash_util::finalize_hashers;
use crate::util::iter_util::{ZipEqDebug, ZipEqFast};
use crate::util::value_encoding::{
    estimate_serialize_datum_size, serialize_datum_into, try_get_exact_serialize_datum_size,
    ValueRowSerializer,
};

/// [`DataChunk`] is a collection of Columns,
/// a with visibility mask for each row.
/// For instance, we could have a [`DataChunk`] of this format.
///
/// | v1 | v2 | v3 |
/// |----|----|----|
/// | 1  | a  | t  |
/// | 2  | b  | f  |
/// | 3  | c  | t  |
/// | 4  | d  | f  |
///
/// Our columns are v1, v2, v3.
/// Then, if the Visibility Mask hides rows 2 and 4,
/// We will only have these rows visible:
///
/// | v1 | v2 | v3 |
/// |----|----|----|
/// | 1  | a  | t  |
/// | 3  | c  | t  |
#[derive(Clone, PartialEq)]
#[must_use]
pub struct DataChunk {
    columns: Vec<ArrayRef>,
    vis2: Vis,
}

impl DataChunk {
    /// Create a `DataChunk` with `columns` and visibility. The visibility can either be a `Bitmap`
    /// or a simple cardinality number.
    pub fn new<V: Into<Vis>>(columns: Vec<ArrayRef>, vis: V) -> Self {
        let vis: Vis = vis.into();
        let capacity = vis.len();
        for column in &columns {
            assert_eq!(capacity, column.len());
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
    pub fn from_rows(rows: &[impl Row], data_types: &[DataType]) -> Self {
        let mut array_builders = data_types
            .iter()
            .map(|data_type| data_type.create_array_builder(1))
            .collect::<Vec<_>>();

        for row in rows {
            for (datum, builder) in row.iter().zip_eq_debug(array_builders.iter_mut()) {
                builder.append_datum(datum);
            }
        }

        let new_columns = array_builders
            .into_iter()
            .map(|builder| builder.finish().into())
            .collect::<Vec<_>>();
        DataChunk::new(new_columns, rows.len())
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

    pub fn into_parts(self) -> (Vec<ArrayRef>, Vis) {
        (self.columns, self.vis2)
    }

    pub fn dimension(&self) -> usize {
        self.columns.len()
    }

    /// `cardinality` returns the number of visible tuples
    pub fn cardinality(&self) -> usize {
        match &self.vis2 {
            Vis::Bitmap(b) => b.count_ones(),
            Vis::Compact(len) => *len,
        }
    }

    /// `capacity` returns physical length of any chunk column
    pub fn capacity(&self) -> usize {
        self.vis2.len()
    }

    pub fn vis(&self) -> &Vis {
        &self.vis2
    }

    pub fn selectivity(&self) -> f64 {
        match &self.vis2 {
            Vis::Bitmap(b) => {
                if b.is_empty() {
                    0.0
                } else {
                    b.count_ones() as f64 / b.len() as f64
                }
            }
            Vis::Compact(_) => 1.0,
        }
    }

    pub fn with_visibility(&self, visibility: Bitmap) -> Self {
        DataChunk::new(self.columns.clone(), visibility)
    }

    pub fn visibility(&self) -> Option<&Bitmap> {
        self.vis2.as_visibility()
    }

    pub fn set_vis(&mut self, vis: Vis) {
        for column in &self.columns {
            assert_eq!(vis.len(), column.len())
        }
        self.vis2 = vis;
    }

    pub fn set_visibility(&mut self, visibility: Bitmap) {
        for column in &self.columns {
            assert_eq!(visibility.len(), column.len())
        }
        self.vis2 = Vis::Bitmap(visibility);
    }

    pub fn column_at(&self, idx: usize) -> &ArrayRef {
        &self.columns[idx]
    }

    pub fn columns(&self) -> &[ArrayRef] {
        &self.columns
    }

    /// Divides one chunk into two at an column index.
    ///
    /// # Panics
    ///
    /// Panics if `idx > columns.len()`.
    pub fn split_column_at(&self, idx: usize) -> (Self, Self) {
        let (left, right) = self.columns.split_at(idx);
        let left = DataChunk::new(left.to_vec(), self.vis2.clone());
        let right = DataChunk::new(right.to_vec(), self.vis2.clone());
        (left, right)
    }

    pub fn to_protobuf(&self) -> PbDataChunk {
        assert!(
            matches!(self.vis2, Vis::Compact(_)),
            "must be compacted before transfer"
        );
        let mut proto = PbDataChunk {
            cardinality: self.cardinality() as u32,
            columns: Default::default(),
        };
        let column_ref = &mut proto.columns;
        for array in &self.columns {
            column_ref.push(array.to_protobuf());
        }
        proto
    }

    /// `compact` will convert the chunk to compact format.
    /// Compacting removes the hidden rows, and returns a new visibility
    /// mask which indicates this.
    ///
    /// `compact` has trade-offs:
    ///
    /// Cost:
    /// It has to rebuild the each column, meaning it will incur cost
    /// of copying over bytes from the original column array to the new one.
    ///
    /// Benefit:
    /// The main benefit is that the data chunk is smaller, taking up less memory.
    /// We can also save the cost of iterating over many hidden rows.
    pub fn compact(self) -> Self {
        match &self.vis2 {
            Vis::Compact(_) => self,
            Vis::Bitmap(visibility) => {
                let cardinality = visibility.count_ones();
                let columns = self
                    .columns
                    .into_iter()
                    .map(|col| {
                        let array = col;
                        array.compact(visibility, cardinality).into()
                    })
                    .collect::<Vec<_>>();
                Self::new(columns, cardinality)
            }
        }
    }

    pub fn from_protobuf(proto: &PbDataChunk) -> ArrayResult<Self> {
        let mut columns = vec![];
        for any_col in proto.get_columns() {
            let cardinality = proto.get_cardinality() as usize;
            columns.push(ArrayImpl::from_protobuf(any_col, cardinality)?.into());
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

        let mut total_capacity = chunks.iter().map(|chunk| chunk.capacity()).sum();
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
            .map(|col| col.create_builder(new_chunk_require))
            .collect();
        let mut array_len = new_chunk_require;
        let mut new_chunks = Vec::with_capacity(num_chunks);
        while chunk_idx < chunks.len() {
            let capacity = chunks[chunk_idx].capacity();
            let num_rows_left = capacity - start_row_idx;
            let actual_acquire = std::cmp::min(new_chunk_require, num_rows_left);
            let end_row_idx = start_row_idx + actual_acquire - 1;
            array_builders
                .iter_mut()
                .zip_eq_fast(chunks[chunk_idx].columns())
                .for_each(|(builder, column)| {
                    let mut array_builder = column.create_builder(end_row_idx - start_row_idx + 1);
                    for row_idx in start_row_idx..=end_row_idx {
                        array_builder.append_datum(column.value_at(row_idx));
                    }
                    builder.append_array(&array_builder.finish());
                });
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
                let new_columns: Vec<ArrayRef> = array_builders
                    .drain(..)
                    .map(|builder| builder.finish().into())
                    .collect();

                array_builders = new_columns
                    .iter()
                    .map(|col_type| col_type.create_builder(new_chunk_require))
                    .collect();

                let data_chunk = DataChunk::new(new_columns, array_len);
                new_chunks.push(data_chunk);

                new_chunk_require = std::cmp::min(total_capacity, each_size_limit);
                array_len = new_chunk_require;
            }
        }

        Ok(new_chunks)
    }

    /// Compute hash values for each row.
    pub fn get_hash_values<H: BuildHasher>(
        &self,
        column_idxes: &[usize],
        hasher_builder: H,
    ) -> Vec<HashCode<H>> {
        let mut states = Vec::with_capacity(self.capacity());
        states.resize_with(self.capacity(), || hasher_builder.build_hasher());
        // Compute hash for the specified columns.
        for column_idx in column_idxes {
            let array = self.column_at(*column_idx);
            array.hash_vec(&mut states[..]);
        }
        finalize_hashers(&mut states[..])
            .into_iter()
            .map(|hash_code| hash_code.into())
            .collect_vec()
    }

    /// Random access a tuple in a data chunk. Return in a row format.
    /// # Arguments
    /// * `pos` - Index of look up tuple
    /// * `RowRef` - Reference of data tuple
    /// * bool - whether this tuple is visible
    pub fn row_at(&self, pos: usize) -> (RowRef<'_>, bool) {
        let row = self.row_at_unchecked_vis(pos);
        let vis = self.vis2.is_set(pos);
        (row, vis)
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
                .iter()
                .map(|v| {
                    match v {
                        None => "".to_owned(), // null
                        Some(scalar) => scalar.to_text(),
                    }
                })
                .collect();
            table.add_row(cells);
        }
        table.to_string()
    }

    /// Keep the specified columns and set the rest elements to null.
    ///
    /// # Example
    ///
    /// ```text
    /// i i i                            i i i
    /// 1 2 3  --> keep_columns([1]) --> . 2 .
    /// 4 5 6                            . 5 .
    /// ```
    pub fn keep_columns(&self, column_indices: &[usize]) -> Self {
        let capacity: usize = self.capacity();
        let columns = (self.columns.iter().enumerate())
            .map(|(i, column)| {
                if column_indices.contains(&i) {
                    column.clone()
                } else {
                    let mut builder = column.create_builder(capacity);
                    builder.append_datum_n(capacity, None as DatumRef<'_>);
                    builder.finish().into()
                }
            })
            .collect();
        DataChunk {
            columns,
            vis2: self.vis2.clone(),
        }
    }

    /// Reorder (and possibly remove) columns. e.g. if `column_mapping` is `[2, 1, 0]`, and
    /// the chunk contains column `[a, b, c]`, then the output will be
    /// `[c, b, a]`. If `column_mapping` is [2, 0], then the output will be `[c, a]`
    /// If the input mapping is identity mapping, no reorder will be performed.
    pub fn reorder_columns(self, column_mapping: &[usize]) -> Self {
        if column_mapping.iter().copied().eq(0..self.columns().len()) {
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

    /// Reorder rows by indexes.
    pub fn reorder_rows(&self, indexes: &[usize]) -> Self {
        let mut array_builders: Vec<ArrayBuilderImpl> = self
            .columns
            .iter()
            .map(|col| col.create_builder(indexes.len()))
            .collect();
        for &i in indexes {
            for (builder, col) in array_builders.iter_mut().zip_eq_fast(&self.columns) {
                builder.append_datum(col.value_at(i));
            }
        }
        let columns = array_builders
            .into_iter()
            .map(|builder| builder.finish().into())
            .collect();
        DataChunk::new(columns, indexes.len())
    }

    fn partition_sizes_for_columns(&self, col_indices: &[usize]) -> (usize, Vec<&ArrayRef>) {
        let mut col_variable: Vec<&ArrayRef> = vec![];
        let mut row_len_fixed: usize = 0;
        for i in col_indices {
            let col = &self.columns[*i];
            if let Some(field_len) = try_get_exact_serialize_datum_size(col) {
                row_len_fixed += field_len;
            } else {
                col_variable.push(col);
            }
        }
        (row_len_fixed, col_variable)
    }

    /// Partition fixed size datums and variable length ones.
    /// ---
    /// In some cases, we have fixed size for the entire column,
    /// when the datatypes are fixed size or the datums are constants.
    /// As such we can compute the size for it just once for the column.
    ///
    /// Otherwise, for variable sized datatypes, such as `varchar`,
    /// we have to individually compute their sizes per row.
    fn partition_sizes(&self) -> (usize, Vec<&ArrayRef>) {
        let mut col_variable: Vec<&ArrayRef> = vec![];
        let mut row_len_fixed: usize = 0;
        for c in &self.columns {
            if let Some(field_len) = try_get_exact_serialize_datum_size(c) {
                row_len_fixed += field_len;
            } else {
                col_variable.push(c);
            }
        }
        (row_len_fixed, col_variable)
    }

    unsafe fn compute_size_of_variable_cols_in_row(
        variable_cols: &[&ArrayRef],
        row_idx: usize,
    ) -> usize {
        variable_cols
            .iter()
            .map(|col| estimate_serialize_datum_size(col.value_at_unchecked(row_idx)))
            .sum::<usize>()
    }

    unsafe fn init_buffer(
        row_len_fixed: usize,
        variable_cols: &[&ArrayRef],
        row_idx: usize,
    ) -> Vec<u8> {
        Vec::with_capacity(
            row_len_fixed + Self::compute_size_of_variable_cols_in_row(variable_cols, row_idx),
        )
    }

    pub fn compute_key_sizes_by_columns(&self, column_indices: &[usize]) -> Vec<usize> {
        let (row_len_fixed, col_variable) = self.partition_sizes_for_columns(column_indices);
        let mut sizes: Vec<usize> = Vec::with_capacity(self.capacity());
        let update_sizes = |sizes: &mut Vec<usize>, col_variable, i| unsafe {
            sizes.push(row_len_fixed + Self::compute_size_of_variable_cols_in_row(col_variable, i))
        };
        match &self.vis2 {
            Vis::Bitmap(vis) => {
                let rows_num = vis.len();
                for i in 0..rows_num {
                    // SAFETY(value_at_unchecked): the idx is always in bound.
                    unsafe {
                        if vis.is_set_unchecked(i) {
                            update_sizes(&mut sizes, &col_variable, i);
                        } else {
                            sizes.push(0)
                        }
                    }
                }
            }
            Vis::Compact(rows_num) => {
                for i in 0..*rows_num {
                    update_sizes(&mut sizes, &col_variable, i);
                }
            }
        }
        sizes
    }

    /// Serialize each row into value encoding bytes.
    ///
    /// The returned vector's size is `self.capacity()` and for the invisible row will give a empty
    /// bytes.
    // Note(bugen): should we exclude the invisible rows in the output so that the caller won't need
    // to handle visibility again?
    pub fn serialize(&self) -> Vec<Bytes> {
        let buffers = match &self.vis2 {
            Vis::Bitmap(vis) => {
                let rows_num = vis.len();
                let mut buffers: Vec<Vec<u8>> = vec![];
                let (row_len_fixed, col_variable) = self.partition_sizes();

                // First initialize buffer with the right size to avoid re-allocations
                for i in 0..rows_num {
                    // SAFETY(value_at_unchecked): the idx is always in bound.
                    unsafe {
                        if vis.is_set_unchecked(i) {
                            buffers.push(Self::init_buffer(row_len_fixed, &col_variable, i));
                        } else {
                            buffers.push(vec![]);
                        }
                    }
                }

                // Then do the actual serialization
                for c in &self.columns {
                    let c = c;
                    assert_eq!(c.len(), rows_num);
                    for (i, buffer) in buffers.iter_mut().enumerate() {
                        // SAFETY(value_at_unchecked): the idx is always in bound.
                        unsafe {
                            if vis.is_set_unchecked(i) {
                                serialize_datum_into(c.value_at_unchecked(i), buffer);
                            }
                        }
                    }
                }
                buffers
            }
            Vis::Compact(rows_num) => {
                let mut buffers: Vec<Vec<u8>> = vec![];
                let (row_len_fixed, col_variable) = self.partition_sizes();
                for i in 0..*rows_num {
                    unsafe {
                        buffers.push(Self::init_buffer(row_len_fixed, &col_variable, i));
                    }
                }
                for c in &self.columns {
                    let c = c;
                    assert_eq!(c.len(), *rows_num);
                    for (i, buffer) in buffers.iter_mut().enumerate() {
                        // SAFETY(value_at_unchecked): the idx is always in bound.
                        unsafe {
                            serialize_datum_into(c.value_at_unchecked(i), buffer);
                        }
                    }
                }
                buffers
            }
        };

        buffers.into_iter().map(|item| item.into()).collect_vec()
    }

    /// Serialize each row into bytes with given serializer.
    ///
    /// This is similar to `serialize` but it uses a custom serializer. Prefer `serialize` if
    /// possible since it might be more efficient due to columnar operations.
    pub fn serialize_with(&self, serializer: &impl ValueRowSerializer) -> Vec<Bytes> {
        let mut results = Vec::with_capacity(self.capacity());
        for row in self.rows_with_holes() {
            results.push(if let Some(row) = row {
                serializer.serialize(row).into()
            } else {
                Bytes::new()
            });
        }
        results
    }

    /// Estimate size of hash keys. Their indices in a row are indicated by `column_indices`.
    /// Size here refers to the number of u8s required to store the serialized datum.
    pub fn estimate_value_encoding_size(&self, column_indices: &[usize]) -> usize {
        if self.capacity() == 0 {
            0
        } else {
            column_indices
                .iter()
                .map(|idx| {
                    let datum = self.column_at(*idx).datum_at(0);
                    estimate_serialize_datum_size(datum)
                })
                .sum()
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

impl<'a> From<&'a StructArray> for DataChunk {
    fn from(array: &'a StructArray) -> Self {
        let columns = array.fields().map(|array| array.clone().into()).collect();
        Self {
            columns,
            vis2: Vis::Compact(array.len()),
        }
    }
}

impl EstimateSize for DataChunk {
    fn estimated_heap_size(&self) -> usize {
        self.columns
            .iter()
            .map(|a| a.estimated_heap_size())
            .sum::<usize>()
            + self.vis2.estimated_heap_size()
    }
}

/// Test utilities for [`DataChunk`].
pub trait DataChunkTestExt {
    /// SEED for generating data chunk.
    const SEED: u64 = 0xFF67FEABBAEF76FF;

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
    /// //     B: bool
    /// //     I: i64
    /// //     i: i32
    /// //     F: f64
    /// //     f: f32
    /// //     T: str
    /// //    TS: Timestamp
    /// //   SRL: Serial
    /// // {i,f}: struct
    /// ```
    fn from_pretty(s: &str) -> Self;

    /// Insert one invisible hole after every record.
    fn with_invisible_holes(self) -> Self
    where
        Self: Sized;

    /// Panic if the chunk is invalid.
    fn assert_valid(&self);

    /// Generate data chunk when supplied with `chunk_size` and column data types.
    fn gen_data_chunk(
        chunk_offset: usize,
        chunk_size: usize,
        data_types: &[DataType],
        string_properties: &VarcharProperty,
    ) -> Self;

    /// Generate data chunks when supplied with `chunk_size` and column data types.
    fn gen_data_chunks(
        num_of_chunks: usize,
        chunk_size: usize,
        data_types: &[DataType],
        string_properties: &VarcharProperty,
    ) -> Vec<Self>
    where
        Self: Sized;
}

impl DataChunkTestExt for DataChunk {
    fn from_pretty(s: &str) -> Self {
        use crate::types::ScalarImpl;
        fn parse_type(s: &str) -> DataType {
            match s {
                "B" => DataType::Boolean,
                "I" => DataType::Int64,
                "i" => DataType::Int32,
                "F" => DataType::Float64,
                "f" => DataType::Float32,
                "TS" => DataType::Timestamp,
                "T" => DataType::Varchar,
                "SRL" => DataType::Serial,
                array if array.starts_with('{') && array.ends_with('}') => {
                    DataType::Struct(Arc::new(StructType {
                        fields: array[1..array.len() - 1]
                            .split(',')
                            .map(parse_type)
                            .collect_vec(),
                        field_names: vec![],
                    }))
                }
                _ => todo!("unsupported type: {s:?}"),
            }
        }

        let mut lines = s.split('\n').filter(|l| !l.trim().is_empty());
        // initialize array builders from the first line
        let header = lines.next().unwrap().trim();
        let datatypes = header
            .split_ascii_whitespace()
            .take_while(|c| *c != "//")
            .map(parse_type)
            .collect::<Vec<_>>();
        let mut array_builders = datatypes
            .iter()
            .map(|ty| ty.create_array_builder(1))
            .collect::<Vec<_>>();
        let mut visibility = vec![];
        for line in lines {
            let mut token = line.trim().split_ascii_whitespace();
            // allow `zip` since `token` may longer than `array_builders`
            #[allow(clippy::disallowed_methods)]
            for ((builder, ty), val_str) in
                array_builders.iter_mut().zip(&datatypes).zip(&mut token)
            {
                let datum = match val_str {
                    "." => None,
                    "t" => Some(true.into()),
                    "f" => Some(false.into()),
                    _ => Some(ScalarImpl::from_text(val_str.as_bytes(), ty).unwrap()),
                };
                builder.append_datum(datum);
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
            .map(|builder| builder.finish().into())
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
                let arr = col;
                let mut builder = arr.create_builder(n * 2);
                for v in arr.iter() {
                    builder.append_datum(&v.to_owned_datum());
                    builder.append_null();
                }

                builder.finish().into()
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
            assert_eq!(col.len(), n);
        }
    }

    fn gen_data_chunk(
        chunk_offset: usize,
        chunk_size: usize,
        data_types: &[DataType],
        varchar_properties: &VarcharProperty,
    ) -> Self {
        let mut columns = Vec::new();
        // Generate columns of this chunk.
        for data_type in data_types {
            let mut array_builder = data_type.create_array_builder(chunk_size);
            for j in 0..chunk_size {
                let offset = ((chunk_offset + 1) * (j + 1)) as u64;
                if *data_type == DataType::Varchar {
                    let datum = FieldGeneratorImpl::with_varchar(varchar_properties, Self::SEED)
                        .generate_datum(offset);
                    array_builder.append_datum(&datum);
                } else {
                    let mut data_gen = FieldGeneratorImpl::with_number_random(
                        data_type.clone(),
                        None,
                        None,
                        Self::SEED,
                    )
                    .unwrap();
                    let datum = data_gen.generate_datum(offset);
                    array_builder.append_datum(datum);
                }
                // FIXME(kwannoel): This misses the case where it is neither Varchar or numeric.
            }
            columns.push(array_builder.finish().into());
        }
        DataChunk::new(columns, Bitmap::ones(chunk_size))
    }

    fn gen_data_chunks(
        num_of_chunks: usize,
        chunk_size: usize,
        data_types: &[DataType],
        varchar_properties: &VarcharProperty,
    ) -> Vec<Self> {
        (0..num_of_chunks)
            .map(|i| Self::gen_data_chunk(i, chunk_size, data_types, varchar_properties))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crate::array::*;
    use crate::row::Row;

    #[test]
    fn test_rechunk() {
        let test_case = |num_chunks: usize, chunk_size: usize, new_chunk_size: usize| {
            let mut chunks = vec![];
            for chunk_idx in 0..num_chunks {
                let mut builder = PrimitiveArrayBuilder::<i32>::new(0);
                for i in chunk_size * chunk_idx..chunk_size * (chunk_idx + 1) {
                    builder.append(Some(i as i32));
                }
                let chunk = DataChunk::new(vec![Arc::new(builder.finish().into())], chunk_size);
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
                builder.append(Some(i as i32));
            }
            let arr = builder.finish();
            columns.push(Arc::new(arr.into()))
        }
        let chunk: DataChunk = DataChunk::new(columns, length);
        for row in chunk.rows() {
            for i in 0..num_of_columns {
                let val = row.datum_at(i).unwrap();
                assert_eq!(val.into_int32(), i as i32);
            }
        }
    }

    #[test]
    fn test_to_pretty_string() {
        let chunk = DataChunk::new(
            vec![
                Arc::new(I64Array::from_iter([1, 2, 3, 4]).into()),
                Arc::new(I64Array::from_iter([Some(6), None, Some(7), None]).into()),
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
        assert_eq!(
            chunk.clone().reorder_columns(&[2, 1, 0]),
            DataChunk::from_pretty(
                "I I I
                 1 5 2
                 2 9 4
                 3 9 6",
            )
        );
        assert_eq!(
            chunk.clone().reorder_columns(&[2, 0]),
            DataChunk::from_pretty(
                "I I
                 1 2
                 2 4
                 3 6",
            )
        );
        assert_eq!(chunk.clone().reorder_columns(&[0, 1, 2]), chunk);
        assert_eq!(chunk.reorder_columns(&[]).cardinality(), 3);
    }

    #[test]
    fn test_chunk_estimated_size() {
        assert_eq!(
            120,
            DataChunk::from_pretty(
                "I I I
                 1 5 2
                 2 9 4
                 3 9 6",
            )
            .estimated_heap_size()
        );
        assert_eq!(
            80,
            DataChunk::from_pretty(
                "I I
                 1 2
                 2 4
                 3 6",
            )
            .estimated_heap_size()
        );
    }
}
