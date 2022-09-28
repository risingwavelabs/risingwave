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

use itertools::Itertools;
use risingwave_common::array::{ArrayBuilderImpl, ArrayResult, Op, Row, RowRef, StreamChunk};
use risingwave_common::types::DataType;

/// Build a array and it's corresponding operations.
pub struct StreamChunkBuilder {
    /// operations in the data chunk to build
    ops: Vec<Op>,

    /// arrays in the data chunk to build
    column_builders: Vec<ArrayBuilderImpl>,

    /// Data types of columns
    data_types: Vec<DataType>,

    /// The start position of the columns of the side
    /// stream coming from. If the coming side is the
    /// left, the `update_start_pos` should be 0.
    /// If the coming side is the right, the `update_start_pos`
    /// is the number of columns of the left side.
    update_start_pos: usize,

    /// The start position of the columns of the opposite side
    /// stream coming from. If the coming side is the
    /// left, the `matched_start_pos` should be the number of columns of the left side.
    /// If the coming side is the right, the `matched_start_pos`
    /// should be 0.
    matched_start_pos: usize,

    /// Maximum capacity of column builder
    capacity: usize,

    /// Size of column builder
    size: usize,
}

impl Drop for StreamChunkBuilder {
    fn drop(&mut self) {
        assert_eq!(self.size, 0, "dropping non-empty stream chunk builder");
    }
}

impl StreamChunkBuilder {
    pub fn new(
        capacity: usize,
        data_types: &[DataType],
        update_start_pos: usize,
        matched_start_pos: usize,
    ) -> ArrayResult<Self> {
        // Leave room for paired `UpdateDelete` and `UpdateInsert`. When there are `capacity - 1`
        // ops in current builder and the last op is `UpdateDelete`, we delay the chunk generation
        // until `UpdateInsert` comes. This means that the effective output message size will indeed
        // be at most the original `capacity`
        let reduced_capacity = capacity - 1;
        assert!(reduced_capacity > 0);

        let ops = Vec::with_capacity(reduced_capacity);
        let column_builders = data_types
            .iter()
            .map(|datatype| datatype.create_array_builder(reduced_capacity))
            .collect();
        Ok(Self {
            ops,
            column_builders,
            data_types: data_types.to_owned(),
            update_start_pos,
            matched_start_pos,
            capacity: reduced_capacity,
            size: 0,
        })
    }

    /// Increase chunk size
    ///
    /// A [`StreamChunk`] will be returned when `size == capacity`
    fn inc_size(&mut self) -> ArrayResult<Option<StreamChunk>> {
        self.size += 1;

        // Take a chunk when capacity is exceeded, but splitting `UpdateDelete` and `UpdateInsert`
        // should be avoided
        if self.size >= self.capacity && self.ops[self.ops.len() - 1] != Op::UpdateDelete {
            self.take()
        } else {
            Ok(None)
        }
    }

    /// Append a row with coming update value and matched value
    ///
    /// A [`StreamChunk`] will be returned when `size == capacity`
    pub fn append_row(
        &mut self,
        op: Op,
        row_update: &RowRef<'_>,
        row_matched: &Row,
    ) -> ArrayResult<Option<StreamChunk>> {
        self.ops.push(op);
        for (i, d) in row_update.values().enumerate() {
            self.column_builders[i + self.update_start_pos].append_datum_ref(d);
        }
        for (i, d) in row_matched.values().enumerate() {
            self.column_builders[i + self.matched_start_pos].append_datum(d);
        }

        self.inc_size()
    }

    /// Append a row with coming update value and fill the other side with null.
    ///
    /// A [`StreamChunk`] will be returned when `size == capacity`
    pub fn append_row_update(
        &mut self,
        op: Op,
        row_update: &RowRef<'_>,
    ) -> ArrayResult<Option<StreamChunk>> {
        self.ops.push(op);
        for (i, d) in row_update.values().enumerate() {
            self.column_builders[i + self.update_start_pos].append_datum_ref(d);
        }
        for i in 0..self.column_builders.len() - row_update.size() {
            self.column_builders[i + self.matched_start_pos].append_datum(&None);
        }

        self.inc_size()
    }

    /// append a row with matched value and fill the coming side with null.
    ///
    /// A [`StreamChunk`] will be returned when `size == capacity`
    pub fn append_row_matched(
        &mut self,
        op: Op,
        row_matched: &Row,
    ) -> ArrayResult<Option<StreamChunk>> {
        self.ops.push(op);
        for i in 0..self.column_builders.len() - row_matched.size() {
            self.column_builders[i + self.update_start_pos].append_datum_ref(None);
        }
        for i in 0..row_matched.size() {
            self.column_builders[i + self.matched_start_pos].append_datum(&row_matched[i]);
        }

        self.inc_size()
    }

    pub fn take(&mut self) -> ArrayResult<Option<StreamChunk>> {
        self.size = 0;
        let new_columns = self
            .column_builders
            .iter_mut()
            .zip_eq(&self.data_types)
            .map(|(builder, datatype)| {
                std::mem::replace(builder, datatype.create_array_builder(self.capacity)).finish()
            })
            .map(Into::into)
            .collect::<Vec<_>>();

        Ok(Some(StreamChunk::new(
            std::mem::take(&mut self.ops),
            new_columns,
            None,
        )))
    }
}
