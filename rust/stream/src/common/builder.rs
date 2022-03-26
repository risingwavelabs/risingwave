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

use std::sync::Arc;

use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilderImpl, Op, Row, RowRef, StreamChunk};
use risingwave_common::error::Result;
use risingwave_common::types::DataType;

/// Build a array and it's corresponding operations.
pub struct StreamChunkBuilder {
    /// operations in the data chunk to build
    ops: Vec<Op>,

    /// arrays in the data chunk to build
    column_builders: Vec<ArrayBuilderImpl>,

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
}

impl StreamChunkBuilder {
    pub fn new(
        capacity: usize,
        data_types: &[DataType],
        update_start_pos: usize,
        matched_start_pos: usize,
    ) -> Result<Self> {
        let ops = Vec::with_capacity(capacity);
        let column_builders = data_types
            .iter()
            .map(|datatype| datatype.create_array_builder(capacity))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            ops,
            column_builders,
            update_start_pos,
            matched_start_pos,
        })
    }

    /// append a row with coming update value and matched value.
    pub fn append_row(&mut self, op: Op, row_update: &RowRef<'_>, row_matched: &Row) -> Result<()> {
        self.ops.push(op);
        for i in 0..row_update.size() {
            self.column_builders[i + self.update_start_pos].append_datum_ref(row_update[i])?;
        }
        for i in 0..row_matched.size() {
            self.column_builders[i + self.matched_start_pos].append_datum(&row_matched[i])?;
        }
        Ok(())
    }

    /// append a row with coming update value and fill the other side with null.
    pub fn append_row_update(&mut self, op: Op, row_update: &RowRef<'_>) -> Result<()> {
        self.ops.push(op);
        for i in 0..row_update.size() {
            self.column_builders[i + self.update_start_pos].append_datum_ref(row_update[i])?;
        }
        for i in 0..self.column_builders.len() - row_update.size() {
            self.column_builders[i + self.matched_start_pos].append_datum(&None)?;
        }
        Ok(())
    }

    /// append a row with matched value and fill the coming side with null.
    pub fn append_row_matched(&mut self, op: Op, row_matched: &Row) -> Result<()> {
        self.ops.push(op);
        for i in 0..self.column_builders.len() - row_matched.size() {
            self.column_builders[i + self.update_start_pos].append_datum_ref(None)?;
        }
        for i in 0..row_matched.size() {
            self.column_builders[i + self.matched_start_pos].append_datum(&row_matched[i])?;
        }
        Ok(())
    }

    pub fn finish(self) -> Result<StreamChunk> {
        let new_arrays = self
            .column_builders
            .into_iter()
            .map(|builder| builder.finish())
            .collect::<Result<Vec<_>>>()?;

        let new_columns = new_arrays
            .into_iter()
            .map(|array_impl| Column::new(Arc::new(array_impl)))
            .collect::<Vec<_>>();

        Ok(StreamChunk::new(self.ops, new_columns, None))
    }
}
