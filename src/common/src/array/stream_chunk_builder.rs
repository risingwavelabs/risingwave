// Copyright 2024 RisingWave Labs
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

use crate::array::stream_record::Record;
use crate::array::{ArrayBuilderImpl, Op, StreamChunk};
use crate::buffer::BitmapBuilder;
use crate::row::Row;
use crate::types::{DataType, DatumRef};
use crate::util::iter_util::ZipEqFast;

/// Build stream chunks with fixed chunk size from rows or records.
pub struct StreamChunkBuilder {
    /// operations in the data chunk to build
    ops: Vec<Op>,

    /// arrays in the data chunk to build
    column_builders: Vec<ArrayBuilderImpl>,

    /// Visibility
    vis_builder: BitmapBuilder,

    /// Data types of columns
    data_types: Vec<DataType>,

    /// Max number of rows in a chunk. When it's `Some(n)`, the chunk builder will, if necessary,
    /// yield a chunk of which the size is strictly less than or equal to `n` when appending records.
    /// When it's `None`, the chunk builder will yield chunks only when `take` is called.
    max_chunk_size: Option<usize>,

    /// The initial capacity of `ops` and `ArrayBuilder`s.
    initial_capacity: usize,

    /// Number of currently pending `Op`s.
    size: usize,
}

impl Drop for StreamChunkBuilder {
    fn drop(&mut self) {
        // Possible to fail when async task gets cancelled.
        if self.size != 0 {
            tracing::warn!(
                remaining = self.size,
                "dropping non-empty stream chunk builder"
            );
        }
    }
}

const MAX_INITIAL_CAPACITY: usize = 4096;
const DEFAULT_INITIAL_CAPACITY: usize = 64;

impl StreamChunkBuilder {
    /// Create a new `StreamChunkBuilder` with a fixed max chunk size.
    /// The max chunk size must be at least 2, otherwise it cannot produce
    /// any chunk with `UpdateDelete + UpdateInsert`.
    pub fn new(max_chunk_size: usize, data_types: Vec<DataType>) -> Self {
        assert!(max_chunk_size >= 2);

        let initial_capacity = max_chunk_size.min(MAX_INITIAL_CAPACITY);

        let ops = Vec::with_capacity(initial_capacity);
        let column_builders = data_types
            .iter()
            .map(|datatype| datatype.create_array_builder(initial_capacity))
            .collect();
        Self {
            ops,
            column_builders,
            data_types,
            vis_builder: BitmapBuilder::default(),
            max_chunk_size: Some(max_chunk_size),
            initial_capacity,
            size: 0,
        }
    }

    /// Create a new `StreamChunkBuilder` with unlimited chunk size.
    /// The builder will only yield chunks when `take` is called.
    pub fn unlimited(data_types: Vec<DataType>, initial_capacity: Option<usize>) -> Self {
        let initial_capacity = initial_capacity.unwrap_or(DEFAULT_INITIAL_CAPACITY);
        Self {
            ops: Vec::with_capacity(initial_capacity),
            column_builders: data_types
                .iter()
                .map(|datatype| datatype.create_array_builder(initial_capacity))
                .collect(),
            data_types,
            vis_builder: BitmapBuilder::default(),
            max_chunk_size: None,
            initial_capacity,
            size: 0,
        }
    }

    /// Append an iterator of output index and datum to the builder, return a chunk if the builder
    /// is full.
    ///
    /// Note: the caller must ensure that each column occurs exactly once in `iter`.
    #[must_use]
    pub fn append_iter<'a>(
        &mut self,
        op: Op,
        iter: impl IntoIterator<Item = (usize, DatumRef<'a>)>,
    ) -> Option<StreamChunk> {
        self.append_iter_inner::<true>(op, iter)
    }

    /// Append a row to the builder, return a chunk if the builder is full.
    #[must_use]
    pub fn append_row(&mut self, op: Op, row: impl Row) -> Option<StreamChunk> {
        self.append_iter_inner::<true>(op, row.iter().enumerate())
    }

    /// Append an invisible row to the builder, return a chunk if the builder is full.
    #[must_use]
    pub fn append_row_invisible(&mut self, op: Op, row: impl Row) -> Option<StreamChunk> {
        self.append_iter_inner::<false>(op, row.iter().enumerate())
    }

    /// Append a record to the builder, return a chunk if the builder is full.
    #[must_use]
    pub fn append_record(&mut self, record: Record<impl Row>) -> Option<StreamChunk> {
        match record {
            Record::Insert { new_row } => self.append_row(Op::Insert, new_row),
            Record::Delete { old_row } => self.append_row(Op::Delete, old_row),
            Record::Update { old_row, new_row } => {
                let none = self.append_row(Op::UpdateDelete, old_row);
                debug_assert!(none.is_none());
                self.append_row(Op::UpdateInsert, new_row)
            }
        }
    }

    /// Take all the pending data and return a chunk. If there is no pending data, return `None`.
    /// Note that if this is an unlimited chunk builder, the only way to get a chunk is to call
    /// `take`.
    #[must_use]
    pub fn take(&mut self) -> Option<StreamChunk> {
        if self.size == 0 {
            return None;
        }
        self.size = 0;

        let ops = std::mem::replace(&mut self.ops, Vec::with_capacity(self.initial_capacity));
        let columns = self
            .column_builders
            .iter_mut()
            .zip_eq_fast(&self.data_types)
            .map(|(builder, datatype)| {
                std::mem::replace(
                    builder,
                    datatype.create_array_builder(self.initial_capacity),
                )
                .finish()
            })
            .map(Into::into)
            .collect::<Vec<_>>();
        let vis = std::mem::take(&mut self.vis_builder).finish();

        Some(StreamChunk::with_visibility(ops, columns, vis))
    }

    #[must_use]
    fn append_iter_inner<'a, const VIS: bool>(
        &mut self,
        op: Op,
        iter: impl IntoIterator<Item = (usize, DatumRef<'a>)>,
    ) -> Option<StreamChunk> {
        let res = if let Some(max_chunk_size) = self.max_chunk_size
            && self.size == max_chunk_size - 1
            && op.is_update_delete()
        {
            // Let's ensure chunk size is really <= `max_chunk_size`.
            // If we are appending an `Update`, and the size will exceed the `max_chunk_size` after
            // appending `U-` and `U+`, we should take the existing data first, ensuring we have
            // space for the new `U-` and `U+`.
            self.take()
        } else {
            None
        };

        self.ops.push(op);
        for (i, datum) in iter {
            self.column_builders[i].append(datum);
        }
        self.vis_builder.append(VIS);
        self.size += 1;

        if res.is_some() {
            assert_eq!(self.size, 1);
            res
        } else if let Some(max_chunk_size) = self.max_chunk_size
            && self.size == max_chunk_size
        {
            self.take()
        } else {
            None
        }
    }
}
