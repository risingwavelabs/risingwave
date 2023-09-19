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

use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{ArrayBuilderImpl, Op, StreamChunk};
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DatumRef};
use risingwave_common::util::iter_util::ZipEqFast;

/// Build stream chunks with fixed chunk size from rows or records.
pub struct StreamChunkBuilder {
    /// operations in the data chunk to build
    ops: Vec<Op>,

    /// arrays in the data chunk to build
    column_builders: Vec<ArrayBuilderImpl>,

    /// Data types of columns
    data_types: Vec<DataType>,

    /// Maximum capacity of column builder
    capacity: usize,

    /// Size of column builder
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

impl StreamChunkBuilder {
    pub fn new(chunk_size: usize, data_types: Vec<DataType>) -> Self {
        assert!(chunk_size > 0);

        let ops = Vec::with_capacity(chunk_size);
        let column_builders = data_types
            .iter()
            .map(|datatype| datatype.create_array_builder(chunk_size))
            .collect();
        Self {
            ops,
            column_builders,
            data_types,
            capacity: chunk_size,
            size: 0,
        }
    }

    /// Increase chunk size
    ///
    /// A [`StreamChunk`] will be returned when `size == capacity`
    #[must_use]
    fn inc_size(&mut self) -> Option<StreamChunk> {
        self.size += 1;

        // Take a chunk when capacity is exceeded. Splitting `UpdateDelete` and `UpdateInsert`
        // should be avoided, so when the last one is `UpdateDelete`, we delay the chunk until
        // `UpdateInsert` comes. This means the output chunk size may exceed the given `chunk_size`,
        // and theoretically at most `chunk_size + 1` if inputs are consistent.
        if self.size >= self.capacity && self.ops[self.ops.len() - 1] != Op::UpdateDelete {
            self.take()
        } else {
            None
        }
    }

    /// Append an iterator of output index and datum to the builder, return a chunk if the builder
    /// is full.
    /// Note: the caller must ensure that each column occurs exactly once in `iter`.
    fn append_iter<'a>(
        &mut self,
        op: Op,
        iter: impl IntoIterator<Item = (usize, DatumRef<'a>)>,
    ) -> Option<StreamChunk> {
        self.ops.push(op);
        for (i, datum) in iter {
            self.column_builders[i].append(datum);
        }
        self.inc_size()
    }

    /// Append a row to the builder, return a chunk if the builder is full.
    #[must_use]
    pub fn append_row(&mut self, op: Op, row: impl Row) -> Option<StreamChunk> {
        self.append_iter(op, row.iter().enumerate())
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

    #[must_use]
    pub fn take(&mut self) -> Option<StreamChunk> {
        if self.size == 0 {
            return None;
        }

        self.size = 0;
        let new_columns = self
            .column_builders
            .iter_mut()
            .zip_eq_fast(&self.data_types)
            .map(|(builder, datatype)| {
                std::mem::replace(builder, datatype.create_array_builder(self.capacity)).finish()
            })
            .map(Into::into)
            .collect::<Vec<_>>();

        Some(StreamChunk::new(
            std::mem::replace(&mut self.ops, Vec::with_capacity(self.capacity)),
            new_columns,
        ))
    }
}

type IndexMappings = Vec<(usize, usize)>;

/// Build stream chunks with fixed chunk size from joined two sides of rows.
pub struct JoinStreamChunkBuilder {
    builder: StreamChunkBuilder,

    /// The column index mapping from update side to output.
    update_to_output: IndexMappings,

    /// The column index mapping from matched side to output.
    matched_to_output: IndexMappings,
}

impl JoinStreamChunkBuilder {
    pub fn new(
        chunk_size: usize,
        data_types: Vec<DataType>,
        update_to_output: IndexMappings,
        matched_to_output: IndexMappings,
    ) -> Self {
        Self {
            builder: StreamChunkBuilder::new(chunk_size, data_types),
            update_to_output,
            matched_to_output,
        }
    }

    /// Get the mappings from left/right input indices to the output indices. The mappings can be
    /// used to create [`JoinStreamChunkBuilder`] later.
    ///
    /// Please note the semantics of `update` and `matched` when creating the builder: either left
    /// or right side can be `update` side or `matched` side, the key is to call the corresponding
    /// append method once you passed `left_to_output`/`right_to_output` to
    /// `update_to_output`/`matched_to_output`.
    pub fn get_i2o_mapping(
        output_indices: &[usize],
        left_len: usize,
        right_len: usize,
    ) -> (IndexMappings, IndexMappings) {
        let mut left_to_output = vec![];
        let mut right_to_output = vec![];

        for (output_idx, &idx) in output_indices.iter().enumerate() {
            if idx < left_len {
                left_to_output.push((idx, output_idx))
            } else if idx >= left_len && idx < left_len + right_len {
                right_to_output.push((idx - left_len, output_idx));
            } else {
                unreachable!("output_indices out of bound")
            }
        }
        (left_to_output, right_to_output)
    }

    /// Append a row with coming update value and matched value.
    ///
    /// A [`StreamChunk`] will be returned when `size == capacity`.
    #[must_use]
    pub fn append_row(
        &mut self,
        op: Op,
        row_update: impl Row,
        row_matched: impl Row,
    ) -> Option<StreamChunk> {
        self.builder.append_iter(
            op,
            self.update_to_output
                .iter()
                .map(|&(update_idx, output_idx)| (output_idx, row_update.datum_at(update_idx)))
                .chain(
                    self.matched_to_output
                        .iter()
                        .map(|&(matched_idx, output_idx)| {
                            (output_idx, row_matched.datum_at(matched_idx))
                        }),
                ),
        )
    }

    /// Append a row with coming update value and fill the other side with null.
    ///
    /// A [`StreamChunk`] will be returned when `size == capacity`.
    #[must_use]
    pub fn append_row_update(&mut self, op: Op, row_update: impl Row) -> Option<StreamChunk> {
        self.builder.append_iter(
            op,
            self.update_to_output
                .iter()
                .map(|&(update_idx, output_idx)| (output_idx, row_update.datum_at(update_idx)))
                .chain(
                    self.matched_to_output
                        .iter()
                        .map(|&(_, output_idx)| (output_idx, DatumRef::None)),
                ),
        )
    }

    /// Append a row with matched value and fill the coming side with null.
    ///
    /// A [`StreamChunk`] will be returned when `size == capacity`.
    #[must_use]
    pub fn append_row_matched(&mut self, op: Op, row_matched: impl Row) -> Option<StreamChunk> {
        self.builder.append_iter(
            op,
            self.update_to_output
                .iter()
                .map(|&(_, output_idx)| (output_idx, DatumRef::None))
                .chain(
                    self.matched_to_output
                        .iter()
                        .map(|&(matched_idx, output_idx)| {
                            (output_idx, row_matched.datum_at(matched_idx))
                        }),
                ),
        )
    }

    /// Take out the remaining rows as a chunk. Return `None` if the builder is empty.
    #[must_use]
    pub fn take(&mut self) -> Option<StreamChunk> {
        self.builder.take()
    }
}
