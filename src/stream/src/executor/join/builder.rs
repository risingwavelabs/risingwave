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

use risingwave_common::array::stream_chunk_builder::StreamChunkBuilder;
use risingwave_common::array::{Op, RowRef, StreamChunk};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, DatumRef};

use self::row::JoinRow;
// Re-export `StreamChunkBuilder`.
use super::*;
use super::{JoinTypePrimitive, SideTypePrimitive};

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

pub struct JoinChunkBuilder<const T: JoinTypePrimitive, const SIDE: SideTypePrimitive> {
    stream_chunk_builder: JoinStreamChunkBuilder,
}

impl<const T: JoinTypePrimitive, const SIDE: SideTypePrimitive> JoinChunkBuilder<T, SIDE> {
    pub fn new(stream_chunk_builder: JoinStreamChunkBuilder) -> Self {
        Self {
            stream_chunk_builder,
        }
    }

    pub fn with_match_on_insert(
        &mut self,
        row: &RowRef<'_>,
        matched_row: &JoinRow<OwnedRow>,
    ) -> Option<StreamChunk> {
        // Left/Right Anti sides
        if is_anti(T) {
            if matched_row.is_zero_degree() && only_forward_matched_side(T, SIDE) {
                self.stream_chunk_builder
                    .append_row_matched(Op::Delete, &matched_row.row)
            } else {
                None
            }
        // Left/Right Semi sides
        } else if is_semi(T) {
            if matched_row.is_zero_degree() && only_forward_matched_side(T, SIDE) {
                self.stream_chunk_builder
                    .append_row_matched(Op::Insert, &matched_row.row)
            } else {
                None
            }
        // Outer sides
        } else if matched_row.is_zero_degree() && outer_side_null(T, SIDE) {
            // if the matched_row does not have any current matches
            // `StreamChunkBuilder` guarantees that `UpdateDelete` will never
            // issue an output chunk.
            if self
                .stream_chunk_builder
                .append_row_matched(Op::UpdateDelete, &matched_row.row)
                .is_some()
            {
                unreachable!("`Op::UpdateDelete` should not yield chunk");
            }
            self.stream_chunk_builder
                .append_row(Op::UpdateInsert, row, &matched_row.row)
        // Inner sides
        } else {
            self.stream_chunk_builder
                .append_row(Op::Insert, row, &matched_row.row)
        }
    }

    pub fn with_match_on_delete(
        &mut self,
        row: &RowRef<'_>,
        matched_row: &JoinRow<OwnedRow>,
    ) -> Option<StreamChunk> {
        // Left/Right Anti sides
        if is_anti(T) {
            if matched_row.is_zero_degree() && only_forward_matched_side(T, SIDE) {
                self.stream_chunk_builder
                    .append_row_matched(Op::Insert, &matched_row.row)
            } else {
                None
            }
        // Left/Right Semi sides
        } else if is_semi(T) {
            if matched_row.is_zero_degree() && only_forward_matched_side(T, SIDE) {
                self.stream_chunk_builder
                    .append_row_matched(Op::Delete, &matched_row.row)
            } else {
                None
            }
        // Outer sides
        } else if matched_row.is_zero_degree() && outer_side_null(T, SIDE) {
            // if the matched_row does not have any current
            // matches
            if self
                .stream_chunk_builder
                .append_row(Op::UpdateDelete, row, &matched_row.row)
                .is_some()
            {
                unreachable!("`Op::UpdateDelete` should not yield chunk");
            }
            self.stream_chunk_builder
                .append_row_matched(Op::UpdateInsert, &matched_row.row)
        // Inner sides
        } else {
            // concat with the matched_row and append the new
            // row
            // FIXME: we always use `Op::Delete` here to avoid
            // violating
            // the assumption for U+ after U-.
            self.stream_chunk_builder
                .append_row(Op::Delete, row, &matched_row.row)
        }
    }

    #[inline]
    pub fn forward_exactly_once_if_matched(
        &mut self,
        op: Op,
        row: RowRef<'_>,
    ) -> Option<StreamChunk> {
        // if it's a semi join and the side needs to be maintained.
        if is_semi(T) && forward_exactly_once(T, SIDE) {
            self.stream_chunk_builder.append_row_update(op, row)
        } else {
            None
        }
    }

    #[inline]
    pub fn forward_if_not_matched(&mut self, op: Op, row: RowRef<'_>) -> Option<StreamChunk> {
        // if it's outer join or anti join and the side needs to be maintained.
        if (is_anti(T) && forward_exactly_once(T, SIDE)) || is_outer_side(T, SIDE) {
            self.stream_chunk_builder.append_row_update(op, row)
        } else {
            None
        }
    }

    #[inline]
    pub fn take(&mut self) -> Option<StreamChunk> {
        self.stream_chunk_builder.take()
    }
}
