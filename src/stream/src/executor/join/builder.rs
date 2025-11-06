// Copyright 2025 RisingWave Labs
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
use risingwave_common::row::Row;
use risingwave_common::types::{DataType, DatumRef};

use self::row::JoinRow;
// Re-export `StreamChunkBuilder`.
use super::*;

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
        // Enforce that the chunk size is at least 2, so that appending two rows to the chunk
        // builder will at most yield one chunk. `with_match` depends on and gets simplified
        // by such property.
        let chunk_size = chunk_size.max(2);

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

    /// Remove unnecessary updates in the pattern `-(k, old), +(k, NULL), -(k, NULL), +(k, new)`
    /// to avoid this issue: <https://github.com/risingwavelabs/risingwave/issues/17450>
    pub fn post_process(c: StreamChunk) -> StreamChunk {
        c.eliminate_adjacent_noop_update()
    }

    /// TODO(kwannoel): We can actually reuse a lot of the logic between `with_match_on_insert`
    /// and `with_match_on_delete`. We should refactor this to avoid code duplication.
    /// We just introduce this wrapper function to avoid large code diffs.
    pub fn with_match<const OP: JoinOpPrimitive>(
        &mut self,
        row: &RowRef<'_>,
        matched_row: &JoinRow<impl Row>,
    ) -> Option<StreamChunk> {
        match OP {
            JoinOp::Insert => self.with_match_on_insert(row, matched_row),
            JoinOp::Delete => self.with_match_on_delete(row, matched_row),
        }
    }

    pub fn with_match_on_insert(
        &mut self,
        row: &RowRef<'_>,
        matched_row: &JoinRow<impl Row>,
    ) -> Option<StreamChunk> {
        // Left/Right Anti sides
        if is_anti(T) {
            if matched_row.is_zero_degree() && only_forward_matched_side(T, SIDE) {
                self.stream_chunk_builder
                    .append_row_matched(Op::Delete, &matched_row.row)
                    .map(Self::post_process)
            } else {
                None
            }
        // Left/Right Semi sides
        } else if is_semi(T) {
            if matched_row.is_zero_degree() && only_forward_matched_side(T, SIDE) {
                self.stream_chunk_builder
                    .append_row_matched(Op::Insert, &matched_row.row)
                    .map(Self::post_process)
            } else {
                None
            }
        // Outer sides
        } else if matched_row.is_zero_degree() && outer_side_null(T, SIDE) {
            // if the matched_row does not have any current matches

            // The current side part of the stream key changes from NULL to non-NULL.
            // Thus we cannot use `UpdateDelete` and `UpdateInsert`, as it requires the
            // stream key to remain the same.
            let chunk1 = self
                .stream_chunk_builder
                .append_row_matched(Op::Delete, &matched_row.row);
            let chunk2 = self
                .stream_chunk_builder
                .append_row(Op::Insert, row, &matched_row.row);

            // We've enforced chunk size to be at least 2, so it's impossible to have 2 chunks yield.
            // TODO: better to ensure they are in the same chunk to make `post_process` more effective.
            assert!(chunk1.is_none() || chunk2.is_none());
            chunk1.or(chunk2).map(Self::post_process)
        // Inner sides
        } else {
            self.stream_chunk_builder
                .append_row(Op::Insert, row, &matched_row.row)
                .map(Self::post_process)
        }
    }

    pub fn with_match_on_delete(
        &mut self,
        row: &RowRef<'_>,
        matched_row: &JoinRow<impl Row>,
    ) -> Option<StreamChunk> {
        // Left/Right Anti sides
        if is_anti(T) {
            if matched_row.is_zero_degree() && only_forward_matched_side(T, SIDE) {
                self.stream_chunk_builder
                    .append_row_matched(Op::Insert, &matched_row.row)
                    .map(Self::post_process)
            } else {
                None
            }
        // Left/Right Semi sides
        } else if is_semi(T) {
            if matched_row.is_zero_degree() && only_forward_matched_side(T, SIDE) {
                self.stream_chunk_builder
                    .append_row_matched(Op::Delete, &matched_row.row)
                    .map(Self::post_process)
            } else {
                None
            }
        // Outer sides
        } else if matched_row.is_zero_degree() && outer_side_null(T, SIDE) {
            // if the matched_row does not have any current matches

            // The current side part of the stream key changes from non-NULL to NULL.
            // Thus we cannot use `UpdateDelete` and `UpdateInsert`, as it requires the
            // stream key to remain the same.
            let chunk1 = self
                .stream_chunk_builder
                .append_row(Op::Delete, row, &matched_row.row);
            let chunk2 = self
                .stream_chunk_builder
                .append_row_matched(Op::Insert, &matched_row.row);

            // We've enforced chunk size to be at least 2, so it's impossible to have 2 chunks yield.
            // TODO: better to ensure they are in the same chunk to make `post_process` more effective.
            assert!(chunk1.is_none() || chunk2.is_none());
            chunk1.or(chunk2).map(Self::post_process)

        // Inner sides
        } else {
            // concat with the matched_row and append the new row
            // FIXME: we always use `Op::Delete` here to avoid
            // violating the assumption for U+ after U-, we can actually do better.
            self.stream_chunk_builder
                .append_row(Op::Delete, row, &matched_row.row)
                .map(Self::post_process)
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
            self.stream_chunk_builder
                .append_row_update(op, row)
                .map(Self::post_process)
        } else {
            None
        }
    }

    #[inline]
    pub fn forward_if_not_matched(&mut self, op: Op, row: RowRef<'_>) -> Option<StreamChunk> {
        // if it's outer join or anti join and the side needs to be maintained.
        if (is_anti(T) && forward_exactly_once(T, SIDE)) || is_outer_side(T, SIDE) {
            self.stream_chunk_builder
                .append_row_update(op, row)
                .map(Self::post_process)
        } else {
            None
        }
    }

    #[inline]
    pub fn take(&mut self) -> Option<StreamChunk> {
        self.stream_chunk_builder.take().map(Self::post_process)
    }
}
