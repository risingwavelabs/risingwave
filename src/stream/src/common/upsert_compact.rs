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

use itertools::Itertools;
use risingwave_common::array::StreamChunk;
use risingwave_common::array::stream_chunk::StreamChunkMut;
use risingwave_common::row::RowExt;
use risingwave_common::types::DataType;

pub use super::change_buffer::InconsistencyBehavior;
use crate::common::change_buffer::ChangeBuffer;
use crate::common::change_buffer::kind::UPSERT;

/// A helper to remove unnecessary changes to convert into upsert format.
pub struct StreamChunkUpsertCompactor {
    chunks: Vec<StreamChunk>,
    key: Vec<usize>,
}

impl StreamChunkUpsertCompactor {
    pub fn new(key: Vec<usize>, chunks: Vec<StreamChunk>) -> Self {
        Self { chunks, key }
    }

    pub fn into_inner(self) -> (Vec<StreamChunk>, Vec<usize>) {
        (self.chunks, self.key)
    }

    /// Remove unnecessary changes to convert into upsert format for given chunks, by modifying
    /// the visibility and ops in place.
    ///
    /// Refer to `StreamKind::Upsert` for the definition of upsert format. Basically, we only
    /// keep the new row for updates, and there won't be any `UpdateDelete` or `UpdateInsert`
    /// operations.
    pub fn into_compacted_chunks_inline(
        self,
        ib: InconsistencyBehavior,
    ) -> impl Iterator<Item = StreamChunk> {
        let (chunks, key_indices) = self.into_inner();

        let estimate_size = chunks.iter().map(|c| c.cardinality()).sum();
        let mut cb = ChangeBuffer::with_capacity(estimate_size).with_inconsistency_behavior(ib);

        let mut chunks = chunks.into_iter().map(StreamChunkMut::from).collect_vec();
        for chunk in &mut chunks {
            for (row, mut op_row) in chunk.to_rows_mut() {
                let op = op_row.op().normalize_update();
                let key = row.project(&key_indices);
                // Make all rows invisible first.
                op_row.set_vis(false);
                op_row.set_op(op);
                cb.apply_op_row(op, key, op_row);
            }
        }

        // For the rows that survive compaction, make them visible.
        for record in cb.into_records() {
            // Rewrite `Update` to `Insert` by calling `into_upsert`.
            record.into_upsert().map(|mut row| row.set_vis(true));
        }

        chunks.into_iter().map(|c| c.into())
    }

    /// Remove unnecessary changes to convert into upsert format for given chunks, by filtering
    /// them out and constructing new chunks, with the given chunk size.
    pub fn into_compacted_chunks_reconstructed(
        self,
        chunk_size: usize,
        data_types: Vec<DataType>,
        ib: InconsistencyBehavior,
    ) -> Vec<StreamChunk> {
        let (chunks, key_indices) = self.into_inner();

        let estimate_size = chunks.iter().map(|c| c.cardinality()).sum();
        let mut cb = ChangeBuffer::with_capacity(estimate_size).with_inconsistency_behavior(ib);

        for chunk in &chunks {
            for record in chunk.records() {
                cb.apply_record(record, |&row| row.project(&key_indices));
            }
        }

        cb.into_chunks::<UPSERT>(data_types, chunk_size)
    }
}

/// Remove unnecessary changes to convert into upsert format for given chunk, by modifying the
/// visibility and ops in place.
///
/// This is the same as [`StreamChunkUpsertCompactor::into_compacted_chunks_inline`] with only one chunk.
pub fn into_upsert_compacted_chunk(
    stream_chunk: StreamChunk,
    key_indices: &[usize],
    ib: InconsistencyBehavior,
) -> StreamChunk {
    StreamChunkUpsertCompactor::new(key_indices.to_vec(), vec![stream_chunk])
        .into_compacted_chunks_inline(ib)
        .exactly_one()
        .unwrap_or_else(|_| unreachable!("should have exactly one chunk in the output"))
}

#[cfg(test)]
mod tests {
    use risingwave_common::test_prelude::StreamChunkTestExt;

    use super::*;

    #[test]
    fn test_compact_chunk_inline() {
        let key = [0, 1];
        let chunks = vec![
            StreamChunk::from_pretty(
                " I I I
                - 1 1 1
                + 1 1 2
                + 2 5 7
                + 4 9 2
                - 2 5 7
                + 2 5 5
                - 6 6 9
                + 6 6 9
                - 9 9 1",
            ),
            StreamChunk::from_pretty(
                " I I I
                - 6 6 9
                + 9 9 9
                - 9 9 4
                + 2 2 2
                + 9 9 1",
            ),
        ];
        let compactor = StreamChunkUpsertCompactor::new(key.to_vec(), chunks);
        let mut iter = compactor.into_compacted_chunks_inline(InconsistencyBehavior::Panic);

        let chunk = iter.next().unwrap().compact_vis();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I
                + 1 1 2
                + 4 9 2
                + 2 5 5
                - 6 6 9",
            ),
            "{}",
            chunk.to_pretty()
        );

        let chunk = iter.next().unwrap().compact_vis();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I
                + 2 2 2",
            ),
            "{}",
            chunk.to_pretty()
        );

        assert_eq!(iter.next(), None);
    }

    #[test]
    fn test_compact_chunk_reconstructed() {
        let key = [0, 1];
        let chunks = vec![
            StreamChunk::from_pretty(
                " I I I
            - 1 1 1
            + 1 1 2
            + 2 5 7
            + 4 9 2
            - 2 5 7
            + 2 5 5
            - 6 6 9
            + 6 6 9
            - 9 9 1",
            ),
            StreamChunk::from_pretty(
                " I I I
            - 6 6 9
            + 9 9 9
            - 9 9 4
            + 2 2 2
            + 9 9 1",
            ),
        ];
        let compactor = StreamChunkUpsertCompactor::new(key.to_vec(), chunks);

        let chunks = compactor.into_compacted_chunks_reconstructed(
            100,
            vec![DataType::Int64, DataType::Int64, DataType::Int64],
            InconsistencyBehavior::Panic,
        );
        let chunk = chunks.into_iter().next().unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                "  I I I
                 + 1 1 2
                 + 4 9 2
                 + 2 5 5
                 - 6 6 9
                 + 2 2 2",
            ),
            "{}",
            chunk.to_pretty()
        );
    }
}
