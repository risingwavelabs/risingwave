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

use std::collections::HashMap;

use itertools::Itertools;
use risingwave_common::array::stream_chunk::StreamChunkMut;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::row::RowExt;
use risingwave_common::types::DataType;
use risingwave_pb::stream_plan::stream_node::StreamKind;

pub use super::change_buffer::InconsistencyBehavior;
use crate::common::change_buffer::output_kind::{RETRACT, UPSERT};
use crate::common::change_buffer::{ChangeBuffer, OutputKind};

/// A helper to remove unnecessary changes in the stream chunks based on the key.
pub struct StreamChunkCompactor {
    chunks: Vec<StreamChunk>,
    key: Vec<usize>,
    input_kind: StreamKind,
}

impl StreamChunkCompactor {
    /// For upsert input, compaction retains the final operation per `key`. Lossless compaction
    /// of the input stream requires its stream key; using another key requires caller-defined
    /// reconciliation. Delete rows only guarantee the stream key columns, and inserts may
    /// overwrite rows from earlier batches. Upsert input always produces upsert operations,
    /// since the old rows needed for retract output are unavailable.
    pub fn new(key: Vec<usize>, chunks: Vec<StreamChunk>, input_kind: StreamKind) -> Self {
        Self {
            chunks,
            key,
            input_kind,
        }
    }

    /// Remove unnecessary changes in the given chunks, by modifying the visibility and ops in place.
    pub fn into_compacted_chunks_inline<const KIND: OutputKind>(
        self,
        ib: InconsistencyBehavior,
    ) -> Vec<StreamChunk> {
        let Self {
            chunks,
            key: key_indices,
            input_kind,
        } = self;

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
                if input_kind == StreamKind::Upsert {
                    cb.apply_upsert_op_row(op, key, op_row);
                } else {
                    cb.apply_op_row(op, key, op_row);
                }
            }
        }

        // For the rows that survive compaction, make them visible.
        for record in cb.into_records() {
            match record {
                Record::Insert { mut new_row } => new_row.set_vis(true),
                Record::Delete { mut old_row } => old_row.set_vis(true),
                Record::Update {
                    mut old_row,
                    mut new_row,
                } => {
                    match KIND {
                        // For upsert output, we only keep the new row with normalized op (`Insert`).
                        UPSERT => new_row.set_vis(true),
                        // For retract output, we keep both the old and new row, and rewrite the ops
                        // to `U-` and `U+` if they are adjacent.
                        RETRACT => {
                            old_row.set_vis(true);
                            new_row.set_vis(true);
                            if old_row.same_chunk(&new_row)
                                && old_row.index() + 1 == new_row.index()
                            {
                                old_row.set_op(Op::UpdateDelete);
                                new_row.set_op(Op::UpdateInsert);
                            }
                        }
                    }
                }
            }
        }

        chunks.into_iter().map(|c| c.into()).collect()
    }

    /// Remove unnecessary changes in the given chunks, by filtering them out and constructing new
    /// chunks, with the given chunk size.
    ///
    /// As in [`Self::into_compacted_chunks_inline`], each operation is applied on its own, so an
    /// update may change the key.
    pub fn into_compacted_chunks_reconstructed<const KIND: OutputKind>(
        self,
        chunk_size: usize,
        data_types: Vec<DataType>,
        ib: InconsistencyBehavior,
    ) -> Vec<StreamChunk> {
        let Self {
            chunks,
            key: key_indices,
            input_kind,
        } = self;

        let estimate_size = chunks.iter().map(|c| c.cardinality()).sum();
        let mut cb = ChangeBuffer::with_capacity(estimate_size).with_inconsistency_behavior(ib);

        for (op, row) in chunks.iter().flat_map(|c| c.rows()) {
            let key = row.project(&key_indices);
            if input_kind == StreamKind::Upsert {
                cb.apply_upsert_op_row(op, key, row);
            } else {
                cb.apply_op_row(op, key, row);
            }
        }

        cb.into_chunks::<KIND>(data_types, chunk_size)
    }
}

/// Remove unnecessary changes in the given chunk, by modifying the visibility and ops in place.
///
/// This is the same as [`StreamChunkCompactor::into_compacted_chunks_inline`] with only one chunk.
pub fn compact_chunk_inline<const KIND: OutputKind>(
    stream_chunk: StreamChunk,
    key_indices: &[usize],
    input_kind: StreamKind,
    ib: InconsistencyBehavior,
) -> StreamChunk {
    Itertools::exactly_one(
        StreamChunkCompactor::new(key_indices.to_vec(), vec![stream_chunk], input_kind)
            .into_compacted_chunks_inline::<KIND>(ib)
            .into_iter(),
    )
    .unwrap_or_else(|_| unreachable!("should have exactly one chunk in the output"))
}

/// Keeps the last change of each key in the given chunk, by modifying the visibility and ops in
/// place.
///
/// Unlike [`compact_chunk_inline`], this does not assume that the chunk starts from what its
/// consumer holds: an insert and a later delete of a key do not cancel, and an update back to the
/// old row is kept. For retract output, the delete that a key starts with gives the old row.
pub fn keep_last_change_inline(
    stream_chunk: StreamChunk,
    key_indices: &[usize],
    output_kind: OutputKind,
) -> StreamChunk {
    let mut chunk = StreamChunkMut::from(stream_chunk);
    let is_delete = |op| matches!(op, Op::Delete | Op::UpdateDelete);

    // For each key, the delete that it starts with, if any, and its last change.
    let changes = {
        let mut changes: HashMap<_, (Option<usize>, usize)> = HashMap::new();
        for i in (0..chunk.capacity()).filter(|i| chunk.vis(*i)) {
            let first_delete = is_delete(chunk.op(i)).then_some(i);
            changes
                .entry(chunk.row_ref(i).project(key_indices))
                .and_modify(|(_, last)| *last = i)
                .or_insert((first_delete, i));
        }
        changes.into_values().collect_vec()
    };

    for i in 0..chunk.capacity() {
        chunk.set_vis(i, false);
    }
    for (first_delete, last) in changes {
        if is_delete(chunk.op(last)) {
            let old = match output_kind {
                RETRACT => first_delete.unwrap_or(last),
                UPSERT => last,
            };
            chunk.set_vis(old, true);
            chunk.set_op(old, Op::Delete);
        } else if output_kind == RETRACT
            && let Some(old) = first_delete
        {
            let adjacent = old + 1 == last;
            chunk.set_vis(old, true);
            chunk.set_op(
                old,
                if adjacent {
                    Op::UpdateDelete
                } else {
                    Op::Delete
                },
            );
            chunk.set_vis(last, true);
            chunk.set_op(
                last,
                if adjacent {
                    Op::UpdateInsert
                } else {
                    Op::Insert
                },
            );
        } else {
            chunk.set_vis(last, true);
            chunk.set_op(last, Op::Insert);
        }
    }
    chunk.into()
}

#[cfg(test)]
mod tests {
    use risingwave_common::test_prelude::StreamChunkTestExt;

    use super::*;

    #[test]
    fn test_keep_last_change_inline() {
        let input = StreamChunk::from_pretty(
            "  I I
             + 1 10
             - 1 10
             - 2 20
             + 2 21
             - 3 30
             + 3 31
             - 3 31
             + 4 40
             + 4 41
             - 5 50
             + 6 60
             + 5 50",
        );
        // Key `1` keeps its delete though it follows the insert, key `3` deletes the row it started
        // with, and key `5` keeps the update back to its old row.
        assert_eq!(
            keep_last_change_inline(input.clone(), &[0], RETRACT).compact_vis(),
            StreamChunk::from_pretty(
                "  I I
                 - 1 10
                U- 2 20
                U+ 2 21
                 - 3 30
                 + 4 41
                 - 5 50
                 + 6 60
                 + 5 50",
            )
        );
        assert_eq!(
            keep_last_change_inline(input, &[0], UPSERT).compact_vis(),
            StreamChunk::from_pretty(
                " I I
                - 1 10
                + 2 21
                - 3 31
                + 4 41
                + 6 60
                + 5 50",
            )
        );
    }

    #[test]
    fn test_upsert_input_retains_final_operation() {
        let input = StreamChunk::from_pretty(
            "I I
            + 1 10
            - 1 .
            + 2 10
            + 2 20
            - 3 .
            + 3 20
            - 4 .
            - 4 .",
        );
        let expected = StreamChunk::from_pretty("I I\n- 1 .\n+ 2 20\n+ 3 20\n- 4 .");
        for reconstructed in [false, true] {
            let chunks = compact_upsert(vec![input.clone()], reconstructed);
            assert_eq!(StreamChunk::concat(chunks).compact_vis(), expected);
        }
    }

    fn compact_upsert(chunks: Vec<StreamChunk>, reconstructed: bool) -> Vec<StreamChunk> {
        let compactor = StreamChunkCompactor::new(vec![0], chunks, StreamKind::Upsert);
        if reconstructed {
            compactor.into_compacted_chunks_reconstructed::<UPSERT>(
                2,
                vec![DataType::Int64; 2],
                InconsistencyBehavior::Panic,
            )
        } else {
            compactor.into_compacted_chunks_inline::<UPSERT>(InconsistencyBehavior::Panic)
        }
    }

    /// Compaction must preserve the effect on downstream state, including when chunks are
    /// independently compacted by the writer and then merged and compacted by the log reader.
    #[test]
    fn test_upsert_compaction_composes() {
        use std::collections::HashMap;

        use risingwave_common::row::{OwnedRow, Row};

        fn apply(
            mut state: HashMap<OwnedRow, OwnedRow>,
            chunks: &[StreamChunk],
        ) -> HashMap<OwnedRow, OwnedRow> {
            for (op, row) in chunks.iter().flat_map(|c| c.rows()) {
                let key = row.project(&[0]).into_owned_row();
                match op {
                    Op::Insert => {
                        state.insert(key, row.to_owned_row());
                    }
                    Op::Delete => {
                        state.remove(&key);
                    }
                    _ => panic!("upsert compaction must normalize operations"),
                }
            }
            state
        }

        let events = ["+ 1 10", "+ 1 20", "- 1 .", "+ 2 10", "+ 2 20", "- 2 ."]
            .map(|event| StreamChunk::from_pretty(&format!("I I\n{event}")));
        let initial_states = ["", "+ 1 99", "+ 2 99", "+ 1 99\n+ 2 99"].map(|rows| {
            apply(
                HashMap::new(),
                &[StreamChunk::from_pretty(&format!("I I\n{rows}"))],
            )
        });

        // Exhaust all sequences of up to four operations across two independent keys, with
        // two different inserted values and key-only tombstones.
        for len in 1..=4 {
            for mut sequence in 0..events.len().pow(len) {
                let input = (0..len)
                    .map(|_| {
                        let event = events[sequence % events.len()].clone();
                        sequence /= events.len();
                        event
                    })
                    .collect_vec();
                for reconstructed in [false, true] {
                    let once = compact_upsert(input.clone(), reconstructed);
                    for split in 0..=input.len() {
                        let mut partitions = compact_upsert(input[..split].to_vec(), reconstructed);
                        partitions.extend(compact_upsert(input[split..].to_vec(), reconstructed));
                        let twice =
                            compact_upsert(vec![StreamChunk::concat(partitions)], reconstructed);
                        for initial in &initial_states {
                            let expected = apply(initial.clone(), &input);
                            assert_eq!(apply(initial.clone(), &once), expected);
                            assert_eq!(apply(initial.clone(), &twice), expected);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn test_compact_chunk_inline_upsert() {
        test_compact_chunk_inline::<UPSERT>();
    }

    #[test]
    fn test_compact_chunk_inline_retract() {
        test_compact_chunk_inline::<RETRACT>();
    }

    fn test_compact_chunk_inline<const KIND: OutputKind>() {
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
        let compactor = StreamChunkCompactor::new(key.to_vec(), chunks, StreamKind::Retract);
        let mut iter = compactor
            .into_compacted_chunks_inline::<KIND>(InconsistencyBehavior::Panic)
            .into_iter();

        let chunk = iter.next().unwrap().compact_vis();
        let expected = match KIND {
            RETRACT => StreamChunk::from_pretty(
                " I I I
                U- 1 1 1
                U+ 1 1 2
                + 4 9 2
                + 2 5 5
                - 6 6 9",
            ),
            UPSERT => StreamChunk::from_pretty(
                " I I I
                + 1 1 2
                + 4 9 2
                + 2 5 5
                - 6 6 9",
            ),
        };
        assert_eq!(chunk, expected, "{}", chunk.to_pretty());

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

    /// The two rows of an update are compacted apart, as they may have different keys.
    #[test]
    fn test_compact_update_changing_key() {
        let input = StreamChunk::from_pretty(
            "  I I
            U- 1 1
            U+ 1 2",
        );
        for reconstructed in [false, true] {
            let compactor =
                StreamChunkCompactor::new(vec![0, 1], vec![input.clone()], StreamKind::Retract);
            let chunks = if reconstructed {
                compactor.into_compacted_chunks_reconstructed::<RETRACT>(
                    2,
                    vec![DataType::Int64; 2],
                    InconsistencyBehavior::Panic,
                )
            } else {
                compactor.into_compacted_chunks_inline::<RETRACT>(InconsistencyBehavior::Panic)
            };
            assert_eq!(
                StreamChunk::concat(chunks).compact_vis(),
                StreamChunk::from_pretty(
                    " I I
                    - 1 1
                    + 1 2",
                ),
                "reconstructed: {reconstructed}"
            );
        }
    }

    #[test]
    fn test_compact_chunk_reconstructed_upsert() {
        test_compact_chunk_reconstructed::<UPSERT>();
    }

    #[test]
    fn test_compact_chunk_reconstructed_retract() {
        test_compact_chunk_reconstructed::<RETRACT>();
    }

    fn test_compact_chunk_reconstructed<const KIND: OutputKind>() {
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
        let compactor = StreamChunkCompactor::new(key.to_vec(), chunks, StreamKind::Retract);

        let chunks = compactor.into_compacted_chunks_reconstructed::<KIND>(
            100,
            vec![DataType::Int64, DataType::Int64, DataType::Int64],
            InconsistencyBehavior::Panic,
        );
        let chunk = chunks.into_iter().next().unwrap();
        let expected = match KIND {
            RETRACT => StreamChunk::from_pretty(
                "  I I I
                 U- 1 1 1
                 U+ 1 1 2
                 + 4 9 2
                 + 2 5 5
                 - 6 6 9
                 + 2 2 2",
            ),
            UPSERT => StreamChunk::from_pretty(
                "  I I I
                 + 1 1 2
                 + 4 9 2
                 + 2 5 5
                 - 6 6 9
                 + 2 2 2",
            ),
        };
        assert_eq!(chunk, expected, "{}", chunk.to_pretty());
    }
}
