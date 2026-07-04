// Copyright 2026 RisingWave Labs
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

// The diff core is introduced before the planner/executor plumbing so the merge behavior can be
// tested independently.
#![allow(dead_code)]

use std::cmp::Ordering;

use futures::{Stream, StreamExt, pin_mut};
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::{OrderType, cmp_datum_iter};

use crate::executor::{StreamExecutorError, StreamExecutorResult};

#[allow(dead_code)]
pub enum SnapshotReadOutput {
    Chunk(StreamChunk),
    Progress(OwnedRow),
    Finished,
}

/// Builds upsert-style correction chunks that transform the current CDC table state (`right`) into
/// the upstream snapshot state (`left`).
///
/// Both inputs must be sorted by the same primary-key order. The emitted chunks are intentionally
/// not full retract changelog:
///
/// * `+left` for left-only rows and changed rows.
/// * `-right` for right-only rows.
/// * no output for rows equal on `compare_indices`.
///
/// For same-primary-key changed rows, only the new upstream snapshot row is emitted. The current
/// CDC table path consumes this through the table materialize with overwrite conflict handling,
/// which can derive the old row from state and produce the correct downstream update semantics.
/// Do not feed this output directly to a consumer that requires explicit `-old, +new` retract
/// pairs for updates.
pub(crate) fn diff_ordered_rows_to_chunks(
    left: impl IntoIterator<Item = OwnedRow>,
    right: impl IntoIterator<Item = OwnedRow>,
    pk_indices: &[usize],
    pk_order: &[OrderType],
    compare_indices: &[usize],
    data_types: &[DataType],
    chunk_size: usize,
) -> Vec<StreamChunk> {
    diff_ordered_rows_to_snapshot_outputs(
        left,
        right,
        pk_indices,
        pk_order,
        compare_indices,
        data_types,
        chunk_size,
    )
    .into_iter()
    .filter_map(|output| match output {
        SnapshotReadOutput::Chunk(chunk) => Some(chunk),
        SnapshotReadOutput::Progress(_) => None,
        SnapshotReadOutput::Finished => unreachable!("diff output should not include Finished"),
    })
    .collect()
}

/// Builds snapshot-reader outputs for diff backfill.
///
/// Equal rows are not emitted as chunks, but they can still advance the snapshot read progress.
/// The progress event is required by the non-parallel CDC backfill path, where `current_pk_pos`
/// controls which upstream changelog rows may be released.
pub(crate) fn diff_ordered_rows_to_snapshot_outputs(
    left: impl IntoIterator<Item = OwnedRow>,
    right: impl IntoIterator<Item = OwnedRow>,
    pk_indices: &[usize],
    pk_order: &[OrderType],
    compare_indices: &[usize],
    data_types: &[DataType],
    chunk_size: usize,
) -> Vec<SnapshotReadOutput> {
    assert!(chunk_size > 0, "chunk_size must be greater than 0");
    assert_eq!(
        pk_indices.len(),
        pk_order.len(),
        "pk_indices and pk_order must have the same length"
    );

    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();
    let mut output = DiffChunkBuilder::new(data_types, chunk_size);
    let mut outputs = vec![];
    let mut pending_progress = None;

    loop {
        match (left.peek(), right.peek()) {
            (Some(left_row), Some(right_row)) => {
                match compare_pk(left_row, right_row, pk_indices, pk_order) {
                    Ordering::Less => {
                        output.push(Op::Insert, left.next().expect("peeked left row"));
                    }
                    Ordering::Greater => {
                        output.push(Op::Delete, right.next().expect("peeked right row"));
                    }
                    Ordering::Equal => {
                        let left_row = left.next().expect("peeked left row");
                        let right_row = right.next().expect("peeked right row");
                        if !rows_equal_on_indices(&left_row, &right_row, compare_indices) {
                            output.push(Op::Insert, left_row);
                            outputs.extend(output.take_flushed_chunks());
                            pending_progress = None;
                        } else {
                            pending_progress = Some(project_pk(&left_row, pk_indices));
                        }
                    }
                }
            }
            (Some(_), None) => {
                output.push(Op::Insert, left.next().expect("peeked left row"));
                outputs.extend(output.take_flushed_chunks());
                pending_progress = None;
            }
            (None, Some(_)) => {
                output.push(Op::Delete, right.next().expect("peeked right row"));
                outputs.extend(output.take_flushed_chunks());
                pending_progress = None;
            }
            (None, None) => break,
        }
    }

    outputs.extend(output.finish());
    if let Some(pos) = pending_progress {
        outputs.push(SnapshotReadOutput::Progress(pos));
    }
    outputs
}

#[try_stream(ok = SnapshotReadOutput, error = StreamExecutorError)]
pub(crate) async fn diff_ordered_row_streams(
    left: impl Stream<Item = StreamExecutorResult<OwnedRow>> + Send,
    right: impl Stream<Item = StreamExecutorResult<OwnedRow>> + Send,
    pk_indices: Vec<usize>,
    pk_order: Vec<OrderType>,
    compare_indices: Vec<usize>,
    data_types: Vec<DataType>,
    chunk_size: usize,
) {
    assert!(chunk_size > 0, "chunk_size must be greater than 0");
    assert_eq!(
        pk_indices.len(),
        pk_order.len(),
        "pk_indices and pk_order must have the same length"
    );

    let left_stream = left;
    let right_stream = right;
    pin_mut!(left_stream);
    pin_mut!(right_stream);

    let mut left_row = left_stream.next().await.transpose()?;
    let mut right_row = right_stream.next().await.transpose()?;
    let mut output = DiffChunkBuilder::new(&data_types, chunk_size);
    let mut pending_progress = None;
    let mut skipped_since_progress = 0;

    loop {
        match (&left_row, &right_row) {
            (Some(left), Some(right)) => match compare_pk(left, right, &pk_indices, &pk_order) {
                Ordering::Less => {
                    output.push(Op::Insert, left_row.take().expect("checked left row"));
                    for output in output.take_flushed_chunks() {
                        yield output;
                    }
                    pending_progress = None;
                    skipped_since_progress = 0;
                    left_row = left_stream.next().await.transpose()?;
                }
                Ordering::Greater => {
                    output.push(Op::Delete, right_row.take().expect("checked right row"));
                    for output in output.take_flushed_chunks() {
                        yield output;
                    }
                    pending_progress = None;
                    skipped_since_progress = 0;
                    right_row = right_stream.next().await.transpose()?;
                }
                Ordering::Equal => {
                    let left = left_row.take().expect("checked left row");
                    let right = right_row.take().expect("checked right row");
                    if !rows_equal_on_indices(&left, &right, &compare_indices) {
                        output.push(Op::Insert, left);
                        for output in output.take_flushed_chunks() {
                            yield output;
                        }
                        pending_progress = None;
                        skipped_since_progress = 0;
                    } else {
                        pending_progress = Some(project_pk(&left, &pk_indices));
                        skipped_since_progress += 1;
                        if skipped_since_progress >= chunk_size {
                            // A progress position is checkpointed by the non-parallel executor.
                            // Flush any earlier correction first so recovery can never resume past
                            // output that was still buffered locally.
                            output.flush();
                            for output in output.take_flushed_chunks() {
                                yield output;
                            }
                            yield SnapshotReadOutput::Progress(
                                pending_progress.take().expect("progress exists"),
                            );
                            skipped_since_progress = 0;
                        }
                    }
                    left_row = left_stream.next().await.transpose()?;
                    right_row = right_stream.next().await.transpose()?;
                }
            },
            (Some(_), None) => {
                output.push(Op::Insert, left_row.take().expect("checked left row"));
                for output in output.take_flushed_chunks() {
                    yield output;
                }
                pending_progress = None;
                skipped_since_progress = 0;
                left_row = left_stream.next().await.transpose()?;
            }
            (None, Some(_)) => {
                output.push(Op::Delete, right_row.take().expect("checked right row"));
                for output in output.take_flushed_chunks() {
                    yield output;
                }
                pending_progress = None;
                skipped_since_progress = 0;
                right_row = right_stream.next().await.transpose()?;
            }
            (None, None) => break,
        }
    }

    for output in output.finish() {
        yield output;
    }
    if let Some(pos) = pending_progress {
        yield SnapshotReadOutput::Progress(pos);
    }
    yield SnapshotReadOutput::Finished;
}

fn compare_pk(
    left: &OwnedRow,
    right: &OwnedRow,
    pk_indices: &[usize],
    pk_order: &[OrderType],
) -> Ordering {
    let left_pk = left.project(pk_indices);
    let right_pk = right.project(pk_indices);
    cmp_datum_iter(left_pk.iter(), right_pk.iter(), pk_order.iter().copied())
}

fn rows_equal_on_indices(left: &OwnedRow, right: &OwnedRow, indices: &[usize]) -> bool {
    indices
        .iter()
        .all(|idx| left.datum_at(*idx) == right.datum_at(*idx))
}

fn project_pk(row: &OwnedRow, pk_indices: &[usize]) -> OwnedRow {
    row.project(pk_indices).into_owned_row()
}

struct DiffChunkBuilder<'a> {
    data_types: &'a [DataType],
    chunk_size: usize,
    buffer: Vec<(Op, OwnedRow)>,
    chunks: Vec<SnapshotReadOutput>,
}

impl<'a> DiffChunkBuilder<'a> {
    fn new(data_types: &'a [DataType], chunk_size: usize) -> Self {
        Self {
            data_types,
            chunk_size,
            buffer: Vec::with_capacity(chunk_size),
            chunks: vec![],
        }
    }

    fn push(&mut self, op: Op, row: OwnedRow) {
        self.buffer.push((op, row));
        if self.buffer.len() >= self.chunk_size {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }
        self.chunks
            .push(SnapshotReadOutput::Chunk(StreamChunk::from_rows(
                &self.buffer,
                self.data_types,
            )));
        self.buffer.clear();
    }

    fn take_flushed_chunks(&mut self) -> Vec<SnapshotReadOutput> {
        std::mem::take(&mut self.chunks)
    }

    fn finish(mut self) -> Vec<SnapshotReadOutput> {
        self.flush();
        self.chunks
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::types::ScalarImpl;

    use super::*;

    fn row(pk: i32, value: &str) -> OwnedRow {
        OwnedRow::new(vec![Some(pk.into()), Some(ScalarImpl::from(value))])
    }

    fn rows_from_chunks(chunks: &[StreamChunk]) -> Vec<(Op, OwnedRow)> {
        chunks
            .iter()
            .flat_map(|chunk| {
                chunk
                    .rows()
                    .map(|(op, row)| (op, row.to_owned_row()))
                    .collect_vec()
            })
            .collect_vec()
    }

    fn rows_from_outputs(outputs: &[SnapshotReadOutput]) -> Vec<(Op, OwnedRow)> {
        outputs
            .iter()
            .filter_map(|output| match output {
                SnapshotReadOutput::Chunk(chunk) => {
                    Some(rows_from_chunks(std::slice::from_ref(chunk)))
                }
                SnapshotReadOutput::Progress(_) => None,
                SnapshotReadOutput::Finished => None,
            })
            .flatten()
            .collect_vec()
    }

    fn progress_from_outputs(outputs: &[SnapshotReadOutput]) -> Vec<OwnedRow> {
        outputs
            .iter()
            .filter_map(|output| match output {
                SnapshotReadOutput::Chunk(_) => None,
                SnapshotReadOutput::Progress(pos) => Some(pos.clone()),
                SnapshotReadOutput::Finished => None,
            })
            .collect_vec()
    }

    async fn collect_stream_outputs(
        left: Vec<OwnedRow>,
        right: Vec<OwnedRow>,
        chunk_size: usize,
    ) -> Vec<SnapshotReadOutput> {
        let left = futures::stream::iter(left.into_iter().map(Ok));
        let right = futures::stream::iter(right.into_iter().map(Ok));
        let outputs = diff_ordered_row_streams(
            left,
            right,
            vec![0],
            vec![OrderType::ascending()],
            vec![0, 1],
            vec![DataType::Int32, DataType::Varchar],
            chunk_size,
        );
        pin_mut!(outputs);

        let mut collected = vec![];
        while let Some(output) = outputs.next().await {
            collected.push(output.expect("diff output"));
        }
        collected
    }

    fn diff(left: Vec<OwnedRow>, right: Vec<OwnedRow>, chunk_size: usize) -> Vec<(Op, OwnedRow)> {
        let chunks = diff_ordered_rows_to_chunks(
            left,
            right,
            &[0],
            &[OrderType::ascending()],
            &[0, 1],
            &[DataType::Int32, DataType::Varchar],
            chunk_size,
        );
        rows_from_chunks(&chunks)
    }

    #[test]
    fn test_diff_insert_delete_update_and_skip() {
        let left = vec![row(1, "same"), row(2, "left-only"), row(4, "changed-new")];
        let right = vec![row(1, "same"), row(3, "right-only"), row(4, "changed-old")];

        assert_eq!(
            diff(left, right, 16),
            vec![
                (Op::Insert, row(2, "left-only")),
                (Op::Delete, row(3, "right-only")),
                (Op::Insert, row(4, "changed-new")),
            ]
        );
    }

    #[test]
    fn test_diff_respects_chunk_size() {
        let chunks = diff_ordered_rows_to_chunks(
            vec![row(1, "a"), row(2, "b"), row(3, "c")],
            vec![],
            &[0],
            &[OrderType::ascending()],
            &[0, 1],
            &[DataType::Int32, DataType::Varchar],
            2,
        );

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].cardinality(), 2);
        assert_eq!(chunks[1].cardinality(), 1);
    }

    #[test]
    fn test_diff_can_ignore_internal_columns() {
        let data_types = [DataType::Int32, DataType::Varchar, DataType::Int64];
        let left = vec![OwnedRow::new(vec![
            Some(1.into()),
            Some(ScalarImpl::from("same")),
            Some(42_i64.into()),
        ])];
        let right = vec![OwnedRow::new(vec![
            Some(1.into()),
            Some(ScalarImpl::from("same")),
            Some(41_i64.into()),
        ])];

        let chunks = diff_ordered_rows_to_chunks(
            left,
            right,
            &[0],
            &[OrderType::ascending()],
            &[0, 1],
            &data_types,
            16,
        );

        assert!(chunks.is_empty());
    }

    #[test]
    fn test_diff_emits_progress_for_all_equal_rows() {
        let outputs = diff_ordered_rows_to_snapshot_outputs(
            vec![row(1, "same"), row(2, "same")],
            vec![row(1, "same"), row(2, "same")],
            &[0],
            &[OrderType::ascending()],
            &[0, 1],
            &[DataType::Int32, DataType::Varchar],
            16,
        );

        assert!(rows_from_outputs(&outputs).is_empty());
        assert_eq!(
            progress_from_outputs(&outputs),
            vec![OwnedRow::new(vec![Some(2.into())])]
        );
    }

    #[test]
    fn test_diff_emits_progress_for_equal_tail() {
        let outputs = diff_ordered_rows_to_snapshot_outputs(
            vec![row(1, "left-only"), row(2, "same")],
            vec![row(2, "same")],
            &[0],
            &[OrderType::ascending()],
            &[0, 1],
            &[DataType::Int32, DataType::Varchar],
            16,
        );

        assert_eq!(
            rows_from_outputs(&outputs),
            vec![(Op::Insert, row(1, "left-only"))]
        );
        assert_eq!(
            progress_from_outputs(&outputs),
            vec![OwnedRow::new(vec![Some(2.into())])]
        );
    }

    #[tokio::test]
    async fn test_streaming_diff_emits_finished() {
        let outputs = collect_stream_outputs(
            vec![row(1, "same"), row(2, "new")],
            vec![row(1, "same"), row(3, "old")],
            16,
        )
        .await;

        assert_eq!(
            rows_from_outputs(&outputs),
            vec![(Op::Insert, row(2, "new")), (Op::Delete, row(3, "old"))]
        );
        assert!(matches!(outputs.last(), Some(SnapshotReadOutput::Finished)));
    }

    #[tokio::test]
    async fn test_streaming_diff_emits_periodic_progress() {
        let outputs = collect_stream_outputs(
            vec![row(1, "same"), row(2, "same"), row(3, "same")],
            vec![row(1, "same"), row(2, "same"), row(3, "same")],
            2,
        )
        .await;

        assert_eq!(
            progress_from_outputs(&outputs),
            vec![
                OwnedRow::new(vec![Some(2.into())]),
                OwnedRow::new(vec![Some(3.into())]),
            ]
        );
        assert!(matches!(outputs.last(), Some(SnapshotReadOutput::Finished)));
    }

    #[tokio::test]
    async fn test_streaming_diff_flushes_correction_before_progress() {
        let outputs = collect_stream_outputs(
            vec![row(1, "new"), row(2, "same"), row(3, "same")],
            vec![row(1, "old"), row(2, "same"), row(3, "same")],
            2,
        )
        .await;

        assert!(matches!(
            outputs.first(),
            Some(SnapshotReadOutput::Chunk(_))
        ));
        assert_eq!(
            rows_from_outputs(&outputs[..1]),
            vec![(Op::Insert, row(1, "new"))]
        );
        assert!(matches!(
            outputs.get(1),
            Some(SnapshotReadOutput::Progress(pos))
                if pos == &OwnedRow::new(vec![Some(3.into())])
        ));
        assert!(matches!(outputs.last(), Some(SnapshotReadOutput::Finished)));
    }

    #[test]
    fn test_diff_supports_descending_pk_order() {
        let left = vec![row(5, "left"), row(3, "same"), row(1, "left")];
        let right = vec![row(4, "right"), row(3, "same"), row(2, "right")];
        let chunks = diff_ordered_rows_to_chunks(
            left,
            right,
            &[0],
            &[OrderType::descending()],
            &[0, 1],
            &[DataType::Int32, DataType::Varchar],
            16,
        );

        assert_eq!(
            rows_from_chunks(&chunks),
            vec![
                (Op::Insert, row(5, "left")),
                (Op::Delete, row(4, "right")),
                (Op::Delete, row(2, "right")),
                (Op::Insert, row(1, "left")),
            ]
        );
    }
}
