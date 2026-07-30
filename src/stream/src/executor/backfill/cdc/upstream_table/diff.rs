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
use std::sync::Arc;

use futures::{Stream, StreamExt, pin_mut};
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, StreamChunk, StreamChunkBuilder};
use risingwave_common::bail;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::DataType;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common_rate_limit::RateLimiter;

use crate::executor::backfill::utils::cmp_pk_unsigned_aware;
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
#[expect(clippy::too_many_arguments)]
pub(crate) fn diff_ordered_rows_to_chunks(
    left: impl IntoIterator<Item = OwnedRow>,
    right: impl IntoIterator<Item = OwnedRow>,
    pk_indices: &[usize],
    pk_order: &[OrderType],
    pk_needs_unsigned_i64_compare: &[bool],
    compare_indices: &[usize],
    data_types: &[DataType],
    chunk_size: usize,
) -> Vec<StreamChunk> {
    diff_ordered_rows_to_snapshot_outputs(
        left,
        right,
        pk_indices,
        pk_order,
        pk_needs_unsigned_i64_compare,
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
#[expect(clippy::too_many_arguments)]
pub(crate) fn diff_ordered_rows_to_snapshot_outputs(
    left: impl IntoIterator<Item = OwnedRow>,
    right: impl IntoIterator<Item = OwnedRow>,
    pk_indices: &[usize],
    pk_order: &[OrderType],
    pk_needs_unsigned_i64_compare: &[bool],
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
    assert_eq!(
        pk_indices.len(),
        pk_needs_unsigned_i64_compare.len(),
        "pk_indices and unsigned comparison flags must have the same length"
    );
    assert!(
        !pk_needs_unsigned_i64_compare.iter().any(|flag| *flag),
        "CDC resnapshot does not support MySQL BIGINT UNSIGNED primary keys"
    );

    let mut left = left.into_iter().peekable();
    let mut right = right.into_iter().peekable();
    let mut output = StreamChunkBuilder::new(chunk_size, data_types.to_vec());
    let mut outputs = vec![];
    let mut pending_progress = None;

    loop {
        match (left.peek(), right.peek()) {
            (Some(left_row), Some(right_row)) => {
                match compare_pk(
                    left_row,
                    right_row,
                    pk_indices,
                    pk_order,
                    pk_needs_unsigned_i64_compare,
                ) {
                    Ordering::Less => {
                        if let Some(chunk) =
                            output.append_row(Op::Insert, left.next().expect("peeked left row"))
                        {
                            outputs.push(SnapshotReadOutput::Chunk(chunk));
                        }
                    }
                    Ordering::Greater => {
                        if let Some(chunk) =
                            output.append_row(Op::Delete, right.next().expect("peeked right row"))
                        {
                            outputs.push(SnapshotReadOutput::Chunk(chunk));
                        }
                    }
                    Ordering::Equal => {
                        let left_row = left.next().expect("peeked left row");
                        let right_row = right.next().expect("peeked right row");
                        if !rows_equal_on_indices(&left_row, &right_row, compare_indices) {
                            if let Some(chunk) = output.append_row(Op::Insert, left_row) {
                                outputs.push(SnapshotReadOutput::Chunk(chunk));
                            }
                            pending_progress = None;
                        } else {
                            pending_progress = Some(project_pk(&left_row, pk_indices));
                        }
                    }
                }
            }
            (Some(_), None) => {
                if let Some(chunk) =
                    output.append_row(Op::Insert, left.next().expect("peeked left row"))
                {
                    outputs.push(SnapshotReadOutput::Chunk(chunk));
                }
                pending_progress = None;
            }
            (None, Some(_)) => {
                if let Some(chunk) =
                    output.append_row(Op::Delete, right.next().expect("peeked right row"))
                {
                    outputs.push(SnapshotReadOutput::Chunk(chunk));
                }
                pending_progress = None;
            }
            (None, None) => break,
        }
    }

    if let Some(chunk) = output.take() {
        outputs.push(SnapshotReadOutput::Chunk(chunk));
    }
    if let Some(pos) = pending_progress {
        outputs.push(SnapshotReadOutput::Progress(pos));
    }
    outputs
}

#[expect(clippy::too_many_arguments)]
#[try_stream(ok = SnapshotReadOutput, error = StreamExecutorError)]
pub(crate) async fn diff_ordered_row_streams(
    left: impl Stream<Item = StreamExecutorResult<OwnedRow>> + Send,
    right: impl Stream<Item = StreamExecutorResult<OwnedRow>> + Send,
    pk_indices: Vec<usize>,
    pk_order: Vec<OrderType>,
    pk_needs_unsigned_i64_compare: Vec<bool>,
    compare_indices: Vec<usize>,
    data_types: Vec<DataType>,
    chunk_size: usize,
    rate_limiter: Arc<RateLimiter>,
) {
    if chunk_size == 0 {
        bail!("chunk_size must be greater than 0");
    }
    if pk_indices.len() != pk_order.len() {
        bail!("pk_indices and pk_order must have the same length");
    }
    if pk_indices.len() != pk_needs_unsigned_i64_compare.len() {
        bail!("pk_indices and unsigned comparison flags must have the same length");
    }
    if pk_needs_unsigned_i64_compare.iter().any(|flag| *flag) {
        bail!(
            "CDC resnapshot does not support MySQL BIGINT UNSIGNED primary keys: the upstream \
             unsigned order differs from the signed RisingWave storage scan order"
        );
    }

    let left_stream = left;
    let right_stream = right;
    pin_mut!(left_stream);
    pin_mut!(right_stream);

    let mut left_row = left_stream.next().await.transpose()?;
    let mut right_row = right_stream.next().await.transpose()?;
    let mut output = StreamChunkBuilder::new(chunk_size, data_types);
    let mut pending_progress = None;
    let mut skipped_since_progress = 0;

    loop {
        match (&left_row, &right_row) {
            (Some(left), Some(right)) => match compare_pk(
                left,
                right,
                &pk_indices,
                &pk_order,
                &pk_needs_unsigned_i64_compare,
            ) {
                Ordering::Less => {
                    if let Some(chunk) =
                        output.append_row(Op::Insert, left_row.take().expect("checked left row"))
                    {
                        yield SnapshotReadOutput::Chunk(chunk);
                    }
                    pending_progress = None;
                    skipped_since_progress = 0;
                    left_row = left_stream.next().await.transpose()?;
                }
                Ordering::Greater => {
                    rate_limiter.wait(1).await;
                    if let Some(chunk) =
                        output.append_row(Op::Delete, right_row.take().expect("checked right row"))
                    {
                        yield SnapshotReadOutput::Chunk(chunk);
                    }
                    pending_progress = None;
                    skipped_since_progress = 0;
                    right_row = right_stream.next().await.transpose()?;
                }
                Ordering::Equal => {
                    let left = left_row.take().expect("checked left row");
                    let right = right_row.take().expect("checked right row");
                    if !rows_equal_on_indices(&left, &right, &compare_indices) {
                        if let Some(chunk) = output.append_row(Op::Insert, left) {
                            yield SnapshotReadOutput::Chunk(chunk);
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
                            if let Some(chunk) = output.take() {
                                yield SnapshotReadOutput::Chunk(chunk);
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
                if let Some(chunk) =
                    output.append_row(Op::Insert, left_row.take().expect("checked left row"))
                {
                    yield SnapshotReadOutput::Chunk(chunk);
                }
                pending_progress = None;
                skipped_since_progress = 0;
                left_row = left_stream.next().await.transpose()?;
            }
            (None, Some(_)) => {
                rate_limiter.wait(1).await;
                if let Some(chunk) =
                    output.append_row(Op::Delete, right_row.take().expect("checked right row"))
                {
                    yield SnapshotReadOutput::Chunk(chunk);
                }
                pending_progress = None;
                skipped_since_progress = 0;
                right_row = right_stream.next().await.transpose()?;
            }
            (None, None) => break,
        }
    }

    if let Some(chunk) = output.take() {
        yield SnapshotReadOutput::Chunk(chunk);
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
    pk_needs_unsigned_i64_compare: &[bool],
) -> Ordering {
    let left_pk = left.project(pk_indices);
    let right_pk = right.project(pk_indices);
    cmp_pk_unsigned_aware(
        left_pk.iter(),
        right_pk.iter(),
        pk_order,
        pk_needs_unsigned_i64_compare,
    )
}

fn rows_equal_on_indices(left: &OwnedRow, right: &OwnedRow, indices: &[usize]) -> bool {
    indices
        .iter()
        .all(|idx| left.datum_at(*idx) == right.datum_at(*idx))
}

fn project_pk(row: &OwnedRow, pk_indices: &[usize]) -> OwnedRow {
    row.project(pk_indices).into_owned_row()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::types::ScalarImpl;
    use risingwave_common_rate_limit::RateLimit;

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
            vec![false],
            vec![0, 1],
            vec![DataType::Int32, DataType::Varchar],
            chunk_size,
            Arc::new(RateLimiter::new(RateLimit::Disabled)),
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
            &[false],
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
            &[false],
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
            &[false],
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
            &[false],
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
            &[false],
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

    #[tokio::test]
    async fn test_streaming_diff_rejects_unsigned_i64_pk_ordering() {
        let left = futures::stream::iter(vec![
            Ok(OwnedRow::new(vec![
                Some(ScalarImpl::Int64(5)),
                Some(ScalarImpl::from("low")),
            ])),
            Ok(OwnedRow::new(vec![
                Some(ScalarImpl::Int64(-1)),
                Some(ScalarImpl::from("high")),
            ])),
        ]);
        let right = futures::stream::iter(vec![
            Ok(OwnedRow::new(vec![
                Some(ScalarImpl::Int64(-1)),
                Some(ScalarImpl::from("high")),
            ])),
            Ok(OwnedRow::new(vec![
                Some(ScalarImpl::Int64(5)),
                Some(ScalarImpl::from("low")),
            ])),
        ]);
        let outputs = diff_ordered_row_streams(
            left,
            right,
            vec![0],
            vec![OrderType::ascending()],
            vec![true],
            vec![0, 1],
            vec![DataType::Int64, DataType::Varchar],
            1,
            Arc::new(RateLimiter::new(RateLimit::Disabled)),
        );
        pin_mut!(outputs);

        let Some(Err(err)) = outputs.next().await else {
            panic!("unsigned i64 primary key ordering must be rejected");
        };
        assert!(err.to_string().contains("BIGINT UNSIGNED"));
    }

    #[tokio::test]
    async fn test_storage_only_deletes_share_snapshot_rate_limiter() {
        let rate_limiter = Arc::new(RateLimiter::new(RateLimit::Pause));
        let left = futures::stream::iter(Vec::<StreamExecutorResult<OwnedRow>>::new());
        let right = futures::stream::iter(vec![
            Ok(row(1, "old")),
            Ok(row(2, "old")),
            Ok(row(3, "old")),
        ]);
        let outputs = diff_ordered_row_streams(
            left,
            right,
            vec![0],
            vec![OrderType::ascending()],
            vec![false],
            vec![0, 1],
            vec![DataType::Int32, DataType::Varchar],
            2,
            rate_limiter.clone(),
        );
        pin_mut!(outputs);

        assert!(
            tokio::time::timeout(Duration::from_millis(20), outputs.next())
                .await
                .is_err(),
            "right-only delete bypassed the paused snapshot limiter"
        );

        rate_limiter.update(RateLimit::Disabled);
        let mut collected = vec![];
        while let Some(output) = outputs.next().await {
            collected.push(output.expect("diff output"));
        }
        assert_eq!(
            rows_from_outputs(&collected),
            vec![
                (Op::Delete, row(1, "old")),
                (Op::Delete, row(2, "old")),
                (Op::Delete, row(3, "old")),
            ]
        );
    }

    #[tokio::test]
    async fn test_diff_does_not_double_charge_left_rows() {
        let left = futures::stream::iter(vec![Ok(row(1, "new"))]);
        let right = futures::stream::iter(Vec::<StreamExecutorResult<OwnedRow>>::new());
        let outputs = diff_ordered_row_streams(
            left,
            right,
            vec![0],
            vec![OrderType::ascending()],
            vec![false],
            vec![0, 1],
            vec![DataType::Int32, DataType::Varchar],
            1,
            Arc::new(RateLimiter::new(RateLimit::Pause)),
        );
        pin_mut!(outputs);

        let output = tokio::time::timeout(Duration::from_millis(20), outputs.next())
            .await
            .expect("left row was charged twice")
            .expect("diff output")
            .expect("successful diff output");
        assert_eq!(
            rows_from_outputs(&[output]),
            vec![(Op::Insert, row(1, "new"))]
        );
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
            &[false],
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
