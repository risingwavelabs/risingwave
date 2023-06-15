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

use std::cmp::Ordering;
use std::ops::Bound;
use std::pin::pin;
use std::sync::Arc;

use await_tree::InstrumentAwait;
use either::Either;
use futures::stream::select_with_strategy;
use futures::{pin_mut, stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::Datum;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::{cmp_datum, OrderType};
use risingwave_storage::table::collect_data_chunk;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::{expect_first_barrier, BoxedExecutor, Executor, ExecutorInfo, Message, PkIndicesRef};
use crate::common::table::iter_utils::merge_sort;
use crate::common::table::state_table::StateTable;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{PkIndices, StreamExecutorResult, Watermark};
use crate::task::{ActorId, CreateMviewProgress};

pub struct ArrangementBackfillExecutor<S: StateStore> {
    /// Upstream table
    upstream_table: StateTable<S>,

    /// Upstream with the same schema with the upstream table.
    upstream: BoxedExecutor,

    /// Internal state table for persisting state of backfill state.
    state_table: Option<StateTable<S>>,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    progress: CreateMviewProgress,

    actor_id: ActorId,

    info: ExecutorInfo,

    metrics: Arc<StreamingMetrics>,
}

const CHUNK_SIZE: usize = 1024;

impl<S> ArrangementBackfillExecutor<S>
where
    S: StateStore,
{
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)]
    pub fn new(
        upstream_table: StateTable<S>,
        upstream: BoxedExecutor,
        state_table: Option<StateTable<S>>,
        output_indices: Vec<usize>,
        progress: CreateMviewProgress,
        schema: Schema,
        pk_indices: PkIndices,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "ArrangementBackfillExecutor".to_owned(),
            },
            upstream_table,
            upstream,
            state_table,
            output_indices,
            actor_id: progress.actor_id(),
            progress,
            metrics,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // The primary key columns, in the output columns of the upstream_table scan.
        // let pk_in_output_indices = self.upstream_table.pk_in_output_indices().unwrap();
        let pk_in_output_indices = self.upstream_table.pk_indices();
        let state_len = pk_in_output_indices.len() + 2; // +1 for backfill_finished, +1 for vnode key.

        let pk_order = self.upstream_table.pk_serde().get_order_types();

        let upstream_table_id = self.upstream_table.table_id();

        let schema = Arc::new(self.upstream.schema().clone());

        let mut upstream = self.upstream.execute();

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let init_epoch = first_barrier.epoch.prev;
        if let Some(state_table) = self.state_table.as_mut() {
            state_table.init_epoch(first_barrier.epoch);
        }

        let is_finished = if let Some(state_table) = self.state_table.as_mut() {
            let is_finished = Self::check_all_vnode_finished(state_table, state_len).await?;
            if is_finished {
                assert!(!first_barrier.is_newly_added(self.actor_id));
            }
            is_finished
        } else {
            // Maintain backwards compatibility with no state table
            !first_barrier.is_newly_added(self.actor_id)
        };

        // If the snapshot is empty, we don't need to backfill.
        // We cannot complete progress now, as we want to persist
        // finished state to state store first.
        // As such we will wait for next barrier.
        let is_snapshot_empty: bool = {
            if is_finished {
                // It is finished, so just assign a value to avoid accessing storage table again.
                false
            } else {
                let snapshot = Self::snapshot_read(
                    schema.clone(),
                    &self.upstream_table,
                    init_epoch,
                    None,
                    false,
                );
                pin_mut!(snapshot);
                snapshot.try_next().await?.unwrap().is_none()
            }
        };

        // | backfill_is_finished | snapshot_empty | need_to_backfill |
        // | t                    | t/f            | f                |
        // | f                    | t              | f                |
        // | f                    | f              | t                |
        let to_backfill = !is_finished && !is_snapshot_empty;

        // Current position of the upstream_table storage primary key.
        // `None` means it starts from the beginning.
        let mut current_pos: Option<OwnedRow> = None;

        // Use these to persist state.
        // They contain the backfill position,
        // as well as the progress.
        // However, they do not contain the vnode key at index 0.
        // That is filled in when we flush the state table.
        let mut current_state: Vec<Datum> = vec![None; state_len];
        let mut old_state: Option<Vec<Datum>> = None;

        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);

        // If no need backfill, but state was still "unfinished" we need to finish it.
        // So we just update the state + progress to meta at the next barrier to finish progress,
        // and forward other messages.
        //
        // Reason for persisting on second barrier rather than first:
        // We can't update meta with progress as finished until state_table
        // has been updated.
        // We also can't update state_table in first epoch, since state_table
        // expects to have been initialized in previous epoch.

        // The epoch used to snapshot read upstream mv.
        let mut snapshot_read_epoch = init_epoch;

        // Keep track of rows from the snapshot.
        let mut total_snapshot_processed_rows: u64 = 0;

        // Backfill Algorithm:
        //
        //   backfill_stream
        //  /               \
        // upstream       snapshot
        //
        // We construct a backfill stream with upstream as its left input and mv snapshot read
        // stream as its right input. When a chunk comes from upstream, we will buffer it.
        //
        // When a barrier comes from upstream:
        //  - Update the `snapshot_read_epoch`.
        //  - For each row of the upstream chunk buffer, forward it to downstream if its pk <=
        //    `current_pos`, otherwise ignore it.
        //  - reconstruct the whole backfill stream with upstream and new mv snapshot read stream
        //    with the `snapshot_read_epoch`.
        //
        // When a chunk comes from snapshot, we forward it to the downstream and raise
        // `current_pos`.
        //
        // When we reach the end of the snapshot read stream, it means backfill has been
        // finished.
        //
        // Once the backfill loop ends, we forward the upstream directly to the downstream.
        if to_backfill {
            'backfill_loop: loop {
                let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];

                let left_upstream = upstream.by_ref().map(Either::Left);

                let right_snapshot = pin!(Self::snapshot_read(
                    schema.clone(),
                    &self.upstream_table,
                    snapshot_read_epoch,
                    current_pos.clone(),
                    true
                )
                .map(Either::Right),);

                // Prefer to select upstream, so we can stop snapshot stream as soon as the barrier
                // comes.
                let backfill_stream =
                    select_with_strategy(left_upstream, right_snapshot, |_: &mut ()| {
                        stream::PollNext::Left
                    });

                let mut cur_barrier_snapshot_processed_rows: u64 = 0;
                let mut cur_barrier_upstream_processed_rows: u64 = 0;

                #[for_await]
                for either in backfill_stream {
                    match either {
                        // Upstream
                        Either::Left(msg) => {
                            match msg? {
                                Message::Barrier(barrier) => {
                                    // If it is a barrier, switch snapshot and consume
                                    // upstream buffer chunk

                                    // Consume upstream buffer chunk
                                    if let Some(current_pos) = &current_pos {
                                        for chunk in upstream_chunk_buffer.drain(..) {
                                            cur_barrier_upstream_processed_rows +=
                                                chunk.cardinality() as u64;
                                            yield Message::Chunk(Self::mapping_chunk(
                                                Self::mark_chunk(
                                                    chunk,
                                                    current_pos,
                                                    pk_in_output_indices,
                                                    pk_order,
                                                ),
                                                &self.output_indices,
                                            ));
                                        }
                                    }

                                    self.metrics
                                        .backfill_snapshot_read_row_count
                                        .with_label_values(&[
                                            upstream_table_id.to_string().as_str(),
                                            self.actor_id.to_string().as_str(),
                                        ])
                                        .inc_by(cur_barrier_snapshot_processed_rows);

                                    self.metrics
                                        .backfill_upstream_output_row_count
                                        .with_label_values(&[
                                            upstream_table_id.to_string().as_str(),
                                            self.actor_id.to_string().as_str(),
                                        ])
                                        .inc_by(cur_barrier_upstream_processed_rows);

                                    // Update snapshot read epoch.
                                    snapshot_read_epoch = barrier.epoch.prev;

                                    self.progress.update(
                                        barrier.epoch.curr,
                                        snapshot_read_epoch,
                                        total_snapshot_processed_rows,
                                    );

                                    // Persist state on barrier
                                    Self::persist_state(
                                        barrier.epoch,
                                        &mut self.state_table,
                                        false,
                                        &current_pos,
                                        &mut old_state,
                                        &mut current_state,
                                    )
                                    .await?;

                                    yield Message::Barrier(barrier);
                                    // Break the for loop and start a new snapshot read stream.
                                    break;
                                }
                                Message::Chunk(chunk) => {
                                    // Buffer the upstream chunk.
                                    upstream_chunk_buffer.push(chunk.compact());
                                }
                                Message::Watermark(_) => {
                                    // Ignore watermark during backfill.
                                }
                            }
                        }
                        // Snapshot read
                        Either::Right(msg) => {
                            match msg? {
                                None => {
                                    // End of the snapshot read stream.
                                    // We should not mark the chunk anymore,
                                    // otherwise, we will ignore some rows
                                    // in the buffer. Here we choose to never mark the chunk.
                                    // Consume with the renaming stream buffer chunk without mark.
                                    for chunk in upstream_chunk_buffer.drain(..) {
                                        let chunk_cardinality = chunk.cardinality() as u64;
                                        cur_barrier_snapshot_processed_rows += chunk_cardinality;
                                        total_snapshot_processed_rows += chunk_cardinality;
                                        yield Message::Chunk(Self::mapping_chunk(
                                            chunk,
                                            &self.output_indices,
                                        ));
                                    }

                                    break 'backfill_loop;
                                }
                                Some(chunk) => {
                                    // Raise the current position.
                                    // As snapshot read streams are ordered by pk, so we can
                                    // just use the last row to update `current_pos`.
                                    current_pos = Self::update_pos(&chunk, pk_in_output_indices);

                                    let chunk_cardinality = chunk.cardinality() as u64;
                                    cur_barrier_snapshot_processed_rows += chunk_cardinality;
                                    total_snapshot_processed_rows += chunk_cardinality;
                                    yield Message::Chunk(Self::mapping_chunk(
                                        chunk,
                                        &self.output_indices,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        tracing::trace!(
            actor = self.actor_id,
            "Backfill has already finished and forward messages directly to the downstream"
        );

        // Wait for first barrier to come after backfill is finished.
        // So we can update our progress + persist the status.
        while let Some(Ok(msg)) = upstream.next().await {
            if let Some(msg) = Self::mapping_message(msg, &self.output_indices) {
                // If not finished then we need to update state, otherwise no need.
                if let Message::Barrier(barrier) = &msg && !is_finished {
                    // If snapshot was empty, we do not need to backfill,
                    // but we still need to persist the finished state.
                    // We currently persist it on the second barrier here rather than first.
                    // This is because we can't update state table in first epoch,
                    // since it expects to have been initialized in previous epoch
                    // (there's no epoch before the first epoch).
                    if is_snapshot_empty {
                        current_pos =
                            Self::construct_initial_finished_state(pk_in_output_indices.len())
                    }

                    // We will update current_pos at least once,
                    // since snapshot read has to be non-empty,
                    // Or snapshot was empty and we construct a placeholder state.
                    debug_assert_ne!(current_pos, None);

                    Self::persist_state(
                        barrier.epoch,
                        &mut self.state_table,
                        true,
                        &current_pos,
                        &mut old_state,
                        &mut current_state,
                    )
                    .await?;
                    self.progress.finish(barrier.epoch.curr);
                    yield msg;
                    break;
                }
                yield msg;
            }
        }

        // After progress finished + state persisted,
        // we can forward messages directly to the downstream,
        // as backfill is finished.
        #[for_await]
        for msg in upstream {
            if let Some(msg) = Self::mapping_message(msg?, &self.output_indices) {
                if let Some(state_table) = self.state_table.as_mut() && let Message::Barrier(barrier) = &msg {
                        state_table.commit_no_data_expected(barrier.epoch);
                    }
                yield msg;
            }
        }
    }

    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    async fn snapshot_read(
        schema: Arc<Schema>,
        upstream_table: &StateTable<S>,
        _epoch: u64,
        current_pos: Option<OwnedRow>,
        _ordered: bool,
    ) {
        // `current_pos` is None means it needs to scan from the beginning, so we use Unbounded to
        // scan. Otherwise, use Excluded.
        let range_bounds: (Bound<OwnedRow>, Bound<OwnedRow>) =
            if let Some(current_pos) = current_pos {
                // If `current_pos` is an empty row which means upstream mv contains only one row
                // and it has been consumed. The iter interface doesn't support
                // `Excluded(empty_row)` range bound, so we can simply return `None`.
                if current_pos.is_empty() {
                    assert!(upstream_table.pk_indices().is_empty());
                    yield None;
                    return Ok(());
                }

                (Bound::Excluded(current_pos), Bound::Unbounded)
            } else {
                (Bound::Unbounded, Bound::Unbounded)
            };
        let iterators = upstream_table
            .iter_all_vnode_ranges(&range_bounds, Default::default())
            .await?;
        let pinned_iter: Vec<_> = iterators.into_iter().map(Box::pin).collect_vec();
        let iter = merge_sort(pinned_iter);
        pin_mut!(iter);
        while let Some(data_chunk) = collect_data_chunk(&mut iter, &schema, Some(CHUNK_SIZE))
            .instrument_await("arrangement_backfill_snapshot_read")
            .await?
        {
            if data_chunk.cardinality() != 0 {
                let ops = vec![Op::Insert; data_chunk.capacity()];
                let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
                yield Some(stream_chunk);
            }
        }

        yield None;
    }

    /// Mark chunk:
    /// For each row of the chunk, forward it to downstream if its pk <= `current_pos`, otherwise
    /// ignore it. We implement it by changing the visibility bitmap.
    fn mark_chunk(
        chunk: StreamChunk,
        current_pos: &OwnedRow,
        pk_in_output_indices: PkIndicesRef<'_>,
        pk_order: &[OrderType],
    ) -> StreamChunk {
        let chunk = chunk.compact();
        let (data, ops) = chunk.into_parts();
        let mut new_visibility = BitmapBuilder::with_capacity(ops.len());
        // Use project to avoid allocation.
        for v in data.rows().map(|row| {
            match row
                .project(pk_in_output_indices)
                .iter()
                .zip_eq_fast(pk_order.iter().copied())
                .cmp_by(current_pos.iter(), |(x, order), y| cmp_datum(x, y, order))
            {
                Ordering::Less | Ordering::Equal => true,
                Ordering::Greater => false,
            }
        }) {
            new_visibility.append(v);
        }
        let (columns, _) = data.into_parts();
        StreamChunk::new(ops, columns, Some(new_visibility.finish()))
    }

    /// Builds a new stream chunk with `output_indices`.
    fn mapping_chunk(chunk: StreamChunk, output_indices: &[usize]) -> StreamChunk {
        let (ops, columns, visibility) = chunk.into_inner();
        let mapped_columns = output_indices.iter().map(|&i| columns[i].clone()).collect();
        StreamChunk::new(ops, mapped_columns, visibility)
    }

    fn mapping_watermark(watermark: Watermark, upstream_indices: &[usize]) -> Option<Watermark> {
        watermark.transform_with_indices(upstream_indices)
    }

    fn mapping_message(msg: Message, upstream_indices: &[usize]) -> Option<Message> {
        match msg {
            Message::Barrier(_) => Some(msg),
            Message::Watermark(watermark) => {
                Self::mapping_watermark(watermark, upstream_indices).map(Message::Watermark)
            }
            Message::Chunk(chunk) => {
                Some(Message::Chunk(Self::mapping_chunk(chunk, upstream_indices)))
            }
        }
    }

    /// Schema
    /// | vnode | pk | `backfill_finished` |
    ///
    /// For `current_pos` and `old_pos` are just pk of upstream.
    /// They should be strictly increasing.
    async fn persist_state(
        epoch: EpochPair,
        table: &mut Option<StateTable<S>>,
        is_finished: bool,
        current_pos: &Option<OwnedRow>,
        old_state: &mut Option<Vec<Datum>>,
        current_state: &mut [Datum],
    ) -> StreamExecutorResult<()> {
        // Backwards compatibility with no state table in backfill.
        let Some(table) = table else {
            return Ok(())
        };
        if let Some(current_pos_inner) = current_pos {
            // state w/o vnodes.
            Self::build_temporary_state(current_state, is_finished, current_pos_inner);
            Self::flush_data(table, epoch, old_state, current_state).await?;
            *old_state = Some(current_state.into());
        } else {
            table.commit_no_data_expected(epoch);
        }
        Ok(())
    }

    /// Flush the data
    async fn flush_data(
        table: &mut StateTable<S>,
        epoch: EpochPair,
        old_state: &mut Option<Vec<Datum>>,
        current_partial_state: &mut [Datum],
    ) -> StreamExecutorResult<()> {
        let vnodes = table.vnodes().clone();
        if let Some(old_state) = old_state {
            if old_state[1..] == current_partial_state[1..] {
                table.commit_no_data_expected(epoch);
                return Ok(());
            } else {
                vnodes.iter_vnodes_scalar().for_each(|vnode| {
                    let datum = Some(vnode.into());
                    current_partial_state[0] = datum.clone();
                    old_state[0] = datum;
                    table.write_record(Record::Update {
                        old_row: &old_state[..],
                        new_row: &(*current_partial_state),
                    })
                });
            }
        } else {
            // No existing state, create a new entry.
            vnodes.iter_vnodes_scalar().for_each(|vnode| {
                let datum = Some(vnode.into());
                // fill the state
                current_partial_state[0] = datum;
                table.write_record(Record::Insert {
                    new_row: &(*current_partial_state),
                })
            });
        }
        table.commit(epoch).await
    }

    // We want to avoid building a row for every vnode.
    // Instead we can just modify a single row, and dispatch it to state table to write.
    fn build_temporary_state(row_state: &mut [Datum], is_finished: bool, current_pos: &OwnedRow) {
        row_state[1..current_pos.len() + 1].clone_from_slice(current_pos.as_inner());
        row_state[current_pos.len() + 1] = Some(is_finished.into());
    }

    fn update_pos(chunk: &StreamChunk, pk_in_output_indices: &[usize]) -> Option<OwnedRow> {
        Some(
            chunk
                .rows()
                .last()
                .unwrap()
                .1
                .project(pk_in_output_indices)
                .into_owned_row(),
        )
    }

    // TODO(kwannoel): I'm not sure if ["None" ..] encoding is appropriate
    // for the case where upstream snapshot is empty, and we want to persist
    // backfill state as "finished".
    // Could it be confused with another case where pk position comprised of nulls?
    // I don't think it will matter,
    // because they both record that backfill is finished.
    // We can revisit in future if necessary.
    fn construct_initial_finished_state(pos_len: usize) -> Option<OwnedRow> {
        Some(OwnedRow::new(vec![None; pos_len]))
    }

    /// All vnodes should be persisted with status finished.
    /// TODO: In the future we will support partial backfill recovery.
    /// When that is done, this logic may need to be rewritten to handle
    /// partially complete states per vnode.
    async fn check_all_vnode_finished(
        state_table: &StateTable<S>,
        state_len: usize,
    ) -> StreamExecutorResult<bool> {
        debug_assert!(!state_table.vnode_bitmap().is_empty());
        let vnodes = state_table.vnodes().iter_vnodes_scalar();
        let mut is_finished = true;
        for vnode in vnodes {
            let key: &[Datum] = &[Some(vnode.into())];
            let row = state_table.get_row(key).await?;

            // original_backfill_datum_pos = (state_len - 1)
            // value indices are set, so we can -1 for the pk (a single vnode).
            let backfill_datum_pos = state_len - 2;
            let vnode_is_finished = if let Some(row) = row
                && let Some(vnode_is_finished) = row.datum_at(backfill_datum_pos)
            {
                vnode_is_finished.into_bool()
            } else {
                false
            };
            if !vnode_is_finished {
                is_finished = false;
                break;
            }
        }
        Ok(is_finished)
    }
}

impl<S> Executor for ArrangementBackfillExecutor<S>
where
    S: StateStore,
{
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}
