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

use std::ops::Bound;
use std::pin::pin;
use std::sync::Arc;

use await_tree::InstrumentAwait;
use either::Either;
use futures::stream::select_with_strategy;
use futures::{pin_mut, stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::Datum;
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::table::collect_data_chunk;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::backfill::utils::{
    build_temporary_state, check_all_vnode_finished, construct_initial_finished_state, flush_data,
    mapping_chunk, mapping_message, mark_chunk_ref, update_pos,
};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    expect_first_barrier, Barrier, BoxedExecutor, BoxedMessageStream, Executor, ExecutorInfo,
    Message, PkIndices, PkIndicesRef, StreamExecutorError, StreamExecutorResult,
};
use crate::task::{ActorId, CreateMviewProgress};

pub struct ArrangementBackfillExecutor<S: StateStore> {
    /// Upstream table
    upstream_table: StateTable<S>,

    /// Upstream with the same schema with the upstream table.
    upstream: BoxedExecutor,

    /// Internal state table for persisting state of backfill state.
    state_table: StateTable<S>,

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
        state_table: StateTable<S>,
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
        let pk_in_output_indices = self
            .upstream_table
            .pk_indices()
            .iter()
            .map(|&i| self.output_indices.iter().position(|&j| i == j))
            .collect::<Option<Vec<_>>>()
            .unwrap();
        let state_len = pk_in_output_indices.len() + 2; // +1 for backfill_finished, +1 for vnode key.
        let pk_order = self.upstream_table.pk_serde().get_order_types().to_vec();
        let upstream_table_id = self.upstream_table.table_id();
        let mut upstream_table = self.upstream_table;

        let schema = Arc::new(self.upstream.schema().clone());

        let mut upstream = self.upstream.execute();

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        self.state_table.init_epoch(first_barrier.epoch);

        let is_finished = check_all_vnode_finished(&self.state_table, state_len).await?;
        if is_finished {
            assert!(!first_barrier.is_newly_added(self.actor_id));
        }

        // If the snapshot is empty, we don't need to backfill.
        // We cannot complete progress now, as we want to persist
        // finished state to state store first.
        // As such we will wait for next barrier.
        let is_snapshot_empty: bool = {
            if is_finished {
                // It is finished, so just assign a value to avoid accessing storage table again.
                false
            } else {
                let snapshot = Self::snapshot_read(schema.clone(), &upstream_table, None);
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
        let mut snapshot_read_epoch;

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
            let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];
            let mut pending_barrier: Option<Barrier> = None;
            'backfill_loop: loop {
                let mut cur_barrier_snapshot_processed_rows: u64 = 0;
                let mut cur_barrier_upstream_processed_rows: u64 = 0;

                // Process barrier
                // Each time we break out of the backfill_stream loop,
                // it is typically due to barrier (except the first time).
                // So we should immediately process barrier if there's one pending.
                if let Some(barrier) = pending_barrier.clone() {
                    let upstream_chunk_buffer_is_empty = upstream_chunk_buffer.is_empty();
                    for chunk in upstream_chunk_buffer.drain(..) {
                        cur_barrier_upstream_processed_rows += chunk.cardinality() as u64;
                        // Flush downstream.
                        // If no current_pos, means no snapshot processed yet.
                        // Also means we don't need propagate any updates <= current_pos.
                        if let Some(current_pos) = &current_pos {
                            yield Message::Chunk(mapping_chunk(
                                mark_chunk_ref(
                                    &chunk,
                                    current_pos,
                                    &pk_in_output_indices,
                                    &pk_order,
                                ),
                                &self.output_indices,
                            ));
                        }
                        // Replicate
                        upstream_table.write_chunk(chunk);
                    }
                    if upstream_chunk_buffer_is_empty {
                        upstream_table.commit_no_data_expected(barrier.epoch)
                    } else {
                        upstream_table.commit(barrier.epoch).await?;
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
                }

                let left_upstream = upstream.by_ref().map(Either::Left);

                let right_snapshot =
                    pin!(
                        Self::snapshot_read(schema.clone(), &upstream_table, current_pos.clone(),)
                            .map(Either::Right),
                    );

                // Prefer to select upstream, so we can stop snapshot stream as soon as the barrier
                // comes.
                let backfill_stream =
                    select_with_strategy(left_upstream, right_snapshot, |_: &mut ()| {
                        stream::PollNext::Left
                    });

                #[for_await]
                for either in backfill_stream {
                    match either {
                        // Upstream
                        Either::Left(msg) => {
                            match msg? {
                                Message::Barrier(barrier) => {
                                    // We have to process the barrier outside of the loop.
                                    // This is because our state_table reference is still live
                                    // here, we have to break the loop to drop it,
                                    // so we can do replication of upstream state_table.
                                    pending_barrier = Some(barrier);

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
                                        yield Message::Chunk(mapping_chunk(
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
                                    current_pos = update_pos(&chunk, &pk_in_output_indices);

                                    let chunk_cardinality = chunk.cardinality() as u64;
                                    cur_barrier_snapshot_processed_rows += chunk_cardinality;
                                    total_snapshot_processed_rows += chunk_cardinality;
                                    yield Message::Chunk(mapping_chunk(
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
            if let Some(msg) = mapping_message(msg, &self.output_indices) {
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
                            construct_initial_finished_state(pk_in_output_indices.len())
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
            if let Some(msg) = mapping_message(msg?, &self.output_indices) {
                if let Message::Barrier(barrier) = &msg {
                    self.state_table.commit_no_data_expected(barrier.epoch);
                }
                yield msg;
            }
        }
    }

    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    async fn snapshot_read(
        schema: Arc<Schema>,
        upstream_table: &StateTable<S>,
        current_pos: Option<OwnedRow>,
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
        let iter = upstream_table
            .iter_all_with_pk_range(&range_bounds, Default::default())
            .await?;
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

    /// Schema
    /// | vnode | pk | `backfill_finished` |
    ///
    /// For `current_pos` and `old_pos` are just pk of upstream.
    /// They should be strictly increasing.
    async fn persist_state(
        epoch: EpochPair,
        table: &mut StateTable<S>,
        is_finished: bool,
        current_pos: &Option<OwnedRow>,
        old_state: &mut Option<Vec<Datum>>,
        current_state: &mut [Datum],
    ) -> StreamExecutorResult<()> {
        if let Some(current_pos_inner) = current_pos {
            // state w/o vnodes.
            build_temporary_state(current_state, is_finished, current_pos_inner);
            flush_data(table, epoch, old_state, current_state).await?;
            *old_state = Some(current_state.into());
        } else {
            table.commit_no_data_expected(epoch);
        }
        Ok(())
    }
}

impl<S> Executor for ArrangementBackfillExecutor<S>
where
    S: StateStore,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}
