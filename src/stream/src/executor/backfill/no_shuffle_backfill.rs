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

use std::pin::pin;
use std::sync::Arc;

use either::Either;
use futures::stream::select_with_strategy;
use futures::{pin_mut, stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::Datum;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::{bail, row};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::StateStore;

use crate::common::table::state_table::StateTable;
use crate::executor::backfill::utils;
use crate::executor::backfill::utils::{
    compute_bounds, construct_initial_finished_state, get_new_pos, iter_chunks, mapping_chunk,
    mapping_message, mark_chunk, owned_row_iter,
};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    expect_first_barrier, Barrier, BoxedExecutor, BoxedMessageStream, Executor, ExecutorInfo,
    Message, PkIndices, PkIndicesRef, StreamExecutorError, StreamExecutorResult,
};
use crate::task::{ActorId, CreateMviewProgress};

/// vnode, `is_finished`, `row_count`, all occupy 1 column each.
const METADATA_STATE_LEN: usize = 3;

/// Schema: | vnode | pk ... | `backfill_finished` | `row_count` |
/// We can decode that into `BackfillState` on recovery.
#[derive(Debug, Eq, PartialEq)]
pub struct BackfillState {
    current_pos: Option<OwnedRow>,
    old_state: Option<Vec<Datum>>,
    is_finished: bool,
    row_count: u64,
}

/// An implementation of the [RFC: Use Backfill To Let Mv On Mv Stream Again](https://github.com/risingwavelabs/rfcs/pull/13).
/// `BackfillExecutor` is used to create a materialized view on another materialized view.
///
/// It can only buffer chunks between two barriers instead of unbundled memory usage of
/// `RearrangedChainExecutor`.
///
/// It uses the latest epoch to read the snapshot of the upstream mv during two barriers and all the
/// `StreamChunk` of the snapshot read will forward to the downstream.
///
/// It uses `current_pos` to record the progress of the backfill (the pk of the upstream mv) and
/// `current_pos` is initiated as an empty `Row`.
///
/// All upstream messages during the two barriers interval will be buffered and decide to forward or
/// ignore based on the `current_pos` at the end of the later barrier. Once `current_pos` reaches
/// the end of the upstream mv pk, the backfill would finish.
///
/// Notice:
/// The pk we are talking about here refers to the storage primary key.
/// We rely on the scheduler to schedule the `BackfillExecutor` together with the upstream mv/table
/// in the same worker, so that we can read uncommitted data from the upstream table without
/// waiting.
pub struct BackfillExecutor<S: StateStore> {
    /// Upstream table
    upstream_table: StorageTable<S>,
    /// Upstream with the same schema with the upstream table.
    upstream: BoxedExecutor,

    /// Internal state table for persisting state of backfill state.
    state_table: Option<StateTable<S>>,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    /// PTAL at the docstring for `CreateMviewProgress` to understand how we compute it.
    progress: CreateMviewProgress,

    actor_id: ActorId,

    info: ExecutorInfo,

    metrics: Arc<StreamingMetrics>,

    chunk_size: usize,
}

impl<S> BackfillExecutor<S>
where
    S: StateStore,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        upstream_table: StorageTable<S>,
        upstream: BoxedExecutor,
        state_table: Option<StateTable<S>>,
        output_indices: Vec<usize>,
        progress: CreateMviewProgress,
        schema: Schema,
        pk_indices: PkIndices,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        executor_id: u64,
    ) -> Self {
        Self {
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: format!("BackfillExecutor {:X}", executor_id),
            },
            upstream_table,
            upstream,
            state_table,
            output_indices,
            actor_id: progress.actor_id(),
            progress,
            metrics,
            chunk_size,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // The primary key columns, in the output columns of the upstream_table scan.
        let pk_in_output_indices = self.upstream_table.pk_in_output_indices().unwrap();

        let state_len = pk_in_output_indices.len() + METADATA_STATE_LEN;

        let pk_order = self.upstream_table.pk_serializer().get_order_types();

        let upstream_table_id = self.upstream_table.table_id().table_id;

        let mut upstream = self.upstream.execute();

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let init_epoch = first_barrier.epoch.prev;
        if let Some(state_table) = self.state_table.as_mut() {
            state_table.init_epoch(first_barrier.epoch);
        }

        let BackfillState {
            mut current_pos,
            is_finished,
            row_count,
            mut old_state,
        } = Self::recover_backfill_state(self.state_table.as_ref(), pk_in_output_indices.len())
            .await?;
        tracing::trace!(is_finished, row_count, "backfill state recovered");

        let mut builder =
            DataChunkBuilder::new(self.upstream_table.schema().data_types(), self.chunk_size);

        // Use this buffer to construct state,
        // which will then be persisted.
        let mut current_state: Vec<Datum> = vec![None; state_len];

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
        let mut total_snapshot_processed_rows: u64 = row_count;

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
        if !is_finished {
            let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];
            let mut pending_barrier: Option<Barrier> = None;
            'backfill_loop: loop {
                let mut cur_barrier_snapshot_processed_rows: u64 = 0;
                let mut cur_barrier_upstream_processed_rows: u64 = 0;
                let mut snapshot_read_complete = false;
                let mut has_snapshot_read = false;

                // We should not buffer rows from previous epoch, else we can have duplicates.
                assert!(upstream_chunk_buffer.is_empty());

                {
                    let left_upstream = upstream.by_ref().map(Either::Left);

                    let right_snapshot = pin!(Self::snapshot_read(
                        &self.upstream_table,
                        snapshot_read_epoch,
                        current_pos.clone(),
                        true,
                        &mut builder
                    )
                    .map(Either::Right),);

                    // Prefer to select upstream, so we can stop snapshot stream as soon as the
                    // barrier comes.
                    let mut backfill_stream =
                        select_with_strategy(left_upstream, right_snapshot, |_: &mut ()| {
                            stream::PollNext::Left
                        });

                    #[for_await]
                    for either in &mut backfill_stream {
                        match either {
                            // Upstream
                            Either::Left(msg) => {
                                match msg? {
                                    Message::Barrier(barrier) => {
                                        // We have to process barrier outside of the loop.
                                        // This is because the backfill stream holds a mutable
                                        // reference to our chunk builder.
                                        // We want to create another mutable reference
                                        // to flush remaining chunks from the chunk builder
                                        // on barrier.
                                        // Hence we break here and process it after this block.
                                        pending_barrier = Some(barrier);
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
                                has_snapshot_read = true;
                                match msg? {
                                    None => {
                                        // End of the snapshot read stream.
                                        // We should not mark the chunk anymore,
                                        // otherwise, we will ignore some rows
                                        // in the buffer. Here we choose to never mark the chunk.
                                        // Consume with the renaming stream buffer chunk without
                                        // mark.
                                        for chunk in upstream_chunk_buffer.drain(..) {
                                            let chunk_cardinality = chunk.cardinality() as u64;
                                            cur_barrier_upstream_processed_rows +=
                                                chunk_cardinality;
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
                                        current_pos =
                                            Some(get_new_pos(&chunk, &pk_in_output_indices));

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

                    // Before processing barrier, if did not snapshot read,
                    // do a snapshot read first.
                    // This is so we don't lose the tombstone iteration progress.
                    if !has_snapshot_read {
                        let (_, snapshot) = backfill_stream.into_inner();
                        #[for_await]
                        for msg in snapshot {
                            let Either::Right(msg) = msg else {
                                bail!("BUG: snapshot_read contains upstream messages");
                            };
                            match msg? {
                                None => {
                                    // End of the snapshot read stream.
                                    // We let the barrier handling logic take care of upstream updates.
                                    // But we still want to exit backfill loop, so we mark snapshot read complete.
                                    snapshot_read_complete = true;
                                    break;
                                }
                                Some(chunk) => {
                                    // Raise the current position.
                                    // As snapshot read streams are ordered by pk, so we can
                                    // just use the last row to update `current_pos`.
                                    current_pos = Some(get_new_pos(&chunk, &pk_in_output_indices));

                                    let chunk_cardinality = chunk.cardinality() as u64;
                                    cur_barrier_snapshot_processed_rows += chunk_cardinality;
                                    total_snapshot_processed_rows += chunk_cardinality;
                                    yield Message::Chunk(mapping_chunk(
                                        chunk,
                                        &self.output_indices,
                                    ));
                                    break;
                                }
                            }
                        }
                    }
                }
                // When we break out of inner backfill_stream loop, it means we have a barrier.
                // If there are no updates and there are no snapshots left,
                // we already finished backfill and should have exited the outer backfill loop.
                let barrier = match pending_barrier.take() {
                    Some(barrier) => barrier,
                    None => bail!("BUG: current_backfill loop exited without a barrier"),
                };

                // Process barrier:
                // - consume snapshot rows left in builder
                // - consume upstream buffer chunk
                // - switch snapshot

                // Consume snapshot rows left in builder
                let chunk = builder.consume_all();
                if let Some(chunk) = chunk {
                    let chunk_cardinality = chunk.cardinality() as u64;
                    let ops = vec![Op::Insert; chunk.capacity()];
                    let chunk = StreamChunk::from_parts(ops, chunk);
                    current_pos = Some(get_new_pos(&chunk, &pk_in_output_indices));

                    cur_barrier_snapshot_processed_rows += chunk_cardinality;
                    total_snapshot_processed_rows += chunk_cardinality;
                    yield Message::Chunk(mapping_chunk(chunk, &self.output_indices));
                }

                // Consume upstream buffer chunk
                // If no current_pos, means we did not process any snapshot
                // yet. In that case
                // we can just ignore the upstream buffer chunk, but still need to clean it.
                if let Some(current_pos) = &current_pos {
                    for chunk in upstream_chunk_buffer.drain(..) {
                        cur_barrier_upstream_processed_rows += chunk.cardinality() as u64;
                        yield Message::Chunk(mapping_chunk(
                            mark_chunk(chunk, current_pos, &pk_in_output_indices, pk_order),
                            &self.output_indices,
                        ));
                    }
                } else {
                    upstream_chunk_buffer.clear()
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
                    total_snapshot_processed_rows,
                    &mut old_state,
                    &mut current_state,
                )
                .await?;

                tracing::trace!(
                    epoch = ?barrier.epoch,
                    ?current_pos,
                    total_snapshot_processed_rows,
                    "Backfill state persisted"
                );

                yield Message::Barrier(barrier);

                if snapshot_read_complete {
                    break 'backfill_loop;
                }

                // We will switch snapshot at the start of the next iteration of the backfill loop.
            }
        }

        tracing::trace!("Backfill has finished, waiting for barrier");

        // Wait for first barrier to come after backfill is finished.
        // So we can update our progress + persist the status.
        while let Some(Ok(msg)) = upstream.next().await {
            if let Some(msg) = mapping_message(msg, &self.output_indices) {
                // If not finished then we need to update state, otherwise no need.
                if let Message::Barrier(barrier) = &msg {
                    if is_finished {
                        // If already finished, no need persist any state.
                    } else {
                        // If snapshot was empty, we do not need to backfill,
                        // but we still need to persist the finished state.
                        // We currently persist it on the second barrier here rather than first.
                        // This is because we can't update state table in first epoch,
                        // since it expects to have been initialized in previous epoch
                        // (there's no epoch before the first epoch).
                        if current_pos.is_none() {
                            current_pos =
                                Some(construct_initial_finished_state(pk_in_output_indices.len()))
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
                            total_snapshot_processed_rows,
                            &mut old_state,
                            &mut current_state,
                        )
                        .await?;
                        tracing::trace!(
                            epoch = ?barrier.epoch,
                            ?current_pos,
                            total_snapshot_processed_rows,
                            "Backfill position persisted after completion"
                        );
                    }

                    // For both backfill finished before recovery,
                    // and backfill which just finished, we need to update mview tracker,
                    // it does not persist this information.
                    self.progress
                        .finish(barrier.epoch.curr, total_snapshot_processed_rows);
                    tracing::trace!(
                        epoch = ?barrier.epoch,
                        "Updated CreateMaterializedTracker"
                    );
                    yield msg;
                    break;
                }
                yield msg;
            }
        }

        tracing::trace!(
            "Backfill has already finished and forward messages directly to the downstream"
        );

        // After progress finished + state persisted,
        // we can forward messages directly to the downstream,
        // as backfill is finished.
        // We don't need to report backfill progress any longer, as it has finished.
        // It will always be at 100%.
        #[for_await]
        for msg in upstream {
            if let Some(msg) = mapping_message(msg?, &self.output_indices) {
                yield msg;
            }
        }
    }

    async fn recover_backfill_state(
        state_table: Option<&StateTable<S>>,
        pk_len: usize,
    ) -> StreamExecutorResult<BackfillState> {
        let Some(state_table) = state_table else {
            // If no state table, but backfill is present, it must be from an old cluster.
            // In that case backfill must be finished, otherwise it won't have been persisted.
            return Ok(BackfillState {
                current_pos: None,
                is_finished: true,
                row_count: 0,
                old_state: None,
            });
        };
        let mut vnodes = state_table.vnodes().iter_vnodes_scalar();
        let first_vnode = vnodes.next().unwrap();
        let key: &[Datum] = &[Some(first_vnode.into())];
        let row = state_table.get_row(key).await?;
        let expected_state = Self::deserialize_backfill_state(row, pk_len);

        // All vnode partitions should have same state (no scale-in supported).
        for vnode in vnodes {
            let key: &[Datum] = &[Some(vnode.into())];
            let row = state_table.get_row(key).await?;
            let state = Self::deserialize_backfill_state(row, pk_len);
            assert_eq!(state.is_finished, expected_state.is_finished);
        }
        Ok(expected_state)
    }

    fn deserialize_backfill_state(row: Option<OwnedRow>, pk_len: usize) -> BackfillState {
        let Some(row) = row else {
            return BackfillState {
                current_pos: None,
                is_finished: false,
                row_count: 0,
                old_state: None,
            };
        };
        let row = row.into_inner();
        let mut old_state = vec![None; pk_len + METADATA_STATE_LEN];
        old_state[1..row.len() + 1].clone_from_slice(&row);
        let current_pos = Some((&row[0..pk_len]).into_owned_row());
        let is_finished = row[pk_len].clone().map_or(false, |d| d.into_bool());
        let row_count = row
            .get(pk_len + 1)
            .cloned()
            .unwrap_or(None)
            .map_or(0, |d| d.into_int64() as u64);
        BackfillState {
            current_pos,
            is_finished,
            row_count,
            old_state: Some(old_state),
        }
    }

    /// Snapshot read the upstream mv.
    /// The rows from upstream snapshot read will be buffered inside the `builder`.
    /// If snapshot is dropped before its rows are consumed,
    /// remaining data in `builder` must be flushed manually.
    /// Otherwise when we scan a new snapshot, it is possible the rows in the `builder` would be
    /// present, Then when we flush we contain duplicate rows.
    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    async fn snapshot_read<'a>(
        upstream_table: &'a StorageTable<S>,
        epoch: u64,
        current_pos: Option<OwnedRow>,
        ordered: bool,
        builder: &'a mut DataChunkBuilder,
    ) {
        let range_bounds = compute_bounds(upstream_table.pk_indices(), current_pos);
        let range_bounds = match range_bounds {
            None => {
                yield None;
                return Ok(());
            }
            Some(range_bounds) => range_bounds,
        };

        // We use uncommitted read here, because we have already scheduled the `BackfillExecutor`
        // together with the upstream mv.
        let iter = upstream_table
            .batch_iter_with_pk_bounds(
                HummockReadEpoch::NoWait(epoch),
                row::empty(),
                range_bounds,
                ordered,
                PrefetchOptions::new_for_exhaust_iter(),
            )
            .await?;

        let row_iter = owned_row_iter(iter);
        pin_mut!(row_iter);

        #[for_await]
        for chunk in iter_chunks(row_iter, builder) {
            yield chunk?;
        }
    }

    async fn persist_state(
        epoch: EpochPair,
        table: &mut Option<StateTable<S>>,
        is_finished: bool,
        current_pos: &Option<OwnedRow>,
        row_count: u64,
        old_state: &mut Option<Vec<Datum>>,
        current_state: &mut [Datum],
    ) -> StreamExecutorResult<()> {
        // Backwards compatibility with no state table in backfill.
        let Some(table) = table else { return Ok(()) };
        utils::persist_state(
            epoch,
            table,
            is_finished,
            current_pos,
            row_count,
            old_state,
            current_state,
        )
        .await
    }
}

impl<S> Executor for BackfillExecutor<S>
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
