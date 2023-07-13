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

use std::collections::HashMap;
use std::pin::pin;
use std::sync::Arc;

use either::Either;
use futures::stream::select_with_strategy;
use futures::{pin_mut, stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::bail;
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::types::Datum;
use risingwave_common::util::select_all;
use risingwave_storage::StateStore;

use crate::common::table::state_table::ReplicatedStateTable;
use crate::executor::backfill::utils::{
    compute_bounds, construct_initial_finished_state, get_progress_per_vnode, iter_chunks,
    mapping_chunk, mapping_message, mark_chunk_ref_by_vnode, persist_state_per_vnode,
    update_pos_by_vnode, BackfillProgressPerVnode, BackfillState,
};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    expect_first_barrier, Barrier, BoxedExecutor, BoxedMessageStream, Executor, ExecutorInfo,
    Message, PkIndices, PkIndicesRef, StreamExecutorError,
};
use crate::task::{ActorId, CreateMviewProgress};

/// Similar to [`BackfillExecutor`].
/// Main differences:
/// - [`ArrangementBackfillExecutor`] can reside on a different CN, so it can be scaled
///   independently.
/// - To synchronize upstream shared buffer, it is initialized with a [`ReplicatedStateTable`].
pub struct ArrangementBackfillExecutor<S: StateStore> {
    /// Upstream table
    upstream_table: ReplicatedStateTable<S>,

    /// Upstream with the same schema with the upstream table.
    upstream: BoxedExecutor,

    /// Internal state table for persisting state of backfill state.
    state_table: ReplicatedStateTable<S>,

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
        upstream_table: ReplicatedStateTable<S>,
        upstream: BoxedExecutor,
        state_table: ReplicatedStateTable<S>,
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
        // Table scan scans a subset of the columns of the upstream table.
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

        let progress_per_vnode = get_progress_per_vnode(&self.state_table, state_len).await?;

        let is_completely_finished = progress_per_vnode
            .iter()
            .all(|(_, p)| *p == BackfillProgressPerVnode::Completed);
        if is_completely_finished {
            assert!(!first_barrier.is_newly_added(self.actor_id));
        }

        let mut backfill_state: BackfillState = progress_per_vnode.into();
        let mut committed_progress = HashMap::new();

        // If the snapshot is empty, we don't need to backfill.
        // We cannot complete progress now, as we want to persist
        // finished state to state store first.
        // As such we will wait for next barrier.
        let is_snapshot_empty: bool = {
            if is_completely_finished {
                // It is finished, so just assign a value to avoid accessing storage table again.
                false
            } else {
                let snapshot = Self::snapshot_read_per_vnode(
                    schema.clone(),
                    &upstream_table,
                    backfill_state.clone(), // FIXME: temporary workaround... How to avoid it?
                );
                pin_mut!(snapshot);
                snapshot.try_next().await?.unwrap().is_none()
            }
        };

        // | backfill_is_finished | snapshot_empty | -> | need_to_backfill |
        // | -------------------- | -------------- | -- | ---------------- |
        // | t                    | t/f            | -> | f                |
        // | f                    | t              | -> | f                |
        // | f                    | f              | -> | t                |
        let to_backfill = !is_completely_finished && !is_snapshot_empty;

        // Use these to persist state.
        // They contain the backfill position, and the progress.
        // However, they do not contain the vnode key (index 0).
        // That is filled in when we flush the state table.
        let mut temporary_state: Vec<Datum> = vec![None; state_len];

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

        // Arrangement Backfill Algorithm:
        //
        //   backfill_stream
        //  /               \
        // upstream       snapshot
        //
        // We construct a backfill stream with upstream as its left input and mv snapshot read
        // stream as its right input. When a chunk comes from upstream, we will buffer it.
        //
        // When a barrier comes from upstream:
        //  Immediately break out of backfill loop.
        //  - For each row of the upstream chunk buffer, compute vnode.
        //  - Get the `current_pos` corresponding to the vnode. Forward it to downstream if its pk
        //    <= `current_pos`, otherwise ignore it.
        //  - Flush all buffered upstream_chunks to replicated state table.
        //  - Update the `snapshot_read_epoch`.
        //  - Reconstruct the whole backfill stream with upstream and new mv snapshot read stream
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

                // NOTE(kwannoel): Scope it so that immutable reference to `upstream_table` can be
                // dropped. Then we can write to `upstream_table` on barrier in the
                // next block.
                {
                    let left_upstream = upstream.by_ref().map(Either::Left);

                    let right_snapshot = pin!(Self::snapshot_read_per_vnode(
                        schema.clone(),
                        &upstream_table,
                        backfill_state.clone(), // FIXME: temporary workaround, how to avoid it?
                    )
                    .map(Either::Right),);

                    // Prefer to select upstream, so we can stop snapshot stream as soon as the
                    // barrier comes.
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
                                        // Consume with the renaming stream buffer chunk without
                                        // mark.
                                        for chunk in upstream_chunk_buffer.drain(..) {
                                            let chunk_cardinality = chunk.cardinality() as u64;
                                            cur_barrier_snapshot_processed_rows +=
                                                chunk_cardinality;
                                            total_snapshot_processed_rows += chunk_cardinality;
                                            yield Message::Chunk(mapping_chunk(
                                                chunk,
                                                &self.output_indices,
                                            ));
                                        }

                                        break 'backfill_loop;
                                    }
                                    Some((vnode, chunk)) => {
                                        // Raise the current position.
                                        // As snapshot read streams are ordered by pk, so we can
                                        // just use the last row to update `current_pos`.
                                        update_pos_by_vnode(
                                            vnode,
                                            &chunk,
                                            &pk_in_output_indices,
                                            &mut backfill_state,
                                        );

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

                // Process barrier
                // When we break out of inner backfill_stream loop, it means we have a barrier.
                // If there are no updates and there are no snapshots left,
                // we already finished backfill and should have exited the outer backfill loop.
                let barrier = match pending_barrier.take() {
                    Some(barrier) => barrier,
                    None => bail!("BUG: current_backfill loop exited without a barrier"),
                };
                let upstream_chunk_buffer_is_empty = upstream_chunk_buffer.is_empty();
                for chunk in upstream_chunk_buffer.drain(..) {
                    cur_barrier_upstream_processed_rows += chunk.cardinality() as u64;
                    // FIXME: Replace with `snapshot_is_processed`
                    // Flush downstream.
                    // If no current_pos, means no snapshot processed yet.
                    // Also means we don't need propagate any updates <= current_pos.
                    if backfill_state.has_progress() {
                        yield Message::Chunk(mapping_chunk(
                            mark_chunk_ref_by_vnode(
                                &chunk,
                                &backfill_state,
                                &pk_in_output_indices,
                                &pk_order,
                            )?,
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
                    .arrangement_backfill_snapshot_read_row_count
                    .with_label_values(&[
                        upstream_table_id.to_string().as_str(),
                        self.actor_id.to_string().as_str(),
                    ])
                    .inc_by(cur_barrier_snapshot_processed_rows);

                self.metrics
                    .arrangement_backfill_upstream_output_row_count
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
                persist_state_per_vnode(
                    barrier.epoch,
                    &mut self.state_table,
                    false,
                    &mut backfill_state,
                    &mut committed_progress,
                    &mut temporary_state,
                )
                .await?;

                yield Message::Barrier(barrier);
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
                if let Message::Barrier(barrier) = &msg && !is_completely_finished {
                    // If snapshot was empty, we do not need to backfill,
                    // but we still need to persist the finished state.
                    // We currently persist it on the second barrier here rather than first.
                    // This is because we can't update state table in first epoch,
                    // since it expects to have been initialized in previous epoch
                    // (there's no epoch before the first epoch).
                    if is_snapshot_empty {
                        let finished_state = construct_initial_finished_state(pk_in_output_indices.len());
                        for vnode in upstream_table.vnodes().iter_vnodes() {
                            backfill_state.update_progress(vnode, BackfillProgressPerVnode::InProgress(finished_state.clone()));
                        }
                    }

                    persist_state_per_vnode(
                        barrier.epoch,
                        &mut self.state_table,
                        false,
                        &mut backfill_state,
                        &mut committed_progress,
                        &mut temporary_state,
                    ).await?;

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

    /// Read snapshot per vnode.
    /// These streams should be sorted in storage layer.
    /// 1. Get row iterator / vnode.
    /// 2. Merge it with `select_all`.
    /// 3. Change it into a chunk iterator with `iter_chunks`.
    /// This means it should fetch a row from each iterator to form a chunk.
    ///
    /// NOTE(kwannoel): We interleave at chunk per vnode level rather than rows.
    /// This is so that we can compute `current_pos` once per chunk, since they correspond to 1
    /// vnode.
    ///
    /// TODO(kwannoel): Support partially complete snapshot reads.
    /// That will require the following changes:
    /// Instead of returning stream chunk and vnode, we need to dispatch 3 diff messages:
    /// 1. COMPLETE_VNODE(vnode): Current iterator is complete, in that case we need to handle it
    ///    in arrangement backfill. We should not buffer updates for this vnode, and forward
    ///    all messages.
    /// 2. MESSAGE(CHUNK): Current iterator is not complete, in that case we
    ///    need to buffer updates for this vnode.
    /// 3. FINISHED: All iterators finished.
    ///
    /// For now we only support the case where all iterators are complete.
    #[try_stream(ok = Option<(VirtualNode, StreamChunk)>, error = StreamExecutorError)]
    async fn snapshot_read_per_vnode(
        schema: Arc<Schema>,
        upstream_table: &ReplicatedStateTable<S>,
        backfill_state: BackfillState,
    ) {
        let mut streams = Vec::with_capacity(upstream_table.vnodes().len());
        for vnode in upstream_table.vnodes().iter_vnodes() {
            let backfill_progress = backfill_state.get_progress(&vnode)?;
            let current_pos = match backfill_progress {
                BackfillProgressPerVnode::Completed => {
                    continue;
                }
                BackfillProgressPerVnode::NotStarted => None,
                BackfillProgressPerVnode::InProgress(current_pos) => Some(current_pos.clone()),
            };
            let range_bounds = compute_bounds(upstream_table.pk_indices(), current_pos.clone());
            if range_bounds.is_none() {
                continue;
            }
            let range_bounds = range_bounds.unwrap();
            let vnode_row_iter = upstream_table
                .iter_with_pk_range(&range_bounds, vnode, Default::default())
                .await?;
            // TODO: Is there some way to avoid double-pin here?
            let vnode_row_iter = Box::pin(vnode_row_iter);
            let vnode_chunk_iter = iter_chunks(vnode_row_iter, &schema, CHUNK_SIZE)
                .map_ok(move |chunk_opt| chunk_opt.map(|chunk| (vnode, chunk)));
            // TODO: Is there some way to avoid double-pin
            streams.push(Box::pin(vnode_chunk_iter));
        }
        #[for_await]
        for chunk in select_all(streams) {
            yield chunk?;
        }
        yield None;
        return Ok(());
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
