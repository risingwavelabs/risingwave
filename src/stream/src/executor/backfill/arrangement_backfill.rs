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

use either::Either;
use futures::stream::{select_all, select_with_strategy};
use futures::{TryStreamExt, stream};
use itertools::Itertools;
use risingwave_common::array::{DataChunk, Op};
use risingwave_common::bail;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common_rate_limit::{MonitoredRateLimiter, RateLimit, RateLimiter};
use risingwave_pb::common::ThrottleType;
use risingwave_storage::row_serde::value_serde::ValueRowSerde;
use risingwave_storage::store::PrefetchOptions;

use crate::common::table::state_table::ReplicatedStateTable;
#[cfg(debug_assertions)]
use crate::executor::backfill::utils::METADATA_STATE_LEN;
use crate::executor::backfill::utils::{
    BackfillProgressPerVnode, BackfillState, compute_bounds, create_builder,
    get_progress_per_vnode, mapping_chunk, mapping_message, mark_chunk_ref_by_vnode,
    persist_state_per_vnode, update_pos_by_vnode,
};
use crate::executor::prelude::*;
use crate::task::{CreateMviewProgressReporter, FragmentId};

type Builders = HashMap<VirtualNode, DataChunkBuilder>;

/// Similar to [`super::no_shuffle_backfill::BackfillExecutor`].
/// Main differences:
/// - [`ArrangementBackfillExecutor`] can reside on a different CN, so it can be scaled
///   independently.
/// - To synchronize upstream shared buffer, it is initialized with a [`ReplicatedStateTable`].
pub struct ArrangementBackfillExecutor<S: StateStore, SD: ValueRowSerde> {
    /// Upstream table
    upstream_table: ReplicatedStateTable<S, SD>,

    /// Upstream with the same schema with the upstream table.
    upstream: Executor,

    /// Internal state table for persisting state of backfill state.
    state_table: StateTable<S>,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    progress: CreateMviewProgressReporter,

    actor_id: ActorId,

    metrics: Arc<StreamingMetrics>,

    chunk_size: usize,

    rate_limiter: MonitoredRateLimiter,

    /// Fragment id of the fragment this backfill node belongs to.
    fragment_id: FragmentId,
}

impl<S, SD> ArrangementBackfillExecutor<S, SD>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)]
    pub fn new(
        upstream_table: ReplicatedStateTable<S, SD>,
        upstream: Executor,
        state_table: StateTable<S>,
        output_indices: Vec<usize>,
        progress: CreateMviewProgressReporter,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        rate_limit: RateLimit,
        fragment_id: FragmentId,
    ) -> Self {
        let rate_limiter = RateLimiter::new(rate_limit).monitored(upstream_table.table_id());
        Self {
            upstream_table,
            upstream,
            state_table,
            output_indices,
            actor_id: progress.actor_id(),
            progress,
            metrics,
            chunk_size,
            rate_limiter,
            fragment_id,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        tracing::debug!("backfill executor started");
        // The primary key columns, in the output columns of the upstream_table scan.
        // Table scan scans a subset of the columns of the upstream table.
        let pk_in_output_indices = self.upstream_table.pk_in_output_indices().unwrap();
        #[cfg(debug_assertions)]
        let state_len = self.upstream_table.pk_indices().len() + METADATA_STATE_LEN;
        let pk_order = self.upstream_table.pk_serde().get_order_types().to_vec();
        let upstream_table_id = self.upstream_table.table_id();
        let mut upstream_table = self.upstream_table;
        let vnodes = upstream_table.vnodes().clone();

        // These builders will build data chunks.
        // We must supply them with the full datatypes which correspond to
        // pk + output_indices.
        let snapshot_data_types = self
            .upstream
            .schema()
            .fields()
            .iter()
            .map(|field| field.data_type.clone())
            .collect_vec();
        let mut builders: Builders = upstream_table
            .vnodes()
            .iter_vnodes()
            .map(|vnode| {
                let builder = create_builder(
                    self.rate_limiter.rate_limit(),
                    self.chunk_size,
                    snapshot_data_types.clone(),
                );
                (vnode, builder)
            })
            .collect();

        let mut upstream = self.upstream.execute();

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let mut global_pause = first_barrier.is_pause_on_startup();
        let mut backfill_paused = first_barrier.is_backfill_pause_on_startup(self.fragment_id);
        let first_epoch = first_barrier.epoch;
        let is_newly_added = first_barrier.is_newly_added(self.actor_id);
        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);

        self.state_table.init_epoch(first_epoch).await?;

        let progress_per_vnode = get_progress_per_vnode(&self.state_table).await?;

        let is_completely_finished = progress_per_vnode.iter().all(|(_, p)| {
            matches!(
                p.current_state(),
                &BackfillProgressPerVnode::Completed { .. }
            )
        });
        if is_completely_finished {
            assert!(!is_newly_added);
        }

        upstream_table.init_epoch(first_epoch).await?;

        let mut backfill_state: BackfillState = progress_per_vnode.into();

        let to_backfill = !is_completely_finished;

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
        let mut total_snapshot_processed_rows: u64 = backfill_state.get_snapshot_row_count();

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

            let metrics = self
                .metrics
                .new_backfill_metrics(upstream_table_id, self.actor_id);

            'backfill_loop: loop {
                let mut cur_barrier_snapshot_processed_rows: u64 = 0;
                let mut cur_barrier_upstream_processed_rows: u64 = 0;
                let mut snapshot_read_complete = false;
                let mut has_snapshot_read = false;

                // NOTE(kwannoel): Scope it so that immutable reference to `upstream_table` can be
                // dropped. Then we can write to `upstream_table` on barrier in the
                // next block.
                {
                    let left_upstream = upstream.by_ref().map(Either::Left);

                    // Check if stream paused
                    let paused = global_pause
                        || backfill_paused
                        || matches!(self.rate_limiter.rate_limit(), RateLimit::Pause);
                    // Create the snapshot stream
                    let right_snapshot = pin!(
                        Self::make_snapshot_stream(
                            &upstream_table,
                            backfill_state.clone(), // FIXME: Use mutable reference instead.
                            paused,
                            &self.rate_limiter,
                        )
                        .map(Either::Right)
                    );

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
                                        upstream_chunk_buffer.push(chunk.compact_vis());
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
                                        // Consume remaining rows in the builder.
                                        for (vnode, builder) in &mut builders {
                                            if let Some(data_chunk) = builder.consume_all() {
                                                let chunk = Self::handle_snapshot_chunk(
                                                    data_chunk,
                                                    *vnode,
                                                    &pk_in_output_indices,
                                                    &mut backfill_state,
                                                    &mut cur_barrier_snapshot_processed_rows,
                                                    &mut total_snapshot_processed_rows,
                                                    &self.output_indices,
                                                )?;
                                                tracing::trace!(
                                                    source = "snapshot",
                                                    state = "finish_backfill_stream",
                                                    action = "drain_snapshot_buffers",
                                                    ?vnode,
                                                    "{:#?}",
                                                    chunk,
                                                );
                                                yield Message::Chunk(chunk);
                                            }
                                        }

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
                                            let chunk = mapping_chunk(chunk, &self.output_indices);
                                            tracing::trace!(
                                                source = "upstream",
                                                state = "finish_backfill_stream",
                                                action = "drain_upstream_buffer",
                                                "{:#?}",
                                                chunk,
                                            );
                                            yield Message::Chunk(chunk);
                                        }
                                        metrics
                                            .backfill_snapshot_read_row_count
                                            .inc_by(cur_barrier_snapshot_processed_rows);
                                        metrics
                                            .backfill_upstream_output_row_count
                                            .inc_by(cur_barrier_upstream_processed_rows);
                                        break 'backfill_loop;
                                    }
                                    Some((vnode, row)) => {
                                        let builder = builders.get_mut(&vnode).unwrap();
                                        if let Some(chunk) = builder.append_one_row(row) {
                                            let chunk = Self::handle_snapshot_chunk(
                                                chunk,
                                                vnode,
                                                &pk_in_output_indices,
                                                &mut backfill_state,
                                                &mut cur_barrier_snapshot_processed_rows,
                                                &mut total_snapshot_processed_rows,
                                                &self.output_indices,
                                            )?;
                                            tracing::trace!(
                                                source = "snapshot",
                                                state = "process_backfill_stream",
                                                action = "drain_full_snapshot_buffer",
                                                "{:#?}",
                                                chunk,
                                            );
                                            yield Message::Chunk(chunk);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Before processing barrier, if did not snapshot read,
                    // do a snapshot read first.
                    // This is so we don't lose the tombstone iteration progress.
                    // Or if s3 read latency is high, we don't fail to read from s3.
                    //
                    // If paused, we can't read any snapshot records, skip this.
                    //
                    // If rate limit is set, respect the rate limit, check if we can read,
                    // If we can't, skip it. If no rate limit set, we can read.
                    let rate_limit_ready = self.rate_limiter.check(1).is_ok();
                    if !has_snapshot_read && !paused && rate_limit_ready {
                        debug_assert!(builders.values().all(|b| b.is_empty()));
                        let (_, snapshot) = backfill_stream.into_inner();
                        #[for_await]
                        for msg in snapshot {
                            let Either::Right(msg) = msg else {
                                bail!("BUG: snapshot_read contains upstream messages");
                            };
                            match msg? {
                                None => {
                                    // End of the snapshot read stream.
                                    // We let the barrier handling logic take care of upstream updates:
                                    // it must forward all buffered upstream chunks without marking.
                                    // But we still want to exit backfill loop, so we mark snapshot read complete.
                                    snapshot_read_complete = true;
                                    break;
                                }
                                Some((vnode, row)) => {
                                    let builder = builders.get_mut(&vnode).unwrap();
                                    if let Some(chunk) = builder.append_one_row(row) {
                                        let chunk = Self::handle_snapshot_chunk(
                                            chunk,
                                            vnode,
                                            &pk_in_output_indices,
                                            &mut backfill_state,
                                            &mut cur_barrier_snapshot_processed_rows,
                                            &mut total_snapshot_processed_rows,
                                            &self.output_indices,
                                        )?;
                                        tracing::trace!(
                                            source = "snapshot",
                                            state = "process_backfill_stream",
                                            action = "snapshot_read_at_least_one",
                                            "{:#?}",
                                            chunk,
                                        );
                                        yield Message::Chunk(chunk);
                                    }

                                    break;
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

                // Process barrier:
                // - consume snapshot rows left in builder.
                // - consume upstream buffer chunk
                // - handle mutations
                // - switch snapshot

                // consume snapshot rows left in builder.
                // NOTE(kwannoel): `zip_eq_debug` does not work here,
                // we encounter "higher-ranked lifetime error".
                for (vnode, chunk) in builders.iter_mut().map(|(vnode, b)| {
                    let chunk = b.consume_all().map(|chunk| {
                        let ops = vec![Op::Insert; chunk.capacity()];
                        StreamChunk::from_parts(ops, chunk)
                    });
                    (vnode, chunk)
                }) {
                    if let Some(chunk) = chunk {
                        let chunk_cardinality = chunk.cardinality() as u64;
                        // Raise the current position.
                        // As snapshot read streams are ordered by pk, so we can
                        // just use the last row to update `current_pos`.
                        update_pos_by_vnode(
                            *vnode,
                            &chunk,
                            &pk_in_output_indices,
                            &mut backfill_state,
                            chunk_cardinality,
                        )?;

                        cur_barrier_snapshot_processed_rows += chunk_cardinality;
                        total_snapshot_processed_rows += chunk_cardinality;
                        let chunk = mapping_chunk(chunk, &self.output_indices);
                        tracing::trace!(
                            source = "snapshot",
                            state = "process_barrier",
                            action = "consume_remaining_snapshot",
                            "{:#?}",
                            chunk,
                        );
                        yield Message::Chunk(chunk);
                    }
                }

                // consume upstream buffer chunk
                for chunk in upstream_chunk_buffer.drain(..) {
                    cur_barrier_upstream_processed_rows += chunk.cardinality() as u64;
                    if snapshot_read_complete {
                        // The snapshot read above reached the end of the snapshot, so every
                        // snapshot row has already been emitted and the buffered upstream
                        // chunks are entirely newer than the snapshot. We exit the backfill
                        // loop after this barrier and will never read the replicated table
                        // again, so rows filtered out by `current_pos` here would be lost.
                        // Forward the buffered chunks without marking, the same as when the
                        // snapshot stream ends inside the backfill stream above.
                        let chunk = mapping_chunk(chunk.clone(), &self.output_indices);
                        tracing::trace!(
                            source = "upstream",
                            state = "process_barrier",
                            action = "drain_upstream_buffer_after_snapshot_read_complete",
                            "{:#?}",
                            chunk,
                        );
                        yield Message::Chunk(chunk);
                    } else if backfill_state.has_progress() {
                        let chunk = mapping_chunk(
                            mark_chunk_ref_by_vnode(
                                &chunk,
                                &backfill_state,
                                &pk_in_output_indices,
                                &upstream_table,
                                &pk_order,
                            )?,
                            &self.output_indices,
                        );
                        tracing::trace!(
                            source = "upstream",
                            state = "process_barrier",
                            action = "consume_remaining_upstream",
                            "{:#?}",
                            chunk,
                        );
                        yield Message::Chunk(chunk);
                    }

                    // Replicate
                    upstream_table.write_chunk(chunk);
                }

                upstream_table
                    .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                    .await?;

                metrics
                    .backfill_snapshot_read_row_count
                    .inc_by(cur_barrier_snapshot_processed_rows);
                metrics
                    .backfill_upstream_output_row_count
                    .inc_by(cur_barrier_upstream_processed_rows);

                // Update snapshot read epoch.
                snapshot_read_epoch = barrier.epoch.prev;

                // TODO(kwannoel): Not sure if this holds for arrangement backfill.
                // May need to revisit it.
                // Need to check it after scale-in / scale-out.
                self.progress.update(
                    barrier.epoch,
                    snapshot_read_epoch,
                    total_snapshot_processed_rows,
                );

                // If the snapshot read is complete, the buffered upstream chunks have been forwarded
                // without marking above, including rows beyond `current_pos` or in vnodes that
                // have not started. Persist all vnodes as finished on this barrier, so that the
                // persisted progress is consistent with the forwarded rows. Otherwise, recovering
                // from this barrier would scan these rows from the snapshot and emit them again.
                if snapshot_read_complete {
                    for vnode in upstream_table.vnodes().iter_vnodes() {
                        backfill_state.finish_progress(vnode, upstream_table.pk_indices().len());
                    }
                }

                // Persist state on barrier
                persist_state_per_vnode(
                    barrier.epoch,
                    &mut self.state_table,
                    &mut backfill_state,
                    #[cfg(debug_assertions)]
                    state_len,
                    vnodes.iter_vnodes(),
                )
                .await?;

                tracing::trace!(
                    barrier = ?barrier,
                    "barrier persisted"
                );

                // handle mutations
                if let Some(mutation) = barrier.mutation.as_deref() {
                    use crate::executor::Mutation;
                    match mutation {
                        Mutation::Pause => {
                            global_pause = true;
                        }
                        Mutation::Resume => {
                            global_pause = false;
                        }
                        Mutation::StartFragmentBackfill { fragment_ids } if backfill_paused => {
                            if fragment_ids.contains(&self.fragment_id) {
                                backfill_paused = false;
                            }
                        }
                        Mutation::Throttle(fragment_to_apply) => {
                            if let Some(entry) = fragment_to_apply.get(&self.fragment_id)
                                && entry.throttle_type() == ThrottleType::Backfill
                            {
                                let new_rate_limit = entry.rate_limit.into();
                                let old_rate_limit = self.rate_limiter.update(new_rate_limit);
                                if old_rate_limit != new_rate_limit {
                                    tracing::info!(
                                        old_rate_limit = ?old_rate_limit,
                                        new_rate_limit = ?new_rate_limit,
                                        %upstream_table_id,
                                        actor_id = %self.actor_id,
                                        "backfill rate limit changed",
                                    );
                                    builders = upstream_table
                                        .vnodes()
                                        .iter_vnodes()
                                        .map(|vnode| {
                                            let builder = create_builder(
                                                new_rate_limit,
                                                self.chunk_size,
                                                snapshot_data_types.clone(),
                                            );
                                            (vnode, builder)
                                        })
                                        .collect();
                                }
                            }
                        }
                        _ => {}
                    }
                }

                yield Message::Barrier(barrier);

                // We will switch snapshot at the start of the next iteration of the backfill loop.
                // Unless snapshot read is already completed.
                if snapshot_read_complete {
                    break 'backfill_loop;
                }
            }
        }

        tracing::debug!("snapshot read finished, wait to commit state on next barrier");

        // Update our progress as finished in state table.

        // Wait for first barrier to come after backfill is finished.
        // So we can update our progress + persist the status.
        while let Some(Ok(msg)) = upstream.next().await {
            if let Some(msg) = mapping_message(msg, &self.output_indices) {
                // If not finished then we need to update state, otherwise no need.
                if let Message::Barrier(barrier) = &msg {
                    if is_completely_finished {
                        // If already finished, no need to persist any state. But we need to advance the epoch anyway
                        self.state_table
                            .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                            .await?;
                    } else {
                        // If snapshot was empty, we do not need to backfill,
                        // but we still need to persist the finished state.
                        // We currently persist it on the second barrier here rather than first.
                        // This is because we can't update state table in first epoch,
                        // since it expects to have been initialized in previous epoch
                        // (there's no epoch before the first epoch).
                        for vnode in upstream_table.vnodes().iter_vnodes() {
                            backfill_state
                                .finish_progress(vnode, upstream_table.pk_indices().len());
                        }

                        persist_state_per_vnode(
                            barrier.epoch,
                            &mut self.state_table,
                            &mut backfill_state,
                            #[cfg(debug_assertions)]
                            state_len,
                            vnodes.iter_vnodes(),
                        )
                        .await?;
                    }

                    self.progress
                        .finish(barrier.epoch, total_snapshot_processed_rows);
                    yield msg;
                    break;
                }
                // Allow other messages to pass through.
                // We won't yield twice here, since if there's a barrier,
                // we will always break out of the loop.
                yield msg;
            }
        }

        tracing::debug!("backfill finished");

        // After progress finished + state persisted,
        // we can forward messages directly to the downstream,
        // as backfill is finished.
        #[for_await]
        for msg in upstream {
            if let Some(msg) = mapping_message(msg?, &self.output_indices) {
                if let Message::Barrier(barrier) = &msg {
                    // If already finished, no need persist any state, but we need to advance the epoch of the state table anyway.
                    self.state_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
                }
                yield msg;
            }
        }
    }

    #[try_stream(ok = Option<(VirtualNode, OwnedRow)>, error = StreamExecutorError)]
    async fn make_snapshot_stream<'a>(
        upstream_table: &'a ReplicatedStateTable<S, SD>,
        backfill_state: BackfillState,
        paused: bool,
        rate_limiter: &'a MonitoredRateLimiter,
    ) {
        if paused {
            #[for_await]
            for _ in tokio_stream::pending() {
                bail!("BUG: paused stream should not yield");
            }
        } else {
            // Checked the rate limit is not zero.
            #[for_await]
            for r in Self::snapshot_read_per_vnode(upstream_table, backfill_state) {
                let r = r?;
                rate_limiter.wait(1).await;
                yield r;
            }
        }
    }

    fn handle_snapshot_chunk(
        chunk: DataChunk,
        vnode: VirtualNode,
        pk_in_output_indices: &[usize],
        backfill_state: &mut BackfillState,
        cur_barrier_snapshot_processed_rows: &mut u64,
        total_snapshot_processed_rows: &mut u64,
        output_indices: &[usize],
    ) -> StreamExecutorResult<StreamChunk> {
        let chunk = StreamChunk::from_parts(vec![Op::Insert; chunk.capacity()], chunk);
        // Raise the current position.
        // As snapshot read streams are ordered by pk, so we can
        // just use the last row to update `current_pos`.
        let snapshot_row_count_delta = chunk.cardinality() as u64;
        update_pos_by_vnode(
            vnode,
            &chunk,
            pk_in_output_indices,
            backfill_state,
            snapshot_row_count_delta,
        )?;

        let chunk_cardinality = chunk.cardinality() as u64;
        *cur_barrier_snapshot_processed_rows += chunk_cardinality;
        *total_snapshot_processed_rows += chunk_cardinality;
        Ok(mapping_chunk(chunk, output_indices))
    }

    /// Read snapshot per vnode.
    /// These streams should be sorted in storage layer.
    /// 1. Get row iterator / vnode.
    /// 2. Merge it with `select_all`.
    /// 3. Change it into a chunk iterator with `iter_chunks`.
    /// This means it should fetch a row from each iterator to form a chunk.
    ///
    /// We interleave at chunk per vnode level rather than rows.
    /// This is so that we can compute `current_pos` once per chunk, since they correspond to 1
    /// vnode.
    ///
    /// The stream contains pairs of `(VirtualNode, StreamChunk)`.
    /// The `VirtualNode` is the vnode that the chunk belongs to.
    /// The `StreamChunk` is the chunk that contains the rows from the vnode.
    /// If it's `None`, it means the vnode has no more rows for this snapshot read.
    ///
    /// The `snapshot_read_epoch` is supplied as a parameter for `state_table`.
    /// It is required to ensure we read a fully-checkpointed snapshot the **first time**.
    ///
    /// The rows from upstream snapshot read will be buffered inside the `builder`.
    /// If snapshot is dropped before its rows are consumed,
    /// remaining data in `builder` must be flushed manually.
    /// Otherwise when we scan a new snapshot, it is possible the rows in the `builder` would be
    /// present, Then when we flush we contain duplicate rows.
    #[try_stream(ok = Option<(VirtualNode, OwnedRow)>, error = StreamExecutorError)]
    async fn snapshot_read_per_vnode(
        upstream_table: &ReplicatedStateTable<S, SD>,
        backfill_state: BackfillState,
    ) {
        let mut iterators = vec![];
        for vnode in upstream_table.vnodes().iter_vnodes() {
            let backfill_progress = backfill_state.get_progress(&vnode)?;
            let current_pos = match backfill_progress {
                BackfillProgressPerVnode::NotStarted => None,
                BackfillProgressPerVnode::Completed { .. } => {
                    continue;
                }
                BackfillProgressPerVnode::InProgress { current_pos, .. } => {
                    Some(current_pos.clone())
                }
            };

            let range_bounds = compute_bounds(upstream_table.pk_indices(), current_pos.clone());
            if range_bounds.is_none() {
                continue;
            }
            let range_bounds = range_bounds.unwrap();

            tracing::trace!(
                vnode = ?vnode,
                current_pos = ?current_pos,
                range_bounds = ?range_bounds,
                "iter_with_vnode"
            );
            let vnode_row_iter = upstream_table
                .iter_with_vnode(
                    vnode,
                    &range_bounds,
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;

            let vnode_row_iter = vnode_row_iter.map_ok(move |row| (vnode, row));

            let vnode_row_iter = Box::pin(vnode_row_iter);

            iterators.push(vnode_row_iter);
        }

        // TODO(kwannoel): We can provide an option between snapshot read in parallel vs serial.
        let vnode_row_iter = select_all(iterators);

        #[for_await]
        for vnode_and_row in vnode_row_iter {
            yield Some(vnode_and_row?);
        }
        yield None;
        return Ok(());
    }
}

impl<S, SD> Execute for ArrangementBackfillExecutor<S, SD>
where
    S: StateStore,
    SD: ValueRowSerde,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::row::{OwnedRow, Row};
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::{EpochPair, test_epoch};
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_common::util::value_encoding::BasicSerde;
    use risingwave_pb::catalog::PbTable;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::{gen_pbtable, gen_pbtable_with_value_indices};
    use crate::executor::test_utils::MockSource;
    use crate::task::barrier_test_utils::LocalBarrierTestEnv;

    const UPSTREAM_TABLE_ID: TableId = TableId::new(1);
    const BACKFILL_STATE_TABLE_ID: TableId = TableId::new(2);

    /// Upstream mv: `(k bigint primary key, v bigint)`.
    fn upstream_table_catalog() -> PbTable {
        gen_pbtable(
            UPSTREAM_TABLE_ID,
            vec![
                ColumnDesc::unnamed(ColumnId::from(0), DataType::Int64),
                ColumnDesc::unnamed(ColumnId::from(1), DataType::Int64),
            ],
            vec![OrderType::ascending()],
            vec![0],
            1,
        )
    }

    /// Backfill state table: `| vnode | k | backfill_finished | row_count |`, keyed by vnode.
    fn backfill_state_table_catalog() -> PbTable {
        gen_pbtable_with_value_indices(
            BACKFILL_STATE_TABLE_ID,
            vec![
                ColumnDesc::unnamed(ColumnId::from(0), VirtualNode::RW_TYPE),
                ColumnDesc::unnamed(ColumnId::from(1), DataType::Int64),
                ColumnDesc::unnamed(ColumnId::from(2), DataType::Boolean),
                ColumnDesc::unnamed(ColumnId::from(3), DataType::Int64),
            ],
            vec![OrderType::ascending()],
            vec![0],
            1,
            vec![1, 2, 3],
        )
    }

    fn row(k: i64, v: i64) -> OwnedRow {
        OwnedRow::new(vec![Some(k.into()), Some(v.into())])
    }

    fn upstream(messages: Vec<Message>) -> Executor {
        MockSource::with_messages(messages)
            .stop_on_finish(false)
            .into_executor(
                Schema::new(vec![
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                ]),
                vec![0],
            )
    }

    /// Commit `rows` to the upstream mv in the epoch `epoch`, as the upstream materialize
    /// executor does.
    async fn commit_upstream_rows(store: &MemoryStateStore, epoch: u64, rows: Vec<OwnedRow>) {
        let mut upstream_mv = StateTable::from_table_catalog_inconsistent_op(
            &upstream_table_catalog(),
            store.clone(),
            None,
        )
        .await;
        let mut epoch = EpochPair::new_test_epoch(epoch);
        upstream_mv.init_epoch(epoch).await.unwrap();
        for row in rows {
            upstream_mv.insert(row);
        }
        epoch.inc_for_test();
        upstream_mv.commit_for_test(epoch).await.unwrap();
    }

    /// Prepare the committed upstream mv `{(1, 10)}` and the backfill progress
    /// `InProgress { current_pos: 1, snapshot_row_count: 1 }` for the singleton vnode in epoch
    /// `test_epoch(1)`, as if row 1 had been backfilled before a recovery.
    async fn prepare_in_progress_backfill(store: &MemoryStateStore) {
        commit_upstream_rows(store, test_epoch(1), vec![row(1, 10)]).await;

        let mut state_table =
            StateTable::from_table_catalog(&backfill_state_table_catalog(), store.clone(), None)
                .await;
        let mut epoch = EpochPair::new_test_epoch(test_epoch(1));
        state_table.init_epoch(epoch).await.unwrap();
        state_table.insert(OwnedRow::new(vec![
            Some(VirtualNode::ZERO.to_scalar().into()),
            Some(1_i64.into()),
            Some(false.into()),
            Some(1_i64.into()),
        ]));
        epoch.inc_for_test();
        state_table.commit_for_test(epoch).await.unwrap();
    }

    async fn build_executor(
        store: MemoryStateStore,
        upstream: Executor,
        test_env: &LocalBarrierTestEnv,
    ) -> BoxedMessageStream {
        let upstream_table = ReplicatedStateTable::<_, BasicSerde>::new_replicated(
            &upstream_table_catalog(),
            store.clone(),
            None,
            vec![ColumnId::from(0), ColumnId::from(1)],
        )
        .await;
        let state_table =
            StateTable::from_table_catalog(&backfill_state_table_catalog(), store, None).await;
        let progress =
            CreateMviewProgressReporter::for_test(test_env.local_barrier_manager.clone());
        ArrangementBackfillExecutor::new(
            upstream_table,
            upstream,
            state_table,
            vec![0, 1],
            progress,
            Arc::new(StreamingMetrics::unused()),
            1024,
            RateLimit::Disabled,
            0.into(),
        )
        .boxed()
        .execute()
    }

    /// Pull messages until `num_barriers` barriers are received. Returns the visible rows
    /// forwarded to downstream and the `curr` epochs of the received barriers.
    async fn collect_until_barriers(
        stream: &mut BoxedMessageStream,
        num_barriers: usize,
    ) -> (Vec<OwnedRow>, Vec<u64>) {
        let mut rows = vec![];
        let mut barrier_epochs = vec![];
        while barrier_epochs.len() < num_barriers {
            match stream.next().await.unwrap().unwrap() {
                Message::Barrier(barrier) => barrier_epochs.push(barrier.epoch.curr),
                Message::Chunk(chunk) => {
                    for (op, row) in chunk.rows() {
                        assert_eq!(op, Op::Insert);
                        rows.push(row.into_owned_row());
                    }
                }
                Message::Watermark(_) => {}
            }
        }
        (rows, barrier_epochs)
    }

    /// The upstream mv is still empty when backfill starts, and the upstream keeps producing
    /// data for the whole first epoch, so the snapshot is never polled by the backfill stream.
    /// The snapshot read before processing the barrier then hits the end of the (empty)
    /// snapshot. The buffered upstream rows must still be forwarded, since backfill finishes
    /// right after this barrier.
    #[tokio::test]
    async fn test_snapshot_read_complete_on_barrier_forwards_upstream_when_not_started() {
        let store = MemoryStateStore::new();
        let test_env = LocalBarrierTestEnv::for_test().await;

        let upstream = upstream(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(StreamChunk::from_pretty(
                " I I
                + 1 10
                + 2 20",
            )),
            Message::Chunk(StreamChunk::from_pretty(
                " I I
                + 3 30",
            )),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(StreamChunk::from_pretty(
                " I I
                + 4 40",
            )),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ]);

        let mut backfill = build_executor(store, upstream, &test_env).await;
        let (rows, barrier_epochs) = collect_until_barriers(&mut backfill, 3).await;
        assert_eq!(
            barrier_epochs,
            vec![test_epoch(1), test_epoch(2), test_epoch(3)]
        );
        assert_eq!(rows, vec![row(1, 10), row(2, 20), row(3, 30), row(4, 40)]);
    }

    /// Same as above, but the vnode has already made progress. Buffered upstream rows beyond
    /// `current_pos` must still be forwarded when the snapshot read before processing the
    /// barrier finds no more rows.
    #[tokio::test]
    async fn test_snapshot_read_complete_on_barrier_forwards_upstream_when_in_progress() {
        let store = MemoryStateStore::new();
        let test_env = LocalBarrierTestEnv::for_test().await;
        prepare_in_progress_backfill(&store).await;

        let upstream = upstream(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(StreamChunk::from_pretty(
                " I I
                + 2 20
                + 3 30",
            )),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
            Message::Chunk(StreamChunk::from_pretty(
                " I I
                + 4 40",
            )),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
        ]);

        let mut backfill = build_executor(store, upstream, &test_env).await;
        let (rows, barrier_epochs) = collect_until_barriers(&mut backfill, 3).await;
        assert_eq!(
            barrier_epochs,
            vec![test_epoch(2), test_epoch(3), test_epoch(4)]
        );
        assert_eq!(rows, vec![row(2, 20), row(3, 30), row(4, 40)]);
    }

    /// Recover right after the barrier on which the snapshot read completes and the buffered
    /// upstream rows are forwarded, before the next barrier. The upstream mv has committed the
    /// forwarded rows in that checkpoint, so backfill must not emit them again from the
    /// snapshot after recovery.
    #[tokio::test]
    async fn test_recover_after_snapshot_read_complete_on_barrier_when_not_started() {
        let store = MemoryStateStore::new();
        let test_env = LocalBarrierTestEnv::for_test().await;

        let upstream_before_recovery = upstream(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(StreamChunk::from_pretty(
                " I I
                + 1 10
                + 2 20
                + 3 30",
            )),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
        ]);
        let mut backfill = build_executor(store.clone(), upstream_before_recovery, &test_env).await;
        let (rows, barrier_epochs) = collect_until_barriers(&mut backfill, 2).await;
        assert_eq!(barrier_epochs, vec![test_epoch(1), test_epoch(2)]);
        assert_eq!(rows, vec![row(1, 10), row(2, 20), row(3, 30)]);
        drop(backfill);

        // The upstream mv commits the rows forwarded above in the same checkpoint.
        commit_upstream_rows(
            &store,
            test_epoch(1),
            vec![row(1, 10), row(2, 20), row(3, 30)],
        )
        .await;

        // Recover from the checkpoint committed by the barrier of `test_epoch(2)`.
        let upstream_after_recovery = upstream(vec![
            Message::Barrier(Barrier::with_prev_epoch_for_test(
                test_epoch(3),
                test_epoch(1),
            )),
            Message::Chunk(StreamChunk::from_pretty(
                " I I
                + 4 40",
            )),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
        ]);
        let mut backfill = build_executor(store, upstream_after_recovery, &test_env).await;
        let (rows, barrier_epochs) = collect_until_barriers(&mut backfill, 2).await;
        assert_eq!(barrier_epochs, vec![test_epoch(3), test_epoch(4)]);
        assert_eq!(rows, vec![row(4, 40)]);
    }

    /// Same as above, but the vnode has already made progress before the snapshot read
    /// completes.
    #[tokio::test]
    async fn test_recover_after_snapshot_read_complete_on_barrier_when_in_progress() {
        let store = MemoryStateStore::new();
        let test_env = LocalBarrierTestEnv::for_test().await;
        prepare_in_progress_backfill(&store).await;

        let upstream_before_recovery = upstream(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(StreamChunk::from_pretty(
                " I I
                + 2 20
                + 3 30",
            )),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
        ]);
        let mut backfill = build_executor(store.clone(), upstream_before_recovery, &test_env).await;
        let (rows, barrier_epochs) = collect_until_barriers(&mut backfill, 2).await;
        assert_eq!(barrier_epochs, vec![test_epoch(2), test_epoch(3)]);
        assert_eq!(rows, vec![row(2, 20), row(3, 30)]);
        drop(backfill);

        // The upstream mv commits the rows forwarded above in the same checkpoint.
        commit_upstream_rows(&store, test_epoch(2), vec![row(2, 20), row(3, 30)]).await;

        // Recover from the checkpoint committed by the barrier of `test_epoch(3)`.
        let upstream_after_recovery = upstream(vec![
            Message::Barrier(Barrier::with_prev_epoch_for_test(
                test_epoch(4),
                test_epoch(2),
            )),
            Message::Chunk(StreamChunk::from_pretty(
                " I I
                + 4 40",
            )),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(5))),
        ]);
        let mut backfill = build_executor(store, upstream_after_recovery, &test_env).await;
        let (rows, barrier_epochs) = collect_until_barriers(&mut backfill, 2).await;
        assert_eq!(barrier_epochs, vec![test_epoch(4), test_epoch(5)]);
        assert_eq!(rows, vec![row(4, 40)]);
    }
}
