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

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::{Either as FutureEither, pending, select};
use futures::{StreamExt, TryStreamExt, pin_mut};
use futures_async_stream::try_stream;
use risingwave_common::array::{DataChunk, Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::Datum;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_common_rate_limit::{MonitoredRateLimiter, RateLimit, RateLimiter};
use risingwave_pb::common::ThrottleType;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;

use crate::common::table::state_table::{FlushedStateTableReader, StateTable};
#[cfg(debug_assertions)]
use crate::executor::backfill::utils::METADATA_STATE_LEN;
use crate::executor::backfill::utils::{
    BackfillProgressPerVnode, BackfillState, create_builder, get_progress_per_vnode,
    mark_chunk_ref_by_vnode, persist_state_per_vnode, update_pos_by_vnode,
};
use crate::executor::prelude::*;
use crate::task::{CreateMviewProgressReporter, FragmentId};

type Builders = HashMap<VirtualNode, DataChunkBuilder>;

/// The `LocalityProviderExecutor` provides locality for operators during backfilling.
/// It buffers input data into a state table using locality columns as primary key prefix.
///
/// The executor implements a proper backfill process similar to arrangement backfill:
/// 1. Backfill phase: Buffer incoming data and provide locality-ordered snapshot reads
/// 2. Forward phase: Once backfill is complete, forward upstream messages directly
///
/// Key improvements over the original implementation:
/// - Removes arbitrary barrier buffer limit
/// - Implements proper upstream chunk tracking during backfill
/// - Uses per-vnode progress tracking for better state management
pub struct LocalityProviderExecutor<S: StateStore> {
    /// Upstream input
    upstream: Executor,

    /// Locality columns (indices in input schema)
    #[expect(dead_code)]
    locality_columns: Vec<usize>,

    /// State table for buffering input data
    state_table: StateTable<S>,

    /// Progress table for tracking backfill progress per vnode
    progress_table: StateTable<S>,

    input_schema: Schema,

    /// Progress reporter for materialized view creation
    progress: CreateMviewProgressReporter,

    fragment_id: FragmentId,

    actor_id: ActorId,

    /// Metrics
    metrics: Arc<StreamingMetrics>,

    /// Chunk size for output
    chunk_size: usize,

    rate_limiter: MonitoredRateLimiter,
}

impl<S: StateStore> LocalityProviderExecutor<S> {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        upstream: Executor,
        locality_columns: Vec<usize>,
        state_table: StateTable<S>,
        progress_table: StateTable<S>,
        input_schema: Schema,
        progress: CreateMviewProgressReporter,
        metrics: Arc<StreamingMetrics>,
        chunk_size: usize,
        fragment_id: FragmentId,
        rate_limit: RateLimit,
    ) -> Self {
        let rate_limiter = RateLimiter::new(rate_limit).monitored(state_table.table_id());
        Self {
            upstream,
            locality_columns,
            state_table,
            progress_table,
            input_schema,
            actor_id: progress.actor_id(),
            progress,
            metrics,
            chunk_size,
            fragment_id,
            rate_limiter,
        }
    }

    /// Returns the new rate limit if it changed.
    fn apply_throttle(
        rate_limiter: &MonitoredRateLimiter,
        fragment_id: FragmentId,
        barrier: &Barrier,
    ) -> Option<RateLimit> {
        let Some(Mutation::Throttle(fragment_to_apply)) = barrier.mutation.as_deref() else {
            return None;
        };
        let entry = fragment_to_apply.get(&fragment_id)?;
        if entry.throttle_type() != ThrottleType::Backfill {
            return None;
        }
        let new_rate_limit = entry.rate_limit.into();
        let old_rate_limit = rate_limiter.update(new_rate_limit);
        (old_rate_limit != new_rate_limit).then(|| {
            tracing::info!(
                ?old_rate_limit,
                ?new_rate_limit,
                %fragment_id,
                "locality backfill rate limit changed"
            );
            new_rate_limit
        })
    }

    /// Creates a snapshot stream that reads from state table in locality order
    #[try_stream(ok = (VirtualNode, OwnedRow), error = StreamExecutorError)]
    async fn make_snapshot_stream<'a>(
        reader: FlushedStateTableReader<S>,
        backfill_state: BackfillState,
        rate_limiter: &'a MonitoredRateLimiter,
    ) {
        // Read from state table per vnode in locality order
        for vnode in reader.vnodes().iter_vnodes() {
            let current_pos = match backfill_state.get_progress(&vnode)? {
                BackfillProgressPerVnode::NotStarted => None,
                BackfillProgressPerVnode::Completed { .. } => {
                    // Skip completed vnodes
                    continue;
                }
                BackfillProgressPerVnode::InProgress { current_pos, .. } => {
                    Some(current_pos.clone())
                }
            };

            // Compute range bounds for iteration based on current position
            let range_bounds = if let Some(ref pos) = current_pos {
                let start_bound = std::ops::Bound::Excluded(pos.as_inner());
                (start_bound, std::ops::Bound::<&[Datum]>::Unbounded)
            } else {
                (
                    std::ops::Bound::<&[Datum]>::Unbounded,
                    std::ops::Bound::<&[Datum]>::Unbounded,
                )
            };

            // Iterate over rows for this vnode
            let iter = reader
                .iter_with_vnode(
                    vnode,
                    &range_bounds,
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;
            pin_mut!(iter);

            while let Some(row) = iter.try_next().await? {
                rate_limiter.wait(1).await;
                yield (vnode, row);
            }
        }
    }

    fn handle_snapshot_chunk(
        data_chunk: DataChunk,
        vnode: VirtualNode,
        pk_indices: &[usize],
        backfill_state: &mut BackfillState,
        cur_barrier_snapshot_processed_rows: &mut u64,
    ) -> StreamExecutorResult<StreamChunk> {
        let chunk = StreamChunk::from_parts(vec![Op::Insert; data_chunk.cardinality()], data_chunk);
        let chunk_cardinality = chunk.cardinality() as u64;
        // As snapshot read streams are ordered by pk, the last row is the new position.
        update_pos_by_vnode(vnode, &chunk, pk_indices, backfill_state, chunk_cardinality)?;
        *cur_barrier_snapshot_processed_rows += chunk_cardinality;
        Ok(chunk)
    }
}

impl<S: StateStore> Execute for LocalityProviderExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<S: StateStore> LocalityProviderExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut upstream = self.upstream.execute();
        let backfill_operator_id = self.progress.backfill_operator_id();

        // Wait for first barrier to initialize
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let first_epoch = first_barrier.epoch;
        let mut global_pause = first_barrier.is_pause_on_startup();

        // Propagate the first barrier
        yield Message::Barrier(first_barrier);

        let mut state_table = self.state_table;
        let mut progress_table = self.progress_table;
        let rate_limiter = self.rate_limiter;

        // Initialize state tables
        state_table.init_epoch(first_epoch).await?;
        progress_table.init_epoch(first_epoch).await?;

        let progress_per_vnode = get_progress_per_vnode(&progress_table).await?;
        let is_completely_finished = progress_per_vnode.iter().all(|(_, progress)| {
            matches!(
                progress.current_state(),
                BackfillProgressPerVnode::Completed { .. }
            )
        });
        // A provider that has not replayed any vnode waits for `StartFragmentBackfill`.
        let mut backfill_paused = progress_per_vnode.iter().all(|(_, progress)| {
            matches!(
                progress.current_state(),
                BackfillProgressPerVnode::NotStarted
            )
        });
        let mut backfill_state: BackfillState = progress_per_vnode.into();

        // Get pk info from state table
        let pk_indices = state_table.pk_indices().to_vec();
        let pk_order = state_table.pk_serde().get_order_types().to_vec();
        #[cfg(debug_assertions)]
        let state_len = pk_indices.len() + METADATA_STATE_LEN;

        // Locality Provider Backfill Algorithm (adapted from Arrangement Backfill):
        //
        //   backfill_stream
        //  /               \
        // upstream       snapshot (from state_table)
        //
        // We construct a backfill stream with upstream as its left input and locality-ordered
        // snapshot read stream as its right input. When a chunk comes from upstream, we buffer it.
        //
        // When a barrier comes from upstream:
        //  - For each row of the upstream chunk buffer, compute vnode.
        //  - Get the `current_pos` corresponding to the vnode. Forward it to downstream if its
        //    locality key <= `current_pos`, otherwise ignore it.
        //  - Flush all buffered upstream_chunks to state table.
        //  - Persist backfill progress to progress table.
        //  - Reconstruct the snapshot read stream only if buffered upstream chunks changed the
        //    state table. Otherwise, continue the same snapshot read stream across the barrier.
        //
        // When a chunk comes from snapshot, we forward it to the downstream and raise
        // `current_pos`.
        //
        // When we reach the end of the snapshot read stream, it means backfill has been
        // finished.
        //
        // Once the backfill loop ends, we forward the upstream directly to the downstream.

        if !is_completely_finished {
            let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];

            let metrics = self
                .metrics
                .new_backfill_metrics(state_table.table_id(), self.actor_id);

            // Create builders for snapshot data chunks
            let snapshot_data_types = self.input_schema.data_types();
            let vnodes = state_table.vnodes().clone();
            let new_builders = |rate_limit| -> Builders {
                vnodes
                    .iter_vnodes()
                    .map(|vnode| {
                        let builder = create_builder(
                            rate_limit,
                            self.chunk_size,
                            snapshot_data_types.clone(),
                        );
                        (vnode, builder)
                    })
                    .collect()
            };
            let mut builders = new_builders(rate_limiter.rate_limit());

            let snapshot_reader = state_table.flushed_snapshot_reader();
            let snapshot_stream = Self::make_snapshot_stream(
                snapshot_reader.clone(),
                backfill_state.clone(),
                &rate_limiter,
            );
            pin_mut!(snapshot_stream);

            'backfill_loop: loop {
                let mut cur_barrier_snapshot_processed_rows: u64 = 0;
                let mut cur_barrier_upstream_processed_rows: u64 = 0;
                let paused =
                    global_pause || backfill_paused || rate_limiter.rate_limit().is_paused();
                let mut state_table_changed = false;

                // Prefer upstream so a ready barrier can pause snapshot output promptly, while
                // keeping the snapshot stream itself alive across barriers with no upstream data.
                let barrier = loop {
                    let upstream_next = upstream.next();
                    let mut snapshot_stream_ref = snapshot_stream.as_mut();
                    let snapshot_next = async move {
                        if paused {
                            pending().await
                        } else {
                            snapshot_stream_ref.next().await
                        }
                    };
                    pin_mut!(upstream_next);
                    pin_mut!(snapshot_next);

                    match select(upstream_next, snapshot_next).await {
                        FutureEither::Left((msg, _)) => match msg.transpose()? {
                            Some(Message::Barrier(barrier)) => {
                                // Process the barrier after draining the snapshot builders.
                                break barrier;
                            }
                            Some(Message::Chunk(chunk)) => {
                                let chunk = chunk.compact_vis();
                                if paused {
                                    // The positions don't move while paused, so the chunk is
                                    // marked and written at once instead of buffered for the epoch.
                                    cur_barrier_upstream_processed_rows +=
                                        chunk.cardinality() as u64;
                                    if backfill_state.has_progress() {
                                        yield Message::Chunk(mark_chunk_ref_by_vnode(
                                            &chunk,
                                            &backfill_state,
                                            &pk_indices,
                                            &state_table,
                                            &pk_order,
                                        )?);
                                    }
                                    state_table.write_chunk(chunk);
                                    state_table.try_flush().await?;
                                    state_table_changed = true;
                                } else {
                                    upstream_chunk_buffer.push(chunk);
                                }
                            }
                            Some(Message::Watermark(_)) => {
                                // Ignore watermark during backfill.
                            }
                            None => {
                                return Err(anyhow::anyhow!(
                                    "locality provider upstream ended unexpectedly during backfill"
                                )
                                .into());
                            }
                        },
                        FutureEither::Right((msg, _)) => match msg.transpose()? {
                            Some((vnode, row)) => {
                                let builder = builders.get_mut(&vnode).unwrap();
                                if let Some(data_chunk) = builder.append_one_row(row) {
                                    let chunk = Self::handle_snapshot_chunk(
                                        data_chunk,
                                        vnode,
                                        &pk_indices,
                                        &mut backfill_state,
                                        &mut cur_barrier_snapshot_processed_rows,
                                    )?;
                                    yield Message::Chunk(chunk);
                                }
                            }
                            None => {
                                // End of the snapshot read stream.
                                // Consume remaining rows in the builders.
                                for (vnode, builder) in &mut builders {
                                    if let Some(data_chunk) = builder.consume_all() {
                                        let chunk = Self::handle_snapshot_chunk(
                                            data_chunk,
                                            *vnode,
                                            &pk_indices,
                                            &mut backfill_state,
                                            &mut cur_barrier_snapshot_processed_rows,
                                        )?;
                                        yield Message::Chunk(chunk);
                                    }
                                }

                                // Consume remaining rows in the upstream buffer.
                                for chunk in upstream_chunk_buffer.drain(..) {
                                    let chunk_cardinality = chunk.cardinality() as u64;
                                    cur_barrier_upstream_processed_rows += chunk_cardinality;
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
                        },
                    }
                };

                // Consume remaining rows from builders at barrier
                for (vnode, builder) in &mut builders {
                    if let Some(data_chunk) = builder.consume_all() {
                        let chunk = Self::handle_snapshot_chunk(
                            data_chunk,
                            *vnode,
                            &pk_indices,
                            &mut backfill_state,
                            &mut cur_barrier_snapshot_processed_rows,
                        )?;
                        yield Message::Chunk(chunk);
                    }
                }

                if let Some(new_rate_limit) =
                    Self::apply_throttle(&rate_limiter, self.fragment_id, &barrier)
                {
                    builders = new_builders(new_rate_limit);
                }

                // Process upstream buffer chunks with marking
                state_table_changed |= !upstream_chunk_buffer.is_empty();
                for chunk in upstream_chunk_buffer.drain(..) {
                    cur_barrier_upstream_processed_rows += chunk.cardinality() as u64;
                    if backfill_state.has_progress() {
                        yield Message::Chunk(mark_chunk_ref_by_vnode(
                            &chunk,
                            &backfill_state,
                            &pk_indices,
                            &state_table,
                            &pk_order,
                        )?);
                    }
                    // Persist buffered upstream chunk into state table so subsequent snapshot
                    // iterations see the latest writes.
                    state_table.write_chunk(chunk);
                }

                let barrier_epoch = barrier.epoch;
                barrier.assume_no_update_vnode_bitmap(self.actor_id)?;
                state_table
                    .commit_assert_no_update_vnode_bitmap(barrier_epoch)
                    .await?;

                if !backfill_paused {
                    self.progress.update_with_buffered_rows(
                        barrier_epoch,
                        barrier_epoch.curr, // Use barrier epoch as snapshot read epoch
                        backfill_state.get_snapshot_row_count(),
                        0,
                    );
                }

                persist_state_per_vnode(
                    barrier_epoch,
                    &mut progress_table,
                    &mut backfill_state,
                    #[cfg(debug_assertions)]
                    state_len,
                    vnodes.iter_vnodes(),
                )
                .await?;

                metrics
                    .backfill_snapshot_read_row_count
                    .inc_by(cur_barrier_snapshot_processed_rows);
                metrics
                    .backfill_upstream_output_row_count
                    .inc_by(cur_barrier_upstream_processed_rows);

                if let Some(mutation) = barrier.mutation.as_deref() {
                    match mutation {
                        Mutation::Pause => global_pause = true,
                        Mutation::Resume => global_pause = false,
                        _ => {}
                    }
                }
                if backfill_paused && barrier.should_start_backfill(backfill_operator_id) {
                    tracing::info!(
                        fragment_id = %self.fragment_id,
                        %backfill_operator_id,
                        "Start backfill of locality provider",
                    );
                    backfill_paused = false;
                }

                yield Message::Barrier(barrier);

                if state_table_changed {
                    snapshot_stream.set(Self::make_snapshot_stream(
                        snapshot_reader.clone(),
                        backfill_state.clone(),
                        &rate_limiter,
                    ));
                }
            }

            tracing::debug!("Locality provider backfill finished, forwarding upstream directly");

            // Wait for first barrier after backfill completion to mark progress as finished
            while let Some(Ok(msg)) = upstream.next().await {
                match msg {
                    Message::Barrier(barrier) => {
                        barrier.assume_no_update_vnode_bitmap(self.actor_id)?;

                        // no-op commit state table
                        state_table
                            .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                            .await?;

                        for vnode in state_table.vnodes().iter_vnodes() {
                            backfill_state.finish_progress(vnode, pk_indices.len());
                        }

                        // At completion, we report the replayed rows as buffered rows to make
                        // progress accurate.
                        let total_snapshot_processed_rows = backfill_state.get_snapshot_row_count();
                        self.progress.finish_with_buffered_rows(
                            barrier.epoch,
                            total_snapshot_processed_rows,
                            total_snapshot_processed_rows,
                        );

                        persist_state_per_vnode(
                            barrier.epoch,
                            &mut progress_table,
                            &mut backfill_state,
                            #[cfg(debug_assertions)]
                            state_len,
                            state_table.vnodes().iter_vnodes(),
                        )
                        .await?;

                        yield Message::Barrier(barrier);
                        break; // Exit the loop after processing the barrier
                    }
                    Message::Chunk(chunk) => {
                        // Forward chunks directly during completion phase
                        yield Message::Chunk(chunk);
                    }
                    Message::Watermark(watermark) => {
                        // Forward watermarks directly during completion phase
                        yield Message::Watermark(watermark);
                    }
                }
            }
        }

        let mut report_finished_on_first_barrier = is_completely_finished;
        // After backfill completion, forward messages directly
        #[for_await]
        for msg in upstream {
            let msg = msg?;

            match msg {
                Message::Barrier(barrier) => {
                    barrier.assume_no_update_vnode_bitmap(self.actor_id)?;

                    // Commit state tables but don't modify them
                    state_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
                    progress_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
                    if report_finished_on_first_barrier {
                        // At completion, we report the replayed rows as buffered rows to make
                        // progress accurate.
                        let total_snapshot_rows = backfill_state.get_snapshot_row_count();
                        self.progress.finish_with_buffered_rows(
                            barrier.epoch,
                            total_snapshot_rows,
                            total_snapshot_rows,
                        );
                        report_finished_on_first_barrier = false;
                    }
                    yield Message::Barrier(barrier);
                }
                _ => {
                    // Forward all other messages directly
                    yield msg;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use risingwave_common::array::StreamChunkTestExt;
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable_with_dist_key;
    use crate::executor::backfill::utils::BackfillStatePerVnode;

    #[tokio::test]
    async fn test_mark_chunk_splits_unmatched_updates() {
        // Ordered by `(a, b)` and sharded by `b`, where `b` is the stream key and `a` the locality
        // column, so the rows of an update that changes `a` stay in a vnode but may fall on both
        // sides of the backfill position.
        let table = gen_pbtable_with_dist_key(
            TableId::new(1),
            vec![
                ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
                ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
            ],
            vec![OrderType::ascending(), OrderType::ascending()],
            vec![0, 1],
            0,
            vec![1],
        );
        let state_table = StateTable::from_table_catalog(
            &table,
            MemoryStateStore::new(),
            Some(Bitmap::ones(VirtualNode::COUNT_FOR_TEST).into()),
        )
        .await;
        let in_progress_vnodes = (1..=3i64)
            .map(|b| {
                state_table
                    .compute_vnode_by_pk(OwnedRow::new(vec![Some(0i64.into()), Some(b.into())]))
            })
            .collect_vec();
        let backfill_state: BackfillState = state_table
            .vnodes()
            .iter_vnodes()
            .map(|vnode| {
                let progress = if in_progress_vnodes.contains(&vnode) {
                    BackfillProgressPerVnode::InProgress {
                        current_pos: OwnedRow::new(vec![Some(5i64.into()), Some(0i64.into())]),
                        snapshot_row_count: 1,
                    }
                } else {
                    BackfillProgressPerVnode::NotStarted
                };
                (
                    vnode,
                    BackfillStatePerVnode::new(progress.clone(), progress),
                )
            })
            .collect_vec()
            .into();

        let chunk = StreamChunk::from_pretty(
            " I I
            U- 3 1
            U+ 7 1
            U- 7 2
            U+ 3 2
            U- 1 3
            U+ 2 3",
        );
        let marked = mark_chunk_ref_by_vnode(
            &chunk,
            &backfill_state,
            state_table.pk_indices(),
            &state_table,
            state_table.pk_serde().get_order_types(),
        )
        .unwrap();
        assert_eq!(
            marked.compact_vis().to_pretty().to_string(),
            StreamChunk::from_pretty(
                " I I
                - 3 1
                + 3 2
                U- 1 3
                U+ 2 3",
            )
            .to_pretty()
            .to_string()
        );
    }
}
