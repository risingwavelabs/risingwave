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

use either::Either;
use futures::stream::select_with_strategy;
use futures::{TryStreamExt, pin_mut, stream};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::{VirtualNode, VnodeBitmapExt};
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_common::types::{Datum, ToOwnedDatum};
use risingwave_common::util::sort_util::cmp_datum_iter;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;

use crate::common::table::state_table::StateTable;
use crate::executor::prelude::*;
use crate::task::{CreateMviewProgressReporter, FragmentId};

/// Progress state for tracking backfill per vnode
#[derive(Clone, Debug, PartialEq, Eq)]
enum LocalityBackfillProgress {
    /// Backfill not started for this vnode
    NotStarted,
    /// Backfill in progress, tracking current position
    InProgress {
        /// Current position in the locality-ordered scan
        current_pos: OwnedRow,
        /// Number of rows processed for this vnode
        processed_rows: u64,
    },
    /// Backfill completed for this vnode
    Completed {
        /// Final position reached
        final_pos: OwnedRow,
        /// Total rows processed for this vnode
        total_rows: u64,
    },
}

/// State management for locality provider backfill process
#[derive(Clone, Debug)]
struct LocalityBackfillState {
    /// Progress per vnode
    per_vnode: HashMap<VirtualNode, LocalityBackfillProgress>,
    /// Total snapshot rows read across all vnodes
    total_snapshot_rows: u64,
}

impl LocalityBackfillState {
    fn new(vnodes: impl Iterator<Item = VirtualNode>) -> Self {
        let per_vnode = vnodes
            .map(|vnode| (vnode, LocalityBackfillProgress::NotStarted))
            .collect();
        Self {
            per_vnode,
            total_snapshot_rows: 0,
        }
    }

    fn is_completed(&self) -> bool {
        self.per_vnode
            .values()
            .all(|progress| matches!(progress, LocalityBackfillProgress::Completed { .. }))
    }

    fn vnodes(&self) -> impl Iterator<Item = (VirtualNode, &LocalityBackfillProgress)> {
        self.per_vnode
            .iter()
            .map(|(&vnode, progress)| (vnode, progress))
    }

    fn has_progress(&self) -> bool {
        self.per_vnode
            .values()
            .any(|progress| matches!(progress, LocalityBackfillProgress::InProgress { .. }))
    }

    fn update_progress(&mut self, vnode: VirtualNode, new_pos: OwnedRow, row_count_delta: u64) {
        let progress = self.per_vnode.get_mut(&vnode).unwrap();
        match progress {
            LocalityBackfillProgress::NotStarted => {
                *progress = LocalityBackfillProgress::InProgress {
                    current_pos: new_pos,
                    processed_rows: row_count_delta,
                };
            }
            LocalityBackfillProgress::InProgress { processed_rows, .. } => {
                *progress = LocalityBackfillProgress::InProgress {
                    current_pos: new_pos,
                    processed_rows: *processed_rows + row_count_delta,
                };
            }
            LocalityBackfillProgress::Completed { .. } => {
                // Already completed, shouldn't update
            }
        }
        self.total_snapshot_rows += row_count_delta;
    }

    fn finish_vnode(&mut self, vnode: VirtualNode, pk_len: usize) {
        let progress = self.per_vnode.get_mut(&vnode).unwrap();
        match progress {
            LocalityBackfillProgress::NotStarted => {
                // Create a final position with pk_len NULL values to indicate completion
                let final_pos = OwnedRow::new(vec![None; pk_len]);
                *progress = LocalityBackfillProgress::Completed {
                    final_pos,
                    total_rows: 0,
                };
            }
            LocalityBackfillProgress::InProgress {
                current_pos,
                processed_rows,
            } => {
                *progress = LocalityBackfillProgress::Completed {
                    final_pos: current_pos.clone(),
                    total_rows: *processed_rows,
                };
            }
            LocalityBackfillProgress::Completed { .. } => {
                // Already completed
            }
        }
    }

    fn get_progress(&self, vnode: &VirtualNode) -> &LocalityBackfillProgress {
        self.per_vnode.get(vnode).unwrap()
    }
}

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
    locality_columns: Vec<usize>,

    /// State table for buffering input data
    state_table: StateTable<S>,

    /// Progress table for tracking backfill progress per vnode
    progress_table: StateTable<S>,

    /// Schema of the input
    input_schema: Schema,

    /// Progress reporter for materialized view creation
    progress: CreateMviewProgressReporter,

    /// Actor ID for this executor
    actor_id: ActorId,

    /// Metrics
    metrics: Arc<StreamingMetrics>,

    /// Chunk size for output
    chunk_size: usize,

    /// Fragment ID of the fragment this LocalityProvider belongs to
    fragment_id: FragmentId,
}

impl<S: StateStore> LocalityProviderExecutor<S> {
    #[allow(clippy::too_many_arguments)]
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
    ) -> Self {
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
        }
    }

    /// Creates a snapshot stream that reads from state table in locality order
    #[try_stream(ok = Option<(VirtualNode, OwnedRow)>, error = StreamExecutorError)]
    async fn make_snapshot_stream<'a>(
        state_table: &'a StateTable<S>,
        backfill_state: LocalityBackfillState,
    ) {
        // Read from state table per vnode in locality order
        for vnode in state_table.vnodes().iter_vnodes() {
            let progress = backfill_state.get_progress(&vnode);

            let current_pos = match progress {
                LocalityBackfillProgress::NotStarted => None,
                LocalityBackfillProgress::Completed { .. } => {
                    // Skip completed vnodes
                    continue;
                }
                LocalityBackfillProgress::InProgress { current_pos, .. } => {
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
            let iter = state_table
                .iter_with_vnode(
                    vnode,
                    &range_bounds,
                    PrefetchOptions::prefetch_for_small_range_scan(),
                )
                .await?;
            pin_mut!(iter);

            while let Some(row) = iter.try_next().await? {
                yield Some((vnode, row));
            }
        }

        // Signal end of stream
        yield None;
    }

    /// Persist backfill state to progress table
    async fn persist_backfill_state(
        progress_table: &mut StateTable<S>,
        backfill_state: &LocalityBackfillState,
    ) -> StreamExecutorResult<()> {
        for (vnode, progress) in &backfill_state.per_vnode {
            let (is_finished, current_pos, row_count) = match progress {
                LocalityBackfillProgress::NotStarted => continue, // Don't persist NotStarted
                LocalityBackfillProgress::InProgress {
                    current_pos,
                    processed_rows,
                } => (false, current_pos.clone(), *processed_rows),
                LocalityBackfillProgress::Completed {
                    final_pos,
                    total_rows,
                } => (true, final_pos.clone(), *total_rows),
            };

            // Build progress row: vnode + current_pos + is_finished + row_count
            let mut row_data = vec![Some(vnode.to_scalar().into())];
            row_data.extend(current_pos);
            row_data.push(Some(risingwave_common::types::ScalarImpl::Bool(
                is_finished,
            )));
            row_data.push(Some(risingwave_common::types::ScalarImpl::Int64(
                row_count as i64,
            )));

            let new_row = OwnedRow::new(row_data);

            // Check if there's an existing row for this vnode to determine insert vs update
            // This ensures state operation consistency - update existing rows, insert new ones
            let key_data = vec![Some(vnode.to_scalar().into())];
            let key = OwnedRow::new(key_data);

            if let Some(existing_row) = progress_table.get_row(&key).await? {
                // Update existing state - ensures proper state transition for recovery
                progress_table.update(existing_row, new_row);
            } else {
                // Insert new state - first time persisting for this vnode
                progress_table.insert(new_row);
            }
        }
        Ok(())
    }

    /// Load backfill state from progress table
    async fn load_backfill_state(
        progress_table: &StateTable<S>,
        _locality_columns: &[usize],
    ) -> StreamExecutorResult<LocalityBackfillState> {
        let mut backfill_state = LocalityBackfillState::new(progress_table.vnodes().iter_vnodes());
        let mut total_snapshot_rows = 0;

        // For each vnode, try to get its progress state
        for vnode in progress_table.vnodes().iter_vnodes() {
            // Build key: vnode + NULL values for locality columns (to match progress table schema)
            let key_data = vec![Some(vnode.to_scalar().into())];

            let key = OwnedRow::new(key_data);

            if let Some(row) = progress_table.get_row(&key).await? {
                // Parse is_finished flag (second to last column)
                let finished_col_idx = row.len() - 2;
                let is_finished = row
                    .datum_at(finished_col_idx)
                    .map(|d| d.into_bool())
                    .unwrap_or(false);

                // Parse row count (last column)
                let row_count = row
                    .datum_at(row.len() - 1)
                    .map(|d| d.into_int64() as u64)
                    .unwrap_or(0);

                let current_pos_data: Vec<Datum> = (1..finished_col_idx)
                    .map(|i| row.datum_at(i).to_owned_datum())
                    .collect();
                let current_pos = OwnedRow::new(current_pos_data);

                // Set progress based on is_finished flag
                let progress = if is_finished {
                    LocalityBackfillProgress::Completed {
                        final_pos: current_pos,
                        total_rows: row_count,
                    }
                } else {
                    LocalityBackfillProgress::InProgress {
                        current_pos,
                        processed_rows: row_count,
                    }
                };

                backfill_state.per_vnode.insert(vnode, progress);
                total_snapshot_rows += row_count;
            }
            // If no row found, keep the default NotStarted state
        }

        backfill_state.total_snapshot_rows = total_snapshot_rows;
        Ok(backfill_state)
    }

    /// Mark chunk for forwarding based on backfill progress
    fn mark_chunk(
        chunk: StreamChunk,
        backfill_state: &LocalityBackfillState,
        state_table: &StateTable<S>,
    ) -> StreamExecutorResult<StreamChunk> {
        let chunk = chunk.compact();
        let (data, ops) = chunk.into_parts();
        let mut new_visibility = risingwave_common::bitmap::BitmapBuilder::with_capacity(ops.len());

        let pk_indices = state_table.pk_indices();
        let pk_order = state_table.pk_serde().get_order_types();

        for (_i, row) in data.rows().enumerate() {
            // Project to primary key columns for comparison
            let pk = row.project(pk_indices);
            let vnode = state_table.compute_vnode_by_pk(pk);

            let visible = match backfill_state.get_progress(&vnode) {
                LocalityBackfillProgress::Completed { .. } => true,
                LocalityBackfillProgress::NotStarted => false,
                LocalityBackfillProgress::InProgress { current_pos, .. } => {
                    // Compare primary key with current position
                    cmp_datum_iter(pk.iter(), current_pos.iter(), pk_order.iter().copied()).is_le()
                }
            };

            new_visibility.append(visible);
        }

        let (columns, _) = data.into_parts();
        let chunk = StreamChunk::with_visibility(ops, columns, new_visibility.finish());
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

        // Wait for first barrier to initialize
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let first_epoch = first_barrier.epoch;

        // Propagate the first barrier
        yield Message::Barrier(first_barrier);

        let mut state_table = self.state_table;
        let mut progress_table = self.progress_table;

        // Initialize state tables
        state_table.init_epoch(first_epoch).await?;
        progress_table.init_epoch(first_epoch).await?;

        // Load backfill state from progress table
        let mut backfill_state =
            Self::load_backfill_state(&progress_table, &self.locality_columns).await?;

        // Get pk info from state table
        let pk_indices = state_table.pk_indices().iter().cloned().collect_vec();

        let need_backfill = !backfill_state.is_completed();

        let need_buffering = backfill_state
            .per_vnode
            .values()
            .all(|progress| matches!(progress, LocalityBackfillProgress::NotStarted));

        // Initial buffering phase before backfill - wait for StartFragmentBackfill mutation (if needed)
        if need_buffering {
            // Enter buffering phase - buffer data until StartFragmentBackfill is received
            let mut start_backfill = false;

            #[for_await]
            for msg in upstream.by_ref() {
                let msg = msg?;

                match msg {
                    Message::Watermark(_) => {
                        // Ignore watermarks during initial buffering
                    }
                    Message::Chunk(chunk) => {
                        state_table.write_chunk(chunk);
                        state_table.try_flush().await?;
                    }
                    Message::Barrier(barrier) => {
                        let epoch = barrier.epoch;

                        // Check for StartFragmentBackfill mutation
                        if let Some(mutation) = barrier.mutation.as_deref() {
                            use crate::executor::Mutation;
                            if let Mutation::StartFragmentBackfill { fragment_ids } = mutation {
                                tracing::info!(
                                    "Start backfill of locality provider with fragment id: {:?}",
                                    &self.fragment_id
                                );
                                if fragment_ids.contains(&self.fragment_id) {
                                    start_backfill = true;
                                }
                            }
                        }

                        // Commit state tables
                        let post_commit1 = state_table.commit(epoch).await?;
                        let post_commit2 = progress_table.commit(epoch).await?;

                        yield Message::Barrier(barrier);
                        post_commit1.post_yield_barrier(None).await?;
                        post_commit2.post_yield_barrier(None).await?;

                        // Start backfill when StartFragmentBackfill mutation is received
                        if start_backfill {
                            break;
                        }
                    }
                }
            }
        }

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
        //  - Reconstruct the whole backfill stream with upstream and new snapshot read stream.
        //
        // When a chunk comes from snapshot, we forward it to the downstream and raise
        // `current_pos`.
        //
        // When we reach the end of the snapshot read stream, it means backfill has been
        // finished.
        //
        // Once the backfill loop ends, we forward the upstream directly to the downstream.

        if need_backfill {
            let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];
            let mut pending_barrier: Option<Barrier> = None;

            let metrics = self
                .metrics
                .new_backfill_metrics(state_table.table_id(), self.actor_id);

            'backfill_loop: loop {
                let mut cur_barrier_snapshot_processed_rows: u64 = 0;
                let mut cur_barrier_upstream_processed_rows: u64 = 0;
                let _snapshot_read_complete = false;

                // Create the backfill stream with upstream and snapshot
                {
                    let left_upstream = upstream.by_ref().map(Either::Left);
                    let right_snapshot = pin!(
                        Self::make_snapshot_stream(&state_table, backfill_state.clone(),)
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
                                match msg? {
                                    None => {
                                        // End of the snapshot read stream.
                                        // Consume remaining rows in the buffer.
                                        for chunk in upstream_chunk_buffer.drain(..) {
                                            let chunk_cardinality = chunk.cardinality() as u64;
                                            cur_barrier_upstream_processed_rows +=
                                                chunk_cardinality;
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
                                        // Extract primary key from row for progress tracking
                                        let pk = row.clone().project(&pk_indices);

                                        // Convert projected row to OwnedRow for progress tracking
                                        let pk_owned = pk.into_owned_row();

                                        // Update progress for this vnode
                                        backfill_state.update_progress(vnode, pk_owned, 1);

                                        cur_barrier_snapshot_processed_rows += 1;

                                        // Create chunk with single row
                                        let chunk = StreamChunk::from_rows(
                                            &[(Op::Insert, row)],
                                            &self.input_schema.data_types(),
                                        );
                                        yield Message::Chunk(chunk);
                                    }
                                }
                            }
                        }
                    }
                }

                // Process barrier
                let barrier = match pending_barrier.take() {
                    Some(barrier) => barrier,
                    None => break 'backfill_loop, // Reached end of backfill
                };

                // Process upstream buffer chunks with marking
                for chunk in upstream_chunk_buffer.drain(..) {
                    cur_barrier_upstream_processed_rows += chunk.cardinality() as u64;

                    // Mark chunk based on backfill progress
                    if backfill_state.has_progress() {
                        let marked_chunk =
                            Self::mark_chunk(chunk.clone(), &backfill_state, &state_table)?;
                        yield Message::Chunk(marked_chunk);
                    }
                }

                // no-op commit state table
                state_table
                    .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                    .await?;

                // Update progress with current epoch and snapshot read count
                let total_snapshot_processed_rows: u64 = backfill_state
                    .vnodes()
                    .map(|(_, progress)| match progress {
                        &LocalityBackfillProgress::InProgress { processed_rows, .. } => {
                            processed_rows
                        }
                        &LocalityBackfillProgress::Completed { total_rows, .. } => total_rows,
                        &LocalityBackfillProgress::NotStarted => 0,
                    })
                    .sum();

                self.progress.update(
                    barrier.epoch,
                    barrier.epoch.curr, // Use barrier epoch as snapshot read epoch
                    total_snapshot_processed_rows,
                );

                // Persist backfill progress
                Self::persist_backfill_state(
                    &mut progress_table,
                    &backfill_state,
                )
                .await?;
                let barrier_epoch = barrier.epoch;
                let post_commit = progress_table.commit(barrier_epoch).await?;

                metrics
                    .backfill_snapshot_read_row_count
                    .inc_by(cur_barrier_snapshot_processed_rows);
                metrics
                    .backfill_upstream_output_row_count
                    .inc_by(cur_barrier_upstream_processed_rows);

                yield Message::Barrier(barrier);
                post_commit.post_yield_barrier(None).await?;

                // Check if all vnodes are complete
                if backfill_state.is_completed() {
                    // Backfill is complete, finish progress reporting
                    let total_snapshot_processed_rows: u64 = backfill_state
                        .vnodes()
                        .map(|(_, progress)| {
                            match progress {
                                &LocalityBackfillProgress::Completed { total_rows, .. } => {
                                    total_rows
                                }
                                _ => 0, // Should all be completed at this point
                            }
                        })
                        .sum();

                    self.progress
                        .finish(barrier_epoch, total_snapshot_processed_rows);
                    break 'backfill_loop;
                }
            }
        }

        tracing::debug!("Locality provider backfill finished, forwarding upstream directly");

        // Wait for first barrier after backfill completion to mark progress as finished
        if need_backfill && !backfill_state.is_completed() {
            while let Some(Ok(msg)) = upstream.next().await {
                match msg {
                    Message::Barrier(barrier) => {
                        // no-op commit state table
                        state_table
                            .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                            .await?;

                        // Mark all vnodes as completed
                        for vnode in state_table.vnodes().iter_vnodes() {
                            backfill_state.finish_vnode(vnode, pk_indices.len());
                        }

                        // Calculate final total processed rows
                        let total_snapshot_processed_rows: u64 = backfill_state
                            .vnodes()
                            .map(|(_, progress)| match progress {
                                &LocalityBackfillProgress::Completed { total_rows, .. } => {
                                    total_rows
                                }
                                &LocalityBackfillProgress::InProgress {
                                    processed_rows, ..
                                } => processed_rows,
                                &LocalityBackfillProgress::NotStarted => 0,
                            })
                            .sum();

                        // Finish progress reporting
                        self.progress
                            .finish(barrier.epoch, total_snapshot_processed_rows);

                        // Persist final state
                        Self::persist_backfill_state(
                            &mut progress_table,
                            &backfill_state,
                        )
                        .await?;
                        let post_commit = progress_table.commit(barrier.epoch).await?;

                        yield Message::Barrier(barrier);
                        post_commit.post_yield_barrier(None).await?;
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

        // TODO: truncate the state table after backfill.

        // After backfill completion, forward messages directly
        #[for_await]
        for msg in upstream {
            let msg = msg?;

            match msg {
                Message::Barrier(barrier) => {
                    // Commit state tables but don't modify them
                    state_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
                    progress_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
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
