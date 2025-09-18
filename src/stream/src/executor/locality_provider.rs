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

use std::sync::Arc;

use futures::{TryStreamExt, pin_mut};
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common::util::epoch::EpochPair;
use risingwave_storage::StateStore;
use risingwave_storage::store::PrefetchOptions;

use crate::common::table::state_table::StateTable;
use crate::executor::prelude::*;
use crate::task::{ActorId, FragmentId};

/// The `LocalityProviderExecutor` provides locality for operators during backfilling.
/// It buffers input data into a state table using locality columns as primary key prefix.
///
/// The executor has two phases:
/// 1. Backfill phase: Buffer incoming data into state table
/// 2. Serve phase: Provide buffered data with locality after receiving backfill completion signal
pub struct LocalityProviderExecutor<S: StateStore> {
    /// Upstream input
    upstream: Executor,

    /// Locality columns (indices in input schema)
    locality_columns: Vec<usize>,

    /// State table for buffering input data
    state_table: StateTable<S>,

    /// Progress table for tracking backfill progress
    progress_table: StateTable<S>,

    /// Schema of the input
    input_schema: Schema,

    /// Actor ID
    actor_id: ActorId,

    /// Fragment ID
    fragment_id: FragmentId,

    /// Metrics
    metrics: Arc<StreamingMetrics>,

    /// Chunk size for output
    chunk_size: usize,
}

impl<S: StateStore> LocalityProviderExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        upstream: Executor,
        locality_columns: Vec<usize>,
        state_table: StateTable<S>,
        progress_table: StateTable<S>,
        input_schema: Schema,
        actor_id: ActorId,
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
            actor_id,
            fragment_id,
            metrics,
            chunk_size,
        }
    }

    /// Provide buffered data with locality (static method)
    async fn provide_locality_data(
        state_table: &StateTable<S>,
        input_schema: &Schema,
        chunk_size: usize,
        _epoch: EpochPair,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        // Iterate through state table which is already ordered by locality columns
        // Use iter_with_prefix to get all rows (empty prefix = all rows)
        let empty_prefix: &[risingwave_common::types::Datum] = &[];
        let iter = state_table
            .iter_with_prefix(
                empty_prefix,
                &(
                    std::ops::Bound::<&[risingwave_common::types::Datum]>::Unbounded,
                    std::ops::Bound::<&[risingwave_common::types::Datum]>::Unbounded,
                ),
                PrefetchOptions::default(),
            )
            .await?;
        pin_mut!(iter);

        let mut output_rows = Vec::new();
        while let Some(keyed_row) = iter.try_next().await? {
            output_rows.push((Op::Insert, keyed_row));

            // If we've collected enough rows, emit a chunk
            if output_rows.len() >= chunk_size {
                let chunk = StreamChunk::from_rows(&output_rows, &input_schema.data_types());
                return Ok(Some(chunk));
            }
        }

        // Emit remaining rows if any
        if !output_rows.is_empty() {
            let chunk = StreamChunk::from_rows(&output_rows, &input_schema.data_types());
            Ok(Some(chunk))
        } else {
            Ok(None)
        }
    }

    /// Update progress and persist state (static method)
    fn update_progress(
        progress_table: &mut StateTable<S>,
        _epoch: EpochPair,
    ) -> StreamExecutorResult<()> {
        // For LocalityProvider, we use a simple boolean flag to indicate completion
        // Insert a single row into progress table to mark backfill as finished
        let vnodes: Vec<_> = progress_table.vnodes().iter_vnodes().collect();
        for vnode in vnodes {
            let row = [
                Some(vnode.to_scalar().into()),
                Some(risingwave_common::types::ScalarImpl::Bool(true)),
            ];
            progress_table.insert(&row);
        }
        Ok(())
    }

    /// Check progress state by reading progress table (static method)
    /// Returns (`has_progress_state`, `is_backfill_finished`)
    /// - `has_progress_state`: true if we have any progress state recorded
    /// - `is_backfill_finished`: true if backfill is completed (only valid when `has_progress_state` is true)
    async fn check_backfill_progress(
        progress_table: &StateTable<S>,
    ) -> StreamExecutorResult<(bool, bool)> {
        let mut vnodes = progress_table.vnodes().iter_vnodes_scalar();
        let first_vnode = vnodes.next().unwrap();
        let key: &[risingwave_common::types::Datum] = &[Some(first_vnode.into())];

        if let Some(row) = progress_table.get_row(key).await? {
            // Row exists, check the finished flag
            let is_finished: bool = row.datum_at(1).unwrap().into_bool();
            Ok((true, is_finished))
        } else {
            // No row exists, backfill not started yet
            Ok((false, false))
        }
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
        // Extract actor_id before we consume self
        let actor_id = self.actor_id;

        let mut upstream = self.upstream.execute();

        // Wait for first barrier to initialize
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let first_epoch = first_barrier.epoch;
        let is_newly_added = first_barrier.is_newly_added(actor_id);

        // Initialize state tables
        self.state_table.init_epoch(first_epoch).await?;
        self.progress_table.init_epoch(first_epoch).await?;

        // Propagate the first barrier
        yield Message::Barrier(first_barrier);

        // Check progress state using static method to avoid borrowing issues
        let (has_progress_state, is_backfill_finished) =
            Self::check_backfill_progress(&self.progress_table).await?;

        // Determine what to do based on progress state:
        // - If no progress state exists: need to buffer chunks (backfill not started)
        // - If progress state exists but not finished: backfill in progress, no buffering anymore
        // - If progress state exists and finished: pass-through mode (backfill completed)
        let need_buffering = !has_progress_state;
        let is_completely_finished = has_progress_state && is_backfill_finished;

        if is_completely_finished {
            assert!(!is_newly_added);
        }

        tracing::info!(
            actor_id = actor_id,
            has_progress_state = has_progress_state,
            is_backfill_finished = is_backfill_finished,
            need_buffering = need_buffering,
            "LocalityProvider initialized"
        );

        if need_buffering {
            // Enter buffering phase - buffer data until backfill completion signal
            let mut backfill_complete = false;

            #[for_await]
            for msg in upstream.by_ref() {
                let msg = msg?;

                match msg {
                    Message::Watermark(_) => {
                        // Forward watermarks
                        yield msg;
                    }
                    Message::Chunk(chunk) => {
                        for (op, row_ref) in chunk.rows() {
                            match op {
                                Op::Insert | Op::UpdateInsert => {
                                    self.state_table.insert(row_ref);
                                }
                                Op::Delete | Op::UpdateDelete => {
                                    self.state_table.delete(row_ref);
                                }
                            }
                        }
                    }
                    Message::Barrier(barrier) => {
                        let epoch = barrier.epoch;

                        // Commit state tables
                        let post_commit = self.state_table.commit(epoch).await?;

                        // Check if this is a backfill completion signal
                        // For now, use a simple heuristic (in practice, this should be a proper signal)
                        if !backfill_complete {
                            // TODO: Replace with actual backfill completion detection
                            // For now, assume backfill completes after receiving some data
                            backfill_complete = true; // Simplified for demo

                            if backfill_complete {
                                tracing::info!(
                                    actor_id = actor_id,
                                    "LocalityProvider backfill completed, updating progress"
                                );

                                // Update progress to completed
                                Self::update_progress(&mut self.progress_table, epoch)?;
                                let progress_post_commit =
                                    self.progress_table.commit(epoch).await?;

                                // Provide buffered data with locality
                                // if let Some(locality_chunk) =
                                //     self.provide_locality_data(epoch).await?
                                // {
                                //     yield Message::Chunk(locality_chunk);
                                // }

                                yield Message::Barrier(barrier);
                                progress_post_commit.post_yield_barrier(None).await?;
                                break; // Exit buffering phase
                            }
                        }

                        yield Message::Barrier(barrier);
                        post_commit.post_yield_barrier(None).await?;
                    }
                }
            }

            tracing::debug!(
                actor_id = actor_id,
                "LocalityProvider backfill finished, entering passthrough mode"
            );
        }

        // After backfill completion (or if already completed), forward messages directly
        #[for_await]
        for msg in upstream {
            let msg = msg?;

            match msg {
                Message::Barrier(barrier) => {
                    // Commit state tables but don't modify them
                    self.state_table
                        .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                        .await?;
                    self.progress_table
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

#[cfg(test)]
mod tests {
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::MockSource;

    #[tokio::test]
    async fn test_locality_provider_basic() {
        // This is a basic test structure
        // TODO: Implement comprehensive tests
    }
}
