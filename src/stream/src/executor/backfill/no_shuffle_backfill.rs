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

use risingwave_common::bail;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_storage::table::batch_table::BatchTable;

use crate::executor::backfill::utils::mapping_message;
use crate::executor::prelude::*;
use crate::task::{CreateMviewProgressReporter, FragmentId};

/// Schema: | vnode | pk ... | `backfill_finished` | `row_count` |
/// We can decode that into `BackfillState` on recovery.
#[derive(Debug, Eq, PartialEq)]
pub struct BackfillState {
    current_pos: Option<OwnedRow>,
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
    upstream_table: BatchTable<S>,
    /// Upstream with the same schema with the upstream table.
    upstream: Executor,

    /// Internal state table for persisting state of backfill state.
    state_table: Option<StateTable<S>>,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    /// PTAL at the docstring for `CreateMviewProgress` to understand how we compute it.
    progress: CreateMviewProgressReporter,

    actor_id: ActorId,

    fragment_id: FragmentId,
}

impl<S> BackfillExecutor<S>
where
    S: StateStore,
{
    pub fn new(
        upstream_table: BatchTable<S>,
        upstream: Executor,
        state_table: Option<StateTable<S>>,
        output_indices: Vec<usize>,
        progress: CreateMviewProgressReporter,
        fragment_id: FragmentId,
    ) -> Self {
        let actor_id = progress.actor_id();
        Self {
            upstream_table,
            upstream,
            state_table,
            output_indices,
            progress,
            actor_id,
            fragment_id,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // The primary key columns.
        // We receive a pruned chunk from the upstream table,
        // which will only contain output columns of the scan on the upstream table.
        // The pk indices specify the pk columns of the pruned chunk.
        let pk_indices = self.upstream_table.pk_in_output_indices().unwrap();

        let upstream_table_id = self.upstream_table.table_id();

        let mut upstream = self.upstream.execute();

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let first_epoch = first_barrier.epoch;
        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);

        if let Some(state_table) = self.state_table.as_mut() {
            state_table.init_epoch(first_epoch).await?;
        }

        let BackfillState {
            current_pos,
            is_finished,
            row_count,
            ..
        } = Self::recover_backfill_state(self.state_table.as_ref(), pk_indices.len()).await?;
        tracing::trace!(is_finished, row_count, "backfill state recovered");

        if !is_finished {
            bail!(
                "legacy no-shuffle backfill recovered unfinished progress; cancel and recreate the streaming job. upstream_table_id={:?}, fragment_id={:?}, actor_id={}, current_pos={:?}, row_count={}",
                upstream_table_id,
                self.fragment_id,
                self.actor_id,
                current_pos,
                row_count,
            );
        }

        tracing::trace!("Backfill has finished, waiting for barrier");

        // Wait for first barrier to come after backfill is finished.
        // So we can update our progress + persist the status.
        while let Some(Ok(msg)) = upstream.next().await {
            if let Some(msg) = mapping_message(msg, &self.output_indices) {
                if let Message::Barrier(barrier) = &msg {
                    // If already finished, no need persist any state, but we need to advance the
                    // epoch of the state table anyway.
                    if let Some(table) = &mut self.state_table {
                        table
                            .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                            .await?;
                    }

                    // Backfill progress in meta is not persisted by the executor, so report it
                    // again after recovery.
                    self.progress.finish(barrier.epoch, row_count);
                    tracing::trace!(
                        epoch = ?barrier.epoch,
                        "Updated CreateMaterializedTracker"
                    );
                    yield msg;
                    break;
                }
                // Allow other messages to pass through.
                // We won't yield twice here, since if there's a barrier,
                // we will always break out of the loop.
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
                if let Message::Barrier(barrier) = &msg {
                    // If already finished, no need persist any state, but we need to advance the epoch of the state table anyway.
                    if let Some(table) = &mut self.state_table {
                        table
                            .commit_assert_no_update_vnode_bitmap(barrier.epoch)
                            .await?;
                    }
                }

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
            };
        };
        let row = row.into_inner();
        let current_pos = Some((&row[0..pk_len]).into_owned_row());
        let is_finished = row[pk_len].clone().is_some_and(|d| d.into_bool());
        let row_count = row
            .get(pk_len + 1)
            .cloned()
            .unwrap_or(None)
            .map_or(0, |d| d.into_int64() as u64);
        BackfillState {
            current_pos,
            is_finished,
            row_count,
        }
    }
}

impl<S> Execute for BackfillExecutor<S>
where
    S: StateStore,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}
