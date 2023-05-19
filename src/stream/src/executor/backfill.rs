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
use risingwave_common::array::stream_record::Record;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::{BitmapBuilder};
use risingwave_common::catalog::Schema;

use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::types::{Datum};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::{cmp_datum, OrderType};
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::TableIter;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::{expect_first_barrier, BoxedExecutor, Executor, ExecutorInfo, Message, PkIndicesRef};
use crate::common::table::state_table::StateTable;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{PkIndices, StreamExecutorResult, Watermark};
use crate::task::{ActorId, CreateMviewProgress};

/// An implementation of the RFC: Use Backfill To Let Mv On Mv Stream Again.(https://github.com/risingwavelabs/rfcs/pull/13)
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
    state_table: StateTable<S>,

    /// Upstream dist key to compute vnode
    dist_key_in_pk: Vec<usize>,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    progress: CreateMviewProgress,

    actor_id: ActorId,

    info: ExecutorInfo,

    metrics: Arc<StreamingMetrics>,
}

const CHUNK_SIZE: usize = 1024;

impl<S> BackfillExecutor<S>
where
    S: StateStore,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        upstream_table: StorageTable<S>,
        upstream: BoxedExecutor,
        state_table: StateTable<S>,
        dist_key_in_pk: Vec<usize>,
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
                identity: "BackfillExecutor".to_owned(),
            },
            upstream_table,
            upstream,
            state_table,
            dist_key_in_pk,
            output_indices,
            actor_id: progress.actor_id(),
            progress,
            metrics,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // The primary key columns, in the output columns of the upstream_table scan.
        let pk_in_output_indices = self.upstream_table.pk_in_output_indices().unwrap();

        let pk_order = self.upstream_table.pk_serializer().get_order_types();

        let _dist_key_in_pk = self.dist_key_in_pk;

        let upstream_table_id = self.upstream_table.table_id().table_id;

        let mut upstream = self.upstream.execute();

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let init_epoch = first_barrier.epoch.prev;
        self.state_table.init_epoch(first_barrier.epoch);

        // If the internal persisted state is "finished" for this executor, we are done, no need
        // to_create_mv
        // FIXME(kwannoel): This is unimplemented. TODO: Remove the line below once complete.
        let to_create_mv = first_barrier.is_newly_added(self.actor_id);
        // If the snapshot is empty, we don't need to backfill.
        let is_snapshot_empty: bool = {
            let snapshot = Self::snapshot_read(&self.upstream_table, init_epoch, None, false);
            pin_mut!(snapshot);
            snapshot.try_next().await?.unwrap().is_none()
        };
        // Whether we still need to backfill
        let to_backfill = to_create_mv && !is_snapshot_empty;

        if to_create_mv && is_snapshot_empty {
            // Directly finish the progress as the snapshot is empty.
            self.progress.finish(first_barrier.epoch.curr);
        }

        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);

        if !to_backfill {
            #[for_await]
            for message in upstream {
                // Then forward messages directly to the downstream.
                if let Some(message) = Self::mapping_message(message?, &self.output_indices) {
                    yield message;
                }
            }

            return Ok(());
        }

        // The epoch used to snapshot read upstream mv.
        let mut snapshot_read_epoch = init_epoch;

        // Current position of the upstream_table storage primary key.
        // `None` means it starts from the beginning.
        let mut current_pos: Option<OwnedRow> = None;

        // Use this to track old persisted state.
        let mut old_state: Option<Vec<Datum>> = None;

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
        'backfill_loop: loop {
            let mut upstream_chunk_buffer: Vec<StreamChunk> = vec![];

            let left_upstream = upstream.by_ref().map(Either::Left);

            let right_snapshot = pin!(Self::snapshot_read(
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
                                for chunk in upstream_chunk_buffer.drain(..) {
                                    cur_barrier_upstream_processed_rows +=
                                        chunk.cardinality() as u64;
                                    if let Some(current_pos) = &current_pos {
                                        yield Message::Chunk(Self::mapping_chunk(
                                            Self::mark_chunk(
                                                chunk,
                                                current_pos,
                                                &pk_in_output_indices,
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
                                current_pos = Self::update_pos(&chunk, &pk_in_output_indices);

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

        tracing::trace!(
            actor = self.actor_id,
            "Backfill has already finished and forward messages directly to the downstream"
        );

        // Wait for first barrier to come after backfill is finished.
        // So we can update our progress + persist the status.
        while let Some(Ok(msg)) = upstream.next().await {
            match &msg {
                // Set persist state to finish on next barrier.
                Message::Barrier(barrier) => {
                    self.progress.finish(barrier.epoch.curr);
                    Self::persist_state(
                        barrier.epoch,
                        &mut self.state_table,
                        true,
                        &current_pos,
                        &mut old_state,
                    )
                    .await?;
                    yield msg;
                    break;
                }

                // If it's not a barrier, just forward
                Message::Chunk(_) | Message::Watermark(_) => {
                    if let Some(msg) = Self::mapping_message(msg, &self.output_indices) {
                        yield msg;
                    }
                }
            }
        }

        // After progress finished + state persisted,
        // we can forward messages directly to the downstream,
        // as backfill is finished.
        #[for_await]
        for msg in upstream {
            if let Some(msg) = Self::mapping_message(msg?, &self.output_indices) {
                yield msg;
            }
        }
    }

    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    async fn snapshot_read(
        upstream_table: &StorageTable<S>,
        epoch: u64,
        current_pos: Option<OwnedRow>,
        ordered: bool,
    ) {
        // `current_pos` is None means it needs to scan from the beginning, so we use Unbounded to
        // scan. Otherwise, use Excluded.
        let range_bounds = if let Some(current_pos) = current_pos {
            // If `current_pos` is an empty row which means upstream mv contains only one row and it
            // has been consumed. The iter interface doesn't support
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

        pin_mut!(iter);

        while let Some(data_chunk) = iter
            .collect_data_chunk(upstream_table.schema(), Some(CHUNK_SIZE))
            .instrument_await("backfill_snapshot_read")
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
        table: &mut StateTable<S>,
        is_finished: bool,
        current_pos: &Option<OwnedRow>,
        old_state: &mut Option<Vec<Datum>>,
    ) -> StreamExecutorResult<()> {
        if let Some(current_pos_inner) = current_pos {
            // state w/o vnodes.
            let current_partial_state = Self::build_temporary_state(is_finished, current_pos_inner);
            Self::flush_data(table, epoch, old_state, current_partial_state.clone()).await?;
            *old_state = Some(current_partial_state);
        } else {
            table.commit_no_data_expected(epoch);
        }
        Ok(())
    }

    /// For `current_pos` and `old_pos` are just pk of upstream.
    /// They should be strictly increasing.
    // FIXME(kwannoel): Currently state table expects a Primary Key.
    // For that we use vnode, so we can update position per vnode to support hash-distributed
    // backfill.
    // However this also means that it computes a new `vnode` partition for each vnode.
    // State table interface should be updated, such that it can reuse this `vnode`
    // as both `PRIMARY KEY` and `vnode`.
    async fn flush_data(
        table: &mut StateTable<S>,
        epoch: EpochPair,
        old_state: &Option<Vec<Datum>>,
        mut current_partial_state: Vec<Datum>,
    ) -> StreamExecutorResult<()> {
        let vnodes = table.vnodes().clone();
        if let Some(old_state) = old_state {
            if *old_state != current_partial_state {
                vnodes.iter_ones().for_each(|vnode| {
                    let datum = Some((vnode as i16).into());
                    // fill the state
                    current_partial_state[0] = datum;
                    table.write_record(Record::Update {
                        old_row: &old_state[..],
                        new_row: &current_partial_state[..],
                    })
                });
            } else {
                table.commit_no_data_expected(epoch);
                return Ok(());
            }
        } else {
            vnodes.iter_ones().for_each(|vnode| {
                let datum = Some((vnode as i16).into());
                // fill the state
                current_partial_state[0] = datum;
                table.write_record(Record::Insert {
                    new_row: &current_partial_state[..],
                })
            });
        }
        table.commit(epoch).await
    }

    // We want to avoid building a row for every vnode.
    // Instead we can just modify a single row, and dispatch it to state table to write.
    fn build_temporary_state(is_finished: bool, current_pos: &OwnedRow) -> Vec<Datum> {
        let mut row_state = vec![None; current_pos.len() + 2];
        row_state[1..current_pos.len() + 1].clone_from_slice(current_pos.as_inner());
        row_state[current_pos.len() + 1] = Some(is_finished.into());
        row_state
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
}

impl<S> Executor for BackfillExecutor<S>
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
