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

use await_tree::InstrumentAwait;
use either::Either;
use futures::stream::select_with_strategy;
use futures::{pin_mut, stream, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{self, OwnedRow, Row, RowExt};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_common::util::sort_util::OrderType;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::store::PrefetchOptions;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::TableIter;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::{expect_first_barrier, BoxedExecutor, Executor, ExecutorInfo, Message, PkIndicesRef};
use crate::executor::{PkIndices, Watermark};
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
    table: StorageTable<S>,
    /// Upstream with the same schema with the upstream table.
    upstream: BoxedExecutor,

    /// The column indices need to be forwarded to the downstream.
    upstream_indices: Vec<usize>,

    progress: CreateMviewProgress,

    actor_id: ActorId,

    info: ExecutorInfo,
}

const CHUNK_SIZE: usize = 1024;

impl<S> BackfillExecutor<S>
where
    S: StateStore,
{
    pub fn new(
        table: StorageTable<S>,
        upstream: BoxedExecutor,
        upstream_indices: Vec<usize>,
        progress: CreateMviewProgress,
        schema: Schema,
        pk_indices: PkIndices,
    ) -> Self {
        Self {
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "BackfillExecutor".to_owned(),
            },
            table,
            upstream,
            upstream_indices,
            actor_id: progress.actor_id(),
            progress,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // Table storage primary key.
        let table_pk_indices = self.table.pk_indices();
        let pk_order = self.table.pk_serializer().get_order_types();
        let upstream_indices = self.upstream_indices;

        let mut upstream = self.upstream.execute();

        // Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let init_epoch = first_barrier.epoch.prev;

        // If the barrier is a conf change of creating this mview, we follow the procedure of
        // backfill. Otherwise, it means we've recovered and we can forward the upstream messages
        // directly.
        let to_create_mv = first_barrier.is_add_dispatcher(self.actor_id);
        // If the snapshot is empty, we don't need to backfill.
        let is_snapshot_empty: bool = {
            let snapshot = Self::snapshot_read(&self.table, init_epoch, None, false);
            pin_mut!(snapshot);
            snapshot.try_next().await?.unwrap().is_none()
        };
        let to_backfill = to_create_mv && !is_snapshot_empty;

        if to_create_mv && is_snapshot_empty {
            // Directly finish the progress as the snapshot is empty.
            self.progress.finish(first_barrier.epoch.curr);
        }

        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier);

        if !to_backfill {
            // Forward messages directly to the downstream.
            #[for_await]
            for message in upstream {
                if let Some(message) = Self::mapping_message(message?, &upstream_indices) {
                    yield message;
                }
            }

            return Ok(());
        }

        // The epoch used to snapshot read upstream mv.
        let mut snapshot_read_epoch = init_epoch;

        // Current position of the table storage primary key.
        // `None` means it starts from the beginning.
        let mut current_pos: Option<OwnedRow> = None;

        // Keep track of rows from the upstream and snapshot.
        let mut processed_rows: u64 = 0;

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

            let right_snapshot = Box::pin(
                Self::snapshot_read(&self.table, snapshot_read_epoch, current_pos.clone(), true)
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
                    Either::Left(msg) => {
                        match msg? {
                            Message::Barrier(barrier) => {
                                // If it is a barrier, switch snapshot and consume
                                // upstream buffer chunk

                                // Consume upstream buffer chunk
                                for chunk in upstream_chunk_buffer.drain(..) {
                                    if let Some(current_pos) = &current_pos {
                                        yield Message::Chunk(Self::mapping_chunk(
                                            Self::mark_chunk(
                                                chunk,
                                                current_pos,
                                                table_pk_indices,
                                                pk_order,
                                            ),
                                            &upstream_indices,
                                        ));
                                    }
                                }

                                // Update snapshot read epoch.
                                snapshot_read_epoch = barrier.epoch.prev;

                                self.progress.update(
                                    barrier.epoch.curr,
                                    snapshot_read_epoch,
                                    processed_rows,
                                );

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
                    Either::Right(msg) => {
                        match msg? {
                            None => {
                                // End of the snapshot read stream.
                                // We need to set current_pos to the maximum value or do not
                                // mark the chunk anymore, otherwise, we will ignore some rows
                                // in the buffer. Here we choose to never mark the chunk.
                                // Consume with the renaming stream buffer chunk without mark.
                                for chunk in upstream_chunk_buffer.drain(..) {
                                    processed_rows += chunk.cardinality() as u64;
                                    yield Message::Chunk(Self::mapping_chunk(
                                        chunk,
                                        &upstream_indices,
                                    ));
                                }

                                // Finish backfill.
                                break 'backfill_loop;
                            }
                            Some(chunk) => {
                                // Raise the current position.
                                // As snapshot read streams are ordered by pk, so we can
                                // just use the last row to update `current_pos`.
                                current_pos = Some(
                                    chunk
                                        .rows()
                                        .last()
                                        .unwrap()
                                        .1
                                        .project(table_pk_indices)
                                        .into_owned_row(),
                                );
                                processed_rows += chunk.cardinality() as u64;
                                yield Message::Chunk(Self::mapping_chunk(chunk, &upstream_indices));
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

        // Backfill has already finished.
        // Forward messages directly to the downstream.
        #[for_await]
        for msg in upstream {
            if let Some(msg) = Self::mapping_message(msg?, &upstream_indices) {
                if let Some(barrier) = msg.as_barrier() {
                    self.progress.finish(barrier.epoch.curr);
                }
                yield msg;
            }
        }
    }

    #[try_stream(ok = Option<StreamChunk>, error = StreamExecutorError)]
    async fn snapshot_read(
        table: &StorageTable<S>,
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
                assert!(table.pk_indices().is_empty());
                yield None;
                return Ok(());
            }

            (Bound::Excluded(current_pos), Bound::Unbounded)
        } else {
            (Bound::Unbounded, Bound::Unbounded)
        };
        // We use uncommitted read here, because we have already scheduled the `BackfillExecutor`
        // together with the upstream mv.
        let iter = table
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
            .collect_data_chunk(table.schema(), Some(CHUNK_SIZE))
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
        table_pk_indices: PkIndicesRef<'_>,
        pk_order: &[OrderType],
    ) -> StreamChunk {
        let chunk = chunk.compact();
        let (data, ops) = chunk.into_parts();
        let mut new_visibility = BitmapBuilder::with_capacity(ops.len());
        // Use project to avoid allocation.
        for v in data.rows().map(|row| {
            match row
                .project(table_pk_indices)
                .iter()
                .zip_eq_fast(pk_order.iter())
                .cmp_by(current_pos.iter(), |(x, order), y| match order {
                    OrderType::Ascending => x.cmp(&y),
                    OrderType::Descending => y.cmp(&x),
                }) {
                Ordering::Less | Ordering::Equal => true,
                Ordering::Greater => false,
            }
        }) {
            new_visibility.append(v);
        }
        let (columns, _) = data.into_parts();
        StreamChunk::new(ops, columns, Some(new_visibility.finish()))
    }

    fn mapping_chunk(chunk: StreamChunk, upstream_indices: &[usize]) -> StreamChunk {
        let (ops, columns, visibility) = chunk.into_inner();
        let mapped_columns = upstream_indices
            .iter()
            .map(|&i| columns[i].clone())
            .collect();
        StreamChunk::new(ops, mapped_columns, visibility)
    }

    fn mapping_watermark(watermark: Watermark, upstream_indices: &[usize]) -> Option<Watermark> {
        upstream_indices
            .iter()
            .position(|&idx| idx == watermark.col_idx)
            .map(|idx| watermark.with_idx(idx))
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
