// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Bound;
use std::sync::Arc;

use async_stack_trace::StackTrace;
use futures::{pin_mut, SinkExt, stream, StreamExt, TryFutureExt};
use futures::channel::{mpsc, oneshot};
use futures::stream::select_with_strategy;
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, Row, StreamChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::TableIter;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::{expect_first_barrier, BoxedExecutor, Executor, ExecutorInfo, Message, MessageStream};
use crate::executor::PkIndices;
use crate::task::{ActorId, CreateMviewProgress};

pub struct BackfillExecutor<S: StateStore> {
    table: StorageTable<S>,

    upstream: BoxedExecutor,

    upstream_indices: Arc<[usize]>,

    current_pos: Row,

    progress: CreateMviewProgress,

    actor_id: ActorId,

    info: ExecutorInfo,
}

fn mapping(upstream_indices: &[usize], msg: Message) -> Message {
    match msg {
        Message::Watermark(_) => {
            todo!("https://github.com/risingwavelabs/risingwave/issues/6042")
        }

        Message::Chunk(chunk) => {
            let (ops, columns, visibility) = chunk.into_inner();
            let mapped_columns = upstream_indices
                .iter()
                .map(|&i| columns[i].clone())
                .collect();
            Message::Chunk(StreamChunk::new(ops, mapped_columns, visibility))
        }
        _ => msg,
    }
}

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
            upstream_indices: upstream_indices.into(),
            current_pos: Row::new(vec![]),
            actor_id: progress.actor_id(),
            progress,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        // 0. Project the upstream with `upstream_indices`.
        let backfill_pk_indices = self.pk_indices().to_vec();
        let upstream_indices = self.upstream_indices.to_vec();
        let table_pk_indices = self.table.pk_indices().to_vec();
        let upstream_pk_indices = self.upstream.pk_indices().to_vec();
        assert_eq!(table_pk_indices, upstream_pk_indices);

        let mut upstream = self
            .upstream
            .execute()
            .map(move |result| result.map(|msg| mapping(&upstream_indices.clone(), msg)));

        // 1. Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let init_epoch = first_barrier.epoch.prev;

        // If the barrier is a conf change of creating this mview, init snapshot from its epoch
        // and begin to consume the snapshot.
        // Otherwise, it means we've recovered and the snapshot is already consumed.
        let to_consume_snapshot = first_barrier.is_add_dispatcher(self.actor_id);

        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier.clone());

        if to_consume_snapshot {

            let (mut switch_tx, switch_rx) = mpsc::channel(1);
            let (buffer_chunk_tx, mut buffer_chunk_rx) = mpsc::unbounded();
            let (stop_tx, stop_rx) = oneshot::channel();

            let snapshot = Box::pin(Self::switch_snapshot_read(
                &self.table,
                init_epoch,
                self.current_pos.clone(),
                table_pk_indices,
                switch_rx,
                stop_tx,
                self.actor_id.clone(),
            ));

            let upstream_barriers =  Box::pin(
                Self::buffer_upstream_chunk(&mut upstream, buffer_chunk_tx, stop_rx));

            let backfill_stream =
                select_with_strategy(upstream_barriers, snapshot, |_: &mut ()| {
                    stream::PollNext::Left
                });

            #[for_await]
            for msg in backfill_stream {
                match msg? {
                    Message::Barrier(barrier) => {
                        // If it is a checkpoint barrier, switch snapshot and consume upstream buffer chunk
                        if barrier.checkpoint {
                            println!("Actor: {:?} meet checkpoint barrier before sent switch epoch = {}", &self.actor_id, &barrier.epoch.prev);
                            // Switch snapshot
                            switch_tx.send(barrier.epoch.prev.clone()).map_err(|_| {
                                StreamExecutorError::channel_closed("stop backfill")
                            }).await?;

                            println!("Actor: {:?} meet checkpoint barrier after sent switch", &self.actor_id);

                            // Consume upstream buffer chunk
                            while let Ok(Some(chunk)) = buffer_chunk_rx.try_next() {
                                let chunk = chunk.compact();
                                println!("Actor: {:?} apply chunk {}", &self.actor_id, chunk.to_pretty_string());
                                let (data, ops) = chunk.into_parts();
                                let mut new_visibility = BitmapBuilder::with_capacity(ops.len());
                                for v in data.rows().map(|row| {
                                    row.row_by_indices(&backfill_pk_indices) < self.current_pos
                                }) {
                                    new_visibility.append(v);
                                }
                                let (columns, _) = data.into_parts();
                                yield Message::Chunk(StreamChunk::new(
                                    ops,
                                    columns,
                                    Some(new_visibility.finish()),
                                ));
                            }

                            println!("Actor: {:?} apply upstream buffer", &self.actor_id);
                        }
                        let barrier_prev_epoch = barrier.epoch.prev.clone();
                        let checkpoint = barrier.checkpoint.clone();
                        yield Message::Barrier(barrier);
                        if checkpoint {
                            println!("Actor: {:?} yield checkpoint barrier epoch = {}", &self.actor_id, &barrier_prev_epoch);
                        }
                        self.progress.update(barrier_prev_epoch.clone(), barrier_prev_epoch);
                    },
                    Message::Chunk(chunk) => {
                        // Must be snapshot chunk
                        yield mapping(&self.upstream_indices, Message::Chunk(chunk));
                    },
                    Message::Watermark(_) => todo!("https://github.com/risingwavelabs/risingwave/issues/6042"),
                }
            }

            println!("Actor: {:?} backfill finishes", &self.actor_id);

            // Consume with the renaming stream buffer chunk
            while let Ok(Some(chunk)) = buffer_chunk_rx.try_next() {
                println!("Actor: {:?} apply remaining buffer chunk {}", &self.actor_id, &chunk.to_pretty_string());
                let chunk = chunk.compact();

                let (data, ops) = chunk.into_parts();
                let mut new_visibility = BitmapBuilder::with_capacity(ops.len());
                for v in data.rows().map(|row| {
                    row.row_by_indices(&backfill_pk_indices) < self.current_pos
                }) {
                    new_visibility.append(v);
                }
                let (columns, _) = data.into_parts();
                yield Message::Chunk(StreamChunk::new(
                    ops,
                    columns,
                    Some(new_visibility.finish()),
                ));
            }

            // Backfill finishes and process the upstream finally

            let mut finish_on_barrier = |msg: &Message| {
                if let Some(barrier) = msg.as_barrier() {
                    self.progress.finish(barrier.epoch.curr);
                }
            };

            let mut remaining_upstream = upstream;

            println!("Actor: {:?} apply remaining upstream", &self.actor_id);

            #[for_await]
            for msg in &mut remaining_upstream {
                let msg: Message = msg?;
                finish_on_barrier(&msg);
                yield msg;
            }
        } else {
            // If there's no need to consume the snapshot ...
            // We directly forward the messages from the upstream.

            #[for_await]
            for message in upstream {
                yield message?;
            }
        }
    }

    #[expect(clippy::needless_lifetimes, reason = "code generated by try_stream")]
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn snapshot_read(table: &StorageTable<S>, epoch: u64, current_pos: Row) {
        // Empty row means it need to scan from the beginning, so we use Unbounded to scan.
        // Otherwise, use Excluded.
        let range_bounds = if current_pos.0.is_empty() {
            (Bound::Unbounded, Bound::Unbounded)
        } else {
            (Bound::Excluded(current_pos), Bound::Unbounded)
        };
        let iter = table
            .batch_iter_with_pk_bounds(
                HummockReadEpoch::Committed(epoch),
                Row::empty(),
                range_bounds,
            )
            .await?;

        pin_mut!(iter);

        while let Some(data_chunk) = iter
            .collect_data_chunk(table.schema(), Some(1024))
            .stack_trace("batch_query_executor_collect_chunk")
            .await?
        {
            let ops = vec![Op::Insert; data_chunk.capacity()];
            let stream_chunk = StreamChunk::from_parts(ops, data_chunk);
            yield Message::Chunk(stream_chunk);
        }
    }

    #[expect(clippy::needless_lifetimes, reason = "code generated by try_stream")]
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn buffer_upstream_chunk<U>(
        upstream: &mut U,
        buffer_chunk_tx: mpsc::UnboundedSender<StreamChunk>,
        mut stop_rx: oneshot::Receiver<()>,
    ) where
        U: MessageStream + std::marker::Unpin,
    {
        loop {
            use futures::future::{select, Either};

            // Stop when `stop_rx` is received.
            match select(&mut stop_rx, upstream.next()).await {
                Either::Left((Ok(_), _)) => break,
                Either::Left((Err(_e), _)) => {
                    return Err(StreamExecutorError::channel_closed("stop buffer upstream chunk"))
                }

                Either::Right((Some(msg), _)) => {
                    let msg = msg?;
                    match msg {
                        Message::Barrier(_) => yield msg,
                        Message::Chunk(chunk) => {
                            buffer_chunk_tx
                                .unbounded_send(chunk)
                                .map_err(|_| StreamExecutorError::channel_closed("barrier upstream"))?;
                        },
                        Message::Watermark(_) => {
                            todo!("https://github.com/risingwavelabs/risingwave/issues/6042")
                        }
                    }

                }
                Either::Right((None, _)) => {
                    Err(StreamExecutorError::channel_closed("upstream"))?;
                }
            }
        }
    }

    #[expect(clippy::needless_lifetimes, reason = "code generated by try_stream")]
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn switch_snapshot_read(
        table: &StorageTable<S>,
        init_epoch: u64,
        init_current_pos: Row,
        table_pk_indices: PkIndices,
        mut switch_rx: mpsc::Receiver<u64>,
        stop_tx: oneshot::Sender<()>,
        actor_id: ActorId,
    )
    {
        let mut epoch = init_epoch;
        let mut current_pos = init_current_pos;
        'outer: loop {
            use futures::future::{select, Either};

            let mut snapshot = Box::pin(Self::snapshot_read(
                table,
                epoch,
                current_pos.clone(),
            ));

            println!("Actor: {} snapshot_read: current_pos = {:?}", &actor_id, &current_pos);
            'inner: loop {
                match select(switch_rx.next(), snapshot.next()).await {
                    Either::Left((Some(ep), _)) => {
                        epoch = ep;
                        println!("Actor: {} receive switch epoch {:?}", &actor_id, &epoch);
                        break 'inner;
                    },
                    Either::Left((None, _)) => {
                        return Err(StreamExecutorError::channel_closed("stop snapshot read"))
                    }

                    Either::Right((Some(msg), _)) => {
                        let msg = msg?;
                        match msg {
                            Message::Chunk(ref chunk) => {
                                // Raise current position
                                current_pos = chunk
                                    .rows()
                                    .last()
                                    .unwrap()
                                    .1
                                    .row_by_indices(&table_pk_indices);
                                println!("Actor: {} snapshot push current_pos = {:?}",&actor_id,  &current_pos);
                            }
                            Message::Barrier(_) | Message::Watermark(_) => unreachable!()
                        }

                        yield msg;
                    }
                    Either::Right((None, _)) => {
                        println!("Actor: {} end of snapshot", &actor_id);
                        stop_tx.send(()).map_err(|_| {
                            StreamExecutorError::channel_closed("stop buffer upstream chunk")
                        })?;
                        break 'outer;
                    }
                    // RIGHT ERR?
                }
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
