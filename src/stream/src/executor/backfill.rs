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
use futures::{pin_mut, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::array::{Op, Row, StreamChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::table::TableIter;
use risingwave_storage::StateStore;

use super::error::StreamExecutorError;
use super::{expect_first_barrier, BoxedExecutor, Executor, ExecutorInfo, Message};
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
        // let mut upstream = self.upstream.execute();

        let mut upstream = self
            .upstream
            .execute()
            .map(move |result| result.map(|msg| mapping(&upstream_indices.clone(), msg)));

        // 1. Poll the upstream to get the first barrier.
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let create_epoch = first_barrier.epoch;

        // If the barrier is a conf change of creating this mview, init snapshot from its epoch
        // and begin to consume the snapshot.
        // Otherwise, it means we've recovered and the snapshot is already consumed.
        let to_consume_snapshot = first_barrier.is_add_dispatcher(self.actor_id);

        // The first barrier message should be propagated.
        yield Message::Barrier(first_barrier.clone());

        if to_consume_snapshot {
            let mut epoch = create_epoch.prev;

            loop {
                let batch_size = 1000;
                let mut fetch_row_count = 0;
                let mut snapshot = Box::pin(Self::snapshot_read(
                    &self.table,
                    epoch,
                    self.current_pos.clone(),
                ));

                println!(
                    "Actor: {:?}. Start snapshot_read with current_pos = {:?}",
                    &self.actor_id, &self.current_pos
                );
                #[for_await]
                for message in &mut snapshot {
                    match message? {
                        Message::Chunk(chunk) => {
                            fetch_row_count += chunk.cardinality();
                            self.current_pos = chunk
                                .rows()
                                .last()
                                .unwrap()
                                .1
                                .row_by_indices(&table_pk_indices);
                            println!(
                                "Actor: {:?}. chunk: {}",
                                &self.actor_id,
                                &chunk.to_pretty_string()
                            );
                            yield mapping(&self.upstream_indices, Message::Chunk(chunk));
                            if fetch_row_count > batch_size {
                                break;
                            }
                        }
                        Message::Barrier(_) | Message::Watermark(_) => unreachable!(),
                    }
                }

                println!("Apply upstream message");
                println!("Actor: {:?}. Apply upstream message", &self.actor_id);

                while let Some(message) = upstream.next().await {
                    match message? {
                        Message::Chunk(chunk) => {
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
                        Message::Barrier(barrier) => {
                            epoch = barrier.epoch.prev;
                            let checkpoint = barrier.checkpoint;
                            yield Message::Barrier(barrier);
                            if checkpoint {
                                break;
                            }
                        }
                        Message::Watermark(_) => {
                            todo!("https://github.com/risingwavelabs/risingwave/issues/6042")
                        }
                    }
                }

                self.progress.update(epoch, epoch);

                // End of backFill
                if fetch_row_count < batch_size {
                    println!("Actor: {:?}. End of backFill", &self.actor_id);
                    break;
                }
            }

            let mut finish_on_barrier = |msg: &Message| {
                if let Some(barrier) = msg.as_barrier() {
                    self.progress.finish(barrier.epoch.curr);
                }
            };

            while let Some(message) = upstream.next().await {
                let message: Message = message?;
                finish_on_barrier(&message);
                yield message;
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
