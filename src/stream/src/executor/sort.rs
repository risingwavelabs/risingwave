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

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::Op;
use risingwave_common::catalog::Schema;
use risingwave_storage::StateStore;

use super::aggregation::ChunkBuilder;
use super::sort_buffer::SortBuffer;
use super::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor,
    ExecutorInfo, Message, PkIndices, PkIndicesRef, StreamExecutorError, Watermark,
};
use crate::common::table::state_table::StateTable;

pub struct SortExecutor<S: StateStore> {
    input: BoxedExecutor,
    inner: ExecutorInner<S>,
}

pub struct SortExecutorArgs<S: StateStore> {
    pub input: BoxedExecutor,

    pub actor_ctx: ActorContextRef,
    pub pk_indices: PkIndices,
    pub executor_id: u64,

    pub buffer_table: StateTable<S>,
    pub chunk_size: usize,
    pub sort_column_index: usize,
}

struct ExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    buffer_table: StateTable<S>,
    chunk_size: usize,
    sort_column_index: usize,
}

struct ExecutionVars<S: StateStore> {
    buffer: SortBuffer<S>,
    buffer_changed: bool,
}

impl<S: StateStore> Executor for SortExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.executor_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.inner.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.inner.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.inner.info.identity
    }
}

impl<S: StateStore> SortExecutor<S> {
    pub fn new(args: SortExecutorArgs<S>) -> Self {
        let schema = args.input.schema().clone();
        Self {
            input: args.input,
            inner: ExecutorInner {
                actor_ctx: args.actor_ctx,
                info: ExecutorInfo {
                    identity: format!("SortExecutor {:X}", args.executor_id),
                    schema,
                    pk_indices: args.pk_indices,
                },
                buffer_table: args.buffer_table,
                chunk_size: args.chunk_size,
                sort_column_index: args.sort_column_index,
            },
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn executor_inner(self) {
        let Self {
            input,
            inner: mut this,
        } = self;

        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        this.buffer_table.init_epoch(barrier.epoch);
        yield Message::Barrier(barrier);

        let mut vars = ExecutionVars {
            buffer: SortBuffer::new(this.sort_column_index, &this.buffer_table),
            buffer_changed: false,
        };

        #[for_await]
        for msg in input {
            match msg? {
                Message::Watermark(watermark @ Watermark { col_idx, .. })
                    if col_idx == this.sort_column_index =>
                {
                    let mut chunk_builder =
                        ChunkBuilder::new(this.chunk_size, &this.info.schema.data_types());

                    #[for_await]
                    for row in vars
                        .buffer
                        .consume(watermark.val.clone(), &mut this.buffer_table)
                    {
                        let row = row?;
                        if let Some(chunk) = chunk_builder.append_row(Op::Insert, row) {
                            yield Message::Chunk(chunk);
                        }
                    }
                    if let Some(chunk) = chunk_builder.take() {
                        yield Message::Chunk(chunk);
                    }
                    vars.buffer_changed = true;

                    yield Message::Watermark(watermark);
                }
                Message::Watermark(watermark) => yield Message::Watermark(watermark),
                Message::Chunk(chunk) => {
                    vars.buffer.apply_chunk(chunk, &mut this.buffer_table);
                    vars.buffer_changed = true;
                }
                Message::Barrier(barrier) => {
                    if vars.buffer_changed {
                        this.buffer_table.commit(barrier.epoch).await?;
                    } else {
                        this.buffer_table.commit_no_data_expected(barrier.epoch);
                    }
                    vars.buffer_changed = false;

                    // Update the vnode bitmap for state tables of all agg calls if asked.
                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(this.actor_ctx.id) {
                        let (_, cache_may_stale) =
                            this.buffer_table.update_vnode_bitmap(vnode_bitmap);

                        // Manipulate the cache if necessary.
                        if cache_may_stale {
                            vars.buffer.clear_cache();
                        }
                    }

                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};
    use crate::executor::{ActorContext, BoxedMessageStream, Executor};

    async fn create_executor<S: StateStore>(
        sort_column_index: usize,
        store: S,
    ) -> (MessageSender, BoxedMessageStream) {
        let input_schema = Schema::new(vec![
            Field::unnamed(DataType::Int64), // pk
            Field::unnamed(DataType::Int64),
        ]);
        let input_pk_indices = vec![0];

        // state table schema = input schema
        let table_columns = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
        ];

        // note that the sort column is the first table pk column to ensure ordering
        let table_pk_indices = vec![sort_column_index, 0];
        let table_order_types = vec![OrderType::ascending(), OrderType::ascending()];
        let buffer_table = StateTable::new_without_distribution(
            store,
            TableId::new(1),
            table_columns,
            table_order_types,
            table_pk_indices,
        )
        .await;

        let (tx, source) = MockSource::channel(input_schema, input_pk_indices.clone());
        let sort_executor = SortExecutor::new(SortExecutorArgs {
            input: source.boxed(),
            actor_ctx: ActorContext::create(123),
            pk_indices: input_pk_indices,
            executor_id: 1,
            buffer_table,
            chunk_size: 1024,
            sort_column_index,
        });
        (tx, sort_executor.boxed().execute())
    }

    #[tokio::test]
    async fn test_sort_executor() {
        let sort_column_index = 1;

        let store = MemoryStateStore::new();
        let (mut tx, mut sort_executor) = create_executor(sort_column_index, store).await;

        // Init barrier
        tx.push_barrier(1, false);

        // Consume the barrier
        sort_executor.expect_barrier().await;

        // Init watermark
        tx.push_int64_watermark(0, 0_i64);
        tx.push_int64_watermark(sort_column_index, 0_i64);

        // Consume the watermark
        sort_executor.expect_watermark().await;
        sort_executor.expect_watermark().await;

        // Push data chunk1
        tx.push_chunk(StreamChunk::from_pretty(
            " I I
            + 1 1
            + 2 2
            + 3 6
            + 4 7",
        ));

        // Push watermark1 on an irrelevant column
        tx.push_int64_watermark(0, 3_i64);

        // Consume the watermark
        sort_executor.expect_watermark().await;

        // Push watermark1 on sorted column
        tx.push_int64_watermark(sort_column_index, 3_i64);

        // Consume the data chunk
        let chunk = sort_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 1 1
                + 2 2"
            )
        );

        // Consume the watermark
        sort_executor.expect_watermark().await;

        // Push data chunk2
        tx.push_chunk(StreamChunk::from_pretty(
            " I I
            + 98 4
            + 37 5
            + 60 8",
        ));

        // Push barrier
        tx.push_barrier(2, false);

        // Consume the barrier
        sort_executor.expect_barrier().await;

        // Push watermark2 on an irrelevant column
        tx.push_int64_watermark(0, 7_i64);

        // Consume the watermark
        sort_executor.expect_watermark().await;

        // Push watermark2 on sorted column
        tx.push_int64_watermark(sort_column_index, 7_i64);

        // Consume the data chunk
        let chunk = sort_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 98 4
                + 37 5
                + 3 6"
            )
        );

        // Consume the watermark
        sort_executor.expect_watermark().await;
    }

    #[tokio::test]
    async fn test_sort_executor_fail_over() {
        let sort_column_index = 1;

        let store = MemoryStateStore::new();
        let (mut tx, mut sort_executor) = create_executor(sort_column_index, store.clone()).await;

        // Init barrier
        tx.push_barrier(1, false);

        // Consume the barrier
        sort_executor.expect_barrier().await;

        // Init watermark
        tx.push_int64_watermark(0, 0_i64);
        tx.push_int64_watermark(sort_column_index, 0_i64);

        // Consume the watermark
        sort_executor.expect_watermark().await;
        sort_executor.expect_watermark().await;

        // Push data chunk
        tx.push_chunk(StreamChunk::from_pretty(
            " I I
            + 1 1
            + 2 2
            + 3 6
            + 4 7",
        ));

        // Push barrier
        tx.push_barrier(2, false);

        // Consume the barrier
        sort_executor.expect_barrier().await;

        // Mock fail over
        let (mut recovered_tx, mut recovered_sort_executor) =
            create_executor(sort_column_index, store).await;

        // Push barrier
        recovered_tx.push_barrier(3, false);

        // Consume the barrier
        recovered_sort_executor.expect_barrier().await;

        // Push watermark on sorted column
        recovered_tx.push_int64_watermark(sort_column_index, 3_i64);

        // Consume the data chunk
        let chunk = recovered_sort_executor.expect_chunk().await;
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I
                + 1 1
                + 2 2"
            )
        );

        // Consume the watermark
        recovered_sort_executor.expect_watermark().await;
    }
}
