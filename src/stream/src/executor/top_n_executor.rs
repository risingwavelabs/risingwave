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

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{Op, Row, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;

use super::expect_first_barrier;
use crate::executor::error::{StreamExecutorError, StreamExecutorResult};
use crate::executor::{BoxedExecutor, BoxedMessageStream, Executor, Message, PkIndicesRef};

#[async_trait]
pub trait TopNExecutorBase: Send + 'static {
    /// Apply the chunk to the dirty state and get the diffs.
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<StreamChunk>;

    /// Flush the buffered chunk to the storage backend.
    async fn flush_data(&mut self, epoch: u64) -> StreamExecutorResult<()>;

    /// See [`Executor::schema`].
    fn schema(&self) -> &Schema;

    /// See [`Executor::pk_indices`].
    fn pk_indices(&self) -> PkIndicesRef<'_>;

    /// See [`Executor::identity`].
    fn identity(&self) -> &str;

    /// Update the vnode bitmap for the state tables, only used by Group Top-N since it's
    /// distributed.
    fn update_state_table_vnode_bitmap(&mut self, _vnode_bitmap: Arc<Bitmap>) {}

    async fn init(&mut self, epoch: u64) -> StreamExecutorResult<()>;
}

/// The struct wraps a [`TopNExecutorBase`]
pub struct TopNExecutorWrapper<E> {
    pub(super) input: BoxedExecutor,
    pub(super) inner: E,
}

impl<E> Executor for TopNExecutorWrapper<E>
where
    E: TopNExecutorBase,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.top_n_executor_execute().boxed()
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        self.inner.pk_indices()
    }

    fn identity(&self) -> &str {
        self.inner.identity()
    }
}

impl<E> TopNExecutorWrapper<E>
where
    E: TopNExecutorBase,
{
    /// We remark that topN executor diffs from aggregate executor as it must output diffs
    /// whenever it applies a batch of input data. Therefore, topN executor flushes data only
    /// instead of computing diffs and flushing when receiving a barrier.
    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub(crate) async fn top_n_executor_execute(mut self: Box<Self>) {
        let mut input = self.input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        self.inner.init(barrier.epoch.prev).await?;

        let mut epoch = barrier.epoch.curr;

        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => yield Message::Chunk(self.inner.apply_chunk(chunk).await?),
                Message::Barrier(barrier) => {
                    self.inner.flush_data(epoch).await?;
                    epoch = barrier.epoch.curr;
                    yield Message::Barrier(barrier)
                }
            };
        }
    }
}

pub(crate) fn generate_output(
    new_rows: Vec<Row>,
    new_ops: Vec<Op>,
    schema: &Schema,
) -> StreamExecutorResult<StreamChunk> {
    if !new_rows.is_empty() {
        let mut data_chunk_builder = DataChunkBuilder::new(schema.data_types(), new_rows.len() + 1);
        for row in &new_rows {
            let res = data_chunk_builder.append_one_row_from_datums(row.0.iter());
            debug_assert!(res.is_none());
        }
        // since `new_rows` is not empty, we unwrap directly
        let new_data_chunk = data_chunk_builder.consume_all().unwrap();
        let new_stream_chunk = StreamChunk::new(new_ops, new_data_chunk.columns().to_vec(), None);
        Ok(new_stream_chunk)
    } else {
        let columns = schema
            .create_array_builders(0)
            .into_iter()
            .map(|x| Column::new(Arc::new(x.finish())))
            .collect_vec();
        Ok(StreamChunk::new(vec![], columns, None))
    }
}
