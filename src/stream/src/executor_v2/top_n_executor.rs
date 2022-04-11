use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;

use crate::executor_v2::error::{StreamExecutorResult, TracedStreamExecutorError};
use crate::executor_v2::{BoxedExecutor, BoxedMessageStream, Executor, Message, PkIndicesRef};

#[async_trait]
pub trait TopNExecutorBase: Send + 'static {
    /// Apply the chunk to the dirty state and get the diffs.
    async fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
        epoch: u64,
    ) -> StreamExecutorResult<StreamChunk>;

    /// Flush the buffered chunk to the storage backend.
    async fn flush_data(&mut self, epoch: u64) -> StreamExecutorResult<()>;
}

/// The struct wraps a [`TopNExecutorBase`]
pub struct TopNExecutorWrapper<E> {
    pub(super) input: BoxedExecutor,
    pub(super) inner: E,
}

impl<E> Executor for TopNExecutorWrapper<E>
where
    E: TopNExecutorBase + Executor,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.top_n_executor_execute().boxed()
    }

    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef {
        self.inner.pk_indices()
    }

    fn identity(&self) -> &str {
        self.inner.identity()
    }
}

#[async_trait]
impl<E> TopNExecutorBase for TopNExecutorWrapper<E>
where
    E: TopNExecutorBase,
{
    async fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
        epoch: u64,
    ) -> StreamExecutorResult<StreamChunk> {
        self.inner.apply_chunk(chunk, epoch).await
    }

    async fn flush_data(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        self.inner.flush_data(epoch).await
    }
}

impl<E> TopNExecutorWrapper<E>
where
    E: TopNExecutorBase,
{
    /// We remark that topN executor diffs from aggregate executor as it must output diffs
    /// whenever it applies a batch of input data. Therefore, topN executor flushes data only
    /// instead of computing diffs and flushing when receiving a barrier.
    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    pub(crate) async fn top_n_executor_execute(mut self: Box<Self>) {
        let mut input = self.input.execute();
        let first_msg = input.next().await.unwrap()?;
        let barrier = first_msg
            .as_barrier()
            .expect("the first message received by agg executor must be a barrier");
        let mut epoch = barrier.epoch.curr;
        yield first_msg;

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    yield Message::Chunk(self.inner.apply_chunk(chunk, epoch).await?)
                }
                Message::Barrier(barrier) => {
                    self.inner.flush_data(epoch).await?;
                    epoch = barrier.epoch.curr;
                    yield Message::Barrier(barrier)
                }
            };
        }
    }
}
