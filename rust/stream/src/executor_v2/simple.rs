use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};

use super::{BoxedExecutor, BoxedMessageStream, Executor, Message, PkIndicesRef, StreamChunk};

pub trait SimpleExecutor: Send + 'static {
    /// convert a single chunk to another chunk.
    fn map_chunk(&mut self, chunk: StreamChunk) -> Result<StreamChunk>;

    /// See [`super::Executor::schema`].
    fn schema(&self) -> &Schema;

    /// See [`super::Executor::pk_indices`].
    fn pk_indices(&self) -> PkIndicesRef;

    /// See [`super::Executor::identity`].
    fn identity(&self) -> &str;
}

pub struct SimpleExecutorWrapper<E> {
    input: BoxedExecutor,
    inner: E,
}

impl<E> Executor for SimpleExecutorWrapper<E>
where
    E: SimpleExecutor,
{
    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef {
        self.inner.pk_indices()
    }

    fn identity(&self) -> &str {
        self.inner.identity()
    }

    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<E> SimpleExecutorWrapper<E>
where
    E: SimpleExecutor,
{
    #[try_stream(ok = Message, error = RwError)]
    async fn execute_inner(self) {
        let input = self.input.execute();
        let mut inner = self.inner;
        #[for_await]
        for msg in input {
            let msg = msg?;
            yield match msg {
                Message::Chunk(chunk) => Message::Chunk(inner.map_chunk(chunk)?),
                m => m,
            }
        }
    }
}
