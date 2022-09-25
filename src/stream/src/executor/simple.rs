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

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;

use super::error::{StreamExecutorError, StreamExecutorResult};
use super::{BoxedExecutor, BoxedMessageStream, Executor, Message, PkIndicesRef, StreamChunk};

/// Executor which can handle [`StreamChunk`]s one by one.
pub trait SimpleExecutor: Send + 'static {
    /// convert a single chunk to zero or one chunks.
    fn map_filter_chunk(&mut self, chunk: StreamChunk)
        -> StreamExecutorResult<Option<StreamChunk>>;

    /// See [`super::Executor::schema`].
    fn schema(&self) -> &Schema;

    /// See [`super::Executor::pk_indices`].
    fn pk_indices(&self) -> PkIndicesRef<'_>;

    /// See [`super::Executor::identity`].
    fn identity(&self) -> &str;
}

/// The struct wraps a [`SimpleExecutor`], and implements the interface of [`Executor`].
pub struct SimpleExecutorWrapper<E> {
    pub(super) input: BoxedExecutor,
    pub(super) inner: E,
}

impl<E> Executor for SimpleExecutorWrapper<E>
where
    E: SimpleExecutor,
{
    fn schema(&self) -> &Schema {
        self.inner.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
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
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let input = self.input.execute();
        let mut inner = self.inner;
        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => match inner.map_filter_chunk(chunk)? {
                    Some(new_chunk) => yield Message::Chunk(new_chunk),
                    None => continue,
                },
                m => yield m,
            }
        }
    }
}
