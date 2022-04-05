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

use std::fmt::Debug;

use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;

use super::error::{StreamExecutorResult, TracedStreamExecutorError};
use super::{BoxedExecutor, BoxedMessageStream, Executor, Message, PkIndicesRef, StreamChunk};

/// Executor which can handle [`StreamChunk`]s one by one.
#[async_trait]
pub trait SimpleExecutor: Send + 'static {
    /// convert a single chunk to another chunk.
    // TODO: How about use `map_filter_chunk` and output chunk optionally?
    async fn map_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<StreamChunk>;

    /// See [`super::Executor::schema`].
    fn schema(&self) -> &Schema;

    /// See [`super::Executor::pk_indices`].
    fn pk_indices(&self) -> PkIndicesRef;

    /// See [`super::Executor::identity`].
    fn identity(&self) -> &str;
}

/// The struct wraps a [`SimpleExecutor`], and implements the interface of [`Executor`].
pub struct SimpleExecutorWrapper<E> {
    pub(super) input: BoxedExecutor,
    pub(super) inner: E,
}

impl<E> std::fmt::Debug for SimpleExecutorWrapper<E>
where
    E: SimpleExecutor + Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl<E> Executor for SimpleExecutorWrapper<E>
where
    E: SimpleExecutor + Debug,
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
    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(self) {
        let input = self.input.execute();
        let mut inner = self.inner;
        #[for_await]
        for msg in input {
            let msg = msg?;
            yield match msg {
                Message::Chunk(chunk) => Message::Chunk(inner.map_chunk(chunk).await?),
                m => m,
            }
        }
    }
}
