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
