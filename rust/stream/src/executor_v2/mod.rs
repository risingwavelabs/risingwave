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

use futures::stream::BoxStream;
pub use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

pub use super::executor::{
    Barrier, Executor as ExecutorV1, Message, Mutation, PkIndices, PkIndicesRef,
};

mod filter;
mod simple;
#[cfg(test)]
mod test_utils;
mod v1_compact;

pub use filter::FilterExecutor;
pub(crate) use simple::{SimpleExecutor, SimpleExecutorWrapper};
pub use v1_compact::StreamExecutorV1;

pub type BoxedExecutor = Box<dyn Executor>;
pub type BoxedMessageStream = BoxStream<'static, Result<Message>>;

/// Static information of an executor.
#[derive(Debug)]
pub struct ExecutorInfo {
    /// See [`Executor::schema`].
    pub schema: Schema,

    /// See [`Executor::pk_indices`].
    pub pk_indices: PkIndices,

    /// See [`Executor::identity`].
    pub identity: String,
}

/// `Executor` supports handling of control messages.
pub trait Executor: Send + 'static {
    fn execute(self: Box<Self>) -> BoxedMessageStream;

    /// Return the schema of the OUTPUT of the executor.
    fn schema(&self) -> &Schema;

    /// Return the primary key indices of the OUTPUT of the executor.
    /// Schema is used by both OLAP and streaming, therefore
    /// pk indices are maintained independently.
    fn pk_indices(&self) -> PkIndicesRef;

    /// Identity of the executor.
    fn identity(&self) -> &str;

    #[inline(always)]
    fn info(&self) -> ExecutorInfo {
        let schema = self.schema().to_owned();
        let pk_indices = self.pk_indices().to_owned();
        let identity = self.identity().to_owned();
        ExecutorInfo {
            schema,
            pk_indices,
            identity,
        }
    }

    /// Return an Executor which satisfied [`ExecutorV1`].
    fn v1(self: Box<Self>) -> StreamExecutorV1
    where
        Self: Sized,
    {
        let info = self.info();
        let stream = self.execute();
        let stream = Box::pin(stream);

        StreamExecutorV1 { stream, info }
    }
}
