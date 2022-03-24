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

pub use super::executor::{Executor as ExecutorV1, Message, PkIndices, PkIndicesRef};

mod filter;
mod simple;
mod v1_compact;

pub use filter::SimpleFilterExecutor;
pub use simple::{SimpleExecutor, SimpleExecutorWrapper};
pub use v1_compact::StreamExecutorV1;

pub type BoxedExecutor = Box<dyn Executor>;
pub type BoxedMessageStream = BoxStream<'static, Result<Message>>;

/// Static information of executors.
pub struct ExecutorInfo {
    /// The schema of the executor's output.
    pub schema: Schema,

    /// The primary key indices of the executor's input.
    pub pk_indices: PkIndices,

    /// The identity of the executor.
    pub identity: String,
}

pub trait Executor: Send + 'static {
    fn execute(self: Box<Self>) -> BoxedMessageStream;

    fn schema(&self) -> &Schema;

    fn pk_indices(&self) -> PkIndicesRef;

    fn identity(&self) -> &str;

    /// Return an Executor which satisfied [`ExecutorV1`].
    fn v1(self: Box<Self>) -> StreamExecutorV1
    where
        Self: Sized,
    {
        let schema = self.schema().to_owned();
        let pk_indices = self.pk_indices().to_owned();
        let identity = self.identity().to_owned();
        let stream = self.execute();
        let stream = Box::pin(stream);

        StreamExecutorV1 {
            stream,
            schema,
            pk_indices,
            identity,
        }
    }
}
