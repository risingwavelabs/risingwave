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

mod error;

use error::StreamExecutorResult;
use futures::stream::BoxStream;
pub use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;

pub use super::executor::{
    Barrier, Executor as ExecutorV1, Message, Mutation, PkIndices, PkIndicesRef,
};

pub mod aggregation;
mod batch_query;
#[allow(dead_code)]
mod chain;
mod filter;
mod global_simple_agg;
mod hash_agg;
mod hop_window;
mod local_simple_agg;
mod lookup;
pub mod merge;
pub(crate) mod mview;
mod project;
mod rearranged_chain;
pub mod receiver;
mod simple;
#[cfg(test)]
mod test_utils;
mod top_n;
mod top_n_appendonly;
mod top_n_executor;
mod v1_compat;

pub use batch_query::BatchQueryExecutor;
pub use filter::FilterExecutor;
pub use global_simple_agg::SimpleAggExecutor;
pub use hash_agg::HashAggExecutor;
pub use hop_window::HopWindowExecutor;
pub use local_simple_agg::LocalSimpleAggExecutor;
pub use lookup::*;
pub use merge::MergeExecutor;
pub use mview::*;
pub use project::ProjectExecutor;
pub use rearranged_chain::RearrangedChainExecutor as ChainExecutor;
pub(crate) use simple::{SimpleExecutor, SimpleExecutorWrapper};
pub use top_n::TopNExecutor;
pub use top_n_appendonly::AppendOnlyTopNExecutor;
pub use v1_compat::{ExecutorV1AsV2, StreamExecutorV1};

pub type BoxedExecutor = Box<dyn Executor>;
pub type BoxedMessageStream = BoxStream<'static, StreamExecutorResult<Message>>;
pub type MessageStreamItem = StreamExecutorResult<Message>;
pub trait MessageStream = futures::Stream<Item = MessageStreamItem> + Send;

/// The maximum chunk length produced by executor at a time.
const PROCESSING_WINDOW_SIZE: usize = 1024;

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

    fn execute_with_epoch(self: Box<Self>, _epoch: u64) -> BoxedMessageStream {
        self.execute()
    }

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

    /// Return an executor which implements [`ExecutorV1`].
    fn v1(self: Box<Self>) -> StreamExecutorV1
    where
        Self: Sized,
    {
        let info = self.info();
        let stream = self.execute();
        let stream = Box::pin(stream);

        StreamExecutorV1 {
            executor_v2: None,
            stream: Some(stream),
            info,
        }
    }

    /// Return an executor which implements [`ExecutorV1`] and requires [`ExecutorV1::init`] to be
    /// called before executing.
    fn v1_uninited(self: Box<Self>) -> StreamExecutorV1
    where
        Self: Sized,
    {
        let info = self.info();

        StreamExecutorV1 {
            executor_v2: Some(self),
            stream: None,
            info,
        }
    }

    fn boxed(self) -> BoxedExecutor
    where
        Self: Sized + Send + 'static,
    {
        Box::new(self)
    }
}
