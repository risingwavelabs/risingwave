// Copyright 2025 RisingWave Labs
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

// Re-export everything from submodules
pub mod agg_executor;
pub mod hash_join_executor;
mod mock_source;
pub mod top_n_executor;

use async_trait::async_trait;
use futures::{FutureExt, StreamExt, TryStreamExt};
use futures_async_stream::try_stream;
pub use mock_source::*;
use risingwave_common::catalog::Schema;
use risingwave_common::types::{DataType, ScalarImpl};
use risingwave_common::util::epoch::{EpochExt, test_epoch};
use tokio::sync::mpsc;

use super::error::StreamExecutorError;
use super::{
    Barrier, BoxedMessageStream, Execute, Executor, ExecutorInfo, Message, MessageStream,
    StreamChunk, StreamExecutorResult, Watermark,
};

// Keep the prelude module here since it imports from multiple submodules
pub mod prelude {
    pub use std::sync::Arc;
    pub use std::sync::atomic::AtomicU64;

    pub use risingwave_common::array::StreamChunk;
    pub use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    pub use risingwave_common::test_prelude::StreamChunkTestExt;
    pub use risingwave_common::types::DataType;
    pub use risingwave_common::util::sort_util::OrderType;
    pub use risingwave_storage::StateStore;
    pub use risingwave_storage::memory::MemoryStateStore;

    pub use crate::common::table::state_table::StateTable;
    pub use crate::executor::test_utils::expr::build_from_pretty;
    pub use crate::executor::test_utils::{MessageSender, MockSource, StreamExecutorTestExt};
    pub use crate::executor::{ActorContext, BoxedMessageStream, Execute, PkIndices};
}

/// Trait for testing `StreamExecutor` more easily.
///
/// With `next_unwrap_ready`, we can retrieve the next message from the executor without `await`ing,
/// so that we can immediately panic if the executor is not ready instead of getting stuck. This is
/// useful for testing.
#[async_trait]
pub trait StreamExecutorTestExt: MessageStream + Unpin {
    /// Asserts that the executor is pending (not ready) now.
    ///
    /// Panics if it is ready.
    fn next_unwrap_pending(&mut self) {
        if let Some(r) = self.try_next().now_or_never() {
            panic!("expect pending stream, but got `{:?}`", r);
        }
    }

    /// Asserts that the executor is ready now, returning the next message.
    ///
    /// Panics if it is pending.
    fn next_unwrap_ready(&mut self) -> StreamExecutorResult<Message> {
        match self.next().now_or_never() {
            Some(Some(r)) => r,
            Some(None) => panic!("expect ready stream, but got terminated"),
            None => panic!("expect ready stream, but got pending"),
        }
    }

    /// Asserts that the executor is ready on a [`StreamChunk`] now, returning the next chunk.
    ///
    /// Panics if it is pending or the next message is not a [`StreamChunk`].
    fn next_unwrap_ready_chunk(&mut self) -> StreamExecutorResult<StreamChunk> {
        self.next_unwrap_ready()
            .map(|msg| msg.into_chunk().expect("expect chunk"))
    }

    /// Asserts that the executor is ready on a [`Barrier`] now, returning the next barrier.
    ///
    /// Panics if it is pending or the next message is not a [`Barrier`].
    fn next_unwrap_ready_barrier(&mut self) -> StreamExecutorResult<Barrier> {
        self.next_unwrap_ready()
            .map(|msg| msg.into_barrier().expect("expect barrier"))
    }

    /// Asserts that the executor is ready on a [`Watermark`] now, returning the next barrier.
    ///
    /// Panics if it is pending or the next message is not a [`Watermark`].
    fn next_unwrap_ready_watermark(&mut self) -> StreamExecutorResult<Watermark> {
        self.next_unwrap_ready()
            .map(|msg| msg.into_watermark().expect("expect watermark"))
    }

    async fn expect_barrier(&mut self) -> Barrier {
        let msg = self.next().await.unwrap().unwrap();
        msg.into_barrier().unwrap()
    }

    async fn expect_chunk(&mut self) -> StreamChunk {
        let msg = self.next().await.unwrap().unwrap();
        msg.into_chunk().unwrap()
    }

    async fn expect_watermark(&mut self) -> Watermark {
        let msg = self.next().await.unwrap().unwrap();
        msg.into_watermark().unwrap()
    }
}

// FIXME: implement on any `impl MessageStream` if the analyzer works well.
impl StreamExecutorTestExt for BoxedMessageStream {}

/// `row_nonnull` builds a `OwnedRow` with concrete values.
/// TODO: add macro row!, which requires a new trait `ToScalarValue`.
#[macro_export]
macro_rules! row_nonnull {
    [$( $value:expr ),*] => {
        {
            risingwave_common::row::OwnedRow::new(vec![$(Some($value.into()), )*])
        }
    };
}

pub mod expr {
    use risingwave_expr::expr::NonStrictExpression;

    pub fn build_from_pretty(s: impl AsRef<str>) -> NonStrictExpression {
        NonStrictExpression::for_test(risingwave_expr::expr::build_from_pretty(s))
    }
}
