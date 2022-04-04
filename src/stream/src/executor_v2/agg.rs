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
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::{Field, Schema};

use crate::executor::{AggCall, ExecutorState};
use crate::executor_v2::error::{StreamExecutorResult, TracedStreamExecutorError};
use crate::executor_v2::{
    BoxedExecutor, BoxedMessageStream, Executor, Message, PkIndicesRef, StatefulExecutor,
};

/// Trait for [`crate::executor_v2::LocalSimpleAggExecutor`], providing
/// an implementation of [`Executor::execute`] by [`AggExecutorWrapper::agg_executor_execute`].
pub trait AggExecutor: StatefulExecutor {
    /// Apply the chunk to the dirty state.
    fn map_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<()>;

    /// Flush the buffered chunk to the storage backend, and get the edits of the states. If there's
    /// no dirty states to flush, return `Ok(None)`.
    fn flush_data(&mut self) -> StreamExecutorResult<Option<StreamChunk>>;
}

/// The struct wraps a [`AggExecutor`]
pub struct AggExecutorWrapper<E> {
    pub(super) input: BoxedExecutor,
    pub(super) inner: E,
}

impl<E> Executor for AggExecutorWrapper<E>
where
    E: AggExecutor,
{
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.agg_executor_execute().boxed()
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

impl<E> StatefulExecutor for AggExecutorWrapper<E>
where
    E: AggExecutor,
{
    fn executor_state(&self) -> &ExecutorState {
        self.inner.executor_state()
    }

    fn update_executor_state(&mut self, new_state: ExecutorState) {
        self.inner.update_executor_state(new_state)
    }
}

impl<E> AggExecutor for AggExecutorWrapper<E>
where
    E: AggExecutor,
{
    fn map_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<()> {
        self.inner.map_chunk(chunk)
    }

    fn flush_data(&mut self) -> StreamExecutorResult<Option<StreamChunk>> {
        self.inner.flush_data()
    }
}

impl<E> AggExecutorWrapper<E>
where
    E: AggExecutor,
{
    /// An implementation of [`Executor::execute`] for [`AggExecutor`].
    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    pub(crate) async fn agg_executor_execute(mut self: Box<Self>) {
        let input = self.input.execute();
        #[for_await]
        for msg in input {
            let msg = msg?;
            if self.inner.try_init_executor(&msg).is_some() {
                // Pass through the first msg directly after initializing theself
                yield msg;
            } else {
                match msg {
                    Message::Chunk(chunk) => self.inner.map_chunk(chunk)?,
                    Message::Barrier(barrier) if barrier.is_stop_mutation() => {
                        yield Message::Barrier(barrier);
                        break;
                    }
                    Message::Barrier(barrier) => {
                        let epoch = barrier.epoch.curr;
                        if let Some(chunk) = self.inner.flush_data()? {
                            // Cache the barrier_msg and send it later.
                            self.inner
                                .update_executor_state(ExecutorState::Active(epoch));
                            yield Message::Chunk(chunk);
                            yield Message::Barrier(barrier);
                        } else {
                            // No fresh data need to flush, just forward the barrier.
                            self.inner
                                .update_executor_state(ExecutorState::Active(epoch));
                            yield Message::Barrier(barrier)
                        };
                    }
                }
            }
        }
    }
}

/// Generate [`crate::executor::HashAggExecutor`]'s schema from `input`, `agg_calls` and
/// `group_key_indices`. For [`crate::executor::HashAggExecutor`], the group key indices should be
/// provided.
pub fn generate_agg_schema(
    input: &dyn Executor,
    agg_calls: &[AggCall],
    group_key_indices: Option<&[usize]>,
) -> Schema {
    let aggs = agg_calls
        .iter()
        .map(|agg| Field::unnamed(agg.return_type.clone()));

    let fields = if let Some(key_indices) = group_key_indices {
        let keys = key_indices
            .iter()
            .map(|idx| input.schema().fields[*idx].clone());

        keys.chain(aggs).collect()
    } else {
        aggs.collect()
    };

    Schema { fields }
}
