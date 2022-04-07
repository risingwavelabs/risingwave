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

use async_trait::async_trait;
use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::{Row, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_storage::{Keyspace, StateStore};
use static_assertions::const_assert_eq;

use crate::executor::managed_state::aggregation::ManagedStateImpl;
use crate::executor::{AggCall, AggState, PkDataTypes, ROW_COUNT_COLUMN};
use crate::executor_v2::error::{
    StreamExecutorError, StreamExecutorResult, TracedStreamExecutorError,
};
use crate::executor_v2::{BoxedExecutor, BoxedMessageStream, Executor, Message, PkIndicesRef};

/// Trait for [`crate::executor_v2::LocalSimpleAggExecutor`], providing
/// an implementation of [`Executor::execute`] by [`AggExecutorWrapper::agg_executor_execute`].
#[async_trait]
pub trait AggExecutor: Send + 'static {
    /// Apply the chunk to the dirty state.
    async fn apply_chunk(&mut self, chunk: StreamChunk, epoch: u64) -> StreamExecutorResult<()>;

    /// Flush the buffered chunk to the storage backend, and get the edits of the states. If there's
    /// no dirty states to flush, return `Ok(None)`.
    async fn flush_data(&mut self, epoch: u64) -> StreamExecutorResult<Option<StreamChunk>>;
}

/// The struct wraps a [`AggExecutor`]
pub struct AggExecutorWrapper<E> {
    pub(super) input: BoxedExecutor,
    pub(super) inner: E,
}

impl<E> Executor for AggExecutorWrapper<E>
where
    E: AggExecutor + Executor,
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

#[async_trait]
impl<E> AggExecutor for AggExecutorWrapper<E>
where
    E: AggExecutor,
{
    async fn apply_chunk(&mut self, chunk: StreamChunk, epoch: u64) -> StreamExecutorResult<()> {
        self.inner.apply_chunk(chunk, epoch).await
    }

    async fn flush_data(&mut self, epoch: u64) -> StreamExecutorResult<Option<StreamChunk>> {
        self.inner.flush_data(epoch).await
    }
}

impl<E> AggExecutorWrapper<E>
where
    E: AggExecutor,
{
    /// An implementation of [`Executor::execute`] for [`AggExecutor`].
    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    pub(crate) async fn agg_executor_execute(mut self: Box<Self>) {
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
                Message::Chunk(chunk) => self.inner.apply_chunk(chunk, epoch).await?,
                Message::Barrier(barrier) => {
                    let next_epoch = barrier.epoch.curr;
                    if let Some(chunk) = self.inner.flush_data(epoch).await? {
                        assert_eq!(epoch, barrier.epoch.prev);
                        yield Message::Chunk(chunk);
                    }
                    yield Message::Barrier(barrier);
                    epoch = next_epoch;
                }
            }
        }
    }
}

/// Generate [`crate::executor_v2::HashAggExecutor`]'s schema from `input`, `agg_calls` and
/// `group_key_indices`. For [`crate::executor_v2::HashAggExecutor`], the group key indices should
/// be provided.
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

/// Generate initial [`AggState`] from `agg_calls`. For [`crate::executor_v2::HashAggExecutor`], the
/// group key should be provided.
pub async fn generate_agg_state<S: StateStore>(
    key: Option<&Row>,
    agg_calls: &[AggCall],
    keyspace: &Keyspace<S>,
    pk_data_types: PkDataTypes,
    epoch: u64,
) -> StreamExecutorResult<AggState<S>> {
    let mut managed_states = vec![];

    // Currently the loop here only works if `ROW_COUNT_COLUMN` is 0.
    const_assert_eq!(ROW_COUNT_COLUMN, 0);
    let mut row_count = None;

    for (idx, agg_call) in agg_calls.iter().enumerate() {
        // TODO: in pure in-memory engine, we should not do this serialization.

        // The prefix of the state is `agg_call_idx / [group_key]`
        let keyspace = if let Some(key) = key {
            let bytes = key.serialize().unwrap();
            keyspace.append_u16(idx as u16).append(bytes)
        } else {
            keyspace.append_u16(idx as u16)
        };

        let mut managed_state = ManagedStateImpl::create_managed_state(
            agg_call.clone(),
            keyspace,
            row_count,
            pk_data_types.clone(),
            idx == ROW_COUNT_COLUMN,
        )
        .await
        .map_err(StreamExecutorError::agg_state_error)?;

        if idx == ROW_COUNT_COLUMN {
            // For the rowcount state, we should record the rowcount.
            let output = managed_state
                .get_output(epoch)
                .await
                .map_err(StreamExecutorError::agg_state_error)?;
            row_count = Some(output.as_ref().map(|x| *x.as_int64() as usize).unwrap_or(0));
        }

        managed_states.push(managed_state);
    }

    Ok(AggState {
        managed_states,
        prev_states: None,
    })
}
