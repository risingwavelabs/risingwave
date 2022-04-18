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
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilderImpl, ArrayImpl, ArrayRef, Op, Row, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::hash::HashCode;
use risingwave_common::types::Datum;
use risingwave_storage::{Keyspace, StateStore};
use static_assertions::const_assert_eq;

use super::AggCall;
use crate::executor::managed_state::aggregation::ManagedStateImpl;
use crate::executor::PkDataTypes;
use crate::executor_v2::error::{
    StreamExecutorError, StreamExecutorResult, TracedStreamExecutorError,
};
use crate::executor_v2::{BoxedExecutor, BoxedMessageStream, Executor, Message, PkIndicesRef};

/// States for [`crate::executor_v2::LocalSimpleAggExecutor`],
/// [`crate::executor_v2::SimpleAggExecutor`] and [`crate::executor_v2::HashAggExecutor`].
pub struct AggState<S: StateStore> {
    /// Current managed states for all [`AggCall`]s.
    pub managed_states: Vec<ManagedStateImpl<S>>,

    /// Previous outputs of managed states. Initializing with `None`.
    pub prev_states: Option<Vec<Datum>>,
}

impl<S: StateStore> Debug for AggState<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AggState")
            .field("prev_states", &self.prev_states)
            .finish()
    }
}

/// We assume the first state of aggregation is always `StreamingRowCountAgg`.
pub const ROW_COUNT_COLUMN: usize = 0;

impl<S: StateStore> AggState<S> {
    pub async fn row_count(&mut self, epoch: u64) -> Result<i64> {
        Ok(self.managed_states[ROW_COUNT_COLUMN]
            .get_output(epoch)
            .await?
            .map(|x| *x.as_int64())
            .unwrap_or(0))
    }

    pub fn prev_row_count(&self) -> i64 {
        match &self.prev_states {
            Some(states) => states[ROW_COUNT_COLUMN]
                .as_ref()
                .map(|x| *x.as_int64())
                .unwrap_or(0),
            None => 0,
        }
    }

    /// Returns whether `prev_states` is filled.
    pub fn is_dirty(&self) -> bool {
        self.prev_states.is_some()
    }

    /// Used for recording the output of current states as previous states, before applying new
    /// changes to the state. If the state is already marked dirty in this epoch, this function does
    /// no-op.
    /// After calling this function, `self.is_dirty()` will return `true`.
    pub async fn may_mark_as_dirty(&mut self, epoch: u64) -> Result<()> {
        if self.is_dirty() {
            return Ok(());
        }

        let mut outputs = vec![];
        for state in &mut self.managed_states {
            outputs.push(state.get_output(epoch).await?);
        }
        self.prev_states = Some(outputs);
        Ok(())
    }

    /// Build changes into `builders` and `new_ops`, according to previous and current states. Note
    /// that for [`crate::executor_v2::HashAggExecutor`].
    ///
    /// Returns how many rows are appended in builders.
    pub async fn build_changes(
        &mut self,
        builders: &mut [ArrayBuilderImpl],
        new_ops: &mut Vec<Op>,
        epoch: u64,
    ) -> Result<usize> {
        if !self.is_dirty() {
            return Ok(0);
        }

        let row_count = self.row_count(epoch).await?;
        let prev_row_count = self.prev_row_count();

        trace!(
            "prev_row_count = {}, row_count = {}",
            prev_row_count,
            row_count
        );

        let appended = match (prev_row_count, row_count) {
            (0, 0) => {
                // previous state is empty, current state is also empty.
                // FIXME: for `SimpleAgg`, should we still build some changes when `row_count` is 0
                // while other aggs may not be `0`?

                0
            }

            (0, _) => {
                // previous state is empty, current state is not empty, insert one `Insert` op.
                new_ops.push(Op::Insert);

                for (builder, state) in builders.iter_mut().zip_eq(self.managed_states.iter_mut()) {
                    let data = state.get_output(epoch).await?;
                    trace!("append_datum (0 -> N): {:?}", &data);
                    builder.append_datum(&data)?;
                }

                1
            }

            (_, 0) => {
                // previous state is not empty, current state is empty, insert one `Delete` op.
                new_ops.push(Op::Delete);

                for (builder, state) in builders
                    .iter_mut()
                    .zip_eq(self.prev_states.as_ref().unwrap().iter())
                {
                    trace!("append_datum (N -> 0): {:?}", &state);
                    builder.append_datum(state)?;
                }

                1
            }

            _ => {
                // previous state is not empty, current state is not empty, insert two `Update` op.
                new_ops.push(Op::UpdateDelete);
                new_ops.push(Op::UpdateInsert);

                for (builder, prev_state, cur_state) in itertools::multizip((
                    builders.iter_mut(),
                    self.prev_states.as_ref().unwrap().iter(),
                    self.managed_states.iter_mut(),
                )) {
                    let cur_state = cur_state.get_output(epoch).await?;
                    trace!(
                        "append_datum (N -> N): prev = {:?}, cur = {:?}",
                        prev_state,
                        &cur_state
                    );

                    builder.append_datum(prev_state)?;
                    builder.append_datum(&cur_state)?;
                }

                2
            }
        };

        self.prev_states = None;

        Ok(appended)
    }
}

/// Get clones of aggregation inputs by `agg_calls` and `columns`.
pub fn agg_input_arrays(agg_calls: &[AggCall], columns: &[Column]) -> Vec<Vec<ArrayRef>> {
    agg_calls
        .iter()
        .map(|agg| {
            agg.args
                .val_indices()
                .iter()
                .map(|val_idx| columns[*val_idx].array())
                .collect()
        })
        .collect()
}

/// Get references to aggregation inputs by `agg_calls` and `columns`.
pub fn agg_input_array_refs<'a>(
    agg_calls: &[AggCall],
    columns: &'a [Column],
) -> Vec<Vec<&'a ArrayImpl>> {
    agg_calls
        .iter()
        .map(|agg| {
            agg.args
                .val_indices()
                .iter()
                .map(|val_idx| columns[*val_idx].array_ref())
                .collect()
        })
        .collect()
}

/// Trait for [`crate::executor_v2::LocalSimpleAggExecutor`], providing
/// an implementation of [`Executor::execute`] by [`AggExecutorWrapper::agg_executor_execute`].
#[async_trait]
pub trait AggExecutor: Send + 'static {
    /// Apply the chunk to the dirty state.
    async fn apply_chunk(&mut self, chunk: StreamChunk, epoch: u64) -> StreamExecutorResult<()>;

    /// Flush the buffered chunk to the storage backend, and get the edits of the states. If there's
    /// no dirty states to flush, return `Ok(None)`.
    async fn flush_data(&mut self, epoch: u64) -> StreamExecutorResult<Option<StreamChunk>>;

    /// See [`Executor::schema`].
    fn schema(&self) -> &Schema;

    /// See [`Executor::pk_indices`].
    fn pk_indices(&self) -> PkIndicesRef;

    /// See [`Executor::identity`].
    fn identity(&self) -> &str;
}

/// The struct wraps a [`AggExecutor`]
pub struct AggExecutorWrapper<E> {
    pub(crate) input: BoxedExecutor,
    pub(crate) inner: E,
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
    key_hash_code: Option<HashCode>,
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
            key_hash_code.clone(),
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
