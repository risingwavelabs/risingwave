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
//
use std::fmt::Debug;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{ArrayBuilderImpl, ArrayImpl, ArrayRef, Op, Row, StreamChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::Result;
use risingwave_common::types::Datum;
use risingwave_storage::keyspace::Segment;
use risingwave_storage::{Keyspace, StateStore};
use static_assertions::const_assert_eq;

use super::AggCall;
use crate::executor::managed_state::aggregation::ManagedStateImpl;
use crate::executor::{Barrier, Executor, ExecutorState, Message, PkDataTypes, StatefulExecutor};

/// States for [`SimpleAggExecutor`] and [`HashAggExecutor`].
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
const ROW_COUNT_COLUMN: usize = 0;

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
    /// that for [`HashAggExecutor`].
    ///
    /// Returns how many rows are appended in buidlers.
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

/// Trait for [`SimpleAggExecutor`] and [`HashAggExecutor`], providing an implementaion of
/// [`Executor::next`] by [`agg_executor_next`].
#[async_trait]
pub trait AggExecutor: StatefulExecutor {
    /// If exists, we should send a Barrier while next called.
    fn cached_barrier_message_mut(&mut self) -> &mut Option<Barrier>;

    /// Apply the chunk to the dirty state.
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> Result<()>;

    /// Flush the buffered chunk to the storage backend, and get the edits of the states. If there's
    /// no dirty states to flush, return `Ok(None)`.
    async fn flush_data(&mut self) -> Result<Option<StreamChunk>>;

    fn input(&mut self) -> &mut dyn Executor;
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

/// An implementaion of [`Executor::next`] for [`AggExecutor`].
pub async fn agg_executor_next<E: AggExecutor>(executor: &mut E) -> Result<Message> {
    if let Some(barrier) = std::mem::take(executor.cached_barrier_message_mut()) {
        return Ok(Message::Barrier(barrier));
    }

    loop {
        let msg = executor.input().next().await?;
        if executor.try_init_executor(&msg).is_some() {
            // Pass through the first msg directly after initializing the executor
            return Ok(msg);
        }
        match msg {
            Message::Chunk(chunk) => executor.apply_chunk(chunk).await?,
            Message::Barrier(barrier) if barrier.is_stop_mutation() => {
                return Ok(Message::Barrier(barrier));
            }
            Message::Barrier(barrier) => {
                let epoch = barrier.epoch.curr;
                // TODO: handle epoch rollback, and set cached_barrier_message.
                return if let Some(chunk) = executor.flush_data().await? {
                    // Cache the barrier_msg and send it later.
                    *executor.cached_barrier_message_mut() = Some(barrier);
                    executor.update_executor_state(ExecutorState::Active(epoch));
                    Ok(Message::Chunk(chunk))
                } else {
                    // No fresh data need to flush, just forward the barrier.
                    executor.update_executor_state(ExecutorState::Active(epoch));
                    Ok(Message::Barrier(barrier))
                };
            }
        }
    }
}

/// Generate [`HashAgg`]'s schema from `input`, `agg_calls` and `group_key_indices`. For
/// [`HashAggExecutor`], the group key indices should be provided.
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

/// Generate initial [`AggState`] from `agg_calls`. For [`HashAggExecutor`], the group key should be
/// provided.
pub async fn generate_agg_state<S: StateStore>(
    key: Option<&Row>,
    agg_calls: &[AggCall],
    keyspace: &Keyspace<S>,
    pk_data_types: PkDataTypes,
    epoch: u64,
) -> Result<AggState<S>> {
    let mut managed_states = vec![];

    // Currently the loop here only works if `ROW_COUNT_COLUMN` is 0.
    const_assert_eq!(ROW_COUNT_COLUMN, 0);
    let mut row_count = None;

    for (idx, agg_call) in agg_calls.iter().enumerate() {
        // TODO: in pure in-memory engine, we should not do this serialization.

        // The prefix of the state is <(group key) / state id />
        let keyspace = {
            let mut ks = keyspace.clone();
            if let Some(key) = key {
                let bytes = key.serialize().unwrap();
                ks.push(Segment::VariantLength(bytes));
            }
            ks.push(Segment::u16(idx as u16));
            ks
        };

        let mut managed_state = ManagedStateImpl::create_managed_state(
            agg_call.clone(),
            keyspace,
            row_count,
            pk_data_types.clone(),
            idx == ROW_COUNT_COLUMN,
        )
        .await?;

        if idx == ROW_COUNT_COLUMN {
            // For the rowcount state, we should record the rowcount.
            let output = managed_state.get_output(epoch).await?;
            row_count = Some(output.as_ref().map(|x| *x.as_int64() as usize).unwrap_or(0));
        }

        managed_states.push(managed_state);
    }

    Ok(AggState {
        managed_states,
        prev_states: None,
    })
}
