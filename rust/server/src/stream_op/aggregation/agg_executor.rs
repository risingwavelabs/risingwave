use async_trait::async_trait;
use bytes::BufMut;
use risingwave_common::{
    array::{column::Column, ArrayBuilderImpl, ArrayImpl, Op, Row, StreamChunk},
    catalog::{Field, Schema},
    error::Result,
    types::Datum,
};
use static_assertions::const_assert_eq;

use crate::stream_op::{
    state_aggregation::ManagedStateImpl, Barrier, Executor, Keyspace, Message, StateStore,
};

use super::AggCall;

/// Hash key for [`HashAggExecutor`].
pub type HashKey = Row;

/// States for [`SimpleAggExecutor`] and [`HashAggExecutor`].
pub struct AggState<S: StateStore> {
    /// Current managed states for all [`AggCall`]s.
    pub managed_states: Vec<ManagedStateImpl<S>>,

    /// Previous outputs of managed states. Initializing with `None`.
    pub prev_states: Option<Vec<Datum>>,
}

/// We assume the first state of aggregation is always `StreamingRowCountAgg`.
const ROW_COUNT_COLUMN: usize = 0;

impl<S: StateStore> AggState<S> {
    pub async fn row_count(&mut self) -> Result<i64> {
        Ok(self.managed_states[ROW_COUNT_COLUMN]
            .get_output()
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
    pub async fn may_mark_as_dirty(&mut self) -> Result<()> {
        if self.is_dirty() {
            return Ok(());
        }

        let mut outputs = vec![];
        for state in &mut self.managed_states {
            outputs.push(state.get_output().await?);
        }
        self.prev_states = Some(outputs);
        Ok(())
    }

    /// Build changes into `builders` and `new_ops`, according to previous and current states. Note
    /// that for [`HashAggExecutor`], a key should be passed in to build group key columns.
    /// Returns whether this state is empty (and may be deleted).
    pub async fn build_changes(
        &mut self,
        builders: &mut [ArrayBuilderImpl],
        new_ops: &mut Vec<Op>,
        key: Option<&HashKey>,
    ) -> Result<bool> {
        if !self.is_dirty() {
            return Ok(false);
        }

        let row_count = self.row_count().await?;
        let prev_row_count = self.prev_row_count();

        // First several columns are used for group keys in HashAgg.
        let agg_call_offset = key.map(|k| k.0.len()).unwrap_or_default();

        match (prev_row_count, row_count) {
            (0, 0) => {
                // previous state is empty, current state is also empty.
                // FIXME: for `SimpleAgg`, should we still build some changes when `row_count` is 0
                // while other aggs may not be `0`?
            }

            (0, _) => {
                // previous state is empty, current state is not empty, insert one `Insert` op.
                new_ops.push(Op::Insert);

                if let Some(key) = key {
                    for (builder, datum) in builders.iter_mut().zip(key.0.iter()) {
                        builder.append_datum(datum)?;
                    }
                }

                for (builder, state) in builders[agg_call_offset..]
                    .iter_mut()
                    .zip(self.managed_states.iter_mut())
                {
                    builder.append_datum(&state.get_output().await?)?;
                }
            }

            (_, 0) => {
                // previous state is not empty, current state is empty, insert one `Delete` op.
                new_ops.push(Op::Delete);

                if let Some(key) = key {
                    for (builder, datum) in builders.iter_mut().zip(key.0.iter()) {
                        builder.append_datum(datum)?;
                    }
                }

                for (builder, state) in builders[agg_call_offset..]
                    .iter_mut()
                    .zip(self.prev_states.as_ref().unwrap().iter())
                {
                    builder.append_datum(state)?;
                }
            }

            _ => {
                // previous state is not empty, current state is not empty, insert two `Update` op.
                new_ops.push(Op::UpdateDelete);
                new_ops.push(Op::UpdateInsert);

                if let Some(key) = key {
                    for (builder, datum) in builders.iter_mut().zip(key.0.iter()) {
                        builder.append_datum(datum)?;
                        builder.append_datum(datum)?;
                    }
                }

                for (builder, prev_state, cur_state) in itertools::multizip((
                    builders[agg_call_offset..].iter_mut(),
                    self.prev_states.as_ref().unwrap().iter(),
                    self.managed_states.iter_mut(),
                )) {
                    builder.append_datum(prev_state)?;
                    builder.append_datum(&cur_state.get_output().await?)?;
                }
            }
        }

        // unmark dirty
        self.prev_states = None;

        let empty = row_count == 0;
        Ok(empty)
    }
}

/// Trait for [`SimpleAggExecutor`] and [`HashAggExecutor`], providing an implementaion of
/// [`Executor::next`] by [`agg_executor_next`].
#[async_trait]
pub trait AggExecutor: Executor {
    /// If exists, we should send a Barrier while next called.
    fn cached_barrier_message_mut(&mut self) -> &mut Option<Barrier>;

    /// Apply the chunk to the dirty state.
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> Result<()>;

    /// Flush the buffered chunk to the storage backend, and get the edits of the states. If there's
    /// no dirty states to flush, return `Ok(None)`.
    async fn flush_data(&mut self) -> Result<Option<StreamChunk>>;

    fn input(&mut self) -> &mut dyn Executor;
}

/// Get aggregation inputs by `agg_calls` and `columns`.
pub fn agg_input_arrays<'a>(
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
        match msg {
            Message::Chunk(chunk) => executor.apply_chunk(chunk).await?,
            Message::Barrier(barrier) if barrier.stop => {
                return Ok(Message::Barrier(barrier));
            }
            Message::Barrier(barrier) => {
                if let Some(chunk) = executor.flush_data().await? {
                    // Cache the barrier_msg and send it later.
                    *executor.cached_barrier_message_mut() = Some(barrier);
                    return Ok(Message::Chunk(chunk));
                } else {
                    // No fresh data need to flush, just forward the barrier.
                    return Ok(Message::Barrier(barrier));
                }
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
    let aggs = agg_calls.iter().map(|agg| Field {
        data_type: agg.return_type.clone(),
    });

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
    key: Option<&HashKey>,
    agg_calls: &[AggCall],
    keyspace: &Keyspace<S>,
    pk_length: usize,
) -> Result<AggState<S>> {
    let mut managed_states = vec![];

    // Currently the loop here only works if `ROW_COUNT_COLUMN` is 0.
    const_assert_eq!(ROW_COUNT_COLUMN, 0);
    let mut row_count = None;

    for (idx, agg_call) in agg_calls.iter().enumerate() {
        // TODO: in pure in-memory engine, we should not do this serialization.
        let mut encoded_group_key = key
            .map(|k| {
                let mut key = k.serialize().unwrap();
                key.push(b'/');
                key
            })
            .unwrap_or_default();
        encoded_group_key.put_u16(idx as u16);
        encoded_group_key.push(b'/');

        // The prefix of the state is <(group key) / state id />
        let keyspace = keyspace.keyspace(&encoded_group_key);
        let mut managed_state = ManagedStateImpl::create_managed_state(
            agg_call.clone(),
            keyspace,
            row_count,
            pk_length,
            idx == ROW_COUNT_COLUMN,
        )
        .await?;

        if idx == ROW_COUNT_COLUMN {
            // For the rowcount state, we should record the rowcount.
            let output = managed_state.get_output().await?;
            row_count = Some(output.as_ref().map(|x| *x.as_int64() as usize).unwrap_or(0));
        }

        managed_states.push(managed_state);
    }

    Ok(AggState {
        managed_states,
        prev_states: None,
    })
}
