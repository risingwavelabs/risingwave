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
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::aggregation::agg_call_filter_res;
use super::*;
use crate::common::StateTableColumnMapping;
use crate::executor::aggregation::{
    generate_agg_schema, generate_managed_agg_state, AggCall, AggState,
};
use crate::executor::error::StreamExecutorError;
use crate::executor::{BoxedMessageStream, Message, PkIndices};

/// `GlobalSimpleAggExecutor` is the aggregation operator for streaming system.
/// To create an aggregation operator, states and expressions should be passed along the
/// constructor.
///
/// `GlobalSimpleAggExecutor` maintain multiple states together. If there are `n`
/// states and `n` expressions, there will be `n` columns as output.
///
/// As the engine processes data in chunks, it is possible that multiple update
/// messages could consolidate to a single row update. For example, our source
/// emits 1000 inserts in one chunk, and we aggregates count function on that.
/// Current `GlobalSimpleAggExecutor` will only emit one row for a whole chunk.
/// Therefore, we "automatically" implemented a window function inside
/// `GlobalSimpleAggExecutor`.
pub struct GlobalSimpleAggExecutor<S: StateStore> {
    input: Box<dyn Executor>,
    info: ExecutorInfo,
    ctx: ActorContextRef,

    /// Pk indices from input
    input_pk_indices: Vec<usize>,

    /// Schema from input
    input_schema: Schema,

    /// Aggregation states of the current operator.
    /// This is an `Option` and the initial state is built when `Executor::next` is called, since
    /// we may not want `Self::new` to be an `async` function.
    states: Option<AggState<S>>,

    /// An operator will support multiple aggregation calls.
    agg_calls: Vec<AggCall>,

    /// Relational state tables for each aggregation calls.
    state_tables: Vec<StateTable<S>>,

    /// State table column mappings for each aggregation calls,
    state_table_col_mappings: Vec<Arc<StateTableColumnMapping>>,
}

impl<S: StateStore> Executor for GlobalSimpleAggExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

impl<S: StateStore> GlobalSimpleAggExecutor<S> {
    pub fn new(
        ctx: ActorContextRef,
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        pk_indices: PkIndices,
        executor_id: u64,
        mut state_tables: Vec<StateTable<S>>,
        state_table_col_mappings: Vec<Vec<usize>>,
    ) -> Result<Self> {
        let input_info = input.info();
        let schema = generate_agg_schema(input.as_ref(), &agg_calls, None);

        // // TODO: enable sanity check for globle simple agg executor <https://github.com/risingwavelabs/risingwave/issues/3885>
        for state_table in &mut state_tables {
            state_table.disable_sanity_check();
        }

        Ok(Self {
            ctx,
            input,
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: format!("GlobalSimpleAggExecutor-{:X}", executor_id),
            },
            input_pk_indices: input_info.pk_indices,
            input_schema: input_info.schema,
            states: None,
            agg_calls,
            state_tables,
            state_table_col_mappings: state_table_col_mappings
                .into_iter()
                .map(StateTableColumnMapping::new)
                .map(Arc::new)
                .collect(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_chunk(
        ctx: &ActorContextRef,
        identity: &str,
        agg_calls: &[AggCall],
        input_pk_indices: &[usize],
        _input_schema: &Schema,
        states: &mut Option<AggState<S>>,
        chunk: StreamChunk,
        epoch: u64,
        state_tables: &mut [StateTable<S>],
        state_table_col_mappings: &[Arc<StateTableColumnMapping>],
    ) -> StreamExecutorResult<()> {
        let capacity = chunk.capacity();
        let (ops, columns, visibility) = chunk.into_inner();
        let column_refs = columns.iter().map(|col| col.array_ref()).collect_vec();

        // 1. Retrieve previous state from the KeyedState. If they didn't exist, the ManagedState
        // will automatically create new ones for them.
        if states.is_none() {
            let state = generate_managed_agg_state(
                None,
                agg_calls,
                input_pk_indices.to_vec(),
                epoch,
                state_tables,
                state_table_col_mappings,
            )
            .await?;
            *states = Some(state);
        }
        let states = states.as_mut().unwrap();

        // 2. Mark the state as dirty by filling prev states
        states.may_mark_as_dirty(epoch, state_tables).await?;

        // 3. Apply batch to each of the state (per agg_call)
        for ((agg_state, agg_call), state_table) in states
            .managed_states
            .iter_mut()
            .zip_eq(agg_calls.iter())
            .zip_eq(state_tables.iter_mut())
        {
            let vis_map = agg_call_filter_res(
                ctx,
                identity,
                agg_call,
                &columns,
                visibility.as_ref(),
                capacity,
            )?;
            agg_state
                .apply_chunk(&ops, vis_map.as_ref(), &column_refs, epoch, state_table)
                .await?;
        }

        Ok(())
    }

    async fn flush_data(
        schema: &Schema,
        states: &mut Option<AggState<S>>,
        epoch: u64,
        state_tables: &mut [StateTable<S>],
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        // --- Flush states to the state store ---
        // Some state will have the correct output only after their internal states have been fully
        // flushed.

        let states = match states.as_mut() {
            Some(states) if states.is_dirty() => states,
            _ => return Ok(None), // Nothing to flush.
        };

        for (state, state_table) in states
            .managed_states
            .iter_mut()
            .zip_eq(state_tables.iter_mut())
        {
            state.flush(state_table)?;
        }

        // Batch commit state tables.
        for state_table in state_tables.iter_mut() {
            state_table.commit(epoch).await?;
        }

        // --- Create array builders ---
        // As the datatype is retrieved from schema, it contains both group key and aggregation
        // state outputs.
        let mut builders = schema.create_array_builders(2);
        let mut new_ops = Vec::with_capacity(2);

        // --- Retrieve modified states and put the changes into the builders ---
        states
            .build_changes(&mut builders, &mut new_ops, epoch, state_tables)
            .await?;

        let columns: Vec<Column> = builders
            .into_iter()
            .map(|builder| builder.finish().into())
            .collect();

        let chunk = StreamChunk::new(new_ops, columns, None);

        Ok(Some(chunk))
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let GlobalSimpleAggExecutor {
            ctx,
            input,
            info,
            input_pk_indices,
            input_schema,
            mut states,
            agg_calls,
            mut state_tables,
            state_table_col_mappings,
        } = self;
        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let mut epoch = barrier.epoch.curr;
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    Self::apply_chunk(
                        &ctx,
                        &info.identity,
                        &agg_calls,
                        &input_pk_indices,
                        &input_schema,
                        &mut states,
                        chunk,
                        epoch,
                        &mut state_tables,
                        &state_table_col_mappings,
                    )
                    .await?;
                }
                Message::Barrier(barrier) => {
                    let next_epoch = barrier.epoch.curr;
                    if let Some(chunk) =
                        Self::flush_data(&info.schema, &mut states, epoch, &mut state_tables)
                            .await?
                    {
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

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{Field, TableId};
    use risingwave_common::types::*;
    use risingwave_expr::expr::*;
    use risingwave_storage::memory::MemoryStateStore;

    use crate::executor::aggregation::{AggArgs, AggCall};
    use crate::executor::test_utils::agg_executor::new_boxed_simple_agg_executor;
    use crate::executor::test_utils::*;
    use crate::executor::*;

    #[tokio::test]
    async fn test_local_simple_aggregation_in_memory() {
        test_local_simple_aggregation(create_in_memory_keyspace_agg(4)).await
    }

    async fn test_local_simple_aggregation(keyspace: Vec<(MemoryStateStore, TableId)>) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                // primary key column`
                Field::unnamed(DataType::Int64),
            ],
        };
        let (mut tx, source) = MockSource::channel(schema, vec![2]); // pk
        tx.push_barrier(1, false);
        tx.push_chunk(StreamChunk::from_pretty(
            "   I   I    I
            + 100 200 1001
            +  10  14 1002
            +   4 300 1003",
        ));
        tx.push_barrier(2, false);
        tx.push_chunk(StreamChunk::from_pretty(
            "   I   I    I
            - 100 200 1001
            -  10  14 1002 D
            -   4 300 1003
            + 104 500 1004",
        ));
        tx.push_barrier(3, false);

        // This is local simple aggregation, so we add another row count state
        let append_only = false;
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
            AggCall {
                kind: AggKind::Min,
                args: AggArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
            },
        ];

        let simple_agg = new_boxed_simple_agg_executor(
            ActorContext::create(123),
            keyspace.clone(),
            Box::new(source),
            agg_calls,
            vec![2],
            1,
            vec![],
        );
        let mut simple_agg = simple_agg.execute();

        // Consume the init barrier
        simple_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = simple_agg.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I   I   I  I
                + 3 114 514  4"
            )
        );
        assert_matches!(
            simple_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = simple_agg.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I   I   I  I
                U- 3 114 514  4
                U+ 2 114 514 10"
            )
        );
    }
}
