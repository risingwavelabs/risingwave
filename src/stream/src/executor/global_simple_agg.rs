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
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::aggregation::{agg_call_filter_res, AggStateTable};
use super::*;
use crate::error::StreamResult;
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
    agg_state: Option<AggState<S>>,

    /// An operator will support multiple aggregation calls.
    agg_calls: Vec<AggCall>,

    /// Relational state tables for each aggregation calls.
    /// `None` means the agg call need not to maintain a state table by itself.
    agg_state_tables: Vec<Option<AggStateTable<S>>>,

    /// State table for the previous result of all agg calls.
    /// The outputs of all managed agg states are collected and stored in this
    /// table when `flush_data` is called.
    result_table: StateTable<S>,

    /// Extreme state cache size
    extreme_cache_size: usize,

    /// Mark the agg state is changed in the current epoch or not.
    state_changed: bool,
}

impl<S: StateStore> Executor for GlobalSimpleAggExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

impl<S: StateStore> GlobalSimpleAggExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: ActorContextRef,
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        mut agg_state_tables: Vec<Option<AggStateTable<S>>>,
        mut result_table: StateTable<S>,
        pk_indices: PkIndices,
        executor_id: u64,
        extreme_cache_size: usize,
    ) -> StreamResult<Self> {
        let input_info = input.info();
        let schema = generate_agg_schema(input.as_ref(), &agg_calls, None);

        // TODO: enable sanity check for globle simple agg executor <https://github.com/risingwavelabs/risingwave/issues/3885>
        agg_state_tables
            .iter_mut()
            .filter_map(Option::as_mut)
            .for_each(|state_table| {
                state_table.table.disable_sanity_check();
            });
        result_table.disable_sanity_check();

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
            agg_state: None,
            agg_calls,
            agg_state_tables,
            result_table,
            extreme_cache_size,
            state_changed: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_chunk(
        ctx: &ActorContextRef,
        identity: &str,
        agg_calls: &[AggCall],
        agg_state_tables: &mut [Option<AggStateTable<S>>],
        result_table: &mut StateTable<S>,
        input_pk_indices: &PkIndices,
        _input_schema: &Schema,
        agg_state: &mut Option<AggState<S>>,
        chunk: StreamChunk,
        extreme_cache_size: usize,
        state_changed: &mut bool,
    ) -> StreamExecutorResult<()> {
        let capacity = chunk.capacity();
        let (ops, columns, visibility) = chunk.into_inner();
        let column_refs = columns.iter().map(|col| col.array_ref()).collect_vec();

        // Create `AggState` if not exists. This will fetch previous agg result
        // from the result table.
        if agg_state.is_none() {
            *agg_state = Some(
                generate_managed_agg_state(
                    None,
                    agg_calls,
                    agg_state_tables,
                    result_table,
                    input_pk_indices,
                    extreme_cache_size,
                )
                .await?,
            );
        }
        let agg_state = agg_state.as_mut().unwrap();

        // Mark state as changed.
        *state_changed = true;

        // Apply batch to each of the state (per agg_call)
        for ((managed_state, agg_call), agg_state_table) in agg_state
            .managed_states()
            .iter_mut()
            .zip_eq(agg_calls.iter())
            .zip_eq(agg_state_tables.iter_mut())
        {
            let vis_map = agg_call_filter_res(
                ctx,
                identity,
                agg_call,
                &columns,
                visibility.as_ref(),
                capacity,
            )?;
            managed_state
                .apply_chunk(
                    &ops,
                    vis_map.as_ref(),
                    &column_refs,
                    agg_state_table.as_mut(),
                )
                .await?;
        }

        Ok(())
    }

    async fn flush_data(
        schema: &Schema,
        agg_state: &mut Option<AggState<S>>,
        epoch: EpochPair,
        agg_state_tables: &mut [Option<AggStateTable<S>>],
        result_table: &mut StateTable<S>,
        state_changed: &mut bool,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        // --- Flush states to the state store ---

        if *state_changed {
            let agg_state = agg_state.as_mut().unwrap();

            // Batch commit data.
            futures::future::try_join_all(
                agg_state_tables
                    .iter_mut()
                    .filter_map(Option::as_mut)
                    .map(|state_table| state_table.table.commit(epoch)),
            )
            .await?;

            // --- Create array builders ---
            // As the datatype is retrieved from schema, it contains both group key and aggregation
            // state outputs.
            let mut builders = schema.create_array_builders(2);
            let mut new_ops = Vec::with_capacity(2);
            // --- Retrieve modified states and put the changes into the builders ---
            let (_, result_row) = agg_state
                .build_changes(&mut builders, &mut new_ops, &agg_state_tables)
                .await?;
            result_table.upsert(result_row);
            result_table.commit(epoch).await?;

            let columns: Vec<Column> = builders
                .into_iter()
                .map(|builder| builder.finish().into())
                .collect();

            let chunk = StreamChunk::new(new_ops, columns, None);

            *state_changed = false;
            Ok(Some(chunk))
        } else {
            // Nothing to flush.
            // Call commit on state table to increment the epoch.
            agg_state_tables
                .iter_mut()
                .filter_map(Option::as_mut)
                .for_each(|state_table| {
                    state_table.table.commit_no_data_expected(epoch);
                });
            result_table.commit_no_data_expected(epoch);
            return Ok(None);
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let GlobalSimpleAggExecutor {
            ctx,
            input,
            info,
            input_pk_indices,
            input_schema,
            mut agg_state,
            agg_calls,
            extreme_cache_size,
            mut agg_state_tables,
            mut result_table,
            mut state_changed,
        } = self;
        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        agg_state_tables
            .iter_mut()
            .filter_map(Option::as_mut)
            .for_each(|state_table| {
                state_table.table.init_epoch(barrier.epoch);
            });
        result_table.init_epoch(barrier.epoch);

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
                        &mut agg_state_tables,
                        &mut result_table,
                        &input_pk_indices,
                        &input_schema,
                        &mut agg_state,
                        chunk,
                        extreme_cache_size,
                        &mut state_changed,
                    )
                    .await?;
                }
                Message::Barrier(barrier) => {
                    if let Some(chunk) = Self::flush_data(
                        &info.schema,
                        &mut agg_state,
                        barrier.epoch,
                        &mut agg_state_tables,
                        &mut result_table,
                        &mut state_changed,
                    )
                    .await?
                    {
                        yield Message::Chunk(chunk);
                    }
                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::Field;
    use risingwave_common::types::*;
    use risingwave_expr::expr::*;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::StateStore;

    use crate::executor::aggregation::{AggArgs, AggCall};
    use crate::executor::test_utils::agg_executor::new_boxed_simple_agg_executor;
    use crate::executor::test_utils::*;
    use crate::executor::*;

    #[tokio::test]
    async fn test_local_simple_aggregation_in_memory() {
        test_local_simple_aggregation(MemoryStateStore::new()).await
    }

    async fn test_local_simple_aggregation<S: StateStore>(store: S) {
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
            store,
            Box::new(source),
            agg_calls,
            vec![2],
            1,
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
