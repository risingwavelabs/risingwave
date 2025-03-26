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

use std::collections::HashMap;

use risingwave_common::array::stream_record::Record;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::aggregate::{AggCall, BoxedAggregateFunction, build_retractable};
use risingwave_pb::stream_plan::PbAggNodeVersion;

use super::agg_group::{AggGroup, AlwaysOutput};
use super::agg_state::AggStateStorage;
use super::distinct::DistinctDeduplicater;
use super::{AggExecutorArgs, SimpleAggExecutorExtraArgs, agg_call_filter_res, iter_table_storage};
use crate::executor::prelude::*;

/// `SimpleAggExecutor` is the aggregation operator for streaming system.
/// To create an aggregation operator, states and expressions should be passed along the
/// constructor.
///
/// `SimpleAggExecutor` maintains multiple states together. If there are `n` states and `n`
/// expressions, there will be `n` columns as output.
///
/// As the engine processes data in chunks, it is possible that multiple update
/// messages could consolidate to a single row update. For example, our source
/// emits 1000 inserts in one chunk, and we aggregate the count function on that.
/// Current `SimpleAggExecutor` will only emit one row for a whole chunk.
/// Therefore, we "automatically" implement a window function inside
/// `SimpleAggExecutor`.
pub struct SimpleAggExecutor<S: StateStore> {
    input: Executor,
    inner: ExecutorInner<S>,
}

struct ExecutorInner<S: StateStore> {
    /// Version of aggregation executors.
    version: PbAggNodeVersion,

    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Pk indices from input. Only used by `AggNodeVersion` before `ISSUE_13465`.
    input_pk_indices: Vec<usize>,

    /// Schema from input.
    input_schema: Schema,

    /// An operator will support multiple aggregation calls.
    agg_calls: Vec<AggCall>,

    /// Aggregate functions.
    agg_funcs: Vec<BoxedAggregateFunction>,

    /// Index of row count agg call (`count(*)`) in the call list.
    row_count_index: usize,

    /// State storage for each agg calls.
    storages: Vec<AggStateStorage<S>>,

    /// Intermediate state table for value-state agg calls.
    /// The state of all value-state aggregates are collected and stored in this
    /// table when `flush_data` is called.
    intermediate_state_table: StateTable<S>,

    /// State tables for deduplicating rows on distinct key for distinct agg calls.
    /// One table per distinct column (may be shared by multiple agg calls).
    distinct_dedup_tables: HashMap<usize, StateTable<S>>,

    /// Watermark epoch.
    watermark_epoch: AtomicU64Ref,

    /// Extreme state cache size
    extreme_cache_size: usize,

    /// Required by the downstream `RowMergeExecutor`,
    /// currently only used by the `approx_percentile`'s two phase plan
    must_output_per_barrier: bool,
}

impl<S: StateStore> ExecutorInner<S> {
    fn all_state_tables_mut(&mut self) -> impl Iterator<Item = &mut StateTable<S>> {
        iter_table_storage(&mut self.storages)
            .chain(self.distinct_dedup_tables.values_mut())
            .chain(std::iter::once(&mut self.intermediate_state_table))
    }
}

struct ExecutionVars<S: StateStore> {
    /// The single [`AggGroup`].
    agg_group: AggGroup<S, AlwaysOutput>,

    /// Distinct deduplicater to deduplicate input rows for each distinct agg call.
    distinct_dedup: DistinctDeduplicater<S>,

    /// Mark the agg state is changed in the current epoch or not.
    state_changed: bool,
}

impl<S: StateStore> Execute for SimpleAggExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl<S: StateStore> SimpleAggExecutor<S> {
    pub fn new(args: AggExecutorArgs<S, SimpleAggExecutorExtraArgs>) -> StreamResult<Self> {
        let input_info = args.input.info().clone();
        Ok(Self {
            input: args.input,
            inner: ExecutorInner {
                version: args.version,
                actor_ctx: args.actor_ctx,
                info: args.info,
                input_pk_indices: input_info.pk_indices,
                input_schema: input_info.schema,
                agg_funcs: args.agg_calls.iter().map(build_retractable).try_collect()?,
                agg_calls: args.agg_calls,
                row_count_index: args.row_count_index,
                storages: args.storages,
                intermediate_state_table: args.intermediate_state_table,
                distinct_dedup_tables: args.distinct_dedup_tables,
                watermark_epoch: args.watermark_epoch,
                extreme_cache_size: args.extreme_cache_size,
                must_output_per_barrier: args.extra.must_output_per_barrier,
            },
        })
    }

    async fn apply_chunk(
        this: &mut ExecutorInner<S>,
        vars: &mut ExecutionVars<S>,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<()> {
        if chunk.cardinality() == 0 {
            // If the chunk is empty, do nothing.
            return Ok(());
        }

        // Calculate the row visibility for every agg call.
        let mut call_visibilities = Vec::with_capacity(this.agg_calls.len());
        for agg_call in &this.agg_calls {
            let vis = agg_call_filter_res(agg_call, &chunk).await?;
            call_visibilities.push(vis);
        }

        // Deduplicate for distinct columns.
        let visibilities = vars
            .distinct_dedup
            .dedup_chunk(
                chunk.ops(),
                chunk.columns(),
                call_visibilities,
                &mut this.distinct_dedup_tables,
                None,
            )
            .await?;

        // Materialize input chunk if needed and possible.
        for (storage, visibility) in this.storages.iter_mut().zip_eq_fast(visibilities.iter()) {
            if let AggStateStorage::MaterializedInput { table, mapping, .. } = storage {
                let chunk = chunk.project_with_vis(mapping.upstream_columns(), visibility.clone());
                table.write_chunk(chunk);
            }
        }

        // Apply chunk to each of the state (per agg_call).
        vars.agg_group
            .apply_chunk(&chunk, &this.agg_calls, &this.agg_funcs, visibilities)
            .await?;

        // Mark state as changed.
        vars.state_changed = true;

        Ok(())
    }

    async fn flush_data(
        this: &mut ExecutorInner<S>,
        vars: &mut ExecutionVars<S>,
        epoch: EpochPair,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        if vars.state_changed || vars.agg_group.is_uninitialized() {
            // Flush distinct dedup state.
            vars.distinct_dedup.flush(&mut this.distinct_dedup_tables)?;

            // Build and apply change for intermediate states.
            if let Some(inter_states_change) =
                vars.agg_group.build_states_change(&this.agg_funcs)?
            {
                this.intermediate_state_table
                    .write_record(inter_states_change);
            }
        }
        vars.state_changed = false;

        // Build and apply change for the final outputs.
        let (outputs_change, _stats) = vars
            .agg_group
            .build_outputs_change(&this.storages, &this.agg_funcs)
            .await?;

        let change =
            outputs_change.expect("`AlwaysOutput` strategy will output a change in any case");
        let chunk = if !this.must_output_per_barrier
            && let Record::Update { old_row, new_row } = &change
            && old_row == new_row
        {
            // for cases without approx percentile, we don't need to output the change if it's noop
            None
        } else {
            Some(change.to_stream_chunk(&this.info.schema.data_types()))
        };

        // Commit all state tables.
        futures::future::try_join_all(
            this.all_state_tables_mut()
                .map(|table| table.commit_assert_no_update_vnode_bitmap(epoch)),
        )
        .await?;

        Ok(chunk)
    }

    async fn try_flush_data(this: &mut ExecutorInner<S>) -> StreamExecutorResult<()> {
        futures::future::try_join_all(this.all_state_tables_mut().map(|table| table.try_flush()))
            .await?;
        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let Self {
            input,
            inner: mut this,
        } = self;

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);

        for table in this.all_state_tables_mut() {
            table.init_epoch(first_epoch).await?;
        }

        let distinct_dedup = DistinctDeduplicater::new(
            &this.agg_calls,
            this.watermark_epoch.clone(),
            &this.distinct_dedup_tables,
            &this.actor_ctx,
        );

        // This will fetch previous agg states from the intermediate state table.
        let mut vars = ExecutionVars {
            agg_group: AggGroup::create(
                this.version,
                None,
                &this.agg_calls,
                &this.agg_funcs,
                &this.storages,
                &this.intermediate_state_table,
                &this.input_pk_indices,
                this.row_count_index,
                false, // emit on window close
                this.extreme_cache_size,
                &this.input_schema,
            )
            .await?,
            distinct_dedup,
            state_changed: false,
        };

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(_) => {}
                Message::Chunk(chunk) => {
                    Self::apply_chunk(&mut this, &mut vars, chunk).await?;
                    Self::try_flush_data(&mut this).await?;
                }
                Message::Barrier(barrier) => {
                    if let Some(chunk) =
                        Self::flush_data(&mut this, &mut vars, barrier.epoch).await?
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
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::agg_executor::new_boxed_simple_agg_executor;
    use crate::executor::test_utils::*;

    #[tokio::test]
    async fn test_simple_aggregation_in_memory() {
        test_simple_aggregation(MemoryStateStore::new()).await
    }

    async fn test_simple_aggregation<S: StateStore>(store: S) {
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                // primary key column`
                Field::unnamed(DataType::Int64),
            ],
        };
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, vec![2]);
        tx.push_barrier(test_epoch(1), false);
        tx.push_barrier(test_epoch(2), false);
        tx.push_chunk(StreamChunk::from_pretty(
            "   I   I    I
            + 100 200 1001
            +  10  14 1002
            +   4 300 1003",
        ));
        tx.push_barrier(test_epoch(3), false);
        tx.push_chunk(StreamChunk::from_pretty(
            "   I   I    I
            - 100 200 1001
            -  10  14 1002 D
            -   4 300 1003
            + 104 500 1004",
        ));
        tx.push_barrier(test_epoch(4), false);

        let agg_calls = vec![
            AggCall::from_pretty("(count:int8)"),
            AggCall::from_pretty("(sum:int8 $0:int8)"),
            AggCall::from_pretty("(sum:int8 $1:int8)"),
            AggCall::from_pretty("(min:int8 $0:int8)"),
        ];

        let simple_agg = new_boxed_simple_agg_executor(
            ActorContext::for_test(123),
            store,
            source,
            false,
            agg_calls,
            0,
            vec![2],
            1,
            false,
        )
        .await;
        let mut simple_agg = simple_agg.execute();

        // Consume the init barrier
        simple_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = simple_agg.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I   I   I  I
                + 0   .   .  . "
            )
        );
        assert_matches!(
            simple_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        // Consume stream chunk
        let msg = simple_agg.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I   I   I  I
                U- 0   .   .  .
                U+ 3 114 514  4"
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

    // NOTE(kwannoel): `approx_percentile` + `keyed_merge` depend on this property for correctness.
    #[tokio::test]
    async fn test_simple_aggregation_always_output_per_epoch() {
        let store = MemoryStateStore::new();
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                // primary key column`
                Field::unnamed(DataType::Int64),
            ],
        };
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, vec![2]);
        // initial barrier
        tx.push_barrier(test_epoch(1), false);
        // next barrier
        tx.push_barrier(test_epoch(2), false);
        tx.push_chunk(StreamChunk::from_pretty(
            "   I   I    I
            + 100 200 1001
            - 100 200 1001",
        ));
        tx.push_barrier(test_epoch(3), false);
        tx.push_barrier(test_epoch(4), false);

        let agg_calls = vec![
            AggCall::from_pretty("(count:int8)"),
            AggCall::from_pretty("(sum:int8 $0:int8)"),
            AggCall::from_pretty("(sum:int8 $1:int8)"),
            AggCall::from_pretty("(min:int8 $0:int8)"),
        ];

        let simple_agg = new_boxed_simple_agg_executor(
            ActorContext::for_test(123),
            store,
            source,
            false,
            agg_calls,
            0,
            vec![2],
            1,
            true,
        )
        .await;
        let mut simple_agg = simple_agg.execute();

        // Consume the init barrier
        simple_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = simple_agg.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I   I   I  I
                + 0   .   .  . "
            )
        );
        assert_matches!(
            simple_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        // Consume stream chunk
        let msg = simple_agg.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I   I   I  I
                U- 0   .   .  .
                U+ 0   .   .  ."
            )
        );
        assert_matches!(
            simple_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        // Consume stream chunk
        let msg = simple_agg.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I   I   I  I
                U- 0   .   .  .
                U+ 0   .   .  ."
            )
        );
        assert_matches!(
            simple_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );
    }

    // NOTE(kwannoel): `approx_percentile` + `keyed_merge` depend on this property for correctness.
    #[tokio::test]
    async fn test_simple_aggregation_omit_noop_update() {
        let store = MemoryStateStore::new();
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                // primary key column`
                Field::unnamed(DataType::Int64),
            ],
        };
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, vec![2]);
        // initial barrier
        tx.push_barrier(test_epoch(1), false);
        // next barrier
        tx.push_barrier(test_epoch(2), false);
        tx.push_chunk(StreamChunk::from_pretty(
            "   I   I    I
                + 100 200 1001
                - 100 200 1001",
        ));
        tx.push_barrier(test_epoch(3), false);
        tx.push_barrier(test_epoch(4), false);

        let agg_calls = vec![
            AggCall::from_pretty("(count:int8)"),
            AggCall::from_pretty("(sum:int8 $0:int8)"),
            AggCall::from_pretty("(sum:int8 $1:int8)"),
            AggCall::from_pretty("(min:int8 $0:int8)"),
        ];

        let simple_agg = new_boxed_simple_agg_executor(
            ActorContext::for_test(123),
            store,
            source,
            false,
            agg_calls,
            0,
            vec![2],
            1,
            false,
        )
        .await;
        let mut simple_agg = simple_agg.execute();

        // Consume the init barrier
        simple_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = simple_agg.next().await.unwrap().unwrap();
        assert_eq!(
            *msg.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I   I   I  I
                + 0   .   .  . "
            )
        );
        assert_matches!(
            simple_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        // No stream chunk
        assert_matches!(
            simple_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        // No stream chunk
        assert_matches!(
            simple_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );
    }
}
