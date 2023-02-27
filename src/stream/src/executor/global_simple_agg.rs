// Copyright 2023 RisingWave Labs
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

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::row::RowExt;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_storage::StateStore;

use super::agg_common::AggExecutorArgs;
use super::aggregation::{
    agg_call_filter_res, iter_table_storage, AggChangesInfo, AggStateStorage, DistinctDeduplicater,
};
use super::*;
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::aggregation::{generate_agg_schema, AggCall, AggGroup};
use crate::executor::error::StreamExecutorError;
use crate::executor::{BoxedMessageStream, Message};
use crate::task::AtomicU64Ref;

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
    inner: ExecutorInner<S>,
}

struct ExecutorInner<S: StateStore> {
    actor_ctx: ActorContextRef,
    info: ExecutorInfo,

    /// Pk indices from input.
    input_pk_indices: Vec<usize>,

    /// Schema from input.
    input_schema: Schema,

    /// An operator will support multiple aggregation calls.
    agg_calls: Vec<AggCall>,

    /// State storage for each agg calls.
    storages: Vec<AggStateStorage<S>>,

    /// State table for the previous result of all agg calls.
    /// The outputs of all managed agg states are collected and stored in this
    /// table when `flush_data` is called.
    result_table: StateTable<S>,

    /// State tables for deduplicating rows on distinct key for distinct agg calls.
    /// One table per distinct column (may be shared by multiple agg calls).
    distinct_dedup_tables: HashMap<usize, StateTable<S>>,

    /// Watermark epoch.
    watermark_epoch: AtomicU64Ref,

    /// Extreme state cache size
    extreme_cache_size: usize,
}

impl<S: StateStore> ExecutorInner<S> {
    fn all_state_tables_mut(&mut self) -> impl Iterator<Item = &mut StateTable<S>> {
        iter_table_storage(&mut self.storages)
            .chain(self.distinct_dedup_tables.values_mut())
            .chain(std::iter::once(&mut self.result_table))
    }

    fn all_state_tables_except_result_mut(&mut self) -> impl Iterator<Item = &mut StateTable<S>> {
        iter_table_storage(&mut self.storages).chain(self.distinct_dedup_tables.values_mut())
    }
}

struct ExecutionVars<S: StateStore> {
    /// The single [`AggGroup`].
    agg_group: AggGroup<S>,

    /// Distinct deduplicater to deduplicate input rows for each distinct agg call.
    distinct_dedup: DistinctDeduplicater<S>,

    /// Mark the agg state is changed in the current epoch or not.
    state_changed: bool,
}

impl<S: StateStore> Executor for GlobalSimpleAggExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.inner.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.inner.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.inner.info.identity
    }
}

impl<S: StateStore> GlobalSimpleAggExecutor<S> {
    pub fn new(args: AggExecutorArgs<S>) -> StreamResult<Self> {
        let input_info = args.input.info();
        let schema = generate_agg_schema(args.input.as_ref(), &args.agg_calls, None);
        Ok(Self {
            input: args.input,
            inner: ExecutorInner {
                actor_ctx: args.actor_ctx,
                info: ExecutorInfo {
                    schema,
                    pk_indices: args.pk_indices,
                    identity: format!("GlobalSimpleAggExecutor-{:X}", args.executor_id),
                },
                input_pk_indices: input_info.pk_indices,
                input_schema: input_info.schema,
                agg_calls: args.agg_calls,
                storages: args.storages,
                result_table: args.result_table,
                distinct_dedup_tables: args.distinct_dedup_tables,
                watermark_epoch: args.watermark_epoch,
                extreme_cache_size: args.extreme_cache_size,
            },
        })
    }

    async fn apply_chunk(
        this: &mut ExecutorInner<S>,
        vars: &mut ExecutionVars<S>,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<()> {
        // Decompose the input chunk.
        let capacity = chunk.capacity();
        let (ops, columns, visibility) = chunk.into_inner();

        // Calculate the row visibility for every agg call.
        let visibilities: Vec<_> = this
            .agg_calls
            .iter()
            .map(|agg_call| {
                agg_call_filter_res(
                    &this.actor_ctx,
                    &this.info.identity,
                    agg_call,
                    &columns,
                    visibility.as_ref(),
                    capacity,
                )
            })
            .try_collect()?;

        // Materialize input chunk if needed.
        this.storages
            .iter_mut()
            .zip_eq_fast(visibilities.iter().map(Option::as_ref))
            .for_each(|(storage, visibility)| {
                if let AggStateStorage::MaterializedInput { table, mapping } = storage {
                    let needed_columns = mapping
                        .upstream_columns()
                        .iter()
                        .map(|col_idx| columns[*col_idx].clone())
                        .collect();
                    table.write_chunk(StreamChunk::new(
                        ops.clone(),
                        needed_columns,
                        visibility.cloned(),
                    ));
                }
            });

        // Deduplicate for distinct columns.
        let visibilities = vars
            .distinct_dedup
            .dedup_chunk(
                &ops,
                &columns,
                visibilities,
                &mut this.distinct_dedup_tables,
                None,
            )
            .await?;

        // Apply chunk to each of the state (per agg_call).
        vars.agg_group
            .apply_chunk(&mut this.storages, &ops, &columns, visibilities)?;

        // Mark state as changed.
        vars.state_changed = true;

        Ok(())
    }

    async fn flush_data(
        this: &mut ExecutorInner<S>,
        vars: &mut ExecutionVars<S>,
        epoch: EpochPair,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        if vars.state_changed {
            // Flush agg states.
            vars.agg_group
                .flush_state_if_needed(&mut this.storages)
                .await?;

            // Flush distinct dedup state.
            vars.distinct_dedup.flush(&mut this.distinct_dedup_tables)?;

            // Commit all state tables except for result table.
            futures::future::try_join_all(
                this.all_state_tables_except_result_mut()
                    .map(|table| table.commit(epoch)),
            )
            .await?;

            // Create array builders.
            // As the datatype is retrieved from schema, it contains both group key and aggregation
            // state outputs.
            let mut builders = this.info.schema.create_array_builders(2);
            let mut new_ops = Vec::with_capacity(2);
            // Retrieve modified states and put the changes into the builders.
            let curr_outputs = vars.agg_group.get_outputs(&this.storages).await?;
            let AggChangesInfo {
                result_row,
                prev_outputs,
                n_appended_ops,
            } = vars
                .agg_group
                .build_changes(curr_outputs, &mut builders, &mut new_ops);

            if n_appended_ops == 0 {
                // Agg result is not changed.
                this.result_table.commit_no_data_expected(epoch);
                return Ok(None);
            }

            // Update the result table with latest agg outputs.
            if let Some(prev_outputs) = prev_outputs {
                let old_row = vars.agg_group.group_key().chain(prev_outputs);
                this.result_table.update(old_row, result_row);
            } else {
                this.result_table.insert(result_row);
            }
            this.result_table.commit(epoch).await?;

            let columns = builders
                .into_iter()
                .map(|builder| builder.finish().into())
                .collect();

            let chunk = StreamChunk::new(new_ops, columns, None);

            vars.state_changed = false;
            Ok(Some(chunk))
        } else {
            // No state is changed.
            // Call commit on state table to increment the epoch.
            this.all_state_tables_mut().for_each(|table| {
                table.commit_no_data_expected(epoch);
            });
            Ok(None)
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let GlobalSimpleAggExecutor {
            input,
            inner: mut this,
        } = self;

        let mut input = input.execute();
        let barrier = expect_first_barrier(&mut input).await?;
        this.all_state_tables_mut().for_each(|table| {
            table.init_epoch(barrier.epoch);
        });

        let mut vars = ExecutionVars {
            // Create `AggGroup`. This will fetch previous agg result from the result table.
            agg_group: AggGroup::create(
                None,
                &this.agg_calls,
                &this.storages,
                &this.result_table,
                &this.input_pk_indices,
                this.extreme_cache_size,
                &this.input_schema,
            )
            .await?,
            distinct_dedup: DistinctDeduplicater::new(&this.agg_calls, &this.watermark_epoch),
            state_changed: false,
        };

        vars.distinct_dedup.dedup_caches_mut().for_each(|cache| {
            cache.update_epoch(barrier.epoch.curr);
        });

        if vars.agg_group.is_uninitialized() {
            let data_types = this
                .input_schema
                .fields
                .iter()
                .map(|f| f.data_type())
                .collect::<Vec<_>>();
            let chunk = StreamChunk::from_rows(&[], &data_types[..]);
            // Apply empty chunk
            Self::apply_chunk(&mut this, &mut vars, chunk).await?;
        }

        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(_) => {}
                Message::Chunk(chunk) => {
                    Self::apply_chunk(&mut this, &mut vars, chunk).await?;
                }
                Message::Barrier(barrier) => {
                    if let Some(chunk) =
                        Self::flush_data(&mut this, &mut vars, barrier.epoch).await?
                    {
                        yield Message::Chunk(chunk);
                    }
                    vars.distinct_dedup.dedup_caches_mut().for_each(|cache| {
                        cache.update_epoch(barrier.epoch.curr);
                    });
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
        tx.push_barrier(2, false);
        tx.push_chunk(StreamChunk::from_pretty(
            "   I   I    I
            + 100 200 1001
            +  10  14 1002
            +   4 300 1003",
        ));
        tx.push_barrier(3, false);
        tx.push_chunk(StreamChunk::from_pretty(
            "   I   I    I
            - 100 200 1001
            -  10  14 1002 D
            -   4 300 1003
            + 104 500 1004",
        ));
        tx.push_barrier(4, false);

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
                distinct: false,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Min,
                args: AggArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only,
                filter: None,
                distinct: false,
            },
        ];

        let simple_agg = new_boxed_simple_agg_executor(
            ActorContext::create(123),
            store,
            Box::new(source),
            agg_calls,
            vec![2],
            1,
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
}
