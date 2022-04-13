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

use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_storage::store::GLOBAL_STORAGE_TABLE_ID;
use risingwave_storage::{Keyspace, StateStore};

use super::{Executor, ExecutorInfo, StreamExecutorResult};
use crate::executor::{agg_input_array_refs, pk_input_array_refs, AggCall, AggState, PkIndicesRef};
use crate::executor_v2::agg::{
    generate_agg_schema, generate_agg_state, AggExecutor, AggExecutorWrapper,
};
use crate::executor_v2::error::StreamExecutorError;
use crate::executor_v2::{BoxedMessageStream, PkIndices};

/// `SimpleAggExecutor` is the aggregation operator for streaming system.
/// To create an aggregation operator, states and expressions should be passed along the
/// constructor.
///
/// `SimpleAggExecutor` maintain multiple states together. If there are `n`
/// states and `n` expressions, there will be `n` columns as output.
///
/// As the engine processes data in chunks, it is possible that multiple update
/// messages could consolidate to a single row update. For example, our source
/// emits 1000 inserts in one chunk, and we aggregates count function on that.
/// Current `SimpleAggExecutor` will only emit one row for a whole chunk.
/// Therefore, we "automatically" implemented a window function inside
/// `SimpleAggExecutor`.
pub type SimpleAggExecutor<S> = AggExecutorWrapper<AggSimpleAggExecutor<S>>;

impl<S: StateStore> SimpleAggExecutor<S> {
    pub fn new(
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        keyspace: Keyspace<S>,
        pk_indices: PkIndices,
        executor_id: u64,
        key_indices: Vec<usize>,
    ) -> Result<Self> {
        let info = input.info();
        let schema = generate_agg_schema(input.as_ref(), &agg_calls, None);

        Ok(AggExecutorWrapper {
            input,
            inner: AggSimpleAggExecutor::new(
                info,
                agg_calls,
                keyspace,
                pk_indices,
                schema,
                executor_id,
                key_indices,
            )?,
        })
    }
}

pub struct AggSimpleAggExecutor<S: StateStore> {
    info: ExecutorInfo,

    /// Schema of the executor.
    schema: Schema,

    /// Primary key indices.
    pk_indices: PkIndices,

    /// The executor operates on this keyspace.
    keyspace: Keyspace<S>,

    /// Aggregation states of the current operator.
    /// This is an `Option` and the initial state is built when `Executor::next` is called, since
    /// we may not want `Self::new` to be an `async` function.
    states: Option<AggState<S>>,

    /// An operator will support multiple aggregation calls.
    agg_calls: Vec<AggCall>,

    #[allow(dead_code)]
    /// Indices of the columns on which key distribution depends.
    key_indices: Vec<usize>,
}

impl<S: StateStore> AggSimpleAggExecutor<S> {
    pub fn new(
        input_info: ExecutorInfo,
        agg_calls: Vec<AggCall>,
        keyspace: Keyspace<S>,
        pk_indices: PkIndices,
        schema: Schema,
        executor_id: u64,
        key_indices: Vec<usize>,
    ) -> Result<Self> {
        Ok(Self {
            info: ExecutorInfo {
                schema: input_info.schema,
                pk_indices: input_info.pk_indices,
                identity: format!("SimpleAggExecutor {:X}", executor_id),
            },
            schema,
            pk_indices,
            keyspace,
            states: None,
            agg_calls,
            key_indices,
        })
    }

    fn is_dirty(&self) -> bool {
        self.states.as_ref().map(|s| s.is_dirty()).unwrap_or(false)
    }
}

impl<S: StateStore> Executor for AggSimpleAggExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        panic!("Should execute by wrapper")
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.info.identity.as_str()
    }

    fn clear_cache(&mut self) -> Result<()> {
        assert!(
            !self.is_dirty(),
            "cannot clear cache while states of simple agg are dirty"
        );
        self.states.take();
        Ok(())
    }
}

#[async_trait]
impl<S: StateStore> AggExecutor for AggSimpleAggExecutor<S> {
    async fn apply_chunk(&mut self, chunk: StreamChunk, epoch: u64) -> StreamExecutorResult<()> {
        let (ops, columns, visibility) = chunk.into_inner();

        // --- Retrieve all aggregation inputs in advance ---
        let all_agg_input_arrays = agg_input_array_refs(&self.agg_calls, &columns);
        let pk_input_arrays = pk_input_array_refs(&self.info.pk_indices, &columns);
        let input_pk_data_types = self
            .info
            .pk_indices
            .iter()
            .map(|idx| self.info.schema.fields[*idx].data_type.clone())
            .collect();

        // When applying batch, we will send columns of primary keys to the last N columns.
        let all_agg_data = all_agg_input_arrays
            .into_iter()
            .map(|mut input_arrays| {
                input_arrays.extend(pk_input_arrays.iter());
                input_arrays
            })
            .collect_vec();

        // 1. Retrieve previous state from the KeyedState. If they didn't exist, the ManagedState
        // will automatically create new ones for them.
        if self.states.is_none() {
            let state = generate_agg_state(
                None,
                &self.agg_calls,
                &self.keyspace,
                input_pk_data_types,
                epoch,
            )
            .await?;
            self.states = Some(state);
        }
        let states = self.states.as_mut().unwrap();

        // 2. Mark the state as dirty by filling prev states
        states
            .may_mark_as_dirty(epoch)
            .await
            .map_err(StreamExecutorError::agg_state_error)?;

        // 3. Apply batch to each of the state (per agg_call)
        for (agg_state, data) in states.managed_states.iter_mut().zip_eq(all_agg_data.iter()) {
            agg_state
                .apply_batch(&ops, visibility.as_ref(), data, epoch)
                .await
                .map_err(StreamExecutorError::agg_state_error)?;
        }

        Ok(())
    }

    async fn flush_data(&mut self, epoch: u64) -> StreamExecutorResult<Option<StreamChunk>> {
        // --- Flush states to the state store ---
        // Some state will have the correct output only after their internal states have been fully
        // flushed.

        let states = match self.states.as_mut() {
            Some(states) if states.is_dirty() => states,
            _ => return Ok(None), // Nothing to flush.
        };

        // TODO(partial checkpoint): use the table id obtained locally.
        let mut write_batch = self
            .keyspace
            .state_store()
            .start_write_batch(GLOBAL_STORAGE_TABLE_ID);
        for state in &mut states.managed_states {
            state
                .flush(&mut write_batch)
                .map_err(StreamExecutorError::agg_state_error)?;
        }
        write_batch
            .ingest(epoch)
            .await
            .map_err(StreamExecutorError::agg_state_error)?;

        // --- Create array builders ---
        // As the datatype is retrieved from schema, it contains both group key and aggregation
        // state outputs.
        let mut builders = self
            .schema
            .create_array_builders(2)
            .map_err(StreamExecutorError::eval_error)?;
        let mut new_ops = Vec::with_capacity(2);

        // --- Retrieve modified states and put the changes into the builders ---
        states
            .build_changes(&mut builders, &mut new_ops, epoch)
            .await
            .map_err(StreamExecutorError::agg_state_error)?;

        let columns: Vec<Column> = builders
            .into_iter()
            .map(|builder| -> Result<_> { Ok(Column::new(Arc::new(builder.finish()?))) })
            .try_collect()
            .map_err(StreamExecutorError::eval_error)?;

        let chunk = StreamChunk::new(new_ops, columns, None);

        Ok(Some(chunk))
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::StreamExt;
    use global_simple_agg::*;
    use risingwave_common::array::{I64Array, Op, Row};
    use risingwave_common::catalog::Field;
    use risingwave_common::column_nonnull;
    use risingwave_common::types::*;
    use risingwave_expr::expr::*;

    use crate::executor::AggArgs;
    use crate::executor_v2::test_utils::*;
    use crate::executor_v2::*;
    use crate::*;

    #[tokio::test]
    async fn test_local_simple_aggregation_in_memory() {
        test_local_simple_aggregation(create_in_memory_keyspace()).await
    }

    async fn test_local_simple_aggregation(keyspace: Keyspace<impl StateStore>) {
        let chunk1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I64Array, [100, 10, 4] },
                column_nonnull! { I64Array, [200, 14, 300] },
                // primary key column
                column_nonnull! { I64Array, [1001, 1002, 1003] },
            ],
            None,
        );
        let chunk2 = StreamChunk::new(
            vec![Op::Delete, Op::Delete, Op::Delete, Op::Insert],
            vec![
                column_nonnull! { I64Array, [100, 10, 4, 104] },
                column_nonnull! { I64Array, [200, 14, 300, 500] },
                // primary key column
                column_nonnull! { I64Array, [1001, 1002, 1003, 1004] },
            ],
            Some((vec![true, false, true, true]).try_into().unwrap()),
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                // primary key column`
                Field::unnamed(DataType::Int64),
            ],
        };

        let mut source = MockSource::new(schema, vec![2]); // pk
        source.push_barrier(1, false);
        source.push_chunks([chunk1].into_iter());
        source.push_barrier(2, false);
        source.push_chunks([chunk2].into_iter());
        source.push_barrier(3, false);

        // This is local simple aggregation, so we add another row count state
        let agg_calls = vec![
            AggCall {
                kind: AggKind::RowCount,
                args: AggArgs::None,
                return_type: DataType::Int64,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
            },
            AggCall {
                kind: AggKind::Min,
                args: AggArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
            },
        ];

        let simple_agg = Box::new(
            SimpleAggExecutor::new(Box::new(source), agg_calls, keyspace, vec![], 1, vec![])
                .unwrap(),
        );
        let mut simple_agg = simple_agg.execute();

        // Consume the init barrier
        simple_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = simple_agg.next().await.unwrap().unwrap();
        if let Message::Chunk(chunk) = msg {
            let (data_chunk, ops) = chunk.into_parts();
            let rows = ops
                .into_iter()
                .zip_eq(data_chunk.rows().map(Row::from))
                .collect_vec();
            let expected_rows = [(Op::Insert, row_nonnull![3_i64, 114_i64, 514_i64, 4_i64])];

            assert_eq!(rows, expected_rows);
        } else {
            unreachable!("unexpected message {:?}", msg);
        }

        assert_matches!(
            simple_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = simple_agg.next().await.unwrap().unwrap();
        if let Message::Chunk(chunk) = msg {
            let (data_chunk, ops) = chunk.into_parts();
            let rows = ops
                .into_iter()
                .zip_eq(data_chunk.rows().map(Row::from))
                .collect_vec();
            let expected_rows = [
                (
                    Op::UpdateDelete,
                    row_nonnull![3_i64, 114_i64, 514_i64, 4_i64],
                ),
                (
                    Op::UpdateInsert,
                    row_nonnull![2_i64, 114_i64, 514_i64, 10_i64],
                ),
            ];

            assert_eq!(rows, expected_rows);
        } else {
            unreachable!("unexpected message {:?}", msg);
        }
    }
}
