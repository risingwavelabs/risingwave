//! Streaming Aggregators

use std::sync::Arc;

use super::{aggregation::*, Barrier, Keyspace, StateStore};
use super::{Executor, Message};
use crate::stream_op::PKVec;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::*;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use async_trait::async_trait;

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
pub struct SimpleAggExecutor<S: StateStore> {
    /// Schema of the executor.
    schema: Schema,

    /// Primary key indices.
    pk_indices: PKVec,

    /// If `next_barrier_message` exists, we should send a Barrier while next called.
    // TODO: This can be optimized while async gen fn stablized.
    cached_barrier_message: Option<Barrier>,

    /// The executor operates on this keyspace.
    keyspace: Keyspace<S>,

    /// Aggregation states of the current operator.
    /// This is an `Option` and the initial state is built when `Executor::next` is called, since
    /// we may not want `Self::new` to be an `async` function.
    state: Option<AggState<S>>,

    /// The input of the current operator.
    input: Box<dyn Executor>,

    /// An operator will support multiple aggregation calls.
    agg_calls: Vec<AggCall>,
}

impl<S: StateStore> SimpleAggExecutor<S> {
    pub fn new(
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        keyspace: Keyspace<S>,
        pk_indices: PKVec,
    ) -> Self {
        let schema = generate_agg_schema(input.as_ref(), &agg_calls, None);

        Self {
            schema,
            pk_indices,
            cached_barrier_message: None,
            keyspace,
            state: None,
            input,
            agg_calls,
        }
    }
}

#[async_trait]
impl<S: StateStore> AggExecutor for SimpleAggExecutor<S> {
    fn cached_barrier_message_mut(&mut self) -> &mut Option<Barrier> {
        &mut self.cached_barrier_message
    }

    async fn apply_chunk(&mut self, chunk: StreamChunk) -> Result<()> {
        let StreamChunk {
            ops,
            columns,
            visibility,
        } = chunk;

        // --- Retrieve all aggregation inputs in advance ---
        let all_agg_input_arrays = agg_input_arrays(&self.agg_calls, &columns);

        // 1. Retrieve previous state from the KeyedState. If they didn't exist, the ManagedState
        // will automatically create new ones for them.
        if self.state.is_none() {
            let state =
                generate_agg_state(None, &self.agg_calls, &self.keyspace, self.pk_indices.len())
                    .await?;
            self.state = Some(state);
        }
        let state = self.state.as_mut().unwrap();

        // 2. Mark the state as dirty by filling prev states
        state.may_mark_as_dirty().await?;

        // 3. Apply batch to each of the state
        for (agg_state, input_arrays) in state
            .managed_states
            .iter_mut()
            .zip(all_agg_input_arrays.iter())
        {
            if input_arrays.is_empty() {
                agg_state
                    .apply_batch(&ops, visibility.as_ref(), &[])
                    .await?;
            } else {
                agg_state
                    .apply_batch(&ops, visibility.as_ref(), &[input_arrays[0]]) // TODO: multiple args
                    .await?;
            }
        }

        Ok(())
    }

    async fn flush_data(&mut self) -> Result<Option<StreamChunk>> {
        // --- Flush states to the state store ---
        // Some state will have the correct output only after their internal states have been fully
        // flushed.

        let states = match self.state.as_mut() {
            Some(states) if states.is_dirty() => states,
            _ => return Ok(None), // Nothing to flush.
        };

        let mut write_batch = vec![];
        for state in &mut states.managed_states {
            state.flush(&mut write_batch)?;
        }

        self.keyspace
            .state_store()
            .ingest_batch(write_batch)
            .await?;

        // --- Create array builders ---
        // As the datatype is retrieved from schema, it contains both group key and aggregation
        // state outputs.
        let mut builders = self.schema.create_array_builders(2)?;
        let mut new_ops = Vec::with_capacity(2);

        // --- Retrieve modified states and put the changes into the builders ---
        let _ = states
            .build_changes(&mut builders, &mut new_ops, None)
            .await?;

        let columns: Vec<Column> = builders
            .into_iter()
            .zip(self.schema.data_types_clone())
            .map(|(builder, data_type)| -> Result<_> {
                Ok(Column::new(Arc::new(builder.finish()?), data_type))
            })
            .try_collect()?;

        let chunk = StreamChunk {
            ops: new_ops,
            columns,
            visibility: None,
        };

        Ok(Some(chunk))
    }

    fn input(&mut self) -> &mut dyn Executor {
        self.input.as_mut()
    }
}

#[async_trait]
impl<S: StateStore> Executor for SimpleAggExecutor<S> {
    async fn next(&mut self) -> Result<Message> {
        agg_executor_next(self).await
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> &[usize] {
        &self.pk_indices
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use risingwave_common::catalog::Field;
    use risingwave_common::{column_nonnull, expr::*, types::*};

    use super::super::keyspace::MemoryStateStore;
    use super::*;
    use crate::stream_op::test_utils::*;
    use crate::stream_op::*;
    use crate::*;

    fn create_in_memory_keyspace() -> Keyspace<impl StateStore> {
        Keyspace::new(MemoryStateStore::new(), b"test_executor_2333".to_vec())
    }

    #[tokio::test]
    async fn test_local_simple_aggregation_in_memory() {
        test_local_simple_aggregation(create_in_memory_keyspace()).await
    }

    async fn test_local_simple_aggregation(keyspace: Keyspace<impl StateStore>) {
        let chunk1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [100, 10, 4] },
                column_nonnull! { I64Array, Int64Type, [200, 14, 300] },
            ],
            visibility: None,
        };
        let chunk2 = StreamChunk {
            ops: vec![Op::Delete, Op::Delete, Op::Delete, Op::Insert],
            columns: vec![
                column_nonnull! { I64Array, Int64Type, [100, 10, 4, 104] },
                column_nonnull! { I64Array, Int64Type, [200, 14, 300, 500] },
            ],
            visibility: Some((vec![true, false, true, true]).try_into().unwrap()),
        };
        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int64Type::create(false),
                },
                Field {
                    data_type: Int64Type::create(false),
                },
            ],
        };

        let mut source = MockSource::new(schema);
        source.push_chunks([chunk1].into_iter());
        source.push_barrier(1, false);
        source.push_chunks([chunk2].into_iter());
        source.push_barrier(2, false);

        // This is local simple aggregation, so we add another row count state
        let agg_calls = vec![
            AggCall {
                kind: AggKind::RowCount,
                args: AggArgs::None,
                return_type: Int64Type::create(false),
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(Int64Type::create(false), 0),
                return_type: Int64Type::create(false),
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(Int64Type::create(false), 1),
                return_type: Int64Type::create(false),
            },
            AggCall {
                kind: AggKind::Min,
                args: AggArgs::Unary(Int64Type::create(false), 0),
                return_type: Int64Type::create(false),
            },
        ];

        let mut simple_agg = SimpleAggExecutor::new(Box::new(source), agg_calls, keyspace, vec![]);

        let msg = simple_agg.next().await.unwrap();
        if let Message::Chunk(chunk) = msg {
            let (data_chunk, ops) = chunk.into_parts();
            let rows = ops
                .into_iter()
                .zip(data_chunk.rows().map(Row::from))
                .collect_vec();
            let expected_rows = [(Op::Insert, row_nonnull![3_i64, 114_i64, 514_i64, 4_i64])];

            assert_eq!(rows, expected_rows);
        } else {
            unreachable!("unexpected message {:?}", msg);
        }

        assert_matches!(simple_agg.next().await.unwrap(), Message::Barrier { .. });

        let msg = simple_agg.next().await.unwrap();
        if let Message::Chunk(chunk) = msg {
            let (data_chunk, ops) = chunk.into_parts();
            let rows = ops
                .into_iter()
                .zip(data_chunk.rows().map(Row::from))
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
