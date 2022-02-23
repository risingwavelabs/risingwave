use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{
    agg_executor_next, create_streaming_agg_state, generate_agg_schema, AggCall, AggExecutor,
    Barrier, Executor, Message, PkIndices, PkIndicesRef, StreamingAggStateImpl,
};

#[derive(Debug)]
pub struct LocalSimpleAggExecutor {
    /// Schema of the executor.
    schema: Schema,

    /// Primary key indices.
    pk_indices: PkIndices,

    /// If `next_barrier_message` exists, we should send a Barrier while next called.
    // TODO: This can be optimized while async gen fn stablized.
    cached_barrier_message: Option<Barrier>,

    /// Aggregation states after last barrier.
    states: Vec<Box<dyn StreamingAggStateImpl>>,

    /// Represents whether there is new data in the epoch.
    is_dirty: bool,

    /// The input of the current operator.
    input: Box<dyn Executor>,

    /// An operator will support multiple aggregation calls.
    agg_calls: Vec<AggCall>,

    /// Identity string
    identity: String,

    /// Logical Operator Info
    op_info: String,
    /// Epoch
    epoch: Option<u64>,
}

impl LocalSimpleAggExecutor {
    pub fn new(
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        pk_indices: PkIndices,
        executor_id: u64,
        op_info: String,
    ) -> Result<Self> {
        // simple agg does not have group key
        let schema = generate_agg_schema(input.as_ref(), &agg_calls, None);
        let states: Vec<_> = agg_calls
            .iter()
            .map(|agg_call| {
                create_streaming_agg_state(
                    agg_call.args.arg_types(),
                    &agg_call.kind,
                    &agg_call.return_type,
                    None,
                )
            })
            .try_collect()?;
        Ok(Self {
            schema,
            pk_indices,
            cached_barrier_message: None,
            states,
            is_dirty: false,
            input,
            agg_calls,
            identity: format!("LocalSimpleAggExecutor {:X}", executor_id),
            op_info,
            epoch: None,
        })
    }
}

#[async_trait]
impl Executor for LocalSimpleAggExecutor {
    async fn next(&mut self) -> Result<Message> {
        agg_executor_next(self).await
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }

    fn logical_operator_info(&self) -> &str {
        &self.op_info
    }
}

#[async_trait]
impl AggExecutor for LocalSimpleAggExecutor {
    fn current_epoch(&self) -> Option<u64> {
        self.epoch
    }

    fn update_epoch(&mut self, new_epoch: u64) {
        self.epoch = Some(new_epoch);
    }

    fn cached_barrier_message_mut(&mut self) -> &mut Option<Barrier> {
        &mut self.cached_barrier_message
    }

    async fn apply_chunk(&mut self, chunk: StreamChunk) -> Result<()> {
        let (ops, columns, visibility) = chunk.into_inner();
        self.agg_calls
            .iter()
            .zip_eq(self.states.iter_mut())
            .try_for_each(|(agg_call, state)| {
                let cols = agg_call
                    .args
                    .val_indices()
                    .iter()
                    .map(|idx| columns[*idx].array_ref())
                    .collect_vec();
                state.apply_batch(&ops, visibility.as_ref(), &cols[..])
            })?;
        self.is_dirty = true;
        Ok(())
    }

    async fn flush_data(&mut self) -> Result<Option<StreamChunk>> {
        if !self.is_dirty {
            return Ok(None);
        }
        let mut builders = self.schema.create_array_builders(1)?;
        self.states
            .iter_mut()
            .zip_eq(builders.iter_mut())
            .try_for_each(|(state, builder)| -> Result<_> {
                builder.append_datum(&state.get_output()?)?;
                state.reset();
                Ok(())
            })?;
        let columns: Vec<Column> = builders
            .into_iter()
            .map(|builder| -> Result<_> { Ok(Column::new(Arc::new(builder.finish()?))) })
            .try_collect()?;
        let ops = vec![Op::Insert; 1];
        self.is_dirty = false;
        Ok(Some(StreamChunk::new(ops, columns, None)))
    }

    fn input(&mut self) -> &mut dyn Executor {
        self.input.as_mut()
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use itertools::Itertools;
    use risingwave_common::array::{I64Array, Op, Row, StreamChunk};
    use risingwave_common::column_nonnull;
    use risingwave_common::error::Result;
    use risingwave_common::expr::AggKind;
    use risingwave_common::types::DataType;

    use super::LocalSimpleAggExecutor;
    use crate::executor::test_utils::{schemas, MockSource};
    use crate::executor::{AggArgs, AggCall, Executor, Message};
    use crate::row_nonnull;

    #[tokio::test]
    async fn test_no_chunk() -> Result<()> {
        let schema = schemas::iii();
        let mut source = MockSource::new(schema, vec![2]);
        source.push_barrier(1, false);
        source.push_barrier(2, false);
        source.push_barrier(3, false);

        let agg_calls = vec![AggCall {
            kind: AggKind::RowCount,
            args: AggArgs::None,
            return_type: DataType::Int64,
        }];

        let mut simple_agg = LocalSimpleAggExecutor::new(
            Box::new(source),
            agg_calls,
            vec![],
            1,
            "LocalSimpleAggExecutor".to_string(),
        )?;

        assert_matches!(simple_agg.next().await.unwrap(), Message::Barrier { .. });
        assert_matches!(simple_agg.next().await.unwrap(), Message::Barrier { .. });
        assert_matches!(simple_agg.next().await.unwrap(), Message::Barrier { .. });

        Ok(())
    }

    #[tokio::test]
    async fn test_local_simple_agg() -> Result<()> {
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
        let schema = schemas::iii();

        let mut source = MockSource::new(schema, vec![2]); // pk
        source.push_chunks([chunk1].into_iter());
        source.push_barrier(1, false);
        source.push_chunks([chunk2].into_iter());
        source.push_barrier(2, false);

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
        ];

        let mut simple_agg = LocalSimpleAggExecutor::new(
            Box::new(source),
            agg_calls,
            vec![],
            1,
            "LocalSimpleAggExecutor".to_string(),
        )?;

        let msg = simple_agg.next().await?;
        if let Message::Chunk(chunk) = msg {
            let (data_chunk, ops) = chunk.into_parts();
            let rows = ops
                .into_iter()
                .zip_eq(data_chunk.rows().map(Row::from))
                .collect_vec();
            let expected_rows = [(Op::Insert, row_nonnull![3_i64, 114_i64, 514_i64])];

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
                .zip_eq(data_chunk.rows().map(Row::from))
                .collect_vec();
            let expected_rows = [(Op::Insert, row_nonnull![-1_i64, 0i64, 0i64])];

            assert_eq!(rows, expected_rows);
        } else {
            unreachable!("unexpected message {:?}", msg);
        }

        Ok(())
    }
}
