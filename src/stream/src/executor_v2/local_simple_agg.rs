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
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{Executor, ExecutorInfo, StreamExecutorResult};
use crate::executor::{
    create_streaming_agg_state, AggCall, PkIndicesRef, StreamingAggStateImpl,
};
use crate::executor_v2::agg::{generate_agg_schema, AggExecutor, AggExecutorWrapper};
use crate::executor_v2::error::StreamExecutorError;
use crate::executor_v2::{BoxedMessageStream, PkIndices};

pub type LocalSimpleAggExecutor = AggExecutorWrapper<AggLocalSimpleAggExecutor>;

impl LocalSimpleAggExecutor {
    pub fn new(
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        pk_indices: PkIndices,
        executor_id: u64,
    ) -> Result<Self> {
        let info = input.info();
        let schema = generate_agg_schema(input.as_ref(), &agg_calls, None);

        Ok(AggExecutorWrapper {
            input,
            inner: AggLocalSimpleAggExecutor::new(
                info,
                agg_calls,
                pk_indices,
                schema,
                executor_id,
            )?,
        })
    }
}

pub struct AggLocalSimpleAggExecutor {
    info: ExecutorInfo,

    /// Schema of the executor.
    schema: Schema,

    /// Primary key indices.
    pk_indices: PkIndices,

    /// Aggregation states after last barrier.
    states: Vec<Box<dyn StreamingAggStateImpl>>,

    /// Represents whether there is new data in the epoch.
    is_dirty: bool,

    /// An operator will support multiple aggregation calls.
    agg_calls: Vec<AggCall>,
}

impl AggLocalSimpleAggExecutor {
    pub fn new(
        input_info: ExecutorInfo,
        agg_calls: Vec<AggCall>,
        pk_indices: PkIndices,
        schema: Schema,
        executor_id: u64,
    ) -> Result<Self> {
        // simple agg does not have group key
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
            info: ExecutorInfo {
                schema: input_info.schema,
                pk_indices: input_info.pk_indices,
                identity: format!("LocalSimpleAggExecutor {:X}", executor_id),
            },
            schema,
            pk_indices,
            states,
            is_dirty: false,
            agg_calls,
        })
    }
}

impl Executor for AggLocalSimpleAggExecutor {
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
}

#[async_trait]
impl AggExecutor for AggLocalSimpleAggExecutor {
    async fn apply_chunk(&mut self, chunk: StreamChunk, _epoch: u64) -> StreamExecutorResult<()> {
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
            })
            .map_err(StreamExecutorError::agg_state_error)?;
        self.is_dirty = true;
        Ok(())
    }

    async fn flush_data(&mut self, _epoch: u64) -> StreamExecutorResult<Option<StreamChunk>> {
        if !self.is_dirty {
            return Ok(None);
        }
        let mut builders = self
            .schema
            .create_array_builders(1)
            .map_err(StreamExecutorError::eval_error)?;
        self.states
            .iter_mut()
            .zip_eq(builders.iter_mut())
            .try_for_each(|(state, builder)| -> Result<_> {
                let data = state.get_output()?;
                trace!("append_datum: {:?}", data);
                builder.append_datum(&data)?;
                state.reset();
                Ok(())
            })
            .map_err(StreamExecutorError::agg_state_error)?;
        let columns: Vec<Column> = builders
            .into_iter()
            .map(|builder| -> Result<_> { Ok(Column::new(Arc::new(builder.finish()?))) })
            .try_collect()
            .map_err(StreamExecutorError::eval_error)?;
        let ops = vec![Op::Insert; 1];
        self.is_dirty = false;
        Ok(Some(StreamChunk::new(ops, columns, None)))
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::{I64Array, Op, Row, StreamChunk};
    use risingwave_common::catalog::schema_test_utils;
    use risingwave_common::column_nonnull;
    use risingwave_common::error::Result;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::AggKind;

    use crate::executor::{AggArgs, AggCall, Message};
    use crate::executor_v2::test_utils::MockSource;
    use crate::executor_v2::{Executor, LocalSimpleAggExecutor};
    use crate::row_nonnull;

    #[tokio::test]
    async fn test_no_chunk() -> Result<()> {
        let schema = schema_test_utils::ii();
        let mut source = MockSource::new(schema, vec![2]);
        source.push_barrier(1, false);
        source.push_barrier(2, false);
        source.push_barrier(3, false);

        let agg_calls = vec![AggCall {
            kind: AggKind::RowCount,
            args: AggArgs::None,
            return_type: DataType::Int64,
        }];

        let simple_agg = Box::new(LocalSimpleAggExecutor::new(
            Box::new(source),
            agg_calls,
            vec![],
            1,
        )?);
        let mut simple_agg = simple_agg.execute();

        assert_matches!(
            simple_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );
        assert_matches!(
            simple_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );
        assert_matches!(
            simple_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

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
        let schema = schema_test_utils::iii();

        let mut source = MockSource::new(schema, vec![2]); // pk\
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
        ];

        let simple_agg = Box::new(LocalSimpleAggExecutor::new(
            Box::new(source),
            agg_calls,
            vec![],
            1,
        )?);
        let mut simple_agg = simple_agg.execute();

        // Consume the init barrier
        simple_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = simple_agg.next().await.unwrap().unwrap();
        println!("{:?}", msg);
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

        println!("{:?}", simple_agg.next().await.unwrap().unwrap());
        // assert_matches!(, Message::Barrier { .. });

        let msg = simple_agg.next().await.unwrap().unwrap();
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
