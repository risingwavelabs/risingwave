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

use futures::StreamExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::{BoxedMessageStream, Executor, ExecutorInfo, Message, StreamExecutorResult};
use crate::executor::{create_streaming_agg_state, AggCall, PkIndicesRef, StreamingAggStateImpl};
use crate::executor_v2::agg::generate_agg_schema;
use crate::executor_v2::error::{StreamExecutorError, TracedStreamExecutorError};
use crate::executor_v2::PkIndices;

pub struct LocalSimpleAggExecutor {
    pub(super) input: Box<dyn Executor>,
    pub(super) info: ExecutorInfo,
    pub(super) agg_calls: Vec<AggCall>,
}

impl Executor for LocalSimpleAggExecutor {
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

impl LocalSimpleAggExecutor {
    fn apply_chunk(
        agg_calls: &[AggCall],
        states: &mut [Box<dyn StreamingAggStateImpl>],
        chunk: StreamChunk,
    ) -> StreamExecutorResult<()> {
        let (ops, columns, visibility) = chunk.into_inner();
        agg_calls
            .iter()
            .zip_eq(states.iter_mut())
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
        Ok(())
    }

    #[try_stream(ok = Message, error = TracedStreamExecutorError)]
    async fn execute_inner(self) {
        let LocalSimpleAggExecutor {
            input,
            info,
            agg_calls,
        } = self;
        let input = input.execute();
        let mut is_dirty = false;
        let mut states: Vec<_> = agg_calls
            .iter()
            .map(|agg_call| {
                create_streaming_agg_state(
                    agg_call.args.arg_types(),
                    &agg_call.kind,
                    &agg_call.return_type,
                    None,
                )
            })
            .try_collect()
            .map_err(StreamExecutorError::agg_state_error)?;

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    Self::apply_chunk(&agg_calls, &mut states, chunk)?;
                    is_dirty = true;
                }
                m @ Message::Barrier(_) => {
                    if is_dirty {
                        is_dirty = false;

                        let mut builders = info
                            .schema
                            .create_array_builders(1)
                            .map_err(StreamExecutorError::eval_error)?;
                        states
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
                            .map(|builder| -> Result<_> {
                                Ok(Column::new(Arc::new(builder.finish()?)))
                            })
                            .try_collect()
                            .map_err(StreamExecutorError::eval_error)?;
                        let ops = vec![Op::Insert; 1];

                        yield Message::Chunk(StreamChunk::new(ops, columns, None));
                    }

                    yield m;
                }
            }
        }
    }
}

impl LocalSimpleAggExecutor {
    pub fn new(
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        pk_indices: PkIndices,
        executor_id: u64,
    ) -> Result<Self> {
        let schema = generate_agg_schema(input.as_ref(), &agg_calls, None);
        let info = ExecutorInfo {
            schema,
            pk_indices,
            identity: format!("LocalSimpleAggExecutor-{}", executor_id),
        };

        Ok(LocalSimpleAggExecutor {
            input,
            info,
            agg_calls,
        })
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
            let expected_rows = [(Op::Insert, row_nonnull![-1_i64, 0i64, 0i64])];

            assert_eq!(rows, expected_rows);
        } else {
            unreachable!("unexpected message {:?}", msg);
        }

        Ok(())
    }
}
