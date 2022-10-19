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
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;

use super::aggregation::agg_impl::{create_streaming_agg_impl, StreamingAggImpl};
use super::aggregation::{agg_call_filter_res, generate_agg_schema, AggCall};
use super::error::StreamExecutorError;
use super::*;
use crate::error::StreamResult;

pub struct LocalSimpleAggExecutor {
    ctx: ActorContextRef,
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

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

impl LocalSimpleAggExecutor {
    fn apply_chunk(
        ctx: &ActorContextRef,
        identity: &str,
        agg_calls: &[AggCall],
        aggregators: &mut [Box<dyn StreamingAggImpl>],
        chunk: StreamChunk,
    ) -> StreamExecutorResult<()> {
        let capacity = chunk.capacity();
        let (ops, columns, visibility) = chunk.into_inner();
        let visibilities: Vec<_> = agg_calls
            .iter()
            .map(|agg_call| {
                agg_call_filter_res(
                    ctx,
                    identity,
                    agg_call,
                    &columns,
                    visibility.as_ref(),
                    capacity,
                )
            })
            .try_collect()?;
        agg_calls
            .iter()
            .zip_eq(visibilities)
            .zip_eq(aggregators)
            .try_for_each(|((agg_call, visibility), state)| {
                let col_refs = agg_call
                    .args
                    .val_indices()
                    .iter()
                    .map(|idx| columns[*idx].array_ref())
                    .collect_vec();
                state.apply_batch(&ops, visibility.as_ref(), &col_refs)
            })?;
        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let LocalSimpleAggExecutor {
            ctx,
            input,
            info,
            agg_calls,
        } = self;
        let input = input.execute();
        let mut is_dirty = false;
        let mut aggregators: Vec<_> = agg_calls
            .iter()
            .map(|agg_call| {
                create_streaming_agg_impl(
                    agg_call.args.arg_types(),
                    &agg_call.kind,
                    &agg_call.return_type,
                    None,
                )
            })
            .try_collect()?;

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Chunk(chunk) => {
                    Self::apply_chunk(&ctx, &info.identity, &agg_calls, &mut aggregators, chunk)?;
                    is_dirty = true;
                }
                m @ Message::Barrier(_) => {
                    if is_dirty {
                        is_dirty = false;

                        let mut builders = info.schema.create_array_builders(1);
                        aggregators
                            .iter_mut()
                            .zip_eq(builders.iter_mut())
                            .try_for_each(|(state, builder)| {
                                let data = state.get_output()?;
                                trace!("append_datum: {:?}", data);
                                builder.append_datum(&data);
                                state.reset();
                                Ok::<_, StreamExecutorError>(())
                            })?;
                        let columns: Vec<Column> = builders
                            .into_iter()
                            .map(|builder| Ok::<_, StreamExecutorError>(builder.finish().into()))
                            .try_collect()?;
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
        ctx: ActorContextRef,
        input: Box<dyn Executor>,
        agg_calls: Vec<AggCall>,
        pk_indices: PkIndices,
        executor_id: u64,
    ) -> StreamResult<Self> {
        let schema = generate_agg_schema(input.as_ref(), &agg_calls, None);
        let info = ExecutorInfo {
            schema,
            pk_indices,
            identity: format!("LocalSimpleAggExecutor-{}", executor_id),
        };

        Ok(LocalSimpleAggExecutor {
            ctx,
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
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::schema_test_utils;
    use risingwave_common::types::DataType;
    use risingwave_expr::expr::AggKind;

    use super::*;
    use crate::executor::aggregation::{AggArgs, AggCall};
    use crate::executor::test_utils::MockSource;
    use crate::executor::{Executor, LocalSimpleAggExecutor};

    #[tokio::test]
    async fn test_no_chunk() {
        let schema = schema_test_utils::ii();
        let (mut tx, source) = MockSource::channel(schema, vec![2]);
        tx.push_barrier(1, false);
        tx.push_barrier(2, false);
        tx.push_barrier(3, false);

        let agg_calls = vec![AggCall {
            kind: AggKind::Count,
            args: AggArgs::None,
            return_type: DataType::Int64,
            order_pairs: vec![],
            append_only: false,
            filter: None,
        }];

        let simple_agg = Box::new(
            LocalSimpleAggExecutor::new(
                ActorContext::for_test(123),
                Box::new(source),
                agg_calls,
                vec![],
                1,
            )
            .unwrap(),
        );
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
    }

    #[tokio::test]
    async fn test_local_simple_agg() {
        let schema = schema_test_utils::iii();
        let (mut tx, source) = MockSource::channel(schema, vec![2]); // pk\
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
        let agg_calls = vec![
            AggCall {
                kind: AggKind::Count,
                args: AggArgs::None,
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only: false,
                filter: None,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only: false,
                filter: None,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                order_pairs: vec![],
                append_only: false,
                filter: None,
            },
        ];

        let simple_agg = Box::new(
            LocalSimpleAggExecutor::new(
                ActorContext::for_test(123),
                Box::new(source),
                agg_calls,
                vec![],
                1,
            )
            .unwrap(),
        );
        let mut simple_agg = simple_agg.execute();

        // Consume the init barrier
        simple_agg.next().await.unwrap().unwrap();
        // Consume stream chunk
        let msg = simple_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I   I   I
                + 3 114 514"
            )
        );

        assert_matches!(
            simple_agg.next().await.unwrap().unwrap(),
            Message::Barrier { .. }
        );

        let msg = simple_agg.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I
                + -1 0 0"
            )
        );
    }
}
