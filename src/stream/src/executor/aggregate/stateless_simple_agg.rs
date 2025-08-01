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

use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_expr::aggregate::{
    AggCall, AggregateState, BoxedAggregateFunction, build_retractable,
};

use super::agg_call_filter_res;
use crate::executor::prelude::*;

pub struct StatelessSimpleAggExecutor {
    _ctx: ActorContextRef,
    pub(super) input: Executor,
    pub(super) schema: Schema,
    pub(super) aggs: Vec<BoxedAggregateFunction>,
    pub(super) agg_calls: Vec<AggCall>,
}

impl Execute for StatelessSimpleAggExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

impl StatelessSimpleAggExecutor {
    async fn apply_chunk(
        agg_calls: &[AggCall],
        aggs: &[BoxedAggregateFunction],
        states: &mut [AggregateState],
        chunk: &StreamChunk,
    ) -> StreamExecutorResult<()> {
        for ((agg, call), state) in aggs.iter().zip_eq_fast(agg_calls).zip_eq_fast(states) {
            let vis = agg_call_filter_res(call, chunk).await?;
            let chunk = chunk.project_with_vis(call.args.val_indices(), vis);
            agg.update(state, &chunk).await?;
        }
        Ok(())
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let StatelessSimpleAggExecutor {
            _ctx,
            input,
            schema,
            aggs,
            agg_calls,
        } = self;
        let input = input.execute();
        let mut is_dirty = false;
        let mut states: Vec<_> = aggs.iter().map(|agg| agg.create_state()).try_collect()?;

        #[for_await]
        for msg in input {
            let msg = msg?;
            match msg {
                Message::Watermark(_) => {}
                Message::Chunk(chunk) => {
                    Self::apply_chunk(&agg_calls, &aggs, &mut states, &chunk).await?;
                    is_dirty = true;
                }
                m @ Message::Barrier(_) => {
                    if is_dirty {
                        is_dirty = false;

                        let mut builders = schema.create_array_builders(1);
                        for ((agg, state), builder) in aggs
                            .iter()
                            .zip_eq_fast(states.iter_mut())
                            .zip_eq_fast(builders.iter_mut())
                        {
                            let data = agg.get_result(state).await?;
                            *state = agg.create_state()?;
                            trace!("append: {:?}", data);
                            builder.append(data);
                        }
                        let columns = builders
                            .into_iter()
                            .map(|builder| Ok::<_, StreamExecutorError>(builder.finish().into()))
                            .try_collect()?;
                        let ops = vec![Op::Insert; 1];

                        yield Message::Chunk(StreamChunk::new(ops, columns));
                    }

                    yield m;
                }
            }
        }
    }
}

impl StatelessSimpleAggExecutor {
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        schema: Schema,
        agg_calls: Vec<AggCall>,
    ) -> StreamResult<Self> {
        let aggs = agg_calls.iter().map(build_retractable).try_collect()?;
        Ok(StatelessSimpleAggExecutor {
            _ctx: ctx,
            input,
            schema,
            aggs,
            agg_calls,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::schema_test_utils;
    use risingwave_common::util::epoch::test_epoch;

    use super::*;
    use crate::executor::test_utils::MockSource;
    use crate::executor::test_utils::agg_executor::generate_agg_schema;

    #[tokio::test]
    async fn test_no_chunk() {
        let schema = schema_test_utils::ii();
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, vec![2]);
        tx.push_barrier(test_epoch(1), false);
        tx.push_barrier(test_epoch(2), false);
        tx.push_barrier(test_epoch(3), false);

        let agg_calls = vec![AggCall::from_pretty("(count:int8)")];
        let schema = generate_agg_schema(&source, &agg_calls, None);

        let simple_agg =
            StatelessSimpleAggExecutor::new(ActorContext::for_test(123), source, schema, agg_calls)
                .unwrap();
        let mut simple_agg = simple_agg.boxed().execute();

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
        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, vec![2]);
        tx.push_barrier(test_epoch(1), false);
        tx.push_chunk(StreamChunk::from_pretty(
            "   I   I    I
            + 100 200 1001
            +  10  14 1002
            +   4 300 1003",
        ));
        tx.push_barrier(test_epoch(2), false);
        tx.push_chunk(StreamChunk::from_pretty(
            "   I   I    I
            - 100 200 1001
            -  10  14 1002 D
            -   4 300 1003
            + 104 500 1004",
        ));
        tx.push_barrier(test_epoch(3), false);

        let agg_calls = vec![
            AggCall::from_pretty("(count:int8)"),
            AggCall::from_pretty("(sum:int8 $0:int8)"),
            AggCall::from_pretty("(sum:int8 $1:int8)"),
        ];
        let schema = generate_agg_schema(&source, &agg_calls, None);

        let simple_agg =
            StatelessSimpleAggExecutor::new(ActorContext::for_test(123), source, schema, agg_calls)
                .unwrap();
        let mut simple_agg = simple_agg.boxed().execute();

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
