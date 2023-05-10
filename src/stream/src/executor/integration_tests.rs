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

use std::sync::{Arc, Mutex};

use anyhow::Context;
use futures::StreamExt;
use futures_async_stream::try_stream;
use multimap::MultiMap;
use risingwave_common::array::*;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::*;
use risingwave_expr::agg::{AggArgs, AggCall, AggKind};
use risingwave_expr::expr::*;
use risingwave_storage::memory::MemoryStateStore;

use super::exchange::permit::channel_for_test;
use super::*;
use crate::executor::actor::ActorContext;
use crate::executor::dispatch::*;
use crate::executor::exchange::output::{BoxedOutput, LocalOutput};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::receiver::ReceiverExecutor;
use crate::executor::test_utils::agg_executor::new_boxed_simple_agg_executor;
use crate::executor::{Executor, MergeExecutor, ProjectExecutor, StatelessSimpleAggExecutor};
use crate::task::SharedContext;

/// This test creates a merger-dispatcher pair, and run a sum. Each chunk
/// has 0~9 elements. We first insert the 10 chunks, then delete them,
/// and do this again and again.
#[tokio::test]
async fn test_merger_sum_aggr() {
    let actor_ctx = ActorContext::create(0);
    // `make_actor` build an actor to do local aggregation
    let make_actor = |input_rx| {
        let _schema = Schema {
            fields: vec![Field::unnamed(DataType::Int64)],
        };
        let input = ReceiverExecutor::for_test(input_rx);
        // for the local aggregator, we need two states: row count and sum
        let aggregator = StatelessSimpleAggExecutor::new(
            actor_ctx.clone(),
            input.boxed(),
            vec![
                AggCall {
                    kind: AggKind::Count,
                    args: AggArgs::None,
                    return_type: DataType::Int64,
                    column_orders: vec![],
                    filter: None,
                    distinct: false,
                },
                AggCall {
                    kind: AggKind::Sum,
                    args: AggArgs::Unary(DataType::Int64, 0),
                    return_type: DataType::Int64,
                    column_orders: vec![],
                    filter: None,
                    distinct: false,
                },
            ],
            vec![],
            1,
        )
        .unwrap();
        let (tx, rx) = channel_for_test();
        let consumer = SenderConsumer {
            input: aggregator.boxed(),
            channel: Box::new(LocalOutput::new(233, tx)),
        };
        let context = SharedContext::for_test().into();
        let actor = Actor::new(
            consumer,
            vec![],
            context,
            StreamingMetrics::unused().into(),
            actor_ctx.clone(),
        );
        (actor, rx)
    };

    // join handles of all actors
    let mut handles = vec![];

    // input and output channels of the local aggregation actors
    let mut inputs = vec![];
    let mut outputs = vec![];

    let ctx = Arc::new(SharedContext::for_test());
    let metrics = Arc::new(StreamingMetrics::unused());

    // create 17 local aggregation actors
    for _ in 0..17 {
        let (tx, rx) = channel_for_test();
        let (actor, channel) = make_actor(rx);
        outputs.push(channel);
        handles.push(tokio::spawn(actor.run()));
        inputs.push(Box::new(LocalOutput::new(233, tx)) as BoxedOutput);
    }

    // create a round robin dispatcher, which dispatches messages to the actors
    let (input, rx) = channel_for_test();
    let schema = Schema {
        fields: vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Int64),
        ],
    };
    let receiver_op = Box::new(ReceiverExecutor::for_test(rx));
    let dispatcher = DispatchExecutor::new(
        receiver_op,
        vec![DispatcherImpl::RoundRobin(RoundRobinDataDispatcher::new(
            inputs,
            vec![0],
            0,
        ))],
        0,
        ctx,
        metrics,
    );
    let context = SharedContext::for_test().into();
    let actor = Actor::new(
        dispatcher,
        vec![],
        context,
        StreamingMetrics::unused().into(),
        actor_ctx.clone(),
    );
    handles.push(tokio::spawn(actor.run()));

    // use a merge operator to collect data from dispatchers before sending them to aggregator
    let merger = MergeExecutor::for_test(outputs, schema);

    // for global aggregator, we need to sum data and sum row count
    let is_append_only = false;
    let aggregator = new_boxed_simple_agg_executor(
        actor_ctx.clone(),
        MemoryStateStore::new(),
        merger.boxed(),
        is_append_only,
        vec![
            AggCall {
                kind: AggKind::Sum0,
                args: AggArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
                column_orders: vec![],
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 1),
                return_type: DataType::Int64,
                column_orders: vec![],
                filter: None,
                distinct: false,
            },
            AggCall {
                kind: AggKind::Count, // as row count, index: 2
                args: AggArgs::None,
                return_type: DataType::Int64,
                column_orders: vec![],
                filter: None,
                distinct: false,
            },
        ],
        2, // row_count_index
        vec![],
        2,
    )
    .await;

    let projection = ProjectExecutor::new(
        actor_ctx.clone(),
        aggregator,
        vec![],
        vec![
            // TODO: use the new streaming_if_null expression here, and add `None` tests
            Box::new(InputRefExpression::new(DataType::Int64, 1)),
        ],
        3,
        MultiMap::new(),
        0.0,
    );

    let items = Arc::new(Mutex::new(vec![]));
    let consumer = MockConsumer {
        input: projection.boxed(),
        data: items.clone(),
    };
    let context = SharedContext::for_test().into();
    let actor = Actor::new(
        consumer,
        vec![],
        context,
        StreamingMetrics::unused().into(),
        actor_ctx.clone(),
    );
    handles.push(tokio::spawn(actor.run()));

    let mut epoch = 1;
    input
        .send(Message::Barrier(Barrier::new_test_barrier(epoch)))
        .await
        .unwrap();
    epoch += 1;
    for j in 0..11 {
        let op = if j % 2 == 0 { Op::Insert } else { Op::Delete };
        for i in 0..10 {
            let chunk = StreamChunk::new(
                vec![op; i],
                vec![I64Array::from_iter(vec![1; i]).into()],
                None,
            );
            input.send(Message::Chunk(chunk)).await.unwrap();
        }
        input
            .send(Message::Barrier(Barrier::new_test_barrier(epoch)))
            .await
            .unwrap();
        epoch += 1;
    }
    input
        .send(Message::Barrier(
            Barrier::new_test_barrier(epoch)
                .with_mutation(Mutation::Stop([0].into_iter().collect())),
        ))
        .await
        .unwrap();

    // wait for all actors
    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    let data = items.lock().unwrap();
    let array = data.last().unwrap().column_at(0).as_int64();
    assert_eq!(array.value_at(array.len() - 1), Some((0..10).sum()));
}

struct MockConsumer {
    input: BoxedExecutor,
    data: Arc<Mutex<Vec<StreamChunk>>>,
}

impl StreamConsumer for MockConsumer {
    type BarrierStream = impl Stream<Item = StreamResult<Barrier>> + Send;

    fn execute(self: Box<Self>) -> Self::BarrierStream {
        let mut input = self.input.execute();
        let data = self.data;
        #[try_stream]
        async move {
            while let Some(item) = input.next().await {
                match item? {
                    Message::Watermark(_) => {
                        todo!("https://github.com/risingwavelabs/risingwave/issues/6042")
                    }
                    Message::Chunk(chunk) => data.lock().unwrap().push(chunk),
                    Message::Barrier(barrier) => yield barrier,
                }
            }
        }
    }
}

/// `SenderConsumer` consumes data from input executor and send it into a channel.
pub struct SenderConsumer {
    input: BoxedExecutor,
    channel: BoxedOutput,
}

impl StreamConsumer for SenderConsumer {
    type BarrierStream = impl Stream<Item = StreamResult<Barrier>> + Send;

    fn execute(self: Box<Self>) -> Self::BarrierStream {
        let mut input = self.input.execute();
        let mut channel = self.channel;
        #[try_stream]
        async move {
            while let Some(item) = input.next().await {
                let msg = item?;
                let barrier = msg.as_barrier().cloned();

                channel.send(msg).await.context("failed to send message")?;

                if let Some(barrier) = barrier {
                    yield barrier;
                }
            }
        }
    }
}
