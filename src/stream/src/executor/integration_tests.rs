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

use std::sync::{Arc, Mutex};

use futures::channel::mpsc::channel;
use futures::SinkExt;
use risingwave_common::array::column::Column;
use risingwave_common::array::*;
use risingwave_common::catalog::Field;
use risingwave_common::types::*;
use risingwave_expr::expr::*;

use super::*;
use crate::executor::test_utils::create_in_memory_keyspace;
use crate::executor_v2::receiver::ReceiverExecutor;
use crate::executor_v2::{
    Executor as ExecutorV2, LocalSimpleAggExecutor, MergeExecutor, SimpleAggExecutor,
};
use crate::task::SharedContext;

pub struct MockConsumer {
    input: Box<dyn Executor>,
    data: Arc<Mutex<Vec<StreamChunk>>>,
}
impl std::fmt::Debug for MockConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MockConsumer")
            .field("input", &self.input)
            .finish()
    }
}

impl MockConsumer {
    pub fn new(input: Box<dyn Executor>, data: Arc<Mutex<Vec<StreamChunk>>>) -> Self {
        Self { input, data }
    }
}

#[async_trait]
impl StreamConsumer for MockConsumer {
    async fn next(&mut self) -> Result<Option<Barrier>> {
        match self.input.next().await? {
            Message::Chunk(chunk) => self.data.lock().unwrap().push(chunk),
            Message::Barrier(barrier) => return Ok(Some(barrier)),
        }
        Ok(None)
    }
}

/// This test creates a merger-dispatcher pair, and run a sum. Each chunk
/// has 0~9 elements. We first insert the 10 chunks, then delete them,
/// and do this again and again.
#[tokio::test]
async fn test_merger_sum_aggr() {
    // `make_actor` build an actor to do local aggregation
    let make_actor = |input_rx| {
        let schema = Schema {
            fields: vec![Field::unnamed(DataType::Int64)],
        };
        let input = Box::new(ReceiverExecutor::new(schema, vec![], input_rx)).v1();
        // for the local aggregator, we need two states: row count and sum
        let aggregator = Box::new(
            LocalSimpleAggExecutor::new_from_v1(
                Box::new(input),
                vec![
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
                ],
                vec![],
                1,
                "LocalSimpleAggExecutor".to_string(),
            )
            .unwrap(),
        )
        .v1();
        let (tx, rx) = channel(16);
        let consumer =
            SenderConsumer::new(Box::new(aggregator), Box::new(LocalOutput::new(233, tx)));
        let context = SharedContext::for_test().into();
        let actor = Actor::new(Box::new(consumer), 0, context);
        (actor, rx)
    };

    // join handles of all actors
    let mut handles = vec![];

    // input and output channels of the local aggregation actors
    let mut inputs = vec![];
    let mut outputs = vec![];

    let ctx = Arc::new(SharedContext::for_test());

    // create 17 local aggregation actors
    for _ in 0..17 {
        let (tx, rx) = channel(16);
        let (actor, channel) = make_actor(rx);
        outputs.push(channel);
        handles.push(tokio::spawn(actor.run()));
        inputs.push(Box::new(LocalOutput::new(233, tx)) as Box<dyn Output>);
    }

    // create a round robin dispatcher, which dispatches messages to the actors
    let (mut input, rx) = channel(16);
    let schema = Schema {
        fields: vec![Field::unnamed(DataType::Int64)],
    };
    let receiver_op = Box::new(ReceiverExecutor::new(schema.clone(), vec![], rx)).v1();
    let dispatcher = DispatchExecutor::new(
        Box::new(receiver_op),
        DispatcherImpl::RoundRobin(RoundRobinDataDispatcher::new(inputs)),
        0,
        ctx,
    );
    let context = SharedContext::for_test().into();
    let actor = Actor::new(Box::new(dispatcher), 0, context);
    handles.push(tokio::spawn(actor.run()));

    // use a merge operator to collect data from dispatchers before sending them to aggregator
    let merger = Box::new(MergeExecutor::new(schema, vec![], 0, outputs)).v1();

    // for global aggregator, we need to sum data and sum row count
    let aggregator = Box::new(
        SimpleAggExecutor::new_from_v1(
            Box::new(merger),
            vec![
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
            ],
            create_in_memory_keyspace(),
            vec![],
            2,
            "SimpleAggExecutor".to_string(),
            vec![],
        )
        .unwrap(),
    )
    .v1();

    let projection = ProjectExecutor::new(
        Box::new(aggregator),
        vec![],
        vec![
            // TODO: use the new streaming_if_null expression here, and add `None` tests
            Box::new(InputRefExpression::new(DataType::Int64, 1)),
        ],
        3,
        "ProjectExecutor".to_string(),
    );
    let items = Arc::new(Mutex::new(vec![]));
    let consumer = MockConsumer::new(Box::new(projection), items.clone());
    let context = SharedContext::for_test().into();
    let actor = Actor::new(Box::new(consumer), 0, context);
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
                vec![Column::new(Arc::new(
                    I64Array::from_slice(vec![Some(1); i].as_slice())
                        .unwrap()
                        .into(),
                ))],
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
            Barrier::new_test_barrier(epoch).with_mutation(Mutation::Stop(HashSet::from([0]))),
        ))
        .await
        .unwrap();

    // wait for all actors
    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    let data = items.lock().unwrap();
    let array = data.last().unwrap().column_at(0).array_ref().as_int64();
    assert_eq!(array.value_at(array.len() - 1), Some((0..10).sum()));
}
