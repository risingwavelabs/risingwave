use std::sync::{Arc, Mutex};

use approx::assert_relative_eq;
use futures::channel::mpsc::channel;
use futures::SinkExt;
use risingwave_common::array::column::Column;
use risingwave_common::array::Op::*;
use risingwave_common::array::*;
use risingwave_common::catalog::Field;
use risingwave_common::expr::expr_binary_nonnull::new_binary_expr;
use risingwave_common::expr::expr_binary_nullable::new_nullable_binary_expr;
use risingwave_common::expr::expr_unary::new_unary_expr;
use risingwave_common::expr::*;
use risingwave_common::types::*;
use risingwave_pb::expr::expr_node::Type;

use super::*;
use crate::executor::test_utils::create_in_memory_keyspace;
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
        let input = ReceiverExecutor::new(schema, vec![], input_rx, "ReceiverExecutor".to_string());
        // for the local aggregator, we need two states: row count and sum
        let aggregator = LocalSimpleAggExecutor::new(
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
        .unwrap();
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
    let receiver_op =
        ReceiverExecutor::new(schema.clone(), vec![], rx, "ReceiverExecutor".to_string());
    let dispatcher = DispatchExecutor::new(
        Box::new(receiver_op),
        RoundRobinDataDispatcher::new(inputs),
        0,
        ctx,
    );
    let context = SharedContext::for_test().into();
    let actor = Actor::new(Box::new(dispatcher), 0, context);
    handles.push(tokio::spawn(actor.run()));

    // use a merge operator to collect data from dispatchers before sending them to aggregator
    let merger = MergeExecutor::new(schema, vec![], 0, outputs, "MergerExecutor".to_string());

    // for global aggregator, we need to sum data and sum row count
    let aggregator = SimpleAggExecutor::new(
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
    );

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

    let mut epoch = 0;
    input
        .send(Message::Barrier(Barrier::new(epoch)))
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
            .send(Message::Barrier(Barrier::new(epoch)))
            .await
            .unwrap();
        epoch += 1;
    }
    input
        .send(Message::Barrier(
            Barrier::new(epoch).with_mutation(Mutation::Stop(HashSet::from([0]))),
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

fn str_to_timestamp(elem: &str) -> NaiveDateTimeWrapper {
    use risingwave_common::vector_op::cast::str_to_timestamp;
    str_to_timestamp(elem).unwrap()
}

fn make_tpchq6_expr() -> (BoxedExpression, BoxedExpression) {
    let const_1994_01_01 = LiteralExpression::new(
        DataType::Char,
        Some(ScalarImpl::Utf8("1994-01-01 00:00:00".to_string())),
    );
    let const_1995_01_01 = LiteralExpression::new(
        DataType::Char,
        Some(ScalarImpl::Utf8("1995-01-01 00:00:00".to_string())),
    );
    let const_0_05 =
        LiteralExpression::new(DataType::Float64, Some(ScalarImpl::Float64(0.05.into())));
    let const_0_07 =
        LiteralExpression::new(DataType::Float64, Some(ScalarImpl::Float64(0.07.into())));
    let const_24 = LiteralExpression::new(DataType::Int32, Some(ScalarImpl::Int32(24)));
    let t_shipdate = DataType::Timestamp;
    let l_shipdate = InputRefExpression::new(t_shipdate.clone(), 0);
    let l_shipdate_2 = InputRefExpression::new(t_shipdate, 0);
    let t_discount = DataType::Float64;
    let l_discount = InputRefExpression::new(t_discount.clone(), 1);
    let l_discount_2 = InputRefExpression::new(t_discount.clone(), 1);
    let l_discount_3 = InputRefExpression::new(t_discount, 1);
    let t_quantity = DataType::Float64;
    let l_quantity = InputRefExpression::new(t_quantity, 2);
    let t_extended_price = DataType::Float64;
    let l_extended_price = InputRefExpression::new(t_extended_price, 3);

    let l_shipdate_geq_cast =
        new_unary_expr(Type::Cast, DataType::Timestamp, Box::new(const_1994_01_01));

    let l_shipdate_le_cast =
        new_unary_expr(Type::Cast, DataType::Timestamp, Box::new(const_1995_01_01));

    let l_shipdate_geq = new_binary_expr(
        Type::GreaterThanOrEqual,
        DataType::Boolean,
        Box::new(l_shipdate),
        l_shipdate_geq_cast,
    );

    let l_shipdate_le = new_binary_expr(
        Type::LessThanOrEqual,
        DataType::Boolean,
        Box::new(l_shipdate_2),
        l_shipdate_le_cast,
    );

    let l_discount_geq = new_binary_expr(
        Type::GreaterThanOrEqual,
        DataType::Boolean,
        Box::new(l_discount),
        Box::new(const_0_05),
    );

    let l_discount_leq = new_binary_expr(
        Type::LessThanOrEqual,
        DataType::Boolean,
        Box::new(l_discount_2),
        Box::new(const_0_07),
    );

    let l_quantity_le = new_binary_expr(
        Type::LessThan,
        DataType::Boolean,
        Box::new(l_quantity),
        Box::new(const_24),
    );

    let and = new_nullable_binary_expr(Type::And, DataType::Boolean, l_shipdate_geq, l_shipdate_le);

    let and = new_nullable_binary_expr(Type::And, DataType::Boolean, and, l_discount_geq);

    let and = new_nullable_binary_expr(Type::And, DataType::Boolean, and, l_discount_leq);

    let and = new_nullable_binary_expr(Type::And, DataType::Boolean, and, l_quantity_le);

    let multiply = new_binary_expr(
        Type::Multiply,
        DataType::Float64,
        Box::new(l_extended_price),
        Box::new(l_discount_3),
    );

    (and, multiply)
}

// select
//   sum(l_extendedprice * l_discount) as revenue
// from
//   lineitem
// where
//   l_shipdate >= '1994-01-01'
//   and l_shipdate < '1995-01-01'
//   and l_discount between 0.05 and 0.07
//   and l_quantity < 24

// Columns:
// 0. l_shipdate DATETIME
// 1. l_discount DOUBLE
// 2. l_quantity INTEGER
// 3. l_extendedprice DOUBLE
#[tokio::test]
async fn test_tpch_q6() {
    let schema = Schema {
        fields: vec![
            Field::with_name(DataType::Timestamp, String::from("l_shipdate")),
            Field::with_name(DataType::Float64, String::from("l_discount")),
            Field::with_name(DataType::Float64, String::from("l_quantity")),
            Field::with_name(DataType::Float64, String::from("l_extendedprice")),
        ],
    };

    // make an actor after dispatcher, which includes filter, projection, and local aggregator.
    let make_actor = |input_rx| {
        let (and, multiply) = make_tpchq6_expr();
        let input = ReceiverExecutor::new(
            schema.clone(),
            vec![],
            input_rx,
            "ReceiverExecutor".to_string(),
        );

        let filter = FilterExecutor::new(Box::new(input), and, 1, "FilterExecutor".to_string());
        let projection = ProjectExecutor::new(
            Box::new(filter),
            vec![],
            vec![multiply],
            2,
            "ProjectExecutor".to_string(),
        );

        // for local aggregator, we need to sum data and count rows
        let aggregator = LocalSimpleAggExecutor::new(
            Box::new(projection),
            vec![
                AggCall {
                    kind: AggKind::RowCount,
                    args: AggArgs::None,
                    return_type: DataType::Int64,
                },
                AggCall {
                    kind: AggKind::Sum,
                    args: AggArgs::Unary(DataType::Float64, 0),
                    return_type: DataType::Float64,
                },
            ],
            vec![],
            3,
            "LocalSimpleAggExecutor".to_string(),
        )
        .unwrap();
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

    // create 10 actors
    for _ in 0..10 {
        let (tx, rx) = channel(16);
        let (actor, channel) = make_actor(rx);
        outputs.push(channel);
        handles.push(tokio::spawn(actor.run()));
        inputs.push(Box::new(LocalOutput::new(233, tx)) as Box<dyn Output>);
    }

    // create a round robin dispatcher, which dispatches messages to the actors
    let (mut input, rx) = channel(16);
    let receiver_op =
        ReceiverExecutor::new(schema.clone(), vec![], rx, "ReceiverExecutor".to_string());
    let dispatcher = DispatchExecutor::new(
        Box::new(receiver_op),
        RoundRobinDataDispatcher::new(inputs),
        0,
        ctx.clone(),
    );
    let context = SharedContext::for_test().into();
    let actor = Actor::new(Box::new(dispatcher), 0, context);
    handles.push(tokio::spawn(actor.run()));

    // use a merge operator to collect data from dispatchers before sending them to aggregator
    let merger = MergeExecutor::new(
        schema.clone(),
        vec![],
        0,
        outputs,
        "MergerExecutor".to_string(),
    );

    // create a global aggregator to sum data and sum row count
    let aggregator = SimpleAggExecutor::new(
        Box::new(merger),
        vec![
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Int64, 0),
                return_type: DataType::Int64,
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(DataType::Float64, 1),
                return_type: DataType::Float64,
            },
        ],
        create_in_memory_keyspace(),
        vec![],
        4,
        "SimpleAggExecutor".to_string(),
    );
    let projection = ProjectExecutor::new(
        Box::new(aggregator),
        vec![],
        vec![
            // TODO: use the new streaming_if_null expression here, and add `None` tests
            Box::new(InputRefExpression::new(DataType::Float64, 1)),
        ],
        5,
        "ProjectExecutor".to_string(),
    );

    let items = Arc::new(Mutex::new(vec![]));
    let consumer = MockConsumer::new(Box::new(projection), items.clone());
    let context = SharedContext::for_test().into();
    let actor = Actor::new(Box::new(consumer), 0, context);

    // start merger thread
    handles.push(tokio::spawn(actor.run()));

    let d_shipdate = vec![
        "1990-01-01 00:00:00",
        "1990-01-01 00:00:00",
        "1994-01-01 00:00:00", // this row (2) matches condition on l_shipdate
        "1994-01-01 00:00:00", // this row (3) matches condition on l_shipdate
        "1994-01-01 00:00:00", // this row (4) matches condition on l_shipdate
        "1995-01-01 00:00:00",
        "1996-01-01 00:00:00",
        "1996-01-01 00:00:00",
        "1996-01-01 00:00:00",
        "1996-01-01 00:00:00",
    ]
    .into_iter()
    .map(str_to_timestamp)
    .map(Some)
    .collect::<Vec<_>>();

    let d_discount = vec![
        0.055, 0.08, 0.055, 0.08, 0.055, 0.08, 0.055, 0.08, 0.055, 0.08,
    ] // odd rows matches condition on discount
    .into_iter()
    .map(|f| Some(f.into()))
    .collect::<Vec<_>>();

    let d_quantity = vec![20f64, 20.0, 20.0, 20.0, 20.0, 30.0, 30.0, 30.0, 30.0, 30.0] // first 5 elements matches condition on quantity
        .into_iter()
        .map(|f| Some(f.into()))
        .collect::<Vec<_>>();

    let d_extended_price = vec![10f64; 10]
        .into_iter()
        .map(|f| Some(f.into()))
        .collect::<Vec<_>>();

    let make_chunk = |op: Op| {
        StreamChunk::new(
            vec![op; 10],
            vec![
                Column::new(Arc::new(
                    NaiveDateTimeArray::from_slice(d_shipdate.as_slice())
                        .unwrap()
                        .into(),
                )),
                Column::new(Arc::new(
                    F64Array::from_slice(d_discount.as_slice()).unwrap().into(),
                )),
                Column::new(Arc::new(
                    F64Array::from_slice(d_quantity.as_slice()).unwrap().into(),
                )),
                Column::new(Arc::new(
                    F64Array::from_slice(d_extended_price.as_slice())
                        .unwrap()
                        .into(),
                )),
            ],
            None,
        )
    };

    input.send(Message::Barrier(Barrier::new(0))).await.unwrap();
    for i in 0..100 {
        input
            .send(Message::Chunk(make_chunk(Insert)))
            .await
            .unwrap();

        if i % 10 == 0 {
            input
                .send(Message::Barrier(Barrier {
                    epoch: (i / 10) + 1,
                    ..Barrier::default()
                }))
                .await
                .unwrap();
        }
    }

    input
        .send(Message::Chunk(make_chunk(Insert)))
        .await
        .unwrap();

    input
        .send(Message::Barrier(Barrier::new(100)))
        .await
        .unwrap();
    input
        .send(Message::Barrier(
            Barrier::new(200).with_mutation(Mutation::Stop(HashSet::from([0]))),
        ))
        .await
        .unwrap();

    // wait for all actors
    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    {
        let data = items.lock().unwrap();

        let result = data.iter().collect::<Vec<_>>();

        let chunk = result.first().unwrap();
        assert_eq!(chunk.ops(), vec![Insert]);
        assert_eq!(chunk.columns().len(), 1);
        assert_eq!(
            chunk.column_at(0).array_ref().as_float64().value_at(0),
            Some(1.1.into())
        );

        let chunk = result.last().unwrap();
        assert_eq!(chunk.ops(), vec![UpdateDelete, UpdateInsert]);
        assert_eq!(chunk.columns().len(), 1);
        assert_relative_eq!(
            chunk
                .column_at(0)
                .array_ref()
                .as_float64()
                .value_at(1)
                .unwrap()
                .0,
            111.1,
            epsilon = f64::EPSILON * 1000.0
        );
    }
}
