use crate::array::*;
use crate::expr::*;
use crate::types::*;
use std::sync::{Arc, Mutex};

use super::ReceiverOperator;
use super::*;
use approx::assert_relative_eq;

use crate::stream_op::data_source::MockConsumer;
use futures::channel::mpsc::channel;
use futures::SinkExt;

/// This test creates a merger-dispatcher pair, and run a sum. Each chunk
/// has 0~9 elements. We first insert the 10 chunks, then delete them,
/// and do this again and again.
#[tokio::test]
async fn test_merger_sum_aggr() {
    // `make_actor` build an actor to do local aggregation
    let make_actor = |input_rx| {
        let input = ReceiverOperator::new(input_rx);
        // for the local aggregator, we need two states: sum and row count
        let aggregator = AggregationOperator::new(
            Box::new(input),
            vec![
                AggCall {
                    kind: AggKind::Sum,
                    args: AggArgs::Unary(Int64Type::create(false), 0),
                    return_type: Int64Type::create(false),
                },
                AggCall {
                    kind: AggKind::RowCount,
                    args: AggArgs::None,
                    return_type: Int64Type::create(false),
                },
            ],
        )
        .unwrap();
        let (tx, rx) = channel(16);
        let consumer = SenderConsumer::new(Box::new(aggregator), Box::new(ChannelOutput::new(tx)));
        let actor = Actor::new(Box::new(consumer));
        (actor, rx)
    };

    // join handles of all actors
    let mut handles = vec![];

    // input and output channels of the local aggregation actors
    let mut inputs = vec![];
    let mut outputs = vec![];

    // create 17 local aggregation actors
    for _ in 0..17 {
        let (tx, rx) = channel(16);
        let (actor, channel) = make_actor(rx);
        outputs.push(channel);
        handles.push(tokio::spawn(actor.run()));
        inputs.push(Box::new(ChannelOutput::new(tx)) as Box<dyn Output>);
    }

    // create a round robin dispatcher, which dispatches messages to the actors
    let (mut input, rx) = channel(16);
    let receiver_op = ReceiverOperator::new(rx);
    let dispatcher =
        DispatchOperator::new(Box::new(receiver_op), RoundRobinDataDispatcher::new(inputs));
    let actor = Actor::new(Box::new(dispatcher));
    handles.push(tokio::spawn(actor.run()));

    // use a merge operator to collect data from dispatchers before sending them to aggregator
    let merger = MergeOperator::new(outputs);

    // for global aggregator, we need to sum data and sum row count
    let aggregator = AggregationOperator::new(
        Box::new(merger),
        vec![
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
        ],
    )
    .unwrap();
    let projection = ProjectionOperator::new(
        Box::new(aggregator),
        vec![
            // TODO: use the new streaming_if_null expression here, and add `None` tests
            Box::new(InputRefExpression::new(Int64Type::create(false), 0)),
        ],
    );
    let items = Arc::new(Mutex::new(vec![]));
    let consumer = MockConsumer::new(Box::new(projection), items.clone());
    let actor = Actor::new(Box::new(consumer));
    handles.push(tokio::spawn(actor.run()));

    for j in 0..11 {
        let op;
        if j % 2 == 0 {
            op = Op::Insert;
        } else {
            op = Op::Delete;
        }
        for i in 0..10 {
            let chunk = StreamChunk {
                ops: (0..i).map(|_| op).collect::<Vec<_>>(),
                columns: vec![Column::new(
                    Arc::new(
                        I64Array::from_slice(
                            (0..i).map(|_| Some(1)).collect::<Vec<_>>().as_slice(),
                        )
                        .unwrap()
                        .into(),
                    ),
                    Int64Type::create(false),
                )],
                visibility: None,
            };
            input.send(Message::Chunk(chunk)).await.unwrap();
        }
        input.send(Message::Barrier(j)).await.unwrap();
    }
    input.send(Message::Terminate).await.unwrap();

    // wait for all actors
    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    let data = items.lock().unwrap();
    let array = data.last().unwrap().columns[0].array_ref().as_int64();
    assert_eq!(array.value_at(array.len() - 1), Some((0..10).sum()));
}

fn str_to_timestamp(elem: &str) -> i64 {
    use crate::vector_op::cast::str_to_timestamp;
    str_to_timestamp(elem).unwrap()
}

fn make_tpchq6_expr() -> (
    DataTypeRef,
    DataTypeRef,
    DataTypeRef,
    DataTypeRef,
    ConjunctionExpression,
    ArithmeticExpression,
) {
    let const_1994_01_01 = LiteralExpression::new(
        StringType::create(true, 20, DataTypeKind::Char),
        Some(ScalarImpl::UTF8("1994-01-01 00:00:00".to_string())),
    );
    let const_1995_01_01 = LiteralExpression::new(
        StringType::create(true, 20, DataTypeKind::Char),
        Some(ScalarImpl::UTF8("1995-01-01 00:00:00".to_string())),
    );
    let const_0_05 =
        LiteralExpression::new(Float64Type::create(false), Some(ScalarImpl::Float64(0.05)));
    let const_0_07 =
        LiteralExpression::new(Float64Type::create(false), Some(ScalarImpl::Float64(0.07)));
    let const_24 = LiteralExpression::new(Int32Type::create(false), Some(ScalarImpl::Int32(24)));
    let t_shipdate = TimestampType::create(false, 10);
    let l_shipdate = InputRefExpression::new(t_shipdate.clone(), 0);
    let l_shipdate_2 = InputRefExpression::new(t_shipdate.clone(), 0);
    let t_discount = Float64Type::create(false);
    let l_discount = InputRefExpression::new(t_discount.clone(), 1);
    let l_discount_2 = InputRefExpression::new(t_discount.clone(), 1);
    let l_discount_3 = InputRefExpression::new(t_discount.clone(), 1);
    let t_quantity = Int32Type::create(false);
    let l_quantity = InputRefExpression::new(t_quantity.clone(), 2);
    let t_extended_price = Float64Type::create(false);
    let l_extended_price = InputRefExpression::new(t_extended_price.clone(), 3);

    let l_shipdate_geq_cast =
        TypeCastExpression::new(TimestampType::create(false, 10), Box::new(const_1994_01_01));

    let l_shipdate_le_cast =
        TypeCastExpression::new(TimestampType::create(false, 10), Box::new(const_1995_01_01));

    let l_shipdate_geq = CompareExpression::new(
        BoolType::create(false),
        CompareOperatorKind::GreaterThanOrEqual,
        Box::new(l_shipdate),
        Box::new(l_shipdate_geq_cast),
    );

    let l_shipdate_le = CompareExpression::new(
        BoolType::create(false),
        CompareOperatorKind::LessThan,
        Box::new(l_shipdate_2),
        Box::new(l_shipdate_le_cast),
    );

    let l_discount_geq = CompareExpression::new(
        BoolType::create(false),
        CompareOperatorKind::GreaterThanOrEqual,
        Box::new(l_discount),
        Box::new(const_0_05),
    );

    let l_discount_leq = CompareExpression::new(
        BoolType::create(false),
        CompareOperatorKind::LessThanOrEqual,
        Box::new(l_discount_2),
        Box::new(const_0_07),
    );

    let l_quantity_le = CompareExpression::new(
        BoolType::create(false),
        CompareOperatorKind::LessThan,
        Box::new(l_quantity),
        Box::new(const_24),
    );

    let and = ConjunctionExpression::new(
        BoolType::create(false),
        ConjunctionOperatorKind::And,
        Box::new(l_shipdate_geq),
        Some(Box::new(l_shipdate_le)),
    );

    let and = ConjunctionExpression::new(
        BoolType::create(false),
        ConjunctionOperatorKind::And,
        Box::new(and),
        Some(Box::new(l_discount_geq)),
    );

    let and = ConjunctionExpression::new(
        BoolType::create(false),
        ConjunctionOperatorKind::And,
        Box::new(and),
        Some(Box::new(l_discount_leq)),
    );

    let and = ConjunctionExpression::new(
        BoolType::create(false),
        ConjunctionOperatorKind::And,
        Box::new(and),
        Some(Box::new(l_quantity_le)),
    );

    let multiply = ArithmeticExpression::new(
        Float64Type::create(false),
        ArithmeticOperatorKind::Multiply,
        Box::new(l_extended_price),
        Box::new(l_discount_3),
    );

    (
        t_shipdate,
        t_discount,
        t_quantity,
        t_extended_price,
        and,
        multiply,
    )
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
//
// Columns:
// 0. l_shipdate DATETIME
// 1. l_discount DOUBLE
// 2. l_quantity INTEGER
// 3. l_extendedprice DOUBLE
#[tokio::test]
async fn test_tpch_q6() {
    let (t_shipdate, t_discount, t_quantity, t_extended_price, _, _) = make_tpchq6_expr();

    // make an actor after dispatcher, which includes filter, projection, and local aggregator.
    let make_actor = |input_rx| {
        let (_, _, _, _, and, multiply) = make_tpchq6_expr();
        let input = ReceiverOperator::new(input_rx);

        let filter = FilterOperator::new(Box::new(input), Box::new(and));
        let projection = ProjectionOperator::new(Box::new(filter), vec![Box::new(multiply)]);

        // for local aggregator, we need to sum data and count rows
        let aggregator = AggregationOperator::new(
            Box::new(projection),
            vec![
                AggCall {
                    kind: AggKind::Sum,
                    args: AggArgs::Unary(Float64Type::create(false), 0),
                    return_type: Float64Type::create(false),
                },
                AggCall {
                    kind: AggKind::RowCount,
                    args: AggArgs::None,
                    return_type: Int64Type::create(false),
                },
            ],
        )
        .unwrap();
        let (tx, rx) = channel(16);
        let consumer = SenderConsumer::new(Box::new(aggregator), Box::new(ChannelOutput::new(tx)));
        let actor = Actor::new(Box::new(consumer));
        (actor, rx)
    };

    // join handles of all actors
    let mut handles = vec![];

    // input and output channels of the local aggregation actors
    let mut inputs = vec![];
    let mut outputs = vec![];

    // create 10 actors
    for _ in 0..10 {
        let (tx, rx) = channel(16);
        let (actor, channel) = make_actor(rx);
        outputs.push(channel);
        handles.push(tokio::spawn(actor.run()));
        inputs.push(Box::new(ChannelOutput::new(tx)) as Box<dyn Output>);
    }

    // create a round robin dispatcher, which dispatches messages to the actors
    let (mut input, rx) = channel(16);
    let receiver_op = ReceiverOperator::new(rx);
    let dispatcher =
        DispatchOperator::new(Box::new(receiver_op), RoundRobinDataDispatcher::new(inputs));
    let actor = Actor::new(Box::new(dispatcher));
    handles.push(tokio::spawn(actor.run()));

    // use a merge operator to collect data from dispatchers before sending them to aggregator
    let merger = MergeOperator::new(outputs);

    // create a global aggregator to sum data and sum row count
    let aggregator = AggregationOperator::new(
        Box::new(merger),
        vec![
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(Float64Type::create(false), 0),
                return_type: Float64Type::create(false),
            },
            AggCall {
                kind: AggKind::Sum,
                args: AggArgs::Unary(Int64Type::create(false), 1),
                return_type: Int64Type::create(false),
            },
        ],
    )
    .unwrap();
    let projection = ProjectionOperator::new(
        Box::new(aggregator),
        vec![
            // TODO: use the new streaming_if_null expression here, and add `None` tests
            Box::new(InputRefExpression::new(Float64Type::create(false), 0)),
        ],
    );

    let items = Arc::new(Mutex::new(vec![]));
    let consumer = MockConsumer::new(Box::new(projection), items.clone());
    let actor = Actor::new(Box::new(consumer));

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
    .map(Some)
    .collect::<Vec<_>>();
    let d_quantity = vec![20, 20, 20, 20, 20, 30, 30, 30, 30, 30] // first 5 elements matches condition on quantity
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();
    let d_extended_price = vec![10; 10].into_iter().map(Some).collect::<Vec<_>>();

    use super::Op::*;

    let make_chunk = |op: Op| StreamChunk {
        ops: vec![op; 10],
        visibility: None,
        columns: vec![
            Column::new(
                Arc::new(I64Array::from_slice(d_shipdate.as_slice()).unwrap().into()),
                t_shipdate.clone(),
            ),
            Column::new(
                Arc::new(F64Array::from_slice(d_discount.as_slice()).unwrap().into()),
                t_discount.clone(),
            ),
            Column::new(
                Arc::new(I64Array::from_slice(d_quantity.as_slice()).unwrap().into()),
                t_quantity.clone(),
            ),
            Column::new(
                Arc::new(
                    I64Array::from_slice(d_extended_price.as_slice())
                        .unwrap()
                        .into(),
                ),
                t_extended_price.clone(),
            ),
        ],
    };

    for i in 0..100 {
        input
            .send(Message::Chunk(make_chunk(Insert)))
            .await
            .unwrap();
        input
            .send(Message::Chunk(make_chunk(Delete)))
            .await
            .unwrap();
        if i % 10 == 0 {
            input.send(Message::Barrier(i / 10)).await.unwrap();
        }
    }

    input
        .send(Message::Chunk(make_chunk(Insert)))
        .await
        .unwrap();
    input.send(Message::Terminate).await.unwrap();

    // wait for all actors
    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    {
        let data = items.lock().unwrap();

        let result = data.iter().collect::<Vec<_>>();

        let chunk = result.first().unwrap();
        assert_eq!(chunk.ops, vec![Insert]);
        assert_eq!(chunk.columns.len(), 1);
        assert_eq!(
            chunk.columns[0].array_ref().as_float64().value_at(0),
            Some(1.1)
        );

        let chunk = result.last().unwrap();
        assert_eq!(chunk.ops, vec![UpdateDelete, UpdateInsert]);
        assert_eq!(chunk.columns.len(), 1);
        assert_relative_eq!(
            chunk.columns[0]
                .array_ref()
                .as_float64()
                .value_at(1)
                .unwrap(),
            1.1,
            epsilon = f64::EPSILON * 100.0
        );
    }
}
