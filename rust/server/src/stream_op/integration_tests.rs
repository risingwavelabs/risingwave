use crate::array2::*;
use crate::expr::*;
use crate::types::*;
use std::sync::Arc;
use std::sync::Mutex;

use super::UnarySimpleProcessor;
use super::*;

use approx::assert_relative_eq;
use futures::channel::mpsc::channel;

/// `TestConsumer` is only used in tests, which collects all result into
/// a vector.
pub struct TestConsumer {
    items: Arc<Mutex<Vec<StreamChunk>>>,
}

impl TestConsumer {
    pub fn new(items: Arc<Mutex<Vec<StreamChunk>>>) -> Self {
        Self { items }
    }
}

#[async_trait]
impl UnaryStreamOperator for TestConsumer {
    async fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<()> {
        let mut items = self.items.lock().unwrap();
        items.push(chunk);
        Ok(())
    }
}

#[async_trait]
impl StreamOperator for TestConsumer {
    async fn consume_barrier(&mut self, _: u64) -> Result<()> {
        Ok(())
    }

    async fn consume_terminate(&mut self) -> Result<()> {
        Ok(())
    }
}

/// This test creates a merger-dispatcher pair, and run a sum. Each chunk
/// has 0~9 elements. We first insert the 10 chunks, then delete them,
/// and do this again and again.
#[tokio::test]
async fn test_merger_sum_aggr() {
    let make_actor = |sender| {
        let output = ChannelOutput::new(sender);
        // for the local aggregator, we need two states: sum and row count
        let aggregator = AggregationOperator::new(
            vec![
                Box::new(StreamingSumAgg::<I64Array>::new()),
                Box::new(StreamingRowCountAgg::new()),
            ],
            Box::new(output),
            vec![
                Arc::new(Int64Type::new(false)),
                Arc::new(Int64Type::new(false)),
            ],
            vec![0, 0],
        );
        let (tx, rx) = channel(16);
        let output = ChannelOutput::new(tx);
        (
            UnarySimpleProcessor::new(rx, Box::new(aggregator)),
            Box::new(output) as Box<dyn Output>,
        )
    };

    let mut rxs = vec![];
    let mut outputs = vec![];
    let mut handles = vec![];

    // create 17 actors
    for _ in 0..17 {
        let (tx, rx) = channel(16);
        let (consumer, actor) = make_actor(tx);
        outputs.push(actor);
        handles.push(tokio::spawn(consumer.run()));
        rxs.push(rx);
    }

    // create a round robin dispatcher, which dispatches messages to the actors
    let mut dispatcher = Dispatcher::new(RoundRobinDataDispatcher::new(outputs));

    // create a global aggregator and use a `TestConsumer` to collect all message
    let items = Arc::new(Mutex::new(vec![]));
    let consumer = TestConsumer::new(items.clone());
    let output = OperatorOutput::new(Box::new(consumer));
    let projection = ProjectionOperator::new(
        Box::new(output),
        vec![
            // TODO: use the new streaming_if_null expression here, and add `None` tests
            Box::new(InputRefExpression::new(Arc::new(Int64Type::new(false)), 0)),
        ],
    );
    let output = OperatorOutput::new(Box::new(projection));
    // for global aggregator, we need to sum data and sum row count
    let aggregator = AggregationOperator::new(
        vec![
            Box::new(StreamingSumAgg::<I64Array>::new()),
            Box::new(StreamingSumAgg::<I64Array>::new()),
        ],
        Box::new(output),
        vec![
            Arc::new(Int64Type::new(false)),
            Arc::new(Int64Type::new(false)),
        ],
        vec![0, 1],
    );

    // use a merger to collect data from dispatchers before sending them to aggregator
    let merger = UnaryMergeProcessor::new(rxs, Box::new(aggregator));

    // start merger thread
    let join_handle = tokio::spawn(merger.run());

    for j in 0..11 {
        let op;
        if j % 2 == 0 {
            op = Op::Insert;
        } else {
            op = Op::Delete;
        }
        for i in 0..10 {
            dispatcher
                .dispatch(Message::Chunk(StreamChunk {
                    ops: (0..i).map(|_| op).collect::<Vec<_>>(),
                    columns: vec![Column::new(
                        Arc::new(
                            I64Array::from_slice(
                                (0..i).map(|_| Some(1)).collect::<Vec<_>>().as_slice(),
                            )
                            .unwrap()
                            .into(),
                        ),
                        Arc::new(Int64Type::new(false)),
                    )],
                    visibility: None,
                }))
                .await
                .unwrap();
        }
        dispatcher.dispatch(Message::Barrier(j)).await.unwrap();
    }
    dispatcher.dispatch(Message::Terminate).await.unwrap();

    // wait for `Merger`
    join_handle.await.unwrap().unwrap();

    // wait for `Consumer`s
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
    let const_0_05 = LiteralExpression::new(
        Arc::new(Float64Type::new(false)),
        Some(ScalarImpl::Float64(0.05)),
    );
    let const_0_07 = LiteralExpression::new(
        Arc::new(Float64Type::new(false)),
        Some(ScalarImpl::Float64(0.07)),
    );
    let const_24 =
        LiteralExpression::new(Arc::new(Int32Type::new(false)), Some(ScalarImpl::Int32(24)));
    let t_shipdate = Arc::new(TimestampType::new(false, 10));
    let l_shipdate = InputRefExpression::new(t_shipdate.clone(), 0);
    let l_shipdate_2 = InputRefExpression::new(t_shipdate.clone(), 0);
    let t_discount = Arc::new(Float64Type::new(false));
    let l_discount = InputRefExpression::new(t_discount.clone(), 1);
    let l_discount_2 = InputRefExpression::new(t_discount.clone(), 1);
    let l_discount_3 = InputRefExpression::new(t_discount.clone(), 1);
    let t_quantity = Arc::new(Int32Type::new(false));
    let l_quantity = InputRefExpression::new(t_quantity.clone(), 2);
    let t_extended_price = Arc::new(Float64Type::new(false));
    let l_extended_price = InputRefExpression::new(t_extended_price.clone(), 3);

    let l_shipdate_geq_cast = TypeCastExpression::new(
        Arc::new(TimestampType::new(false, 10)),
        Box::new(const_1994_01_01),
    );

    let l_shipdate_le_cast = TypeCastExpression::new(
        Arc::new(TimestampType::new(false, 10)),
        Box::new(const_1995_01_01),
    );

    let l_shipdate_geq = CompareExpression::new(
        Arc::new(BoolType::new(false)),
        CompareOperatorKind::GreaterThanOrEqual,
        Box::new(l_shipdate),
        Box::new(l_shipdate_geq_cast),
    );

    let l_shipdate_le = CompareExpression::new(
        Arc::new(BoolType::new(false)),
        CompareOperatorKind::LessThan,
        Box::new(l_shipdate_2),
        Box::new(l_shipdate_le_cast),
    );

    let l_discount_geq = CompareExpression::new(
        Arc::new(BoolType::new(false)),
        CompareOperatorKind::GreaterThanOrEqual,
        Box::new(l_discount),
        Box::new(const_0_05),
    );

    let l_discount_leq = CompareExpression::new(
        Arc::new(BoolType::new(false)),
        CompareOperatorKind::LessThanOrEqual,
        Box::new(l_discount_2),
        Box::new(const_0_07),
    );

    let l_quantity_le = CompareExpression::new(
        Arc::new(BoolType::new(false)),
        CompareOperatorKind::LessThan,
        Box::new(l_quantity),
        Box::new(const_24),
    );

    let and = ConjunctionExpression::new(
        Arc::new(BoolType::new(false)),
        ConjunctionOperatorKind::And,
        Box::new(l_shipdate_geq),
        Some(Box::new(l_shipdate_le)),
    );

    let and = ConjunctionExpression::new(
        Arc::new(BoolType::new(false)),
        ConjunctionOperatorKind::And,
        Box::new(and),
        Some(Box::new(l_discount_geq)),
    );

    let and = ConjunctionExpression::new(
        Arc::new(BoolType::new(false)),
        ConjunctionOperatorKind::And,
        Box::new(and),
        Some(Box::new(l_discount_leq)),
    );

    let and = ConjunctionExpression::new(
        Arc::new(BoolType::new(false)),
        ConjunctionOperatorKind::And,
        Box::new(and),
        Some(Box::new(l_quantity_le)),
    );

    let multiply = ArithmeticExpression::new(
        Arc::new(Float64Type::new(false)),
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
    let make_actor = |sender| {
        let (_, _, _, _, and, multiply) = make_tpchq6_expr();
        let output = ChannelOutput::new(sender);
        // for local aggregator, we need to sum data and count rows
        let aggregator = AggregationOperator::new(
            vec![
                Box::new(StreamingFloatSumAgg::<F64Array>::new()),
                Box::new(StreamingRowCountAgg::new()),
            ],
            Box::new(output),
            vec![
                Arc::new(Float64Type::new(false)),
                Arc::new(Int64Type::new(false)),
            ],
            vec![0, 0],
        );
        let output = OperatorOutput::new(Box::new(aggregator));
        let projection = ProjectionOperator::new(Box::new(output), vec![Box::new(multiply)]);
        let output = OperatorOutput::new(Box::new(projection));
        let filter = FilterOperator::new(Box::new(output), Box::new(and));
        let (tx, rx) = channel(16);
        let output = ChannelOutput::new(tx);
        (
            UnarySimpleProcessor::new(rx, Box::new(filter)),
            Box::new(output) as Box<dyn Output>,
        )
    };

    let mut rxs = vec![];
    let mut outputs = vec![];
    let mut join_handles = vec![];

    // create 10 actors
    for _ in 0..10 {
        let (tx, rx) = channel(16);
        let (consumer, actor) = make_actor(tx);
        outputs.push(actor);
        join_handles.push(tokio::spawn(consumer.run()));
        rxs.push(rx);
    }

    // create a round robin dispatcher, which dispatches messages to the actors
    let mut dispatcher = Dispatcher::new(RoundRobinDataDispatcher::new(outputs));

    // create a global aggregator and use a `TestConsumer` to collect all message
    let items = Arc::new(Mutex::new(vec![]));
    let consumer = TestConsumer::new(items.clone());
    let output = OperatorOutput::new(Box::new(consumer));
    let projection = ProjectionOperator::new(
        Box::new(output),
        vec![
            // TODO: use the new streaming_if_null expression here, and add `None` tests
            Box::new(InputRefExpression::new(
                Arc::new(Float64Type::new(false)),
                0,
            )),
        ],
    );
    let output = OperatorOutput::new(Box::new(projection));
    // for global aggregator, we need to sum data and sum row count
    let aggregator = AggregationOperator::new(
        vec![
            Box::new(StreamingFloatSumAgg::<F64Array>::new()),
            Box::new(StreamingSumAgg::<I64Array>::new()),
        ],
        Box::new(output),
        vec![
            Arc::new(Float64Type::new(false)),
            Arc::new(Int64Type::new(false)),
        ],
        vec![0, 1],
    );

    // use a merger to collect data from dispatchers before sending them to aggregator
    let merger = UnaryMergeProcessor::new(rxs, Box::new(aggregator));

    // start merger thread
    let join_handle = tokio::spawn(merger.run());

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
        dispatcher
            .dispatch(Message::Chunk(make_chunk(Insert)))
            .await
            .unwrap();
        dispatcher
            .dispatch(Message::Chunk(make_chunk(Delete)))
            .await
            .unwrap();
        if i % 10 == 0 {
            dispatcher.dispatch(Message::Barrier(i / 10)).await.unwrap();
        }
    }

    dispatcher
        .dispatch(Message::Chunk(make_chunk(Insert)))
        .await
        .unwrap();
    dispatcher.dispatch(Message::Terminate).await.unwrap();

    // wait for `Merger`
    join_handle.await.unwrap().unwrap();

    // wait for `Processor`s
    for handle in join_handles {
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
