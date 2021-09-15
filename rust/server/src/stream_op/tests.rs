use crate::array2::*;
use crate::expr::*;
use crate::types::*;
use std::sync::Arc;
use std::sync::Mutex;

use super::*;

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
    async fn consume_barrier(&mut self, _epoch: u64) -> Result<()> {
        unreachable!()
    }
}

fn str_to_timestamp(elem: &str) -> i64 {
    use crate::vector_op::cast::str_to_timestamp;
    str_to_timestamp(elem).unwrap()
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
    let const_1994_01_01 = LiteralExpression::new(
        StringType::create(true, 20, DataTypeKind::Char),
        Some(ScalarImpl::UTF8("1994-01-01 00:00:00".to_string())),
    );
    let const_1995_01_01 = LiteralExpression::new(
        StringType::create(true, 20, DataTypeKind::Char),
        Some(ScalarImpl::UTF8("1995-01-01 00:00:00".to_string())),
    );
    let const_0_05 = LiteralExpression::new(
        Arc::new(Float32Type::new(false)),
        Some(ScalarImpl::Float32(0.05)),
    );
    let const_0_07 = LiteralExpression::new(
        Arc::new(Float32Type::new(false)),
        Some(ScalarImpl::Float32(0.07)),
    );
    let const_24 =
        LiteralExpression::new(Arc::new(Int32Type::new(false)), Some(ScalarImpl::Int32(24)));
    let t_shipdate = Arc::new(TimestampType::new(false, 10));
    let l_shipdate = InputRefExpression::new(t_shipdate.clone(), 0);
    let l_shipdate_2 = InputRefExpression::new(t_shipdate.clone(), 0);
    let t_discount = Arc::new(Float32Type::new(false));
    let l_discount = InputRefExpression::new(t_discount.clone(), 1);
    let l_discount_2 = InputRefExpression::new(t_discount.clone(), 1);
    let l_discount_3 = InputRefExpression::new(t_discount.clone(), 1);
    let t_quantity = Arc::new(Int32Type::new(false));
    let l_quantity = InputRefExpression::new(t_quantity.clone(), 2);
    let t_extended_price = Arc::new(Float32Type::new(false));
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
        Arc::new(Float32Type::new(false)),
        ArithmeticOperatorKind::Multiply,
        Box::new(l_extended_price),
        Box::new(l_discount_3),
    );

    let items = Arc::new(Mutex::new(vec![]));
    let consumer = TestConsumer::new(items.clone());
    let output = LocalOutput::new(Box::new(consumer));
    let aggregator = AggregationOperator::new(
        Box::new(StreamingFloatSumAgg::<F32Array>::new()),
        Box::new(output),
        Arc::new(Float32Type::new(false)),
    );
    let output = LocalOutput::new(Box::new(aggregator));
    let projection = ProjectionOperator::new(Box::new(output), vec![Box::new(multiply)]);
    let output = LocalOutput::new(Box::new(projection));
    let mut filter = FilterOperator::new(Box::new(output), Box::new(and));

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

    let chunk = StreamChunk {
        ops: vec![Insert; 10],
        visibility: None,
        cardinality: 10,
        columns: vec![
            Column::new(
                Arc::new(I64Array::from_slice(d_shipdate.as_slice()).unwrap().into()),
                t_shipdate,
            ),
            Column::new(
                Arc::new(F32Array::from_slice(d_discount.as_slice()).unwrap().into()),
                t_discount,
            ),
            Column::new(
                Arc::new(I64Array::from_slice(d_quantity.as_slice()).unwrap().into()),
                t_quantity,
            ),
            Column::new(
                Arc::new(
                    I64Array::from_slice(d_extended_price.as_slice())
                        .unwrap()
                        .into(),
                ),
                t_extended_price,
            ),
        ],
    };

    filter.consume_chunk(chunk).await.unwrap();

    {
        let data = items.lock().unwrap();
        // should insert 1 row 1 col, with data 0.55 * 10 * 2 = 1.1
        assert_eq!(data.len(), 1);
        let chunk = &data[0];
        assert_eq!(chunk.ops, vec![Insert]);
        assert_eq!(chunk.columns.len(), 1);
        assert_eq!(
            chunk.columns[0].array_ref().as_float32().value_at(0),
            Some(1.1)
        );
    }
}
