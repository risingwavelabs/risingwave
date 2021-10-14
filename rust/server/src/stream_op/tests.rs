use super::data_source::*;
use super::Result;
use super::{FilterOperator, ProjectionOperator};
use crate::array;
use crate::array::column::Column;
use crate::array::{Array, ArrayBuilder, I64Array, I64ArrayBuilder};
use crate::buffer::Bitmap;
use crate::expr::*;
use crate::stream_op::*;
use crate::types::{ArithmeticOperatorKind, BoolType, Int64Type};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

pub struct MockSource {
    chunks: VecDeque<StreamChunk>,
}

impl MockSource {
    pub fn new(chunks: Vec<StreamChunk>) -> Self {
        Self {
            chunks: chunks.into_iter().collect(),
        }
    }
}

#[async_trait]
impl StreamOperator for MockSource {
    async fn next(&mut self) -> Result<Message> {
        match self.chunks.pop_front() {
            Some(chunk) => Ok(Message::Chunk(chunk)),
            None => Ok(Message::Terminate),
        }
    }
}

#[tokio::test]
async fn test_projection() -> Result<()> {
    let start: i64 = 114514;
    let scalar: i64 = 1;
    let repeat: (i64, i64) = (-20, 20);
    let mock_data = MockData::new(start.., scalar, (repeat.0..repeat.1).cycle());
    let source = MockDataSource::new(mock_data);

    let left_type = Int64Type::create(false);
    let left_expr = InputRefExpression::new(left_type, 0);
    let right_type = Int64Type::create(false);
    let right_expr = InputRefExpression::new(right_type, 1);
    let test_expr = ArithmeticExpression::new(
        Int64Type::create(false),
        ArithmeticOperatorKind::Plus,
        Box::new(left_expr),
        Box::new(right_expr),
    );

    let projection_op = Box::new(ProjectionOperator::new(
        Box::new(source),
        vec![Box::new(test_expr)],
    ));

    let data = Arc::new(Mutex::new(vec![]));
    let mut consumer = MockConsumer::new(projection_op, data.clone());

    for _ in 0..10 {
        consumer.next().await?;
    }

    let data = data.lock().unwrap();
    let mut expected = start;
    for chunk in data.iter() {
        assert!(chunk.columns.len() == 1);
        let arr = chunk.columns[0].array_ref().as_int64();
        for i in 0..arr.len() {
            let v = arr.value_at(i).expect("arr[i] exists");
            assert_eq!(v, expected + scalar);
            expected += 1;
        }
    }
    println!("{} items collected.", expected - start);
    Ok(())
}

#[tokio::test]
async fn test_filter() -> Result<()> {
    let start: i64 = -1024;
    let scalar: i64 = 0;
    let repeat: (i64, i64) = (-20, 20);
    let mock_data = MockData::new(start.., scalar, (repeat.0..repeat.1).cycle());
    let source = MockDataSource::new(mock_data);

    let left_type = Int64Type::create(false);
    let left_expr = InputRefExpression::new(left_type, 1);
    let right_type = Int64Type::create(false);
    let right_expr = InputRefExpression::new(right_type, 2);
    let test_expr = CompareExpression::new(
        BoolType::create(false),
        CompareOperatorKind::GreaterThan,
        Box::new(left_expr),
        Box::new(right_expr),
    );
    let filter_op = FilterOperator::new(Box::new(source), Box::new(test_expr));

    let data = Arc::new(Mutex::new(vec![]));
    let mut consumer = MockConsumer::new(Box::new(filter_op), data.clone());

    for _ in 0..10 {
        consumer.next().await?;
    }

    let data = data.lock().unwrap();
    let mut items_collected = 0;
    for chunk in data.iter() {
        assert!(chunk.columns.len() == 3);
        let ops = &chunk.ops;
        let visibility = chunk.visibility.as_ref().unwrap();
        let vis_len = visibility.num_bits();
        let arr_scalar = chunk.columns[1].array_ref().as_int64();
        let arr_repeat = chunk.columns[2].array_ref().as_int64();
        assert_eq!(vis_len, ops.len());
        assert_eq!(vis_len, chunk.columns[0].array_ref().len());
        let mut bool_array = Vec::with_capacity(vis_len);
        for bit in visibility.iter() {
            bool_array.push(bit);
        }
        for i in 0..bool_array.len() {
            let bit = bool_array[i];
            assert_eq!(bit, arr_scalar.value_at(i) > arr_repeat.value_at(i));

            if ops[i] == Op::UpdateDelete && bit {
                assert!(i + 1 >= vis_len || !(ops[i + 1] == Op::UpdateInsert) || bool_array[i + 1]);
            }
            if ops[i] == Op::UpdateInsert && bit {
                assert!(i == 0 || !(ops[i - 1] == Op::UpdateDelete) || bool_array[i - 1]);
            }
            items_collected += 1;
        }
    }
    println!("{} items collected.", items_collected);
    Ok(())
}

#[tokio::test]
async fn test_local_hash_aggregation_count() {
    let ops1 = vec![Op::Insert, Op::Insert, Op::Insert];
    let array1 = array! { I64Array, [Some(1), Some(2), Some(2)] };
    let column1 = Column::new(Arc::new(array1.into()), Int64Type::create(false));
    let chunk1 = StreamChunk {
        ops: ops1,
        columns: vec![column1],
        visibility: None,
    };

    let ops2 = vec![Op::Delete, Op::Delete, Op::Delete];
    let mut builder2 = I64ArrayBuilder::new(0).unwrap();
    builder2.append(Some(1)).unwrap();
    builder2.append(Some(2)).unwrap();
    builder2.append(Some(2)).unwrap();
    let array2 = builder2.finish().unwrap();
    let column2 = Column::new(Arc::new(array2.into()), Arc::new(Int64Type::new(false)));
    let chunk2 = StreamChunk {
        ops: ops2,
        columns: vec![column2],
        visibility: Some(Bitmap::from_vec(vec![true, false, true]).unwrap()),
    };

    let source = MockSource::new(vec![chunk1, chunk2]);

    // This is local hash aggregation, so we add another row count state
    let keys = vec![0];
    let agg_calls = vec![
        AggCall {
            kind: AggKind::Count,
            args: AggArgs::Unary(Int64Type::create(false), 0),
            return_type: Int64Type::create(false),
        },
        // This is local hash aggregation, so we add another row count state
        AggCall {
            kind: AggKind::Count,
            args: AggArgs::None,
            return_type: Int64Type::create(false),
        },
    ];
    let hash_aggregator = HashAggregationOperator::new(Box::new(source), agg_calls, keys);

    let data = Arc::new(Mutex::new(Vec::new()));
    let mut consumer = MockConsumer::new(Box::new(hash_aggregator), data.clone());

    assert!(consumer.next().await.unwrap());
    assert_eq!(data.lock().unwrap().len(), 1);
    let real_output = data.lock().unwrap().drain(0..=0).next().unwrap();
    assert_eq!(real_output.ops.len(), 2);
    assert_eq!(real_output.ops[0], Op::Insert);
    assert_eq!(real_output.ops[1], Op::Insert);
    assert_eq!(real_output.columns.len(), 3);
    let key_column = real_output.columns[0].array_ref().as_int64();
    assert_eq!(key_column.value_at(0).unwrap(), 1);
    assert_eq!(key_column.value_at(1).unwrap(), 2);
    let agg_column = real_output.columns[1].array_ref().as_int64();
    assert_eq!(agg_column.value_at(0).unwrap(), 1);
    assert_eq!(agg_column.value_at(1).unwrap(), 2);
    let row_count_column = real_output.columns[2].array_ref().as_int64();
    assert_eq!(row_count_column.value_at(0).unwrap(), 1);
    assert_eq!(row_count_column.value_at(1).unwrap(), 2);

    assert!(consumer.next().await.unwrap());
    assert_eq!(data.lock().unwrap().len(), 1);
    let real_output = data.lock().unwrap().drain(0..=0).next().unwrap();
    assert_eq!(real_output.ops.len(), 4);
    assert_eq!(
        real_output.ops,
        vec![
            Op::UpdateDelete,
            Op::UpdateInsert,
            Op::UpdateDelete,
            Op::UpdateInsert
        ]
    );
    assert_eq!(real_output.columns.len(), 3);
    let key_column = real_output.columns[0].array_ref().as_int64();
    assert_eq!(key_column.len(), 4);
    let key_column = (0..4)
        .map(|idx| key_column.value_at(idx).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(key_column, vec![1, 1, 2, 2]);
    let agg_column = real_output.columns[1].array_ref().as_int64();
    assert_eq!(agg_column.len(), 4);
    let agg_column = (0..4)
        .map(|idx| agg_column.value_at(idx).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(agg_column, vec![1, 0, 2, 1]);
    let row_count_column = real_output.columns[2].array_ref().as_int64();
    assert_eq!(row_count_column.len(), 4);
    let row_count_column = (0..4)
        .map(|idx| row_count_column.value_at(idx).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(row_count_column, vec![1, 0, 2, 1]);
}

#[tokio::test]
async fn test_global_hash_aggregation_count() {
    let ops1 = vec![Op::Insert, Op::Insert, Op::Insert];
    let key_array1 = array! { I64Array, [Some(1), Some(2), Some(2)] };
    let key_column1 = Column::new(Arc::new(key_array1.into()), Int64Type::create(false));
    let value_array1 = array! { I64Array, [Some(1), Some(2), Some(2)] };
    let value_column1 = Column::new(Arc::new(value_array1.into()), Int64Type::create(false));
    let row_count_array1 = array! { I64Array, [Some(1), Some(2), Some(2)] };
    let row_count_column1 =
        Column::new(Arc::new(row_count_array1.into()), Int64Type::create(false));
    let chunk1 = StreamChunk {
        ops: ops1,
        columns: vec![key_column1, value_column1, row_count_column1],
        visibility: Some(Bitmap::from_vec(vec![true, true, true]).unwrap()),
    };

    let ops2 = vec![Op::Delete, Op::Delete, Op::Delete, Op::Insert];
    let key_array2 = array! { I64Array, [Some(1), Some(2), Some(2), Some(3)] };
    let key_column2 = Column::new(Arc::new(key_array2.into()), Int64Type::create(false));
    let value_array2 = array! { I64Array, [Some(1), Some(2), Some(1), Some(3)] };
    let value_column2 = Column::new(Arc::new(value_array2.into()), Int64Type::create(false));
    let row_count_array2 = array! { I64Array, [Some(1), Some(2), Some(1), Some(3)] };
    let row_count_column2 =
        Column::new(Arc::new(row_count_array2.into()), Int64Type::create(false));
    let chunk2 = StreamChunk {
        ops: ops2,
        columns: vec![key_column2, value_column2, row_count_column2],
        visibility: Some(Bitmap::from_vec(vec![true, false, true, true]).unwrap()),
    };

    let source = MockSource::new(vec![chunk1, chunk2]);

    // This is local hash aggreagtion, so we add another sum state
    let key_indices = vec![0];
    let agg_calls = vec![
        AggCall {
            kind: AggKind::Sum,
            args: AggArgs::Unary(Int64Type::create(false), 1),
            return_type: Int64Type::create(false),
        },
        // This is local hash aggreagtion, so we add another sum state
        AggCall {
            kind: AggKind::Sum,
            args: AggArgs::Unary(Int64Type::create(false), 2),
            return_type: Int64Type::create(false),
        },
    ];
    let hash_aggregator = HashAggregationOperator::new(Box::new(source), agg_calls, key_indices);

    let data = Arc::new(Mutex::new(Vec::new()));
    let mut consumer = MockConsumer::new(Box::new(hash_aggregator), data.clone());

    assert!(consumer.next().await.unwrap());
    assert_eq!(data.lock().unwrap().len(), 1);
    let real_output = data.lock().unwrap().drain(0..=0).next().unwrap();
    assert_eq!(real_output.ops.len(), 2);
    assert_eq!(real_output.ops[0], Op::Insert);
    assert_eq!(real_output.ops[1], Op::Insert);
    assert_eq!(real_output.columns.len(), 3);
    let key_column = real_output.columns[0].array_ref().as_int64();
    assert_eq!(key_column.value_at(0).unwrap(), 1);
    assert_eq!(key_column.value_at(1).unwrap(), 2);
    let agg_column = real_output.columns[1].array_ref().as_int64();
    assert_eq!(agg_column.value_at(0).unwrap(), 1);
    assert_eq!(agg_column.value_at(1).unwrap(), 4);
    let row_sum_column = real_output.columns[2].array_ref().as_int64();
    assert_eq!(row_sum_column.value_at(0).unwrap(), 1);
    assert_eq!(row_sum_column.value_at(1).unwrap(), 4);

    assert!(consumer.next().await.unwrap());
    assert_eq!(data.lock().unwrap().len(), 1);
    let real_output = data.lock().unwrap().drain(0..=0).next().unwrap();
    assert_eq!(real_output.ops.len(), 5);
    assert_eq!(
        real_output.ops,
        vec![
            Op::UpdateDelete,
            Op::UpdateInsert,
            Op::UpdateDelete,
            Op::UpdateInsert,
            Op::Insert
        ]
    );
    assert_eq!(real_output.columns.len(), 3);
    let key_column = real_output.columns[0].array_ref().as_int64();
    assert_eq!(key_column.len(), 5);
    let key_column = (0..5)
        .map(|idx| key_column.value_at(idx).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(key_column, vec![1, 1, 2, 2, 3]);
    let agg_column = real_output.columns[1].array_ref().as_int64();
    assert_eq!(agg_column.len(), 5);
    let agg_column = (0..5)
        .map(|idx| agg_column.value_at(idx))
        .collect::<Vec<_>>();
    assert_eq!(
        agg_column,
        vec![Some(1), Some(0), Some(4), Some(3), Some(3)]
    );
    let row_sum_column = real_output.columns[2].array_ref().as_int64();
    assert_eq!(row_sum_column.len(), 5);
    let row_sum_column = (0..5)
        .map(|idx| row_sum_column.value_at(idx))
        .collect::<Vec<_>>();
    assert_eq!(
        row_sum_column,
        vec![Some(1), Some(0), Some(4), Some(3), Some(3)]
    );
}
