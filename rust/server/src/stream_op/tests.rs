use super::Result;
use super::{FilterOperator, ProjectionOperator};
use crate::array::{Array, I64Array};
use crate::buffer::Bitmap;
use crate::expr::*;
use crate::stream_op::*;
use crate::types::{ArithmeticOperatorKind, BoolType, Int64Type};
use crate::*;
use itertools::Itertools;
use std::collections::VecDeque;

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
async fn test_projection() {
    let chunk1 = StreamChunk {
        ops: vec![Op::Insert, Op::Insert, Op::Insert],
        columns: vec![
            column_nonnull! { I64Array, Int64Type, [1, 2, 3] },
            column_nonnull! { I64Array, Int64Type, [4, 5, 6] },
        ],
        visibility: None,
    };
    let chunk2 = StreamChunk {
        ops: vec![Op::Insert, Op::Delete],
        columns: vec![
            column_nonnull! { I64Array, Int64Type, [7, 3] },
            column_nonnull! { I64Array, Int64Type, [8, 6] },
        ],
        visibility: Some(Bitmap::from_vec(vec![true, true]).unwrap()),
    };
    let source = MockSource::new(vec![chunk1, chunk2]);

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

    let mut project = ProjectionOperator::new(Box::new(source), vec![Box::new(test_expr)]);

    if let Message::Chunk(chunk) = project.next().await.unwrap() {
        assert_eq!(chunk.ops, vec![Op::Insert, Op::Insert, Op::Insert]);
        assert_eq!(chunk.columns.len(), 1);
        assert_eq!(
            chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
            vec![Some(5), Some(7), Some(9)]
        );
    } else {
        unreachable!();
    }

    if let Message::Chunk(chunk) = project.next().await.unwrap() {
        assert_eq!(chunk.ops, vec![Op::Insert, Op::Delete]);
        assert_eq!(chunk.columns.len(), 1);
        assert_eq!(
            chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
            vec![Some(15), Some(9)]
        );
    } else {
        unreachable!();
    }

    matches!(project.next().await.unwrap(), Message::Terminate);
}

#[tokio::test]
async fn test_filter() {
    let chunk1 = StreamChunk {
        ops: vec![Op::Insert, Op::Insert, Op::Insert, Op::Delete],
        columns: vec![
            column_nonnull! { I64Array, Int64Type, [1, 5, 6, 7] },
            column_nonnull! { I64Array, Int64Type, [4, 2, 6, 5] },
        ],
        visibility: None,
    };
    let chunk2 = StreamChunk {
        ops: vec![
            Op::UpdateDelete, // true -> true
            Op::UpdateInsert, // expect UpdateDelete, UpdateInsert
            Op::UpdateDelete, // true -> false
            Op::UpdateInsert, // expect Delete
            Op::UpdateDelete, // false -> true
            Op::UpdateInsert, // expect Insert
            Op::UpdateDelete, // false -> false
            Op::UpdateInsert, // expect nothing
        ],
        columns: vec![
            column_nonnull! { I64Array, Int64Type, [5, 7, 5, 3, 3, 5, 3, 4] },
            column_nonnull! { I64Array, Int64Type, [3, 5, 3, 5, 5, 3, 5, 6] },
        ],
        visibility: None,
    };
    let source = MockSource::new(vec![chunk1, chunk2]);

    let left_type = Int64Type::create(false);
    let left_expr = InputRefExpression::new(left_type, 0);
    let right_type = Int64Type::create(false);
    let right_expr = InputRefExpression::new(right_type, 1);
    let test_expr = CompareExpression::new(
        BoolType::create(false),
        CompareOperatorKind::GreaterThan,
        Box::new(left_expr),
        Box::new(right_expr),
    );
    let mut filter = FilterOperator::new(Box::new(source), Box::new(test_expr));

    if let Message::Chunk(chunk) = filter.next().await.unwrap() {
        assert_eq!(
            chunk.ops,
            vec![Op::Insert, Op::Insert, Op::Insert, Op::Delete]
        );
        assert_eq!(chunk.columns.len(), 2);
        assert_eq!(
            chunk.visibility.unwrap().iter().collect_vec(),
            vec![false, true, false, true]
        );
    } else {
        unreachable!();
    }

    if let Message::Chunk(chunk) = filter.next().await.unwrap() {
        assert_eq!(chunk.columns.len(), 2);
        assert_eq!(
            chunk.visibility.unwrap().iter().collect_vec(),
            vec![true, true, true, false, false, true, false, false]
        );
        assert_eq!(
            chunk.ops,
            vec![
                Op::UpdateDelete,
                Op::UpdateInsert,
                Op::Delete,
                Op::UpdateInsert,
                Op::UpdateDelete,
                Op::Insert,
                Op::UpdateDelete,
                Op::UpdateInsert
            ]
        );
    } else {
        unreachable!();
    }

    matches!(filter.next().await.unwrap(), Message::Terminate);
}

#[tokio::test]
async fn test_local_hash_aggregation_count() {
    let chunk1 = StreamChunk {
        ops: vec![Op::Insert, Op::Insert, Op::Insert],
        columns: vec![column_nonnull! { I64Array, Int64Type, [1, 2, 2] }],
        visibility: None,
    };
    let chunk2 = StreamChunk {
        ops: vec![Op::Delete, Op::Delete, Op::Delete],
        columns: vec![column_nonnull! { I64Array, Int64Type, [1, 2, 2] }],
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

    let mut hash_agg = HashAggregationOperator::new(Box::new(source), agg_calls, keys);

    if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
        assert_eq!(chunk.ops.len(), 2);
        assert_eq!(chunk.ops[0], Op::Insert);
        assert_eq!(chunk.ops[1], Op::Insert);
        assert_eq!(chunk.columns.len(), 3);
        let key_column = chunk.columns[0].array_ref().as_int64();
        assert_eq!(key_column.value_at(0).unwrap(), 1);
        assert_eq!(key_column.value_at(1).unwrap(), 2);
        let agg_column = chunk.columns[1].array_ref().as_int64();
        assert_eq!(agg_column.value_at(0).unwrap(), 1);
        assert_eq!(agg_column.value_at(1).unwrap(), 2);
        let row_count_column = chunk.columns[2].array_ref().as_int64();
        assert_eq!(row_count_column.value_at(0).unwrap(), 1);
        assert_eq!(row_count_column.value_at(1).unwrap(), 2);
    } else {
        unreachable!();
    }

    if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
        assert_eq!(chunk.ops.len(), 4);
        assert_eq!(
            chunk.ops,
            vec![
                Op::UpdateDelete,
                Op::UpdateInsert,
                Op::UpdateDelete,
                Op::UpdateInsert
            ]
        );
        assert_eq!(chunk.columns.len(), 3);
        let key_column = chunk.columns[0].array_ref().as_int64();
        assert_eq!(key_column.len(), 4);
        let key_column = (0..4)
            .map(|idx| key_column.value_at(idx).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(key_column, vec![1, 1, 2, 2]);
        let agg_column = chunk.columns[1].array_ref().as_int64();
        assert_eq!(agg_column.len(), 4);
        let agg_column = (0..4)
            .map(|idx| agg_column.value_at(idx).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(agg_column, vec![1, 0, 2, 1]);
        let row_count_column = chunk.columns[2].array_ref().as_int64();
        assert_eq!(row_count_column.len(), 4);
        let row_count_column = (0..4)
            .map(|idx| row_count_column.value_at(idx).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(row_count_column, vec![1, 0, 2, 1]);
    } else {
        unreachable!();
    }
}

#[tokio::test]
async fn test_global_hash_aggregation_count() {
    let chunk1 = StreamChunk {
        ops: vec![Op::Insert, Op::Insert, Op::Insert],
        columns: vec![
            column_nonnull! { I64Array, Int64Type, [1, 2, 2] },
            column_nonnull! { I64Array, Int64Type, [1, 2, 2] },
            column_nonnull! { I64Array, Int64Type, [1, 2, 2] },
        ],
        visibility: None,
    };
    let chunk2 = StreamChunk {
        ops: vec![Op::Delete, Op::Delete, Op::Delete, Op::Insert],
        columns: vec![
            column_nonnull! { I64Array, Int64Type, [1, 2, 2, 3] },
            column_nonnull! { I64Array, Int64Type, [1, 2, 1, 3] },
            column_nonnull! { I64Array, Int64Type, [1, 2, 1, 3] },
        ],
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
    let mut hash_agg = HashAggregationOperator::new(Box::new(source), agg_calls, key_indices);

    if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
        assert_eq!(chunk.ops.len(), 2);
        assert_eq!(chunk.ops[0], Op::Insert);
        assert_eq!(chunk.ops[1], Op::Insert);
        assert_eq!(chunk.columns.len(), 3);
        let key_column = chunk.columns[0].array_ref().as_int64();
        assert_eq!(key_column.value_at(0).unwrap(), 1);
        assert_eq!(key_column.value_at(1).unwrap(), 2);
        let agg_column = chunk.columns[1].array_ref().as_int64();
        assert_eq!(agg_column.value_at(0).unwrap(), 1);
        assert_eq!(agg_column.value_at(1).unwrap(), 4);
        let row_sum_column = chunk.columns[2].array_ref().as_int64();
        assert_eq!(row_sum_column.value_at(0).unwrap(), 1);
        assert_eq!(row_sum_column.value_at(1).unwrap(), 4);
    } else {
        unreachable!();
    }

    if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
        assert_eq!(chunk.ops.len(), 5);
        assert_eq!(
            chunk.ops,
            vec![
                Op::UpdateDelete,
                Op::UpdateInsert,
                Op::UpdateDelete,
                Op::UpdateInsert,
                Op::Insert
            ]
        );
        assert_eq!(chunk.columns.len(), 3);
        let key_column = chunk.columns[0].array_ref().as_int64();
        assert_eq!(key_column.len(), 5);
        let key_column = (0..5)
            .map(|idx| key_column.value_at(idx).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(key_column, vec![1, 1, 2, 2, 3]);
        let agg_column = chunk.columns[1].array_ref().as_int64();
        assert_eq!(agg_column.len(), 5);
        let agg_column = (0..5)
            .map(|idx| agg_column.value_at(idx))
            .collect::<Vec<_>>();
        assert_eq!(
            agg_column,
            vec![Some(1), Some(0), Some(4), Some(3), Some(3)]
        );
        let row_sum_column = chunk.columns[2].array_ref().as_int64();
        assert_eq!(row_sum_column.len(), 5);
        let row_sum_column = (0..5)
            .map(|idx| row_sum_column.value_at(idx))
            .collect::<Vec<_>>();
        assert_eq!(
            row_sum_column,
            vec![Some(1), Some(0), Some(4), Some(3), Some(3)]
        );
    } else {
        unreachable!();
    }
}
