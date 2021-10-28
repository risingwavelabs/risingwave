use crate::array::{Array, I64Array};
use crate::expr::*;
use crate::stream_op::test_utils::*;
use crate::stream_op::*;
use crate::types::Int64Type;
use crate::*;
use itertools::Itertools;

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
        visibility: Some((vec![true, false, true]).try_into().unwrap()),
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

    let mut hash_agg = HashAggExecutor::new(Box::new(source), agg_calls, keys);

    if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
        // TODO: 1. refactor the whole test file in this style;
        // TODO: 2. move tests to corresponding function files as testing part rather than gather into a test file
        assert_eq!(chunk.ops, vec![Op::Insert, Op::Insert]);

        assert_eq!(chunk.columns.len(), 3);
        // test key
        assert_eq!(
            chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(2)]
        );
        // test count first row
        assert_eq!(
            chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(2)]
        );
        // test count(*)
        assert_eq!(
            chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(2)]
        );
    } else {
        unreachable!();
    }

    if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
        assert_eq!(
            chunk.ops,
            vec![Op::Delete, Op::UpdateDelete, Op::UpdateInsert]
        );

        assert_eq!(chunk.columns.len(), 3);
        // test key
        assert_eq!(
            chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(2), Some(2)]
        );
        // test count first row
        assert_eq!(
            chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(2), Some(1)]
        );
        // test count(*)
        assert_eq!(
            chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(2), Some(1)]
        );
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
            column_nonnull! { I64Array, Int64Type, [1, 2, 2, 3] },
            column_nonnull! { I64Array, Int64Type, [1, 2, 2, 3] },
        ],
        visibility: Some((vec![true, false, true, true]).try_into().unwrap()),
    };
    let source = MockSource::new(vec![chunk1, chunk2]);

    // This is local hash aggregation, so we add another sum state
    let key_indices = vec![0];
    let agg_calls = vec![
        AggCall {
            kind: AggKind::RowCount,
            args: AggArgs::None,
            return_type: Int64Type::create(false),
        },
        AggCall {
            kind: AggKind::Sum,
            args: AggArgs::Unary(Int64Type::create(false), 1),
            return_type: Int64Type::create(false),
        },
        // This is local hash aggregation, so we add another sum state
        AggCall {
            kind: AggKind::Sum,
            args: AggArgs::Unary(Int64Type::create(false), 2),
            return_type: Int64Type::create(false),
        },
    ];
    let mut hash_agg = HashAggExecutor::new(Box::new(source), agg_calls, key_indices);

    if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
        assert_eq!(chunk.ops, vec![Op::Insert, Op::Insert]);

        assert_eq!(chunk.columns.len(), 4);
        // test key_column
        assert_eq!(
            chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(2)]
        );
        assert_eq!(
            chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(2)],
        );
        // test agg_column
        assert_eq!(
            chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(4)]
        );
        // test row_sum_column
        assert_eq!(
            chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(4)]
        );
    } else {
        unreachable!();
    }

    if let Message::Chunk(chunk) = hash_agg.next().await.unwrap() {
        assert_eq!(
            chunk.ops,
            vec![Op::Delete, Op::UpdateDelete, Op::UpdateInsert, Op::Insert,]
        );

        assert_eq!(chunk.columns.len(), 4);

        // test key_column
        assert_eq!(
            chunk.columns[0].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(2), Some(2), Some(3)]
        );
        // test row_count
        assert_eq!(
            chunk.columns[1].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(2), Some(1), Some(1)]
        );
        // test agg_column
        assert_eq!(
            chunk.columns[2].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(4), Some(2), Some(3),]
        );
        // test row_sum_column
        assert_eq!(
            chunk.columns[3].array_ref().as_int64().iter().collect_vec(),
            vec![Some(1), Some(4), Some(2), Some(3),]
        );
    } else {
        unreachable!();
    }
}
