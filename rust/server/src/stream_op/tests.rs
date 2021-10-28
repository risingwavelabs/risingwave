use super::Result;
use super::{FilterExecutor, ProjectExecutor};
use crate::array::{Array, I32Array, I64Array};
use crate::catalog::TableId;
use crate::error::ErrorCode::InternalError;
use crate::expr::binary_expr::new_binary_expr;
use crate::expr::*;
use crate::storage::{Row, SimpleTableManager, SimpleTableRef, TableManager};
use crate::stream_op::*;
use crate::types::{BoolType, Int32Type, Int64Type, Scalar};
use crate::*;
use itertools::Itertools;
use pb_construct::make_proto;
use pb_convert::FromProtobuf;
use risingwave_proto::data::{DataType, DataType_TypeName};
use risingwave_proto::expr::ExprNode_Type;
use risingwave_proto::plan::{ColumnDesc, ColumnDesc_ColumnEncodingType};
use risingwave_proto::plan::{DatabaseRefId, SchemaRefId, TableRefId};
use smallvec::SmallVec;
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
impl Executor for MockSource {
    async fn next(&mut self) -> Result<Message> {
        match self.chunks.pop_front() {
            Some(chunk) => Ok(Message::Chunk(chunk)),
            None => Ok(Message::Terminate),
        }
    }
}

#[tokio::test]
async fn test_sink() {
    // Prepare storage and memtable.
    let store_mgr = Arc::new(SimpleTableManager::new());
    let table_ref_proto = make_proto!(TableRefId, {
        schema_ref_id: make_proto!(SchemaRefId, {
            database_ref_id: make_proto!(DatabaseRefId, {
                database_id: 0
            })
        }),
        table_id: 1
    });
    let table_id = TableId::from_protobuf(&table_ref_proto)
        .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))
        .unwrap();
    // Two columns of int32 type, the first column is PK.
    let column_desc1 = make_proto!(ColumnDesc,{
        column_type: make_proto!(DataType, { type_name: DataType_TypeName::INT32 }),
        encoding: ColumnDesc_ColumnEncodingType::RAW,
        name: "v1".to_string()
    });
    let column_desc2 = make_proto!(ColumnDesc,{
        column_type: make_proto!(DataType, { type_name: DataType_TypeName::INT32 }),
        encoding: ColumnDesc_ColumnEncodingType::RAW,
        name: "v2".to_string()
    });
    let column_descs = vec![column_desc1, column_desc2];
    let pks = vec![0_usize];
    let _res = store_mgr.create_materialized_view(&table_id, column_descs, pks.clone());
    // Prepare source chunks.
    let chunk1 = StreamChunk {
        ops: vec![Op::Insert, Op::Insert, Op::Insert],
        columns: vec![
            column_nonnull! { I32Array, Int32Type, [1, 2, 3] },
            column_nonnull! { I32Array, Int32Type, [4, 5, 6] },
        ],
        visibility: None,
    };
    let chunk2 = StreamChunk {
        ops: vec![Op::Insert, Op::Delete],
        columns: vec![
            column_nonnull! { I32Array, Int32Type, [7, 3] },
            column_nonnull! { I32Array, Int32Type, [8, 6] },
        ],
        visibility: None,
    };

    let table_ref = store_mgr.get_table(&table_id).unwrap();
    if let SimpleTableRef::Row(table) = table_ref {
        // Prepare stream executors.
        let source = MockSource::new(vec![chunk1, chunk2]);
        let mut sink_executor =
            Box::new(MViewSinkExecutor::new(Box::new(source), table.clone(), pks));

        // First stream chunk. We check the existence of (3) -> (3,6)
        if let Message::Chunk(_chunk) = sink_executor.next().await.unwrap() {
            let mut value_vec = SmallVec::new();
            value_vec.push(Some(3.to_scalar_value()));
            let value_row = Row(value_vec);
            let res_row = table.get(value_row);
            if let Ok(res_row_in) = res_row {
                let datum = res_row_in.unwrap().0.get(1).unwrap().clone();
                // Dirty trick to assert_eq between (&int32 and integer).
                let d_value = datum.unwrap().as_int32() + 1;
                assert_eq!(d_value, 7);
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }

        // Second stream chunk. We check the existence of (7) -> (7,8)
        if let Message::Chunk(_chunk) = sink_executor.next().await.unwrap() {
            // From (7) -> (7,8)
            let mut value_vec = SmallVec::new();
            value_vec.push(Some(7.to_scalar_value()));
            let value_row = Row(value_vec);
            let res_row = table.get(value_row);
            if let Ok(res_row_in) = res_row {
                let datum = res_row_in.unwrap().0.get(1).unwrap().clone();
                let d_value = datum.unwrap().as_int32() + 1;
                assert_eq!(d_value, 9);
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }
    } else {
        unreachable!();
    }
}

#[tokio::test]
async fn test_sink_no_key() {
    // Prepare storage and memtable.
    let store_mgr = Arc::new(SimpleTableManager::new());
    let table_ref_proto = make_proto!(TableRefId, {
        schema_ref_id: make_proto!(SchemaRefId, {
            database_ref_id: make_proto!(DatabaseRefId, {
                database_id: 0
            })
        }),
        table_id: 1
    });
    let table_id = TableId::from_protobuf(&table_ref_proto)
        .map_err(|e| InternalError(format!("Failed to parse table id: {:?}", e)))
        .unwrap();
    // Two columns of int32 type, no pk.
    let column_desc1 = make_proto!(ColumnDesc,{
        column_type: make_proto!(DataType, { type_name: DataType_TypeName::INT32 }),
        encoding: ColumnDesc_ColumnEncodingType::RAW,
        name: "v1".to_string()
    });
    let column_desc2 = make_proto!(ColumnDesc,{
        column_type: make_proto!(DataType, { type_name: DataType_TypeName::INT32 }),
        encoding: ColumnDesc_ColumnEncodingType::RAW,
        name: "v2".to_string()
    });
    let column_descs = vec![column_desc1, column_desc2];
    let _res = store_mgr.create_materialized_view(&table_id, column_descs, vec![]);
    // Prepare source chunks.
    let chunk1 = StreamChunk {
        ops: vec![Op::Insert, Op::Insert, Op::Insert],
        columns: vec![
            column_nonnull! { I32Array, Int32Type, [1, 2, 3] },
            column_nonnull! { I32Array, Int32Type, [4, 5, 6] },
        ],
        visibility: None,
    };
    let chunk2 = StreamChunk {
        ops: vec![Op::Insert, Op::Delete, Op::Insert],
        columns: vec![
            column_nonnull! { I32Array, Int32Type, [7, 3, 1] },
            column_nonnull! { I32Array, Int32Type, [8, 6, 4] },
        ],
        visibility: None,
    };
    // Prepare stream executors.
    let table_ref = store_mgr.get_table(&table_id).unwrap();
    if let SimpleTableRef::Row(table) = table_ref {
        let source = MockSource::new(vec![chunk1, chunk2]);
        let mut sink_executor = Box::new(MViewSinkExecutor::new(
            Box::new(source),
            table.clone(),
            vec![],
        ));

        // First stream chunk. We check the existence of (1,4) -> (1)
        if let Message::Chunk(_chunk) = sink_executor.next().await.unwrap() {
            let mut value_vec = SmallVec::new();
            value_vec.push(Some(1.to_scalar_value()));
            value_vec.push(Some(4.to_scalar_value()));
            let value_row = Row(value_vec);
            let res_row = table.get(value_row);
            if let Ok(res_row_in) = res_row {
                let datum = res_row_in.unwrap().0.get(0).unwrap().clone();
                // Dirty trick to assert_eq between (&int32 and integer).
                let d_value = datum.unwrap().as_int32() + 1;
                assert_eq!(d_value, 2);
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }

        // Second stream chunk. We check the existence of (1,4) -> (2)
        if let Message::Chunk(_chunk) = sink_executor.next().await.unwrap() {
            let mut value_vec = SmallVec::new();
            value_vec.push(Some(1.to_scalar_value()));
            value_vec.push(Some(4.to_scalar_value()));
            let value_row = Row(value_vec);
            let res_row = table.get(value_row);
            if let Ok(res_row_in) = res_row {
                let datum = res_row_in.unwrap().0.get(0).unwrap().clone();
                // Dirty trick to assert_eq between (&int32 and integer).
                let d_value = datum.unwrap().as_int32() + 1;
                assert_eq!(d_value, 3);
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }
    } else {
        unreachable!();
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
        visibility: Some((vec![true, true]).try_into().unwrap()),
    };
    let source = MockSource::new(vec![chunk1, chunk2]);

    let left_type = Int64Type::create(false);
    let left_expr = InputRefExpression::new(left_type, 0);
    let right_type = Int64Type::create(false);
    let right_expr = InputRefExpression::new(right_type, 1);
    let test_expr = new_binary_expr(
        ExprNode_Type::ADD,
        Int64Type::create(false),
        Box::new(left_expr),
        Box::new(right_expr),
    );

    let mut project = ProjectExecutor::new(Box::new(source), vec![test_expr]);

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
    let test_expr = new_binary_expr(
        ExprNode_Type::GREATER_THAN,
        BoolType::create(false),
        Box::new(left_expr),
        Box::new(right_expr),
    );
    let mut filter = FilterExecutor::new(Box::new(source), test_expr);

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
                Op::UpdateInsert,
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
