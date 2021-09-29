use super::data_source::*;
use super::Result;
use super::{FilterOperator, OperatorOutput, Output, ProjectionOperator};
use crate::array2::column::Column;
use crate::array2::{Array, ArrayBuilder, I32ArrayBuilder, I64ArrayBuilder};
use crate::buffer::Bitmap;
use crate::expr::*;
use crate::stream_op::*;
use crate::types::{ArithmeticOperatorKind, BoolType, Int32Type, Int64Type};
use crate::util::hash_util::CRC32FastBuilder;
use futures::channel::oneshot;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_projection() -> Result<()> {
    let start: i64 = 114514;
    let end = start + 1000;
    let scalar: i64 = 1;
    let repeat: (i64, i64) = (-20, 20);
    let mock_data = MockData::new(start..end, scalar, (repeat.0..repeat.1).cycle());
    let data = Arc::new(Mutex::new(vec![]));
    let projection_out = Box::new(MockOutput::new(data.clone()));
    let mut source = MockDataSource::new(mock_data);

    let left_type = Arc::new(Int64Type::new(false));
    let left_expr = InputRefExpression::new(left_type, 0);
    let right_type = Arc::new(Int64Type::new(false));
    let right_expr = InputRefExpression::new(right_type, 1);
    let test_expr = ArithmeticExpression::new(
        Arc::new(Int64Type::new(false)),
        ArithmeticOperatorKind::Plus,
        Box::new(left_expr),
        Box::new(right_expr),
    );

    let projection_op = Box::new(ProjectionOperator::new(
        projection_out,
        vec![Box::new(test_expr)],
    ));
    let source_out = Box::new(OperatorOutput::new(projection_op));
    let (_cancel_tx, cancel_rx) = oneshot::channel();
    source.run(source_out, cancel_rx).await?;

    let data = data.lock().await;
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
    let end = start + 1000;
    let scalar: i64 = 0;
    let repeat: (i64, i64) = (-20, 20);
    let mock_data = MockData::new(start..end, scalar, (repeat.0..repeat.1).cycle());
    let data = Arc::new(Mutex::new(vec![]));
    let filter_out = Box::new(MockOutput::new(data.clone()));
    let mut source = MockDataSource::new(mock_data);

    let left_type = Arc::new(Int64Type::new(false));
    let left_expr = InputRefExpression::new(left_type, 1);
    let right_type = Arc::new(Int64Type::new(false));
    let right_expr = InputRefExpression::new(right_type, 2);
    let test_expr = CompareExpression::new(
        Arc::new(BoolType::new(false)),
        CompareOperatorKind::GreaterThan,
        Box::new(left_expr),
        Box::new(right_expr),
    );

    let filter_op = Box::new(FilterOperator::new(filter_out, Box::new(test_expr)));
    let source_out = Box::new(OperatorOutput::new(filter_op));

    let (_cancel_tx, cancel_rx) = oneshot::channel();
    source.run(source_out, cancel_rx).await?;

    let data = data.lock().await;
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
async fn test_hash_dispatcher() {
    let num_outputs = 5;
    let cardinality = 10;
    let dimension = 4;
    let key_indices = &[0, 2];
    let output_data_vecs = (0..num_outputs)
        .map(|_| Arc::new(Mutex::new(Vec::new())))
        .collect::<Vec<_>>();
    let outputs = output_data_vecs
        .iter()
        .map(|data| Box::new(MockOutput::new(data.clone())) as Box<dyn Output>)
        .collect::<Vec<_>>();
    let mut hash_dispatcher = HashDataDispatcher::new(outputs, key_indices.to_vec());

    let mut ops = Vec::new();
    for idx in 0..cardinality {
        if idx % 2 == 0 {
            ops.push(Op::Insert);
        } else {
            ops.push(Op::Delete);
        }
    }

    let mut start = 19260817..;
    let mut builders = (0..dimension)
        .map(|_| I32ArrayBuilder::new(cardinality).unwrap())
        .collect::<Vec<_>>();
    let mut output_cols = vec![vec![vec![]; dimension]; num_outputs];
    let mut output_ops = vec![vec![]; num_outputs];
    for op in ops.iter() {
        let hash_builder = CRC32FastBuilder {};
        let mut hasher = hash_builder.build_hasher();
        let one_row: Vec<i32> = (0..dimension)
            .map(|_| start.next().unwrap())
            .collect::<Vec<_>>();
        for key_idx in key_indices.iter() {
            let val = one_row[*key_idx];
            let bytes = val.to_le_bytes();
            hasher.update(&bytes);
        }
        let output_idx = hasher.finish() as usize % num_outputs;
        for (builder, val) in builders.iter_mut().zip(one_row.iter()) {
            builder.append(Some(*val)).unwrap();
        }
        output_cols[output_idx]
            .iter_mut()
            .zip(one_row.iter())
            .for_each(|(each_column, val)| each_column.push(*val));
        output_ops[output_idx].push(op);
    }

    let columns = builders
        .into_iter()
        .map(|builder| {
            let array = builder.finish().unwrap();
            Column::new(Arc::new(array.into()), Arc::new(Int32Type::new(false)))
        })
        .collect::<Vec<_>>();

    let chunk = StreamChunk {
        ops,
        columns,
        visibility: None,
    };
    hash_dispatcher.dispatch_data(chunk).await.unwrap();

    for (output_idx, output) in output_data_vecs.into_iter().enumerate() {
        let guard = output.lock().await;
        assert_eq!(guard.len(), 1);
        let real_chunk = guard.get(0).unwrap();
        real_chunk
            .columns
            .iter()
            .zip(output_cols[output_idx].iter())
            .for_each(|(real_col, expect_col)| {
                let real_vals = real_chunk
                    .visibility
                    .as_ref()
                    .unwrap()
                    .iter()
                    .enumerate()
                    .filter(|(_, vis)| *vis)
                    .map(|(row_idx, _)| real_col.array_ref().as_int32().value_at(row_idx).unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(real_vals.len(), expect_col.len());
                assert_eq!(real_vals, *expect_col);
            });
    }
}

#[tokio::test]
async fn test_local_hash_aggregation_count() {
    let input_type = Arc::new(Int64Type::new(false));
    let return_type = Arc::new(Int64Type::new(false));
    let agg_kind = AggKind::Count;
    let data = Arc::new(Mutex::new(Vec::new()));
    let mock_output = Box::new(MockOutput::new(data.clone())) as Box<dyn Output>;
    let keys = vec![0];
    let mut hash_aggregator = HashLocalAggregationOperator::new(
        input_type,
        mock_output,
        return_type,
        keys,
        None,
        agg_kind,
    );

    let ops1 = vec![Op::Insert, Op::Insert, Op::Insert];
    let mut builder1 = I64ArrayBuilder::new(0).unwrap();
    builder1.append(Some(1)).unwrap();
    builder1.append(Some(2)).unwrap();
    builder1.append(Some(2)).unwrap();
    let array1 = builder1.finish().unwrap();
    let column1 = Column::new(Arc::new(array1.into()), Arc::new(Int64Type::new(false)));
    let chunk1 = StreamChunk {
        ops: ops1,
        columns: vec![column1],
        visibility: None,
    };

    hash_aggregator.consume_chunk(chunk1).await.unwrap();
    assert_eq!(data.lock().await.len(), 1);
    let real_output = data.lock().await.drain(0..=0).next().unwrap();
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
    hash_aggregator.consume_chunk(chunk2).await.unwrap();
    assert_eq!(data.lock().await.len(), 1);
    let real_output = data.lock().await.drain(0..=0).next().unwrap();
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
    let input_type = Arc::new(Int64Type::new(false));
    let return_type = Arc::new(Int64Type::new(false));
    let agg_kind = AggKind::Count;
    let data = Arc::new(Mutex::new(Vec::new()));
    let mock_output = Box::new(MockOutput::new(data.clone())) as Box<dyn Output>;
    let key_indices = vec![0];
    let val_indices = 1;
    let mut hash_aggregator = HashGlobalAggregationOperator::new(
        input_type,
        mock_output,
        return_type,
        key_indices,
        val_indices,
        agg_kind,
    );

    let ops1 = vec![Op::Insert, Op::Insert, Op::Insert];
    let mut key_builder1 = I64ArrayBuilder::new(0).unwrap();
    key_builder1.append(Some(1)).unwrap();
    key_builder1.append(Some(2)).unwrap();
    key_builder1.append(Some(2)).unwrap();
    let key_array1 = key_builder1.finish().unwrap();
    let key_column1 = Column::new(Arc::new(key_array1.into()), Arc::new(Int64Type::new(false)));
    let mut val_builder1 = I64ArrayBuilder::new(0).unwrap();
    val_builder1.append(Some(1)).unwrap();
    val_builder1.append(Some(2)).unwrap();
    val_builder1.append(Some(2)).unwrap();
    let value_array1 = val_builder1.finish().unwrap();
    let value_column1 = Column::new(
        Arc::new(value_array1.into()),
        Arc::new(Int64Type::new(false)),
    );
    let mut row_count_builder1 = I64ArrayBuilder::new(0).unwrap();
    row_count_builder1.append(Some(1)).unwrap();
    row_count_builder1.append(Some(2)).unwrap();
    row_count_builder1.append(Some(2)).unwrap();
    let row_count_array1 = row_count_builder1.finish().unwrap();
    let row_count_column1 = Column::new(
        Arc::new(row_count_array1.into()),
        Arc::new(Int64Type::new(false)),
    );
    let chunk1 = StreamChunk {
        ops: ops1,
        columns: vec![key_column1, value_column1, row_count_column1],
        visibility: Some(Bitmap::from_vec(vec![true, true, true]).unwrap()),
    };

    hash_aggregator.consume_chunk(chunk1).await.unwrap();
    assert_eq!(data.lock().await.len(), 1);
    let real_output = data.lock().await.drain(0..=0).next().unwrap();
    assert_eq!(real_output.ops.len(), 2);
    assert_eq!(real_output.ops[0], Op::Insert);
    assert_eq!(real_output.ops[1], Op::Insert);
    assert_eq!(real_output.columns.len(), 2);
    let key_column = real_output.columns[0].array_ref().as_int64();
    assert_eq!(key_column.value_at(0).unwrap(), 1);
    assert_eq!(key_column.value_at(1).unwrap(), 2);
    let agg_column = real_output.columns[1].array_ref().as_int64();
    assert_eq!(agg_column.value_at(0).unwrap(), 1);
    assert_eq!(agg_column.value_at(1).unwrap(), 4);

    let ops2 = vec![Op::Delete, Op::Delete, Op::Delete, Op::Insert];
    let mut key_builder2 = I64ArrayBuilder::new(0).unwrap();
    key_builder2.append(Some(1)).unwrap();
    key_builder2.append(Some(2)).unwrap();
    key_builder2.append(Some(2)).unwrap();
    key_builder2.append(Some(3)).unwrap();
    let key_array2 = key_builder2.finish().unwrap();
    let key_column2 = Column::new(Arc::new(key_array2.into()), Arc::new(Int64Type::new(false)));
    let mut val_builder2 = I64ArrayBuilder::new(0).unwrap();
    val_builder2.append(Some(1)).unwrap();
    val_builder2.append(Some(2)).unwrap();
    val_builder2.append(Some(1)).unwrap();
    val_builder2.append(Some(3)).unwrap();
    let value_array2 = val_builder2.finish().unwrap();
    let value_column2 = Column::new(
        Arc::new(value_array2.into()),
        Arc::new(Int64Type::new(false)),
    );
    let mut row_count_builder2 = I64ArrayBuilder::new(0).unwrap();
    row_count_builder2.append(Some(1)).unwrap();
    row_count_builder2.append(Some(2)).unwrap();
    row_count_builder2.append(Some(1)).unwrap();
    row_count_builder2.append(Some(3)).unwrap();
    let row_count_array2 = row_count_builder2.finish().unwrap();
    let row_count_column2 = Column::new(
        Arc::new(row_count_array2.into()),
        Arc::new(Int64Type::new(false)),
    );
    let chunk2 = StreamChunk {
        ops: ops2,
        columns: vec![key_column2, value_column2, row_count_column2],
        visibility: Some(Bitmap::from_vec(vec![true, false, true, true]).unwrap()),
    };
    hash_aggregator.consume_chunk(chunk2).await.unwrap();
    assert_eq!(data.lock().await.len(), 1);
    let real_output = data.lock().await.drain(0..=0).next().unwrap();
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
    assert_eq!(real_output.columns.len(), 2);
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
    assert_eq!(agg_column, vec![Some(1), None, Some(4), Some(3), Some(3)]);
}
