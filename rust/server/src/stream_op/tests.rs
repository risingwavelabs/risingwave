use super::data_source::*;
use super::{OperatorOutput, Output, ProjectionOperator};
use crate::array2::column::Column;
use crate::array2::{Array, ArrayBuilder, ArrayImpl, I32ArrayBuilder};
use crate::expr::{ArithmeticExpression, InputRefExpression};
use crate::stream_op::Op;
use crate::stream_op::{DataDispatcher, HashDataDispatcher, StreamChunk};
use crate::types::{ArithmeticOperatorKind, Int32Type, Int64Type};
use crate::util::hash_util::CRC32FastBuilder;
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::time;
use tokio::sync::Mutex;

#[tokio::test]
async fn test_projection() {
    let start: i64 = 114514;
    let data = Arc::new(Mutex::new(vec![]));
    let projection_out = Box::new(MockOutput::new(data.clone()));
    let source = Arc::new(MockDataSource::new(start..));
    let source2 = source.clone();

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

    let handle = tokio::spawn(async move {
        tokio::time::sleep(time::Duration::from_millis(10)).await;
        source2.cancel().await.expect("run without error");
    });

    source.run(source_out).await.expect("run without error");

    handle.await.unwrap();

    let data = data.lock().await;
    let mut expected = start;
    for chunk in data.iter() {
        assert!(chunk.columns.len() == 1);
        let arr = chunk.columns[0].array_ref();
        if let ArrayImpl::Int64(arr) = arr {
            for i in 0..arr.len() {
                let v = arr.value_at(i).expect("arr[i] exists");
                assert_eq!(v, expected + 1);
                expected += 1;
            }
        } else {
            unreachable!()
        }
    }
    println!("{} items collected.", expected - start);
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
        cardinality,
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
