use super::data_source::*;
use super::{LocalOutput, ProjectionOperator};
use crate::array2::{Array, ArrayImpl};
use crate::expr::{ArithmeticExpression, InputRefExpression};
use crate::types::{ArithmeticOperatorKind, Int64Type};
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
    let source_out = Box::new(LocalOutput::new(projection_op));

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
