use std::collections::VecDeque;
use std::sync::Arc;

use itertools::Itertools;

use risingwave_proto::expr::ExprNode_Type;

use crate::executor::ExecutorResult::Batch;
use crate::executor::ExecutorResult::Done;
use crate::executor::{Executor, ExecutorResult};
use risingwave_common::array::ArrayImpl;
use risingwave_common::array::DataChunk;
use risingwave_common::buffer::Bitmap;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_common::expr::binary_expr::new_binary_expr;
use risingwave_common::expr::InputRefExpression;
use risingwave_common::types::BoolType;

use super::BoxedExecutor;

// MockExecutor is to mock the input of executor.
// You can bind one or more MockExecutor as the children of the executor to test,
// (HashAgg, e.g), so that allow testing without instantiating real SeqScans and real storage.
pub struct MockExecutor {
    chunks: VecDeque<DataChunk>,
    schema: Schema,
}

impl MockExecutor {
    pub fn new(schema: Schema) -> Self {
        Self {
            chunks: VecDeque::new(),
            schema,
        }
    }

    pub fn with_chunk(chunk: DataChunk, schema: Schema) -> Self {
        let mut ret = Self::new(schema);
        ret.add(chunk);
        ret
    }

    pub fn add(&mut self, chunk: DataChunk) {
        self.chunks.push_back(chunk);
    }
}

#[async_trait::async_trait]
impl Executor for MockExecutor {
    fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn execute(&mut self) -> Result<ExecutorResult> {
        if self.chunks.is_empty() {
            return Ok(Done);
        }
        let chunk = self.chunks.pop_front().unwrap();
        Ok(ExecutorResult::Batch(chunk))
    }

    fn clean(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}
/// if the input from two child executor is same(considering order),
/// it will also check the cloumns structure of chunks from child executor
/// use for executor unit test.
/// if want diff ignoring order, add a `order_by` executor in manual currently, when the `schema` method of `executor` is ready, an order-ignored version will be added.
pub struct DiffExecutor {
    actual: BoxedExecutor,
    expect: BoxedExecutor,
}

pub async fn diff_executor_output(mut actual: BoxedExecutor, mut expect: BoxedExecutor) {
    let mut expect_cardinality = 0;
    let mut actual_cardinality = 0;
    let mut expects = vec![];
    let mut actuals = vec![];
    expect.init().unwrap();
    actual.init().unwrap();
    while let Batch(chunk) = expect.execute().await.unwrap() {
        let chunk = DataChunk::compact(chunk).unwrap();
        expect_cardinality += chunk.cardinality();
        expects.push(Arc::new(chunk));
    }
    while let Batch(chunk) = actual.execute().await.unwrap() {
        let chunk = DataChunk::compact(chunk).unwrap();
        actual_cardinality += chunk.cardinality();
        actuals.push(Arc::new(chunk));
    }
    expect.clean().unwrap();
    actual.clean().unwrap();
    assert_eq!(actual_cardinality, expect_cardinality);
    if actual_cardinality == 0 {
        return;
    }
    let expect = DataChunk::rechunk(expects.as_slice(), actual_cardinality)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let actual = DataChunk::rechunk(actuals.as_slice(), actual_cardinality)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let col_num = expect.columns().len();
    assert_eq!(col_num, actual.columns().len());
    expect
        .columns()
        .iter()
        .zip(actual.columns().iter())
        .for_each(|(c1, c2)| {
            assert_eq!(c1.data_type().to_protobuf(), c2.data_type().to_protobuf())
        });
    let columns = expect
        .columns()
        .iter()
        .chain(actual.columns().iter())
        .cloned()
        .collect_vec();

    let chunk = DataChunk::new(columns, None);
    for idx in 0..col_num {
        let idy = idx + col_num;
        let data_type = chunk.columns()[idx].data_type();
        let except_expr = InputRefExpression::new(data_type.clone(), idx);
        let actual_expr = InputRefExpression::new(data_type.clone(), idy);
        let mut expr = new_binary_expr(
            ExprNode_Type::EQUAL,
            Arc::new(BoolType::new(false)),
            Box::new(except_expr),
            Box::new(actual_expr),
        );
        let res = expr.eval(&chunk).unwrap();
        if let ArrayImpl::Bool(res) = res.as_ref() {
            let res: Bitmap = res.try_into().unwrap();
            res.iter().for_each(|x| {
                if !x {
                    panic!("not equal")
                }
            });
        } else {
            unreachable!();
        }
    }
}
