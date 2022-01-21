use std::collections::VecDeque;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use super::BoxedExecutor;
use crate::executor::Executor;

/// Mock the input of executor.
/// You can bind one or more `MockExecutor` as the children of the executor to test,
/// (`HashAgg`, e.g), so that allow testing without instantiating real `SeqScan`s and real storage.
pub struct MockExecutor {
    chunks: VecDeque<DataChunk>,
    schema: Schema,
    identity: String,
}

impl MockExecutor {
    pub fn new(schema: Schema) -> Self {
        Self {
            chunks: VecDeque::new(),
            schema,
            identity: "MockExecutor".to_string(),
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
    async fn open(&mut self) -> Result<()> {
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if self.chunks.is_empty() {
            return Ok(None);
        }
        let chunk = self.chunks.pop_front().unwrap();
        Ok(Some(chunk))
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

/// if the input from two child executor is same(considering order),
/// it will also check the columns structure of chunks from child executor
/// use for executor unit test.
///
/// if want diff ignoring order, add a `order_by` executor in manual currently, when the `schema`
/// method of `executor` is ready, an order-ignored version will be added.
pub async fn diff_executor_output(mut actual: BoxedExecutor, mut expect: BoxedExecutor) {
    let mut expect_cardinality = 0;
    let mut actual_cardinality = 0;
    let mut expects = vec![];
    let mut actuals = vec![];
    expect.open().await.unwrap();
    actual.open().await.unwrap();
    while let Some(chunk) = expect.next().await.unwrap() {
        let chunk = DataChunk::compact(chunk).unwrap();
        expect_cardinality += chunk.cardinality();
        expects.push(Arc::new(chunk));
    }
    while let Some(chunk) = actual.next().await.unwrap() {
        let chunk = DataChunk::compact(chunk).unwrap();
        actual_cardinality += chunk.cardinality();
        actuals.push(Arc::new(chunk));
    }
    expect.close().await.unwrap();
    actual.close().await.unwrap();
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
        .zip_eq(actual.columns().iter())
        .for_each(|(c1, c2)| assert_eq!(c1.array().to_protobuf(), c2.array().to_protobuf()));

    is_data_chunk_eq(&expect, &actual)
}

fn is_data_chunk_eq(left: &DataChunk, right: &DataChunk) {
    assert!(left.visibility().is_none());
    assert!(right.visibility().is_none());

    assert_eq!(
        left.cardinality(),
        right.cardinality(),
        "two chunks cardinality is different"
    );

    left.rows()
        .zip_eq(right.rows())
        .for_each(|(row1, row2)| assert_eq!(row1, row2));
}
