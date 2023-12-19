// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures_async_stream::try_stream;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::Schema;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use crate::error::{BatchError, Result};
use crate::executor::{BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::task::BatchTaskContext;

pub struct MaxOneRowExecutor {
    child: BoxedExecutor,

    /// Identity string of the executor
    identity: String,
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for MaxOneRowExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<'_, C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let [child]: [_; 1] = inputs.try_into().unwrap();

        let _node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::MaxOneRow
        )?;

        Ok(Box::new(Self {
            child,
            identity: source.plan_node().get_identity().clone(),
        }))
    }
}

impl Executor for MaxOneRowExecutor {
    fn schema(&self) -> &Schema {
        self.child.schema()
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn execute(self: Box<Self>) {
        let data_types = self.child.schema().data_types();
        let mut result = None;

        #[for_await]
        for chunk in self.child.execute() {
            let chunk = chunk?;
            for row in chunk.rows() {
                if result.is_some() {
                    // `MaxOneRow` is currently only used for the runtime check of
                    // scalar subqueries, so we raise a precise error here.
                    bail!("Scalar subquery produced more than one row.");
                } else {
                    // We do not immediately yield the chunk here. Instead, we store
                    // it in `result` and only yield it when the child executor is
                    // exhausted, in case the parent executor cancels the execution
                    // after receiving the row (like `limit 1`).
                    result = Some(DataChunk::from_rows(&[row], &data_types));
                }
            }
        }

        if let Some(result) = result {
            yield result;
        }
    }
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;
    use risingwave_common::array::DataChunk;
    use risingwave_common::catalog::schema_test_utils::ii;
    use risingwave_common::row::Row;
    use risingwave_common::types::Datum;

    use crate::executor::test_utils::MockExecutor;
    use crate::executor::{Executor, MaxOneRowExecutor};

    #[derive(Clone, Copy, Debug)]
    enum ExpectedOutput {
        Empty,
        OneRow,
        Error,
    }

    async fn test_case(input: MockExecutor, expected: ExpectedOutput) {
        let executor = Box::new(MaxOneRowExecutor {
            child: Box::new(input),
            identity: "".to_owned(),
        });

        let outputs: Vec<_> = executor.execute().collect().await;

        match (&outputs[..], expected) {
            (&[], ExpectedOutput::Empty) => {}
            (&[Ok(ref chunk)], ExpectedOutput::OneRow) => assert_eq!(chunk.cardinality(), 1),
            (&[Err(_)], ExpectedOutput::Error) => {}
            _ => panic!("expected {expected:?}, got {outputs:?}"),
        }
    }

    fn row() -> impl Row {
        [Datum::Some(114i32.into()), Datum::Some(514i32.into())]
    }

    #[tokio::test]
    async fn test_empty() {
        let input = MockExecutor::new(ii());

        test_case(input, ExpectedOutput::Empty).await;
    }

    #[tokio::test]
    async fn test_one_row() {
        let mut input = MockExecutor::new(ii());
        input.add(DataChunk::from_rows(&[row()], &ii().data_types()));

        test_case(input, ExpectedOutput::OneRow).await;
    }

    #[tokio::test]
    async fn test_error_1() {
        let mut input = MockExecutor::new(ii());
        input.add(DataChunk::from_rows(&[row(), row()], &ii().data_types()));

        test_case(input, ExpectedOutput::Error).await;
    }

    #[tokio::test]
    async fn test_error_2() {
        let mut input = MockExecutor::new(ii());
        input.add(DataChunk::from_rows(&[row()], &ii().data_types()));
        input.add(DataChunk::from_rows(&[row()], &ii().data_types()));

        test_case(input, ExpectedOutput::Error).await;
    }
}
