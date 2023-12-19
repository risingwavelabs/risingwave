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
        let mut any = false;

        #[for_await]
        for chunk in self.child.execute() {
            let chunk = chunk?;
            for row in chunk.rows() {
                if any {
                    // `MaxOneRow` is currently only used for the runtime check of
                    // scalar subqueries, so we raise a precise error here.
                    bail!("Scalar subquery produced more than one row.");
                } else {
                    any = true;
                    let one_row_chunk = DataChunk::from_rows(&[row], &data_types);
                    yield one_row_chunk;
                }
            }
        }
    }
}
