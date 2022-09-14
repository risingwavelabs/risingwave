// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use futures_async_stream::try_stream;
use risingwave_common::array::column::Column;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::chunk_coalesce::DEFAULT_CHUNK_BUFFER_SIZE;
use risingwave_expr::table_function::{build_from_prost, BoxedTableFunction};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::executor::{BoxedDataChunkStream, Executor, ExecutorBuilder};
use crate::task::BatchTaskContext;

pub struct TableFunctionExecutor {
    schema: Schema,
    identity: String,
    table_function: BoxedTableFunction,
}

impl Executor for TableFunctionExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn identity(&self) -> &str {
        &self.identity
    }

    fn execute(self: Box<Self>) -> BoxedDataChunkStream {
        self.do_execute()
    }
}

impl TableFunctionExecutor {
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let dummy_chunk = DataChunk::new_dummy(1);

        let mut builder = self
            .table_function
            .return_type()
            .create_array_builder(DEFAULT_CHUNK_BUFFER_SIZE);
        let mut len = 0;
        for array in self.table_function.eval(&dummy_chunk)? {
            len += array.len();
            builder.append_array(&array)?;
        }
        let array = Arc::new(builder.finish());
        let ret = DataChunk::new(vec![Column::new(array)], len);
        yield ret
    }
}

pub struct TableFunctionExecutorBuilder {}

impl TableFunctionExecutorBuilder {}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for TableFunctionExecutorBuilder {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(
            inputs.is_empty(),
            "GenerateSeriesExecutor should not have child!"
        );
        let node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::TableFunction
        )?;

        let identity = source.plan_node().get_identity().clone();

        let table_function = build_from_prost(node.table_function.as_ref().unwrap())?;

        let fields = vec![Field::unnamed(table_function.return_type())];

        Ok(Box::new(TableFunctionExecutor {
            schema: Schema { fields },
            identity,
            table_function,
        }))
    }
}
