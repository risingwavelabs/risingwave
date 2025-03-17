// Copyright 2025 RisingWave Labs
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
use risingwave_common::array::{ArrayImpl, DataChunk};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_expr::table_function::{BoxedTableFunction, build_from_prost, check_error};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedExecutor, BoxedExecutorBuilder};
use crate::error::{BatchError, Result};
use crate::executor::{BoxedDataChunkStream, Executor, ExecutorBuilder};

pub struct TableFunctionExecutor {
    schema: Schema,
    identity: String,
    table_function: BoxedTableFunction,
    #[expect(dead_code)]
    chunk_size: usize,
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
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let dummy_chunk = DataChunk::new_dummy(1);

        #[for_await]
        for chunk in self.table_function.eval(&dummy_chunk).await {
            let chunk = chunk?;
            check_error(&chunk)?;
            // remove the first column and expand the second column if its data type is struct
            yield match chunk.column_at(1).as_ref() {
                ArrayImpl::Struct(struct_array) => struct_array.into(),
                _ => chunk.split_column_at(1).1,
            };
        }
    }
}

pub struct TableFunctionExecutorBuilder {}

impl TableFunctionExecutorBuilder {}

impl BoxedExecutorBuilder for TableFunctionExecutorBuilder {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
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

        let chunk_size = source.context().get_config().developer.chunk_size;

        let table_function = build_from_prost(node.table_function.as_ref().unwrap(), chunk_size)?;

        let schema = if let DataType::Struct(fields) = table_function.return_type() {
            (&fields).into()
        } else {
            Schema {
                // TODO: should be named
                fields: vec![Field::unnamed(table_function.return_type())],
            }
        };

        Ok(Box::new(TableFunctionExecutor {
            schema,
            identity,
            table_function,
            chunk_size,
        }))
    }
}
