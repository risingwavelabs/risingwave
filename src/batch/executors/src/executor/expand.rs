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
use itertools::Itertools;
use risingwave_common::array::{Array, DataChunk, I64Array};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::error::{BatchError, Result};

pub struct ExpandExecutor {
    column_subsets: Vec<Vec<usize>>,
    child: BoxedExecutor,
    schema: Schema,
    identity: String,
    chunk_size: usize,
}

impl Executor for ExpandExecutor {
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

impl ExpandExecutor {
    #[try_stream(boxed, ok = DataChunk, error = BatchError)]
    async fn do_execute(self: Box<Self>) {
        let mut data_chunk_builder =
            DataChunkBuilder::new(self.schema.data_types(), self.chunk_size);

        #[for_await]
        for input in self.child.execute() {
            let input = input?;
            for (i, subsets) in self.column_subsets.iter().enumerate() {
                let flags =
                    I64Array::from_iter(std::iter::repeat_n(i as i64, input.capacity())).into_ref();
                let (mut columns, vis) = input.keep_columns(subsets).into_parts();
                columns.extend(input.columns().iter().cloned());
                columns.push(flags);
                let chunk = DataChunk::new(columns, vis);

                for data_chunk in data_chunk_builder.append_chunk(chunk) {
                    yield data_chunk;
                }
            }
        }
        if let Some(chunk) = data_chunk_builder.consume_all() {
            yield chunk;
        }
    }

    pub fn new(input: BoxedExecutor, column_subsets: Vec<Vec<usize>>, chunk_size: usize) -> Self {
        let schema = {
            let mut fields = input.schema().clone().into_fields();
            fields.extend(fields.clone());
            fields.push(Field::with_name(DataType::Int64, "flag"));
            Schema::new(fields)
        };
        Self {
            column_subsets,
            child: input,
            schema,
            identity: "ExpandExecutor".into(),
            chunk_size,
        }
    }
}

impl BoxedExecutorBuilder for ExpandExecutor {
    async fn new_boxed_executor(
        source: &ExecutorBuilder<'_>,
        inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        let expand_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Expand
        )?;

        let column_subsets = expand_node
            .column_subsets
            .iter()
            .map(|subset| {
                subset
                    .column_indices
                    .iter()
                    .map(|idx| *idx as usize)
                    .collect_vec()
            })
            .collect_vec();

        let [input]: [_; 1] = inputs.try_into().unwrap();
        Ok(Box::new(Self::new(
            input,
            column_subsets,
            source.context().get_config().developer.chunk_size,
        )))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::ExpandExecutor;
    use crate::executor::Executor;
    use crate::executor::test_utils::MockExecutor;

    const CHUNK_SIZE: usize = 1024;

    #[tokio::test]
    async fn test_expand_executor() {
        let mock_schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        };
        let expand_schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int64),
            ],
        };
        let mut mock_executor = MockExecutor::new(mock_schema);
        mock_executor.add(DataChunk::from_pretty(
            "i i i
             1 2 3
             2 3 4",
        ));
        let column_subsets = vec![vec![0, 1], vec![1, 2]];
        let expand_executor = Box::new(ExpandExecutor {
            column_subsets,
            child: Box::new(mock_executor),
            schema: expand_schema,
            identity: "ExpandExecutor".to_owned(),
            chunk_size: CHUNK_SIZE,
        });
        let mut stream = expand_executor.execute();
        let res = stream.next().await.unwrap().unwrap();
        let expected_chunk = DataChunk::from_pretty(
            "i i i i i i I
             1 2 . 1 2 3 0
             2 3 . 2 3 4 0
             . 2 3 1 2 3 1
             . 3 4 2 3 4 1",
        );
        assert_eq!(res, expected_chunk);
    }
}
