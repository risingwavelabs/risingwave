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
use itertools::Itertools;
use risingwave_common::array::column::Column;
use risingwave_common::array::{DataChunk, PrimitiveArray};
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_common::util::chunk_coalesce::{DataChunkBuilder, SlicedDataChunk};
use risingwave_pb::batch_plan::plan_node::NodeBody;

use super::{BoxedDataChunkStream, BoxedExecutor, BoxedExecutorBuilder, Executor, ExecutorBuilder};
use crate::task::BatchTaskContext;

pub struct ExpandExecutor {
    expanded_keys: Vec<Vec<usize>>,
    child: BoxedExecutor,
    schema: Schema,
    identity: String,
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
    #[try_stream(boxed, ok = DataChunk, error = RwError)]
    async fn do_execute(self: Box<Self>) {
        let mut data_chunk_builder = DataChunkBuilder::with_default_size(self.schema.data_types());

        #[for_await]
        for data_chunk in self.child.execute() {
            // TODO: handle dummy chunk.
            let data_chunk: DataChunk = data_chunk?.compact()?;
            let cardinality = data_chunk.cardinality();
            let (columns, vis) = data_chunk.into_parts();
            // TODO: use iter to rewrite.
            let mut null_columns = vec![];
            for column in &columns {
                let array = column.array_ref();
                let mut builder = array.create_builder(cardinality)?;
                (0..cardinality).try_for_each(|_i| builder.append_null())?;
                null_columns.push(Column::new(Arc::new(builder.finish()?)));
            }

            for (i, keys) in self.expanded_keys.iter().enumerate() {
                let mut new_columns = null_columns.clone();
                for key in keys {
                    new_columns[*key] = columns[*key].clone();
                }
                let flags = Column::from(PrimitiveArray::<i64>::from_slice(&vec![
                    Some(i as i64);
                    cardinality
                ])?);
                new_columns.push(flags);
                // TODO: maybe rewrite `vis`.
                let new_data_chunk = DataChunk::new(new_columns, vis.clone());
                let mut sliced_data_chunk = SlicedDataChunk::new_checked(new_data_chunk)?;
                loop {
                    let (left_data, output) = data_chunk_builder.append_chunk(sliced_data_chunk)?;
                    match (left_data, output) {
                        (Some(left_data), Some(output)) => {
                            sliced_data_chunk = left_data;
                            yield output;
                        }
                        (None, Some(output)) => {
                            yield output;
                            break;
                        }
                        (None, None) => {
                            break;
                        }
                        _ => {
                            return Err(
                                InternalError("Data chunk builder error".to_string()).into()
                            );
                        }
                    }
                }
            }
        }
        if let Some(chunk) = data_chunk_builder.consume_all()? {
            yield chunk;
        }
    }
}

#[async_trait::async_trait]
impl BoxedExecutorBuilder for ExpandExecutor {
    async fn new_boxed_executor<C: BatchTaskContext>(
        source: &ExecutorBuilder<C>,
        mut inputs: Vec<BoxedExecutor>,
    ) -> Result<BoxedExecutor> {
        ensure!(inputs.len() == 1);
        let expand_node = try_match_expand!(
            source.plan_node().get_node_body().unwrap(),
            NodeBody::Expand
        )?;

        let expanded_keys = expand_node
            .expanded_keys
            .iter()
            .map(|keys| keys.keys.iter().map(|key| *key as usize).collect_vec())
            .collect_vec();

        let child = inputs.remove(0);

        let mut schema = child.schema().clone();
        schema
            .fields
            .push(Field::with_name(DataType::Int64, "flag"));

        Ok(Box::new(Self {
            expanded_keys,
            child,
            schema,
            identity: "ExpandExecutor".to_string(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::{DataChunk, DataChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::ExpandExecutor;
    use crate::executor::test_utils::MockExecutor;
    use crate::executor::Executor;

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
                Field::unnamed(DataType::Int64),
            ],
        };
        let mut mock_executor = MockExecutor::new(mock_schema);
        mock_executor.add(DataChunk::from_pretty(
            "i i i
             1 2 3
             2 3 4",
        ));
        let expanded_keys = vec![vec![0, 1], vec![1, 2]];
        let expand_executor = Box::new(ExpandExecutor {
            expanded_keys,
            child: Box::new(mock_executor),
            schema: expand_schema,
            identity: "ExpandExecutor".to_string(),
        });
        let mut stream = expand_executor.execute();
        let res = stream.next().await.unwrap().unwrap();
        assert_eq!(
            res,
            DataChunk::from_pretty(
                "i i i I
             1 2 . 0
             2 3 . 0
             . 2 3 1
             . 3 4 1"
            )
        );
    }
}
