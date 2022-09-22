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

use std::fmt::Debug;

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

use super::error::StreamExecutorError;
use super::*;

pub struct ExpandExecutor {
    input: BoxedExecutor,
    schema: Schema,
    pk_indices: PkIndices,
    column_subsets: Vec<Vec<usize>>,
}

impl ExpandExecutor {
    pub fn new(
        input: Box<dyn Executor>,
        pk_indices: PkIndices,
        column_subsets: Vec<Vec<usize>>,
    ) -> Self {
        let schema = {
            let mut fields = input.schema().clone().into_fields();
            fields.extend(fields.clone());
            fields.push(Field::with_name(DataType::Int64, "flag"));
            Schema::new(fields)
        };
        Self {
            input,
            schema,
            pk_indices,
            column_subsets,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        #[for_await]
        for msg in self.input.execute() {
            match msg? {
                Message::Chunk(chunk) => {
                    let chunk = chunk.compact()?;
                    let (data_chunk, ops) = chunk.into_parts();
                    assert!(
                        data_chunk.dimension() > 0,
                        "The input data chunk of expand can't be dummy chunk."
                    );
                    let cardinality = data_chunk.cardinality();
                    let (columns, _) = data_chunk.into_parts();

                    #[for_await]
                    for new_columns in
                        Column::expand_columns(cardinality, columns, self.column_subsets.to_owned())
                    {
                        let stream_chunk = StreamChunk::new(ops.clone(), new_columns?, None);
                        yield Message::Chunk(stream_chunk)
                    }
                }
                m => yield m,
            }
        }
    }
}

impl Debug for ExpandExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExpandExecutor")
            .field("column_subsets", &self.column_subsets)
            .finish()
    }
}

impl Executor for ExpandExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        "ExpandExecutor"
    }

    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use risingwave_common::array::{StreamChunk, StreamChunkTestExt};
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;

    use super::ExpandExecutor;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{Executor, PkIndices};

    #[tokio::test]
    async fn test_expand() {
        let chunk1 = StreamChunk::from_pretty(
            " I I I
            + 1 4 1
            + 5 2 2 D
            + 6 6 3
            - 7 5 4",
        );
        let schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        };
        let source = MockSource::with_chunks(schema, PkIndices::new(), vec![chunk1]);

        let column_subsets = vec![vec![0, 1], vec![1, 2]];
        let expand = Box::new(ExpandExecutor::new(
            Box::new(source),
            PkIndices::new(),
            column_subsets,
        ));
        let mut expand = expand.execute();

        let chunk = expand.next().await.unwrap().unwrap().into_chunk().unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I I
                + 1 4 . 1 4 1 0
                + 6 6 . 6 6 3 0
                - 7 5 . 7 5 4 0"
            )
        );

        let chunk = expand.next().await.unwrap().unwrap().into_chunk().unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I I
                + . 4 1 1 4 1 1
                + . 6 3 6 6 3 1
                - . 5 4 7 5 4 1"
            )
        );
    }
}
