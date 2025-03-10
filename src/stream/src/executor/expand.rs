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

use risingwave_common::array::{Array, I64Array};

use crate::executor::prelude::*;

pub struct ExpandExecutor {
    input: Executor,
    column_subsets: Vec<Vec<usize>>,
}

impl ExpandExecutor {
    pub fn new(input: Executor, column_subsets: Vec<Vec<usize>>) -> Self {
        Self {
            input,
            column_subsets,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        #[for_await]
        for msg in self.input.execute() {
            let input = match msg? {
                Message::Chunk(c) => c,
                m => {
                    yield m;
                    continue;
                }
            };
            for (i, subsets) in self.column_subsets.iter().enumerate() {
                let flags =
                    I64Array::from_iter(std::iter::repeat_n(i as i64, input.capacity())).into_ref();
                let (mut columns, vis) = input.data_chunk().keep_columns(subsets).into_parts();
                columns.extend(input.columns().iter().cloned());
                columns.push(flags);
                let chunk = StreamChunk::with_visibility(input.ops(), columns, vis);
                yield Message::Chunk(chunk);
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

impl Execute for ExpandExecutor {
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
    use crate::executor::{Execute, PkIndices};

    #[tokio::test]
    async fn test_expand() {
        let chunk1 = StreamChunk::from_pretty(
            " I I I
            + 1 4 1
            + 5 2 2 D
            + 6 6 3
            - 7 5 4",
        );
        let source = MockSource::with_chunks(vec![chunk1]).into_executor(
            Schema::new(vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ]),
            PkIndices::new(),
        );

        let column_subsets = vec![vec![0, 1], vec![1, 2]];
        let mut expand = ExpandExecutor::new(source, column_subsets)
            .boxed()
            .execute();

        let chunk = expand.next().await.unwrap().unwrap().into_chunk().unwrap();
        assert_eq!(
            chunk,
            StreamChunk::from_pretty(
                " I I I I I I I
                + 1 4 . 1 4 1 0
                + 5 2 . 5 2 2 0 D
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
                + . 2 2 5 2 2 1 D
                + . 6 3 6 6 3 1
                - . 5 4 7 5 4 1"
            )
        );
    }
}
