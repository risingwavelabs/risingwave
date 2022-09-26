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

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;
use risingwave_common::util::select_all;

use super::*;
use crate::executor::{BoxedMessageStream, ExecutorInfo};

/// `UnionExecutor` merges data from multiple inputs.
pub struct UnionExecutor {
    inputs: Vec<BoxedExecutor>,
    info: ExecutorInfo,
}

impl std::fmt::Debug for UnionExecutor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnionExecutor")
            .field("schema", &self.info.schema)
            .field("pk_indices", &self.info.pk_indices)
            .finish()
    }
}

impl UnionExecutor {
    pub fn new(pk_indices: PkIndices, inputs: Vec<BoxedExecutor>) -> Self {
        Self {
            info: ExecutorInfo {
                schema: inputs[0].schema().clone(),
                pk_indices,
                identity: "UnionExecutor".to_string(),
            },
            inputs,
        }
    }
}

impl Executor for UnionExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let streams = self.inputs.into_iter().map(|e| e.execute()).collect();
        merge(streams)
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

/// Merges input streams and aligns with barriers.
pub fn merge(inputs: Vec<BoxedMessageStream>) -> BoxedMessageStream {
    let barrier = Arc::new(tokio::sync::Barrier::new(inputs.len()));
    let mut streams = vec![];
    for input in inputs {
        let barrier = barrier.clone();
        let stream = #[try_stream]
        async move {
            #[for_await]
            for item in input {
                match item? {
                    msg @ Message::Chunk(_) => yield msg,
                    msg @ Message::Barrier(_) => {
                        if barrier.wait().await.is_leader() {
                            // one leader is responsible for sending barrier
                            yield msg;
                        }
                    }
                }
            }
        };
        streams.push(stream.boxed());
    }
    select_all(streams).boxed()
}

#[cfg(test)]
mod tests {
    use async_stream::try_stream;
    use futures::TryStreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;

    use super::*;

    #[tokio::test]
    async fn union() {
        let streams = vec![
            try_stream! {
                yield Message::Chunk(StreamChunk::from_pretty("I\n + 1"));
                yield Message::Barrier(Barrier::new_test_barrier(1));
                yield Message::Chunk(StreamChunk::from_pretty("I\n + 2"));
                yield Message::Barrier(Barrier::new_test_barrier(2));
            }
            .boxed(),
            try_stream! {
                yield Message::Chunk(StreamChunk::from_pretty("I\n + 1"));
                yield Message::Barrier(Barrier::new_test_barrier(1));
                yield Message::Barrier(Barrier::new_test_barrier(2));
                yield Message::Chunk(StreamChunk::from_pretty("I\n + 3"));
            }
            .boxed(),
        ];
        let output: Vec<_> = merge(streams).try_collect().await.unwrap();
        assert_eq!(
            output,
            vec![
                Message::Chunk(StreamChunk::from_pretty("I\n + 1")),
                Message::Chunk(StreamChunk::from_pretty("I\n + 1")),
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Chunk(StreamChunk::from_pretty("I\n + 2")),
                Message::Barrier(Barrier::new_test_barrier(2)),
                Message::Chunk(StreamChunk::from_pretty("I\n + 3")),
            ]
        );
    }
}
