// Copyright 2023 Singularity Data
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

use futures::StreamExt;
use futures_async_stream::try_stream;
use risingwave_common::array::StreamChunk;
use risingwave_common::catalog::Schema;

use super::error::StreamExecutorError;
use super::{expect_first_barrier, BoxedExecutor, Executor, ExecutorInfo, Message};
use crate::executor::PkIndices;
use crate::task::CreateMviewProgress;

/// [`ChainExecutor`] is an executor that enables synchronization between the existing stream and
/// newly appended executors. Currently, [`ChainExecutor`] is mainly used to implement MV on MV
/// feature. It pipes new data of existing MVs to newly created MV only all of the old data in the
/// existing MVs are dispatched.
pub struct ChainExecutor {
    upstream: BoxedExecutor,

    upstream_indices: Vec<usize>,

    progress: CreateMviewProgress,

    info: ExecutorInfo,
}

fn mapping(upstream_indices: &[usize], chunk: StreamChunk) -> StreamChunk {
    let (ops, columns, visibility) = chunk.into_inner();
    let mapped_columns = upstream_indices
        .iter()
        .map(|&i| columns[i].clone())
        .collect();
    StreamChunk::new(ops, mapped_columns, visibility)
}

impl ChainExecutor {
    pub fn new(
        upstream: BoxedExecutor,
        upstream_indices: Vec<usize>,
        progress: CreateMviewProgress,
        schema: Schema,
        pk_indices: PkIndices,
    ) -> Self {
        Self {
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "Chain".into(),
            },
            upstream,
            upstream_indices,
            progress,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let mut upstream = self.upstream.execute();

        // 1. Poll the upstream to get the first barrier.
        let barrier = expect_first_barrier(&mut upstream).await?;

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        // 2. Continuously consume the upstream. Report that we've finished the creation on the
        // first barrier.
        #[for_await]
        for msg in upstream {
            match msg? {
                Message::Watermark(_) => {
                    todo!("https://github.com/risingwavelabs/risingwave/issues/6042")
                }
                Message::Chunk(chunk) => {
                    yield Message::Chunk(mapping(&self.upstream_indices, chunk));
                }
                Message::Barrier(barrier) => {
                    self.progress.finish(barrier.epoch.curr);
                    yield Message::Barrier(barrier);
                }
            }
        }
    }
}

impl Executor for ChainExecutor {
    fn execute(self: Box<Self>) -> super::BoxedMessageStream {
        self.execute_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> super::PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

#[cfg(test)]
mod test {
    use std::default::Default;
    use std::sync::Arc;

    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_pb::stream_plan::Dispatcher;

    use super::ChainExecutor;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{Barrier, Executor, Message, Mutation, PkIndices};
    use crate::task::{CreateMviewProgress, LocalBarrierManager};

    #[tokio::test]
    async fn test_basic() {
        let barrier_manager = LocalBarrierManager::for_test();
        let progress =
            CreateMviewProgress::for_test(Arc::new(parking_lot::Mutex::new(barrier_manager)));
        let actor_id = progress.actor_id();

        let schema = Schema::new(vec![Field::unnamed(DataType::Int64)]);

        let second = Box::new(MockSource::with_messages(
            schema.clone(),
            PkIndices::new(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1).with_mutation(Mutation::Add {
                    adds: maplit::hashmap! {
                        0 => vec![Dispatcher {
                            downstream_actor_id: vec![actor_id],
                            ..Default::default()
                        }],
                    },
                    splits: Default::default(),
                })),
                Message::Chunk(StreamChunk::from_pretty("I\n + 1")),
                Message::Chunk(StreamChunk::from_pretty("I\n + 2")),
            ],
        ));

        let chain = ChainExecutor::new(second, vec![0], progress, schema, PkIndices::new());

        let mut chain = Box::new(chain).execute();
        chain.next().await;

        let mut count = 0;
        while let Some(Message::Chunk(ck)) = chain.next().await.transpose().unwrap() {
            count += 1;
            assert_eq!(ck, StreamChunk::from_pretty(&format!("I\n + {count}")));
        }
        assert_eq!(count, 2);
    }
}
