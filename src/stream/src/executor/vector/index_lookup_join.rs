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

use futures::TryStreamExt;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::bail;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::StateStore;
use risingwave_storage::table::batch_table::VectorIndexReader;

use crate::executor::prelude::try_stream;
use crate::executor::{
    BoxedMessageStream, Execute, Executor, Message, StreamExecutorError, expect_first_barrier,
};

pub struct VectorIndexLookupJoinExecutor<S: StateStore> {
    input: Executor,

    reader: VectorIndexReader<S>,
    vector_column_idx: usize,
}

impl<S: StateStore> Execute for VectorIndexLookupJoinExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        Box::pin(self.execute_inner())
    }
}

impl<S: StateStore> VectorIndexLookupJoinExecutor<S> {
    pub fn new(input: Executor, reader: VectorIndexReader<S>, vector_column_idx: usize) -> Self {
        Self {
            input,
            reader,
            vector_column_idx,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn execute_inner(self) {
        let Self {
            input,
            reader,
            vector_column_idx,
        } = self;

        let mut input = input.execute();

        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        yield Message::Barrier(barrier);

        let mut read_snapshot = reader
            .new_snapshot(HummockReadEpoch::Committed(first_epoch.prev))
            .await?;

        while let Some(msg) = input.try_next().await? {
            match msg {
                Message::Barrier(barrier) => {
                    let is_checkpoint = barrier.is_checkpoint();
                    let prev_epoch = barrier.epoch.prev;
                    yield Message::Barrier(barrier);
                    if is_checkpoint {
                        read_snapshot = reader
                            .new_snapshot(HummockReadEpoch::Committed(prev_epoch))
                            .await?;
                    }
                }
                Message::Chunk(chunk) => {
                    let (chunk, ops) = chunk.into_parts();
                    if ops.iter().any(|op| *op != Op::Insert) {
                        bail!("streaming vector index lookup join only support append-only input");
                    }
                    let chunk = read_snapshot
                        .query_expand_chunk(chunk, vector_column_idx)
                        .await?;
                    yield Message::Chunk(StreamChunk::from_parts(ops, chunk));
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
            }
        }
    }
}
