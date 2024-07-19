// Copyright 2024 RisingWave Labs
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

use std::pin::pin;

use futures::TryStreamExt;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::util::chunk_coalesce::DataChunkBuilder;
use risingwave_hummock_sdk::HummockReadEpoch;
use risingwave_storage::table::batch_table::storage_table::StorageTable;
use risingwave_storage::StateStore;

use crate::executor::backfill::utils::mapping_chunk;
use crate::executor::prelude::{try_stream, StreamExt};
use crate::executor::{
    expect_first_barrier, ActorContextRef, BackfillExecutor, BoxedMessageStream, Execute, Executor,
    Message, StreamExecutorError,
};
use crate::task::CreateMviewProgress;

/// Similar to [`super::no_shuffle_backfill::BackfillExecutor`].
/// Main differences:
/// - [`crate::executor::ArrangementBackfillExecutor`] can reside on a different CN, so it can be scaled
///   independently.
/// - To synchronize upstream shared buffer, it is initialized with a [`ReplicatedStateTable`].
pub struct SnapshotBackfillExecutor<S: StateStore> {
    /// Upstream table
    upstream_table: StorageTable<S>,

    /// Upstream with the same schema with the upstream table.
    upstream: Executor,

    /// The column indices need to be forwarded to the downstream from the upstream and table scan.
    output_indices: Vec<usize>,

    progress: CreateMviewProgress,

    chunk_size: usize,
}

impl<S: StateStore> SnapshotBackfillExecutor<S> {
    pub fn new(
        upstream_table: StorageTable<S>,
        upstream: Executor,
        output_indices: Vec<usize>,
        _actor_ctx: ActorContextRef,
        progress: CreateMviewProgress,
        chunk_size: usize,
    ) -> Self {
        Self {
            upstream_table,
            upstream,
            output_indices,
            progress,
            chunk_size,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self) {
        let upstream = self.upstream.execute();
        let mut upstream = pin!(upstream);
        let first_barrier = expect_first_barrier(&mut upstream).await?;
        let prev_epoch = first_barrier.epoch.prev;
        yield Message::Barrier(first_barrier);
        let snapshot_stream = BackfillExecutor::make_snapshot_stream(
            &self.upstream_table,
            HummockReadEpoch::Committed(prev_epoch),
            None,
            false,
            &None,
        );
        let mut count = 0;
        let mut snapshot_stream = pin!(snapshot_stream);
        let data_types = self.upstream_table.schema().data_types();
        let mut builder = DataChunkBuilder::new(data_types, self.chunk_size);
        while let Some(row) = snapshot_stream.try_next().await? {
            count += 1;
            if let Some(data_chunk) = builder.append_one_row(row) {
                let ops = vec![Op::Insert; data_chunk.capacity()];
                yield Message::Chunk(mapping_chunk(
                    StreamChunk::from_parts(ops, data_chunk),
                    &self.output_indices,
                ));
            }
        }
        if let Some(data_chunk) = builder.consume_all() {
            let ops = vec![Op::Insert; data_chunk.capacity()];
            yield Message::Chunk(mapping_chunk(
                StreamChunk::from_parts(ops, data_chunk),
                &self.output_indices,
            ));
        }
        let barrier = loop {
            match upstream.try_next().await? {
                None => {
                    return Ok(());
                }
                Some(Message::Barrier(barrier)) => {
                    break barrier;
                }
                Some(msg) => {
                    yield msg;
                }
            }
        };
        self.progress.finish(barrier.epoch, count);
        yield Message::Barrier(barrier);
        while let Some(msg) = upstream.try_next().await? {
            yield msg;
        }
    }
}

impl<S: StateStore> Execute for SnapshotBackfillExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
    }
}
