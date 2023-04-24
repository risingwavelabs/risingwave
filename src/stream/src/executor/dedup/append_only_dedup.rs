// Copyright 2023 RisingWave Labs
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

use futures::{stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_storage::StateStore;

use super::cache::DedupCache;
use crate::common::table::state_table::StateTable;
use crate::executor::error::StreamExecutorError;
use crate::executor::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor, Message,
    PkIndices, PkIndicesRef, StreamExecutorResult,
};
use crate::task::AtomicU64Ref;

/// [`AppendOnlyDedupExecutor`] drops any message that has duplicate pk columns with previous
/// messages. It only accepts append-only input, and its output will be append-only as well.
pub struct AppendOnlyDedupExecutor<S: StateStore> {
    input: Option<BoxedExecutor>,
    state_table: StateTable<S>,
    cache: DedupCache<OwnedRow>,

    pk_indices: PkIndices,
    identity: String,
    schema: Schema,
    ctx: ActorContextRef,
}

impl<S: StateStore> AppendOnlyDedupExecutor<S> {
    pub fn new(
        input: BoxedExecutor,
        state_table: StateTable<S>,
        pk_indices: PkIndices,
        executor_id: u64,
        ctx: ActorContextRef,
        watermark_epoch: AtomicU64Ref,
    ) -> Self {
        let schema = input.schema().clone();
        Self {
            input: Some(input),
            state_table,
            cache: DedupCache::new(watermark_epoch),
            pk_indices,
            identity: format!("AppendOnlyDedupExecutor {:X}", executor_id),
            schema,
            ctx,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn executor_inner(mut self) {
        let mut input = self.input.take().unwrap().execute();

        // Consume the first barrier message and initialize state table.
        let barrier = expect_first_barrier(&mut input).await?;
        self.state_table.init_epoch(barrier.epoch);

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        let mut commit_data = false;

        #[for_await]
        for msg in input {
            self.cache.evict();

            match msg? {
                Message::Chunk(chunk) => {
                    // Append-only dedup executor only receives INSERT messages.
                    debug_assert!(chunk.ops().iter().all(|&op| op == Op::Insert));

                    // Extract pk for all rows (regardless of visibility) in the chunk.
                    let keys = chunk
                        .data_chunk()
                        .rows_with_holes()
                        .map(|row_ref| {
                            row_ref.map(|row| row.project(self.pk_indices()).to_owned_row())
                        })
                        .collect_vec();

                    // Ensure that if a key for a visible row exists before, then it is in the
                    // cache, by querying the storage.
                    self.populate_cache(keys.iter().flatten()).await?;

                    // Now check for duplication and insert new keys into the cache.
                    let mut vis_builder = BitmapBuilder::with_capacity(chunk.capacity());
                    for key in keys {
                        match key {
                            Some(key) => {
                                if self.cache.dedup_insert(key) {
                                    // The key doesn't exist before. The row should be visible.
                                    vis_builder.append(true);
                                } else {
                                    // The key exists before. The row shouldn't be visible.
                                    vis_builder.append(false);
                                }
                            }
                            None => {
                                // The row is originally invisible.
                                vis_builder.append(false);
                            }
                        }
                    }

                    let vis = vis_builder.finish();
                    if vis.count_ones() > 0 {
                        // Construct the new chunk and write the data to state table.
                        let (ops, columns, _) = chunk.into_inner();
                        let chunk = StreamChunk::new(ops, columns, Some(vis));
                        self.state_table.write_chunk(chunk.clone());

                        commit_data = true;

                        yield Message::Chunk(chunk);
                    }
                }

                Message::Barrier(barrier) => {
                    if commit_data {
                        // Only commit when we have new data in this epoch.
                        self.state_table.commit(barrier.epoch).await?;
                        commit_data = false;
                    } else {
                        self.state_table.commit_no_data_expected(barrier.epoch);
                    }

                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(self.ctx.id) {
                        let (_prev_vnode_bitmap, cache_may_stale) =
                            self.state_table.update_vnode_bitmap(vnode_bitmap);
                        if cache_may_stale {
                            self.cache.clear();
                        }
                    }

                    yield Message::Barrier(barrier);
                }

                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
            }
        }
    }

    /// Populate the cache with keys that exist in storage before.
    pub async fn populate_cache<'a>(
        &mut self,
        keys: impl Iterator<Item = &'a OwnedRow>,
    ) -> StreamExecutorResult<()> {
        let mut read_from_storage = false;
        let mut futures = vec![];
        for key in keys {
            if self.cache.contains(key) {
                continue;
            }
            read_from_storage = true;

            let table = &self.state_table;
            futures.push(async move { (key, table.get_encoded_row(key).await) });
        }

        if read_from_storage {
            let mut buffered = stream::iter(futures).buffer_unordered(10).fuse();
            while let Some(result) = buffered.next().await {
                let (key, value) = result;
                if value?.is_some() {
                    // Only insert into the cache when we have this key in storage.
                    self.cache.insert(key.to_owned());
                }
            }
        }

        Ok(())
    }
}

impl<S: StateStore> Executor for AppendOnlyDedupExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.executor_inner().boxed()
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.identity
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::state_table::StateTable;
    use crate::executor::test_utils::MockSource;
    use crate::executor::ActorContext;

    #[tokio::test]
    async fn test_dedup_executor() {
        let table_id = TableId::new(1);
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Int64),
        ];
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Int64),
        ]);
        let pk_indices = vec![0];
        let order_types = vec![OrderType::ascending()];

        let state_store = MemoryStateStore::new();
        let state_table = StateTable::new_without_distribution(
            state_store,
            table_id,
            column_descs,
            order_types,
            pk_indices.clone(),
        )
        .await;

        let (mut tx, input) = MockSource::channel(schema, pk_indices.clone());
        let mut dedup_executor = Box::new(AppendOnlyDedupExecutor::new(
            Box::new(input),
            state_table,
            pk_indices,
            1,
            ActorContext::create(123),
            Arc::new(AtomicU64::new(0)),
        ))
        .execute();

        tx.push_barrier(1, false);
        dedup_executor.next().await.unwrap().unwrap();

        let chunk = StreamChunk::from_pretty(
            " I I
            + 1 1
            + 2 2 D
            + 1 7",
        );
        tx.push_chunk(chunk);
        let msg = dedup_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 1 1
                + 2 2 D
                + 1 7 D",
            )
        );

        tx.push_barrier(2, false);
        dedup_executor.next().await.unwrap().unwrap();

        let chunk = StreamChunk::from_pretty(
            " I I
            + 3 9
            + 2 5
            + 1 20",
        );
        tx.push_chunk(chunk);
        let msg = dedup_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                + 3 9
                + 2 5
                + 1 20 D",
            )
        );
    }
}
