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

use futures::stream;
use itertools::Itertools;
use risingwave_common::array::Op;
use risingwave_common::bitmap::BitmapBuilder;
use risingwave_common::row::RowExt;

use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::executor::prelude::*;

type Cache = ManagedLruCache<OwnedRow, ()>;

/// [`AppendOnlyDedupExecutor`] drops any message that has duplicate pk columns with previous
/// messages. It only accepts append-only input, and its output will be append-only as well.
pub struct AppendOnlyDedupExecutor<S: StateStore> {
    ctx: ActorContextRef,

    input: Option<Executor>,
    dedup_cols: Vec<usize>,
    state_table: StateTable<S>,
    cache: Cache,
}

impl<S: StateStore> AppendOnlyDedupExecutor<S> {
    pub fn new(
        ctx: ActorContextRef,
        input: Executor,
        dedup_cols: Vec<usize>,
        state_table: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        let metrics_info =
            MetricsInfo::new(metrics, state_table.table_id(), ctx.id, "AppendOnly Dedup");
        Self {
            ctx,
            input: Some(input),
            dedup_cols,
            state_table,
            cache: Cache::unbounded(watermark_epoch, metrics_info),
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn executor_inner(mut self) {
        let mut input = self.input.take().unwrap().execute();

        // Consume the first barrier message and initialize state table.
        let barrier = expect_first_barrier(&mut input).await?;
        let first_epoch = barrier.epoch;
        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);
        self.state_table.init_epoch(first_epoch).await?;

        #[for_await]
        for msg in input {
            self.cache.evict();

            match msg? {
                Message::Chunk(chunk) => {
                    // Append-only dedup executor only receives INSERT messages.
                    debug_assert!(chunk.ops().iter().all(|&op| op == Op::Insert));

                    // Extract dedup keys for all rows (regardless of visibility) in the chunk.
                    let dedup_keys = chunk
                        .data_chunk()
                        .rows_with_holes()
                        .map(|row_ref| {
                            row_ref.map(|row| row.project(&self.dedup_cols).to_owned_row())
                        })
                        .collect_vec();

                    // Ensure that if a key for a visible row exists before, then it is in the
                    // cache, by querying the storage.
                    self.populate_cache(dedup_keys.iter().flatten()).await?;

                    // Now check for duplication and insert new keys into the cache.
                    let mut vis_builder = BitmapBuilder::with_capacity(chunk.capacity());
                    for key in dedup_keys {
                        match key {
                            Some(key) => {
                                if self.cache.put(key, ()).is_none() {
                                    // The key doesn't exist before. The row should be visible.
                                    // Note that we can do deduplication in such a simple way because
                                    // this executor only accepts append-only input. Otherwise, we can
                                    // only do this if dedup columns contain all the input columns.
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
                        let chunk = StreamChunk::with_visibility(ops, columns, vis);
                        self.state_table.write_chunk(chunk.clone());
                        self.state_table.try_flush().await?;

                        yield Message::Chunk(chunk);
                    }
                }

                Message::Barrier(barrier) => {
                    let post_commit = self.state_table.commit(barrier.epoch).await?;

                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(self.ctx.id);
                    yield Message::Barrier(barrier);

                    if let Some((_, cache_may_stale)) =
                        post_commit.post_yield_barrier(update_vnode_bitmap).await?
                        && cache_may_stale {
                            self.cache.clear();
                        }
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
        let mut futures = vec![];
        for key in keys {
            if self.cache.contains(key) {
                continue;
            }

            let table = &self.state_table;
            futures.push(async move { (key, table.get_encoded_row(key).await) });
        }

        if !futures.is_empty() {
            let mut buffered = stream::iter(futures).buffer_unordered(10).fuse();
            while let Some(result) = buffered.next().await {
                let (key, value) = result;
                if value?.is_some() {
                    // Only insert into the cache when we have this key in storage.
                    self.cache.put(key.to_owned(), ());
                }
            }
        }

        Ok(())
    }
}

impl<S: StateStore> Execute for AppendOnlyDedupExecutor<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.executor_inner().boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::test_utils::gen_pbtable;
    use crate::executor::test_utils::MockSource;

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
        let dedup_col_indices = vec![0];
        let pk_indices = dedup_col_indices.clone();
        let order_types = vec![OrderType::ascending()];

        let state_store = MemoryStateStore::new();
        let state_table = StateTable::from_table_catalog(
            &gen_pbtable(table_id, column_descs, order_types, pk_indices.clone(), 0),
            state_store,
            None,
        )
        .await;

        let (mut tx, source) = MockSource::channel();
        let source = source.into_executor(schema, pk_indices);
        let mut dedup_executor = AppendOnlyDedupExecutor::new(
            ActorContext::for_test(123),
            source,
            dedup_col_indices,
            state_table,
            Arc::new(AtomicU64::new(0)),
            Arc::new(StreamingMetrics::unused()),
        )
        .boxed()
        .execute();

        tx.push_barrier(test_epoch(1), false);
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

        tx.push_barrier(test_epoch(2), false);
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
