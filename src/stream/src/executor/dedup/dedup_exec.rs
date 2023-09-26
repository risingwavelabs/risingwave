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

use std::sync::Arc;

use futures::{stream, StreamExt};
use futures_async_stream::try_stream;
use itertools::Itertools;
use risingwave_common::array::{ArrayBuilder, I64ArrayBuilder, StreamChunk};
use risingwave_common::buffer::BitmapBuilder;
use risingwave_common::catalog::Schema;
use risingwave_common::row::{OwnedRow, Row, RowExt};
use risingwave_storage::StateStore;

use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::StateTable;
use crate::executor::dedup::dedup_cache::DedupCache;
use crate::executor::error::StreamExecutorError;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{
    expect_first_barrier, ActorContextRef, BoxedExecutor, BoxedMessageStream, Executor, Message,
    PkIndices, PkIndicesRef, StreamExecutorResult,
};
use crate::task::AtomicU64Ref;

/// [`DedupExecutor`] drops any message that has duplicate pk columns with previous
/// messages. It accepts both append-only and non-append-only input.
/// It will keep a dup_count as a state and the row is visible to the downstream
/// when the count increases from 0 or decreases to 0.
pub struct DedupExecutor<S: StateStore> {
    input: Option<BoxedExecutor>,
    state_table: StateTable<S>,
    cache: DedupCache<OwnedRow>,

    pk_indices: PkIndices,
    identity: String,
    schema: Schema,
    ctx: ActorContextRef,
}

impl<S: StateStore> DedupExecutor<S> {
    pub fn new(
        input: BoxedExecutor,
        state_table: StateTable<S>,
        pk_indices: PkIndices,
        executor_id: u64,
        ctx: ActorContextRef,
        watermark_epoch: AtomicU64Ref,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        let schema = input.schema().clone();
        let metrics_info = MetricsInfo::new(metrics, state_table.table_id(), ctx.id, "Dedup");
        Self {
            input: Some(input),
            state_table,
            cache: DedupCache::new(watermark_epoch, metrics_info),
            pk_indices,
            identity: format!("DedupExecutor {:X}", executor_id),
            schema,
            ctx,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn executor_inner(mut self) {
        let mut input = self.input.take().unwrap().execute();

        // Consume the first barrier message and initialize state table.
        let barrier = expect_first_barrier(&mut input).await?;

        #[cfg(test)]
        self.state_table.commit_no_data_expected(barrier.epoch);
        #[cfg(not(test))]
        self.state_table.init_epoch(barrier.epoch);

        // The first barrier message should be propagated.
        yield Message::Barrier(barrier);

        #[for_await]
        for msg in input {
            self.cache.evict();

            match msg? {
                Message::Chunk(chunk) => {
                    // Extract pk for all rows (regardless of visibility) in the chunk.
                    let keys = chunk
                        .data_chunk()
                        .rows_with_holes()
                        .map(|row_ref| {
                            row_ref.map(|row| row.project(self.pk_indices()).to_owned_row())
                        })
                        .collect_vec();
                    let ops = chunk.ops();

                    // Ensure that if a key for a visible row exists before, then it is in the
                    // cache, by querying the storage.
                    self.populate_cache(keys.iter().flatten()).await?;

                    // Now check for duplication and insert new keys into the cache.
                    let mut vis_builder = BitmapBuilder::with_capacity(chunk.capacity());
                    let mut cnt_builder = I64ArrayBuilder::new(chunk.capacity());

                    for (key, op) in keys.into_iter().zip_eq(ops.iter()) {
                        match key {
                            Some(key) => {
                                let (is_vis, cnt) = self.cache.apply_dedup(op, key);
                                if is_vis {
                                    // The key doesn't exist before. The row should be visible.
                                    vis_builder.append(true);
                                    cnt_builder.append(Some(cnt));
                                } else {
                                    // The key exists before. The row shouldn't be visible.
                                    vis_builder.append(false);
                                    cnt_builder.append(Some(cnt));
                                }
                            }
                            None => {
                                // The row is originally invisible.
                                vis_builder.append(false);
                                cnt_builder.append(Some(0));
                            }
                        }
                    }

                    let vis = vis_builder.finish();
                    let cnt = cnt_builder.finish();

                    let (ops, mut columns, _) = chunk.into_inner();
                    let chunk = StreamChunk::new(ops.clone(), columns.clone(), Some(vis.clone()));

                    // Construct the new chunk and write the data to state table.
                    columns.push(Arc::new(cnt.into()));
                    let state_chunk = StreamChunk::new(ops, columns, None);
                    self.state_table.write_chunk(state_chunk);

                    yield Message::Chunk(chunk);
                }

                Message::Barrier(barrier) => {
                    self.state_table.commit(barrier.epoch).await?;

                    if let Some(vnode_bitmap) = barrier.as_update_vnode_bitmap(self.ctx.id) {
                        let (_prev_vnode_bitmap, cache_may_stale) =
                            self.state_table.update_vnode_bitmap(vnode_bitmap);
                        if cache_may_stale {
                            self.cache.clear();
                        }
                    }

                    self.cache.update_epoch(barrier.epoch.curr);

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
            futures.push(async move { (key, table.get_row(key).await) });
        }

        if read_from_storage {
            let mut buffered = stream::iter(futures).buffer_unordered(10).fuse();
            while let Some(result) = buffered.next().await {
                let (key, value) = result;
                if let Some(v) = value? {
                    // load row and its count into memory
                    let dup_cnt = v.last().unwrap().into_int64();
                    self.cache.insert(key.to_owned(), dup_cnt);
                }
            }
        }

        Ok(())
    }
}

impl<S: StateStore> Executor for DedupExecutor<S> {
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

    use risingwave_common::array::{I64Array, Op, Utf8Array};
    use risingwave_common::buffer::Bitmap;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, Schema, TableId};
    use risingwave_common::test_prelude::StreamChunkTestExt;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::EpochPair;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::common::table::state_table::StateTable;
    use crate::executor::test_utils::MockSource;
    use crate::executor::ActorContext;

    // dedup executor will see the whole line as pk and the value is number of duplication
    #[tokio::test]
    async fn test_dedup_load_memory() {
        let table_id = TableId::new(1);
        let column_descs = vec![
            ColumnDesc::unnamed(ColumnId::new(0), DataType::Int64),
            ColumnDesc::unnamed(ColumnId::new(1), DataType::Varchar),
            ColumnDesc::unnamed(ColumnId::new(2), DataType::Int64), // duplication count
        ];
        let pk_indices = vec![0, 1];
        let order_types = vec![OrderType::ascending(), OrderType::ascending()];
        let state_store = MemoryStateStore::new();
        let mut state_table = StateTable::new_without_distribution_inconsistent_op(
            state_store,
            table_id,
            column_descs,
            order_types,
            pk_indices.clone(),
        )
        .await;

        // preset state table
        // (1, "a") -> 2
        // (2, "b") -> 4
        let chunk = StreamChunk::new(
            vec![Op::Insert, Op::Insert],
            vec![
                Arc::new(I64Array::from_iter(vec![1, 2]).into()),
                Arc::new(Utf8Array::from_iter(vec!["a", "b"]).into()),
                Arc::new(I64Array::from_iter(vec![2, 4]).into()), // dup_count
            ],
            Some(Bitmap::from_iter(vec![true, true])),
        );
        state_table.init_epoch(EpochPair::new_test_epoch(1));
        state_table.write_chunk(chunk);
        state_table
            .commit(EpochPair::new_test_epoch(2))
            .await
            .unwrap();

        // input schema has fewer columns than state table
        let input_schema = Schema::new(vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Varchar),
        ]);

        let (mut tx, input) = MockSource::channel(input_schema, pk_indices.clone());
        let mut dedup_executor = Box::new(DedupExecutor::new(
            Box::new(input),
            state_table,
            pk_indices,
            1,
            ActorContext::create(123),
            Arc::new(AtomicU64::new(0)),
            Arc::new(StreamingMetrics::unused()),
        ))
        .execute();

        tx.push_barrier(3, false);
        dedup_executor.next().await.unwrap().unwrap();

        let chunk1: StreamChunk = {
            StreamChunk::new(
                vec![Op::Delete, Op::Delete],
                vec![
                    Arc::new(I64Array::from_iter(vec![1, 2]).into()),
                    Arc::new(Utf8Array::from_iter(vec!["a", "b"]).into()),
                ],
                Some(Bitmap::from_iter(vec![true, true])),
            )
        };
        tx.push_chunk(chunk1.clone());
        let msg = dedup_executor.next().await.unwrap().unwrap();
        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I T
               - 1 a D
               - 2 b D
            "
            )
        );

        tx.push_barrier(4, false);
        dedup_executor.next().await.unwrap().unwrap();

        tx.push_chunk(chunk1.clone());
        let msg = dedup_executor.next().await.unwrap().unwrap();

        assert_eq!(
            msg.into_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I T
               - 1 a
               - 2 b D
            "
            )
        );

        tx.push_barrier(5, false);
        dedup_executor.next().await.unwrap().unwrap();
    }
}
