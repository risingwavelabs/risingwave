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

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

use risingwave_common::array::Op;
use risingwave_common::hash::HashKey;
use risingwave_common::row::{RowDeserializer, RowExt};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::sort_util::ColumnOrder;

use super::top_n_cache::TopNCacheTrait;
use super::utils::*;
use super::{ManagedTopNState, TopNCache};
use crate::cache::ManagedLruCache;
use crate::common::metrics::MetricsInfo;
use crate::common::table::state_table::StateTablePostCommit;
use crate::executor::monitor::GroupTopNMetrics;
use crate::executor::prelude::*;

pub type GroupTopNExecutor<K, S, const WITH_TIES: bool> =
    TopNExecutorWrapper<InnerGroupTopNExecutor<K, S, WITH_TIES>>;

impl<K: HashKey, S: StateStore, const WITH_TIES: bool> GroupTopNExecutor<K, S, WITH_TIES> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Executor,
        ctx: ActorContextRef,
        schema: Schema,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
    ) -> StreamResult<Self> {
        let inner = InnerGroupTopNExecutor::new(
            schema,
            storage_key,
            offset_and_limit,
            order_by,
            group_by,
            state_table,
            watermark_epoch,
            &ctx,
        )?;
        Ok(TopNExecutorWrapper { input, ctx, inner })
    }
}

pub struct InnerGroupTopNExecutor<K: HashKey, S: StateStore, const WITH_TIES: bool> {
    schema: Schema,

    /// `LIMIT XXX`. None means no limit.
    limit: usize,

    /// `OFFSET XXX`. `0` means no offset.
    offset: usize,

    /// The storage key indices of the `GroupTopNExecutor`
    storage_key_indices: PkIndices,

    managed_state: ManagedTopNState<S>,

    /// which column we used to group the data.
    group_by: Vec<usize>,

    /// group key -> cache for this group
    caches: GroupTopNCache<K, WITH_TIES>,

    /// Used for serializing pk into `CacheKey`.
    cache_key_serde: CacheKeySerde,

    metrics: GroupTopNMetrics,
}

impl<K: HashKey, S: StateStore, const WITH_TIES: bool> InnerGroupTopNExecutor<K, S, WITH_TIES> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: Schema,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
        ctx: &ActorContext,
    ) -> StreamResult<Self> {
        let metrics_info = MetricsInfo::new(
            ctx.streaming_metrics.clone(),
            state_table.table_id(),
            ctx.id,
            "GroupTopN",
        );
        let metrics = ctx.streaming_metrics.new_group_top_n_metrics(
            state_table.table_id(),
            ctx.id,
            ctx.fragment_id,
        );

        let cache_key_serde = create_cache_key_serde(&storage_key, &schema, &order_by, &group_by);
        let managed_state = ManagedTopNState::<S>::new(state_table, cache_key_serde.clone());

        Ok(Self {
            schema,
            offset: offset_and_limit.0,
            limit: offset_and_limit.1,
            managed_state,
            storage_key_indices: storage_key.into_iter().map(|op| op.column_index).collect(),
            group_by,
            caches: GroupTopNCache::new(watermark_epoch, metrics_info),
            cache_key_serde,
            metrics,
        })
    }
}

pub struct GroupTopNCache<K: HashKey, const WITH_TIES: bool> {
    data: ManagedLruCache<K, TopNCache<WITH_TIES>>,
}

impl<K: HashKey, const WITH_TIES: bool> GroupTopNCache<K, WITH_TIES> {
    pub fn new(watermark_sequence: AtomicU64Ref, metrics_info: MetricsInfo) -> Self {
        let cache = ManagedLruCache::unbounded(watermark_sequence, metrics_info);
        Self { data: cache }
    }
}

impl<K: HashKey, const WITH_TIES: bool> Deref for GroupTopNCache<K, WITH_TIES> {
    type Target = ManagedLruCache<K, TopNCache<WITH_TIES>>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<K: HashKey, const WITH_TIES: bool> DerefMut for GroupTopNCache<K, WITH_TIES> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<K: HashKey, S: StateStore, const WITH_TIES: bool> TopNExecutorBase
    for InnerGroupTopNExecutor<K, S, WITH_TIES>
where
    TopNCache<WITH_TIES>: TopNCacheTrait,
{
    type State = S;

    async fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let keys = K::build_many(&self.group_by, chunk.data_chunk());
        let mut stagings = HashMap::new(); // K -> `TopNStaging`

        for (r, group_cache_key) in chunk.rows_with_holes().zip_eq_debug(keys.iter()) {
            let Some((op, row_ref)) = r else {
                continue;
            };

            // The pk without group by
            let pk_row = row_ref.project(&self.storage_key_indices[self.group_by.len()..]);
            let cache_key = serialize_pk_to_cache_key(pk_row, &self.cache_key_serde);

            let group_key = row_ref.project(&self.group_by);
            self.metrics.group_top_n_total_query_cache_count.inc();
            // If 'self.caches' does not already have a cache for the current group, create a new
            // cache for it and insert it into `self.caches`
            if !self.caches.contains(group_cache_key) {
                self.metrics.group_top_n_cache_miss_count.inc();
                let mut topn_cache =
                    TopNCache::new(self.offset, self.limit, self.schema.data_types());
                self.managed_state
                    .init_topn_cache(Some(group_key), &mut topn_cache)
                    .await?;
                self.caches.put(group_cache_key.clone(), topn_cache);
            }

            let mut cache = self.caches.get_mut(group_cache_key).unwrap();
            let staging = stagings.entry(group_cache_key.clone()).or_default();

            // apply the chunk to state table
            match op {
                Op::Insert | Op::UpdateInsert => {
                    self.managed_state.insert(row_ref);
                    cache.insert(cache_key, row_ref, staging);
                }

                Op::Delete | Op::UpdateDelete => {
                    self.managed_state.delete(row_ref);
                    cache
                        .delete(
                            Some(group_key),
                            &mut self.managed_state,
                            cache_key,
                            row_ref,
                            staging,
                        )
                        .await?;
                }
            }
        }

        self.metrics
            .group_top_n_cached_entry_count
            .set(self.caches.len() as i64);

        let data_types = self.schema.data_types();
        let deserializer = RowDeserializer::new(data_types.clone());
        let mut chunk_builder = StreamChunkBuilder::unlimited(data_types, Some(chunk.capacity()));
        for staging in stagings.into_values() {
            for res in staging.into_deserialized_changes(&deserializer) {
                let (op, row) = res?;
                let _none = chunk_builder.append_row(op, row);
            }
        }
        Ok(chunk_builder.take())
    }

    async fn flush_data(
        &mut self,
        epoch: EpochPair,
    ) -> StreamExecutorResult<StateTablePostCommit<'_, S>> {
        self.managed_state.flush(epoch).await
    }

    async fn try_flush_data(&mut self) -> StreamExecutorResult<()> {
        self.managed_state.try_flush().await
    }

    fn clear_cache(&mut self) {
        self.caches.clear();
    }

    fn evict(&mut self) {
        self.caches.evict()
    }

    async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.init_epoch(epoch).await
    }

    async fn handle_watermark(&mut self, watermark: Watermark) -> Option<Watermark> {
        if watermark.col_idx == self.group_by[0] {
            self.managed_state.update_watermark(watermark.val.clone());
            Some(watermark)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::Field;
    use risingwave_common::hash::SerializedKey;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::top_n_executor::create_in_memory_state_table;
    use crate::executor::test_utils::{MockSource, StreamExecutorTestExt};

    fn create_schema() -> Schema {
        Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        }
    }

    fn storage_key() -> Vec<ColumnOrder> {
        vec![
            ColumnOrder::new(1, OrderType::ascending()),
            ColumnOrder::new(2, OrderType::ascending()),
            ColumnOrder::new(0, OrderType::ascending()),
        ]
    }

    /// group by 1, order by 2
    fn order_by_1() -> Vec<ColumnOrder> {
        vec![ColumnOrder::new(2, OrderType::ascending())]
    }

    /// group by 1,2, order by 0
    fn order_by_2() -> Vec<ColumnOrder> {
        vec![ColumnOrder::new(0, OrderType::ascending())]
    }

    fn pk_indices() -> PkIndices {
        vec![1, 2, 0]
    }

    fn create_stream_chunks() -> Vec<StreamChunk> {
        let chunk0 = StreamChunk::from_pretty(
            "  I I I
            + 10 9 1
            +  8 8 2
            +  7 8 2
            +  9 1 1
            + 10 1 1
            +  8 1 3",
        );
        let chunk1 = StreamChunk::from_pretty(
            "  I I I
            - 10 9 1
            -  8 8 2
            - 10 1 1",
        );
        let chunk2 = StreamChunk::from_pretty(
            " I I I
            - 7 8 2
            - 8 1 3
            - 9 1 1",
        );
        let chunk3 = StreamChunk::from_pretty(
            "  I I I
            +  5 1 1
            +  2 1 1
            +  3 1 2
            +  4 1 3",
        );
        vec![chunk0, chunk1, chunk2, chunk3]
    }

    fn create_source() -> Executor {
        let mut chunks = create_stream_chunks();
        let schema = create_schema();
        MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(std::mem::take(&mut chunks[0])),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(std::mem::take(&mut chunks[1])),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
            Message::Chunk(std::mem::take(&mut chunks[2])),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
            Message::Chunk(std::mem::take(&mut chunks[3])),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(5))),
        ])
        .into_executor(schema, pk_indices())
    }

    #[tokio::test]
    async fn test_without_offset_and_with_limits() {
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::ascending(),
                OrderType::ascending(),
                OrderType::ascending(),
            ],
            &pk_indices(),
        )
        .await;
        let schema = source.schema().clone();
        let top_n = GroupTopNExecutor::<SerializedKey, MemoryStateStore, false>::new(
            source,
            ActorContext::for_test(0),
            schema,
            storage_key(),
            (0, 2),
            order_by_1(),
            vec![1],
            state_table,
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap();
        let mut top_n = top_n.boxed().execute();

        // consume the init barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                "  I I I
                + 10 9 1
                +  8 8 2
                +  7 8 2
                +  9 1 1
                + 10 1 1
                ",
            )
            .sort_rows(),
        );

        // barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                "  I I I
                - 10 9 1
                -  8 8 2
                - 10 1 1
                +  8 1 3
                ",
            )
            .sort_rows(),
        );

        // barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                " I I I
                - 7 8 2
                - 8 1 3
                - 9 1 1
                ",
            )
            .sort_rows(),
        );

        // barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                " I I I
                + 5 1 1
                + 2 1 1
                ",
            )
            .sort_rows(),
        );
    }

    #[tokio::test]
    async fn test_with_offset_and_with_limits() {
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::ascending(),
                OrderType::ascending(),
                OrderType::ascending(),
            ],
            &pk_indices(),
        )
        .await;
        let schema = source.schema().clone();
        let top_n = GroupTopNExecutor::<SerializedKey, MemoryStateStore, false>::new(
            source,
            ActorContext::for_test(0),
            schema,
            storage_key(),
            (1, 2),
            order_by_1(),
            vec![1],
            state_table,
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap();
        let mut top_n = top_n.boxed().execute();

        // consume the init barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                "  I I I
                +  8 8 2
                + 10 1 1
                +  8 1 3
                ",
            )
            .sort_rows(),
        );

        // barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                "  I I I
                -  8 8 2
                - 10 1 1
                ",
            )
            .sort_rows(),
        );

        // barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                " I I I
                - 8 1 3",
            )
            .sort_rows(),
        );

        // barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                " I I I
                + 5 1 1
                + 3 1 2
                ",
            )
            .sort_rows(),
        );
    }

    #[tokio::test]
    async fn test_multi_group_key() {
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64, DataType::Int64],
            &[
                OrderType::ascending(),
                OrderType::ascending(),
                OrderType::ascending(),
            ],
            &pk_indices(),
        )
        .await;
        let schema = source.schema().clone();
        let top_n = GroupTopNExecutor::<SerializedKey, MemoryStateStore, false>::new(
            source,
            ActorContext::for_test(0),
            schema,
            storage_key(),
            (0, 2),
            order_by_2(),
            vec![1, 2],
            state_table,
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap();
        let mut top_n = top_n.boxed().execute();

        // consume the init barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                "  I I I
                + 10 9 1
                +  8 8 2
                +  7 8 2
                +  9 1 1
                + 10 1 1
                +  8 1 3",
            )
            .sort_rows(),
        );

        // barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                "  I I I
                - 10 9 1
                -  8 8 2
                - 10 1 1",
            )
            .sort_rows(),
        );

        // barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                "  I I I
                - 7 8 2
                - 8 1 3
                - 9 1 1",
            )
            .sort_rows(),
        );

        // barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                "  I I I
                +  5 1 1
                +  2 1 1
                +  3 1 2
                +  4 1 3",
            )
            .sort_rows(),
        );
    }

    #[tokio::test]
    async fn test_compact_changes() {
        let schema = create_schema();
        let source = MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(StreamChunk::from_pretty(
                "  I I I
                +  0 0 9
                +  0 0 8
                +  0 0 7
                +  0 0 6
                +  0 1 15
                +  0 1 14",
            )),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(StreamChunk::from_pretty(
                "  I I I
                -  0 0 6
                -  0 0 8
                +  0 0 4
                +  0 0 3
                +  0 1 12
                +  0 2 26
                -  0 1 12
                +  0 1 11",
            )),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
            Message::Chunk(StreamChunk::from_pretty(
                "  I I I
                +  0 0 11", // this should result in no chunk output
            )),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
        ])
        .into_executor(schema.clone(), vec![2]);

        let state_table = create_in_memory_state_table(
            &schema.data_types(),
            &[
                OrderType::ascending(),
                OrderType::ascending(),
                OrderType::ascending(),
            ],
            &[0, 1, 2], // table pk = group key (0, 1) + order key (2) + additional pk (empty)
        )
        .await;

        let top_n = GroupTopNExecutor::<SerializedKey, MemoryStateStore, false>::new(
            source,
            ActorContext::for_test(0),
            schema,
            vec![
                ColumnOrder::new(0, OrderType::ascending()),
                ColumnOrder::new(1, OrderType::ascending()),
                ColumnOrder::new(2, OrderType::ascending()),
            ],
            (0, 2), // (offset, limit)
            vec![ColumnOrder::new(2, OrderType::ascending())],
            vec![0, 1],
            state_table,
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap();
        let mut top_n = top_n.boxed().execute();

        // initial barrier
        top_n.expect_barrier().await;

        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                "  I I I
                +  0 0 7
                +  0 0 6
                +  0 1 15
                +  0 1 14",
            )
            .sort_rows(),
        );
        top_n.expect_barrier().await;

        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                "  I I I
                -  0 0 6
                -  0 0 7
                +  0 0 4
                +  0 0 3
                -  0 1 15
                +  0 1 11
                +  0 2 26",
            )
            .sort_rows(),
        );
        top_n.expect_barrier().await;

        // no output chunk for the last input chunk
        top_n.expect_barrier().await;
    }
}
