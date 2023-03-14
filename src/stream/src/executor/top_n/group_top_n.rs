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

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::buffer::Bitmap;
use risingwave_common::hash::HashKey;
use risingwave_common::row::RowExt;
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::iter_util::ZipEqDebug;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_storage::StateStore;

use super::top_n_cache::TopNCacheTrait;
use super::utils::*;
use super::TopNCache;
use crate::cache::{cache_may_stale, new_unbounded, ExecutorCache};
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::error::StreamExecutorResult;
use crate::executor::managed_state::top_n::ManagedTopNState;
use crate::executor::{ActorContextRef, Executor, ExecutorInfo, PkIndices, Watermark};
use crate::task::AtomicU64Ref;

pub type GroupTopNExecutor<K, S, const WITH_TIES: bool> =
    TopNExecutorWrapper<InnerGroupTopNExecutorNew<K, S, WITH_TIES>>;

impl<K: HashKey, S: StateStore, const WITH_TIES: bool> GroupTopNExecutor<K, S, WITH_TIES> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn Executor>,
        ctx: ActorContextRef,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        executor_id: u64,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        watermark_epoch: AtomicU64Ref,
    ) -> StreamResult<Self> {
        let info = input.info();
        Ok(TopNExecutorWrapper {
            input,
            ctx,
            inner: InnerGroupTopNExecutorNew::new(
                info,
                storage_key,
                offset_and_limit,
                order_by,
                executor_id,
                group_by,
                state_table,
                watermark_epoch,
            )?,
        })
    }
}

pub struct InnerGroupTopNExecutorNew<K: HashKey, S: StateStore, const WITH_TIES: bool> {
    info: ExecutorInfo,

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

    /// Used for serializing pk into CacheKey.
    cache_key_serde: CacheKeySerde,
}

impl<K: HashKey, S: StateStore, const WITH_TIES: bool> InnerGroupTopNExecutorNew<K, S, WITH_TIES> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_info: ExecutorInfo,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        executor_id: u64,
        group_by: Vec<usize>,
        state_table: StateTable<S>,
        lru_manager: AtomicU64Ref,
    ) -> StreamResult<Self> {
        let ExecutorInfo {
            pk_indices, schema, ..
        } = input_info;

        let cache_key_serde =
            create_cache_key_serde(&storage_key, &pk_indices, &schema, &order_by, &group_by);
        let managed_state = ManagedTopNState::<S>::new(state_table, cache_key_serde.clone());

        Ok(Self {
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: format!("GroupTopNExecutor {:X}", executor_id),
            },
            offset: offset_and_limit.0,
            limit: offset_and_limit.1,
            managed_state,
            storage_key_indices: storage_key.into_iter().map(|op| op.column_index).collect(),
            group_by,
            caches: GroupTopNCache::new(lru_manager),
            cache_key_serde,
        })
    }
}

pub struct GroupTopNCache<K: HashKey, const WITH_TIES: bool> {
    data: ExecutorCache<K, TopNCache<WITH_TIES>>,
}

impl<K: HashKey, const WITH_TIES: bool> GroupTopNCache<K, WITH_TIES> {
    pub fn new(lru_manager: AtomicU64Ref) -> Self {
        let cache = ExecutorCache::new(new_unbounded(lru_manager));
        Self { data: cache }
    }
}

impl<K: HashKey, const WITH_TIES: bool> Deref for GroupTopNCache<K, WITH_TIES> {
    type Target = ExecutorCache<K, TopNCache<WITH_TIES>>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<K: HashKey, const WITH_TIES: bool> DerefMut for GroupTopNCache<K, WITH_TIES> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

#[async_trait]
impl<K: HashKey, S: StateStore, const WITH_TIES: bool> TopNExecutorBase
    for InnerGroupTopNExecutorNew<K, S, WITH_TIES>
where
    TopNCache<WITH_TIES>: TopNCacheTrait,
{
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<StreamChunk> {
        let mut res_ops = Vec::with_capacity(self.limit);
        let mut res_rows = Vec::with_capacity(self.limit);
        let chunk = chunk.compact();
        let keys = K::build(&self.group_by, chunk.data_chunk())?;

        for ((op, row_ref), group_cache_key) in chunk.rows().zip_eq_debug(keys.iter()) {
            // The pk without group by
            let pk_row = row_ref.project(&self.storage_key_indices[self.group_by.len()..]);
            let cache_key = serialize_pk_to_cache_key(pk_row, &self.cache_key_serde);

            let group_key = row_ref.project(&self.group_by);

            // If 'self.caches' does not already have a cache for the current group, create a new
            // cache for it and insert it into `self.caches`
            if !self.caches.contains(group_cache_key) {
                let mut topn_cache = TopNCache::new(self.offset, self.limit);
                self.managed_state
                    .init_topn_cache(Some(group_key), &mut topn_cache)
                    .await?;
                self.caches.push(group_cache_key.clone(), topn_cache);
            }
            let cache = self.caches.get_mut(group_cache_key).unwrap();

            // apply the chunk to state table
            match op {
                Op::Insert | Op::UpdateInsert => {
                    self.managed_state.insert(row_ref);
                    cache.insert(cache_key, row_ref, &mut res_ops, &mut res_rows);
                }

                Op::Delete | Op::UpdateDelete => {
                    self.managed_state.delete(row_ref);
                    cache
                        .delete(
                            Some(group_key),
                            &mut self.managed_state,
                            cache_key,
                            row_ref,
                            &mut res_ops,
                            &mut res_rows,
                        )
                        .await?;
                }
            }
        }

        generate_output(res_rows, res_ops, self.schema())
    }

    async fn flush_data(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.flush(epoch).await
    }

    fn info(&self) -> &ExecutorInfo {
        &self.info
    }

    fn update_vnode_bitmap(&mut self, vnode_bitmap: Arc<Bitmap>) {
        let previous_vnode_bitmap = self
            .managed_state
            .state_table
            .update_vnode_bitmap(vnode_bitmap.clone());

        if cache_may_stale(&previous_vnode_bitmap, &vnode_bitmap) {
            self.caches.clear();
        }
    }

    fn evict(&mut self) {
        self.caches.evict()
    }

    async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.state_table.init_epoch(epoch);
        Ok(())
    }

    async fn handle_watermark(&mut self, watermark: Watermark) -> Option<Watermark> {
        if watermark.col_idx == self.group_by[0] {
            self.managed_state
                .state_table
                .update_watermark(watermark.val.clone());
            Some(watermark)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use assert_matches::assert_matches;
    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::hash::SerializedKey;
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::top_n_executor::create_in_memory_state_table;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{ActorContext, Barrier, Message};

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

    fn create_source() -> Box<MockSource> {
        let mut chunks = create_stream_chunks();
        let schema = create_schema();
        Box::new(MockSource::with_messages(
            schema,
            pk_indices(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Barrier(Barrier::new_test_barrier(2)),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Barrier(Barrier::new_test_barrier(3)),
                Message::Chunk(std::mem::take(&mut chunks[2])),
                Message::Barrier(Barrier::new_test_barrier(4)),
                Message::Chunk(std::mem::take(&mut chunks[3])),
                Message::Barrier(Barrier::new_test_barrier(5)),
            ],
        ))
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
        let a = GroupTopNExecutor::<SerializedKey, MemoryStateStore, false>::new(
            source as Box<dyn Executor>,
            ActorContext::create(0),
            storage_key(),
            (0, 2),
            order_by_1(),
            1,
            vec![1],
            state_table,
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap();
        let top_n_executor = Box::new(a);
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                + 10 9 1
                +  8 8 2
                +  7 8 2
                +  9 1 1
                + 10 1 1
                ",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                - 10 9 1
                -  8 8 2
                - 10 1 1
                +  8 1 3
                ",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                " I I I
                - 7 8 2
                - 8 1 3
                - 9 1 1
                ",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                " I I I
                + 5 1 1
                + 2 1 1
                ",
            ),
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
        let top_n_executor = Box::new(
            GroupTopNExecutor::<SerializedKey, MemoryStateStore, false>::new(
                source as Box<dyn Executor>,
                ActorContext::create(0),
                storage_key(),
                (1, 2),
                order_by_1(),
                1,
                vec![1],
                state_table,
                Arc::new(AtomicU64::new(0)),
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                +  8 8 2
                + 10 1 1
                +  8 1 3
                ",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                -  8 8 2
                - 10 1 1
                ",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                " I I I
                - 8 1 3",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                " I I I
                + 5 1 1
                + 3 1 2
                ",
            ),
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
        let top_n_executor = Box::new(
            GroupTopNExecutor::<SerializedKey, MemoryStateStore, false>::new(
                source as Box<dyn Executor>,
                ActorContext::create(0),
                storage_key(),
                (0, 2),
                order_by_2(),
                1,
                vec![1, 2],
                state_table,
                Arc::new(AtomicU64::new(0)),
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                + 10 9 1
                +  8 8 2
                +  7 8 2
                +  9 1 1
                + 10 1 1
                +  8 1 3",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                - 10 9 1
                -  8 8 2
                - 10 1 1",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                - 7 8 2
                - 8 1 3
                - 9 1 1",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I I
                +  5 1 1
                +  2 1 1
                +  3 1 2
                +  4 1 3",
            ),
        );
    }
}
