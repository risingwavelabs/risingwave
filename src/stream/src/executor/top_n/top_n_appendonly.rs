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

use async_trait::async_trait;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::row::{RowDeserializer, RowExt};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_storage::StateStore;

use super::top_n_cache::AppendOnlyTopNCacheTrait;
use super::utils::*;
use super::TopNCache;
use crate::common::table::state_table::StateTable;
use crate::error::StreamResult;
use crate::executor::error::StreamExecutorResult;
use crate::executor::managed_state::top_n::{ManagedTopNState, NO_GROUP_KEY};
use crate::executor::{ActorContextRef, Executor, ExecutorInfo, PkIndices, Watermark};

/// If the input contains only append, `AppendOnlyTopNExecutor` does not need
/// to keep all the data records/rows that have been seen. As long as a record
/// is no longer being in the result set, it can be deleted.
/// TODO: Optimization: primary key may contain several columns and is used to determine
/// the order, therefore the value part should not contain the same columns to save space.
pub type AppendOnlyTopNExecutor<S, const WITH_TIES: bool> =
    TopNExecutorWrapper<InnerAppendOnlyTopNExecutor<S, WITH_TIES>>;

impl<S: StateStore> AppendOnlyTopNExecutor<S, false> {
    #[allow(clippy::too_many_arguments)]
    pub fn new_without_ties(
        input: Box<dyn Executor>,
        ctx: ActorContextRef,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        executor_id: u64,
        state_table: StateTable<S>,
    ) -> StreamResult<Self> {
        let info = input.info();

        Ok(TopNExecutorWrapper {
            input,
            ctx,
            inner: InnerAppendOnlyTopNExecutor::new(
                info,
                storage_key,
                offset_and_limit,
                order_by,
                executor_id,
                state_table,
            )?,
        })
    }
}

impl<S: StateStore> AppendOnlyTopNExecutor<S, true> {
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_ties(
        input: Box<dyn Executor>,
        ctx: ActorContextRef,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        executor_id: u64,
        state_table: StateTable<S>,
    ) -> StreamResult<Self> {
        let info = input.info();

        Ok(TopNExecutorWrapper {
            input,
            ctx,
            inner: InnerAppendOnlyTopNExecutor::new(
                info,
                storage_key,
                offset_and_limit,
                order_by,
                executor_id,
                state_table,
            )?,
        })
    }
}

pub struct InnerAppendOnlyTopNExecutor<S: StateStore, const WITH_TIES: bool> {
    info: ExecutorInfo,

    /// The storage key indices of the `TopNExecutor`
    storage_key_indices: PkIndices,

    /// We are interested in which element is in the range of [offset, offset+limit).
    managed_state: ManagedTopNState<S>,

    /// In-memory cache of top (N + N * `TOPN_CACHE_HIGH_CAPACITY_FACTOR`) rows
    /// TODO: support WITH TIES
    cache: TopNCache<WITH_TIES>,

    /// Used for serializing pk into CacheKey.
    cache_key_serde: CacheKeySerde,
}

impl<S: StateStore, const WITH_TIES: bool> InnerAppendOnlyTopNExecutor<S, WITH_TIES> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_info: ExecutorInfo,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        executor_id: u64,
        state_table: StateTable<S>,
    ) -> StreamResult<Self> {
        let ExecutorInfo {
            pk_indices, schema, ..
        } = input_info;

        let num_offset = offset_and_limit.0;
        let num_limit = offset_and_limit.1;

        let cache_key_serde =
            create_cache_key_serde(&storage_key, &pk_indices, &schema, &order_by, &[]);
        let managed_state = ManagedTopNState::<S>::new(state_table, cache_key_serde.clone());

        Ok(Self {
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: format!("AppendOnlyTopNExecutor {:X}", executor_id),
            },
            managed_state,
            storage_key_indices: storage_key.into_iter().map(|op| op.column_index).collect(),
            cache: TopNCache::new(num_offset, num_limit),
            cache_key_serde,
        })
    }
}

#[async_trait]
impl<S: StateStore, const WITH_TIES: bool> TopNExecutorBase
    for InnerAppendOnlyTopNExecutor<S, WITH_TIES>
where
    TopNCache<WITH_TIES>: AppendOnlyTopNCacheTrait,
{
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<StreamChunk> {
        let mut res_ops = Vec::with_capacity(self.cache.limit);
        let mut res_rows = Vec::with_capacity(self.cache.limit);
        let data_types = self.schema().data_types();
        let row_deserializer = RowDeserializer::new(data_types);
        // apply the chunk to state table
        for (op, row_ref) in chunk.rows() {
            debug_assert_eq!(op, Op::Insert);
            let pk_row = row_ref.project(&self.storage_key_indices);
            let cache_key = serialize_pk_to_cache_key(pk_row, &self.cache_key_serde);
            self.cache.insert(
                cache_key,
                row_ref,
                &mut res_ops,
                &mut res_rows,
                &mut self.managed_state,
                &row_deserializer,
            )?;
        }

        generate_output(res_rows, res_ops, self.schema())
    }

    async fn flush_data(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.flush(epoch).await
    }

    fn info(&self) -> &ExecutorInfo {
        &self.info
    }

    async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.state_table.init_epoch(epoch);
        self.managed_state
            .init_topn_cache(NO_GROUP_KEY, &mut self.cache)
            .await
    }

    async fn handle_watermark(&mut self, _: Watermark) -> Option<Watermark> {
        // TODO(yuhao): handle watermark
        None
    }
}

#[cfg(test)]
mod tests {

    use assert_matches::assert_matches;
    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

    use super::AppendOnlyTopNExecutor;
    use crate::executor::test_utils::top_n_executor::create_in_memory_state_table;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{ActorContext, Barrier, Executor, Message, PkIndices};

    fn create_stream_chunks() -> Vec<StreamChunk> {
        let chunk1 = StreamChunk::from_pretty(
            "  I I
            +  1 0
            +  2 1
            +  3 2
            + 10 3
            +  9 4
            +  8 5",
        );
        let chunk2 = StreamChunk::from_pretty(
            "  I I
            +  7 6
            +  3 7
            +  1 8
            +  9 9",
        );
        let chunk3 = StreamChunk::from_pretty(
            " I  I
            + 1 12
            + 1 13
            + 2 14
            + 3 15",
        );
        vec![chunk1, chunk2, chunk3]
    }

    fn create_schema() -> Schema {
        Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
            ],
        }
    }

    fn storage_key() -> Vec<ColumnOrder> {
        order_by()
    }

    fn order_by() -> Vec<ColumnOrder> {
        vec![
            ColumnOrder::new(0, OrderType::default_ascending()),
            ColumnOrder::new(1, OrderType::default_ascending()),
        ]
    }

    fn pk_indices() -> PkIndices {
        vec![0, 1]
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
            ],
        ))
    }

    #[tokio::test]
    async fn test_append_only_top_n_executor_with_limit() {
        let storage_key = storage_key();
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64],
            &[
                OrderType::default_ascending(),
                OrderType::default_ascending(),
            ],
            &pk_indices(),
        )
        .await;

        let top_n_executor = Box::new(
            AppendOnlyTopNExecutor::new_without_ties(
                source as Box<dyn Executor>,
                ActorContext::create(0),
                storage_key,
                (0, 5),
                order_by(),
                1,
                state_table,
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init epoch
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I
                +  1 0
                +  2 1
                +  3 2
                + 10 3
                +  9 4
                - 10 3
                +  8 5"
            )
        );
        // We added (1, 2, 3, 10, 9, 8).
        // Now (1, 2, 3, 8, 9)
        // Barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I I
                - 9 4
                + 7 6
                - 8 5
                + 3 7
                - 7 6
                + 1 8"
            )
        );
        // We added (7, 3, 1, 9).
        // Now (1, 1, 2, 3, 3)
        // Barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I  I
                - 3  7
                + 1 12
                - 3  2
                + 1 13"
            )
        );
        // We added (1, 1, 2, 3).
        // Now (1, 1, 1, 1, 2)
    }

    #[tokio::test]
    async fn test_append_only_top_n_executor_with_offset_and_limit() {
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64],
            &[
                OrderType::default_ascending(),
                OrderType::default_ascending(),
            ],
            &pk_indices(),
        )
        .await;

        let top_n_executor = Box::new(
            AppendOnlyTopNExecutor::new_without_ties(
                source as Box<dyn Executor>,
                ActorContext::create(0),
                storage_key(),
                (3, 4),
                order_by(),
                1,
                state_table,
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init epoch
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I
                + 10 3
                +  9 4
                +  8 5"
            )
        );
        // We added (1, 2, 3, 10, 9, 8).
        // Now (1, 2, 3) -> (8, 9, 10)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I
                +  7 6
                - 10 3
                +  3 7
                -  9 4
                +  3 2"
            )
        );
        // We added (7, 3, 1, 9).
        // Now (1, 1, 2) -> (3, 3, 7, 8)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                " I  I
                - 8  5
                + 2  1
                - 7  6
                + 1 13
                - 3  7
                + 2 14"
            )
        );
        // We added (1, 1, 2, 3).
        // Now (1, 1, 1) -> (1, 2, 2, 3)
    }
}
