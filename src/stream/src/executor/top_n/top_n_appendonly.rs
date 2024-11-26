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

use risingwave_common::array::Op;
use risingwave_common::row::{RowDeserializer, RowExt};
use risingwave_common::util::epoch::EpochPair;
use risingwave_common::util::sort_util::ColumnOrder;

use super::top_n_cache::{AppendOnlyTopNCacheTrait, TopNStaging};
use super::utils::*;
use super::{ManagedTopNState, TopNCache};
use crate::executor::prelude::*;

/// If the input is append-only, `AppendOnlyGroupTopNExecutor` does not need
/// to keep all the rows seen. As long as a record
/// is no longer in the result set, it can be deleted.
///
/// TODO: Optimization: primary key may contain several columns and is used to determine
/// the order, therefore the value part should not contain the same columns to save space.
pub type AppendOnlyTopNExecutor<S, const WITH_TIES: bool> =
    TopNExecutorWrapper<InnerAppendOnlyTopNExecutor<S, WITH_TIES>>;

impl<S: StateStore, const WITH_TIES: bool> AppendOnlyTopNExecutor<S, WITH_TIES> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Executor,
        ctx: ActorContextRef,
        schema: Schema,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        state_table: StateTable<S>,
    ) -> StreamResult<Self> {
        Ok(TopNExecutorWrapper {
            input,
            ctx,
            inner: InnerAppendOnlyTopNExecutor::new(
                schema,
                storage_key,
                offset_and_limit,
                order_by,
                state_table,
            )?,
        })
    }
}

pub struct InnerAppendOnlyTopNExecutor<S: StateStore, const WITH_TIES: bool> {
    schema: Schema,

    /// The storage key indices of the `TopNExecutor`
    storage_key_indices: PkIndices,

    /// We are interested in which element is in the range of [offset, offset+limit).
    managed_state: ManagedTopNState<S>,

    /// In-memory cache of top (N + N * `TOPN_CACHE_HIGH_CAPACITY_FACTOR`) rows
    /// TODO: support WITH TIES
    cache: TopNCache<WITH_TIES>,

    /// Used for serializing pk into `CacheKey`.
    cache_key_serde: CacheKeySerde,
}

impl<S: StateStore, const WITH_TIES: bool> InnerAppendOnlyTopNExecutor<S, WITH_TIES> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: Schema,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        state_table: StateTable<S>,
    ) -> StreamResult<Self> {
        let num_offset = offset_and_limit.0;
        let num_limit = offset_and_limit.1;

        let cache_key_serde = create_cache_key_serde(&storage_key, &schema, &order_by, &[]);
        let managed_state = ManagedTopNState::<S>::new(state_table, cache_key_serde.clone());
        let data_types = schema.data_types();

        Ok(Self {
            schema,
            managed_state,
            storage_key_indices: storage_key.into_iter().map(|op| op.column_index).collect(),
            cache: TopNCache::new(num_offset, num_limit, data_types),
            cache_key_serde,
        })
    }
}

impl<S: StateStore, const WITH_TIES: bool> TopNExecutorBase
    for InnerAppendOnlyTopNExecutor<S, WITH_TIES>
where
    TopNCache<WITH_TIES>: AppendOnlyTopNCacheTrait,
{
    async fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let mut staging = TopNStaging::new();
        let data_types = self.schema.data_types();
        let deserializer = RowDeserializer::new(data_types.clone());
        // apply the chunk to state table
        for (op, row_ref) in chunk.rows() {
            debug_assert_eq!(op, Op::Insert);
            let pk_row = row_ref.project(&self.storage_key_indices);
            let cache_key = serialize_pk_to_cache_key(pk_row, &self.cache_key_serde);
            self.cache.insert(
                cache_key,
                row_ref,
                &mut staging,
                &mut self.managed_state,
                &deserializer,
            )?;
        }

        if staging.is_empty() {
            return Ok(None);
        }
        let mut chunk_builder = StreamChunkBuilder::unlimited(data_types, Some(staging.len()));
        for res in staging.into_deserialized_changes(&deserializer) {
            let (op, row) = res?;
            let _none = chunk_builder.append_row(op, row);
        }
        Ok(chunk_builder.take())
    }

    async fn flush_data(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.flush(epoch).await
    }

    async fn try_flush_data(&mut self) -> StreamExecutorResult<()> {
        self.managed_state.try_flush().await
    }

    async fn init(&mut self, epoch: EpochPair) -> StreamExecutorResult<()> {
        self.managed_state.init_epoch(epoch).await?;
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

    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_common::util::sort_util::{ColumnOrder, OrderType};

    use super::AppendOnlyTopNExecutor;
    use crate::executor::test_utils::top_n_executor::create_in_memory_state_table;
    use crate::executor::test_utils::{MockSource, StreamExecutorTestExt};
    use crate::executor::{ActorContext, Barrier, Execute, Executor, Message, PkIndices};

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
            ColumnOrder::new(0, OrderType::ascending()),
            ColumnOrder::new(1, OrderType::ascending()),
        ]
    }

    fn pk_indices() -> PkIndices {
        vec![0, 1]
    }

    fn create_source() -> Executor {
        let mut chunks = create_stream_chunks();
        MockSource::with_messages(vec![
            Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
            Message::Chunk(std::mem::take(&mut chunks[0])),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            Message::Chunk(std::mem::take(&mut chunks[1])),
            Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
            Message::Chunk(std::mem::take(&mut chunks[2])),
        ])
        .into_executor(create_schema(), pk_indices())
    }

    #[tokio::test]
    async fn test_append_only_top_n_executor_with_limit() {
        let storage_key = storage_key();
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64],
            &[OrderType::ascending(), OrderType::ascending()],
            &pk_indices(),
        )
        .await;

        let schema = source.schema().clone();
        let top_n = AppendOnlyTopNExecutor::<_, false>::new(
            source,
            ActorContext::for_test(0),
            schema,
            storage_key,
            (0, 5),
            order_by(),
            state_table,
        )
        .unwrap();
        let mut top_n = top_n.boxed().execute();

        // consume the init epoch
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                "  I I
                +  1 0
                +  2 1
                +  3 2
                +  9 4
                +  8 5"
            )
            .sort_rows(),
        );
        // We added (1, 2, 3, 10, 9, 8).
        // Now (1, 2, 3, 8, 9)
        // Barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                " I I
                - 9 4
                - 8 5
                + 3 7
                + 1 8"
            )
            .sort_rows(),
        );
        // We added (7, 3, 1, 9).
        // Now (1, 1, 2, 3, 3)
        // Barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                " I  I
                - 3  7
                + 1 12
                - 3  2
                + 1 13"
            )
            .sort_rows(),
        );
        // We added (1, 1, 2, 3).
        // Now (1, 1, 1, 1, 2)
    }

    #[tokio::test]
    async fn test_append_only_top_n_executor_with_offset_and_limit() {
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64],
            &[OrderType::ascending(), OrderType::ascending()],
            &pk_indices(),
        )
        .await;

        let schema = source.schema().clone();
        let top_n = AppendOnlyTopNExecutor::<_, false>::new(
            source,
            ActorContext::for_test(0),
            schema,
            storage_key(),
            (3, 4),
            order_by(),
            state_table,
        )
        .unwrap();
        let mut top_n = top_n.boxed().execute();

        // consume the init epoch
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                "  I I
                + 10 3
                +  9 4
                +  8 5"
            )
            .sort_rows(),
        );
        // We added (1, 2, 3, 10, 9, 8).
        // Now (1, 2, 3) -> (8, 9, 10)
        // barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                "  I I
                +  7 6
                - 10 3
                +  3 7
                -  9 4
                +  3 2"
            )
            .sort_rows(),
        );
        // We added (7, 3, 1, 9).
        // Now (1, 1, 2) -> (3, 3, 7, 8)
        // barrier
        top_n.expect_barrier().await;
        assert_eq!(
            top_n.expect_chunk().await.sort_rows(),
            StreamChunk::from_pretty(
                " I  I
                - 8  5
                + 2  1
                - 7  6
                + 1 13
                - 3  7
                + 2 14"
            )
            .sort_rows(),
        );
        // We added (1, 1, 2, 3).
        // Now (1, 1, 1) -> (1, 2, 2, 3)
    }
}
