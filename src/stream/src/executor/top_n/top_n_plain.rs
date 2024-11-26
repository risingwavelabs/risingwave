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

use super::top_n_cache::TopNStaging;
use super::utils::*;
use super::{ManagedTopNState, TopNCache, TopNCacheTrait};
use crate::executor::prelude::*;

/// `TopNExecutor` works with input with modification, it keeps all the data
/// records/rows that have been seen, and returns topN records overall.
pub type TopNExecutor<S, const WITH_TIES: bool> =
    TopNExecutorWrapper<InnerTopNExecutor<S, WITH_TIES>>;

impl<S: StateStore, const WITH_TIES: bool> TopNExecutor<S, WITH_TIES> {
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
            inner: InnerTopNExecutor::new(
                schema,
                storage_key,
                offset_and_limit,
                order_by,
                state_table,
            )?,
        })
    }
}

impl<S: StateStore> TopNExecutor<S, true> {
    /// It only has 1 capacity for high cache. Used to test the case where the last element in high
    /// has ties.
    #[allow(clippy::too_many_arguments)]
    #[cfg(test)]
    pub fn new_with_ties_for_test(
        input: Executor,
        ctx: ActorContextRef,
        schema: Schema,
        storage_key: Vec<ColumnOrder>,
        offset_and_limit: (usize, usize),
        order_by: Vec<ColumnOrder>,
        state_table: StateTable<S>,
    ) -> StreamResult<Self> {
        let mut inner =
            InnerTopNExecutor::new(schema, storage_key, offset_and_limit, order_by, state_table)?;

        inner.cache.high_cache_capacity = 2;

        Ok(TopNExecutorWrapper { input, ctx, inner })
    }
}

pub struct InnerTopNExecutor<S: StateStore, const WITH_TIES: bool> {
    schema: Schema,

    /// The storage key indices of the `TopNExecutor`
    storage_key_indices: PkIndices,

    managed_state: ManagedTopNState<S>,

    /// In-memory cache of top (N + N * `TOPN_CACHE_HIGH_CAPACITY_FACTOR`) rows
    cache: TopNCache<WITH_TIES>,

    /// Used for serializing pk into `CacheKey`.
    cache_key_serde: CacheKeySerde,
}

impl<S: StateStore, const WITH_TIES: bool> InnerTopNExecutor<S, WITH_TIES> {
    /// # Arguments
    ///
    /// `storage_key` -- the storage pk. It's composed of the ORDER BY columns and the missing
    /// columns of pk.
    ///
    /// `order_by_len` -- The number of fields of the ORDER BY clause, and will be used to split key
    /// into `CacheKey`.
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

impl<S: StateStore, const WITH_TIES: bool> TopNExecutorBase for InnerTopNExecutor<S, WITH_TIES>
where
    TopNCache<WITH_TIES>: TopNCacheTrait,
{
    async fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let mut staging = TopNStaging::new();

        // apply the chunk to state table
        for (op, row_ref) in chunk.rows() {
            let pk_row = row_ref.project(&self.storage_key_indices);
            let cache_key = serialize_pk_to_cache_key(pk_row, &self.cache_key_serde);
            match op {
                Op::Insert | Op::UpdateInsert => {
                    // First insert input row to state store
                    self.managed_state.insert(row_ref);
                    self.cache.insert(cache_key, row_ref, &mut staging)
                }

                Op::Delete | Op::UpdateDelete => {
                    // First remove the row from state store
                    self.managed_state.delete(row_ref);
                    self.cache
                        .delete(
                            NO_GROUP_KEY,
                            &mut self.managed_state,
                            cache_key,
                            row_ref,
                            &mut staging,
                        )
                        .await?
                }
            }
        }

        let data_types = self.schema.data_types();
        let deserializer = RowDeserializer::new(data_types.clone());
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
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;

    use super::*;
    use crate::executor::test_utils::top_n_executor::create_in_memory_state_table;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{Barrier, Message};

    mod test1 {

        use risingwave_common::util::epoch::test_epoch;

        use super::*;
        use crate::executor::test_utils::StreamExecutorTestExt;

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
                -  3 2
                -  1 0
                +  5 7
                -  2 1
                + 11 8",
            );
            let chunk3 = StreamChunk::from_pretty(
                "  I  I
                +  6  9
                + 12 10
                + 13 11
                + 14 12",
            );
            let chunk4 = StreamChunk::from_pretty(
                "  I  I
                -  5  7
                -  6  9
                - 11  8",
            );
            vec![chunk1, chunk2, chunk3, chunk4]
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
            let mut v = order_by();
            v.extend([ColumnOrder::new(1, OrderType::ascending())]);
            v
        }

        fn order_by() -> Vec<ColumnOrder> {
            vec![ColumnOrder::new(0, OrderType::ascending())]
        }

        fn pk_indices() -> PkIndices {
            vec![0, 1]
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
        async fn test_top_n_executor_with_offset() {
            let source = create_source();
            let state_table = create_in_memory_state_table(
                &[DataType::Int64, DataType::Int64],
                &[OrderType::ascending(), OrderType::ascending()],
                &pk_indices(),
            )
            .await;

            let schema = source.schema().clone();
            let top_n = TopNExecutor::<_, false>::new(
                source,
                ActorContext::for_test(0),
                schema,
                storage_key(),
                (3, 1000),
                order_by(),
                state_table,
            )
            .unwrap();
            let mut top_n = top_n.boxed().execute();

            // consume the init barrier
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
            // Barrier
            top_n.expect_barrier().await;
            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I
                    -  8 5
                    + 11 8"
                )
                .sort_rows(),
            );

            // barrier
            top_n.expect_barrier().await;

            // (8, 9, 10, 11, 12, 13, 14)
            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I  I
                    +  8  5
                    + 12 10
                    + 13 11
                    + 14 12"
                )
                .sort_rows(),
            );
            // barrier
            top_n.expect_barrier().await;

            // (10, 12, 13, 14)
            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I
                    -  8 5
                    -  9 4
                    - 11 8"
                )
                .sort_rows(),
            );
            // barrier
            top_n.expect_barrier().await;
        }

        #[tokio::test]
        async fn test_top_n_executor_with_limit() {
            let source = create_source();
            let state_table = create_in_memory_state_table(
                &[DataType::Int64, DataType::Int64],
                &[OrderType::ascending(), OrderType::ascending()],
                &pk_indices(),
            )
            .await;
            let schema = source.schema().clone();
            let top_n = TopNExecutor::<_, false>::new(
                source,
                ActorContext::for_test(0),
                schema,
                storage_key(),
                (0, 4),
                order_by(),
                state_table,
            )
            .unwrap();
            let mut top_n = top_n.boxed().execute();

            // consume the init barrier
            top_n.expect_barrier().await;
            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I
                    +  1 0
                    +  2 1
                    +  3 2
                    +  8 5"
                )
                .sort_rows(),
            );
            // now () -> (1, 2, 3, 8)

            // barrier
            top_n.expect_barrier().await;
            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I
                    +  7 6
                    -  3 2
                    -  1 0
                    +  5 7
                    -  2 1
                    +  9 4"
                )
                .sort_rows(),
            );

            // (5, 7, 8, 9)
            // barrier
            top_n.expect_barrier().await;

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I
                    -  9 4
                    +  6 9"
                )
                .sort_rows(),
            );
            // (5, 6, 7, 8)
            // barrier
            top_n.expect_barrier().await;

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I
                    -  5 7
                    +  9 4
                    -  6 9
                    + 10 3"
                )
                .sort_rows(),
            );
            // (7, 8, 9, 10)
            // barrier
            top_n.expect_barrier().await;
        }

        // Should have the same result as above, since there are no duplicate sort keys.
        #[tokio::test]
        async fn test_top_n_executor_with_limit_with_ties() {
            let source = create_source();
            let state_table = create_in_memory_state_table(
                &[DataType::Int64, DataType::Int64],
                &[OrderType::ascending(), OrderType::ascending()],
                &pk_indices(),
            )
            .await;
            let schema = source.schema().clone();
            let top_n = TopNExecutor::<_, true>::new(
                source,
                ActorContext::for_test(0),
                schema,
                storage_key(),
                (0, 4),
                order_by(),
                state_table,
            )
            .unwrap();
            let mut top_n = top_n.boxed().execute();

            // consume the init barrier
            top_n.expect_barrier().await;
            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I
                    +  1 0
                    +  2 1
                    +  3 2
                    +  8 5"
                )
                .sort_rows(),
            );
            // now () -> (1, 2, 3, 8)

            // barrier
            top_n.expect_barrier().await;
            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    " I I
                    + 7 6
                    - 3 2
                    - 1 0
                    + 5 7
                    - 2 1
                    + 9 4"
                )
                .sort_rows(),
            );

            // (5, 7, 8, 9)
            // barrier
            top_n.expect_barrier().await;

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I
                    -  9 4
                    +  6 9"
                )
                .sort_rows(),
            );
            // (5, 6, 7, 8)
            // barrier
            top_n.expect_barrier().await;

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I
                    -  5 7
                    +  9 4
                    -  6 9
                    + 10 3"
                )
                .sort_rows(),
            );
            // (7, 8, 9, 10)
            // barrier
            top_n.expect_barrier().await;
        }

        #[tokio::test]
        async fn test_top_n_executor_with_offset_and_limit() {
            let source = create_source();
            let state_table = create_in_memory_state_table(
                &[DataType::Int64, DataType::Int64],
                &[OrderType::ascending(), OrderType::ascending()],
                &pk_indices(),
            )
            .await;
            let schema = source.schema().clone();
            let top_n = TopNExecutor::<_, false>::new(
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

            // consume the init barrier
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
            // barrier
            top_n.expect_barrier().await;
            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I
                    -  8 5
                    + 11 8"
                )
                .sort_rows(),
            );
            // barrier
            top_n.expect_barrier().await;

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I
                    +  8 5"
                )
                .sort_rows(),
            );
            // barrier
            top_n.expect_barrier().await;
            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I  I
                    -  8  5
                    + 12 10
                    -  9  4
                    + 13 11
                    - 11  8
                    + 14 12"
                )
                .sort_rows(),
            );
            // barrier
            top_n.expect_barrier().await;
        }
    }

    mod test2 {

        use risingwave_common::util::epoch::test_epoch;
        use risingwave_storage::memory::MemoryStateStore;

        use super::*;
        use crate::executor::test_utils::top_n_executor::create_in_memory_state_table_from_state_store;
        use crate::executor::test_utils::StreamExecutorTestExt;
        fn create_source_new() -> Executor {
            let mut chunks = vec![
                StreamChunk::from_pretty(
                    " I I I I
                +  1 1 4 1001",
                ),
                StreamChunk::from_pretty(
                    " I I I I
                +  5 1 4 1002 ",
                ),
                StreamChunk::from_pretty(
                    " I I I I
                +  1 9 1 1003
                +  9 8 1 1004
                +  0 2 3 1005",
                ),
                StreamChunk::from_pretty(
                    " I I I I
                +  1 0 2 1006",
                ),
            ];
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                ],
            };
            MockSource::with_messages(vec![
                Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Chunk(std::mem::take(&mut chunks[2])),
                Message::Chunk(std::mem::take(&mut chunks[3])),
                Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            ])
            .into_executor(schema, pk_indices())
        }

        fn create_source_new_before_recovery() -> Executor {
            let mut chunks = [
                StreamChunk::from_pretty(
                    " I I I I
                +  1 1 4 1001",
                ),
                StreamChunk::from_pretty(
                    " I I I I
                +  5 1 4 1002 ",
                ),
            ];
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                ],
            };
            MockSource::with_messages(vec![
                Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            ])
            .into_executor(schema, pk_indices())
        }

        fn create_source_new_after_recovery() -> Executor {
            let mut chunks = [
                StreamChunk::from_pretty(
                    " I I I I
                +  1 9 1 1003
                +  9 8 1 1004
                +  0 2 3 1005",
                ),
                StreamChunk::from_pretty(
                    " I I I I
                +  1 0 2 1006",
                ),
            ];
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                ],
            };
            MockSource::with_messages(vec![
                Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
            ])
            .into_executor(schema, pk_indices())
        }

        fn storage_key() -> Vec<ColumnOrder> {
            order_by()
        }

        fn order_by() -> Vec<ColumnOrder> {
            vec![
                ColumnOrder::new(0, OrderType::ascending()),
                ColumnOrder::new(3, OrderType::ascending()),
            ]
        }

        fn pk_indices() -> PkIndices {
            vec![0, 3]
        }

        #[tokio::test]
        async fn test_top_n_executor_with_offset_and_limit_new() {
            let source = create_source_new();
            let state_table = create_in_memory_state_table(
                &[
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ],
                &[OrderType::ascending(), OrderType::ascending()],
                &pk_indices(),
            )
            .await;
            let schema = source.schema().clone();
            let top_n = TopNExecutor::<_, false>::new(
                source,
                ActorContext::for_test(0),
                schema,
                storage_key(),
                (1, 3),
                order_by(),
                state_table,
            )
            .unwrap();
            let mut top_n = top_n.boxed().execute();

            // consume the init barrier
            top_n.expect_barrier().await;

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I I I
                    +  5 1 4 1002"
                )
                .sort_rows(),
            );

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I I I
                    +  1 9 1 1003
                    +  1 1 4 1001",
                )
                .sort_rows(),
            );

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I I I
                    -  5 1 4 1002
                    +  1 0 2 1006",
                )
                .sort_rows(),
            );

            // barrier
            top_n.expect_barrier().await;
        }

        #[tokio::test]
        async fn test_top_n_executor_with_offset_and_limit_new_after_recovery() {
            let state_store = MemoryStateStore::new();
            let state_table = create_in_memory_state_table_from_state_store(
                &[
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ],
                &[OrderType::ascending(), OrderType::ascending()],
                &pk_indices(),
                state_store.clone(),
            )
            .await;
            let source = create_source_new_before_recovery();
            let schema = source.schema().clone();
            let top_n = TopNExecutor::<_, false>::new(
                source,
                ActorContext::for_test(0),
                schema,
                storage_key(),
                (1, 3),
                order_by(),
                state_table,
            )
            .unwrap();
            let mut top_n = top_n.boxed().execute();

            // consume the init barrier
            top_n.expect_barrier().await;

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I I I
                    +  5 1 4 1002"
                )
                .sort_rows(),
            );

            // barrier
            top_n.expect_barrier().await;

            let state_table = create_in_memory_state_table_from_state_store(
                &[
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ],
                &[OrderType::ascending(), OrderType::ascending()],
                &pk_indices(),
                state_store,
            )
            .await;

            // recovery
            let source = create_source_new_after_recovery();
            let schema = source.schema().clone();
            let top_n_after_recovery = TopNExecutor::<_, false>::new(
                source,
                ActorContext::for_test(0),
                schema,
                storage_key(),
                (1, 3),
                order_by(),
                state_table,
            )
            .unwrap();
            let mut top_n = top_n_after_recovery.boxed().execute();

            // barrier
            top_n.expect_barrier().await;

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I I I
                    +  1 9 1 1003
                    +  1 1 4 1001",
                )
                .sort_rows(),
            );

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    "  I I I I
                    -  5 1 4 1002
                    +  1 0 2 1006",
                )
                .sort_rows(),
            );

            // barrier
            top_n.expect_barrier().await;
        }
    }

    mod test_with_ties {

        use risingwave_common::util::epoch::test_epoch;
        use risingwave_storage::memory::MemoryStateStore;

        use super::*;
        use crate::executor::test_utils::top_n_executor::create_in_memory_state_table_from_state_store;
        use crate::executor::test_utils::StreamExecutorTestExt;

        fn create_source() -> Executor {
            let mut chunks = vec![
                StreamChunk::from_pretty(
                    "  I I
                    +  1 0
                    +  2 1
                    +  3 2
                    + 10 3
                    +  9 4
                    +  8 5
                    ",
                ),
                StreamChunk::from_pretty(
                    "  I I
                    +  3 6
                    +  3 7
                    +  1 8
                    +  2 9
                    + 10 10",
                ),
                StreamChunk::from_pretty(
                    " I I
                    - 1 0",
                ),
                StreamChunk::from_pretty(
                    " I I
                    - 1 8",
                ),
            ];
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                ],
            };
            MockSource::with_messages(vec![
                Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Chunk(std::mem::take(&mut chunks[2])),
                Message::Chunk(std::mem::take(&mut chunks[3])),
                Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            ])
            .into_executor(schema, pk_indices())
        }

        fn storage_key() -> Vec<ColumnOrder> {
            let mut v = order_by();
            v.push(ColumnOrder::new(1, OrderType::ascending()));
            v
        }

        fn order_by() -> Vec<ColumnOrder> {
            vec![ColumnOrder::new(0, OrderType::ascending())]
        }

        fn pk_indices() -> PkIndices {
            vec![0, 1]
        }

        #[tokio::test]
        async fn test_with_ties() {
            let source = create_source();
            let state_table = create_in_memory_state_table(
                &[DataType::Int64, DataType::Int64],
                &[OrderType::ascending(), OrderType::ascending()],
                &pk_indices(),
            )
            .await;
            let schema = source.schema().clone();
            let top_n = TopNExecutor::new_with_ties_for_test(
                source,
                ActorContext::for_test(0),
                schema,
                storage_key(),
                (0, 3),
                order_by(),
                state_table,
            )
            .unwrap();
            let mut top_n = top_n.boxed().execute();

            // consume the init barrier
            top_n.expect_barrier().await;
            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    " I I
                    + 1 0
                    + 2 1
                    + 3 2"
                )
                .sort_rows(),
            );

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    " I I
                    - 3 2
                    + 1 8
                    + 2 9"
                )
                .sort_rows(),
            );

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    " I I
                    - 1 0"
                )
                .sort_rows(),
            );

            // High cache has only 2 capacity, but we need to trigger 3 inserts here!
            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    " I I
                    - 1 8
                    + 3 2
                    + 3 6
                    + 3 7"
                )
                .sort_rows(),
            );

            // barrier
            top_n.expect_barrier().await;
        }

        fn create_source_before_recovery() -> Executor {
            let mut chunks = [
                StreamChunk::from_pretty(
                    "  I I
                    +  1 0
                    +  2 1
                    +  3 2
                    + 10 3
                    +  9 4
                    +  8 5",
                ),
                StreamChunk::from_pretty(
                    "  I I
                    +  3 6
                    +  3 7
                    +  1 8
                    +  2 9
                    + 10 10",
                ),
            ];
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                ],
            };
            MockSource::with_messages(vec![
                Message::Barrier(Barrier::new_test_barrier(test_epoch(1))),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Barrier(Barrier::new_test_barrier(test_epoch(2))),
            ])
            .into_executor(schema, pk_indices())
        }

        fn create_source_after_recovery() -> Executor {
            let mut chunks = [
                StreamChunk::from_pretty(
                    " I I
                    - 1 0",
                ),
                StreamChunk::from_pretty(
                    " I I
                    - 1 8",
                ),
            ];
            let schema = Schema {
                fields: vec![
                    Field::unnamed(DataType::Int64),
                    Field::unnamed(DataType::Int64),
                ],
            };
            MockSource::with_messages(vec![
                Message::Barrier(Barrier::new_test_barrier(test_epoch(3))),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Barrier(Barrier::new_test_barrier(test_epoch(4))),
            ])
            .into_executor(schema, pk_indices())
        }

        #[tokio::test]
        async fn test_with_ties_recovery() {
            let state_store = MemoryStateStore::new();
            let state_table = create_in_memory_state_table_from_state_store(
                &[DataType::Int64, DataType::Int64],
                &[OrderType::ascending(), OrderType::ascending()],
                &pk_indices(),
                state_store.clone(),
            )
            .await;
            let source = create_source_before_recovery();
            let schema = source.schema().clone();
            let top_n = TopNExecutor::new_with_ties_for_test(
                source,
                ActorContext::for_test(0),
                schema,
                storage_key(),
                (0, 3),
                order_by(),
                state_table,
            )
            .unwrap();
            let mut top_n = top_n.boxed().execute();

            // consume the init barrier
            top_n.expect_barrier().await;
            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    " I I
                    + 1 0
                    + 2 1
                    + 3 2"
                )
                .sort_rows(),
            );

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    " I I
                    - 3 2
                    + 1 8
                    + 2 9"
                )
                .sort_rows(),
            );

            // barrier
            top_n.expect_barrier().await;

            let state_table = create_in_memory_state_table_from_state_store(
                &[DataType::Int64, DataType::Int64],
                &[OrderType::ascending(), OrderType::ascending()],
                &pk_indices(),
                state_store,
            )
            .await;

            // recovery
            let source = create_source_after_recovery();
            let schema = source.schema().clone();
            let top_n_after_recovery = TopNExecutor::new_with_ties_for_test(
                source,
                ActorContext::for_test(0),
                schema,
                storage_key(),
                (0, 3),
                order_by(),
                state_table,
            )
            .unwrap();
            let mut top_n = top_n_after_recovery.boxed().execute();

            // barrier
            top_n.expect_barrier().await;

            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    " I I
                    - 1 0"
                )
                .sort_rows(),
            );

            // High cache has only 2 capacity, but we need to trigger 3 inserts here!
            assert_eq!(
                top_n.expect_chunk().await.sort_rows(),
                StreamChunk::from_pretty(
                    " I I
                    - 1 8
                    + 3 2
                    + 3 6
                    + 3 7"
                )
                .sort_rows(),
            );
            // barrier
            top_n.expect_barrier().await;
        }
    }
}
