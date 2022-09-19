// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use async_trait::async_trait;
use risingwave_common::array::{Op, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::util::ordered::{OrderedRow, OrderedRowDeserializer};
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_storage::table::streaming_table::state_table::StateTable;
use risingwave_storage::StateStore;

use super::error::StreamExecutorResult;
use super::managed_state::top_n::ManagedTopNState;
use super::top_n::{generate_executor_pk_indices_info, TopNCache, TopNCacheWithoutTies};
use super::top_n_executor::{generate_output, TopNExecutorBase, TopNExecutorWrapper};
use super::{Executor, ExecutorInfo, PkIndices, PkIndicesRef};
use crate::error::StreamResult;

/// If the input contains only append, `AppendOnlyTopNExecutor` does not need
/// to keep all the data records/rows that have been seen. As long as a record
/// is no longer being in the result set, it can be deleted.
/// TODO: Optimization: primary key may contain several columns and is used to determine
/// the order, therefore the value part should not contain the same columns to save space.
pub type AppendOnlyTopNExecutor<S> = TopNExecutorWrapper<InnerAppendOnlyTopNExecutor<S>>;

impl<S: StateStore> AppendOnlyTopNExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn Executor>,
        order_pairs: Vec<OrderPair>,
        offset_and_limit: (usize, usize),
        pk_indices: PkIndices,
        executor_id: u64,
        key_indices: Vec<usize>,
        state_table: StateTable<S>,
    ) -> StreamResult<Self> {
        let info = input.info();
        let schema = input.schema().clone();

        Ok(TopNExecutorWrapper {
            input,
            inner: InnerAppendOnlyTopNExecutor::new(
                info,
                schema,
                order_pairs,
                offset_and_limit,
                pk_indices,
                executor_id,
                key_indices,
                state_table,
            )?,
        })
    }
}

pub struct InnerAppendOnlyTopNExecutor<S: StateStore> {
    info: ExecutorInfo,

    /// Schema of the executor.
    schema: Schema,

    /// The primary key indices of the `AppendOnlyTopNExecutor`
    pk_indices: PkIndices,

    /// The internal key indices of the `TopNExecutor`
    internal_key_indices: PkIndices,

    /// The order of internal keys of the `TopNExecutor`
    internal_key_order_types: Vec<OrderType>,

    /// We are interested in which element is in the range of [offset, offset+limit).
    managed_state: ManagedTopNState<S>,

    /// In-memory cache of top (N + N * `TOPN_CACHE_HIGH_CAPACITY_FACTOR`) rows
    cache: TopNCacheWithoutTies,

    #[expect(dead_code)]
    /// Indices of the columns on which key distribution depends.
    key_indices: Vec<usize>,
}

impl<S: StateStore> InnerAppendOnlyTopNExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_info: ExecutorInfo,
        schema: Schema,
        order_pairs: Vec<OrderPair>,
        offset_and_limit: (usize, usize),
        pk_indices: PkIndices,
        executor_id: u64,
        key_indices: Vec<usize>,
        state_table: StateTable<S>,
    ) -> StreamResult<Self> {
        let (internal_key_indices, internal_key_data_types, internal_key_order_types) =
            generate_executor_pk_indices_info(&order_pairs, &pk_indices, &schema);

        let ordered_row_deserializer =
            OrderedRowDeserializer::new(internal_key_data_types, internal_key_order_types.clone());

        let num_offset = offset_and_limit.0;
        let num_limit = offset_and_limit.1;
        let managed_state = ManagedTopNState::<S>::new(state_table, ordered_row_deserializer);

        Ok(Self {
            info: ExecutorInfo {
                schema: input_info.schema,
                pk_indices: input_info.pk_indices,
                identity: format!("AppendOnlyTopNExecutor {:X}", executor_id),
            },
            schema,
            managed_state,
            pk_indices,
            internal_key_indices,
            internal_key_order_types,
            cache: TopNCacheWithoutTies::new(num_offset, num_limit),
            key_indices,
        })
    }
}

#[async_trait]
impl<S: StateStore> TopNExecutorBase for InnerAppendOnlyTopNExecutor<S> {
    async fn apply_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<StreamChunk> {
        let mut res_ops = Vec::with_capacity(self.cache.limit);
        let mut res_rows = Vec::with_capacity(self.cache.limit);

        // apply the chunk to state table
        for (op, row_ref) in chunk.rows() {
            debug_assert_eq!(op, Op::Insert);
            let pk_row = row_ref.row_by_indices(&self.internal_key_indices);
            let ordered_pk_row = OrderedRow::new(pk_row, &self.internal_key_order_types);
            let row = row_ref.to_owned_row();

            if self.cache.is_middle_cache_full()
                && ordered_pk_row >= *self.cache.middle.last_key_value().unwrap().0
            {
                continue;
            }
            self.managed_state
                .insert(ordered_pk_row.clone(), row.clone());

            // Then insert input row to corresponding cache range according to its order key
            if !self.cache.is_low_cache_full() {
                self.cache.low.insert(ordered_pk_row, row);
                continue;
            }

            let elem_to_insert_into_middle =
            if let Some(low_last) = self.cache.low.last_entry()
                && ordered_pk_row <= *low_last.key() {
                // Take the last element of `cache.low` and insert input row to it.
                let low_last = low_last.remove_entry();
                self.cache.low.insert(ordered_pk_row, row);
                low_last
            } else {
                (ordered_pk_row, row)
            };

            if !self.cache.is_middle_cache_full() {
                self.cache.middle.insert(
                    elem_to_insert_into_middle.0,
                    elem_to_insert_into_middle.1.clone(),
                );
                res_ops.push(Op::Insert);
                res_rows.push(elem_to_insert_into_middle.1);
                continue;
            }

            // The row must be in the range of [offset, offset+limit).
            // the largest row in `cache.middle` needs to be removed.
            let middle_last = self.cache.middle.pop_last().unwrap();
            debug_assert!(elem_to_insert_into_middle.0 < middle_last.0);

            res_ops.push(Op::Delete);
            res_rows.push(middle_last.1.clone());
            self.managed_state.delete(&middle_last.0, middle_last.1);

            res_ops.push(Op::Insert);
            res_rows.push(elem_to_insert_into_middle.1.clone());
            self.cache
                .middle
                .insert(elem_to_insert_into_middle.0, elem_to_insert_into_middle.1);

            // Unlike normal topN, append only topN does not use the high part of the cache.
        }

        generate_output(res_rows, res_ops, &self.schema)
    }

    async fn flush_data(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        self.managed_state.flush(epoch).await
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }

    async fn init(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        self.managed_state.state_table.init_epoch(epoch);
        self.managed_state
            .init_topn_cache(None, &mut self.cache)
            .await
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
    use risingwave_common::util::sort_util::{OrderPair, OrderType};

    use crate::executor::test_utils::top_n_executor::create_in_memory_state_table;
    use crate::executor::test_utils::MockSource;
    use crate::executor::top_n_appendonly::AppendOnlyTopNExecutor;
    use crate::executor::{Barrier, Epoch, Executor, Message, PkIndices};

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

    fn create_order_pairs() -> Vec<OrderPair> {
        vec![
            OrderPair::new(0, OrderType::Ascending),
            OrderPair::new(1, OrderType::Ascending),
        ]
    }

    fn create_source() -> Box<MockSource> {
        let mut chunks = create_stream_chunks();
        let schema = create_schema();
        Box::new(MockSource::with_messages(
            schema,
            PkIndices::new(),
            vec![
                Message::Barrier(Barrier {
                    epoch: Epoch::new_test_epoch(1),
                    ..Barrier::default()
                }),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Barrier(Barrier {
                    epoch: Epoch::new_test_epoch(2),
                    ..Barrier::default()
                }),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Barrier(Barrier {
                    epoch: Epoch::new_test_epoch(3),
                    ..Barrier::default()
                }),
                Message::Chunk(std::mem::take(&mut chunks[2])),
            ],
        ))
    }

    #[tokio::test]
    async fn test_append_only_top_n_executor_with_limit() {
        let order_pairs = create_order_pairs();
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64],
            &[OrderType::Ascending, OrderType::Ascending],
            &[0, 1],
        );

        let top_n_executor = Box::new(
            AppendOnlyTopNExecutor::new(
                source as Box<dyn Executor>,
                order_pairs,
                (0, 5),
                vec![0, 1],
                1,
                vec![],
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
        let order_pairs = create_order_pairs();
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64],
            &[OrderType::Ascending, OrderType::Ascending],
            &[0, 1],
        );

        let top_n_executor = Box::new(
            AppendOnlyTopNExecutor::new(
                source as Box<dyn Executor>,
                order_pairs,
                (3, 4),
                vec![0, 1],
                1,
                vec![],
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
