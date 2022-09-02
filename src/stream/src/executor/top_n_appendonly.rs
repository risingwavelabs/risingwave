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
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::util::ordered::{OrderedRow, OrderedRowDeserializer};
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_storage::StateStore;

use super::error::StreamExecutorResult;
use super::managed_state::top_n::variants::TOP_N_MAX;
use super::managed_state::top_n::ManagedTopNState;
use super::top_n_executor::{generate_output, TopNExecutorBase, TopNExecutorWrapper};
use super::{Executor, ExecutorInfo, PkIndices, PkIndicesRef};
use crate::executor::top_n::generate_executor_pk_indices_info;

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
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        store: S,
        table_id_l: TableId,
        table_id_h: TableId,

        cache_size: Option<usize>,
        total_count: (usize, usize),
        executor_id: u64,
        key_indices: Vec<usize>,
    ) -> StreamExecutorResult<Self> {
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
                store,
                table_id_l,
                table_id_h,
                cache_size,
                total_count,
                executor_id,
                key_indices,
            )?,
        })
    }
}

pub struct InnerAppendOnlyTopNExecutor<S: StateStore> {
    info: ExecutorInfo,

    /// Schema of the executor.
    schema: Schema,

    /// `LIMIT XXX`. `None` means no limit.
    limit: Option<usize>,

    /// `OFFSET XXX`. `0` means no offset.
    offset: usize,

    /// The primary key indices of the `AppendOnlyTopNExecutor`
    pk_indices: PkIndices,

    /// The internal key indices of the `TopNExecutor`
    internal_key_indices: PkIndices,

    /// The order of internal keys of the `TopNExecutor`
    internal_key_order_types: Vec<OrderType>,

    /// We are only interested in which element is in the range of `[offset, offset+limit)`(right
    /// open interval) but not the rank of such element
    ///
    /// We keep two ordered sets. One set stores the elements in the range of `[0, offset)`, and
    /// another set stores the elements in the range of `[offset, offset+limit)`.
    managed_lower_state: ManagedTopNState<S, TOP_N_MAX>,
    managed_higher_state: ManagedTopNState<S, TOP_N_MAX>,

    /// Marks whether this is first-time execution. If yes, we need to fill in the cache from
    /// storage.
    first_execution: bool,

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
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        store: S,
        table_id_l: TableId,
        table_id_h: TableId,
        cache_size: Option<usize>,
        total_count: (usize, usize),
        executor_id: u64,
        key_indices: Vec<usize>,
    ) -> StreamExecutorResult<Self> {
        let (internal_key_indices, internal_key_data_types, internal_key_order_types) =
            generate_executor_pk_indices_info(&order_pairs, &pk_indices, &schema);

        let row_data_types = schema
            .fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect::<Vec<_>>();
        let ordered_row_deserializer =
            OrderedRowDeserializer::new(internal_key_data_types, internal_key_order_types.clone());
        Ok(Self {
            info: ExecutorInfo {
                schema: input_info.schema,
                pk_indices: input_info.pk_indices,
                identity: format!("AppendOnlyTopNExecutor {:X}", executor_id),
            },
            schema,
            offset: offset_and_limit.0,
            limit: offset_and_limit.1,
            managed_lower_state: ManagedTopNState::<S, TOP_N_MAX>::new(
                cache_size,
                total_count.0,
                store.clone(),
                table_id_l,
                row_data_types.clone(),
                ordered_row_deserializer.clone(),
                internal_key_indices.clone(),
            ),
            managed_higher_state: ManagedTopNState::<S, TOP_N_MAX>::new(
                cache_size,
                total_count.1,
                store,
                table_id_h,
                row_data_types,
                ordered_row_deserializer,
                internal_key_indices.clone(),
            ),
            pk_indices,
            internal_key_indices,
            internal_key_order_types,
            first_execution: true,
            key_indices,
        })
    }
}

#[async_trait]
impl<S: StateStore> TopNExecutorBase for InnerAppendOnlyTopNExecutor<S> {
    async fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
        epoch: u64,
    ) -> StreamExecutorResult<StreamChunk> {
        if self.first_execution {
            self.managed_lower_state.fill_in_cache(epoch).await?;
            self.managed_higher_state.fill_in_cache(epoch).await?;
            self.first_execution = false;
        }

        let num_need_to_keep = self.limit.unwrap_or(usize::MAX);
        let mut new_ops = vec![];
        let mut new_rows = vec![];

        for (op, row_ref) in chunk.rows() {
            assert_eq!(op, Op::Insert);

            let pk_row = row_ref.row_by_indices(&self.internal_key_indices);
            let ordered_pk_row = OrderedRow::new(pk_row, &self.internal_key_order_types);
            let row = row_ref.to_owned_row();

            if self.managed_lower_state.total_count() < self.offset {
                // `elem` is in the range of `[0, offset)`,
                // we ignored it for now as it is not in the result set.
                self.managed_lower_state
                    .insert(ordered_pk_row, row, epoch)?;
                continue;
            }

            // We remark that when offset is 0, every input row has nothing to do with
            // `managed_lower_state`.
            let element_to_compare_with_upper = if self.offset > 0
                && &ordered_pk_row < self.managed_lower_state.top_element().unwrap().0
            {
                // If the new element is smaller than the largest element in [0, offset),
                // the largest element may need to move to [offset, offset+limit).
                let res = self
                    .managed_lower_state
                    .pop_top_element(epoch)
                    .await?
                    .unwrap();
                self.managed_lower_state
                    .insert(ordered_pk_row, row, epoch)?;
                res
            } else {
                (ordered_pk_row, row)
            };

            if self.managed_higher_state.total_count() < num_need_to_keep {
                self.managed_higher_state.insert(
                    element_to_compare_with_upper.0,
                    element_to_compare_with_upper.1.clone(),
                    epoch,
                )?;
                new_ops.push(Op::Insert);
                new_rows.push(element_to_compare_with_upper.1);
            } else if self.managed_higher_state.top_element().unwrap().0
                > &element_to_compare_with_upper.0
            {
                let element_to_pop = self
                    .managed_higher_state
                    .pop_top_element(epoch)
                    .await?
                    .unwrap();
                new_ops.push(Op::Delete);
                new_rows.push(element_to_pop.1);
                new_ops.push(Op::Insert);
                new_rows.push(element_to_compare_with_upper.1.clone());
                self.managed_higher_state.insert(
                    element_to_compare_with_upper.0,
                    element_to_compare_with_upper.1,
                    epoch,
                )?;
            }
            // The "else" case can only be that `element_to_compare_with_upper` is larger than
            // the largest element in [offset, offset+limit), which is already full.
            // Therefore, nothing happens.
        }
        generate_output(new_rows, new_ops, &self.schema)
    }

    async fn flush_data(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        self.managed_higher_state.flush(epoch).await?;
        self.managed_lower_state.flush(epoch).await
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

    async fn init(&mut self, _epoch: u64) -> StreamExecutorResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use assert_matches::assert_matches;
    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::array::StreamChunk;
    use risingwave_common::catalog::{Field, Schema, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_storage::memory::MemoryStateStore;

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
    async fn test_append_only_top_n_executor_with_offset() {
        let order_pairs = create_order_pairs();
        let source = create_source();

        let top_n_executor = Box::new(
            AppendOnlyTopNExecutor::new(
                source as Box<dyn Executor>,
                order_pairs,
                (3, None),
                vec![0, 1],
                MemoryStateStore::new(),
                TableId::from(0x2333),
                TableId::from(0x2334),
                Some(2),
                (0, 0),
                1,
                vec![],
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
                + 7 6
                + 3 7
                + 3 2
                + 9 9"
            )
        );
        // We added (7, 3, 1, 9).
        // Now (1, 1, 2) -> (3, 3, 7, 8, 9, 10)
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
                + 2  1
                + 1 13
                + 2 14
                + 3 15"
            )
        );
        // We added (1, 1, 2, 3).
        // Now (1, 1, 1) -> (1, 2, 2, 3, 3, 3, 7, 8, 9, 10)
    }

    #[tokio::test]
    async fn test_append_only_top_n_executor_with_limit() {
        let order_pairs = create_order_pairs();
        let source = create_source();

        let top_n_executor = Box::new(
            AppendOnlyTopNExecutor::new(
                source as Box<dyn Executor>,
                order_pairs,
                (0, Some(5)),
                vec![0, 1],
                MemoryStateStore::new(),
                TableId::from(0x2333),
                TableId::from(0x2334),
                Some(2),
                (0, 0),
                1,
                vec![],
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

        let top_n_executor = Box::new(
            AppendOnlyTopNExecutor::new(
                source as Box<dyn Executor>,
                order_pairs,
                (3, Some(4)),
                vec![0, 1],
                MemoryStateStore::new(),
                TableId::from(0x2333),
                TableId::from(0x2334),
                Some(2),
                (0, 0),
                1,
                vec![],
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
