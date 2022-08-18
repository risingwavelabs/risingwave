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

use std::collections::BTreeMap;

use async_trait::async_trait;
use risingwave_common::array::{Op, Row, StreamChunk};
use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_common::util::ordered::{OrderedRow, OrderedRowDeserializer};
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_storage::table::state_table::RowBasedStateTable;
use risingwave_storage::StateStore;

use super::error::StreamExecutorResult;
use super::managed_state::top_n::ManagedTopNStateNew;
use super::top_n_executor::{generate_output, TopNExecutorBase, TopNExecutorWrapper};
use super::{Executor, ExecutorInfo, PkIndices, PkIndicesRef};

/// `TopNExecutor` works with input with modification, it keeps all the data
/// records/rows that have been seen, and returns topN records overall.
pub type TopNExecutor<S> = TopNExecutorWrapper<InnerTopNExecutorNew<S>>;

impl<S: StateStore> TopNExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn Executor>,
        order_pairs: Vec<OrderPair>,
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        total_count: usize,
        executor_id: u64,
        key_indices: Vec<usize>,
        state_table: RowBasedStateTable<S>,
    ) -> StreamExecutorResult<Self> {
        let info = input.info();
        let schema = input.schema().clone();

        Ok(TopNExecutorWrapper {
            input,
            inner: InnerTopNExecutorNew::new(
                info,
                schema,
                order_pairs,
                offset_and_limit,
                pk_indices,
                total_count,
                executor_id,
                key_indices,
                state_table,
            )?,
        })
    }
}

pub struct InnerTopNExecutorNew<S: StateStore> {
    info: ExecutorInfo,

    /// Schema of the executor.
    schema: Schema,

    /// The primary key indices of the `TopNExecutor`
    pk_indices: PkIndices,

    /// The internal key indices of the `TopNExecutor`
    internal_key_indices: PkIndices,

    /// The order of internal keys of the `TopNExecutor`
    internal_key_order_types: Vec<OrderType>,

    /// We are interested in which element is in the range of [offset, offset+limit).
    managed_state: ManagedTopNStateNew<S>,

    /// In-memory cache of top (N + N * `TOPN_CACHE_HIGH_CAPACITY_FACTOR`) rows
    cache: TopNCache,

    #[expect(dead_code)]
    /// Indices of the columns on which key distribution depends.
    key_indices: Vec<usize>,
}

const TOPN_CACHE_HIGH_CAPACITY_FACTOR: usize = 2;

pub struct TopNCache {
    pub low: BTreeMap<OrderedRow, Row>,
    pub middle: BTreeMap<OrderedRow, Row>,
    pub high: BTreeMap<OrderedRow, Row>,
    pub high_capacity: usize,
    pub offset: usize,
    pub limit: usize,
}

impl TopNCache {
    pub fn new(offset: usize, limit: usize) -> Self {
        Self {
            low: BTreeMap::new(),
            middle: BTreeMap::new(),
            high: BTreeMap::new(),
            high_capacity: (offset + limit) * TOPN_CACHE_HIGH_CAPACITY_FACTOR,
            offset,
            limit,
        }
    }

    fn is_high_cache_full(&self) -> bool {
        self.high.len() == self.high_capacity
    }

    /// The cache is updated with the data generated by `op`, `ordered_pk_row`, and `row`, and the
    /// modified information in `self.middle` is recorded to `res_ops` and` res_rows`, which will be
    /// used to generate messages to be sent to downstream operators.
    /// Because we may need to add data from the state table to `self.high` during the delete
    /// operation, so we need to pass in `pk_prefix`, `epoch` and `managed_state` to do a prefix
    /// scan of the state table to get the corresponding information.
    #[allow(clippy::too_many_arguments)]
    pub async fn update<S: StateStore>(
        &mut self,
        pk_prefix: Option<&Row>,
        managed_state: &mut ManagedTopNStateNew<S>,
        op: Op,
        ordered_pk_row: OrderedRow,
        row: Row,
        epoch: u64,
        res_ops: &mut Vec<Op>,
        res_rows: &mut Vec<Row>,
    ) -> StreamExecutorResult<()> {
        match op {
            Op::Insert | Op::UpdateInsert => {
                // Then insert input row to corresponding cache range according to its order key
                if self.low.len() < self.offset {
                    debug_assert!(self.middle.is_empty());
                    debug_assert!(self.high.is_empty());
                    self.low.insert(ordered_pk_row, row);
                    return Ok(());
                }

                // If offset is 0, every input row has noting to do with `cache.low`
                let elem_to_compare_with_middle =
                    if self.offset > 0 && ordered_pk_row <= *self.low.last_key_value().unwrap().0 {
                        // If the new row is in the range of [0, offset), the largest row in
                        // `cache.low` needs be moved to `cache.middle`
                        // which covers the range of [offset, offset+limit)
                        let res = self.low.pop_last().unwrap();
                        self.low.insert(ordered_pk_row, row);
                        res
                    } else {
                        (ordered_pk_row, row)
                    };

                if self.middle.len() < self.limit {
                    self.middle.insert(
                        elem_to_compare_with_middle.0,
                        elem_to_compare_with_middle.1.clone(),
                    );
                    res_ops.push(Op::Insert);
                    res_rows.push(elem_to_compare_with_middle.1);
                    return Ok(());
                }

                let elem_to_compare_with_high =
                    if elem_to_compare_with_middle.0 <= *self.middle.last_key_value().unwrap().0 {
                        // If the row in the range of [offset, offset+limit), the largest row in
                        // `cache.middle` needs to be moved to `cache.high`
                        let res = self.middle.pop_last().unwrap();
                        res_ops.push(Op::Delete);
                        res_rows.push(res.1.clone());
                        res_ops.push(Op::Insert);
                        res_rows.push(elem_to_compare_with_middle.1.clone());
                        self.middle
                            .insert(elem_to_compare_with_middle.0, elem_to_compare_with_middle.1);
                        res
                    } else {
                        elem_to_compare_with_middle
                    };

                if !self.is_high_cache_full() {
                    self.high
                        .insert(elem_to_compare_with_high.0, elem_to_compare_with_high.1);
                } else if elem_to_compare_with_high.0 <= *self.high.last_key_value().unwrap().0 {
                    self.high.pop_last();
                    self.high
                        .insert(elem_to_compare_with_high.0, elem_to_compare_with_high.1);
                }
            }

            Op::Delete | Op::UpdateDelete => {
                // Then we need to remove the row from cache
                if self.middle.len() == self.limit
                    && ordered_pk_row > *self.middle.last_key_value().unwrap().0
                {
                    // The row in the range of [offset+limit, +inf)
                    self.high.remove(&ordered_pk_row);
                } else if self.low.len() == self.offset
                    && (self.offset == 0 || ordered_pk_row > *self.low.last_key_value().unwrap().0)
                {
                    // The row in the range of [offset, offset+limit) which is the result set
                    // Try to fill the high cache if it is empty
                    let largest_ordered_key_in_middle = self.middle.last_key_value().unwrap().0;
                    if self.high.is_empty() {
                        managed_state
                            .fill_cache(
                                pk_prefix,
                                &mut self.high,
                                largest_ordered_key_in_middle,
                                self.high_capacity,
                                epoch,
                            )
                            .await?;
                    }

                    self.middle.remove(&ordered_pk_row);
                    res_ops.push(Op::Delete);
                    res_rows.push(row.clone());

                    // Bring one element, if any, from high cache to middle cache
                    if !self.high.is_empty() {
                        let smallest_elem_in_high = self.high.pop_first().unwrap();
                        res_ops.push(Op::Insert);
                        res_rows.push(smallest_elem_in_high.1.clone());

                        self.middle
                            .insert(smallest_elem_in_high.0, smallest_elem_in_high.1);
                    }
                } else {
                    // The row in the range of [0, offset)
                    self.low.remove(&ordered_pk_row);
                    // Bring one element, if any, from middle cache to low cache
                    if !self.middle.is_empty() {
                        // Try to fill the high cache if it is empty
                        let largest_ordered_key_in_middle = self.middle.last_key_value().unwrap().0;
                        if self.high.is_empty() {
                            managed_state
                                .fill_cache(
                                    pk_prefix,
                                    &mut self.high,
                                    largest_ordered_key_in_middle,
                                    self.high_capacity,
                                    epoch,
                                )
                                .await?;
                        }

                        let smallest_elem_in_middle = self.middle.pop_first().unwrap();
                        res_ops.push(Op::Delete);
                        res_rows.push(smallest_elem_in_middle.1.clone());
                        self.low
                            .insert(smallest_elem_in_middle.0, smallest_elem_in_middle.1);
                    }

                    // Bring one element, if any, from high cache to middle cache
                    if !self.high.is_empty() && self.middle.len() == (self.limit - 1) {
                        let smallest_elem_in_high = self.high.pop_first().unwrap();

                        res_ops.push(Op::Insert);
                        res_rows.push(smallest_elem_in_high.1.clone());
                        self.middle
                            .insert(smallest_elem_in_high.0, smallest_elem_in_high.1);
                    }
                }
            }
        }
        Ok(())
    }
}

pub fn generate_internal_key(
    order_pairs: &[OrderPair],
    pk_indices: PkIndicesRef,
    schema: &Schema,
) -> (PkIndices, Vec<DataType>, Vec<OrderType>) {
    let mut internal_key_indices = vec![];
    let mut internal_order_types = vec![];
    for order_pair in order_pairs {
        internal_key_indices.push(order_pair.column_idx);
        internal_order_types.push(order_pair.order_type);
    }
    for pk_index in pk_indices {
        if !internal_key_indices.contains(pk_index) {
            internal_key_indices.push(*pk_index);
            internal_order_types.push(OrderType::Ascending);
        }
    }
    let internal_data_types = internal_key_indices
        .iter()
        .map(|idx| schema.fields()[*idx].data_type())
        .collect();
    (
        internal_key_indices,
        internal_data_types,
        internal_order_types,
    )
}

impl<S: StateStore> InnerTopNExecutorNew<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_info: ExecutorInfo,
        schema: Schema,
        order_pairs: Vec<OrderPair>,
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        total_count: usize,
        executor_id: u64,
        key_indices: Vec<usize>,
        state_table: RowBasedStateTable<S>,
    ) -> StreamExecutorResult<Self> {
        let (internal_key_indices, internal_key_data_types, internal_key_order_types) =
            generate_internal_key(&order_pairs, &pk_indices, &schema);

        let ordered_row_deserializer =
            OrderedRowDeserializer::new(internal_key_data_types, internal_key_order_types.clone());

        let num_offset = offset_and_limit.0;
        let num_limit = offset_and_limit.1;
        let managed_state =
            ManagedTopNStateNew::<S>::new(total_count, state_table, ordered_row_deserializer);

        Ok(Self {
            info: ExecutorInfo {
                schema: input_info.schema,
                pk_indices: input_info.pk_indices,
                identity: format!("TopNExecutorNew {:X}", executor_id),
            },
            schema,
            managed_state,
            pk_indices,
            internal_key_indices,
            internal_key_order_types,
            cache: TopNCache::new(num_offset, num_limit.unwrap_or(1024)),
            key_indices,
        })
    }
}

#[async_trait]
impl<S: StateStore> TopNExecutorBase for InnerTopNExecutorNew<S> {
    async fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
        epoch: u64,
    ) -> StreamExecutorResult<StreamChunk> {
        let mut res_ops = Vec::with_capacity(self.cache.limit);
        let mut res_rows = Vec::with_capacity(self.cache.limit);
        // We have banned streaming query that doesn't have LIMIT, so it is safe to unwrap here

        // apply the chunk to state table
        for (op, row_ref) in chunk.rows() {
            let pk_row = row_ref.row_by_indices(&self.internal_key_indices);
            let ordered_pk_row = OrderedRow::new(pk_row, &self.internal_key_order_types);
            let row = row_ref.to_owned_row();
            match op {
                Op::Insert | Op::UpdateInsert => {
                    // First insert input row to state store
                    self.managed_state
                        .insert(ordered_pk_row.clone(), row.clone(), epoch)?;
                }

                Op::Delete | Op::UpdateDelete => {
                    // First remove the row from state store
                    self.managed_state
                        .delete(&ordered_pk_row, row.clone(), epoch)?;
                }
            }
            self.cache
                .update(
                    None,
                    &mut self.managed_state,
                    op,
                    ordered_pk_row,
                    row,
                    epoch,
                    &mut res_ops,
                    &mut res_rows,
                )
                .await?
        }
        // compare the those two ranges and emit the differantial result
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
        self.managed_state
            .init_topn_cache(None, &mut self.cache, epoch)
            .await
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::StreamExt;
    use itertools::Itertools;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::{ColumnDesc, ColumnId, Field, TableId};
    use risingwave_common::types::DataType;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_storage::memory::MemoryStateStore;

    use super::*;
    use crate::executor::test_utils::MockSource;
    use crate::executor::{Barrier, Message};

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
            - 3 2
            - 1 0
            +  5 7
            - 2 1
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
            - 5  7
            - 6  9
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

    fn create_order_pairs() -> Vec<OrderPair> {
        vec![
            OrderPair::new(0, OrderType::Ascending),
            OrderPair::new(1, OrderType::Ascending),
        ]
    }

    fn create_source_new() -> Box<MockSource> {
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
        Box::new(MockSource::with_messages(
            schema,
            PkIndices::new(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Chunk(std::mem::take(&mut chunks[2])),
                Message::Chunk(std::mem::take(&mut chunks[3])),
                Message::Barrier(Barrier::new_test_barrier(2)),
            ],
        ))
    }

    fn create_source_new_before_recovery() -> Box<MockSource> {
        let mut chunks = vec![
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
        Box::new(MockSource::with_messages(
            schema,
            PkIndices::new(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(1)),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Barrier(Barrier::new_test_barrier(2)),
            ],
        ))
    }

    fn create_source_new_after_recovery() -> Box<MockSource> {
        let mut chunks = vec![
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
        Box::new(MockSource::with_messages(
            schema,
            PkIndices::new(),
            vec![
                Message::Barrier(Barrier::new_test_barrier(3)),
                Message::Chunk(std::mem::take(&mut chunks[0])),
                Message::Chunk(std::mem::take(&mut chunks[1])),
                Message::Barrier(Barrier::new_test_barrier(4)),
            ],
        ))
    }

    fn create_source() -> Box<MockSource> {
        let mut chunks = create_stream_chunks();
        let schema = create_schema();
        Box::new(MockSource::with_messages(
            schema,
            PkIndices::new(),
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

    fn create_in_memory_state_table(
        data_types: &[DataType],
        order_types: &[OrderType],
        pk_indices: &[usize],
    ) -> RowBasedStateTable<MemoryStateStore> {
        let column_descs = data_types
            .iter()
            .enumerate()
            .map(|(id, data_type)| ColumnDesc::unnamed(ColumnId::new(id as i32), data_type.clone()))
            .collect_vec();
        RowBasedStateTable::new_without_distribution(
            MemoryStateStore::new(),
            TableId::new(0),
            column_descs,
            order_types.to_vec(),
            pk_indices.to_vec(),
        )
    }

    #[tokio::test]
    async fn test_top_n_executor_with_offset() {
        let order_types = create_order_pairs();
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64],
            &[OrderType::Ascending, OrderType::Ascending],
            &[0, 1],
        );
        let top_n_executor = Box::new(
            TopNExecutor::new(
                source as Box<dyn Executor>,
                order_types,
                (3, Some(1000)),
                vec![0, 1],
                0,
                1,
                vec![],
                state_table,
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
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
        // Barrier
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
                - 7 6
                - 8 5
                +  8 5
                - 8 5
                + 11 8"
            )
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );

        let res = top_n_executor.next().await.unwrap().unwrap();
        // (8, 9, 10, 11, 12, 13, 14)
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I  I
                +  8  5
                + 12 10
                + 13 11
                + 14 12"
            )
        );
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );

        // (10, 12, 13, 14)
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I
                - 8 5
                - 9 4
                - 11 8"
            )
        );
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
    }

    #[tokio::test]
    async fn test_top_n_executor_with_limit() {
        let order_types = create_order_pairs();
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64],
            &[OrderType::Ascending, OrderType::Ascending],
            &[0, 1],
        );
        let top_n_executor = Box::new(
            TopNExecutor::new(
                source as Box<dyn Executor>,
                order_types,
                (0, Some(4)),
                vec![0, 1],
                0,
                1,
                vec![],
                state_table,
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
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
                - 10 3
                +  9 4
                - 9 4
                +  8 5"
            )
        );
        // now () -> (1, 2, 3, 8)

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
                - 8 5
                +  7 6
                - 3 2
                +  8 5
                - 1 0
                +  9 4
                - 9 4
                +  5 7
                - 2 1
                +  9 4"
            )
        );

        // (5, 7, 8, 9)
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
                - 9 4
                +  6 9"
            )
        );
        // (5, 6, 7, 8)
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
                - 5 7
                +  9 4
                - 6 9
                + 10 3"
            )
        );
        // (7, 8, 9, 10)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
    }

    #[tokio::test]
    async fn test_top_n_executor_with_offset_and_limit() {
        let order_types = create_order_pairs();
        let source = create_source();
        let state_table = create_in_memory_state_table(
            &[DataType::Int64, DataType::Int64],
            &[OrderType::Ascending, OrderType::Ascending],
            &[0, 1],
        );
        let top_n_executor = Box::new(
            TopNExecutor::new(
                source as Box<dyn Executor>,
                order_types,
                (3, Some(4)),
                vec![0, 1],
                0,
                1,
                vec![],
                state_table,
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
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
                - 7 6
                - 8 5
                +  8 5
                - 8 5
                + 11 8"
            )
        );
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
                +  8 5"
            )
        );
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I  I
                - 8  5
                + 12 10
                - 9  4
                + 13 11
                - 11  8
                + 14 12"
            )
        );
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
    }

    #[tokio::test]
    async fn test_top_n_executor_with_offset_and_limit_new() {
        let order_types = vec![OrderPair::new(0, OrderType::Ascending)];

        let source = create_source_new();
        let state_table = create_in_memory_state_table(
            &[
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
            ],
            &[OrderType::Ascending, OrderType::Ascending],
            &[0, 3],
        );
        let top_n_executor = Box::new(
            TopNExecutor::new(
                source as Box<dyn Executor>,
                order_types,
                (1, Some(3)),
                vec![0, 3],
                0,
                1,
                vec![],
                state_table,
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();

        let res = top_n_executor.next().await.unwrap().unwrap();
        // should be empty
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty("  I I I I")
        );

        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I I
                +  5 1 4 1002
                "
            )
        );

        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I I
                +  1 9 1 1003
                +  9 8 1 1004
                - 9 8 1 1004
                +  1 1 4 1001",
            ),
        );

        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I I
                - 5 1 4 1002
                +  1 0 2 1006",
            )
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
    }

    #[tokio::test]
    async fn test_top_n_executor_with_offset_and_limit_new_after_recovery() {
        let order_types = vec![OrderPair::new(0, OrderType::Ascending)];
        let state_table = create_in_memory_state_table(
            &[
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
                DataType::Int64,
            ],
            &[OrderType::Ascending, OrderType::Ascending],
            &[0, 3],
        );
        let top_n_executor = Box::new(
            TopNExecutor::new(
                create_source_new_before_recovery() as Box<dyn Executor>,
                order_types.clone(),
                (1, Some(3)),
                vec![0, 3],
                0,
                1,
                vec![],
                state_table.clone(),
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();

        let res = top_n_executor.next().await.unwrap().unwrap();
        // should be empty
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty("  I I I I")
        );

        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I I
                +  5 1 4 1002
                "
            )
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );

        // recovery
        let top_n_executor_after_recovery = Box::new(
            TopNExecutor::new(
                create_source_new_after_recovery() as Box<dyn Executor>,
                order_types.clone(),
                (1, Some(3)),
                vec![3],
                0,
                1,
                vec![],
                state_table,
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor_after_recovery.execute();

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );

        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I I
                +  1 9 1 1003
                +  9 8 1 1004
                - 9 8 1 1004
                +  1 1 4 1001",
            ),
        );

        let res = top_n_executor.next().await.unwrap().unwrap();
        assert_eq!(
            *res.as_chunk().unwrap(),
            StreamChunk::from_pretty(
                "  I I I I
                - 5 1 4 1002
                +  1 0 2 1006",
            )
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
    }
}
