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
use risingwave_common::types::DataType;
use risingwave_common::util::ordered::{OrderedRow, OrderedRowDeserializer};
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_storage::StateStore;

use super::error::StreamExecutorResult;
use super::managed_state::top_n::ManagedTopNStateNew;
use super::top_n_executor::{generate_output, TopNExecutorBase, TopNExecutorWrapper};
use super::{BoxedMessageStream, Executor, ExecutorInfo, PkIndices, PkIndicesRef};

/// `TopNExecutor` works with input with modification, it keeps all the data
/// records/rows that have been seen, and returns topN records overall.
pub type TopNExecutorNew<S> = TopNExecutorWrapper<InnerTopNExecutorNew<S>>;

impl<S: StateStore> TopNExecutorNew<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn Executor>,
        order_pairs: Vec<OrderPair>,
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        store: S,
        table_id: TableId,
        cache_size: Option<usize>,
        total_count: usize,
        executor_id: u64,
        key_indices: Vec<usize>,
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
                store,
                table_id,
                cache_size,
                total_count,
                executor_id,
                key_indices,
            )?,
        })
    }
}

pub struct InnerTopNExecutorNew<S: StateStore> {
    info: ExecutorInfo,

    /// Schema of the executor.
    schema: Schema,

    /// `LIMIT XXX`. `None` means no limit.
    limit: Option<usize>,
    /// `OFFSET XXX`. `0` means no offset.
    offset: usize,

    /// The primary key indices of the `TopNExecutor`
    pk_indices: PkIndices,

    /// The internal key indices of the `TopNExecutor`
    internal_key_indices: PkIndices,

    /// The order of internal keys of the `TopNExecutor`
    internal_key_order_types: Vec<OrderType>,

    /// We are interested in which element is in the range of [offset, offset+limit).
    managed_state: ManagedTopNStateNew<S>,

    #[allow(dead_code)]
    /// Indices of the columns on which key distribution depends.
    key_indices: Vec<usize>,
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
        store: S,
        table_id: TableId,
        cache_size: Option<usize>,
        total_count: usize,
        executor_id: u64,
        key_indices: Vec<usize>,
    ) -> StreamExecutorResult<Self> {
        let (internal_key_indices, internal_key_data_types, internal_key_order_types) =
            generate_internal_key(&order_pairs, &pk_indices, &schema);

        let ordered_row_deserializer =
            OrderedRowDeserializer::new(internal_key_data_types, internal_key_order_types.clone());

        let row_data_types = schema
            .fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect::<Vec<_>>();

        let managed_state = ManagedTopNStateNew::<S>::new(
            cache_size,
            total_count,
            store,
            table_id,
            row_data_types,
            ordered_row_deserializer,
            internal_key_indices.clone(),
        );

        Ok(Self {
            info: ExecutorInfo {
                schema: input_info.schema,
                pk_indices: input_info.pk_indices,
                identity: format!("TopNExecutorNew {:X}", executor_id),
            },
            schema,
            offset: offset_and_limit.0,
            limit: offset_and_limit.1,
            managed_state,
            pk_indices,
            internal_key_indices,
            internal_key_order_types,
            key_indices,
        })
    }

    async fn flush_inner(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        self.managed_state.flush(epoch).await
    }
}

impl<S: StateStore> Executor for InnerTopNExecutorNew<S> {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        panic!("Should execute by wrapper");
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
}

#[async_trait]
impl<S: StateStore> TopNExecutorBase for InnerTopNExecutorNew<S> {
    async fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
        epoch: u64,
    ) -> StreamExecutorResult<StreamChunk> {
        let mut new_ops = vec![];
        let mut new_rows = vec![];
        let num_limit = self.limit.unwrap_or(0);

        for (op, row_ref) in chunk.rows() {
            let pk_row = row_ref.row_by_indices(&self.internal_key_indices);
            let ordered_pk_row = OrderedRow::new(pk_row, &self.internal_key_order_types);
            let row = row_ref.to_owned_row();
            match op {
                Op::Insert | Op::UpdateInsert => {
                    // Compare the boundary keys before and after insertion:
                    // - if the inserted key in the range of [0, offset), emit nothing
                    // - if in the range of [offset, offset + limit), emit diff of result set
                    // - if in the range of [offset + limit, Inf), emit nothing
                    let (start_row, end_row) = self
                        .managed_state
                        .find_range(self.offset, num_limit, epoch)
                        .await?;

                    self.managed_state
                        .insert(ordered_pk_row.clone(), row.clone(), epoch)
                        .await?;

                    // no offset
                    if self.offset == 0 {
                        if !end_row.is_valid() {
                            // has no limit, emit to result set
                            new_ops.push(Op::Insert);
                            new_rows.push(row.clone());
                            continue;
                        }

                        // check whether new key in the [0, limit]
                        if let Some(end_key) = end_row.ordered_key.as_ref() {
                            if ordered_pk_row < *end_key {
                                // update boundary if needed
                                let (_, new_end_row) = self
                                    .managed_state
                                    .find_range(self.offset, num_limit, epoch)
                                    .await?;

                                if end_row.ordered_key != new_end_row.ordered_key {
                                    new_ops.push(Op::Delete);
                                    new_rows.push(end_row.row);
                                }

                                new_ops.push(Op::Insert);
                                new_rows.push(row.clone());
                            }
                        }
                    } else if self.offset + 1 == self.managed_state.total_count() {
                        // handle the boundary case
                        let (new_start_row, _) = self
                            .managed_state
                            .find_range(self.offset, num_limit, epoch)
                            .await?;
                        new_ops.push(Op::Insert);
                        new_rows.push(new_start_row.row);
                    } else if let Some(start_key) = start_row.ordered_key.as_ref() {
                        if ordered_pk_row < *start_key {
                            let (new_start_row, _) =
                                self.managed_state.find_range(self.offset, 0, epoch).await?;

                            if new_start_row.is_valid() {
                                new_ops.push(Op::Insert);
                                new_rows.push(new_start_row.row.clone());
                            }
                        } else if !end_row.is_valid() {
                            // no limit
                            new_ops.push(Op::Insert);
                            new_rows.push(row);
                        } else if let Some(end_key) = end_row.ordered_key.as_ref() {
                            if ordered_pk_row < *end_key {
                                new_ops.push(Op::Insert);
                                new_rows.push(row);

                                // handle limit
                                let (_, new_end_row) = self
                                    .managed_state
                                    .find_range(self.offset, num_limit, epoch)
                                    .await?;

                                if end_row.ordered_key != new_end_row.ordered_key {
                                    new_ops.push(Op::Delete);
                                    new_rows.push(end_row.row);
                                }
                            }
                        }
                    }
                }
                Op::Delete | Op::UpdateDelete => {
                    // Compare the boundary keys before and after deletion:
                    // - if the deleted key in the range of [0, offset), emit diff of result set
                    // - if in the range of [offset, offset + limit), emit diff of result set
                    // - if in the range of [offset + limit, Inf), emit nothing
                    let (start_row, end_row) = self
                        .managed_state
                        .find_range(self.offset, num_limit, epoch)
                        .await?;

                    // delete the row
                    self.managed_state
                        .delete(&ordered_pk_row, row.clone(), epoch)
                        .await?;

                    // check start end first, since LIMIT may not specified
                    if let Some(start_key) = start_row.ordered_key.as_ref() {
                        if ordered_pk_row < *start_key {
                            // deleted row in [0, offset) the topn range will move forward
                            let (new_start_row, new_end_row) = self
                                .managed_state
                                .find_range(self.offset, num_limit, epoch)
                                .await?;

                            // update both ends
                            if start_row.ordered_key != new_start_row.ordered_key {
                                new_ops.push(Op::Delete);
                                new_rows.push(start_row.row);
                            }

                            if new_end_row.is_valid()
                                && end_row.ordered_key != new_end_row.ordered_key
                            {
                                new_ops.push(Op::Insert);
                                new_rows.push(new_end_row.row);

                                if new_end_row.ordered_key < end_row.ordered_key {
                                    new_ops.push(Op::Delete);
                                    new_rows.push(end_row.row);
                                }
                            }
                        } else {
                            let mut is_contained = end_row.ordered_key.is_none();
                            if let Some(end_key) = end_row.ordered_key.as_ref() {
                                is_contained = ordered_pk_row <= *end_key;
                            }
                            // deleted row in (offset+limit, Inf)
                            if !is_contained {
                                continue;
                            }

                            // deleted row in [offset, offset+limit], emit change of result set
                            new_ops.push(Op::Delete);
                            new_rows.push(row);

                            let (_, new_end_row) = self
                                .managed_state
                                .find_range(self.offset, num_limit, epoch)
                                .await?;

                            // update high end if nedded
                            if new_end_row.is_valid()
                                && new_end_row.ordered_key != end_row.ordered_key
                            {
                                new_ops.push(Op::Insert);
                                new_rows.push(new_end_row.row);
                                if end_row.is_valid()
                                    && new_end_row.ordered_key < end_row.ordered_key
                                {
                                    new_ops.push(Op::Delete);
                                    new_rows.push(end_row.row);
                                    assert!(end_row.ordered_key.is_some());
                                }
                            }
                        }
                    }
                }
            }
        }
        generate_output(new_rows, new_ops, &self.schema)
    }

    async fn flush_data(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        self.flush_inner(epoch).await
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
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use futures::StreamExt;
    use risingwave_common::array::stream_chunk::StreamChunkTestExt;
    use risingwave_common::catalog::Field;
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
    async fn test_top_n_executor_with_offset() {
        let order_types = create_order_pairs();
        let source = create_source();
        let top_n_executor = Box::new(
            TopNExecutorNew::new(
                source as Box<dyn Executor>,
                order_types,
                (3, None),
                vec![0, 1],
                MemoryStateStore::new(),
                TableId::from(0x2333),
                Some(2),
                0,
                1,
                vec![],
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
                "  I I
                +  7 6
                -  7 6
                -  8 5
                +  8 5
                -  8 5
                + 11 8"
            )
        );

        // (5, 7, 8) -> (9, 10, 11)
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
                +  8  5
                + 12 10
                + 13 11
                + 14 12"
            )
        );
        // (5, 6, 7) -> (8, 9, 10, 11, 12, 13, 14)
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
                -  8 5
                -  9 4
                - 11 8"
            )
        );
        // (7, 8, 9) -> (10, 13, 14, _)
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
        let top_n_executor = Box::new(
            TopNExecutorNew::new(
                source as Box<dyn Executor>,
                order_types,
                (0, Some(4)),
                vec![0, 1],
                MemoryStateStore::new(),
                TableId::from(0x2333),
                Some(2),
                0,
                1,
                vec![],
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
                -  9 4
                +  8 5"
            )
        );
        // now () -> (1, 2, 3, 8) -> (9, 10)

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
                -  8 5
                +  7 6
                -  3 2
                +  8 5
                -  1 0
                +  9 4
                -  9 4
                +  5 7
                -  2 1
                +  9 4"
            )
        );

        // () -> (5, 7, 8, 9) -> (10, 11)
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
                -  9 4
                +  6 9"
            )
        );
        // () -> (5, 6, 7, 8) -> (9, 10, 11, 12, 13, 14)
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
                -  5 7
                +  9 4
                -  6 9
                + 10 3"
            )
        );
        // () -> (7, 8, 9, 10) -> (13, 14)
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
        let top_n_executor = Box::new(
            TopNExecutorNew::new(
                source as Box<dyn Executor>,
                order_types,
                (3, Some(4)),
                vec![0, 1],
                MemoryStateStore::new(),
                TableId::from(0x2333),
                Some(2),
                0,
                1,
                vec![],
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
        // now (1, 2, 3) -> (8, 9, 10, _) -> ()

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
                -  7 6
                -  8 5
                +  8 5
                -  8 5
                + 11 8"
            )
        );
        // (5, 7, 8) -> (9, 10, 11, _) -> ()
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
        // (5, 6, 7) -> (8, 9, 10, 11) -> (12, 13, 14)
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
                -  8  5 
                + 12 10
                -  9  4 
                + 13 11
                - 11  8 
                + 14 12"
            )
        );

        // (7, 8, 9) -> (10, 13, 14, _)
        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
    }
}
