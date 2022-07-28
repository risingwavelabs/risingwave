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

use std::collections::HashSet;

use async_trait::async_trait;
use risingwave_common::array::{Op, Row, StreamChunk};
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::types::DataType;
use risingwave_common::util::ordered::{OrderedRow, OrderedRowDeserializer};
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_storage::StateStore;

use super::error::StreamExecutorResult;
use super::managed_state::top_n::{ManagedTopNStateNew, TopNStateRow};
use super::top_n_executor::{generate_output, TopNExecutorBase, TopNExecutorWrapper};
use super::{BoxedMessageStream, Executor, ExecutorInfo, PkIndices, PkIndicesRef};

pub type LocalTopNExecutor<S> = TopNExecutorWrapper<InnerLocalTopNExecutorNew<S>>;

impl<S: StateStore> LocalTopNExecutor<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn Executor>,
        order_pairs: Vec<OrderPair>,
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        store: S,
        table_id: TableId,
        total_count: usize,
        executor_id: u64,
        key_indices: Vec<usize>,
        group_by: usize,
    ) -> StreamExecutorResult<Self> {
        let info = input.info();
        let schema = input.schema().clone();

        Ok(TopNExecutorWrapper {
            input,
            inner: InnerLocalTopNExecutorNew::new(
                info,
                schema,
                order_pairs,
                offset_and_limit,
                pk_indices,
                store,
                table_id,
                total_count,
                executor_id,
                key_indices,
                group_by,
            )?,
        })
    }
}

pub struct InnerLocalTopNExecutorNew<S: StateStore> {
    info: ExecutorInfo,

    /// Schema of the executor.
    schema: Schema,

    /// `LIMIT XXX`. None means no limit.
    limit: Option<usize>,

    /// `OFFSET XXX`. `0` means no offset.
    offset: usize,

    /// The primary key indices of the `LocalTopNExecutor`
    pk_indices: PkIndices,

    /// The internal key indices of the `LocalTopNExecutor`
    internal_key_indices: PkIndices,

    /// The order of internal keys of the `LocalTopNExecutor`
    internal_key_order_types: Vec<OrderType>,

    /// We are interested in which element is in the range of [offset, offset+limit).
    managed_state: ManagedTopNStateNew<S>,

    // TODO: support multiple group keys
    /// which column we used to group the data.
    group_by: usize,

    #[expect(dead_code)]
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

impl<S: StateStore> InnerLocalTopNExecutorNew<S> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input_info: ExecutorInfo,
        schema: Schema,
        order_pairs: Vec<OrderPair>,
        offset_and_limit: (usize, Option<usize>),
        pk_indices: PkIndices,
        store: S,
        table_id: TableId,
        total_count: usize,
        executor_id: u64,
        key_indices: Vec<usize>,
        group_by: usize,
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
            group_by,
        })
    }

    async fn flush_inner(&mut self, epoch: u64) -> StreamExecutorResult<()> {
        self.managed_state.flush(epoch).await
    }
}

impl<S: StateStore> Executor for InnerLocalTopNExecutorNew<S> {
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
impl<S: StateStore> TopNExecutorBase for InnerLocalTopNExecutorNew<S> {
    async fn apply_chunk(
        &mut self,
        chunk: StreamChunk,
        epoch: u64,
    ) -> StreamExecutorResult<StreamChunk> {
        let mut res_ops = Vec::with_capacity(self.limit.unwrap_or(1024));
        let mut res_rows = Vec::with_capacity(self.limit.unwrap_or(1024));

        // Record all groups encountered in the input
        let mut unique_group_keys = HashSet::new();

        // Iterate over all the input, identify which groups they cover.
        for (op, row_ref) in chunk.rows() {
            let row = row_ref.to_owned_row();
            let datum = row[self.group_by].clone();

            // The group is encountered for the first time in this input
            if !unique_group_keys.contains(&datum) {
                // Because our state table is already set up to sort by the column value which is
                // specified by `self.group_by` . Therefore, the first row of the current group
                //  can be directly obtained by prefix scanning
                let prefix_key = Row::new(vec![datum.clone()]);
                let old_rows = self
                    .managed_state
                    .find_range(Some(&prefix_key), self.offset, self.limit, epoch)
                    .await?;
                // TODO: Don't delete all the previous data and then insert new data,
                emit_multi_rows(&mut res_ops, &mut res_rows, Op::Delete, old_rows);
                unique_group_keys.insert(datum);
            }

            let pk_row = row_ref.row_by_indices(&self.internal_key_indices);
            let ordered_pk_row = OrderedRow::new(pk_row, &self.internal_key_order_types);

            match op {
                Op::Insert | Op::UpdateInsert => {
                    self.managed_state.insert(ordered_pk_row, row, epoch)?;
                }
                Op::Delete | Op::UpdateDelete => {
                    self.managed_state.delete(&ordered_pk_row, row, epoch)?;
                }
            }
        }

        // Generate new messages based on the current state table
        for group_key in unique_group_keys {
            let prefix_key = Row::new(vec![group_key.clone()]);
            let new_rows = self
                .managed_state
                .find_range(Some(&prefix_key), self.offset, self.limit, epoch)
                .await?;
            emit_multi_rows(&mut res_ops, &mut res_rows, Op::Insert, new_rows);
        }

        generate_output(res_rows, res_ops, &self.schema)
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

fn emit_multi_rows(
    res_ops: &mut Vec<Op>,
    res_rows: &mut Vec<Row>,
    op: Op,
    rows: Vec<TopNStateRow>,
) {
    rows.into_iter().for_each(|topn_row| {
        res_ops.push(op);
        res_rows.push(topn_row.row);
    })
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
            OrderPair::new(1, OrderType::Ascending),
            OrderPair::new(0, OrderType::Ascending),
        ]
    }

    fn create_stream_chunks() -> Vec<StreamChunk> {
        let chunk0 = StreamChunk::from_pretty(
            "  I I
            + 10 9
            +  8 8
            +  7 8
            +  9 1
            + 10 1
            +  8 1",
        );
        let chunk1 = StreamChunk::from_pretty(
            "  I I
            - 10 9
            -  8 8
            - 10 1",
        );
        let chunk2 = StreamChunk::from_pretty(
            "  I I
            -  7 8
            -  8 1
            -  9 1",
        );
        let chunk3 = StreamChunk::from_pretty(
            "  I  I
            +  5  1
            +  2  1
            +  3  1
            +  4  1",
        );
        vec![chunk0, chunk1, chunk2, chunk3]
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

    fn compare_stream_chunk(lhs_chunk: &StreamChunk, rhs_chunk: &StreamChunk) {
        let mut lhs_message = HashSet::new();
        let mut rhs_message = HashSet::new();
        for (op, row_ref) in lhs_chunk.rows() {
            let row = row_ref.to_owned_row();
            let op_code = match op {
                Op::Insert => 1,
                Op::Delete => 2,
                Op::UpdateDelete => 3,
                Op::UpdateInsert => 4,
            };
            lhs_message.insert((op_code, row.clone()));
        }

        for (op, row_ref) in rhs_chunk.rows() {
            let row = row_ref.to_owned_row();
            let op_code = match op {
                Op::Insert => 1,
                Op::Delete => 2,
                Op::UpdateDelete => 3,
                Op::UpdateInsert => 4,
            };
            rhs_message.insert((op_code, row.clone()));
        }

        assert_eq!(lhs_message, rhs_message);
    }

    #[tokio::test]
    async fn test_local_top_n_executor() {
        test_without_offset_and_with_limits().await;
        test_with_offset_and_with_limits().await;
        test_without_limits().await;
    }
    async fn test_without_offset_and_with_limits() {
        let order_types = create_order_pairs();
        let source = create_source();
        let top_n_executor = Box::new(
            LocalTopNExecutor::new(
                source as Box<dyn Executor>,
                order_types,
                (0, Some(2)),
                vec![1, 0],
                MemoryStateStore::new(),
                TableId::from(0x2333),
                0,
                1,
                vec![],
                1,
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        compare_stream_chunk(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I
            + 10 9
            +  8 8
            +  7 8
            +  9 1
            +  8 1",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        compare_stream_chunk(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I
            - 10 9
            -  8 8
            -  7 8
            -  9 1
            -  8 1
            +  7 8
            +  9 1
            +  8 1",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        compare_stream_chunk(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I
            -  7 8
            -  9 1
            -  8 1",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        compare_stream_chunk(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I
            +  2 1
            +  3 1",
            ),
        );
    }

    async fn test_with_offset_and_with_limits() {
        let order_types = create_order_pairs();
        let source = create_source();
        let top_n_executor = Box::new(
            LocalTopNExecutor::new(
                source as Box<dyn Executor>,
                order_types,
                (1, Some(2)),
                vec![1, 0],
                MemoryStateStore::new(),
                TableId::from(0x2333),
                0,
                1,
                vec![],
                1,
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        compare_stream_chunk(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I
            +  8 8
            +  9 1
            + 10 1",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        compare_stream_chunk(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I
            -  8 8
            -  9 1
            - 10 1
            +  9 1",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        compare_stream_chunk(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I
            -  9 1",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        compare_stream_chunk(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I
            +  3 1
            +  4 1",
            ),
        );
    }

    async fn test_without_limits() {
        let order_types = create_order_pairs();
        let source = create_source();
        let top_n_executor = Box::new(
            LocalTopNExecutor::new(
                source as Box<dyn Executor>,
                order_types,
                (0, None),
                vec![1, 0],
                MemoryStateStore::new(),
                TableId::from(0x2333),
                0,
                1,
                vec![],
                1,
            )
            .unwrap(),
        );
        let mut top_n_executor = top_n_executor.execute();

        // consume the init barrier
        top_n_executor.next().await.unwrap().unwrap();
        let res = top_n_executor.next().await.unwrap().unwrap();
        compare_stream_chunk(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I
            + 10 9
            +  8 8
            +  7 8
            +  9 1
            + 10 1
            +  8 1",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        compare_stream_chunk(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I
            - 10 9
            -  8 8
            -  7 8
            -  9 1
            - 10 1
            -  8 1
            +  7 8
            +  9 1
            +  8 1",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        compare_stream_chunk(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I I
            -  9 1
            -  8 1
            -  7 8",
            ),
        );

        // barrier
        assert_matches!(
            top_n_executor.next().await.unwrap().unwrap(),
            Message::Barrier(_)
        );
        let res = top_n_executor.next().await.unwrap().unwrap();
        compare_stream_chunk(
            res.as_chunk().unwrap(),
            &StreamChunk::from_pretty(
                "  I  I
            +  5  1
            +  2  1
            +  3  1
            +  4  1",
            ),
        );
    }
}
