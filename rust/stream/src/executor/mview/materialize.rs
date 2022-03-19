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
//
use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::Op::*;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnId, Schema, TableId};
use risingwave_common::try_match_expand;
use risingwave_common::util::sort_util::OrderPair;
use risingwave_pb::stream_plan;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_storage::{Keyspace, StateStore};

use super::state::ManagedMViewState;
use crate::executor::{
    Barrier, Executor, ExecutorBuilder, Message, PkIndicesRef, Result, SimpleExecutor, StreamChunk,
};
use crate::task::{ExecutorParams, StreamManagerCore};

/// `MaterializeExecutor` materializes changes in stream into a materialized view on storage.
pub struct MaterializeExecutor<S: StateStore> {
    input: Box<dyn Executor>,

    local_state: ManagedMViewState<S>,

    /// Columns of arrange keys (including pk, group keys, join keys, etc.)
    arrange_columns: Vec<usize>,

    /// Identity string
    identity: String,

    /// Logical Operator Info
    op_info: String,
}

pub struct MaterializeExecutorBuilder {}

impl ExecutorBuilder for MaterializeExecutorBuilder {
    fn new_boxed_executor(
        mut params: ExecutorParams,
        node: &stream_plan::StreamNode,
        store: impl StateStore,
        _stream: &mut StreamManagerCore,
    ) -> Result<Box<dyn Executor>> {
        let node = try_match_expand!(node.get_node().unwrap(), Node::MaterializeNode)?;

        let table_id = TableId::from(&node.table_ref_id);
        let keys = node
            .column_orders
            .iter()
            .map(OrderPair::from_prost)
            .collect();
        let column_ids = node
            .column_ids
            .iter()
            .map(|id| ColumnId::from(*id))
            .collect();

        let keyspace = Keyspace::table_root(store, &table_id);

        Ok(Box::new(MaterializeExecutor::new(
            params.input.remove(0),
            keyspace,
            keys,
            column_ids,
            params.executor_id,
            params.op_info,
        )))
    }
}

impl<S: StateStore> MaterializeExecutor<S> {
    pub fn new(
        input: Box<dyn Executor>,
        keyspace: Keyspace<S>,
        keys: Vec<OrderPair>,
        column_ids: Vec<ColumnId>,
        executor_id: u64,
        op_info: String,
    ) -> Self {
        let arrange_columns = keys.iter().map(|k| k.column_idx).collect();
        let arrange_order_types = keys.iter().map(|k| k.order_type).collect();
        Self {
            input,
            local_state: ManagedMViewState::new(keyspace, column_ids, arrange_order_types),
            arrange_columns,
            identity: format!("MaterializeExecutor {:X}", executor_id),
            op_info,
        }
    }

    async fn flush(&mut self, barrier: Barrier) -> Result<Message> {
        self.local_state.flush(barrier.epoch.prev).await?;
        Ok(Message::Barrier(barrier))
    }
}

#[async_trait]
impl<S: StateStore> Executor for MaterializeExecutor<S> {
    async fn next(&mut self) -> Result<Message> {
        match self.input().next().await {
            Ok(message) => match message {
                Message::Chunk(chunk) => self.consume_chunk(chunk),
                Message::Barrier(b) => self.flush(b).await,
            },
            Err(e) => Err(e),
        }
    }

    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.arrange_columns
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }

    fn logical_operator_info(&self) -> &str {
        &self.op_info
    }
}

impl<S: StateStore> std::fmt::Debug for MaterializeExecutor<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaterializeExecutor")
            .field("input", &self.input)
            .field("arrange_columns", &self.arrange_columns)
            .finish()
    }
}

impl<S: StateStore> SimpleExecutor for MaterializeExecutor<S> {
    fn input(&mut self) -> &mut dyn Executor {
        &mut *self.input
    }

    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        for (idx, op) in chunk.ops().iter().enumerate() {
            // check visibility
            let visible = chunk
                .visibility()
                .as_ref()
                .map(|x| x.is_set(idx).unwrap())
                .unwrap_or(true);
            if !visible {
                continue;
            }

            // assemble pk row
            let arrange_row = Row(self
                .arrange_columns
                .iter()
                .map(|col_idx| chunk.column_at(*col_idx).array_ref().datum_at(idx))
                .collect_vec());

            // assemble row
            let row = Row(chunk
                .columns()
                .iter()
                .map(|x| x.array_ref().datum_at(idx))
                .collect_vec());

            match op {
                Insert | UpdateInsert => {
                    self.local_state.put(arrange_row, row);
                }
                Delete | UpdateDelete => {
                    self.local_state.delete(arrange_row);
                }
            }
        }

        Ok(Message::Chunk(chunk))
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::array::{I32Array, Op};
    use risingwave_common::catalog::{Field, Schema, TableId};
    use risingwave_common::column_nonnull;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::Keyspace;

    use crate::executor::test_utils::*;
    use crate::executor::*;

    #[tokio::test]
    async fn test_materialize_executor() {
        // Prepare storage and memtable.
        let memory_state_store = MemoryStateStore::new();
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let schema = Schema::new(vec![
            Field::unnamed(DataType::Int32),
            Field::unnamed(DataType::Int32),
        ]);
        let column_ids = vec![0.into(), 1.into()];

        // Prepare source chunks.
        let chunk1 = StreamChunk::new(
            vec![Op::Insert, Op::Insert, Op::Insert],
            vec![
                column_nonnull! { I32Array, [1, 2, 3] },
                column_nonnull! { I32Array, [4, 5, 6] },
            ],
            None,
        );
        let chunk2 = StreamChunk::new(
            vec![Op::Insert, Op::Delete],
            vec![
                column_nonnull! { I32Array, [7, 3] },
                column_nonnull! { I32Array, [8, 6] },
            ],
            None,
        );

        // Prepare stream executors.
        let source = MockSource::with_messages(
            schema.clone(),
            PkIndices::new(),
            vec![
                Message::Chunk(chunk1),
                Message::Barrier(Barrier::default()),
                Message::Chunk(chunk2),
                Message::Barrier(Barrier::default()),
            ],
        );

        let keyspace = Keyspace::table_root(memory_state_store.clone(), &table_id);

        let mut materialize_executor = Box::new(MaterializeExecutor::new(
            Box::new(source),
            keyspace,
            vec![OrderPair::new(0, OrderType::Ascending)],
            column_ids,
            1,
            "MaterializeExecutor".to_string(),
        ));

        materialize_executor.next().await.unwrap();

        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.unwrap() {
            Message::Barrier(_) => {
                // Simply assert there is 3 rows (6 elements) in state store instead of doing full
                // comparison
                assert_eq!(
                    memory_state_store
                        .scan::<_, Vec<u8>>(.., None, u64::MAX)
                        .await
                        .unwrap()
                        .len(),
                    6
                );

                // FIXME: restore this test by using new `RowTable` interface
                // let datum = table
                //     .get(Row(vec![Some(3_i32.into())]), 1, u64::MAX)
                //     .await
                //     .unwrap()
                //     .unwrap();
                // assert_eq!(*datum.unwrap().as_int32(), 6_i32);
            }
            _ => unreachable!(),
        }

        materialize_executor.next().await.unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.unwrap() {
            Message::Barrier(_) => {
                // Simply assert there is  3 +1 -1 = 3 rows (6 element) in state store instead of
                // doing full comparison
                assert_eq!(
                    memory_state_store
                        .scan::<_, Vec<u8>>(.., None, u64::MAX)
                        .await
                        .unwrap()
                        .len(),
                    6
                );

                // FIXME: restore this test by using new `RowTable` interface
                // let datum = table
                //     .get(Row(vec![Some(7_i32.into())]), 1, u64::MAX)
                //     .await
                //     .unwrap()
                //     .unwrap();
                // assert_eq!(*datum.unwrap().as_int32(), 8);
            }
            _ => unreachable!(),
        }
    }
}
