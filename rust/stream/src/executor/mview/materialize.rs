use async_trait::async_trait;
use itertools::Itertools;
use risingwave_common::array::Op::*;
use risingwave_common::array::Row;
use risingwave_common::catalog::{ColumnId, Schema};
use risingwave_common::util::sort_util::{OrderPair, OrderType};
use risingwave_storage::{Keyspace, StateStore};

use super::state::ManagedMViewState;
use crate::executor::{
    Barrier, Executor, Message, PkIndicesRef, Result, SimpleExecutor, StreamChunk,
};
/// `MaterializeExecutor` materializes changes in stream into a materialized view on storage.
pub struct MaterializeExecutor<S: StateStore> {
    input: Box<dyn Executor>,

    local_state: ManagedMViewState<S>,

    /// Columns of primary keys
    pk_columns: Vec<usize>,

    /// Identity string
    identity: String,

    /// Logical Operator Info
    op_info: String,
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
        let pk_columns = keys.iter().map(|k| k.column_idx).collect();
        Self {
            input,
            local_state: ManagedMViewState::new(keyspace, column_ids, keys),
            pk_columns,
            identity: format!("MaterializeExecutor {:X}", executor_id),
            op_info,
        }
    }

    async fn flush(&mut self, barrier: Barrier) -> Result<Message> {
        self.local_state.flush(barrier.epoch).await?;
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
        &self.pk_columns
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
            .field("pk_columns", &self.pk_columns)
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
            let pk_row = Row(self
                .pk_columns
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
                    self.local_state.put(pk_row, row);
                }
                Delete | UpdateDelete => {
                    self.local_state.delete(pk_row);
                }
            }
        }

        Ok(Message::Chunk(chunk))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::array::{I32Array, Op, Row};
    use risingwave_common::catalog::{Schema, TableId};
    use risingwave_common::column_nonnull;
    use risingwave_common::util::downcast_arc;
    use risingwave_common::util::sort_util::{OrderPair, OrderType};
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::plan::column_desc::ColumnEncodingType;
    use risingwave_pb::plan::ColumnDesc;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::{SimpleTableManager, TableManager};
    use risingwave_storage::{Keyspace, StateStoreImpl};

    use crate::executor::test_utils::*;
    use crate::executor::*;

    #[tokio::test]
    async fn test_sink() {
        // Prepare storage and memtable.
        let store = MemoryStateStore::new();
        let store_mgr = Arc::new(SimpleTableManager::new(StateStoreImpl::MemoryStateStore(
            store.clone(),
        )));
        let table_id = TableId::new(1);
        // Two columns of int32 type, the first column is PK.
        let columns = vec![
            ColumnDesc {
                column_type: Some(DataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
                encoding: ColumnEncodingType::Raw as i32,
                name: "v1".to_string(),
                column_id: 0,
                ..Default::default()
            },
            ColumnDesc {
                column_type: Some(DataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
                encoding: ColumnEncodingType::Raw as i32,
                name: "v2".to_string(),
                column_id: 1,
                ..Default::default()
            },
        ];
        let pks = vec![0_usize];
        let orderings = vec![OrderType::Ascending];

        store_mgr
            .create_materialized_view(&table_id, &columns, pks.clone(), orderings)
            .unwrap();
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
        let schema = Schema::try_from(&columns).unwrap();
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

        let mut materialize_executor = Box::new(MaterializeExecutor::new(
            Box::new(source),
            Keyspace::table_root(store, &table_id),
            vec![OrderPair::new(0, OrderType::Ascending)],
            vec![0.into()],
            1,
            "MaterializeExecutor".to_string(),
        ));

        let table = downcast_arc::<MViewTable<MemoryStateStore>>(
            store_mgr.get_table(&table_id).unwrap().into_any(),
        )
        .unwrap();

        materialize_executor.next().await.unwrap();
        let epoch = u64::MAX;
        // First stream chunk. We check the existence of (3) -> (3,6)
        match materialize_executor.next().await.unwrap() {
            Message::Barrier(_) => {
                let datum = table
                    .get(Row(vec![Some(3_i32.into())]), 1, epoch)
                    .await
                    .unwrap()
                    .unwrap();
                // Dirty trick to assert_eq between (&int32 and integer).
                let d_value = datum.unwrap().as_int32() + 1;
                assert_eq!(d_value, 7);
            }
            _ => unreachable!(),
        }

        materialize_executor.next().await.unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match materialize_executor.next().await.unwrap() {
            Message::Barrier(_) => {
                let datum = table
                    .get(Row(vec![Some(7_i32.into())]), 1, epoch)
                    .await
                    .unwrap()
                    .unwrap();
                // Dirty trick to assert_eq between (&int32 and integer).
                let d_value = datum.unwrap().as_int32() + 1;
                assert_eq!(d_value, 9);
            }
            _ => unreachable!(),
        }
    }
}
