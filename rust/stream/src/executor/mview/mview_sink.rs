use async_trait::async_trait;
use risingwave_common::array::Op::*;
use risingwave_common::array::Row;
use risingwave_common::catalog::Schema;
use risingwave_common::util::sort_util::OrderType;
use risingwave_storage::{Keyspace, StateStore};

use super::mview_state::ManagedMViewState;
use crate::executor::{
    Barrier, Executor, Message, PkIndicesRef, Result, SimpleExecutor, StreamChunk,
};

/// `MViewSinkExecutor` writes data to a row-based memtable, so that data could
/// be queried by the AP engine.
pub struct MViewSinkExecutor<S: StateStore> {
    input: Box<dyn Executor>,
    schema: Schema,
    local_state: ManagedMViewState<S>,
    pk_columns: Vec<usize>,
}

impl<S: StateStore> MViewSinkExecutor<S> {
    pub fn new(
        input: Box<dyn Executor>,
        keyspace: Keyspace<S>,
        schema: Schema,
        pk_columns: Vec<usize>,
        orderings: Vec<OrderType>,
    ) -> Self {
        Self {
            input,
            local_state: ManagedMViewState::new(
                keyspace,
                schema.clone(),
                pk_columns.clone(),
                orderings,
            ),
            schema,
            pk_columns,
        }
    }

    async fn flush(&mut self, barrier: Barrier) -> Result<Message> {
        self.local_state.flush(barrier.epoch).await?;
        Ok(Message::Barrier(barrier))
    }
}

#[async_trait]
impl<S: StateStore> Executor for MViewSinkExecutor<S> {
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
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_columns
    }
}

impl<S: StateStore> SimpleExecutor for MViewSinkExecutor<S> {
    fn input(&mut self) -> &mut dyn Executor {
        &mut *self.input
    }

    fn consume_chunk(&mut self, chunk: StreamChunk) -> Result<Message> {
        let StreamChunk {
            ops,
            columns,
            visibility,
            ..
        } = &chunk;

        for (idx, op) in ops.iter().enumerate() {
            // check visibility
            let visible = visibility
                .as_ref()
                .map(|x| x.is_set(idx).unwrap())
                .unwrap_or(true);
            if !visible {
                continue;
            }

            // assemble pk row
            let mut pk_row = vec![];
            for column_id in &self.pk_columns {
                let datum = columns[*column_id].array_ref().datum_at(idx);
                pk_row.push(datum);
            }
            let pk_row = Row(pk_row);

            // assemble row
            let mut row = vec![];
            for column in columns {
                let datum = column.array_ref().datum_at(idx);
                row.push(datum);
            }
            let row = Row(row);

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
    use risingwave_common::catalog::{Schema, SchemaId, TableId};
    use risingwave_common::column_nonnull;
    use risingwave_common::util::downcast_arc;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_pb::data::data_type::TypeName;
    use risingwave_pb::data::DataType;
    use risingwave_pb::plan::column_desc::ColumnEncodingType;
    use risingwave_pb::plan::ColumnDesc;
    use risingwave_storage::memory::MemoryStateStore;
    use risingwave_storage::table::TableManager;
    use risingwave_storage::Keyspace;

    use crate::executor::test_utils::*;
    use crate::executor::*;
    use crate::task::{SimpleTableManager, StateStoreImpl, StreamTableManager};
    use crate::*;

    #[tokio::test]
    async fn test_sink() {
        // Prepare storage and memtable.
        let store_mgr = Arc::new(SimpleTableManager::new());
        let table_id = TableId::new(SchemaId::default(), 1);
        // Two columns of int32 type, the first column is PK.
        let columns = vec![
            ColumnDesc {
                column_type: Some(DataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
                encoding: ColumnEncodingType::Raw as i32,
                name: "v1".to_string(),
                is_primary: false,
                column_id: 0,
            },
            ColumnDesc {
                column_type: Some(DataType {
                    type_name: TypeName::Int32 as i32,
                    ..Default::default()
                }),
                encoding: ColumnEncodingType::Raw as i32,
                name: "v2".to_string(),
                is_primary: false,
                column_id: 1,
            },
        ];
        let pks = vec![0_usize];
        let orderings = vec![OrderType::Ascending];

        let state_store = MemoryStateStore::new();
        let state_store_impl = StateStoreImpl::MemoryStateStore(state_store.clone());

        store_mgr
            .create_materialized_view(
                &table_id,
                &columns,
                pks.clone(),
                orderings,
                state_store_impl,
            )
            .unwrap();
        // Prepare source chunks.
        let chunk1 = StreamChunk {
            ops: vec![Op::Insert, Op::Insert, Op::Insert],
            columns: vec![
                column_nonnull! { I32Array, Int32Type, [1, 2, 3] },
                column_nonnull! { I32Array, Int32Type, [4, 5, 6] },
            ],
            visibility: None,
        };
        let chunk2 = StreamChunk {
            ops: vec![Op::Insert, Op::Delete],
            columns: vec![
                column_nonnull! { I32Array, Int32Type, [7, 3] },
                column_nonnull! { I32Array, Int32Type, [8, 6] },
            ],
            visibility: None,
        };

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

        let mut sink_executor = Box::new(MViewSinkExecutor::new(
            Box::new(source),
            Keyspace::table_root(state_store, &table_id),
            schema,
            pks,
            vec![OrderType::Ascending],
        ));

        let table = downcast_arc::<MViewTable<MemoryStateStore>>(
            store_mgr.get_table(&table_id).unwrap().into_any(),
        )
        .unwrap();

        sink_executor.next().await.unwrap();
        // First stream chunk. We check the existence of (3) -> (3,6)
        match sink_executor.next().await.unwrap() {
            Message::Barrier(_) => {
                let datum = table
                    .get(Row(vec![Some(3_i32.into())]), 1)
                    .await
                    .unwrap()
                    .unwrap();
                // Dirty trick to assert_eq between (&int32 and integer).
                let d_value = datum.unwrap().as_int32() + 1;
                assert_eq!(d_value, 7);
            }
            _ => unreachable!(),
        }

        sink_executor.next().await.unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        match sink_executor.next().await.unwrap() {
            Message::Barrier(_) => {
                let datum = table
                    .get(Row(vec![Some(7_i32.into())]), 1)
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
