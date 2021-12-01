use super::{Barrier, KeyedState, RowSerializer};
use super::{Executor, Message, Result, SimpleExecutor, StreamChunk};
use async_trait::async_trait;
use risingwave_common::array::Row;
use risingwave_common::catalog::Schema;
use risingwave_common::util::sort_util::OrderPair;
use std::sync::Arc;

/// `MViewSinkExecutor` writes data to a row-based memtable, so that data could
/// be queried by the AP engine.
pub struct MViewSinkExecutor<StateBackend>
where
    StateBackend: KeyedState<RowSerializer, RowSerializer>,
{
    input: Box<dyn Executor>,
    schema: Schema,
    state: StateBackend,
    pk_col: Vec<usize>,
    order_pairs: Arc<Vec<OrderPair>>,
}

impl<StateBackend> MViewSinkExecutor<StateBackend>
where
    StateBackend: KeyedState<RowSerializer, RowSerializer>,
{
    pub fn new(
        input: Box<dyn Executor>,
        schema: Schema,
        state: StateBackend,
        pk_col: Vec<usize>,
        order_pairs: Arc<Vec<OrderPair>>,
    ) -> Self {
        Self {
            input,
            schema,
            state,
            pk_col,
            order_pairs,
        }
    }

    async fn flush(&mut self, barrier: Barrier) -> Result<Message> {
        self.state.flush().await?;
        Ok(Message::Barrier(barrier))
    }
}

#[async_trait]
impl<StateBackend> Executor for MViewSinkExecutor<StateBackend>
where
    StateBackend: KeyedState<RowSerializer, RowSerializer>,
{
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
}

impl<StateBackend> SimpleExecutor for MViewSinkExecutor<StateBackend>
where
    StateBackend: KeyedState<RowSerializer, RowSerializer>,
{
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
            for column_id in &self.pk_col {
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

            use risingwave_common::array::Op::*;

            match op {
                Insert | UpdateInsert => {
                    self.state.put(pk_row, row);
                }
                Delete | UpdateDelete => {
                    // TODO(MrCroxx): make sure the delete implementation writes tombstones.
                    self.state.delete(&pk_row);
                }
            }
        }

        Ok(Message::Chunk(chunk))
    }
}

#[cfg(test)]
mod tests {

    use crate::stream_op::test_utils::*;
    use crate::stream_op::*;
    use crate::*;
    use risingwave_common::array::{I32Array, Op, Row};
    use risingwave_common::catalog::Field;
    use risingwave_common::types::{Int32Type, Scalar};
    use risingwave_pb::data::{data_type::TypeName, DataType};
    use risingwave_pb::plan::{column_desc::ColumnEncodingType, ColumnDesc};

    use std::sync::Arc;

    #[tokio::test]
    async fn test_sink() {
        // TODO(MrCroxx): use `Field` directly.
        // Two columns of int32 type, the first column is PK.
        let column_desc1 = ColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Int32 as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            name: "v1".to_string(),
            is_primary: false,
            column_id: 0,
        };
        let column_desc2 = ColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Int32 as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            name: "v2".to_string(),
            is_primary: false,
            column_id: 1,
        };
        let columns: Vec<ColumnDesc> = vec![column_desc1, column_desc2];
        let pks = vec![0_usize];

        // TODO: Remove to_prost later.
        let key_schema = Schema::try_from(
            &pks.iter()
                .map(|col_idx| columns[*col_idx].clone())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let value_schema = Schema::try_from(&columns).unwrap();

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

        let schema = Schema {
            fields: vec![
                Field {
                    data_type: Int32Type::create(false),
                },
                Field {
                    data_type: Int32Type::create(false),
                },
            ],
        };

        let source = MockSource::with_messages(
            schema.clone(),
            vec![
                Message::Chunk(chunk1),
                Message::Barrier(Barrier::default()),
                Message::Chunk(chunk2),
                Message::Barrier(Barrier::default()),
            ],
        );

        let mut sink_executor = Box::new(MViewSinkExecutor::new(
            Box::new(source),
            schema,
            InMemoryKeyedState::new(
                RowSerializer::new(key_schema),
                RowSerializer::new(value_schema),
            ),
            pks,
            Arc::new(vec![]),
        ));

        sink_executor.next().await.unwrap();
        // First stream chunk. We check the existence of (3) -> (3,6)
        if let Message::Barrier(_) = sink_executor.next().await.unwrap() {
            let value_row = Row(vec![Some(3_i32.to_scalar_value())]);
            let res_row = sink_executor.state.get(&value_row).await;
            if let Ok(res_row_in) = res_row {
                let datum = res_row_in.unwrap().0.get(1).unwrap().clone();
                // Dirty trick to assert_eq between (&int32 and integer).
                let d_value = datum.unwrap().as_int32() + 1;
                assert_eq!(d_value, 7);
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }

        sink_executor.next().await.unwrap();
        // Second stream chunk. We check the existence of (7) -> (7,8)
        if let Message::Barrier(_) = sink_executor.next().await.unwrap() {
            // From (7) -> (7,8)
            let value_row = Row(vec![Some(7_i32.to_scalar_value())]);
            let res_row = sink_executor.state.get(&value_row).await;
            if let Ok(res_row_in) = res_row {
                let datum = res_row_in.unwrap().0.get(1).unwrap().clone();
                let d_value = datum.unwrap().as_int32() + 1;
                assert_eq!(d_value, 9);
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }
    }
}
