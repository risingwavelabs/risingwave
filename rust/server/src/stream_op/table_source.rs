use crate::stream_op::{Executor, Message, StreamChunk};
use async_trait::async_trait;
use futures::channel::mpsc::UnboundedReceiver;
use futures::StreamExt;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use std::fmt::{Debug, Formatter};

/// `TableSourceExecutor` extracts changes from a Table
pub struct TableSourceExecutor {
    table_id: TableId,
    schema: Schema,
    data_receiver: Option<UnboundedReceiver<StreamChunk>>,
    barrier_receiver: UnboundedReceiver<Message>,
}

impl TableSourceExecutor {
    pub fn new(
        table_id: TableId,
        schema: Schema,
        data_receiver: UnboundedReceiver<StreamChunk>,
        barrier_receiver: UnboundedReceiver<Message>,
    ) -> Self {
        TableSourceExecutor {
            table_id,
            schema,
            data_receiver: Some(data_receiver),
            barrier_receiver,
        }
    }
}

#[async_trait]
impl Executor for TableSourceExecutor {
    async fn next(&mut self) -> Result<Message> {
        if let Some(data_receiver) = &mut self.data_receiver {
            let msg = tokio::select! {
              chunk =data_receiver.next() => {
                chunk.map(Message::Chunk)
              }
              message = self.barrier_receiver.next() => {
                message
              }
            };
            let msg = msg.expect("table stream closed unexpectedly");
            Ok(match msg {
                Message::Chunk(chunk) => Message::Chunk(chunk),
                Message::Barrier { epoch, stop } => {
                    if stop {
                        // Drop the receiver here, the source will encounter an error at the next send.
                        self.data_receiver = None;
                    }

                    Message::Barrier { epoch, stop }
                }
                // TODO: Maybe removed in the future
                Message::Terminate => unreachable!("unreachable"),
            })
        } else {
            Ok(Message::Barrier {
                epoch: 0,
                stop: true,
            })
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl Debug for TableSourceExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableSourceExecutor")
            .field("table_id", &self.table_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{SimpleMemTable, Table};
    use crate::stream_op::Op;
    use futures::channel::mpsc::unbounded;
    use itertools::Itertools;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayImpl, DataChunk};
    use risingwave_common::array::{I32Array, UTF8Array};
    use risingwave_common::array_nonnull;
    use risingwave_common::catalog::test_utils::mock_table_id;
    use risingwave_common::catalog::Field;
    use risingwave_common::types::{DataTypeKind, DataTypeRef, Int32Type, StringType};
    use risingwave_pb::data::{data_type::TypeName, DataType};
    use risingwave_pb::plan::{column_desc::ColumnEncodingType, ColumnDesc};
    use risingwave_pb::ToProto;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_table_source() -> Result<()> {
        let table_id = mock_table_id();
        let column1 = ColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Int32 as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            name: "test_col".to_string(),
            is_primary: false,
        };
        let column2 = ColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Varchar as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            name: "test_col".to_string(),
            is_primary: false,
        };
        let columns = vec![column1.to_proto(), column2.to_proto()];
        let table = SimpleMemTable::new(&table_id, &columns);

        let col1_type = Int32Type::create(false) as DataTypeRef;
        let col2_type = StringType::create(true, 10, DataTypeKind::Varchar);

        // Prepare test data chunks
        let col1_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let col2_arr1: Arc<ArrayImpl> =
            Arc::new(array_nonnull! { UTF8Array, ["foo", "bar", "baz"] }.into());
        let col1_arr2: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [4, 5, 6] }.into());
        let col2_arr2: Arc<ArrayImpl> =
            Arc::new(UTF8Array::from_slice(&[Some("hello"), None, Some("world")])?.into());

        let chunk1 = {
            let col1 = Column::new(col1_arr1.clone(), col1_type.clone());
            let col2 = Column::new(col2_arr1.clone(), col2_type.clone());
            DataChunk::new(vec![col1, col2], None)
        };

        let chunk2 = {
            let col1 = Column::new(col1_arr2.clone(), col1_type.clone());
            let col2 = Column::new(col2_arr2.clone(), col2_type.clone());
            DataChunk::new(vec![col1, col2], None)
        };

        let schema = Schema {
            fields: vec![
                Field {
                    data_type: col1_type.clone(),
                },
                Field {
                    data_type: col2_type.clone(),
                },
            ],
        };

        let stream_recv = table.create_stream()?;
        let (barrier_sender, barrier_receiver) = unbounded();

        let mut source = TableSourceExecutor::new(table_id, schema, stream_recv, barrier_receiver);

        barrier_sender
            .unbounded_send(Message::Barrier {
                epoch: 1,
                stop: false,
            })
            .unwrap();
        // Write 1st chunk
        let card = table.append(chunk1).await?;
        // barrier_sender.start_send(Message::Barrier(0))

        assert_eq!(3, card);

        for _ in 0..2 {
            match source.next().await.unwrap() {
                Message::Chunk(chunk) => {
                    assert_eq!(2, chunk.columns.len());
                    assert_eq!(
                        col1_arr1.iter().collect_vec(),
                        chunk.columns[0].array_ref().iter().collect_vec(),
                    );
                    assert_eq!(
                        col2_arr1.iter().collect_vec(),
                        chunk.columns[1].array_ref().iter().collect_vec()
                    );
                    assert_eq!(vec![Op::Insert; 3], chunk.ops);
                }
                Message::Barrier { epoch, stop: _ } => {
                    assert_eq!(epoch, 1)
                }
                Message::Terminate => unreachable!(),
            }
        }

        // Write 2nd chunk
        let card = table.append(chunk2).await?;
        assert_eq!(3, card);

        if let Message::Chunk(chunk) = source.next().await.unwrap() {
            assert_eq!(2, chunk.columns.len());
            assert_eq!(
                col1_arr2.iter().collect_vec(),
                chunk.columns[0].array_ref().iter().collect_vec()
            );
            assert_eq!(
                col2_arr2.iter().collect_vec(),
                chunk.columns[1].array_ref().iter().collect_vec()
            );
            assert_eq!(vec![Op::Insert; 3], chunk.ops);
        } else {
            unreachable!();
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_table_dropped() -> Result<()> {
        let table_id = mock_table_id();

        let column1 = ColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Int32 as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            name: "test_col".to_string(),
            is_primary: false,
        };
        let column2 = ColumnDesc {
            column_type: Some(DataType {
                type_name: TypeName::Varchar as i32,
                ..Default::default()
            }),
            encoding: ColumnEncodingType::Raw as i32,
            name: "test_col".to_string(),
            is_primary: false,
        };
        let columns = vec![column1.to_proto(), column2.to_proto()];
        let table = SimpleMemTable::new(&table_id, &columns);

        let col1_type = Int32Type::create(false) as DataTypeRef;
        let col2_type = StringType::create(true, 10, DataTypeKind::Varchar);

        // Prepare test data chunks
        let col1_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let col2_arr1: Arc<ArrayImpl> =
            Arc::new(array_nonnull! { UTF8Array, ["foo", "bar", "baz"] }.into());

        let chunk1 = {
            let col1 = Column::new(col1_arr1.clone(), col1_type.clone());
            let col2 = Column::new(col2_arr1.clone(), col2_type.clone());
            DataChunk::new(vec![col1, col2], None)
        };

        let schema = Schema {
            fields: vec![
                Field {
                    data_type: col1_type.clone(),
                },
                Field {
                    data_type: col2_type.clone(),
                },
            ],
        };

        let stream_recv = table.create_stream()?;
        let (barrier_sender, barrier_receiver) = unbounded();

        let mut source = TableSourceExecutor::new(table_id, schema, stream_recv, barrier_receiver);

        table.append(chunk1.clone()).await?;

        barrier_sender
            .unbounded_send(Message::Barrier {
                epoch: 1,
                stop: true,
            })
            .unwrap();

        source.next().await.unwrap();
        source.next().await.unwrap();
        table.append(chunk1).await?;

        assert!(!table.is_stream_connected());

        Ok(())
    }
}
