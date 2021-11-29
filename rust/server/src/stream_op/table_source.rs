use crate::source::{StreamSourceReader, TableStreamReader};
use crate::stream_op::{Barrier, Executor, Message};
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
    stream_reader: Option<TableStreamReader>,
    barrier_receiver: UnboundedReceiver<Message>,
    first_execution: bool,
}

impl TableSourceExecutor {
    pub fn new(
        table_id: TableId,
        schema: Schema,
        stream_reader: TableStreamReader,
        barrier_receiver: UnboundedReceiver<Message>,
    ) -> Self {
        TableSourceExecutor {
            table_id,
            schema,
            stream_reader: Some(stream_reader),
            barrier_receiver,
            first_execution: true,
        }
    }
}

#[async_trait]
impl Executor for TableSourceExecutor {
    async fn next(&mut self) -> Result<Message> {
        if let Some(stream_reader) = &mut self.stream_reader {
            if self.first_execution {
                stream_reader.open().await?;
                self.first_execution = false;
            }

            // FIXME: incorrect usage of `select!`
            let msg = tokio::select! {
              result = stream_reader.next() => {
                Message::Chunk(result?)
              }
              message = self.barrier_receiver.next() => {
                message.expect("barrier channel closed unexpectedly")
              }
            };
            Ok(match msg {
                Message::Chunk(chunk) => {
                    // FIXME: extract the columns with given column ids
                    Message::Chunk(chunk)
                }
                Message::Barrier(barrier) => {
                    if barrier.stop {
                        // Drop the receiver here, the source will encounter an error at the next
                        // send.
                        self.stream_reader = None;
                    }

                    Message::Barrier(barrier)
                }
            })
        } else {
            Ok(Message::Barrier(Barrier {
                epoch: 0,
                stop: true,
            }))
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
    use crate::source::*;
    use crate::storage::{BummockTable, TableColumnDesc};
    use crate::stream_op::{Op, StreamChunk};
    use futures::channel::mpsc::unbounded;
    use itertools::Itertools;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::ArrayImpl;
    use risingwave_common::array::{I32Array, Utf8Array};
    use risingwave_common::array_nonnull;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::{DataTypeKind, DataTypeRef, DecimalType, Int32Type, StringType};
    use std::sync::Arc;

    impl SourceImpl {
        fn as_table(&self) -> &TableSource {
            match self {
                SourceImpl::Table(table) => table,
                _ => panic!("not a table source"),
            }
        }
    }

    #[tokio::test]
    async fn test_table_source() -> Result<()> {
        let table_id = TableId::default();
        let table_columns = vec![
            TableColumnDesc {
                column_id: 0,
                data_type: Arc::new(DecimalType::new(false, 10, 5)?),
            },
            TableColumnDesc {
                column_id: 1,
                data_type: Arc::new(DecimalType::new(false, 10, 5)?),
            },
        ];
        let table = Arc::new(BummockTable::new(&table_id, table_columns));

        let source_manager = MemSourceManager::new();
        source_manager.create_table_source(&table_id, table.clone())?;
        let source = source_manager.get_source(&table_id)?.source;
        let table_source = source.as_table();

        let col1_type = Int32Type::create(false) as DataTypeRef;
        let col2_type = StringType::create(true, 10, DataTypeKind::Varchar);

        // Prepare test data chunks
        let col1_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let col2_arr1: Arc<ArrayImpl> =
            Arc::new(array_nonnull! { Utf8Array, ["foo", "bar", "baz"] }.into());
        let col1_arr2: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [4, 5, 6] }.into());
        let col2_arr2: Arc<ArrayImpl> =
            Arc::new(Utf8Array::from_slice(&[Some("hello"), None, Some("world")])?.into());

        let chunk1 = {
            let col1 = Column::new(col1_arr1.clone(), col1_type.clone());
            let col2 = Column::new(col2_arr1.clone(), col2_type.clone());
            let vis = vec![Op::Insert, Op::Insert, Op::Insert];
            StreamChunk::new(vis, vec![col1, col2], None)
        };

        let chunk2 = {
            let col1 = Column::new(col1_arr2.clone(), col1_type.clone());
            let col2 = Column::new(col2_arr2.clone(), col2_type.clone());
            let vis = vec![Op::Insert, Op::Insert, Op::Insert];
            StreamChunk::new(vis, vec![col1, col2], None)
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

        let column_ids = vec![0, 1];
        let stream_reader = table_source.stream_reader(TableReaderContext {}, column_ids)?;
        let (barrier_sender, barrier_receiver) = unbounded();

        let mut source =
            TableSourceExecutor::new(table_id, schema, stream_reader, barrier_receiver);

        barrier_sender
            .unbounded_send(Message::Barrier(Barrier {
                epoch: 1,
                stop: false,
            }))
            .unwrap();

        let mut writer = table_source.create_writer()?;
        // Write 1st chunk
        writer.write(chunk1).await?;

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
                Message::Barrier(barrier) => {
                    assert_eq!(barrier.epoch, 1)
                }
            }
        }

        // Write 2nd chunk
        writer.write(chunk2).await?;

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
        let table_id = TableId::default();

        let table_columns = vec![
            TableColumnDesc {
                column_id: 0,
                data_type: Arc::new(DecimalType::new(false, 10, 5)?),
            },
            TableColumnDesc {
                column_id: 1,
                data_type: Arc::new(DecimalType::new(false, 10, 5)?),
            },
        ];
        let table = Arc::new(BummockTable::new(&table_id, table_columns));

        let source_manager = MemSourceManager::new();
        source_manager.create_table_source(&table_id, table.clone())?;
        let source = source_manager.get_source(&table_id)?.source;
        let table_source = source.as_table();

        let col1_type = Int32Type::create(false) as DataTypeRef;
        let col2_type = StringType::create(true, 10, DataTypeKind::Varchar);

        // Prepare test data chunks
        let col1_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let col2_arr1: Arc<ArrayImpl> =
            Arc::new(array_nonnull! { Utf8Array, ["foo", "bar", "baz"] }.into());

        let chunk1 = {
            let col1 = Column::new(col1_arr1.clone(), col1_type.clone());
            let col2 = Column::new(col2_arr1.clone(), col2_type.clone());
            let vis = vec![Op::Insert, Op::Insert, Op::Insert];
            StreamChunk::new(vis, vec![col1, col2], None)
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

        let column_ids = vec![0, 1];
        let stream_reader = table_source.stream_reader(TableReaderContext {}, column_ids)?;
        let (barrier_sender, barrier_receiver) = unbounded();

        let mut source =
            TableSourceExecutor::new(table_id, schema, stream_reader, barrier_receiver);

        let mut writer = table_source.create_writer()?;
        writer.write(chunk1.clone()).await?;

        barrier_sender
            .unbounded_send(Message::Barrier(Barrier {
                epoch: 1,
                stop: true,
            }))
            .unwrap();

        source.next().await.unwrap();
        source.next().await.unwrap();
        writer.write(chunk1).await?;

        Ok(())
    }
}
