use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::channel::mpsc::UnboundedReceiver;
use futures::StreamExt;
use risingwave_common::array::column::Column;
use risingwave_common::array::{
    ArrayBuilder, ArrayImpl, I64ArrayBuilder, InternalError, RwError, StreamChunk,
};
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_source::*;

use crate::executor::{Executor, Message, PkIndices, PkIndicesRef};

/// `StreamSourceExecutor` is a streaming source from external systems such as Kafka
pub struct StreamSourceExecutor {
    source_id: TableId,
    source_desc: SourceDesc,
    column_ids: Vec<i32>,
    schema: Schema,
    pk_indices: PkIndices,
    reader: Box<dyn StreamSourceReader>,
    barrier_receiver: UnboundedReceiver<Message>,
    /// current allocated row id
    next_row_id: AtomicU64,
    first_execution: bool,

    /// Identity string
    identity: String,
}

impl StreamSourceExecutor {
    pub fn new(
        source_id: TableId,
        source_desc: SourceDesc,
        column_ids: Vec<i32>,
        schema: Schema,
        pk_indices: PkIndices,
        barrier_receiver: UnboundedReceiver<Message>,
        executor_id: u64,
    ) -> Result<Self> {
        let source = source_desc.clone().source;
        let reader: Box<dyn StreamSourceReader> = match source.as_ref() {
            SourceImpl::HighLevelKafka(s) => Box::new(s.stream_reader(
                HighLevelKafkaSourceReaderContext {
                    query_id: None,
                    bound_timestamp_ms: None,
                },
                column_ids.clone(),
            )?),
            SourceImpl::Table(s) => {
                Box::new(s.stream_reader(TableReaderContext {}, column_ids.clone())?)
            }
            SourceImpl::TableV2(s) => {
                Box::new(s.stream_reader(TableV2ReaderContext, column_ids.clone())?)
            }
        };

        Ok(Self {
            source_id,
            source_desc,
            column_ids,
            schema,
            pk_indices,
            reader,
            barrier_receiver,
            next_row_id: AtomicU64::from(0u64),
            first_execution: true,
            identity: format!("StreamSourceExecutor {:X}", executor_id),
        })
    }

    fn gen_row_column(&mut self, len: usize) -> Column {
        let mut builder = I64ArrayBuilder::new(len).unwrap();

        for _ in 0..len {
            builder
                .append(Some(self.next_row_id.fetch_add(1, Ordering::Relaxed) as i64))
                .unwrap();
        }

        Column::new(Arc::new(ArrayImpl::from(builder.finish().unwrap())))
    }

    fn refill_row_id_column(&mut self, chunk: StreamChunk) -> StreamChunk {
        if let Some(row_id_index) = self.source_desc.row_id_index {
            let row_id_column_id = self.source_desc.columns[row_id_index as usize].column_id;

            if let Some(idx) = self
                .column_ids
                .iter()
                .position(|column_id| *column_id == row_id_column_id)
            {
                let (ops, mut columns, bitmap) = chunk.into_inner();
                columns[idx] = self.gen_row_column(columns[idx].array().len());
                return StreamChunk::new(ops, columns, bitmap);
            }
        }
        chunk
    }
}

#[async_trait]
impl Executor for StreamSourceExecutor {
    async fn next(&mut self) -> Result<Message> {
        if self.first_execution {
            self.reader.open().await?;
            self.first_execution = false
        }

        // FIXME: may lose message
        tokio::select! {
          chunk = self.reader.next() => {
            let mut chunk = chunk?;

            // Refill row id only if not a table source.
            // Note(eric): Currently, rows from external sources are filled with row_ids here,
            // but rows from tables (by insert statements) are filled in InsertExecutor.
            //
            // TODO: in the future, we may add row_id column here for TableV2 as well
            if !matches!(self.source_desc.source.as_ref(), SourceImpl::Table(_) | SourceImpl::TableV2(_)) {
              chunk = self.refill_row_id_column(chunk);
            }

            Ok(Message::Chunk(chunk))
          }
          message = self.barrier_receiver.next() => {
            message.ok_or_else(|| RwError::from(InternalError("stream closed unexpectedly".to_string())))
          }
        }
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.pk_indices
    }

    fn identity(&self) -> &str {
        self.identity.as_str()
    }
}

impl Debug for StreamSourceExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamSourceExecutor")
            .field("source_id", &self.source_id)
            .field("column_ids", &self.column_ids)
            .field("pk_indices", &self.pk_indices)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use futures::channel::mpsc::unbounded;
    use itertools::Itertools;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayImpl, I32Array, I64Array, Op, StreamChunk, Utf8Array};
    use risingwave_common::array_nonnull;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataTypeKind;
    use risingwave_source::*;
    use risingwave_storage::bummock::BummockTable;
    use risingwave_storage::TableColumnDesc;

    use super::*;
    use crate::executor::{Barrier, Mutation, StreamSourceExecutor};

    #[tokio::test]
    async fn test_table_source() -> Result<()> {
        let table_id = TableId::default();

        let rowid_type = DataTypeKind::Int64;
        let col1_type = DataTypeKind::Int32;
        let col2_type = DataTypeKind::Varchar;

        let table_columns = vec![
            TableColumnDesc {
                column_id: 0,
                data_type: rowid_type,
                name: String::new(),
            },
            TableColumnDesc {
                column_id: 1,
                data_type: col1_type,
                name: String::new(),
            },
            TableColumnDesc {
                column_id: 2,
                data_type: col2_type,
                name: String::new(),
            },
        ];
        let table = Arc::new(BummockTable::new(&table_id, table_columns));

        let source_manager = MemSourceManager::new();
        source_manager.create_table_source(&table_id, table.clone())?;
        let source_desc = source_manager.get_source(&table_id)?;
        let source = source_desc.clone().source;
        let table_source = source.as_table();

        // Prepare test data chunks
        let rowid_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I64Array, [0, 0, 0] }.into());
        let col1_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let col2_arr1: Arc<ArrayImpl> =
            Arc::new(array_nonnull! { Utf8Array, ["foo", "bar", "baz"] }.into());
        let rowid_arr2: Arc<ArrayImpl> = Arc::new(array_nonnull! { I64Array, [0, 0, 0] }.into());
        let col1_arr2: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [4, 5, 6] }.into());
        let col2_arr2: Arc<ArrayImpl> =
            Arc::new(Utf8Array::from_slice(&[Some("hello"), None, Some("world")])?.into());

        let chunk1 = {
            let rowid = Column::new(rowid_arr1.clone());
            let col1 = Column::new(col1_arr1.clone());
            let col2 = Column::new(col2_arr1.clone());
            let vis = vec![Op::Insert, Op::Insert, Op::Insert];
            StreamChunk::new(vis, vec![rowid, col1, col2], None)
        };

        let chunk2 = {
            let rowid = Column::new(rowid_arr2.clone());
            let col1 = Column::new(col1_arr2.clone());
            let col2 = Column::new(col2_arr2.clone());
            let vis = vec![Op::Insert, Op::Insert, Op::Insert];
            StreamChunk::new(vis, vec![rowid, col1, col2], None)
        };

        let schema = Schema {
            fields: vec![
                Field::new_without_name(rowid_type),
                Field::new_without_name(col1_type),
                Field::new_without_name(col2_type),
            ],
        };

        let column_ids = vec![0, 1, 2];
        let pk_indices = vec![0];

        let (barrier_sender, barrier_receiver) = unbounded();

        let mut source = StreamSourceExecutor::new(
            table_id,
            source_desc,
            column_ids,
            schema,
            pk_indices,
            barrier_receiver,
            1,
        )
        .unwrap();

        barrier_sender
            .unbounded_send(Message::Barrier(Barrier {
                epoch: 1,
                ..Barrier::default()
            }))
            .unwrap();

        let mut writer = table_source.create_writer()?;
        // Write 1st chunk
        writer.write(chunk1).await?;

        for _ in 0..2 {
            match source.next().await.unwrap() {
                Message::Chunk(chunk) => {
                    assert_eq!(3, chunk.columns().len());
                    assert_eq!(
                        col1_arr1.iter().collect_vec(),
                        chunk.column(1).array_ref().iter().collect_vec(),
                    );
                    assert_eq!(
                        col2_arr1.iter().collect_vec(),
                        chunk.column(2).array_ref().iter().collect_vec()
                    );
                    assert_eq!(vec![Op::Insert; 3], chunk.ops());
                }
                Message::Barrier(barrier) => {
                    assert_eq!(barrier.epoch, 1)
                }
            }
        }

        // Write 2nd chunk
        writer.write(chunk2).await?;

        if let Message::Chunk(chunk) = source.next().await.unwrap() {
            assert_eq!(3, chunk.columns().len());
            assert_eq!(
                col1_arr2.iter().collect_vec(),
                chunk.column(1).array_ref().iter().collect_vec()
            );
            assert_eq!(
                col2_arr2.iter().collect_vec(),
                chunk.column(2).array_ref().iter().collect_vec()
            );
            assert_eq!(vec![Op::Insert; 3], chunk.ops());
        } else {
            unreachable!();
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_table_dropped() -> Result<()> {
        let table_id = TableId::default();

        let rowid_type = DataTypeKind::Int64;
        let col1_type = DataTypeKind::Int32;
        let col2_type = DataTypeKind::Varchar;

        let table_columns = vec![
            TableColumnDesc {
                column_id: 0,
                data_type: rowid_type,
                name: String::new(),
            },
            TableColumnDesc {
                column_id: 1,
                data_type: col1_type,
                name: String::new(),
            },
            TableColumnDesc {
                column_id: 2,
                data_type: col2_type,
                name: String::new(),
            },
        ];
        let table = Arc::new(BummockTable::new(&table_id, table_columns));

        let source_manager = MemSourceManager::new();
        source_manager.create_table_source(&table_id, table.clone())?;
        let source_desc = source_manager.get_source(&table_id)?;
        let source = source_desc.clone().source;
        let table_source = source.as_table();

        // Prepare test data chunks
        let rowid_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I64Array, [0, 0, 0] }.into());
        let col1_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let col2_arr1: Arc<ArrayImpl> =
            Arc::new(array_nonnull! { Utf8Array, ["foo", "bar", "baz"] }.into());

        let chunk1 = {
            let rowid = Column::new(rowid_arr1.clone());
            let col1 = Column::new(col1_arr1.clone());
            let col2 = Column::new(col2_arr1.clone());
            let vis = vec![Op::Insert, Op::Insert, Op::Insert];
            StreamChunk::new(vis, vec![rowid, col1, col2], None)
        };

        let schema = Schema {
            fields: vec![
                Field::new_without_name(rowid_type),
                Field::new_without_name(col1_type),
                Field::new_without_name(col2_type),
            ],
        };

        let column_ids = vec![0, 1, 2];
        let pk_indices = vec![0];

        let (barrier_sender, barrier_receiver) = unbounded();
        let mut source = StreamSourceExecutor::new(
            table_id,
            source_desc,
            column_ids,
            schema,
            pk_indices,
            barrier_receiver,
            1,
        )
        .unwrap();

        let mut writer = table_source.create_writer()?;
        writer.write(chunk1.clone()).await?;

        barrier_sender
            .unbounded_send(Message::Barrier(Barrier {
                epoch: 1,
                mutation: Mutation::Stop(HashSet::default()),
            }))
            .unwrap();

        source.next().await.unwrap();
        source.next().await.unwrap();
        writer.write(chunk1).await?;

        Ok(())
    }
}
