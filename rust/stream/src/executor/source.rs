use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use opentelemetry::metrics::{Counter, MeterProvider};
use opentelemetry::KeyValue;
use risingwave_common::array::column::Column;
use risingwave_common::array::{
    ArrayBuilder, ArrayImpl, I64ArrayBuilder, InternalError, RwError, StreamChunk,
};
use risingwave_common::catalog::{ColumnId, Schema, TableId};
use risingwave_common::error::Result;
use risingwave_source::*;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::executor::monitor::DEFAULT_COMPUTE_STATS;
use crate::executor::{Executor, Message, PkIndices, PkIndicesRef};

/// [`SourceExecutor`] is a streaming source, from risingwave's batch table, or external systems
/// such as Kafka.
pub struct SourceExecutor {
    source_id: TableId,
    source_desc: SourceDesc,
    column_ids: Vec<ColumnId>,
    schema: Schema,
    pk_indices: PkIndices,
    reader: Box<dyn StreamSourceReader>,
    barrier_receiver: UnboundedReceiver<Message>,
    /// current allocated row id
    next_row_id: AtomicU64,
    first_execution: bool,

    /// Identity string
    identity: String,

    /// Logical Operator Info
    op_info: String,

    // monitor
    /// attributes of the OpenTelemetry monitor
    attributes: Vec<KeyValue>,
    source_output_row_count: Counter<u64>,
}

impl SourceExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_id: TableId,
        source_desc: SourceDesc,
        column_ids: Vec<ColumnId>,
        schema: Schema,
        pk_indices: PkIndices,
        barrier_receiver: UnboundedReceiver<Message>,
        executor_id: u64,
        operator_id: u64,
        op_info: String,
    ) -> Result<Self> {
        let source = source_desc.clone().source;
        let reader: Box<dyn StreamSourceReader> = match source.as_ref() {
            SourceImpl::HighLevelKafka(s) => Box::new(s.stream_reader(
                HighLevelKafkaSourceReaderContext {
                    query_id: Some(format!("source-operator-{}", operator_id)),
                    bound_timestamp_ms: None,
                },
                column_ids.clone(),
            )?),
            SourceImpl::TableV2(s) => {
                Box::new(s.stream_reader(TableV2ReaderContext, column_ids.clone())?)
            }
        };
        let source_identify = "Table_".to_string() + &source_id.table_id().to_string();
        let meter = DEFAULT_COMPUTE_STATS
            .clone()
            .prometheus_exporter
            .provider()
            .unwrap()
            .meter("stream_source_monitor", None);
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
            identity: format!("SourceExecutor {:X}", executor_id),
            attributes: vec![KeyValue::new("source_id", source_identify)],
            source_output_row_count: meter
                .u64_counter("stream_source_output_rows_counts")
                .with_description("")
                .init(),
            op_info,
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
impl Executor for SourceExecutor {
    async fn next(&mut self) -> Result<Message> {
        if self.first_execution {
            self.reader.open().await?;
            self.first_execution = false;
        }
        // FIXME: may lose message
        tokio::select! {
          biased; // to ensure `FLUSH` run after any `INSERT`.

          chunk = self.reader.next() => {
            let mut chunk = chunk?;

            // Refill row id only if not a table source.
            // Note(eric): Currently, rows from external sources are filled with row_ids here,
            // but rows from tables (by insert statements) are filled in InsertExecutor.
            //
            // TODO: in the future, we may add row_id column here for TableV2 as well
            if !matches!(self.source_desc.source.as_ref(), SourceImpl::TableV2(_)) {
              chunk = self.refill_row_id_column(chunk);
            }
            self.source_output_row_count.add(chunk.cardinality() as u64, &self.attributes);
            Ok(Message::Chunk(chunk))
          }
          message = self.barrier_receiver.recv() => {
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

    fn logical_operator_info(&self) -> &str {
        &self.op_info
    }
}

impl Debug for SourceExecutor {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceExecutor")
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

    use itertools::Itertools;
    use risingwave_common::array::column::Column;
    use risingwave_common::array::{ArrayImpl, I32Array, I64Array, Op, StreamChunk, Utf8Array};
    use risingwave_common::array_nonnull;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::types::DataType;
    use risingwave_source::*;
    use risingwave_storage::table::test::TestTable;
    use risingwave_storage::TableColumnDesc;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;
    use crate::executor::{Barrier, Epoch, Mutation, SourceExecutor};

    #[tokio::test]
    async fn test_table_source() -> Result<()> {
        let table_id = TableId::default();

        let rowid_type = DataType::Int64;
        let col1_type = DataType::Int32;
        let col2_type = DataType::Varchar;

        let table_columns = vec![
            TableColumnDesc {
                column_id: ColumnId::from(0),
                data_type: rowid_type.clone(),
                name: String::new(),
            },
            TableColumnDesc {
                column_id: ColumnId::from(1),
                data_type: col1_type.clone(),
                name: String::new(),
            },
            TableColumnDesc {
                column_id: ColumnId::from(2),
                data_type: col2_type.clone(),
                name: String::new(),
            },
        ];
        let table = Arc::new(TestTable::new(&table_id, table_columns));

        let source_manager = MemSourceManager::new();
        source_manager.create_table_source_v2(&table_id, table.clone())?;
        let source_desc = source_manager.get_source(&table_id)?;
        let source = source_desc.clone().source;
        let table_source = source.as_table_v2();

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
                Field::unnamed(rowid_type),
                Field::unnamed(col1_type),
                Field::unnamed(col2_type),
            ],
        };

        let column_ids = vec![0, 1, 2].into_iter().map(ColumnId::from).collect();
        let pk_indices = vec![0];

        let (barrier_sender, barrier_receiver) = unbounded_channel();

        let mut source = SourceExecutor::new(
            table_id,
            source_desc,
            column_ids,
            schema,
            pk_indices,
            barrier_receiver,
            1,
            1,
            "SourceExecutor".to_string(),
        )
        .unwrap();

        barrier_sender
            .send(Message::Barrier(Barrier {
                epoch: Epoch::new_test_epoch(1),
                ..Barrier::default()
            }))
            .unwrap();

        // Write 1st chunk
        table_source.write_chunk(chunk1).await?;

        for _ in 0..2 {
            match source.next().await.unwrap() {
                Message::Chunk(chunk) => {
                    assert_eq!(3, chunk.columns().len());
                    assert_eq!(
                        col1_arr1.iter().collect_vec(),
                        chunk.column_at(1).array_ref().iter().collect_vec(),
                    );
                    assert_eq!(
                        col2_arr1.iter().collect_vec(),
                        chunk.column_at(2).array_ref().iter().collect_vec()
                    );
                    assert_eq!(vec![Op::Insert; 3], chunk.ops());
                }
                Message::Barrier(barrier) => {
                    assert_eq!(barrier.epoch, Epoch::new_test_epoch(1))
                }
            }
        }

        // Write 2nd chunk
        table_source.write_chunk(chunk2).await?;

        if let Message::Chunk(chunk) = source.next().await.unwrap() {
            assert_eq!(3, chunk.columns().len());
            assert_eq!(
                col1_arr2.iter().collect_vec(),
                chunk.column_at(1).array_ref().iter().collect_vec()
            );
            assert_eq!(
                col2_arr2.iter().collect_vec(),
                chunk.column_at(2).array_ref().iter().collect_vec()
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

        let rowid_type = DataType::Int64;
        let col1_type = DataType::Int32;
        let col2_type = DataType::Varchar;

        let table_columns = vec![
            TableColumnDesc {
                column_id: ColumnId::from(0),
                data_type: rowid_type.clone(),
                name: String::new(),
            },
            TableColumnDesc {
                column_id: ColumnId::from(1),
                data_type: col1_type.clone(),
                name: String::new(),
            },
            TableColumnDesc {
                column_id: ColumnId::from(2),
                data_type: col2_type.clone(),
                name: String::new(),
            },
        ];
        let table = Arc::new(TestTable::new(&table_id, table_columns));

        let source_manager = MemSourceManager::new();
        source_manager.create_table_source_v2(&table_id, table.clone())?;
        let source_desc = source_manager.get_source(&table_id)?;
        let source = source_desc.clone().source;
        let table_source = source.as_table_v2();

        // Prepare test data chunks
        let rowid_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I64Array, [0, 0, 0] }.into());
        let col1_arr1: Arc<ArrayImpl> = Arc::new(array_nonnull! { I32Array, [1, 2, 3] }.into());
        let col2_arr1: Arc<ArrayImpl> =
            Arc::new(array_nonnull! { Utf8Array, ["foo", "bar", "baz"] }.into());

        let chunk = {
            let rowid = Column::new(rowid_arr1.clone());
            let col1 = Column::new(col1_arr1.clone());
            let col2 = Column::new(col2_arr1.clone());
            let vis = vec![Op::Insert, Op::Insert, Op::Insert];
            StreamChunk::new(vis, vec![rowid, col1, col2], None)
        };

        let schema = Schema {
            fields: vec![
                Field::unnamed(rowid_type),
                Field::unnamed(col1_type),
                Field::unnamed(col2_type),
            ],
        };

        let column_ids = vec![0.into(), 1.into(), 2.into()];
        let pk_indices = vec![0];

        let (barrier_sender, barrier_receiver) = unbounded_channel();
        let mut source = SourceExecutor::new(
            table_id,
            source_desc,
            column_ids,
            schema,
            pk_indices,
            barrier_receiver,
            1,
            1,
            "SourceExecutor".to_string(),
        )
        .unwrap();

        table_source.write_chunk(chunk.clone()).await?;

        barrier_sender
            .send(Message::Barrier(
                Barrier::new_test_barrier(1).with_mutation(Mutation::Stop(HashSet::default())),
            ))
            .unwrap();

        source.next().await.unwrap();
        source.next().await.unwrap();
        table_source.write_chunk(chunk).await?;

        Ok(())
    }
}
