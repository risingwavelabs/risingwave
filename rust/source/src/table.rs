use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_common::array::{DataChunk, DataChunkRef, Op, StreamChunk};
use risingwave_common::error::Result;
use risingwave_storage::bummock::{BummockResult, BummockTable};
use risingwave_storage::table::ScannableTable;
use risingwave_storage::Table;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, RwLock};

use crate::*;

/// `TableSource` is a special internal source to handle table updates from user,
/// including insert/delete/update statements via SQL interface.
///
/// Changed rows are first staged here and will be streamed into a materialized
/// view to construct the table and zero or more downstreams
#[derive(Debug)]
pub struct TableSource {
    core: Arc<RwLock<TableSourceCore>>,

    /// All columns in this table
    columns: Vec<SourceColumnDesc>,

    /// current allocated row id
    next_row_id: AtomicUsize,
}

#[derive(Debug)]
struct TableSourceCore {
    /// The table to be updated
    table: Arc<BummockTable>,

    /// Unflushed rows
    buffer: Vec<StreamChunk>,

    /// Channel used to broadcast changes into downstream streaming operators
    write_channel: broadcast::Sender<StreamChunk>,
}

impl TableSource {
    /// Capacity of broadcast channel.
    const CHANNEL_CAPACITY: usize = 1000;

    pub fn new(table: Arc<BummockTable>) -> Self {
        // TODO: this snippet is redundant
        let source_columns = table.columns().iter().map(SourceColumnDesc::from).collect();

        let (tx, _rx) = broadcast::channel(TableSource::CHANNEL_CAPACITY);
        let core = TableSourceCore {
            table,
            buffer: vec![],
            write_channel: tx,
        };
        TableSource {
            core: Arc::new(RwLock::new(core)),
            columns: source_columns,
            next_row_id: AtomicUsize::new(0),
        }
    }

    pub fn next_row_id(&self) -> usize {
        self.next_row_id.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Debug)]
pub struct TableReaderContext {}

/// `TableBatchReader` reads a latest snapshot from `BummockTable`
#[derive(Debug)]
pub struct TableBatchReader {
    core: Arc<RwLock<TableSourceCore>>,
    column_ids: Vec<i32>,
    snapshot: VecDeque<DataChunkRef>,
}

#[async_trait]
impl BatchSourceReader for TableBatchReader {
    async fn open(&mut self) -> Result<()> {
        let core = self.core.read().await;
        match core.table.get_data_by_columns(&self.column_ids).await? {
            BummockResult::Data(snapshot) => {
                self.snapshot = VecDeque::from(snapshot);
                Ok(())
            }
            BummockResult::DataEof => {
                // Table is empty
                Ok(())
            }
        }
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        if let Some(chunk) = self.snapshot.pop_front() {
            Ok(Some(chunk.deref().clone()))
        } else {
            Ok(None)
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.snapshot.clear();
        Ok(())
    }
}

/// `TableSourceReader` reads changes from a certain table continuously
#[derive(Debug)]
pub struct TableStreamReader {
    core: Arc<RwLock<TableSourceCore>>,

    /// Mappings from the source column to the column to be read
    column_indexes: Vec<usize>,

    /// `existing_data` contains data from the last snapshot and unflushed data
    /// at the time the snapshot created
    existing_data: VecDeque<StreamChunk>,

    read_channel: Option<broadcast::Receiver<StreamChunk>>,
}

#[async_trait]
impl StreamSourceReader for TableStreamReader {
    async fn open(&mut self) -> Result<()> {
        let core = self.core.read().await;
        let column_ids = core.table.get_column_ids();
        let snapshot = match core.table.get_data_by_columns(&column_ids).await? {
            BummockResult::Data(snapshot) => snapshot,
            BummockResult::DataEof => vec![],
        };

        for chunk in snapshot.into_iter() {
            let stream_chunk = StreamChunk::new(
                vec![Op::Insert; chunk.capacity()],
                chunk.columns().to_vec(),
                chunk.visibility().clone(),
            );
            self.existing_data.push_back(stream_chunk);
        }

        for stream_chunk in core.buffer.iter() {
            self.existing_data.push_back(stream_chunk.clone())
        }

        self.read_channel = Some(core.write_channel.subscribe());
        Ok(())
    }

    async fn next(&mut self) -> Result<StreamChunk> {
        // Pop data from snapshot if exists
        let chunk = if let Some(chunk) = self.existing_data.pop_front() {
            chunk
        } else {
            // Otherwise, read from the writing channel
            match self.read_channel.as_mut().unwrap().recv().await {
                Ok(chunk) => chunk,
                Err(err) => {
                    if let RecvError::Closed = err {
                        panic!("TableSource dropped before streaming task terminated")
                    } else {
                        unreachable!();
                    }
                }
            }
        };

        let selected_columns = self
            .column_indexes
            .iter()
            .map(|i| chunk.columns()[*i].clone())
            .collect();
        Ok(StreamChunk::new(
            chunk.ops().to_vec(),
            selected_columns,
            chunk.visibility().clone(),
        ))
    }
}

#[async_trait]
impl Source for TableSource {
    type ReaderContext = TableReaderContext;
    type BatchReader = TableBatchReader;
    type StreamReader = TableStreamReader;
    type Writer = TableWriter;

    // Note(eric): Currently, the `seq_scan_executor` scans from table directly,
    // and does not use the `TableBatchReader` here. Not sure whether it's better.
    fn batch_reader(
        &self,
        _context: Self::ReaderContext,
        column_ids: Vec<i32>,
    ) -> Result<Self::BatchReader> {
        Ok(TableBatchReader {
            core: self.core.clone(),
            snapshot: VecDeque::new(),
            column_ids,
        })
    }

    fn stream_reader(
        &self,
        _context: Self::ReaderContext,
        column_ids: Vec<i32>,
    ) -> Result<Self::StreamReader> {
        let mut column_indexes = vec![];
        for column_id in column_ids {
            let idx = self
                .columns
                .iter()
                .position(|c| c.column_id == column_id)
                .expect("column id not exists");
            column_indexes.push(idx);
        }

        Ok(TableStreamReader {
            core: self.core.clone(),
            existing_data: VecDeque::new(),
            read_channel: None,
            column_indexes,
        })
    }

    fn create_writer(&self) -> Result<Self::Writer> {
        Ok(TableWriter {
            core: self.core.clone(),
        })
    }
}

/// `TableWriter` is for writing data into table.
pub struct TableWriter {
    core: Arc<RwLock<TableSourceCore>>,
}

#[async_trait]
impl SourceWriter for TableWriter {
    async fn write(&mut self, chunk: StreamChunk) -> Result<()> {
        let mut core = self.core.write().await;
        core.buffer.push(chunk.clone());
        // Ignore SendError caused by no receivers
        core.write_channel.send(chunk).ok();
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        let mut core = self.core.write().await;
        let write_batch = std::mem::take(&mut core.buffer);
        for stream_chunk in write_batch {
            core.table.write(&stream_chunk)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use itertools::Itertools;
    use risingwave_common::array::{Array, I64Array, Op};
    use risingwave_common::catalog::TableId;
    use risingwave_common::column_nonnull;
    use risingwave_common::types::Int64Type;
    use risingwave_storage::{Table, TableColumnDesc};

    use super::*;

    #[tokio::test]
    async fn test_table_source() {
        let table = Arc::new(BummockTable::new(
            &TableId::default(),
            vec![TableColumnDesc::new_for_test::<Int64Type>(0)],
        ));

        // Some existing data
        let chunk0 = StreamChunk::new(vec![Op::Insert], vec![column_nonnull!(I64Array, [0])], None);
        table.write(&chunk0).unwrap();

        let source = TableSource::new(table.clone());

        let mut reader1 = source
            .stream_reader(TableReaderContext {}, vec![0])
            .unwrap();
        reader1.open().await.unwrap();

        // insert chunk 1
        let chunk1 = StreamChunk::new(vec![Op::Insert], vec![column_nonnull!(I64Array, [1])], None);

        let mut writer = source.create_writer().unwrap();
        writer.write(chunk1).await.unwrap();
        writer.flush().await.unwrap();

        // reader 1 sees chunk 0,1
        assert_matches!(reader1.next().await.unwrap(), chunk => {
          assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some(0)]);
        });

        assert_matches!(reader1.next().await.unwrap(), chunk => {
          assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some(1)]);
        });

        let mut reader2 = source
            .stream_reader(TableReaderContext {}, vec![0])
            .unwrap();
        reader2.open().await.unwrap();

        // insert chunk 2
        let chunk2 = StreamChunk::new(vec![Op::Insert], vec![column_nonnull!(I64Array, [2])], None);

        let mut writer = source.create_writer().unwrap();
        writer.write(chunk2).await.unwrap();

        // reader 1 sees chunk 2 (even before flush)
        assert_matches!(reader1.next().await.unwrap(), chunk => {
          assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some(2)]);
        });

        writer.flush().await.unwrap();

        // reader 2 sees chunk 0,1,2
        for i in 0..=2 {
            assert_matches!(reader2.next().await.unwrap(), chunk => {
              assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some(i)]);
            });
        }

        let mut batch_reader = source.batch_reader(TableReaderContext {}, vec![0]).unwrap();
        batch_reader.open().await.unwrap();
        for i in 0..=2 {
            assert_matches!(batch_reader.next().await.unwrap(), Some(chunk) => {
              assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some(i)]);
            });
        }
        assert_matches!(batch_reader.next().await.unwrap(), None);
    }
}
