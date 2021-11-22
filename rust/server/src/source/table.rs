use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

use risingwave_common::array::{DataChunk, DataChunkRef};
use risingwave_common::error::Result;

use crate::source::{BatchSourceReader, SourceImpl, SourceWriter, StreamSourceReader};

use crate::storage::{BummockResult, BummockTable, Table};
use crate::stream_op::{Op, StreamChunk};

/// `TableSource` is a special internal source to handle table updates from user,
/// including insert/delete/update statements via SQL interface.
///
/// Changed rows are first staged here and will be streamed into a materialized
/// view to construct the table and zero or more downstreams
#[derive(Debug)]
pub struct TableSource {
    table: Arc<BummockTable>,

    // Use broadcast channel here to support multiple receivers
    write_channel: broadcast::Sender<StreamChunk>,
}

#[derive(Debug)]
pub struct TableBatchReader {}

#[async_trait]
impl BatchSourceReader for TableBatchReader {
    async fn open(&mut self) -> Result<()> {
        todo!()
    }

    async fn next(&mut self) -> Result<Option<DataChunk>> {
        todo!()
    }

    async fn close(&mut self) -> Result<()> {
        todo!()
    }
}

#[derive(Debug)]
pub struct TableReaderContext {}

/// `TableSourceReader` reads changes from a certain table. Note that this is
/// not for OLAP scanning, but for the materialized views and other downstream
/// operators.
#[derive(Debug)]
pub struct TableStreamReader {
    table: Arc<BummockTable>,
    snapshot: Vec<DataChunkRef>,
    rx: broadcast::Receiver<StreamChunk>,
}

impl TableSource {
    pub fn new(table: Arc<BummockTable>) -> Self {
        let (tx, _rx) = broadcast::channel(100);
        TableSource {
            table,
            write_channel: tx,
        }
    }
}

impl SourceImpl for TableSource {
    type ReaderContext = TableReaderContext;
    type BatchReader = TableBatchReader;
    type StreamReader = TableStreamReader;
    type Writer = TableWriter;

    fn batch_reader(&self, _context: Self::ReaderContext) -> Result<Self::BatchReader> {
        todo!()
    }

    fn stream_reader(&self, _context: Self::ReaderContext) -> Result<Self::StreamReader> {
        let rx = self.write_channel.subscribe();
        Ok(TableStreamReader {
            table: self.table.clone(),
            snapshot: vec![],
            rx,
        })
    }

    fn create_writer(&self) -> Result<Self::Writer> {
        let tx = self.write_channel.clone();
        Ok(TableWriter {
            table: self.table.clone(),
            tx,
        })
    }
}

/// `TableWriter` is for writing data into table.
pub struct TableWriter {
    table: Arc<BummockTable>,
    tx: broadcast::Sender<StreamChunk>,
}

#[async_trait]
impl SourceWriter for TableWriter {
    async fn write(&mut self, chunk: &StreamChunk) -> Result<()> {
        self.table.write(chunk)?;

        // Ignore SendError caused by no receivers
        self.tx.send(chunk.clone()).ok();

        Ok(())
    }

    async fn flush(&mut self, _chunk: &StreamChunk) -> Result<()> {
        todo!()
    }
}

#[async_trait]
impl StreamSourceReader for TableStreamReader {
    async fn open(&mut self) -> Result<()> {
        // In the completed design, should wait until first barrier to get the snapshot
        match self.table.get_data().await? {
            BummockResult::Data(mut snapshot) => {
                snapshot.reverse(); // first element should be popped first
                self.snapshot = snapshot;
                Ok(())
            }
            BummockResult::DataEof => {
                // TODO, handle eof here
                Ok(())
            }
        }
    }

    async fn next(&mut self) -> Result<StreamChunk> {
        if let Some(chunk) = self.snapshot.pop() {
            let stream_chunk = StreamChunk::new(
                vec![Op::Insert; chunk.capacity()],
                chunk.columns().to_vec(),
                chunk.visibility().clone(),
            );
            return Ok(stream_chunk);
        }

        match self.rx.recv().await {
            Ok(chunk) => Ok(chunk),
            Err(err) => {
                if let RecvError::Closed = err {
                    panic!("TableSource dropped before streaming task terminated")
                } else {
                    unreachable!();
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use assert_matches::assert_matches;
    use itertools::Itertools;

    use risingwave_common::array::{Array, I64Array};
    use risingwave_common::catalog::test_utils::mock_table_id;
    use risingwave_common::catalog::Schema;
    use risingwave_common::column_nonnull;
    use risingwave_common::types::Int64Type;

    use crate::storage::Table;
    use crate::stream_op::Op;

    use super::*;

    #[tokio::test]
    async fn test_table_source() {
        let table = Arc::new(BummockTable::new(
            &mock_table_id(),
            &Schema { fields: vec![] },
        ));

        // Some existing data
        let chunk0 = StreamChunk::new(
            vec![Op::Insert],
            vec![column_nonnull!(I64Array, Int64Type, [0])],
            None,
        );
        table.write(&chunk0).unwrap();

        let source = TableSource::new(table.clone());

        let mut reader1 = source.stream_reader(TableReaderContext {}).unwrap();
        reader1.open().await.unwrap();

        // insert chunk 1
        let chunk1 = StreamChunk::new(
            vec![Op::Insert],
            vec![column_nonnull!(I64Array, Int64Type, [1])],
            None,
        );

        source
            .create_writer()
            .unwrap()
            .write(&chunk1)
            .await
            .unwrap();

        // reader 1 sees chunk 0,1
        assert_matches!(reader1.next().await.unwrap(), chunk => {
          assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some(0)]);
        });

        assert_matches!(reader1.next().await.unwrap(), chunk => {
          assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some(1)]);
        });

        let mut reader2 = source.stream_reader(TableReaderContext {}).unwrap();
        reader2.open().await.unwrap();

        // insert chunk 2
        let chunk2 = StreamChunk::new(
            vec![Op::Insert],
            vec![column_nonnull!(I64Array, Int64Type, [2])],
            None,
        );
        source
            .create_writer()
            .unwrap()
            .write(&chunk2)
            .await
            .unwrap();

        // reader 1 sees chunk 2
        assert_matches!(reader1.next().await.unwrap(), chunk => {
          assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some(2)]);
        });

        // reader 2 sees chunk 0,1,2
        for i in 0..=2 {
            assert_matches!(reader2.next().await.unwrap(), chunk => {
              assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some(i)]);
            });
        }
    }
}
