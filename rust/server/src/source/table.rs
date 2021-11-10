use crate::source::{Source, SourceMessage, SourceReader};
use crate::storage::{SimpleMemTable, Table};
use crate::stream_op::{Op, StreamChunk};
use async_trait::async_trait;
use risingwave_common::array::DataChunkRef;
use risingwave_common::error::Result;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;

/// `TableSource` is a special internal source to handle table updates from user,
/// including insert/delete/update statements via SQL interface.
///
/// Changed rows are first staged here and will be streamed into a materialized
/// view to construct the table and zero or more downstreams
#[derive(Debug)]
pub struct TableSource {
    table: Arc<SimpleMemTable>,

    // Use broadcast channel here to support multiple receivers
    write_channel: broadcast::Sender<StreamChunk>,
}

impl TableSource {
    pub fn new(table: Arc<SimpleMemTable>) -> Self {
        let (tx, _rx) = broadcast::channel(100);
        TableSource {
            table,
            write_channel: tx,
        }
    }

    pub fn writer(&self) -> TableWriter {
        let tx = self.write_channel.clone();
        TableWriter {
            table: self.table.clone(),
            tx,
        }
    }
}

impl Source for TableSource {
    fn reader(&self) -> Result<Box<dyn SourceReader>> {
        let rx = self.write_channel.subscribe();
        Ok(Box::new(TableSourceReader {
            table: self.table.clone(),
            snapshot: vec![],
            rx,
        }))
    }
}

/// `TableWriter` is for writing data into table.
pub struct TableWriter {
    table: Arc<SimpleMemTable>,
    tx: broadcast::Sender<StreamChunk>,
}

impl TableWriter {
    async fn write(&mut self, chunk: StreamChunk) -> Result<()> {
        self.table.write(&chunk)?;

        // Ignore SendError caused by no receivers
        self.tx.send(chunk).ok();

        Ok(())
    }
}

/// `TableSourceReader` reads changes from a certain table. Note that this is
/// not for OLAP scanning, but for the materialized views and other downstream
/// operators.
#[derive(Debug)]
pub struct TableSourceReader {
    table: Arc<SimpleMemTable>,
    snapshot: Vec<DataChunkRef>,
    rx: broadcast::Receiver<StreamChunk>,
}

#[async_trait]
impl SourceReader for TableSourceReader {
    async fn init(&mut self) -> Result<()> {
        // In the completed design, should wait until first barrier to get the snapshot
        let mut snapshot = self.table.get_data().await?;
        snapshot.reverse(); // first element should be popped first
        self.snapshot = snapshot;
        Ok(())
    }

    async fn poll_message(&mut self) -> Result<Option<SourceMessage>> {
        // TODO(eric): should we do TableScan here to keep consistent with other sources?
        todo!()
    }

    async fn next_message(&mut self) -> Result<SourceMessage> {
        if let Some(chunk) = self.snapshot.pop() {
            let stream_chunk = StreamChunk::new(
                vec![Op::Insert; chunk.capacity()],
                chunk.columns().to_vec(),
                chunk.visibility().clone(),
            );
            return Ok(SourceMessage::Raw(stream_chunk));
        }

        match self.rx.recv().await {
            Ok(chunk) => Ok(SourceMessage::Raw(chunk)),
            Err(err) => {
                if let RecvError::Closed = err {
                    panic!("TableSource dropped before streaming task terminated")
                } else {
                    unreachable!();
                }
            }
        }
    }

    async fn cancel(&mut self) -> Result<()> {
        // do nothing
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::Table;
    use crate::stream_op::Op;
    use assert_matches::assert_matches;
    use itertools::Itertools;
    use risingwave_common::array::{Array, I64Array};
    use risingwave_common::catalog::test_utils::mock_table_id;
    use risingwave_common::column_nonnull;
    use risingwave_common::types::Int64Type;

    #[tokio::test]
    async fn test_table_source() {
        let table = Arc::new(SimpleMemTable::new(&mock_table_id(), &[]));

        // Some existing data
        let chunk0 = StreamChunk::new(
            vec![Op::Insert],
            vec![column_nonnull!(I64Array, Int64Type, [0])],
            None,
        );
        table.write(&chunk0).unwrap();

        let source = TableSource::new(table.clone());

        let mut reader1 = source.reader().unwrap();
        reader1.init().await.unwrap();

        // insert chunk 1
        let chunk1 = StreamChunk::new(
            vec![Op::Insert],
            vec![column_nonnull!(I64Array, Int64Type, [1])],
            None,
        );
        source.writer().write(chunk1).await.unwrap();

        // reader 1 sees chunk 0,1
        assert_matches!(reader1.next_message().await.unwrap(), SourceMessage::Raw(chunk) => {
          assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some(0)]);
        });
        assert_matches!(reader1.next_message().await.unwrap(), SourceMessage::Raw(chunk) => {
          assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some(1)]);
        });

        let mut reader2 = source.reader().unwrap();
        reader2.init().await.unwrap();

        // insert chunk 2
        let chunk2 = StreamChunk::new(
            vec![Op::Insert],
            vec![column_nonnull!(I64Array, Int64Type, [2])],
            None,
        );
        source.writer().write(chunk2).await.unwrap();

        // reader 1 sees chunk 2
        assert_matches!(reader1.next_message().await.unwrap(), SourceMessage::Raw(chunk) => {
          assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some(2)]);
        });

        // reader 2 sees chunk 0,1,2
        for i in 0..=2 {
            assert_matches!(reader2.next_message().await.unwrap(), SourceMessage::Raw(chunk) => {
              assert_eq!(chunk.columns()[0].array_ref().as_int64().iter().collect_vec(), vec![Some(i)]);
            });
        }
    }
}
