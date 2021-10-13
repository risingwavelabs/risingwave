use crate::catalog::TableId;
use crate::error::Result;
use crate::stream_op::{Message, StreamChunk, StreamOperator};
use async_trait::async_trait;
use futures::channel::mpsc::UnboundedReceiver;
use futures::StreamExt;
use std::fmt::{Debug, Formatter};

/// `TableDataSource` extracts changes from a Table
pub struct TableDataSource {
    table_id: TableId,
    receiver: UnboundedReceiver<StreamChunk>,
}

impl TableDataSource {
    pub fn new(table_id: TableId, receiver: UnboundedReceiver<StreamChunk>) -> Self {
        TableDataSource { table_id, receiver }
    }
}

#[async_trait]
impl StreamOperator for TableDataSource {
    async fn next(&mut self) -> Result<Message> {
        let received = self.receiver.next().await;
        if let Some(chunk) = received {
            Ok(Message::Chunk(chunk))
        } else {
            panic!("table stream closed unexpectedly");
        }
    }
}

impl Debug for TableDataSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableDataSource")
            .field("table_id", &self.table_id)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::array::column::Column;
    use crate::array::{ArrayImpl, DataChunk};
    use crate::array::{I32Array, UTF8Array};
    use crate::array_nonnull;
    use crate::catalog::test_utils::mock_table_id;
    use crate::storage::MemColumnarTable;
    use crate::stream_op::data_source::MockConsumer;
    use crate::stream_op::{Op, StreamConsumer};
    use crate::types::{DataTypeKind, DataTypeRef, Int32Type, StringType};
    use itertools::Itertools;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn test_table_source() -> Result<()> {
        let table_id = mock_table_id();
        let table = MemColumnarTable::new(&table_id, 2);

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

        let stream_recv = table.create_stream()?;
        let source = TableDataSource::new(table_id, stream_recv);
        let output_buf = Arc::new(Mutex::new(Vec::new()));
        let mut consumer = MockConsumer::new(Box::new(source), output_buf.clone());

        // Write 1st chunk
        let card = table.append(chunk1)?;
        assert_eq!(3, card);

        consumer.next().await?;
        {
            let chunks = output_buf.lock().unwrap();
            assert_eq!(1, chunks.len());
            let chunk = chunks.get(0).unwrap();
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

        // Write 2nd chunk
        let card = table.append(chunk2)?;
        assert_eq!(3, card);

        consumer.next().await?;
        {
            let chunks = output_buf.lock().unwrap();
            assert_eq!(2, chunks.len());
            let chunk = chunks.get(1).unwrap();
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
        }

        Ok(())
    }
}
