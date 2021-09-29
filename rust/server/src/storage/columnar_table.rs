use crate::array2::{DataChunk, DataChunkRef};
use crate::catalog::TableId;
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::stream_op::StreamChunk;
use futures::channel::mpsc;
use futures::SinkExt;
use std::sync::{Arc, RwLock};

struct MemTableInner {
    data: Vec<DataChunkRef>,
    column_ids: Arc<Vec<i32>>,
    stream_sender: Option<mpsc::UnboundedSender<StreamChunk>>,
}

pub struct MemColumnarTable {
    table_id: TableId,
    inner: RwLock<MemTableInner>,
}

impl MemTableInner {
    fn new(column_count: usize) -> Self {
        Self {
            data: Vec::new(),
            column_ids: Arc::new((0..column_count).map(|x| x as i32).collect()),
            stream_sender: None,
        }
    }
}

impl MemColumnarTable {
    pub fn new(table_id: &TableId, column_count: usize) -> Self {
        Self {
            table_id: table_id.clone(),
            inner: RwLock::new(MemTableInner::new(column_count)),
        }
    }

    pub fn append(&self, data: DataChunk) -> Result<usize> {
        let mut write_guard = self.inner.write().unwrap();

        if let Some(ref mut sender) = write_guard.stream_sender {
            use crate::stream_op::Op;
            let chunk = StreamChunk::new(
                vec![Op::Insert; data.cardinality()],
                Vec::from(data.columns()),
                data.visibility().clone(),
            );
            futures::executor::block_on(async move { sender.send(chunk).await })
                .expect("send changes failed");
        }

        let cardinality = data.cardinality();
        write_guard.data.push(Arc::new(data));
        Ok(cardinality)
    }

    pub fn create_stream(&self) -> Result<mpsc::UnboundedReceiver<StreamChunk>> {
        let mut guard = self.inner.write().unwrap();
        ensure!(
            guard.stream_sender.is_none(),
            "stream of table {:?} exists",
            self.table_id
        );
        let (tx, rx) = mpsc::unbounded();
        let _ = guard.stream_sender.insert(tx);
        Ok(rx)
    }

    pub fn get_data(&self) -> Result<Vec<DataChunkRef>> {
        let table = self.inner.read().unwrap();
        Ok(table.data.clone())
    }

    pub fn get_column_ids(&self) -> Result<Arc<Vec<i32>>> {
        let table = self.inner.read().unwrap();
        Ok(table.column_ids.clone())
    }

    pub fn index_of_column_id(&self, column_id: i32) -> Result<usize> {
        let table = self.inner.read().unwrap();
        if let Some(p) = table.column_ids.iter().position(|c| *c == column_id) {
            Ok(p)
        } else {
            Err(RwError::from(InternalError(format!(
                "column id {:?} not found in table {:?}",
                column_id, self.table_id
            ))))
        }
    }
}
