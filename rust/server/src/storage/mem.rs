use crate::storage::*;
use crate::stream_op::{Op, StreamChunk};
use async_trait::async_trait;
use futures::channel::mpsc;
use futures::SinkExt;
use risingwave_common::array::{DataChunk, DataChunkRef};
use risingwave_common::catalog::TableId;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_proto::plan::ColumnDesc;
use std::sync::{Arc, RwLock};

pub struct SimpleMemTableInner {
  data: Vec<DataChunkRef>,
  column_ids: Arc<Vec<i32>>,
  stream_sender: Option<mpsc::UnboundedSender<StreamChunk>>,
}

/// A simple in-memory table that organizes data in columnar format.
#[derive(Debug)]
pub struct BummockTable {
  columns: Vec<ColumnDesc>,
  table_id: TableId,
  inner: RwLock<SimpleMemTableInner>,
}

impl SimpleMemTableInner {
  pub fn new(column_count: usize) -> Self {
    Self {
      data: Vec::new(),
      column_ids: Arc::new((0..column_count as i32).collect()),
      stream_sender: None,
    }
  }

  fn is_stream_connected(&self) -> bool {
    self.stream_sender.is_some()
  }
}

impl BummockTable {
  pub fn new(table_id: &TableId, columns: &[ColumnDesc]) -> Self {
    Self {
      columns: columns.to_vec(),
      table_id: table_id.clone(),
      inner: RwLock::new(SimpleMemTableInner::new(columns.len())),
    }
  }

  pub fn columns(&self) -> &Vec<ColumnDesc> {
    &self.columns
  }
}

#[async_trait]
impl Table for BummockTable {
  async fn append(&mut self, data: DataChunk) -> Result<usize> {
    let mut write_guard = self.inner.write().unwrap();

    // TODO: remove this
    if let Some(ref mut sender) = write_guard.stream_sender {
      let chunk = StreamChunk::new(
        vec![Op::Insert; data.cardinality()],
        Vec::from(data.columns()),
        data.visibility().clone(),
      );
      futures::executor::block_on(async move { sender.send(chunk).await })
        .or_else(|x| {
          // Disconnection means the receiver is dropped. So the sender shouble be dropped here too.
          if x.is_disconnected() {
            write_guard.stream_sender = None;
            return Ok(());
          }
          Err(x)
        })
        .expect("send changes failed");
    }

    let cardinality = data.cardinality();
    write_guard.data.push(Arc::new(data));
    Ok(cardinality)
  }

  fn create_stream(&mut self) -> Result<mpsc::UnboundedReceiver<StreamChunk>> {
    let mut guard = self.inner.write().unwrap();
    ensure!(
      guard.stream_sender.is_none(),
      "stream of table {:?} exists",
      self.table_id
    );
    let (tx, rx) = mpsc::unbounded();
    guard.stream_sender = Some(tx);
    Ok(rx)
  }

  async fn get_data(&self) -> Result<Vec<DataChunkRef>> {
    let table = self.inner.read().unwrap();
    Ok(table.data.clone())
  }

  fn get_column_ids(&self) -> Result<Arc<Vec<i32>>> {
    let table = self.inner.read().unwrap();
    Ok(table.column_ids.clone())
  }

  fn index_of_column_id(&self, column_id: i32) -> Result<usize> {
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

  fn is_stream_connected(&self) -> bool {
    let read_guard = self.inner.read().unwrap();
    read_guard.is_stream_connected()
  }
}
