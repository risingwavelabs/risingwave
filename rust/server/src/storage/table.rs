use crate::array::{DataChunk, DataChunkRef};
use crate::catalog::TableId;
use crate::error::{ErrorCode, Result, RwError};
use std::sync::{Arc, RwLock};

struct MemTableInner {
  data: Vec<DataChunkRef>,
}

pub(crate) struct MemTable {
  table_id: TableId,
  inner: RwLock<MemTableInner>,
}

impl MemTable {
  pub(crate) fn append(&self, data: DataChunk) -> Result<usize> {
    let mut write_guard = self.inner.write().map_err(|e| {
      RwError::from(ErrorCode::InternalError(format!(
        "failed to acquire write lock for table {:?}, reason: {}",
        self.table_id, e
      )))
    })?;
    let cardinality = data.cardinality();
    write_guard.data.push(Arc::new(data));
    Ok(cardinality)
  }

  pub(crate) fn get_data(&self) -> Result<Vec<DataChunkRef>> {
    let read_guard = self.inner.read().map_err(|e| {
      RwError::from(ErrorCode::InternalError(format!(
        "failed to acquire read lock for table {:?}, reason: {}",
        self.table_id, e
      )))
    })?;

    Ok(read_guard.data.clone())
  }
}
