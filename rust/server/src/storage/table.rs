use crate::array::{DataChunk, DataChunkRef};
use crate::catalog::TableId;
use crate::error::ErrorCode::InternalError;
use crate::error::{ErrorCode, Result, RwError};
use std::sync::{Arc, RwLock, RwLockReadGuard};

struct MemTableInner {
    data: Vec<DataChunkRef>,
    column_ids: Arc<Vec<i32>>,
}

pub(crate) struct MemTable {
    table_id: TableId,
    inner: RwLock<MemTableInner>,
}

pub(crate) type TableRef = Arc<MemTable>;

impl MemTableInner {
    fn new(column_count: usize) -> Self {
        Self {
            data: Vec::new(),
            column_ids: Arc::new((0..column_count).map(|x| x as i32).collect()),
        }
    }
}

impl MemTable {
    pub(crate) fn new(table_id: &TableId, column_count: usize) -> TableRef {
        Arc::new(Self {
            table_id: table_id.clone(),
            inner: RwLock::new(MemTableInner::new(column_count)),
        })
    }

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
        self.get_reader().map(|r| r.data.clone())
    }

    pub(crate) fn get_column_ids(&self) -> Result<Arc<Vec<i32>>> {
        self.get_reader().map(|r| r.column_ids.clone())
    }

    pub(crate) fn index_of_column_id(&self, column_id: i32) -> Result<usize> {
        self.get_reader()
            .map(|r| r.column_ids.iter().position(|c| *c == column_id))
            .and_then(|pos| {
                pos.ok_or_else(|| {
                    InternalError(format!(
                        "column id {:?} not found in table {:?}",
                        column_id, self.table_id
                    ))
                    .into()
                })
            })
    }

    fn get_reader(&self) -> Result<RwLockReadGuard<MemTableInner>> {
        self.inner.read().map_err(|e| {
            RwError::from(ErrorCode::InternalError(format!(
                "failed to acquire read lock for table {:?}, reason: {}",
                self.table_id, e
            )))
        })
    }
}
