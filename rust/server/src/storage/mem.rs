use crate::array::{DataChunk, DataChunkRef};
use crate::catalog::TableId;
use crate::error::ErrorCode::InternalError;
use crate::error::{Result, RwError};
use crate::storage::*;
use crate::stream_op::StreamChunk;
use futures::channel::mpsc;
use futures::SinkExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

/// A simple implementation of in memory table for local tests.
/// It will be replaced in near future when replaced by locally
/// on-disk files.
pub struct SimpleTableManager {
    tables: Mutex<HashMap<TableId, SimpleTableRef>>,
}

/// The enumeration of supported simple tables in `SimpleTableManager`.
#[derive(Clone)]
pub enum SimpleTableRef {
    Columnar(Arc<SimpleMemTable>),
    Row(Arc<MemRowTable>),
}

impl TableManager for SimpleTableManager {
    fn create_table(&self, table_id: &TableId, column_count: usize) -> Result<()> {
        let mut tables = self.get_tables()?;

        ensure!(
            !tables.contains_key(table_id),
            "Table id already exists: {:?}",
            table_id
        );

        ensure!(
            column_count > 0,
            "column count must be positive: {}",
            column_count
        );
        tables.insert(
            table_id.clone(),
            SimpleTableRef::Columnar(Arc::new(SimpleMemTable::new(table_id, column_count))),
        );
        Ok(())
    }

    fn get_table(&self, table_id: &TableId) -> Result<SimpleTableRef> {
        let tables = self.get_tables()?;
        tables
            .get(table_id)
            .cloned()
            .ok_or_else(|| InternalError(format!("Table id not exists: {:?}", table_id)).into())
    }

    fn drop_table(&self, table_id: &TableId) -> Result<()> {
        let mut tables = self.get_tables()?;
        ensure!(
            tables.contains_key(table_id),
            "Table does not exist: {:?}",
            table_id
        );
        tables.remove(table_id);
        Ok(())
    }

    fn create_materialized_view(
        &self,
        table_id: &TableId,
        columns: Vec<ColumnDesc>,
        pk_columns: Vec<usize>,
    ) -> Result<()> {
        let mut tables = self.get_tables()?;

        ensure!(
            !tables.contains_key(table_id),
            "Table id already exists: {:?}",
            table_id
        );
        let column_count = columns.len();
        ensure!(column_count > 0, "There must be more than one column in MV");
        tables.insert(
            table_id.clone(),
            SimpleTableRef::Row(Arc::new(MemRowTable::new(columns, pk_columns))),
        );

        Ok(())
    }
}

impl SimpleTableManager {
    pub fn new() -> Self {
        SimpleTableManager {
            tables: Mutex::new(HashMap::new()),
        }
    }

    fn get_tables(&self) -> Result<MutexGuard<HashMap<TableId, SimpleTableRef>>> {
        Ok(self.tables.lock().unwrap())
    }
}

struct SimpleMemTableInner {
    data: Vec<DataChunkRef>,
    column_ids: Arc<Vec<i32>>,
    stream_sender: Option<mpsc::UnboundedSender<StreamChunk>>,
}

/// A simple in-memory table that organizes data in columnar format.
pub struct SimpleMemTable {
    table_id: TableId,
    inner: RwLock<SimpleMemTableInner>,
}

impl SimpleMemTableInner {
    fn new(column_count: usize) -> Self {
        Self {
            data: Vec::new(),
            column_ids: Arc::new((0..column_count as i32).collect()),
            stream_sender: None,
        }
    }
}

impl SimpleMemTable {
    pub fn new(table_id: &TableId, column_count: usize) -> Self {
        Self {
            table_id: table_id.clone(),
            inner: RwLock::new(SimpleMemTableInner::new(column_count)),
        }
    }
}

impl Table for SimpleMemTable {
    fn append(&self, data: DataChunk) -> Result<usize> {
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

    fn create_stream(&self) -> Result<mpsc::UnboundedReceiver<StreamChunk>> {
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

    fn get_data(&self) -> Result<Vec<DataChunkRef>> {
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
}
