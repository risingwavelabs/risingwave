use crate::catalog::TableId;
use crate::error::ErrorCode::InternalError;
use crate::error::{ErrorCode, Result, RwError};
use crate::storage::table::{MemTable, TableRef};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

pub(crate) trait StorageManager: Sync + Send {
    fn create_table(&self, table_id: &TableId, column_count: usize) -> Result<()>;
    fn get_table(&self, table_id: &TableId) -> Result<TableRef>;
    fn drop_table(&self, table_id: &TableId) -> Result<()>;
}

pub(crate) type StorageManagerRef = Arc<dyn StorageManager>;

pub(crate) struct MemStorageManager {
    tables: Mutex<HashMap<TableId, TableRef>>,
}

impl StorageManager for MemStorageManager {
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
        tables.insert(table_id.clone(), MemTable::new(table_id, column_count));
        Ok(())
    }

    fn get_table(&self, table_id: &TableId) -> Result<TableRef> {
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
}

impl MemStorageManager {
    pub(crate) fn new() -> Self {
        MemStorageManager {
            tables: Mutex::new(HashMap::new()),
        }
    }

    fn get_tables(&self) -> Result<MutexGuard<HashMap<TableId, TableRef>>> {
        self.tables.lock().map_err(|e| {
            RwError::from(ErrorCode::InternalError(format!(
                "failed to acquire storage manager lock: {}",
                e
            )))
        })
    }
}
