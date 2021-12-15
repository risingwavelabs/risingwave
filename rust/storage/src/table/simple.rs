use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use risingwave_common::array::InternalError;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_common::{ensure, gen_error};

use super::{ScannableTableRef, TableManager};
use crate::bummock::BummockTable;
use crate::TableColumnDesc;

#[derive(Default)]
pub struct SimpleTableManager {
    tables: Mutex<HashMap<TableId, Arc<BummockTable>>>,
}

impl AsRef<dyn Any> for SimpleTableManager {
    fn as_ref(&self) -> &dyn Any {
        self as &dyn Any
    }
}

#[async_trait::async_trait]
impl TableManager for SimpleTableManager {
    async fn create_table(
        &self,
        table_id: &TableId,
        table_columns: Vec<TableColumnDesc>,
    ) -> Result<ScannableTableRef> {
        let mut tables = self.get_tables()?;

        ensure!(
            !tables.contains_key(table_id),
            "Table id already exists: {:?}",
            table_id
        );

        let column_count = table_columns.len();
        ensure!(
            column_count > 0,
            "column count must be positive: {}",
            column_count
        );
        let table = Arc::new(BummockTable::new(table_id, table_columns));
        tables.insert(table_id.clone(), table.clone());
        Ok(table as ScannableTableRef)
    }

    fn get_table(&self, table_id: &TableId) -> Result<ScannableTableRef> {
        let tables = self.get_tables()?;
        tables
            .get(table_id)
            .cloned()
            .map(|t| t as ScannableTableRef)
            .ok_or_else(|| InternalError(format!("Table id not exists: {:?}", table_id)).into())
    }

    async fn drop_table(&self, table_id: &TableId) -> Result<()> {
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

impl SimpleTableManager {
    pub fn new() -> Self {
        SimpleTableManager {
            tables: Mutex::new(HashMap::new()),
        }
    }

    fn get_tables(&self) -> Result<MutexGuard<HashMap<TableId, Arc<BummockTable>>>> {
        Ok(self.tables.lock().unwrap())
    }
}
