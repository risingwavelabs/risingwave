use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use risingwave_common::array::InternalError;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_common::{ensure, gen_error};

use super::{ScannableTableRef, TableManager};
use crate::bummock::BummockTable;
use crate::table::mview::MViewTable;
use crate::{dispatch_state_store, Keyspace, StateStoreImpl, TableColumnDesc};

/// A simple implementation of in memory table for local tests.
/// It will be replaced in near future when replaced by locally
/// on-disk files.
pub struct SimpleTableManager {
    tables: Mutex<HashMap<TableId, ScannableTableRef>>,

    /// Used for `TableV2`.
    state_store: StateStoreImpl,
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
        let mut tables = self.get_tables();

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

    async fn create_table_v2(
        &self,
        table_id: &TableId,
        table_columns: Vec<TableColumnDesc>,
    ) -> Result<ScannableTableRef> {
        let mut tables = self.get_tables();

        ensure!(
            !tables.contains_key(table_id),
            "Table id already exists: {:?}",
            table_id
        );

        let table = dispatch_state_store!(self.state_store(), store, {
            let keyspace = Keyspace::table_root(store, table_id);
            Arc::new(MViewTable::new_batch(keyspace, table_columns)) as ScannableTableRef
        });
        tables.insert(table_id.clone(), table.clone());

        Ok(table)
    }

    fn get_table(&self, table_id: &TableId) -> Result<ScannableTableRef> {
        let tables = self.get_tables();
        tables
            .get(table_id)
            .cloned()
            .ok_or_else(|| InternalError(format!("Table id not exists: {:?}", table_id)).into())
    }

    // TODO: the data in StateStore should also be dropped directly/through unpin or some other way.
    async fn drop_table(&self, table_id: &TableId) -> Result<()> {
        let mut tables = self.get_tables();
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
    pub fn new(state_store: StateStoreImpl) -> Self {
        Self {
            tables: Mutex::new(HashMap::new()),
            state_store,
        }
    }

    pub fn with_in_memory_store() -> Self {
        Self::new(StateStoreImpl::shared_in_memory_store())
    }

    pub fn get_tables(&self) -> MutexGuard<HashMap<TableId, ScannableTableRef>> {
        self.tables.lock().unwrap()
    }

    pub fn state_store(&self) -> StateStoreImpl {
        self.state_store.clone()
    }
}
