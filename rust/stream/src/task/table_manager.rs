use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use risingwave_common::array::InternalError;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::{ensure, gen_error};
use risingwave_pb::plan::ColumnDesc;
use risingwave_storage::bummock::BummockTable;
use risingwave_storage::table::{ScannableTableRef, TableManager};
use risingwave_storage::{Keyspace, TableColumnDesc};

use crate::dispatch_state_store;
use crate::executor::MViewTable;
use crate::task::StateStoreImpl;

#[async_trait::async_trait]
/// `TableManager` is an abstraction of managing a collection of tables.
/// The interface between executors and storage should be table-oriented.
/// `Database` is a logical concept and stored as metadata information.
pub trait StreamTableManager: TableManager {
    /// Create materialized view.
    fn create_materialized_view(
        &self,
        table_id: &TableId,
        columns: &[ColumnDesc],
        pk_columns: Vec<usize>,
        orderings: Vec<OrderType>,
        state_store: StateStoreImpl,
    ) -> Result<()>;
}

/// A simple implementation of in memory table for local tests.
/// It will be replaced in near future when replaced by locally
/// on-disk files.
#[derive(Default)]
pub struct SimpleTableManager {
    tables: Mutex<HashMap<TableId, ScannableTableRef>>,
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
        Ok(table)
    }

    fn get_table(&self, table_id: &TableId) -> Result<ScannableTableRef> {
        let tables = self.get_tables()?;
        tables
            .get(table_id)
            .cloned()
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

#[async_trait::async_trait]
impl StreamTableManager for SimpleTableManager {
    fn create_materialized_view(
        &self,
        table_id: &TableId,
        columns: &[ColumnDesc],
        pk_columns: Vec<usize>,
        orderings: Vec<OrderType>,
        state_store: StateStoreImpl,
    ) -> Result<()> {
        let mut tables = self.get_tables()?;
        ensure!(
            !tables.contains_key(table_id),
            "Table id already exists: {:?}",
            table_id
        );
        let column_count = columns.len();
        ensure!(column_count > 0, "There must be more than one column in MV");
        let schema = Schema::try_from(columns)?;

        let table: ScannableTableRef = dispatch_state_store!(state_store, store, {
            Arc::new(MViewTable::new(
                Keyspace::table_root(store, table_id),
                schema,
                pk_columns,
                orderings,
            ))
        });

        tables.insert(table_id.clone(), table);
        Ok(())
    }
}

impl SimpleTableManager {
    pub fn new() -> Self {
        SimpleTableManager {
            tables: Mutex::new(HashMap::new()),
        }
    }

    fn get_tables(&self) -> Result<MutexGuard<HashMap<TableId, ScannableTableRef>>> {
        Ok(self.tables.lock().unwrap())
    }
}

pub type StreamTableManagerRef = Arc<dyn StreamTableManager>;
