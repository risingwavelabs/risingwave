use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard};

use risingwave_common::array::InternalError;
use risingwave_common::catalog::Schema;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_common::{ensure, gen_error};

use risingwave_pb::plan::ColumnDesc;

use risingwave_storage::bummock::BummockTable;
use risingwave_storage::TableColumnDesc;

use super::StateStoreImpl;
use crate::stream_op::{HummockStateStore, MViewTable, MemoryStateStore};

#[async_trait::async_trait]
/// `TableManager` is an abstraction of managing a collection of tables.
/// The interface between executors and storage should be table-oriented.
/// `Database` is a logical concept and stored as metadata information.
pub trait TableManager: Sync + Send {
    /// Create a specific table.
    async fn create_table(
        &self,
        table_id: &TableId,
        table_columns: Vec<TableColumnDesc>,
    ) -> Result<Arc<BummockTable>>;

    /// Get a specific table.
    fn get_table(&self, table_id: &TableId) -> Result<TableImpl>;

    /// Drop a specific table.
    async fn drop_table(&self, table_id: &TableId) -> Result<()>;

    /// Create materialized view.
    fn create_materialized_view(
        &self,
        table_id: &TableId,
        columns: &[ColumnDesc],
        pk_columns: Vec<usize>,
        state_store: StateStoreImpl,
    ) -> Result<Vec<u8>>;
}

/// The enumeration of supported simple tables in `SimpleTableManager`.
#[derive(Clone)]
pub enum TableImpl {
    Bummock(Arc<BummockTable>),
    MViewTable(Arc<MViewTable<HummockStateStore>>),
    TestMViewTable(Arc<MViewTable<MemoryStateStore>>),
}

impl TableImpl {
    pub fn as_memory(&self) -> Arc<MViewTable<MemoryStateStore>> {
        match self {
            Self::TestMViewTable(t) => t.clone(),
            _ => unreachable!(),
        }
    }
}

/// A simple implementation of in memory table for local tests.
/// It will be replaced in near future when replaced by locally
/// on-disk files.
#[derive(Default)]
pub struct SimpleTableManager {
    tables: Mutex<HashMap<TableId, TableImpl>>,
}

#[async_trait::async_trait]
impl TableManager for SimpleTableManager {
    async fn create_table(
        &self,
        table_id: &TableId,
        table_columns: Vec<TableColumnDesc>,
    ) -> Result<Arc<BummockTable>> {
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
        tables.insert(table_id.clone(), TableImpl::Bummock(table.clone()));
        Ok(table)
    }

    fn get_table(&self, table_id: &TableId) -> Result<TableImpl> {
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

    fn create_materialized_view(
        &self,
        table_id: &TableId,
        columns: &[ColumnDesc],
        pk_columns: Vec<usize>,
        state_store: StateStoreImpl,
    ) -> Result<Vec<u8>> {
        let mut tables = self.get_tables()?;
        ensure!(
            !tables.contains_key(table_id),
            "Table id already exists: {:?}",
            table_id
        );
        let column_count = columns.len();
        ensure!(column_count > 0, "There must be more than one column in MV");
        let schema = Schema::try_from(columns)?;
        // TODO(MrCroxx): prefix rule, ref #1801 .
        let prefix: Vec<u8> = format!("mview-{:?}", table_id).into();
        // TODO(MrCroxx): use HummockStateStore and MViewTable if not test
        tables.insert(
            table_id.clone(),
            TableImpl::TestMViewTable(Arc::new(MViewTable::new(
                prefix.clone(),
                schema,
                pk_columns,
                match state_store {
                    StateStoreImpl::MemoryStateStore(s) => s,
                    _ => unreachable!(),
                },
            ))),
        );
        Ok(prefix)
    }
}

impl SimpleTableManager {
    pub fn new() -> Self {
        SimpleTableManager {
            tables: Mutex::new(HashMap::new()),
        }
    }

    fn get_tables(&self) -> Result<MutexGuard<HashMap<TableId, TableImpl>>> {
        Ok(self.tables.lock().unwrap())
    }
}

/// Reference of a `TableManager`.
pub type TableManagerRef = Arc<dyn TableManager>;
