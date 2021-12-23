use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use risingwave_common::array::InternalError;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
use risingwave_common::util::sort_util::OrderType;
use risingwave_common::{ensure, gen_error};
use risingwave_pb::plan::ColumnDesc;
use risingwave_storage::bummock::{BummockResult, BummockTable};
use risingwave_storage::hummock::HummockStateStore;
use risingwave_storage::memory::MemoryStateStore;
use risingwave_storage::table::{ScannableTable, ScannableTableRef, TableIterRef, TableManager};
use risingwave_storage::{Keyspace, TableColumnDesc};

use super::StateStoreImpl;
use crate::stream_op::MViewTable;

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

    pub fn as_bummock(&self) -> Arc<BummockTable> {
        match self {
            Self::Bummock(t) => t.clone(),
            _ => unreachable!(),
        }
    }
}

#[async_trait::async_trait]
impl ScannableTable for TableImpl {
    fn iter(&self) -> Result<TableIterRef> {
        match self {
            TableImpl::Bummock(t) => t.iter(),
            TableImpl::MViewTable(t) => Ok(Box::new(t.iter())),
            TableImpl::TestMViewTable(t) => Ok(Box::new(t.iter())),
        }
    }

    async fn get_data_by_columns(&self, column_ids: &[i32]) -> Result<BummockResult> {
        match self {
            TableImpl::Bummock(t) => t.get_data_by_columns(column_ids).await,
            TableImpl::MViewTable(_) => unimplemented!(),
            TableImpl::TestMViewTable(_) => unimplemented!(),
        }
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Sync + Send> {
        self
    }

    fn schema(&self) -> Schema {
        match self {
            TableImpl::Bummock(t) => t.schema(),
            TableImpl::MViewTable(t) => t.schema().clone(),
            TableImpl::TestMViewTable(t) => t.schema().clone(),
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
        tables.insert(table_id.clone(), TableImpl::Bummock(table.clone()));
        Ok(table)
    }

    fn get_table(&self, table_id: &TableId) -> Result<ScannableTableRef> {
        let tables = self.get_tables()?;
        tables
            .get(table_id)
            .cloned()
            .map(|t| Arc::new(t) as ScannableTableRef)
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

        let table_impl = match state_store {
            StateStoreImpl::MemoryStateStore(store) => {
                TableImpl::TestMViewTable(Arc::new(MViewTable::new(
                    Keyspace::table_root(store, table_id),
                    schema,
                    pk_columns,
                    orderings,
                )))
            }
            StateStoreImpl::HummockStateStore(store) => {
                TableImpl::MViewTable(Arc::new(MViewTable::new(
                    Keyspace::table_root(store, table_id),
                    schema,
                    pk_columns,
                    orderings,
                )))
            }
        };

        tables.insert(table_id.clone(), table_impl);
        Ok(())
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

pub type StreamTableManagerRef = Arc<dyn StreamTableManager>;
