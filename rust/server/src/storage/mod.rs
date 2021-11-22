mod bummock;
pub use bummock::*;

mod row_table;
pub use row_table::*;

pub mod hummock;

mod test_row_table;
pub use test_row_table::*;

mod object;

use crate::stream_op::StreamChunk;
use futures::channel::mpsc;
use risingwave_common::array::DataChunk;
use risingwave_common::array::InternalError;
use risingwave_common::catalog::Schema;
use risingwave_common::catalog::TableId;
use risingwave_common::error::Result;
use risingwave_pb::ToProst;
use risingwave_proto::plan::ColumnDesc;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::{Mutex, MutexGuard};

/// `Table` is an abstraction of the collection of columns and rows.
/// Each `Table` can be viewed as a flat sheet of a user created table.
#[async_trait::async_trait]
pub trait Table: Sync + Send {
    /// Append an entry to the table.
    async fn append(&self, data: DataChunk) -> Result<usize>;

    /// Get data from the table.
    async fn get_data(&self) -> Result<BummockResult>;

    /// Write a batch of changes. For now, we use `StreamChunk` to represent a write batch
    /// An assertion is put to assert only insertion operations are allowed.
    fn write(&self, chunk: &StreamChunk) -> Result<usize>;

    /// Create a stream from the table.
    /// The stream includes all existing rows in tables and changed rows in future.
    /// Note that only one stream is allowed for a table.
    /// Existing stream should be reused if the user create
    /// more than one materialized view from a table.
    fn create_stream(&self) -> Result<mpsc::UnboundedReceiver<StreamChunk>>;

    /// Get the column ids of the table.
    fn get_column_ids(&self) -> Result<Arc<Vec<i32>>>;

    /// Get the indices of the specific column.
    fn index_of_column_id(&self, column_id: i32) -> Result<usize>;

    /// Get the status of stream connection of this table.
    fn is_stream_connected(&self) -> bool;
}

#[async_trait::async_trait]
/// `TableManager` is an abstraction of managing a collection of tables.
/// The interface between executors and storage should be table-oriented.
/// `Database` is a logical concept and stored as metadata information.
pub trait TableManager: Sync + Send {
    /// Create a specific table.
    async fn create_table(&self, table_id: &TableId, schema: &Schema) -> Result<()>;

    /// Get a specific table.
    fn get_table(&self, table_id: &TableId) -> Result<TableTypes>;

    /// Drop a specific table.
    async fn drop_table(&self, table_id: &TableId) -> Result<()>;

    /// Create materialized view.
    fn create_materialized_view(
        &self,
        table_id: &TableId,
        columns: Vec<ColumnDesc>,
        pk_columns: Vec<usize>,
    ) -> Result<()>;
}

/// The enumeration of supported simple tables in `SimpleTableManager`.
#[derive(Clone)]
pub enum TableTypes {
    Row(Arc<MemRowTable>),
    BummockTable(Arc<BummockTable>),
    TestRow(Arc<TestRowTable>),
}

/// A simple implementation of in memory table for local tests.
/// It will be replaced in near future when replaced by locally
/// on-disk files.
pub struct SimpleTableManager {
    tables: Mutex<HashMap<TableId, TableTypes>>,
}

#[async_trait::async_trait]
impl TableManager for SimpleTableManager {
    async fn create_table(&self, table_id: &TableId, schema: &Schema) -> Result<()> {
        let mut tables = self.get_tables()?;

        ensure!(
            !tables.contains_key(table_id),
            "Table id already exists: {:?}",
            table_id
        );

        let column_count = schema.fields.len();
        ensure!(
            column_count > 0,
            "column count must be positive: {}",
            column_count
        );
        tables.insert(
            table_id.clone(),
            TableTypes::BummockTable(Arc::new(BummockTable::new(table_id, schema))),
        );
        Ok(())
    }

    fn get_table(&self, table_id: &TableId) -> Result<TableTypes> {
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

    #[cfg(test)]
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
        // TODO: Remove to_prost later.
        let schema = Schema::try_from(
            &columns
                .into_iter()
                .map(|c| c.to_prost())
                .collect::<Vec<_>>(),
        )?;
        tables.insert(
            table_id.clone(),
            TableTypes::TestRow(Arc::new(TestRowTable::new(schema, pk_columns))),
        );

        Ok(())
    }

    #[cfg(not(test))]
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
        // TODO: Remove to_prost later.
        let schema = Schema::try_from(
            &columns
                .into_iter()
                .map(|c| c.to_prost())
                .collect::<Vec<_>>(),
        )?;
        tables.insert(
            table_id.clone(),
            TableTypes::Row(Arc::new(MemRowTable::new(schema, pk_columns))),
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

    fn get_tables(&self) -> Result<MutexGuard<HashMap<TableId, TableTypes>>> {
        Ok(self.tables.lock().unwrap())
    }
}

/// Reference of a `TableManager`.
pub type TableManagerRef = Arc<dyn TableManager>;

pub enum TableScanOptions {
    SequentialScan,
    SparseIndexScan,
}
