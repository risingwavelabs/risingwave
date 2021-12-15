mod simple;
use std::any::Any;
use std::sync::Arc;

use risingwave_common::array::Row;
use risingwave_common::catalog::{Schema, TableId};
use risingwave_common::error::Result;
pub use simple::*;

use crate::bummock::BummockResult;
use crate::TableColumnDesc;

#[async_trait::async_trait]
/// `TableManager` is an abstraction of managing a collection of tables.
/// The interface between executors and storage should be table-oriented.
/// `Database` is a logical concept and stored as metadata information.
pub trait TableManager: Sync + Send + AsRef<dyn Any> {
    /// Create a specific table.
    async fn create_table(
        &self,
        table_id: &TableId,
        table_columns: Vec<TableColumnDesc>,
    ) -> Result<ScannableTableRef>;

    /// Get a specific table.
    fn get_table(&self, table_id: &TableId) -> Result<ScannableTableRef>;

    /// Drop a specific table.
    async fn drop_table(&self, table_id: &TableId) -> Result<()>;
}

#[async_trait::async_trait]
pub trait TableIter: Sync + Send {
    async fn open(&mut self) -> Result<()>;
    async fn next(&mut self) -> Result<Option<Row>>;
}

#[async_trait::async_trait]
pub trait ScannableTable: Sync + Send + Any {
    fn iter(&self) -> Result<TableIterRef>;
    /// Scan data of specified column ids
    ///
    /// In future, it will accept `predicates` for interested filtering conditions.
    async fn get_data_by_columns(&self, column_ids: &[i32]) -> Result<BummockResult>;
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Sync + Send>;
    fn schema(&self) -> Schema;
}

/// Reference of a `TableManager`.
pub type TableManagerRef = Arc<dyn TableManager>;
pub type ScannableTableRef = Arc<dyn ScannableTable>;
pub type TableIterRef = Box<dyn TableIter>;
